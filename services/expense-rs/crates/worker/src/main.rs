use axum::{routing::get, Json, Router};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use clap::Parser;
use connectors_ai::{
    ensure_llama_extraction_agent, local_ocr_stub, ExtractionRequest, ManagedExtractor,
    StatementExtractor,
};
use connectors_manual::{parse_csv, parse_pdf};
use expense_core::{
    default_app_data_dir, load_extraction_runtime_config_from_env, load_statement_blueprint_schema,
    new_health_status, BlueprintSchemaError, ExtractionRuntimeConfigError, HealthStatus,
    ImportStatus,
};
use serde::Deserialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::{net::SocketAddr, path::PathBuf};
use storage_sqlite::{
    claim_pending_job, clear_import_rows, connect, ensure_default_manual_account,
    get_extraction_settings, get_import_content, get_llama_agent_cache, get_llama_agent_readiness,
    insert_import_rows, mark_job_completed, mark_job_failed, run_migrations, upsert_or_get_statement,
    update_import_extraction_result, update_import_status, upsert_llama_agent_cache,
    upsert_llama_agent_readiness, LlamaAgentReadiness, LlamaAgentReadinessState, ParsedRowInput,
    StatementSummaryInput,
};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = default_db_path())]
    db_path: String,
    #[arg(long, default_value_t = 8082)]
    port: u16,
    #[arg(long, default_value_t = true)]
    migrate: bool,
    #[arg(long, default_value_t = 5)]
    poll_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct ImportJobPayload {
    import_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedFlowMode {
    New,
    Legacy,
}

impl ManagedFlowMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Legacy => "legacy",
        }
    }
}

fn managed_flow_mode_from_env() -> ManagedFlowMode {
    match std::env::var("EXTRACTION_MANAGED_FLOW_MODE")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("legacy") => ManagedFlowMode::Legacy,
        Some("new") | None => ManagedFlowMode::New,
        Some(value) => {
            warn!(mode = value, "invalid EXTRACTION_MANAGED_FLOW_MODE; defaulting to new");
            ManagedFlowMode::New
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();

    let db_path = PathBuf::from(args.db_path);
    let pool = connect(&db_path).await?;

    if args.migrate {
        run_migrations(&pool).await?;
    }

    if let Err(err) = ensure_llama_agent_ready(&pool).await {
        error!(error = %err, "failed to persist llama agent readiness");
    }
    info!(
        managed_flow_mode = managed_flow_mode_from_env().as_str(),
        "managed flow mode resolved"
    );

    let health_addr = SocketAddr::from(([127, 0, 0, 1], args.port));
    tokio::spawn(async move {
        let app = Router::new()
            .route("/health", get(health))
            .route("/api/v1/health", get(health));

        match tokio::net::TcpListener::bind(health_addr).await {
            Ok(listener) => {
                info!(%health_addr, "expense-worker health endpoint listening");
                if let Err(err) = axum::serve(listener, app).await {
                    error!(error = %err, "worker health server failed");
                }
            }
            Err(err) => error!(error = %err, "worker health bind failed"),
        }
    });

    info!(poll_seconds = args.poll_seconds, "worker loop started");
    loop {
        if let Err(err) = process_pending_import_jobs(&pool).await {
            error!(error = %err, "failed to process import jobs");
        }
        sleep(Duration::from_secs(args.poll_seconds)).await;
    }
}

#[cfg(test)]
fn validate_extraction_runtime_contract() -> anyhow::Result<()> {
    let extraction_config = load_extraction_runtime_config_from_env()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    load_statement_blueprint_schema(&extraction_config.llama_schema_version)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(())
}

fn bootstrap_log_path() -> PathBuf {
    if let Ok(explicit) = std::env::var("EXPENSE_BOOTSTRAP_LOG_PATH") {
        return PathBuf::from(explicit);
    }
    expense_core::default_app_data_dir()
        .join("logs")
        .join("extraction-bootstrap.log")
}

fn log_bootstrap_event(payload: serde_json::Value) {
    let path = bootstrap_log_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    let _ = writeln!(file, "{payload}");
}

fn classify_missing_or_schema_state(err_text: &str) -> LlamaAgentReadinessState {
    if err_text.contains("EXTRACTION_CONFIG_MISSING_REQUIRED_ENV") {
        return LlamaAgentReadinessState::Missing;
    }
    LlamaAgentReadinessState::SchemaInvalid
}

fn new_readiness_record(
    state: LlamaAgentReadinessState,
    agent_name: String,
    schema_version: String,
    agent_id: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
) -> LlamaAgentReadiness {
    LlamaAgentReadiness {
        state,
        agent_name,
        schema_version,
        agent_id,
        checked_at: chrono::Utc::now().to_rfc3339(),
        error_code,
        error_message,
    }
}

async fn ensure_llama_agent_ready(pool: &storage_sqlite::SqlitePool) -> anyhow::Result<()> {
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "worker_bootstrap_start"
    }));
    let extraction_config = match load_extraction_runtime_config_from_env() {
        Ok(v) => v,
        Err(err @ ExtractionRuntimeConfigError::MissingRequiredEnv(_)) => {
            let message = err.to_string();
            let readiness = new_readiness_record(
                LlamaAgentReadinessState::Missing,
                std::env::var("LLAMA_AGENT_NAME").unwrap_or_else(|_| "".to_string()),
                std::env::var("LLAMA_SCHEMA_VERSION").unwrap_or_else(|_| "".to_string()),
                None,
                Some("EXTRACTION_CONFIG_MISSING_REQUIRED_ENV".to_string()),
                Some(message.clone()),
            );
            upsert_llama_agent_readiness(pool, &readiness).await?;
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "worker_bootstrap_missing_env",
                "error_code": "EXTRACTION_CONFIG_MISSING_REQUIRED_ENV",
                "error_message": message,
            }));
            return Ok(());
        }
    };

    let versioned_agent_name = connectors_ai::versioned_agent_name(
        &extraction_config.llama_agent_name,
        &extraction_config.llama_schema_version,
    );

    let schema = match load_statement_blueprint_schema(&extraction_config.llama_schema_version) {
        Ok(schema) => schema,
        Err(err) => {
            let code = match err {
                BlueprintSchemaError::VersionNotFound(_) => "EXTRACTION_SCHEMA_INVALID",
                BlueprintSchemaError::InvalidJson(_) => "EXTRACTION_SCHEMA_INVALID",
                BlueprintSchemaError::InvalidContract(_) => "EXTRACTION_SCHEMA_INVALID",
            };
            let readiness = new_readiness_record(
                LlamaAgentReadinessState::SchemaInvalid,
                versioned_agent_name,
                extraction_config.llama_schema_version.clone(),
                None,
                Some(code.to_string()),
                Some(err.to_string()),
            );
            upsert_llama_agent_readiness(pool, &readiness).await?;
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "worker_bootstrap_schema_invalid",
                "error_code": code,
                "error_message": err.to_string(),
            }));
            return Ok(());
        }
    };

    match ensure_llama_extraction_agent(&extraction_config, &schema).await {
        Ok(agent) => {
            upsert_llama_agent_cache(pool, &agent.agent_id, &extraction_config.llama_schema_version)
                .await?;
            let readiness = new_readiness_record(
                LlamaAgentReadinessState::Configured,
                agent.agent_name,
                extraction_config.llama_schema_version,
                Some(agent.agent_id),
                None,
                None,
            );
            upsert_llama_agent_readiness(pool, &readiness).await?;
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "worker_bootstrap_configured",
                "agent_name": readiness.agent_name,
                "schema_version": readiness.schema_version,
                "agent_id": readiness.agent_id,
            }));
        }
        Err(err) => {
            let err_code = err.code.clone();
            let err_message = err.to_string();
            let state = if err.code == "EXTRACTION_SCHEMA_INVALID" {
                LlamaAgentReadinessState::SchemaInvalid
            } else {
                LlamaAgentReadinessState::ApiUnreachable
            };
            let readiness = new_readiness_record(
                state,
                versioned_agent_name,
                extraction_config.llama_schema_version,
                None,
                Some(err_code),
                Some(err_message),
            );
            upsert_llama_agent_readiness(pool, &readiness).await?;
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "worker_bootstrap_api_unreachable",
                "state": readiness.state.as_str(),
                "error_code": readiness.error_code,
                "error_message": readiness.error_message,
            }));
        }
    }

    Ok(())
}

async fn managed_readiness_snapshot(
    pool: &storage_sqlite::SqlitePool,
) -> anyhow::Result<(LlamaAgentReadiness, bool)> {
    let runtime = match load_extraction_runtime_config_from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            let state = classify_missing_or_schema_state(err.to_string().as_str());
            let fallback = new_readiness_record(
                state,
                std::env::var("LLAMA_AGENT_NAME").unwrap_or_else(|_| "".to_string()),
                std::env::var("LLAMA_SCHEMA_VERSION").unwrap_or_else(|_| "".to_string()),
                None,
                Some("EXTRACTION_AGENT_NOT_READY".to_string()),
                Some(err.to_string()),
            );
            return Ok((fallback, false));
        }
    };

    let readiness = get_llama_agent_readiness(pool)
        .await?
        .unwrap_or_else(|| {
            new_readiness_record(
                LlamaAgentReadinessState::Missing,
                connectors_ai::versioned_agent_name(
                    &runtime.llama_agent_name,
                    &runtime.llama_schema_version,
                ),
                runtime.llama_schema_version.clone(),
                None,
                Some("EXTRACTION_AGENT_NOT_READY".to_string()),
                Some("llama agent readiness not initialized".to_string()),
            )
        });

    let cache = get_llama_agent_cache(pool).await?;
    let cache_schema_matches = cache
        .as_ref()
        .is_some_and(|entry| entry.schema_version == runtime.llama_schema_version);
    let readiness_schema_matches = readiness.schema_version == runtime.llama_schema_version;
    let is_ready = readiness.state == LlamaAgentReadinessState::Configured
        && readiness_schema_matches
        && cache_schema_matches;

    if is_ready {
        return Ok((readiness, true));
    }

    if readiness.state == LlamaAgentReadinessState::Configured
        && (!readiness_schema_matches || !cache_schema_matches)
    {
        let downgraded = new_readiness_record(
            LlamaAgentReadinessState::SchemaInvalid,
            readiness.agent_name.clone(),
            readiness.schema_version.clone(),
            readiness.agent_id.clone(),
            Some("EXTRACTION_AGENT_NOT_READY".to_string()),
            Some("readiness/cache schema mismatch".to_string()),
        );
        return Ok((downgraded, false));
    }

    Ok((readiness, false))
}

async fn process_pending_import_jobs(pool: &storage_sqlite::SqlitePool) -> anyhow::Result<()> {
    let Some(job) = claim_pending_job(pool, "import_parse").await? else {
        return Ok(());
    };

    let job_attempt = job.attempts + 1;
    let job_payload: ImportJobPayload = serde_json::from_str(&job.payload_json)?;
    info!(
        job_id = %job.id,
        import_id = %job_payload.import_id,
        attempt = job_attempt,
        "claimed import_parse job"
    );
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "import_job_claimed",
        "job_id": job.id.clone(),
        "import_id": job_payload.import_id.clone(),
        "attempt": job_attempt,
    }));

    let result = async {
        update_import_status(
            pool,
            &job_payload.import_id,
            ImportStatus::Parsing,
            serde_json::json!({}),
            Vec::new(),
            Vec::new(),
            0,
        )
        .await?;
        info!(
            import_id = %job_payload.import_id,
            status = ImportStatus::Parsing.as_str(),
            "import parse status updated"
        );
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "import_status_updated",
            "import_id": job_payload.import_id.clone(),
            "status": ImportStatus::Parsing.as_str(),
            "parsed_rows": 0,
            "errors_count": 0,
            "warnings_count": 0,
            "review_required_count": 0,
        }));

        let blob = get_import_content(pool, &job_payload.import_id).await?;
        let account_id = ensure_default_manual_account(pool).await?;
        let decoded = STANDARD.decode(blob.content_base64.as_bytes())?;
        let settings = get_extraction_settings(pool).await?;
        info!(
            import_id = %job_payload.import_id,
            file_name = %blob.file_name,
            parser_type = %blob.parser_type,
            extraction_mode = %blob.extraction_mode,
            "loaded import payload and settings"
        );
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "import_payload_loaded",
            "import_id": job_payload.import_id.clone(),
            "file_name": blob.file_name.clone(),
            "parser_type": blob.parser_type.clone(),
            "extraction_mode": blob.extraction_mode.clone(),
            "provider_timeout_ms": settings.provider_timeout_ms,
            "max_provider_retries": settings.max_provider_retries,
            "managed_fallback_enabled": settings.managed_fallback_enabled,
        }));

        let mut statement_id_for_rows: Option<String> = None;
        let mut extraction_quality_metrics = serde_json::json!({});
        let mut extraction_reconciliation = serde_json::json!({});
        let parsed = if blob.parser_type == "csv" {
            parse_csv(&decoded, &account_id)
        } else if blob.parser_type == "pdf" {
            let extraction_mode = if blob.extraction_mode.trim().is_empty() {
                settings.default_extraction_mode.clone()
            } else {
                blob.extraction_mode.clone()
            };

            if extraction_mode == "local_ocr" {
                let result = local_ocr_stub(&ExtractionRequest {
                    import_id: job_payload.import_id.clone(),
                    account_id: account_id.clone(),
                    file_name: blob.file_name.clone(),
                    bytes: decoded,
                    max_provider_retries: settings.max_provider_retries,
                    timeout_ms: settings.provider_timeout_ms,
                    managed_fallback_enabled: settings.managed_fallback_enabled,
                })
                .await?;

                update_import_extraction_result(
                    pool,
                    &job_payload.import_id,
                    result.effective_provider.as_deref(),
                    &result
                        .attempts
                        .iter()
                        .map(|item| {
                            serde_json::to_value(item).unwrap_or_else(|_| serde_json::json!({}))
                        })
                        .collect::<Vec<_>>(),
                    &result.diagnostics,
                )
                .await?;
                log_bootstrap_event(serde_json::json!({
                    "ts_utc": chrono::Utc::now().to_rfc3339(),
                    "kind": "local_ocr_stub_result_written",
                    "import_id": job_payload.import_id.clone(),
                    "provider": result.effective_provider.clone(),
                    "errors_count": result.errors.len(),
                }));

                anyhow::bail!(
                    "{}",
                    result
                        .errors
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "LOCAL_OCR_NOT_IMPLEMENTED".to_string())
                );
            }

            let (readiness, managed_ready) = managed_readiness_snapshot(pool).await?;
            if !managed_ready {
                let blocked_state = readiness.state.as_str().to_string();
                log_bootstrap_event(serde_json::json!({
                    "ts_utc": chrono::Utc::now().to_rfc3339(),
                    "kind": "managed_gate_blocked",
                    "import_id": job_payload.import_id.clone(),
                    "state": blocked_state,
                    "agent_name": readiness.agent_name.clone(),
                    "schema_version": readiness.schema_version.clone(),
                    "error_code": readiness.error_code.clone(),
                }));
                let diagnostics = serde_json::json!({
                    "provider": "managed",
                    "managed_flow_mode": managed_flow_mode_from_env().as_str(),
                    "agent_readiness": readiness.clone(),
                });
                update_import_extraction_result(
                    pool,
                    &job_payload.import_id,
                    None,
                    &[],
                    &diagnostics,
                )
                .await?;
                anyhow::bail!(
                    "EXTRACTION_AGENT_NOT_READY:{}",
                    readiness.state.as_str()
                );
            }
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "managed_gate_allowed",
                "import_id": job_payload.import_id.clone(),
                "state": readiness.state.as_str(),
                "agent_name": readiness.agent_name.clone(),
                "schema_version": readiness.schema_version.clone(),
            }));

            let flow_mode = managed_flow_mode_from_env();
            info!(
                import_id = %job_payload.import_id,
                mode = flow_mode.as_str(),
                "managed extraction flow selected"
            );
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "managed_flow_selected",
                "import_id": job_payload.import_id.clone(),
                "mode": flow_mode.as_str(),
            }));
            let extractor = ManagedExtractor::default();
            let request = ExtractionRequest {
                import_id: job_payload.import_id.clone(),
                account_id: account_id.clone(),
                file_name: blob.file_name.clone(),
                bytes: decoded,
                max_provider_retries: settings.max_provider_retries,
                timeout_ms: settings.provider_timeout_ms,
                managed_fallback_enabled: settings.managed_fallback_enabled,
            };
            let result = if flow_mode == ManagedFlowMode::New {
                extractor.extract_pdf_new(&request).await?
            } else {
                extractor.extract_pdf(&request).await?
            };
            info!(
                import_id = %job_payload.import_id,
                mode = flow_mode.as_str(),
                provider = result.effective_provider.as_deref().unwrap_or("none"),
                rows = result.rows.len(),
                warnings = result.warnings.len(),
                errors = result.errors.len(),
                "managed extraction completed"
            );
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "managed_extraction_completed",
                "import_id": job_payload.import_id.clone(),
                "mode": flow_mode.as_str(),
                "provider": result.effective_provider.clone(),
                "rows_count": result.rows.len(),
                "warnings_count": result.warnings.len(),
                "errors_count": result.errors.len(),
            }));

            if flow_mode == ManagedFlowMode::New {
                let statement_context = result
                    .diagnostics
                    .get("statement_context")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let statement_summary = result
                    .diagnostics
                    .get("statement_summary")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let lineage = result
                    .diagnostics
                    .get("provider_lineage")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                if let (Some(period_start), Some(period_end)) = (
                    statement_context.get("period_start").and_then(|v| v.as_str()),
                    statement_context.get("period_end").and_then(|v| v.as_str()),
                ) {
                    if !period_start.is_empty() && !period_end.is_empty() {
                        let statement = upsert_or_get_statement(
                            pool,
                            &account_id,
                            period_start,
                            period_end,
                            statement_context
                                .get("statement_month")
                                .and_then(|v| v.as_str()),
                            Some("llamaextract_jobs"),
                            lineage.get("job_id").and_then(|v| v.as_str()),
                            lineage.get("run_id").and_then(|v| v.as_str()),
                            &lineage,
                            statement_context
                                .get("schema_version")
                                .and_then(|v| v.as_str())
                                .unwrap_or("statement_v1"),
                            StatementSummaryInput {
                                opening_balance_cents: statement_summary
                                    .get("opening_balance_cents")
                                    .and_then(|v| v.as_i64()),
                                opening_balance_date: statement_summary
                                    .get("opening_balance_date")
                                    .and_then(|v| v.as_str())
                                    .map(|v| v.to_string()),
                                closing_balance_cents: statement_summary
                                    .get("closing_balance_cents")
                                    .and_then(|v| v.as_i64()),
                                closing_balance_date: statement_summary
                                    .get("closing_balance_date")
                                    .and_then(|v| v.as_str())
                                    .map(|v| v.to_string()),
                                total_debits_cents: statement_summary
                                    .get("total_debits_cents")
                                    .and_then(|v| v.as_i64()),
                                total_credits_cents: statement_summary
                                    .get("total_credits_cents")
                                    .and_then(|v| v.as_i64()),
                                account_type: statement_summary
                                    .get("account_type")
                                    .and_then(|v| v.as_str())
                                    .map(|v| v.to_string()),
                                account_number_masked: statement_summary
                                    .get("account_number_masked")
                                    .and_then(|v| v.as_str())
                                    .map(|v| v.to_string()),
                                currency_code: statement_summary
                                    .get("currency_code")
                                    .and_then(|v| v.as_str())
                                    .map(|v| v.to_string()),
                            },
                        )
                        .await?;
                        statement_id_for_rows = Some(statement.id);
                        log_bootstrap_event(serde_json::json!({
                            "ts_utc": chrono::Utc::now().to_rfc3339(),
                            "kind": "statement_upserted",
                            "import_id": job_payload.import_id.clone(),
                            "account_id": account_id.clone(),
                            "statement_id": statement_id_for_rows.clone(),
                            "period_start": period_start,
                            "period_end": period_end,
                        }));
                    }
                }
            }

            let diagnostics = serde_json::json!({
                "managed_flow_mode": flow_mode.as_str(),
                "provider_lineage": result.diagnostics.get("provider_lineage").cloned().unwrap_or_else(|| serde_json::json!({})),
                "poll_status_trail": result.diagnostics.get("poll_status_trail").cloned().unwrap_or_else(|| serde_json::json!([])),
                "statement_context": result.diagnostics.get("statement_context").cloned().unwrap_or_else(|| serde_json::json!({})),
                "statement_summary": result.diagnostics.get("statement_summary").cloned().unwrap_or_else(|| serde_json::json!({})),
                "direction_quality": result.diagnostics.get("direction_quality").cloned().unwrap_or_else(|| serde_json::json!({
                    "unknown_count": 0,
                    "conflict_count": 0,
                    "conflicts": []
                })),
                "reconciliation": result.diagnostics.get("reconciliation").cloned().unwrap_or_else(|| serde_json::json!({
                    "skipped": true,
                    "reason": "not_available"
                })),
                "quality_metrics": result.diagnostics.get("quality_metrics").cloned().unwrap_or_else(|| serde_json::json!({})),
                "provider_diagnostics": result.diagnostics.clone(),
                "agent_readiness": readiness,
            });
            extraction_quality_metrics = diagnostics
                .get("quality_metrics")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            extraction_reconciliation = diagnostics
                .get("reconciliation")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));

            update_import_extraction_result(
                pool,
                &job_payload.import_id,
                result.effective_provider.as_deref(),
                &result
                    .attempts
                    .iter()
                    .map(|item| {
                        serde_json::to_value(item).unwrap_or_else(|_| serde_json::json!({}))
                    })
                    .collect::<Vec<_>>(),
                &diagnostics,
            )
            .await?;
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "import_extraction_result_written",
                "import_id": job_payload.import_id.clone(),
                "mode": flow_mode.as_str(),
                "provider": result.effective_provider.clone(),
                "attempts_count": result.attempts.len(),
            }));

            if !result.errors.is_empty() && result.rows.is_empty() {
                anyhow::bail!("{}", result.errors.join(" | "));
            }

            connectors_manual::ParsedImport {
                rows: result
                    .rows
                    .into_iter()
                    .map(|row| connectors_manual::ParsedRow {
                        row_index: row.row_index,
                        booked_at: row.booked_at,
                        amount_cents: row.amount_cents,
                        description: row.description,
                        confidence: row.confidence,
                        parse_error: row.parse_error,
                        normalized_txn_hash: row.normalized_txn_hash,
                        metadata: row.metadata,
                    })
                    .collect(),
                warnings: result.warnings,
                errors: result.errors,
            }
        } else {
            parse_pdf(&decoded, &account_id)
        };

        clear_import_rows(pool, &job_payload.import_id).await?;

        let parsed_rows: Vec<ParsedRowInput> = parsed
            .rows
            .iter()
            .map(|row| {
                let mut normalized_json = serde_json::json!({
                    "booked_at": row.booked_at,
                    "amount_cents": row.amount_cents,
                    "description": row.description,
                });
                if let Some(metadata) = row.metadata.as_ref().and_then(|v| v.as_object()) {
                    if let Some(obj) = normalized_json.as_object_mut() {
                        for (key, value) in metadata {
                            obj.insert(key.clone(), value.clone());
                        }
                    }
                }

                ParsedRowInput {
                    row_index: row.row_index,
                    normalized_json,
                    confidence: row.confidence,
                    parse_error: row.parse_error.clone(),
                    normalized_txn_hash: row.normalized_txn_hash.clone(),
                    account_id: Some(account_id.clone()),
                    statement_id: statement_id_for_rows.clone(),
                }
            })
            .collect();

        insert_import_rows(pool, &job_payload.import_id, parsed_rows).await?;
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "import_rows_inserted",
            "import_id": job_payload.import_id.clone(),
            "rows_total": parsed.rows.len(),
            "statement_id": statement_id_for_rows.clone(),
        }));

        let review_required_count = parsed
            .rows
            .iter()
            .filter(|row| {
                row.metadata
                    .as_ref()
                    .and_then(|v| v.get("direction"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    == "unknown"
            })
            .count() as i64;

        let status = if review_required_count > 0 {
            ImportStatus::ReviewRequired
        } else {
            ImportStatus::ReadyToCommit
        };
        let status_str = status.as_str().to_string();

        let parsed_rows_len = parsed.rows.len();
        let parsed_errors_len = parsed.errors.len();
        let parsed_warnings_len = parsed.warnings.len();
        update_import_status(
            pool,
            &job_payload.import_id,
            status,
            serde_json::json!({
                "parsed_rows": parsed_rows_len,
                "unresolved_direction_count": review_required_count,
                "quality_metrics": extraction_quality_metrics,
                "reconciliation": extraction_reconciliation
            }),
            parsed.errors,
            parsed.warnings,
            review_required_count,
        )
        .await?;
        info!(
            import_id = %job_payload.import_id,
            status = %status_str,
            parsed_rows = parsed_rows_len,
            review_required_count = review_required_count,
            "import parse status updated"
        );
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "import_status_updated",
            "import_id": job_payload.import_id.clone(),
            "status": status_str,
            "parsed_rows": parsed_rows_len,
            "errors_count": parsed_errors_len,
            "warnings_count": parsed_warnings_len,
            "review_required_count": review_required_count,
        }));

        Ok::<(), anyhow::Error>(())
    }
    .await;

    match result {
        Ok(_) => {
            mark_job_completed(pool, &job.id).await?;
            info!(job_id = %job.id, import_id = %job_payload.import_id, "job completed");
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "import_job_completed",
                "job_id": job.id.clone(),
                "import_id": job_payload.import_id.clone(),
            }));
        }
        Err(err) => {
            let err_text = err.to_string();
            error!(
                job_id = %job.id,
                import_id = %job_payload.import_id,
                error = %err_text,
                "job failed"
            );
            update_import_status(
                pool,
                &job_payload.import_id,
                ImportStatus::Failed,
                serde_json::json!({}),
                vec![err_text.clone()],
                Vec::new(),
                0,
            )
            .await?;
            info!(
                import_id = %job_payload.import_id,
                status = ImportStatus::Failed.as_str(),
                error = %err_text,
                "import parse status updated"
            );
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "import_status_updated",
                "import_id": job_payload.import_id.clone(),
                "status": ImportStatus::Failed.as_str(),
                "parsed_rows": 0,
                "errors_count": 1,
                "warnings_count": 0,
                "review_required_count": 0,
                "error": err_text,
            }));
            mark_job_failed(pool, &job.id, job_attempt, &err_text).await?;
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "import_job_failed",
                "job_id": job.id.clone(),
                "import_id": job_payload.import_id.clone(),
                "attempt": job_attempt,
                "error": err_text,
            }));
        }
    }

    Ok(())
}

async fn health() -> Json<HealthStatus> {
    Json(new_health_status("expense-worker"))
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "worker=info,axum=info".into()),
        )
        .compact()
        .init();
}

fn default_db_path() -> String {
    default_app_data_dir()
        .join("expense.db")
        .display()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;
    use std::sync::{Mutex, OnceLock};
    use storage_sqlite::{create_import, get_import_status, run_migrations, CreateImportInput};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[tokio::test]
    async fn health_returns_ok_payload() {
        let Json(status) = health().await;
        assert_eq!(status.service, "expense-worker");
        assert_eq!(status.status, "ok");
    }

    #[test]
    fn default_db_path_points_to_expense_db_file() {
        let value = default_db_path();
        assert!(
            value.ends_with("expense.db"),
            "default db path should end with expense.db, got: {value}"
        );
    }

    #[tokio::test]
    async fn worker_processes_import_job_and_updates_status() {
        let db_path = std::env::current_dir()
            .expect("cwd")
            .join(".tmp")
            .join(format!(
                "worker-import-test-{}.db",
                expense_core::new_idempotency_key()
            ));
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }

        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");

        let source = b"2026-03-01,Coffee,10.50\n03/02/2026,Bad Date,5.00";
        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "statement.pdf".to_string(),
                parser_type: "csv".to_string(),
                content_base64: STANDARD.encode(source),
                source_hash: "worker-hash".to_string(),
                extraction_mode: None,
            },
        )
        .await
        .expect("create import");

        process_pending_import_jobs(&pool)
            .await
            .expect("process import job");

        let status = get_import_status(&pool, &import_id)
            .await
            .expect("get status");
        assert_eq!(status.status, ImportStatus::ReviewRequired.as_str());
        assert_eq!(status.review_required_count, 2);
        assert!(status.summary.to_string().contains("parsed_rows"));

        let rows = storage_sqlite::list_import_rows_for_review(&pool, &import_id)
            .await
            .expect("review rows");
        assert_eq!(rows.len(), 2);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn worker_marks_local_ocr_mode_as_failed_stub() {
        let db_path = std::env::current_dir()
            .expect("cwd")
            .join(".tmp")
            .join(format!(
                "worker-local-ocr-stub-test-{}.db",
                expense_core::new_idempotency_key()
            ));
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }

        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");

        let source = b"%PDF-1.4 binary placeholder";
        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "statement.pdf".to_string(),
                parser_type: "pdf".to_string(),
                content_base64: STANDARD.encode(source),
                source_hash: "worker-local-ocr-hash".to_string(),
                extraction_mode: Some("local_ocr".to_string()),
            },
        )
        .await
        .expect("create import");

        process_pending_import_jobs(&pool)
            .await
            .expect("process import job");

        let status = get_import_status(&pool, &import_id)
            .await
            .expect("get status");
        assert_eq!(status.status, ImportStatus::Failed.as_str());
        assert!(status
            .errors
            .iter()
            .any(|e| e.contains("LOCAL_OCR_NOT_IMPLEMENTED")));

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn worker_blocks_managed_mode_when_agent_not_ready() {
        let _guard = env_lock().lock().expect("env lock");
        for key in [
            "LLAMA_CLOUD_API_KEY",
            "LLAMA_AGENT_NAME",
            "LLAMA_SCHEMA_VERSION",
            "LLAMA_CLOUD_ORGANIZATION_ID",
            "LLAMA_CLOUD_PROJECT_ID",
        ] {
            unsafe { std::env::remove_var(key) };
        }

        let db_path = std::env::current_dir()
            .expect("cwd")
            .join(".tmp")
            .join(format!(
                "worker-managed-gate-test-{}.db",
                expense_core::new_idempotency_key()
            ));
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }

        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");

        let source = b"%PDF-1.4 binary placeholder";
        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "statement.pdf".to_string(),
                parser_type: "pdf".to_string(),
                content_base64: STANDARD.encode(source),
                source_hash: "worker-managed-gate-hash".to_string(),
                extraction_mode: Some("managed".to_string()),
            },
        )
        .await
        .expect("create import");

        process_pending_import_jobs(&pool)
            .await
            .expect("process import job");

        let status = get_import_status(&pool, &import_id)
            .await
            .expect("get status");
        assert_eq!(status.status, ImportStatus::Failed.as_str());
        assert!(status
            .errors
            .iter()
            .any(|e| e.contains("EXTRACTION_AGENT_NOT_READY:missing")));
        assert_eq!(
            status
                .diagnostics
                .get("agent_readiness")
                .and_then(|v| v.get("state"))
                .and_then(|v| v.as_str()),
            Some("missing")
        );

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[test]
    fn extraction_runtime_contract_fails_when_required_env_missing() {
        let _guard = env_lock().lock().expect("env lock");
        for key in [
            "LLAMA_CLOUD_API_KEY",
            "LLAMA_AGENT_NAME",
            "LLAMA_SCHEMA_VERSION",
            "LLAMA_CLOUD_ORGANIZATION_ID",
            "LLAMA_CLOUD_PROJECT_ID",
        ] {
            unsafe { std::env::remove_var(key) };
        }
        let err = validate_extraction_runtime_contract().expect_err("expected missing env");
        assert!(err
            .to_string()
            .contains("EXTRACTION_CONFIG_MISSING_REQUIRED_ENV"));
    }

    #[test]
    fn extraction_runtime_contract_succeeds_with_required_env() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe {
            std::env::set_var("LLAMA_CLOUD_API_KEY", "x");
            std::env::set_var("LLAMA_AGENT_NAME", "agent");
            std::env::set_var("LLAMA_SCHEMA_VERSION", "statement_v1");
            std::env::remove_var("LLAMA_CLOUD_ORGANIZATION_ID");
            std::env::remove_var("LLAMA_CLOUD_PROJECT_ID");
        }

        validate_extraction_runtime_contract().expect("runtime contract should validate");

        for key in [
            "LLAMA_CLOUD_API_KEY",
            "LLAMA_AGENT_NAME",
            "LLAMA_SCHEMA_VERSION",
            "LLAMA_CLOUD_ORGANIZATION_ID",
            "LLAMA_CLOUD_PROJECT_ID",
        ] {
            unsafe { std::env::remove_var(key) };
        }
    }

    #[test]
    fn extraction_runtime_contract_succeeds_with_statement_v2() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe {
            std::env::set_var("LLAMA_CLOUD_API_KEY", "x");
            std::env::set_var("LLAMA_AGENT_NAME", "agent");
            std::env::set_var("LLAMA_SCHEMA_VERSION", "statement_v2");
            std::env::remove_var("LLAMA_CLOUD_ORGANIZATION_ID");
            std::env::remove_var("LLAMA_CLOUD_PROJECT_ID");
        }

        validate_extraction_runtime_contract().expect("runtime contract should validate");

        for key in [
            "LLAMA_CLOUD_API_KEY",
            "LLAMA_AGENT_NAME",
            "LLAMA_SCHEMA_VERSION",
            "LLAMA_CLOUD_ORGANIZATION_ID",
            "LLAMA_CLOUD_PROJECT_ID",
        ] {
            unsafe { std::env::remove_var(key) };
        }
    }

    #[test]
    fn managed_flow_mode_defaults_to_new() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe { std::env::remove_var("EXTRACTION_MANAGED_FLOW_MODE") };
        assert_eq!(managed_flow_mode_from_env(), ManagedFlowMode::New);
    }

    #[test]
    fn managed_flow_mode_accepts_legacy_and_invalid_defaults_new() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe { std::env::set_var("EXTRACTION_MANAGED_FLOW_MODE", "legacy") };
        assert_eq!(managed_flow_mode_from_env(), ManagedFlowMode::Legacy);
        unsafe { std::env::set_var("EXTRACTION_MANAGED_FLOW_MODE", "invalid") };
        assert_eq!(managed_flow_mode_from_env(), ManagedFlowMode::New);
        unsafe { std::env::remove_var("EXTRACTION_MANAGED_FLOW_MODE") };
    }
}
