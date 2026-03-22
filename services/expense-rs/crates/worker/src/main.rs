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
    insert_import_rows, mark_job_completed, mark_job_failed, run_migrations,
    update_import_extraction_result, update_import_status, upsert_llama_agent_cache,
    upsert_llama_agent_readiness, LlamaAgentReadiness, LlamaAgentReadinessState, ParsedRowInput,
};
use tokio::time::{sleep, Duration};
use tracing::{error, info};

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

        let blob = get_import_content(pool, &job_payload.import_id).await?;
        let account_id = ensure_default_manual_account(pool).await?;
        let decoded = STANDARD.decode(blob.content_base64.as_bytes())?;
        let settings = get_extraction_settings(pool).await?;

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

            let extractor = ManagedExtractor::default();
            let result = extractor
                .extract_pdf(&ExtractionRequest {
                    import_id: job_payload.import_id.clone(),
                    account_id: account_id.clone(),
                    file_name: blob.file_name.clone(),
                    bytes: decoded,
                    max_provider_retries: settings.max_provider_retries,
                    timeout_ms: settings.provider_timeout_ms,
                    managed_fallback_enabled: settings.managed_fallback_enabled,
                })
                .await?;

            let diagnostics = serde_json::json!({
                "provider_diagnostics": result.diagnostics.clone(),
                "agent_readiness": readiness,
            });

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
            .map(|row| ParsedRowInput {
                row_index: row.row_index,
                normalized_json: serde_json::json!({
                    "booked_at": row.booked_at,
                    "amount_cents": row.amount_cents,
                    "description": row.description,
                }),
                confidence: row.confidence,
                parse_error: row.parse_error.clone(),
                normalized_txn_hash: row.normalized_txn_hash.clone(),
                account_id: Some(account_id.clone()),
            })
            .collect();

        insert_import_rows(pool, &job_payload.import_id, parsed_rows).await?;

        let review_required_count = parsed
            .rows
            .iter()
            .filter(|row| row.parse_error.is_some() || row.confidence < 0.75)
            .count() as i64;

        let status = if review_required_count > 0 {
            ImportStatus::ReviewRequired
        } else {
            ImportStatus::ReadyToCommit
        };

        update_import_status(
            pool,
            &job_payload.import_id,
            status,
            serde_json::json!({ "parsed_rows": parsed.rows.len() }),
            parsed.errors,
            parsed.warnings,
            review_required_count,
        )
        .await?;

        Ok::<(), anyhow::Error>(())
    }
    .await;

    match result {
        Ok(_) => {
            mark_job_completed(pool, &job.id).await?;
        }
        Err(err) => {
            let err_text = err.to_string();
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
            mark_job_failed(pool, &job.id, job_attempt, &err_text).await?;
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
        assert_eq!(status.review_required_count, 1);
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
}
