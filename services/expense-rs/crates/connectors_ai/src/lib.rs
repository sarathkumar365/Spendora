use anyhow::anyhow;
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use expense_core::{
    compute_row_hash, load_extraction_runtime_config_from_env, normalize_description,
    parse_amount_cents, ExtractionRuntimeConfig,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashSet,
    env,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::time::sleep;

const MIN_PROVIDER_TIMEOUT_MS: i64 = 1_000;
const MAX_PROVIDER_TIMEOUT_MS: i64 = 180_000;
const LLAMA_PHASE_BUDGET_MS: u64 = 180_000;
const JOBS_POLL_HARD_CAP_SECS: u64 = 180;
const RECONCILIATION_TOLERANCE_CENTS: i64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LlamaAgentDescriptor {
    pub agent_id: String,
    pub agent_name: String,
}

#[derive(Debug, Clone)]
pub struct LlamaAgentBootstrapError {
    pub code: String,
    pub message: String,
    pub status_code: Option<u16>,
}

impl std::fmt::Display for LlamaAgentBootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for LlamaAgentBootstrapError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtractionMode {
    Managed,
    LocalOcr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManagedProvider {
    LlamaParse,
    OpenRouterPdfText,
}

impl ManagedProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LlamaParse => "llamaparse",
            Self::OpenRouterPdfText => "openrouter_pdf_text",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderAttempt {
    pub provider: String,
    pub attempt_no: i64,
    pub status_code: Option<u16>,
    pub outcome: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub latency_ms: i64,
    pub retry_decision: String,
    pub raw_response: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedRow {
    pub row_index: i64,
    pub booked_at: String,
    pub amount_cents: i64,
    pub description: String,
    pub confidence: f64,
    pub parse_error: Option<String>,
    pub normalized_txn_hash: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TransactionDirection {
    Debit,
    Credit,
    Transfer,
    Reversal,
    Unknown,
}

impl TransactionDirection {
    fn from_value(value: Option<&str>) -> Option<Self> {
        match value {
            Some(v) if v.eq_ignore_ascii_case("debit") => Some(Self::Debit),
            Some(v) if v.eq_ignore_ascii_case("credit") => Some(Self::Credit),
            Some(v) if v.eq_ignore_ascii_case("transfer") => Some(Self::Transfer),
            Some(v) if v.eq_ignore_ascii_case("reversal") => Some(Self::Reversal),
            Some(v) if v.eq_ignore_ascii_case("unknown") => Some(Self::Unknown),
            _ => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Debit => "debit",
            Self::Credit => "credit",
            Self::Transfer => "transfer",
            Self::Reversal => "reversal",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionResult {
    pub rows: Vec<ExtractedRow>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub effective_provider: Option<String>,
    pub attempts: Vec<ProviderAttempt>,
    pub diagnostics: Value,
}

#[derive(Debug, Clone)]
pub struct ExtractionRequest {
    pub import_id: String,
    pub account_id: String,
    pub file_name: String,
    pub bytes: Vec<u8>,
    pub max_provider_retries: i64,
    pub timeout_ms: i64,
    pub managed_fallback_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobsTerminalStatus {
    Success,
    PartialSuccess,
    Error,
    Cancelled,
}

impl JobsTerminalStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "SUCCESS",
            Self::PartialSuccess => "PARTIAL_SUCCESS",
            Self::Error => "ERROR",
            Self::Cancelled => "CANCELLED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum FallbackGateDecision {
    Allowed,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LlamaTerminalReason {
    ReadyWindowExceeded,
    ProviderReportedFailure,
    NonRetryableFailure,
    SchemaParseFailure,
}

#[derive(Debug, Clone)]
struct LlamaPhaseBudget {
    started_at_utc: chrono::DateTime<chrono::Utc>,
    deadline_at_utc: chrono::DateTime<chrono::Utc>,
    started_at: Instant,
    total_budget_ms: u64,
}

impl LlamaPhaseBudget {
    fn new(total_budget_ms: u64) -> Self {
        let started_at_utc = chrono::Utc::now();
        Self {
            started_at_utc,
            deadline_at_utc: started_at_utc
                + chrono::Duration::milliseconds(total_budget_ms as i64),
            started_at: Instant::now(),
            total_budget_ms,
        }
    }

    fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    fn remaining_ms(&self) -> u64 {
        self.total_budget_ms.saturating_sub(self.elapsed_ms())
    }

    fn is_exhausted(&self) -> bool {
        self.remaining_ms() == 0
    }

    fn call_timeout_ms(&self, configured_timeout_ms: i64) -> Option<u64> {
        let remaining = self.remaining_ms();
        if remaining == 0 {
            return None;
        }
        let configured = configured_timeout_ms.clamp(MIN_PROVIDER_TIMEOUT_MS, MAX_PROVIDER_TIMEOUT_MS) as u64;
        Some(remaining.min(configured).max(1))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LlamaPhaseDiagnostics {
    llama_phase_started_at: String,
    llama_phase_deadline_at: String,
    llama_poll_count: i64,
    llama_last_state: String,
    fallback_reason: Option<String>,
    terminal_failure_classification: Option<String>,
}

impl LlamaPhaseDiagnostics {
    fn new(budget: &LlamaPhaseBudget) -> Self {
        Self {
            llama_phase_started_at: budget.started_at_utc.to_rfc3339(),
            llama_phase_deadline_at: budget.deadline_at_utc.to_rfc3339(),
            llama_poll_count: 0,
            llama_last_state: "pending".to_string(),
            fallback_reason: None,
            terminal_failure_classification: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ManagedExtractor {
    http: reqwest::Client,
}

impl Default for ManagedExtractor {
    fn default() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
pub trait StatementExtractor {
    async fn extract_pdf(&self, request: &ExtractionRequest) -> anyhow::Result<ExtractionResult>;
}

#[async_trait]
impl StatementExtractor for ManagedExtractor {
    async fn extract_pdf(&self, request: &ExtractionRequest) -> anyhow::Result<ExtractionResult> {
        let mut attempts = Vec::new();
        let retries = request.max_provider_retries.clamp(1, 3);
        let llama_budget = LlamaPhaseBudget::new(LLAMA_PHASE_BUDGET_MS);
        let mut llama_diag = LlamaPhaseDiagnostics::new(&llama_budget);
        let mut terminal_reason: Option<LlamaTerminalReason> = None;

        for attempt_no in 1..=retries {
            log_decision_event(
                &request.import_id,
                &request.file_name,
                ManagedProvider::LlamaParse.as_str(),
                attempt_no,
                "continue_llama",
                "llama phase active",
            );
            let started = Instant::now();
            let call = llama_call(
                &self.http,
                request,
                attempt_no,
                ManagedProvider::LlamaParse.as_str(),
                &llama_budget,
            )
            .await;
            let latency = started.elapsed().as_millis() as i64;

            match call {
                Ok(outcome) => {
                    llama_diag.llama_last_state = outcome
                        .llama_last_state
                        .unwrap_or_else(|| "success".to_string());
                    llama_diag.llama_poll_count += outcome.llama_poll_count;
                    let (raw_out, truncated) = truncate_raw(outcome.raw.clone());
                    let attempt = ProviderAttempt {
                        provider: ManagedProvider::LlamaParse.as_str().to_string(),
                        attempt_no,
                        status_code: Some(outcome.status),
                        outcome: "success".to_string(),
                        error_code: None,
                        error_message: None,
                        latency_ms: latency,
                        retry_decision: "stop".to_string(),
                        raw_response: Some(raw_out),
                        truncated,
                    };
                    log_provider_attempt(&request.import_id, &attempt, &request.file_name);
                    attempts.push(attempt);
                    let mapped_rows = outcome
                        .rows
                        .into_iter()
                        .enumerate()
                        .map(|(idx, row)| map_row(idx as i64 + 1, &request.account_id, row))
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    let rows = dedupe_mapped_rows(mapped_rows);

                    return Ok(ExtractionResult {
                        rows,
                        warnings: Vec::new(),
                        errors: Vec::new(),
                        effective_provider: Some(ManagedProvider::LlamaParse.as_str().to_string()),
                        diagnostics: with_attempt_diagnostics(
                            outcome.diagnostics,
                            &attempts,
                            &llama_diag,
                        ),
                        attempts,
                    });
                }
                Err(err) => {
                    if let Some(state) = err.llama_state.clone() {
                        llama_diag.llama_last_state = state;
                    } else {
                        llama_diag.llama_last_state = "failed".to_string();
                    }
                    llama_diag.llama_poll_count += err.llama_poll_count;
                    llama_diag.terminal_failure_classification = Some(err.code.clone());
                    let retryable = is_retryable(&err.code);
                    let should_retry =
                        retryable && attempt_no < retries && !llama_budget.is_exhausted();
                    let (raw_response, truncated) = err
                        .raw_response
                        .clone()
                        .map(truncate_raw)
                        .map(|(raw, was_truncated)| (Some(raw), was_truncated))
                        .unwrap_or((None, false));
                    let attempt = ProviderAttempt {
                        provider: ManagedProvider::LlamaParse.as_str().to_string(),
                        attempt_no,
                        status_code: err.status_code,
                        outcome: "error".to_string(),
                        error_code: Some(err.code.clone()),
                        error_message: Some(err.message.clone()),
                        latency_ms: latency,
                        retry_decision: if should_retry {
                            "retry".to_string()
                        } else {
                            "stop".to_string()
                        },
                        raw_response,
                        truncated,
                    };
                    log_provider_attempt(&request.import_id, &attempt, &request.file_name);
                    attempts.push(attempt);

                    if should_retry {
                        log_decision_event(
                            &request.import_id,
                            &request.file_name,
                            ManagedProvider::LlamaParse.as_str(),
                            attempt_no,
                            "continue_llama",
                            "retryable llama error within phase budget",
                        );
                        sleep(backoff_for_attempt(attempt_no)).await;
                        continue;
                    }
                    terminal_reason = Some(classify_llama_terminal_reason(&err, &llama_budget));
                    break;
                }
            }
        }

        if terminal_reason.is_none() && llama_budget.is_exhausted() {
            terminal_reason = Some(LlamaTerminalReason::ReadyWindowExceeded);
            llama_diag.llama_last_state = "timed_out".to_string();
            llama_diag.terminal_failure_classification =
                Some("MANAGED_PROVIDER_TIMEOUT".to_string());
        }

        let fallback =
            can_fallback_to_openrouter(&terminal_reason, request.managed_fallback_enabled);
        if fallback == FallbackGateDecision::Blocked {
            log_decision_event(
                &request.import_id,
                &request.file_name,
                ManagedProvider::OpenRouterPdfText.as_str(),
                0,
                "fallback_blocked",
                "llama is not terminally failed yet",
            );
            return Ok(ExtractionResult {
                rows: Vec::new(),
                warnings: Vec::new(),
                errors: vec!["MANAGED_ALL_PROVIDERS_FAILED".to_string()],
                effective_provider: None,
                diagnostics: with_attempt_diagnostics(
                    serde_json::json!({ "status": "failed" }),
                    &attempts,
                    &llama_diag,
                ),
                attempts,
            });
        }

        llama_diag.fallback_reason = Some(format!(
            "llama terminal failure: {}",
            terminal_reason
                .as_ref()
                .map(llama_terminal_reason_label)
                .unwrap_or("unknown")
        ));
        log_decision_event(
            &request.import_id,
            &request.file_name,
            ManagedProvider::OpenRouterPdfText.as_str(),
            0,
            "fallback_allowed",
            llama_diag
                .fallback_reason
                .as_deref()
                .unwrap_or("llama terminal failure"),
        );

        for attempt_no in 1..=retries {
            let started = Instant::now();
            let call = openrouter_call(
                &self.http,
                request,
                attempt_no,
                ManagedProvider::OpenRouterPdfText.as_str(),
            )
            .await;
            let latency = started.elapsed().as_millis() as i64;
            match call {
                Ok(outcome) => {
                    let (raw_out, truncated) = truncate_raw(outcome.raw.clone());
                    let attempt = ProviderAttempt {
                        provider: ManagedProvider::OpenRouterPdfText.as_str().to_string(),
                        attempt_no,
                        status_code: Some(outcome.status),
                        outcome: "success".to_string(),
                        error_code: None,
                        error_message: None,
                        latency_ms: latency,
                        retry_decision: "stop".to_string(),
                        raw_response: Some(raw_out),
                        truncated,
                    };
                    log_provider_attempt(&request.import_id, &attempt, &request.file_name);
                    attempts.push(attempt);
                    let mapped_rows = outcome
                        .rows
                        .into_iter()
                        .enumerate()
                        .map(|(idx, row)| map_row(idx as i64 + 1, &request.account_id, row))
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    let rows = dedupe_mapped_rows(mapped_rows);
                    return Ok(ExtractionResult {
                        rows,
                        warnings: Vec::new(),
                        errors: Vec::new(),
                        effective_provider: Some(
                            ManagedProvider::OpenRouterPdfText.as_str().to_string(),
                        ),
                        diagnostics: with_attempt_diagnostics(
                            outcome.diagnostics,
                            &attempts,
                            &llama_diag,
                        ),
                        attempts,
                    });
                }
                Err(err) => {
                    let retryable = is_retryable(&err.code);
                    let should_retry = retryable && attempt_no < retries;
                    let (raw_response, truncated) = err
                        .raw_response
                        .clone()
                        .map(truncate_raw)
                        .map(|(raw, was_truncated)| (Some(raw), was_truncated))
                        .unwrap_or((None, false));
                    let attempt = ProviderAttempt {
                        provider: ManagedProvider::OpenRouterPdfText.as_str().to_string(),
                        attempt_no,
                        status_code: err.status_code,
                        outcome: "error".to_string(),
                        error_code: Some(err.code.clone()),
                        error_message: Some(err.message.clone()),
                        latency_ms: latency,
                        retry_decision: if should_retry {
                            "retry".to_string()
                        } else {
                            "stop".to_string()
                        },
                        raw_response,
                        truncated,
                    };
                    log_provider_attempt(&request.import_id, &attempt, &request.file_name);
                    attempts.push(attempt);
                    if should_retry {
                        sleep(backoff_for_attempt(attempt_no)).await;
                        continue;
                    }
                    break;
                }
            }
        }

        Ok(ExtractionResult {
            rows: Vec::new(),
            warnings: Vec::new(),
            errors: vec!["MANAGED_ALL_PROVIDERS_FAILED".to_string()],
            effective_provider: None,
            diagnostics: with_attempt_diagnostics(
                serde_json::json!({ "status": "failed" }),
                &attempts,
                &llama_diag,
            ),
            attempts,
        })
    }
}

impl ManagedExtractor {
    pub async fn extract_pdf_new(&self, request: &ExtractionRequest) -> anyhow::Result<ExtractionResult> {
        let cfg = load_extraction_runtime_config_from_env().map_err(|e| anyhow!(e.to_string()))?;
        let base_url = env::var("LLAMA_CLOUD_BASE_URL")
            .unwrap_or_else(|_| "https://api.cloud.llamaindex.ai".to_string());
        let agent_name = versioned_agent_name(&cfg.llama_agent_name, &cfg.llama_schema_version);
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "jobs_flow_start",
            "import_id": request.import_id,
            "file_name": request.file_name,
            "base_url": base_url,
            "agent_name": agent_name,
            "schema_version": cfg.llama_schema_version,
        }));
        let agent_id = find_llama_agent_id(&self.http, &base_url, &cfg, &agent_name)
            .await
            .map_err(|e| anyhow!(e.to_string()))?
            .ok_or_else(|| anyhow!("MANAGED_PROVIDER_BAD_REQUEST: agent_id not found for {agent_name}"))?;
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "jobs_agent_resolved",
            "import_id": request.import_id,
            "agent_name": agent_name,
            "agent_id": agent_id,
        }));

        let timeout_ms = request
            .timeout_ms
            .clamp(MIN_PROVIDER_TIMEOUT_MS, MAX_PROVIDER_TIMEOUT_MS) as u64;

        let uploaded = jobs_upload_file(
            &self.http,
            request,
            cfg.llama_cloud_api_key.as_str(),
            timeout_ms,
            base_url.as_str(),
        )
        .await?;
        let created = jobs_create_extraction_job(
            &self.http,
            request,
            cfg.llama_cloud_api_key.as_str(),
            timeout_ms,
            base_url.as_str(),
            uploaded.file_id.as_str(),
            agent_id.as_str(),
        )
        .await?;
        let polled = jobs_poll_status(
            &self.http,
            request,
            cfg.llama_cloud_api_key.as_str(),
            timeout_ms,
            base_url.as_str(),
            created.job_id.as_str(),
        )
        .await?;
        let fetched = jobs_fetch_result(
            &self.http,
            request,
            cfg.llama_cloud_api_key.as_str(),
            timeout_ms,
            base_url.as_str(),
            created.job_id.as_str(),
        )
        .await?;

        let validated = validate_jobs_result_payload(&fetched.payload, &cfg.llama_schema_version)?;
        let (period_start, period_end, period_derived) =
            resolve_period(validated.period_start.clone(), validated.period_end.clone(), &validated.rows)?;

        let mut out_rows = Vec::new();
        for (idx, item) in validated.rows.into_iter().enumerate() {
            out_rows.push(map_statement_row(idx as i64 + 1, &request.account_id, item));
        }
        let rows = dedupe_mapped_rows(out_rows);
        let rows = enrich_unresolved_directions(rows);
        let direction_conflicts = rows
            .iter()
            .filter_map(|row| {
                let parse_error = row.parse_error.as_ref()?;
                if !parse_error.contains("sign_conflict") && !parse_error.contains("missing_or_invalid_type") {
                    return None;
                }
                Some(serde_json::json!({
                    "row_index": row.row_index,
                    "reason": parse_error,
                }))
            })
            .collect::<Vec<_>>();
        let unknown_count = rows
            .iter()
            .filter(|row| {
                row.metadata
                    .as_ref()
                    .and_then(|v| v.get("direction"))
                    .and_then(|v| v.as_str())
                    .is_some_and(|value| value == "unknown")
            })
            .count();
        let statement_summary = validated
            .account_summary
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        let payload_snapshot = validated.payload_snapshot.clone();
        let rows_total = rows.len();
        let rows_with_missing_required_fields = rows
            .iter()
            .filter(|row| row_has_parse_flag(row, "missing_required_field:"))
            .count();
        let rows_with_sign_type_conflicts = rows
            .iter()
            .filter(|row| row_has_parse_flag(row, "sign_type_conflict"))
            .count();
        let rows_with_parse_defaults = rows
            .iter()
            .filter(|row| row_has_parse_flag(row, "parse_default_applied:"))
            .count();
        let direction_review_row_count = direction_review_row_count(&rows, &direction_conflicts);
        let reconciliation = compute_reconciliation(&statement_summary, &rows, RECONCILIATION_TOLERANCE_CENTS);
        let reconciliation_fail_count = reconciliation
            .get("fail_count")
            .and_then(|v| v.as_i64())
            .unwrap_or_default();
        let reconciliation_total_checks = reconciliation
            .get("total_checks")
            .and_then(|v| v.as_i64())
            .unwrap_or_default();
        let quality_metrics = serde_json::json!({
            "rows_total": rows_total,
            "direction_review_row_count": direction_review_row_count,
            "unknown_count": unknown_count,
            "unknown_rate": ratio(unknown_count as i64, rows_total as i64),
            "conflict_count": direction_conflicts.len(),
            "conflict_rate": ratio(direction_conflicts.len() as i64, rows_total as i64),
            "manual_override_count": 0,
            "manual_override_rate": ratio(0, direction_review_row_count),
            "reconciliation_fail_count": reconciliation_fail_count,
            "reconciliation_fail_rate": ratio(reconciliation_fail_count, reconciliation_total_checks),
            "rows_with_missing_required_fields": rows_with_missing_required_fields,
            "rows_with_sign_type_conflicts": rows_with_sign_type_conflicts,
            "rows_with_parse_defaults": rows_with_parse_defaults,
        });
        let diagnostics = serde_json::json!({
            "provider": "llamaextract_jobs",
            "provider_lineage": {
                "provider": "llamaextract_jobs",
                "file_id": uploaded.file_id,
                "job_id": created.job_id,
                "run_id": created.run_id,
                "agent_id": agent_id,
            },
            "poll_status_trail": polled.status_trail,
            "payload_snapshot": payload_snapshot,
            "statement_context": {
                "period_start": period_start,
                "period_end": period_end,
                "period_derived": period_derived,
                "statement_month": validated.statement_month,
                "schema_version": cfg.llama_schema_version,
            },
            "statement_summary": statement_summary,
            "direction_quality": {
                "unknown_count": unknown_count,
                "conflict_count": direction_conflicts.len(),
                "conflicts": direction_conflicts,
            },
            "reconciliation": reconciliation,
            "quality_metrics": quality_metrics,
            "terminal_status": polled.terminal_status.as_str(),
            "validation_counts": {
                "rows_total": rows.len(),
                "rows_with_parse_error": rows.iter().filter(|r| r.parse_error.is_some()).count(),
            }
        });
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "jobs_flow_completed",
            "import_id": request.import_id,
            "provider": "llamaextract_jobs",
            "file_id": uploaded.file_id,
            "job_id": created.job_id,
            "run_id": created.run_id,
            "terminal_status": polled.terminal_status.as_str(),
            "rows_total": rows.len(),
            "rows_with_parse_error": rows.iter().filter(|r| r.parse_error.is_some()).count(),
            "direction_unknown_count": unknown_count,
            "direction_conflict_count": direction_conflicts.len(),
            "period_start": period_start,
            "period_end": period_end,
            "period_derived": period_derived,
        }));
        Ok(ExtractionResult {
            rows,
            warnings: Vec::new(),
            errors: Vec::new(),
            effective_provider: Some("llamaextract_jobs".to_string()),
            attempts: vec![ProviderAttempt {
                provider: "llamaextract_jobs".to_string(),
                attempt_no: 1,
                status_code: Some(200),
                outcome: "success".to_string(),
                error_code: None,
                error_message: None,
                latency_ms: 0,
                retry_decision: "stop".to_string(),
                raw_response: Some(fetched.raw_response),
                truncated: fetched.truncated,
            }],
            diagnostics,
        })
    }
}

#[derive(Debug, Clone)]
struct JobsUploadResult {
    file_id: String,
}

#[derive(Debug, Clone)]
struct JobsCreateResult {
    job_id: String,
    run_id: Option<String>,
}

#[derive(Debug, Clone)]
struct JobsPollResult {
    terminal_status: JobsTerminalStatus,
    status_trail: Vec<Value>,
}

#[derive(Debug, Clone)]
struct JobsResultPayload {
    payload: Value,
    raw_response: String,
    truncated: bool,
}

#[derive(Debug, Clone)]
struct StatementRowCandidate {
    transaction_date: Option<String>,
    details: Option<String>,
    amount: Option<f64>,
    confidence: Option<f64>,
    tx_type: Option<String>,
}

#[derive(Debug, Clone)]
struct ValidatedJobsStatement {
    period_start: Option<String>,
    period_end: Option<String>,
    statement_month: Option<String>,
    account_summary: Option<Value>,
    payload_snapshot: Value,
    rows: Vec<StatementRowCandidate>,
}

async fn jobs_upload_file(
    http: &reqwest::Client,
    request: &ExtractionRequest,
    api_key: &str,
    timeout_ms: u64,
    base_url: &str,
) -> anyhow::Result<JobsUploadResult> {
    let candidate_forms = [
        ("/api/v1/beta/files", "file", true),
        ("/api/v1/files", "upload_file", false),
        ("/api/v1/files", "file", false),
    ];
    let mut last_error: Option<String> = None;
    for (path, file_field, include_purpose) in candidate_forms {
        let endpoint = format!("{base_url}{path}");
        let file_part = reqwest::multipart::Part::bytes(request.bytes.clone())
            .file_name(request.file_name.clone())
            .mime_str("application/pdf")
            .map_err(|e| anyhow!("MANAGED_PROVIDER_BAD_REQUEST: {e}"))?;
        let purpose = env::var("LLAMAEXTRACT_FILE_PURPOSE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "extract".to_string());
        let mut form = reqwest::multipart::Form::new().part(file_field, file_part);
        if include_purpose {
            form = form.text("purpose", purpose);
        }
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "jobs_upload_attempt",
            "import_id": request.import_id,
            "file_name": request.file_name,
            "endpoint_path": path,
            "file_field": file_field,
            "include_purpose": include_purpose,
        }));
        let response = http
            .post(endpoint.clone())
            .header("Authorization", format!("Bearer {api_key}"))
            .multipart(form)
            .timeout(Duration::from_millis(timeout_ms))
            .send()
            .await;
        let Ok(response) = response else {
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "jobs_upload_transport_error",
                "import_id": request.import_id,
                "endpoint_path": path,
                "file_field": file_field,
            }));
            continue;
        };
        let status = response.status();
        let response_headers = redact_headers(response.headers());
        let raw = response.text().await.unwrap_or_default();
        log_external_api_raw_event(&ExternalApiRawEvent {
            ts_utc: chrono::Utc::now().to_rfc3339(),
            kind: "external_api_raw",
            import_id: Some(request.import_id.clone()),
            file_name: Some(request.file_name.clone()),
            provider: "llamaextract_jobs".to_string(),
            attempt_no: None,
            operation: "jobs_upload_file".to_string(),
            method: "POST".to_string(),
            url: endpoint.clone(),
            request_body_meta: Some(serde_json::json!({
                "endpoint_path": path,
                "file_field": file_field,
                "include_purpose": include_purpose,
            })),
            status_code: Some(status.as_u16()),
            response_headers_redacted: response_headers,
            response_body_raw: Some(raw.clone()),
            error_message: None,
        });
        if !status.is_success() {
            let raw_trimmed = truncate_for_log(raw.as_str(), 800);
            last_error = Some(format!(
                "path={path} file_field={file_field} include_purpose={include_purpose} status={} body={}",
                status.as_u16(),
                raw_trimmed
            ));
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "jobs_upload_non_success",
                "import_id": request.import_id,
                "endpoint_path": path,
                "file_field": file_field,
                "include_purpose": include_purpose,
                "status_code": status.as_u16(),
                "response_body": raw_trimmed,
            }));
            if matches!(
                status,
                StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
            ) {
                continue;
            }
            return Err(anyhow!(
                "MANAGED_PROVIDER_API_UNREACHABLE: file upload failed ({}): {}",
                status.as_u16(),
                raw
            ));
        }
        let value: Value = serde_json::from_str(raw.as_str())
            .map_err(|e| anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: upload invalid json: {e}"))?;
        let file_id = find_string_field(
            &value,
            &["id", "file_id", "data.id", "data.file_id", "file.id"],
        )
        .ok_or_else(|| anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: upload missing file id"))?;
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "jobs_upload_success",
            "import_id": request.import_id,
            "file_name": request.file_name,
            "file_id": file_id,
            "endpoint_path": path,
            "file_field": file_field,
            "include_purpose": include_purpose,
        }));
        return Ok(JobsUploadResult { file_id });
    }
    let detail = last_error.unwrap_or_else(|| "upload request could not be completed".to_string());
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "jobs_upload_failed",
        "import_id": request.import_id,
        "detail": detail,
    }));
    Err(anyhow!("MANAGED_PROVIDER_API_UNREACHABLE: {detail}"))
}

async fn jobs_create_extraction_job(
    http: &reqwest::Client,
    request: &ExtractionRequest,
    api_key: &str,
    timeout_ms: u64,
    base_url: &str,
    file_id: &str,
    agent_id: &str,
) -> anyhow::Result<JobsCreateResult> {
    let endpoint = format!("{base_url}/api/v1/extraction/jobs");
    let bodies = [
        serde_json::json!({
            "extraction_agent_id": agent_id,
            "file_id": file_id,
        }),
        serde_json::json!({
            "agent_id": agent_id,
            "file_id": file_id,
        }),
    ];
    let mut last_error: Option<String> = None;
    for body in bodies {
        let body_keys = body
            .as_object()
            .map(|o| o.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "jobs_create_attempt",
            "import_id": request.import_id,
            "job_endpoint": endpoint,
            "body_keys": body_keys,
        }));
        let response = http
            .post(endpoint.clone())
            .header("Authorization", format!("Bearer {api_key}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .timeout(Duration::from_millis(timeout_ms))
            .send()
            .await
            .map_err(|e| anyhow!("MANAGED_PROVIDER_API_UNREACHABLE: {e}"))?;
        let status = response.status();
        let response_headers = redact_headers(response.headers());
        let raw = response.text().await.unwrap_or_default();
        log_external_api_raw_event(&ExternalApiRawEvent {
            ts_utc: chrono::Utc::now().to_rfc3339(),
            kind: "external_api_raw",
            import_id: Some(request.import_id.clone()),
            file_name: Some(request.file_name.clone()),
            provider: "llamaextract_jobs".to_string(),
            attempt_no: None,
            operation: "jobs_create_extraction_job".to_string(),
            method: "POST".to_string(),
            url: endpoint.clone(),
            request_body_meta: Some(serde_json::json!({
                "body_keys": body_keys,
            })),
            status_code: Some(status.as_u16()),
            response_headers_redacted: response_headers,
            response_body_raw: Some(raw.clone()),
            error_message: None,
        });
        if !status.is_success() {
            let raw_trimmed = truncate_for_log(raw.as_str(), 800);
            last_error = Some(format!(
                "job create failed with body_keys={:?} status={} body={}",
                body_keys,
                status.as_u16(),
                raw_trimmed
            ));
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "jobs_create_non_success",
                "import_id": request.import_id,
                "status_code": status.as_u16(),
                "body_keys": body_keys,
                "response_body": raw_trimmed,
            }));
            if matches!(status, StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY) {
                continue;
            }
            return Err(anyhow!(
                "MANAGED_PROVIDER_API_UNREACHABLE: job create failed ({}): {}",
                status.as_u16(),
                raw
            ));
        }
        let value: Value = serde_json::from_str(raw.as_str())
            .map_err(|e| anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: create invalid json: {e}"))?;
        let job_id = find_string_field(&value, &["id", "job_id", "data.id", "data.job_id"])
            .ok_or_else(|| anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: create missing job id"))?;
        let run_id = find_string_field(&value, &["run_id", "data.run_id"]);
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "jobs_create_success",
            "import_id": request.import_id,
            "job_id": job_id,
            "run_id": run_id,
        }));
        return Ok(JobsCreateResult { job_id, run_id });
    }
    let detail = last_error.unwrap_or_else(|| "job create did not return a response".to_string());
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "jobs_create_failed",
        "import_id": request.import_id,
        "detail": detail,
    }));
    Err(anyhow!("MANAGED_PROVIDER_API_UNREACHABLE: {detail}"))
}

async fn jobs_poll_status(
    http: &reqwest::Client,
    request: &ExtractionRequest,
    api_key: &str,
    timeout_ms: u64,
    base_url: &str,
    job_id: &str,
) -> anyhow::Result<JobsPollResult> {
    let max_polls = env::var("LLAMAEXTRACT_POLL_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(120)
        .clamp(3, 180);
    let hard_cap = Duration::from_secs(JOBS_POLL_HARD_CAP_SECS);
    let poll_started_at = Instant::now();
    let mut trail = Vec::new();
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "jobs_poll_start",
        "import_id": request.import_id,
        "job_id": job_id,
        "max_polls": max_polls,
        "hard_cap_seconds": JOBS_POLL_HARD_CAP_SECS,
    }));

    for poll_no in 1..=max_polls {
        if poll_started_at.elapsed() >= hard_cap {
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "jobs_poll_timeout",
                "import_id": request.import_id,
                "job_id": job_id,
                "reason": "hard_cap_reached_before_poll",
                "elapsed_ms": poll_started_at.elapsed().as_millis(),
                "hard_cap_ms": hard_cap.as_millis(),
            }));
            return Err(anyhow!("MANAGED_PROVIDER_TIMEOUT: polling hard cap exceeded"));
        }
        let endpoint = format!("{base_url}/api/v1/extraction/jobs/{job_id}");
        let response = http
            .get(endpoint.clone())
            .header("Authorization", format!("Bearer {api_key}"))
            .header("accept", "application/json")
            .timeout(Duration::from_millis(timeout_ms))
            .send()
            .await
            .map_err(|e| anyhow!("MANAGED_PROVIDER_API_UNREACHABLE: {e}"))?;
        let status = response.status();
        let response_headers = redact_headers(response.headers());
        let raw = response.text().await.unwrap_or_default();
        log_external_api_raw_event(&ExternalApiRawEvent {
            ts_utc: chrono::Utc::now().to_rfc3339(),
            kind: "external_api_raw",
            import_id: Some(request.import_id.clone()),
            file_name: Some(request.file_name.clone()),
            provider: "llamaextract_jobs".to_string(),
            attempt_no: Some(poll_no as i64),
            operation: "jobs_poll_status".to_string(),
            method: "GET".to_string(),
            url: endpoint.clone(),
            request_body_meta: None,
            status_code: Some(status.as_u16()),
            response_headers_redacted: response_headers,
            response_body_raw: Some(raw.clone()),
            error_message: None,
        });
        if !status.is_success() {
            log_bootstrap_event(serde_json::json!({
                "ts_utc": chrono::Utc::now().to_rfc3339(),
                "kind": "jobs_poll_non_success",
                "import_id": request.import_id,
                "job_id": job_id,
                "poll_no": poll_no,
                "status_code": status.as_u16(),
                "response_body": truncate_for_log(raw.as_str(), 800),
            }));
            return Err(anyhow!(
                "MANAGED_PROVIDER_API_UNREACHABLE: poll failed ({}): {}",
                status.as_u16(),
                raw
            ));
        }
        let value: Value = serde_json::from_str(raw.as_str())
            .map_err(|e| anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: poll invalid json: {e}"))?;
        let state = find_string_field(
            &value,
            &["status", "state", "data.status", "data.state", "job.status", "job.state"],
        )
        .unwrap_or_else(|| "UNKNOWN".to_string());
        trail.push(serde_json::json!({
            "poll_no": poll_no,
            "status": state,
        }));
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "jobs_poll_tick",
            "import_id": request.import_id,
            "job_id": job_id,
            "poll_no": poll_no,
            "status": state,
        }));
        match classify_job_status(state.as_str()) {
            JobStatusClass::TerminalSuccess(terminal) => {
                log_bootstrap_event(serde_json::json!({
                    "ts_utc": chrono::Utc::now().to_rfc3339(),
                    "kind": "jobs_poll_terminal_success",
                    "import_id": request.import_id,
                    "job_id": job_id,
                    "poll_no": poll_no,
                    "status": terminal.as_str(),
                }));
                return Ok(JobsPollResult {
                    terminal_status: terminal,
                    status_trail: trail,
                });
            }
            JobStatusClass::TerminalFailure(terminal) => {
                log_bootstrap_event(serde_json::json!({
                    "ts_utc": chrono::Utc::now().to_rfc3339(),
                    "kind": "jobs_poll_terminal_failure",
                    "import_id": request.import_id,
                    "job_id": job_id,
                    "poll_no": poll_no,
                    "status": terminal.as_str(),
                }));
                return Err(anyhow!(
                    "MANAGED_PROVIDER_API_UNREACHABLE: terminal status {}",
                    terminal.as_str()
                ));
            }
            JobStatusClass::InProgress => {
                let delay = adaptive_poll_delay(poll_no as i64);
                let elapsed = poll_started_at.elapsed();
                if elapsed + delay >= hard_cap {
                    log_bootstrap_event(serde_json::json!({
                        "ts_utc": chrono::Utc::now().to_rfc3339(),
                        "kind": "jobs_poll_timeout",
                        "import_id": request.import_id,
                        "job_id": job_id,
                        "reason": "hard_cap_reached_during_backoff",
                        "elapsed_ms": elapsed.as_millis(),
                        "hard_cap_ms": hard_cap.as_millis(),
                        "poll_no": poll_no,
                    }));
                    return Err(anyhow!("MANAGED_PROVIDER_TIMEOUT: polling hard cap exceeded"));
                }
                sleep(delay).await;
            }
            JobStatusClass::Unknown => {
                if poll_no == max_polls {
                    log_bootstrap_event(serde_json::json!({
                        "ts_utc": chrono::Utc::now().to_rfc3339(),
                        "kind": "jobs_poll_unknown_status_exhausted",
                        "import_id": request.import_id,
                        "job_id": job_id,
                        "poll_no": poll_no,
                        "status": state,
                    }));
                    return Err(anyhow!("MANAGED_PROVIDER_UNKNOWN_STATUS: {}", state));
                }
                let delay = adaptive_poll_delay(poll_no as i64);
                let elapsed = poll_started_at.elapsed();
                if elapsed + delay >= hard_cap {
                    log_bootstrap_event(serde_json::json!({
                        "ts_utc": chrono::Utc::now().to_rfc3339(),
                        "kind": "jobs_poll_timeout",
                        "import_id": request.import_id,
                        "job_id": job_id,
                        "reason": "hard_cap_reached_during_backoff",
                        "elapsed_ms": elapsed.as_millis(),
                        "hard_cap_ms": hard_cap.as_millis(),
                        "poll_no": poll_no,
                        "status": state,
                    }));
                    return Err(anyhow!("MANAGED_PROVIDER_TIMEOUT: polling hard cap exceeded"));
                }
                sleep(delay).await;
            }
        }
    }
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "jobs_poll_timeout",
        "import_id": request.import_id,
        "job_id": job_id,
        "reason": "max_polls_exhausted",
        "elapsed_ms": poll_started_at.elapsed().as_millis(),
        "hard_cap_ms": hard_cap.as_millis(),
    }));
    Err(anyhow!("MANAGED_PROVIDER_TIMEOUT: polling exhausted"))
}

async fn jobs_fetch_result(
    http: &reqwest::Client,
    request: &ExtractionRequest,
    api_key: &str,
    timeout_ms: u64,
    base_url: &str,
    job_id: &str,
) -> anyhow::Result<JobsResultPayload> {
    let endpoint = format!("{base_url}/api/v1/extraction/jobs/{job_id}/result");
    let response = http
        .get(endpoint.clone())
        .header("Authorization", format!("Bearer {api_key}"))
        .header("accept", "application/json")
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
        .map_err(|e| anyhow!("MANAGED_PROVIDER_API_UNREACHABLE: {e}"))?;
    let status = response.status();
    let response_headers = redact_headers(response.headers());
    let raw = response.text().await.unwrap_or_default();
    log_external_api_raw_event(&ExternalApiRawEvent {
        ts_utc: chrono::Utc::now().to_rfc3339(),
        kind: "external_api_raw",
        import_id: Some(request.import_id.clone()),
        file_name: Some(request.file_name.clone()),
        provider: "llamaextract_jobs".to_string(),
        attempt_no: None,
        operation: "jobs_fetch_result".to_string(),
        method: "GET".to_string(),
        url: endpoint.clone(),
        request_body_meta: None,
        status_code: Some(status.as_u16()),
        response_headers_redacted: response_headers,
        response_body_raw: Some(raw.clone()),
        error_message: None,
    });
    if !status.is_success() {
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "jobs_result_non_success",
            "import_id": request.import_id,
            "job_id": job_id,
            "status_code": status.as_u16(),
            "response_body": truncate_for_log(raw.as_str(), 800),
        }));
        return Err(anyhow!(
            "MANAGED_PROVIDER_API_UNREACHABLE: fetch result failed ({}): {}",
            status.as_u16(),
            raw
        ));
    }
    let value: Value = serde_json::from_str(raw.as_str())
        .map_err(|e| anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: result invalid json: {e}"))?;
    let raw_preview = truncate_for_log(raw.as_str(), 4000);
    let (raw_response, truncated) = truncate_raw(raw.clone());
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "jobs_result_fetched",
        "import_id": request.import_id,
        "job_id": job_id,
        "payload_top_keys": value
            .as_object()
            .map(|o| o.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default(),
        "response_body": raw_preview,
    }));
    Ok(JobsResultPayload {
        payload: value,
        raw_response,
        truncated,
    })
}

fn truncate_for_log(raw: &str, max_chars: usize) -> String {
    if raw.len() <= max_chars {
        return raw.to_string();
    }
    let mut out = raw.chars().take(max_chars).collect::<String>();
    out.push_str("...(truncated)");
    out
}

enum JobStatusClass {
    TerminalSuccess(JobsTerminalStatus),
    TerminalFailure(JobsTerminalStatus),
    InProgress,
    Unknown,
}

fn classify_job_status(raw: &str) -> JobStatusClass {
    match raw {
        "SUCCESS" => JobStatusClass::TerminalSuccess(JobsTerminalStatus::Success),
        "PARTIAL_SUCCESS" => JobStatusClass::TerminalSuccess(JobsTerminalStatus::PartialSuccess),
        "ERROR" => JobStatusClass::TerminalFailure(JobsTerminalStatus::Error),
        "CANCELLED" => JobStatusClass::TerminalFailure(JobsTerminalStatus::Cancelled),
        "PENDING" | "QUEUED" | "RUNNING" | "IN_PROGRESS" => JobStatusClass::InProgress,
        _ => JobStatusClass::Unknown,
    }
}

fn validate_jobs_result_payload(
    payload: &Value,
    schema_version: &str,
) -> anyhow::Result<ValidatedJobsStatement> {
    let envelope = payload
        .get("result")
        .or_else(|| payload.get("data"))
        .or_else(|| payload.get("output"))
        .unwrap_or(payload);
    if schema_version == "statement_v2" {
        for key in [
            "statement_period",
            "statement_date",
            "account_details",
            "due_this_statement",
            "account_summary",
            "interest_information",
            "transactions",
            "transaction_subtotals",
        ] {
            if envelope.get(key).is_none() {
                return Err(anyhow!(
                    "MANAGED_PROVIDER_SCHEMA_INVALID: missing {}",
                    key
                ));
            }
        }
    }
    let period_start = if schema_version == "statement_v2" {
        envelope
            .get("statement_period")
            .and_then(|v| v.get("start_date"))
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    } else {
        envelope
            .get("period_start")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    };
    let period_end = if schema_version == "statement_v2" {
        envelope
            .get("statement_period")
            .and_then(|v| v.get("end_date"))
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    } else {
        envelope
            .get("period_end")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    };
    let statement_month = if schema_version == "statement_v2" {
        envelope
            .get("statement_date")
            .and_then(|v| v.as_str())
            .and_then(|v| v.get(0..7))
            .map(|v| v.to_string())
    } else {
        envelope
            .get("statement_month")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    };
    let account_summary = envelope.get("account_summary").cloned();
    if schema_version == "statement_v2" && account_summary.is_none() {
        return Err(anyhow!(
            "MANAGED_PROVIDER_SCHEMA_INVALID: missing account_summary"
        ));
    }
    let txs = envelope
        .get("transactions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: missing transactions[]"))?;
    let mut rows = Vec::new();
    for item in txs {
        if schema_version == "statement_v2" {
            rows.push(StatementRowCandidate {
                transaction_date: item
                    .get("transaction_date")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
                details: item
                    .get("details")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
                amount: item.get("amount").and_then(as_f64_from_value),
                confidence: None,
                tx_type: item.get("type").and_then(|v| v.as_str()).map(|v| v.to_string()),
            });
        } else {
            rows.push(StatementRowCandidate {
                transaction_date: item
                    .get("booked_at")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
                details: item
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
                amount: item
                    .get("amount_cents")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as f64 / 100.0),
                confidence: item.get("confidence").and_then(|v| v.as_f64()),
                tx_type: item
                    .get("direction")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
            });
        }
    }
    if rows.is_empty() {
        return Err(anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: no transaction rows"));
    }
    Ok(ValidatedJobsStatement {
        period_start,
        period_end,
        statement_month,
        account_summary,
        payload_snapshot: envelope.clone(),
        rows,
    })
}

fn as_f64_from_value(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|v| v as f64))
        .or_else(|| value.as_u64().map(|v| v as f64))
}

fn resolve_period(
    period_start: Option<String>,
    period_end: Option<String>,
    rows: &[StatementRowCandidate],
) -> anyhow::Result<(String, String, bool)> {
    if let (Some(start), Some(end)) = (period_start, period_end) {
        return Ok((start, end, false));
    }
    let mut dates = rows
        .iter()
        .filter_map(|r| r.transaction_date.as_deref())
        .filter(|d| is_iso_date(d))
        .collect::<Vec<_>>();
    dates.sort_unstable();
    let start = dates
        .first()
        .ok_or_else(|| anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: cannot derive period_start"))?;
    let end = dates
        .last()
        .ok_or_else(|| anyhow!("MANAGED_PROVIDER_SCHEMA_INVALID: cannot derive period_end"))?;
    Ok(((*start).to_string(), (*end).to_string(), true))
}

fn ratio(numerator: i64, denominator: i64) -> f64 {
    if denominator <= 0 {
        return 0.0;
    }
    numerator as f64 / denominator as f64
}

fn row_has_parse_flag(row: &ExtractedRow, prefix_or_flag: &str) -> bool {
    row.metadata
        .as_ref()
        .and_then(|v| v.get("parse_flags"))
        .and_then(|v| v.as_array())
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.as_str()
                    .is_some_and(|value| value == prefix_or_flag || value.starts_with(prefix_or_flag))
            })
        })
}

fn direction_review_row_count(rows: &[ExtractedRow], conflicts: &[Value]) -> i64 {
    let mut indices = HashSet::<i64>::new();
    for row in rows {
        let is_unknown = row
            .metadata
            .as_ref()
            .and_then(|v| v.get("direction"))
            .and_then(|v| v.as_str())
            .is_some_and(|value| value == "unknown");
        if is_unknown {
            indices.insert(row.row_index);
        }
    }
    for item in conflicts {
        if let Some(value) = item.get("row_index").and_then(|v| v.as_i64()) {
            indices.insert(value);
        }
    }
    indices.len() as i64
}

fn compute_reconciliation(summary: &Value, rows: &[ExtractedRow], tolerance_cents: i64) -> Value {
    let opening_balance_cents = summary.get("opening_balance_cents").and_then(|v| v.as_i64());
    let closing_balance_cents = summary.get("closing_balance_cents").and_then(|v| v.as_i64());
    let total_debits_cents = summary.get("total_debits_cents").and_then(|v| v.as_i64());
    let total_credits_cents = summary.get("total_credits_cents").and_then(|v| v.as_i64());

    if opening_balance_cents.is_none()
        || closing_balance_cents.is_none()
        || total_debits_cents.is_none()
        || total_credits_cents.is_none()
    {
        return serde_json::json!({
            "skipped": true,
            "reason": "missing statement_summary fields required for reconciliation",
            "checks": [],
            "fail_count": 0,
            "total_checks": 0,
            "failed": false
        });
    }

    let opening = opening_balance_cents.unwrap_or(0);
    let closing = closing_balance_cents.unwrap_or(0);
    let expected_debits = total_debits_cents.unwrap_or(0);
    let expected_credits = total_credits_cents.unwrap_or(0);

    let net_movement: i64 = rows.iter().map(|row| row.amount_cents).sum();
    let actual_debits: i64 = rows
        .iter()
        .filter(|row| {
            row.metadata
                .as_ref()
                .and_then(|v| v.get("direction"))
                .and_then(|v| v.as_str())
                .is_some_and(|v| v == "debit")
        })
        .map(|row| row.amount_cents.abs())
        .sum();
    let actual_credits: i64 = rows
        .iter()
        .filter(|row| {
            row.metadata
                .as_ref()
                .and_then(|v| v.get("direction"))
                .and_then(|v| v.as_str())
                .is_some_and(|v| v == "credit")
        })
        .map(|row| row.amount_cents.abs())
        .sum();

    let actual_closing = opening + net_movement;
    let checks = vec![
        build_reconciliation_check(
            "balance_equation",
            closing,
            actual_closing,
            tolerance_cents,
        ),
        build_reconciliation_check("debits_total", expected_debits, actual_debits, tolerance_cents),
        build_reconciliation_check("credits_total", expected_credits, actual_credits, tolerance_cents),
    ];
    let fail_count = checks
        .iter()
        .filter(|item| !item.get("pass").and_then(|v| v.as_bool()).unwrap_or(false))
        .count() as i64;
    let total_checks = checks.len() as i64;
    serde_json::json!({
        "skipped": false,
        "checks": checks,
        "fail_count": fail_count,
        "total_checks": total_checks,
        "failed": fail_count > 0
    })
}

fn build_reconciliation_check(
    name: &str,
    expected: i64,
    actual: i64,
    tolerance_cents: i64,
) -> Value {
    let delta = actual - expected;
    serde_json::json!({
        "name": name,
        "expected": expected,
        "actual": actual,
        "delta_cents": delta,
        "tolerance_cents": tolerance_cents,
        "pass": delta.abs() <= tolerance_cents
    })
}

fn enrich_unresolved_directions(rows: Vec<ExtractedRow>) -> Vec<ExtractedRow> {
    let mut out = rows;
    for idx in 0..out.len() {
        let direction_source = out[idx]
            .metadata
            .as_ref()
            .and_then(|v| v.get("direction_source"))
            .and_then(|v| v.as_str())
            .unwrap_or("model");
        if direction_source == "manual" {
            continue;
        }
        let direction = out[idx]
            .metadata
            .as_ref()
            .and_then(|v| v.get("direction"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        if direction != "unknown" {
            continue;
        }

        if let Some((resolved, source, evidence)) = classify_unknown_direction(idx, &out) {
            let mut metadata = out[idx]
                .metadata
                .clone()
                .and_then(|v| v.as_object().cloned())
                .unwrap_or_default();
            metadata.insert("direction".to_string(), Value::String(resolved.to_string()));
            metadata.insert(
                "direction_source".to_string(),
                Value::String(source.to_string()),
            );
            if !metadata.contains_key("direction_confidence") {
                metadata.insert("direction_confidence".to_string(), Value::from(0.55_f64));
            }
            metadata.insert("fallback_applied".to_string(), Value::Bool(true));
            metadata.insert("fallback_evidence".to_string(), evidence);
            out[idx].metadata = Some(Value::Object(metadata));
        }
    }
    out
}

fn classify_unknown_direction(
    index: usize,
    rows: &[ExtractedRow],
) -> Option<(&'static str, &'static str, Value)> {
    if let Some(value) = classify_from_running_balance_delta(index, rows) {
        return Some(value);
    }
    if let Some(value) = classify_from_wording(index, rows) {
        return Some(value);
    }
    classify_from_metadata_cues(index, rows)
}

fn classify_from_running_balance_delta(
    index: usize,
    rows: &[ExtractedRow],
) -> Option<(&'static str, &'static str, Value)> {
    if index == 0 {
        return None;
    }
    let prev = rows
        .get(index.wrapping_sub(1))
        .and_then(|row| row.metadata.as_ref())
        .and_then(|v| v.get("running_balance_cents"))
        .and_then(|v| v.as_i64())?;
    let current = rows
        .get(index)
        .and_then(|row| row.metadata.as_ref())
        .and_then(|v| v.get("running_balance_cents"))
        .and_then(|v| v.as_i64())?;
    let delta = current - prev;
    if delta < 0 {
        return Some((
            "debit",
            "balance_delta",
            serde_json::json!({
                "rule": "running_balance_delta",
                "previous_running_balance_cents": prev,
                "current_running_balance_cents": current,
                "delta_cents": delta
            }),
        ));
    }
    if delta > 0 {
        return Some((
            "credit",
            "balance_delta",
            serde_json::json!({
                "rule": "running_balance_delta",
                "previous_running_balance_cents": prev,
                "current_running_balance_cents": current,
                "delta_cents": delta
            }),
        ));
    }
    None
}

fn classify_from_wording(
    index: usize,
    rows: &[ExtractedRow],
) -> Option<(&'static str, &'static str, Value)> {
    let description = rows.get(index)?.description.to_ascii_lowercase();
    let debit_keywords = [
        "withdrawal",
        "purchase",
        "payment",
        "debit",
        "atm",
        "fee",
        "bill",
        "sent",
        "transfer out",
    ];
    let credit_keywords = [
        "deposit",
        "salary",
        "payroll",
        "refund",
        "credit",
        "interest",
        "cashback",
        "received",
        "transfer in",
    ];
    let transfer_keywords = ["etransfer", "e-transfer", "transfer"];

    if let Some(keyword) = debit_keywords.iter().find(|item| description.contains(**item)) {
        return Some((
            "debit",
            "rule",
            serde_json::json!({ "rule": "description_keyword", "keyword": keyword }),
        ));
    }
    if let Some(keyword) = credit_keywords
        .iter()
        .find(|item| description.contains(**item))
    {
        return Some((
            "credit",
            "rule",
            serde_json::json!({ "rule": "description_keyword", "keyword": keyword }),
        ));
    }
    if let Some(keyword) = transfer_keywords
        .iter()
        .find(|item| description.contains(**item))
    {
        return Some((
            "transfer",
            "rule",
            serde_json::json!({ "rule": "description_keyword", "keyword": keyword }),
        ));
    }
    None
}

fn classify_from_metadata_cues(
    index: usize,
    rows: &[ExtractedRow],
) -> Option<(&'static str, &'static str, Value)> {
    let metadata = rows.get(index)?.metadata.as_ref()?;
    let transaction_type_raw = metadata
        .get("transaction_type_raw")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if transaction_type_raw.contains("withdraw") || transaction_type_raw.contains("debit") {
        return Some((
            "debit",
            "rule",
            serde_json::json!({ "rule": "transaction_type_raw", "value": transaction_type_raw }),
        ));
    }
    if transaction_type_raw.contains("deposit") || transaction_type_raw.contains("credit") {
        return Some((
            "credit",
            "rule",
            serde_json::json!({ "rule": "transaction_type_raw", "value": transaction_type_raw }),
        ));
    }
    if transaction_type_raw.contains("transfer") {
        return Some((
            "transfer",
            "rule",
            serde_json::json!({ "rule": "transaction_type_raw", "value": transaction_type_raw }),
        ));
    }
    None
}

fn map_statement_row(row_index: i64, account_id: &str, row: StatementRowCandidate) -> ExtractedRow {
    let mut parse_errors = Vec::new();
    let mut parse_flags: Vec<String> = Vec::new();

    let booked_at = match row.transaction_date {
        Some(v) if is_iso_date(v.as_str()) => v,
        Some(v) => {
            parse_errors.push(format!("invalid transaction_date format: {v}"));
            parse_flags.push("missing_required_field:transaction_date".to_string());
            parse_flags.push("parse_default_applied:transaction_date".to_string());
            "1970-01-01".to_string()
        }
        None => {
            parse_errors.push("missing transaction_date".to_string());
            parse_flags.push("missing_required_field:transaction_date".to_string());
            parse_flags.push("parse_default_applied:transaction_date".to_string());
            "1970-01-01".to_string()
        }
    };
    let description = row.details.unwrap_or_else(|| {
        parse_errors.push("missing details".to_string());
        parse_flags.push("missing_required_field:details".to_string());
        parse_flags.push("parse_default_applied:details".to_string());
        "Unknown transaction".to_string()
    });
    let amount_units = row.amount.unwrap_or_else(|| {
        parse_errors.push("missing amount".to_string());
        parse_flags.push("missing_required_field:amount".to_string());
        parse_flags.push("parse_default_applied:amount".to_string());
        0.0
    });
    let amount_cents = (amount_units * 100.0).round() as i64;

    let mut direction = TransactionDirection::from_value(row.tx_type.as_deref()).unwrap_or_else(|| {
        parse_errors.push("missing_or_invalid_type; review required".to_string());
        parse_flags.push("missing_required_field:type".to_string());
        TransactionDirection::Unknown
    });

    if direction == TransactionDirection::Debit && amount_cents >= 0 {
        parse_errors.push("type_sign_conflict: debit requires non-positive amount".to_string());
        parse_flags.push("sign_type_conflict".to_string());
    }
    if direction == TransactionDirection::Credit && amount_cents <= 0 {
        parse_errors.push("type_sign_conflict: credit requires non-negative amount".to_string());
        parse_flags.push("sign_type_conflict".to_string());
    }
    if direction == TransactionDirection::Unknown {
        parse_flags.push("parse_default_applied:type".to_string());
    }

    if !parse_flags.is_empty() {
        parse_flags.push("hash_fallback_defaults_applied".to_string());
    }

    let normalized_description = normalize_description(description.as_str());
    let normalized_txn_hash =
        compute_row_hash(account_id, booked_at.as_str(), amount_cents, normalized_description.as_str());
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "direction".to_string(),
        Value::String(direction.as_str().to_string()),
    );
    metadata.insert("type".to_string(), Value::String(direction.as_str().to_string()));
    metadata.insert(
        "amount_units".to_string(),
        Value::from(amount_units),
    );
    if !parse_flags.is_empty() {
        metadata.insert(
            "parse_flags".to_string(),
            Value::Array(parse_flags.into_iter().map(Value::String).collect()),
        );
    }
    metadata.insert(
        "direction_source".to_string(),
        Value::String("model".to_string()),
    );

    ExtractedRow {
        row_index,
        booked_at,
        amount_cents,
        description: normalized_description,
        confidence: row.confidence.unwrap_or(0.7),
        parse_error: if parse_errors.is_empty() {
            None
        } else {
            Some(parse_errors.join("; "))
        },
        normalized_txn_hash,
        metadata: Some(Value::Object(metadata)),
    }
}

fn find_string_field(root: &Value, paths: &[&str]) -> Option<String> {
    for path in paths {
        let mut current = root;
        let mut ok = true;
        for part in path.split('.') {
            if let Some(next) = current.get(part) {
                current = next;
            } else {
                ok = false;
                break;
            }
        }
        if ok {
            if let Some(value) = current.as_str() {
                if !value.trim().is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

pub fn versioned_agent_name(base_name: &str, schema_version: &str) -> String {
    format!("{}--{}", base_name.trim(), schema_version.trim())
}

pub async fn ensure_llama_extraction_agent(
    config: &ExtractionRuntimeConfig,
    schema: &Value,
) -> Result<LlamaAgentDescriptor, LlamaAgentBootstrapError> {
    let http = reqwest::Client::new();
    let base_url = env::var("LLAMA_CLOUD_BASE_URL")
        .unwrap_or_else(|_| "https://api.cloud.llamaindex.ai".to_string());
    let agent_name = versioned_agent_name(&config.llama_agent_name, &config.llama_schema_version);
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "bootstrap_ensure_start",
        "agent_name": agent_name,
        "schema_version": config.llama_schema_version,
    }));

    validate_schema_with_llama(&http, &base_url, config, schema).await?;

    if let Some(found_id) = find_llama_agent_id(&http, &base_url, config, &agent_name).await? {
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "bootstrap_ensure_success",
            "source": "existing_agent",
            "agent_name": agent_name,
            "agent_id": found_id,
            "schema_version": config.llama_schema_version,
        }));
        return Ok(LlamaAgentDescriptor {
            agent_id: found_id,
            agent_name,
        });
    }

    let created_id = create_llama_agent(&http, &base_url, config, &agent_name, schema).await?;
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "bootstrap_ensure_success",
        "source": "created_agent",
        "agent_name": agent_name,
        "agent_id": created_id,
        "schema_version": config.llama_schema_version,
    }));
    Ok(LlamaAgentDescriptor {
        agent_id: created_id,
        agent_name,
    })
}

pub async fn local_ocr_stub(_request: &ExtractionRequest) -> anyhow::Result<ExtractionResult> {
    Ok(ExtractionResult {
        rows: Vec::new(),
        warnings: Vec::new(),
        errors: vec!["LOCAL_OCR_NOT_IMPLEMENTED".to_string()],
        effective_provider: None,
        attempts: Vec::new(),
        diagnostics: serde_json::json!({
            "error_code": "LOCAL_OCR_NOT_IMPLEMENTED",
            "message": "Local OCR mode is planned but not implemented in this phase."
        }),
    })
}

fn scoped_url(
    base_url: &str,
    path: &str,
    config: &ExtractionRuntimeConfig,
) -> Result<reqwest::Url, LlamaAgentBootstrapError> {
    let mut url = reqwest::Url::parse(format!("{base_url}{path}").as_str()).map_err(|e| {
        LlamaAgentBootstrapError {
            code: "EXTRACTION_AGENT_BOOTSTRAP_BAD_URL".to_string(),
            message: e.to_string(),
            status_code: None,
        }
    })?;

    if config.llama_cloud_organization_id.is_some() || config.llama_cloud_project_id.is_some() {
        let mut qp = url.query_pairs_mut();
        if let Some(org) = config.llama_cloud_organization_id.as_deref() {
            if uuid::Uuid::parse_str(org).is_ok() {
                qp.append_pair("organization_id", org);
            } else {
                log_bootstrap_event(serde_json::json!({
                    "ts_utc": chrono::Utc::now().to_rfc3339(),
                    "kind": "bootstrap_scope_id_ignored",
                    "field": "organization_id",
                    "reason": "invalid_uuid",
                    "value": org,
                }));
            }
        }
        if let Some(project) = config.llama_cloud_project_id.as_deref() {
            if uuid::Uuid::parse_str(project).is_ok() {
                qp.append_pair("project_id", project);
            } else {
                log_bootstrap_event(serde_json::json!({
                    "ts_utc": chrono::Utc::now().to_rfc3339(),
                    "kind": "bootstrap_scope_id_ignored",
                    "field": "project_id",
                    "reason": "invalid_uuid",
                    "value": project,
                }));
            }
        }
    }
    Ok(url)
}

async fn validate_schema_with_llama(
    http: &reqwest::Client,
    base_url: &str,
    config: &ExtractionRuntimeConfig,
    schema: &Value,
) -> Result<(), LlamaAgentBootstrapError> {
    let url = scoped_url(
        base_url,
        "/api/v1/extraction/extraction-agents/schema/validation",
        config,
    )?;
    let url_for_log = url.to_string();
    let payload = serde_json::json!({
        "data_schema": schema,
        "schema": schema
    });
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "bootstrap_schema_validation_request",
        "url": url.as_str(),
    }));

    let response = http
        .post(url)
        .bearer_auth(&config.llama_cloud_api_key)
        .json(&payload)
        .send()
        .await
        .map_err(map_bootstrap_network_error)?;

    let status_code = response.status().as_u16();
    let response_headers = redact_headers(response.headers());
    let body = response.text().await.unwrap_or_default();
    log_external_api_raw_event(&ExternalApiRawEvent {
        ts_utc: chrono::Utc::now().to_rfc3339(),
        kind: "external_api_raw",
        import_id: None,
        file_name: None,
        provider: "llamaextract_bootstrap".to_string(),
        attempt_no: None,
        operation: "bootstrap_validate_schema".to_string(),
        method: "POST".to_string(),
        url: url_for_log,
        request_body_meta: Some(serde_json::json!({
            "payload_keys": ["data_schema", "schema"],
        })),
        status_code: Some(status_code),
        response_headers_redacted: response_headers,
        response_body_raw: Some(body.clone()),
        error_message: None,
    });

    if (200..300).contains(&status_code) {
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "bootstrap_schema_validation_success",
            "status_code": status_code,
        }));
        return Ok(());
    }

    let code = if status_code == 400 || status_code == 422 {
        "EXTRACTION_SCHEMA_INVALID"
    } else {
        "EXTRACTION_AGENT_BOOTSTRAP_API_UNREACHABLE"
    };
    Err(LlamaAgentBootstrapError {
        code: code.to_string(),
        message: format!("schema validation failed ({status_code}): {body}"),
        status_code: Some(status_code),
    })
}

async fn find_llama_agent_id(
    http: &reqwest::Client,
    base_url: &str,
    config: &ExtractionRuntimeConfig,
    desired_name: &str,
) -> Result<Option<String>, LlamaAgentBootstrapError> {
    let url = scoped_url(base_url, "/api/v1/extraction/extraction-agents", config)?;
    let url_for_log = url.to_string();
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "bootstrap_agent_list_request",
        "url": url.as_str(),
        "desired_name": desired_name,
    }));
    let response = http
        .get(url)
        .bearer_auth(&config.llama_cloud_api_key)
        .send()
        .await
        .map_err(map_bootstrap_network_error)?;

    let status_code = response.status().as_u16();
    let response_headers = redact_headers(response.headers());
    let raw_body = response.text().await.unwrap_or_default();
    log_external_api_raw_event(&ExternalApiRawEvent {
        ts_utc: chrono::Utc::now().to_rfc3339(),
        kind: "external_api_raw",
        import_id: None,
        file_name: None,
        provider: "llamaextract_bootstrap".to_string(),
        attempt_no: None,
        operation: "bootstrap_find_agent".to_string(),
        method: "GET".to_string(),
        url: url_for_log,
        request_body_meta: None,
        status_code: Some(status_code),
        response_headers_redacted: response_headers,
        response_body_raw: Some(raw_body.clone()),
        error_message: None,
    });

    if !(200..300).contains(&status_code) {
        return Err(LlamaAgentBootstrapError {
            code: "EXTRACTION_AGENT_BOOTSTRAP_API_UNREACHABLE".to_string(),
            message: format!("agent list failed ({status_code}): {raw_body}"),
            status_code: Some(status_code),
        });
    }

    let body: Value = serde_json::from_str(raw_body.as_str())
        .map_err(|e| LlamaAgentBootstrapError {
            code: "EXTRACTION_AGENT_BOOTSTRAP_API_UNREACHABLE".to_string(),
            message: format!("agent list returned invalid json: {e}"),
            status_code: None,
        })?;

    let found = extract_agent_id_by_name(&body, desired_name);
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "bootstrap_agent_list_success",
        "found": found.is_some(),
        "desired_name": desired_name,
    }));
    Ok(found)
}

async fn create_llama_agent(
    http: &reqwest::Client,
    base_url: &str,
    config: &ExtractionRuntimeConfig,
    agent_name: &str,
    schema: &Value,
) -> Result<String, LlamaAgentBootstrapError> {
    let url = scoped_url(base_url, "/api/v1/extraction/extraction-agents", config)?;
    let url_for_log = url.to_string();
    let payload = serde_json::json!({
        "name": agent_name,
        "data_schema": schema,
        "config": {
            "extraction_mode": "BALANCED"
        }
    });
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "bootstrap_agent_create_request",
        "url": url.as_str(),
        "agent_name": agent_name,
    }));

    let response = http
        .post(url)
        .bearer_auth(&config.llama_cloud_api_key)
        .json(&payload)
        .send()
        .await
        .map_err(map_bootstrap_network_error)?;

    let status_code = response.status().as_u16();
    let response_headers = redact_headers(response.headers());
    let raw_body = response.text().await.unwrap_or_default();
    log_external_api_raw_event(&ExternalApiRawEvent {
        ts_utc: chrono::Utc::now().to_rfc3339(),
        kind: "external_api_raw",
        import_id: None,
        file_name: None,
        provider: "llamaextract_bootstrap".to_string(),
        attempt_no: None,
        operation: "bootstrap_create_agent".to_string(),
        method: "POST".to_string(),
        url: url_for_log,
        request_body_meta: Some(serde_json::json!({
            "agent_name": agent_name,
            "payload_keys": ["name", "data_schema", "config"],
        })),
        status_code: Some(status_code),
        response_headers_redacted: response_headers,
        response_body_raw: Some(raw_body.clone()),
        error_message: None,
    });

    if !(200..300).contains(&status_code) {
        return Err(LlamaAgentBootstrapError {
            code: "EXTRACTION_AGENT_BOOTSTRAP_API_UNREACHABLE".to_string(),
            message: format!("agent create failed ({status_code}): {raw_body}"),
            status_code: Some(status_code),
        });
    }

    let body: Value = serde_json::from_str(raw_body.as_str())
        .map_err(|e| LlamaAgentBootstrapError {
            code: "EXTRACTION_AGENT_BOOTSTRAP_API_UNREACHABLE".to_string(),
            message: format!("agent create returned invalid json: {e}"),
            status_code: None,
        })?;

    let agent_id = body
        .get("id")
        .and_then(|v| v.as_str())
        .or_else(|| body.get("data").and_then(|v| v.get("id")).and_then(|v| v.as_str()))
        .ok_or_else(|| LlamaAgentBootstrapError {
            code: "EXTRACTION_AGENT_BOOTSTRAP_API_UNREACHABLE".to_string(),
            message: "agent create response missing id".to_string(),
            status_code: None,
        })?;
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "bootstrap_agent_create_success",
        "agent_name": agent_name,
        "agent_id": agent_id,
    }));
    Ok(agent_id.to_string())
}

fn extract_agent_id_by_name(body: &Value, desired_name: &str) -> Option<String> {
    let list = if let Some(arr) = body.as_array() {
        Some(arr)
    } else {
        body.get("data").and_then(|v| v.as_array())
    }?;

    for item in list {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .or_else(|| item.get("extraction_agent_name").and_then(|v| v.as_str()));
        if name == Some(desired_name) {
            if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                return Some(id.to_string());
            }
        }
    }
    None
}

fn map_bootstrap_network_error(err: reqwest::Error) -> LlamaAgentBootstrapError {
    let mapped = LlamaAgentBootstrapError {
        code: "EXTRACTION_AGENT_BOOTSTRAP_API_UNREACHABLE".to_string(),
        message: err.to_string(),
        status_code: None,
    };
    log_bootstrap_event(serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "bootstrap_network_error",
        "error_code": mapped.code.clone(),
        "error_message": mapped.message.clone(),
    }));
    mapped
}

fn log_bootstrap_event(payload: Value) {
    let log_path = bootstrap_log_path();
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(log_path) else {
        return;
    };
    let _ = writeln!(file, "{payload}");
}

fn bootstrap_log_path() -> PathBuf {
    if let Ok(explicit) = env::var("EXPENSE_BOOTSTRAP_LOG_PATH") {
        return PathBuf::from(explicit);
    }
    expense_core::default_app_data_dir()
        .join("logs")
        .join("extraction-bootstrap.log")
}

#[derive(Debug)]
struct ProviderError {
    code: String,
    message: String,
    status_code: Option<u16>,
    raw_response: Option<String>,
    llama_state: Option<String>,
    llama_poll_count: i64,
}

#[derive(Debug, Clone)]
struct HttpCallMeta<'a> {
    import_id: &'a str,
    file_name: &'a str,
    provider: &'a str,
    attempt_no: i64,
    operation: String,
    method: &'static str,
    url: String,
    timeout_ms: u64,
    request_body_meta: Value,
}

#[derive(Debug, Clone, Serialize)]
struct HttpCallEvent {
    ts_utc: String,
    kind: &'static str,
    import_id: String,
    file_name: String,
    provider: String,
    attempt_no: i64,
    operation: String,
    method: String,
    url: String,
    timeout_ms: u64,
    request_headers_redacted: Value,
    request_body_meta: Value,
    status_code: Option<u16>,
    response_headers_redacted: Value,
    raw_response: Option<String>,
    truncated: bool,
    latency_ms: i64,
    error_code: Option<String>,
    error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ExternalApiRawEvent {
    ts_utc: String,
    kind: &'static str,
    import_id: Option<String>,
    file_name: Option<String>,
    provider: String,
    attempt_no: Option<i64>,
    operation: String,
    method: String,
    url: String,
    request_body_meta: Option<Value>,
    status_code: Option<u16>,
    response_headers_redacted: Value,
    response_body_raw: Option<String>,
    error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DecisionEvent {
    ts_utc: String,
    kind: &'static str,
    import_id: String,
    file_name: String,
    provider: String,
    attempt_no: i64,
    decision: String,
    reason: String,
}

#[derive(Debug, Clone)]
struct HttpCallSuccess {
    status: u16,
    raw: String,
}

#[derive(Debug, Clone)]
struct ProviderCallSuccess {
    status: u16,
    raw: String,
    rows: Vec<ProviderRow>,
    diagnostics: Value,
    llama_poll_count: i64,
    llama_last_state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProviderRow {
    booked_at: String,
    description: String,
    amount_cents: Option<i64>,
    amount: Option<String>,
    confidence: Option<f64>,
    direction: Option<String>,
    direction_confidence: Option<f64>,
    running_balance_cents: Option<i64>,
    transaction_type_raw: Option<String>,
    counterparty: Option<String>,
    reference_id: Option<String>,
    meta: Option<Value>,
}

async fn llama_call(
    http: &reqwest::Client,
    request: &ExtractionRequest,
    attempt_no: i64,
    provider_name: &str,
    budget: &LlamaPhaseBudget,
) -> Result<ProviderCallSuccess, ProviderError> {
    let Some(api_key) = env_var("LLAMAPARSE_API_KEY") else {
        return Err(ProviderError {
            code: "MANAGED_PROVIDER_BAD_REQUEST".to_string(),
            message: "LLAMAPARSE_API_KEY not configured".to_string(),
            status_code: None,
            raw_response: None,
            llama_state: None,
            llama_poll_count: 0,
        });
    };

    let endpoint = env::var("LLAMAPARSE_ENDPOINT")
        .unwrap_or_else(|_| "https://api.cloud.llamaindex.ai/api/v1/parsing/upload".to_string());
    let timeout_ms = budget
        .call_timeout_ms(request.timeout_ms)
        .ok_or_else(|| ProviderError {
            code: "MANAGED_PROVIDER_TIMEOUT".to_string(),
            message: "llama phase budget exhausted before upload".to_string(),
            status_code: None,
            raw_response: None,
            llama_state: Some("timed_out".to_string()),
            llama_poll_count: 0,
        })?;
    let file_part = reqwest::multipart::Part::bytes(request.bytes.clone())
        .file_name(request.file_name.clone())
        .mime_str("application/pdf")
        .map_err(|e| ProviderError {
            code: "MANAGED_PROVIDER_BAD_REQUEST".to_string(),
            message: format!("invalid file mime: {e}"),
            status_code: None,
            raw_response: None,
            llama_state: None,
            llama_poll_count: 0,
        })?;
    let form = reqwest::multipart::Form::new().part("file", file_part);

    let meta = HttpCallMeta {
        import_id: &request.import_id,
        file_name: &request.file_name,
        provider: provider_name,
        attempt_no,
        operation: "upload".to_string(),
        method: "POST",
        url: endpoint.clone(),
        timeout_ms,
        request_body_meta: serde_json::json!({
            "type": "multipart",
            "parser_type": "pdf",
            "file_name": request.file_name,
            "file_size_bytes": request.bytes.len(),
        }),
    };
    let upload = execute_http_call(http, meta, |client| {
        client
            .post(endpoint)
            .header("Authorization", format!("Bearer {api_key}"))
            .multipart(form)
            .timeout(Duration::from_millis(timeout_ms))
    })
    .await?;
    let status = StatusCode::from_u16(upload.status).unwrap_or(StatusCode::OK);
    let raw = upload.raw;

    let value: Value = serde_json::from_str(&raw).map_err(|e| ProviderError {
        code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
        message: format!("llamaparse response is not valid json: {e}"),
        status_code: Some(status.as_u16()),
        raw_response: Some(raw.clone()),
        llama_state: Some("failed".to_string()),
        llama_poll_count: 0,
    })?;

    let (rows, llama_poll_count) = match extract_rows_with_pages_fallback(&value) {
        Ok(rows) if !rows.is_empty() => (rows, 0_i64),
        _ => {
            let job_id = value
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ProviderError {
                    code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
                    message: "llamaparse response missing job id or rows".to_string(),
                    status_code: Some(status.as_u16()),
                    raw_response: Some(raw.clone()),
                    llama_state: Some("failed".to_string()),
                    llama_poll_count: 0,
                })?;

            let (polled, poll_count) =
                llama_poll_result(http, request, api_key.as_str(), job_id, attempt_no, budget)
                    .await?;
            (
                extract_rows_with_pages_fallback(&polled).map_err(|e| ProviderError {
                    code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
                    message: format!("llamaparse result does not contain parseable rows: {e}"),
                    status_code: Some(status.as_u16()),
                    raw_response: Some(raw.clone()),
                    llama_state: Some("failed".to_string()),
                    llama_poll_count: poll_count,
                })?,
                poll_count,
            )
        }
    };

    Ok(ProviderCallSuccess {
        status: status.as_u16(),
        raw,
        rows,
        diagnostics: serde_json::json!({ "provider": "llamaparse" }),
        llama_poll_count,
        llama_last_state: Some("success".to_string()),
    })
}

async fn llama_poll_result(
    http: &reqwest::Client,
    request: &ExtractionRequest,
    api_key: &str,
    job_id: &str,
    attempt_no: i64,
    budget: &LlamaPhaseBudget,
) -> Result<(Value, i64), ProviderError> {
    let base = env::var("LLAMAPARSE_BASE_URL")
        .unwrap_or_else(|_| "https://api.cloud.llamaindex.ai".to_string());
    let result_url = format!("{base}/api/v1/parsing/job/{job_id}/result/json");

    let max_polls = env::var("LLAMAPARSE_POLL_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(12)
        .clamp(3, 20);

    let mut total_polls = 0_i64;
    for poll_no in 1..=max_polls {
        total_polls += 1;
        let Some(timeout_ms) = budget.call_timeout_ms(request.timeout_ms) else {
            return Err(ProviderError {
                code: "MANAGED_PROVIDER_TIMEOUT".to_string(),
                message: "llamaparse phase budget exhausted during polling".to_string(),
                status_code: None,
                raw_response: None,
                llama_state: Some("timed_out".to_string()),
                llama_poll_count: total_polls,
            });
        };
        let meta = HttpCallMeta {
            import_id: &request.import_id,
            file_name: &request.file_name,
            provider: "llamaparse",
            attempt_no,
            operation: format!("poll_result_{poll_no}"),
            method: "GET",
            url: result_url.clone(),
            timeout_ms,
            request_body_meta: serde_json::json!({
                "job_id": job_id,
                "poll_no": poll_no,
            }),
        };
        let poll = execute_http_call(http, meta, |client| {
            client
                .get(&result_url)
                .header("Authorization", format!("Bearer {api_key}"))
                .header("accept", "application/json")
                .timeout(Duration::from_millis(timeout_ms))
        })
        .await;

        let (status, raw) = match poll {
            Ok(ok) => (
                StatusCode::from_u16(ok.status).unwrap_or(StatusCode::OK),
                ok.raw,
            ),
            Err(err) => {
                if err.status_code == Some(StatusCode::NOT_FOUND.as_u16())
                    || err.status_code == Some(StatusCode::UNPROCESSABLE_ENTITY.as_u16())
                {
                    if poll_no < max_polls {
                        let delay = adaptive_poll_delay(poll_no as i64);
                        if budget.remaining_ms() <= delay.as_millis() as u64 {
                            return Err(ProviderError {
                                code: "MANAGED_PROVIDER_TIMEOUT".to_string(),
                                message:
                                    "llamaparse phase budget exhausted while waiting for result"
                                        .to_string(),
                                status_code: err.status_code,
                                raw_response: err.raw_response,
                                llama_state: Some("timed_out".to_string()),
                                llama_poll_count: total_polls,
                            });
                        }
                        sleep(delay).await;
                        continue;
                    }
                    return Err(ProviderError {
                        code: "MANAGED_PROVIDER_TIMEOUT".to_string(),
                        message: "llamaparse result not ready within polling window".to_string(),
                        status_code: err.status_code,
                        raw_response: err.raw_response,
                        llama_state: Some("timed_out".to_string()),
                        llama_poll_count: total_polls,
                    });
                }
                return Err(err);
            }
        };

        if status.is_success() {
            let value: Value = serde_json::from_str(&raw).map_err(|e| ProviderError {
                code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
                message: format!("llamaparse polled result is not valid json: {e}"),
                status_code: Some(status.as_u16()),
                raw_response: Some(raw.clone()),
                llama_state: Some("failed".to_string()),
                llama_poll_count: total_polls,
            })?;
            return Ok((value, total_polls));
        }

        // execute_http_call already classifies non-success status codes into ProviderError.
        return Err(status_error(status, raw));
    }

    Err(ProviderError {
        code: "MANAGED_PROVIDER_TIMEOUT".to_string(),
        message: "llamaparse polling exhausted without result".to_string(),
        status_code: None,
        raw_response: None,
        llama_state: Some("timed_out".to_string()),
        llama_poll_count: total_polls,
    })
}

async fn openrouter_call(
    http: &reqwest::Client,
    request: &ExtractionRequest,
    attempt_no: i64,
    provider_name: &str,
) -> Result<ProviderCallSuccess, ProviderError> {
    let Some(api_key) = env_var("OPENROUTER_API_KEY") else {
        return Err(ProviderError {
            code: "MANAGED_PROVIDER_BAD_REQUEST".to_string(),
            message: "OPENROUTER_API_KEY not configured".to_string(),
            status_code: None,
            raw_response: None,
            llama_state: None,
            llama_poll_count: 0,
        });
    };

    let endpoint = env::var("OPENROUTER_ENDPOINT")
        .unwrap_or_else(|_| "https://openrouter.ai/api/v1/chat/completions".to_string());
    let model = env::var("OPENROUTER_MODEL").unwrap_or_else(|_| "openai/gpt-4o-mini".to_string());
    let timeout_ms = request
        .timeout_ms
        .clamp(MIN_PROVIDER_TIMEOUT_MS, MAX_PROVIDER_TIMEOUT_MS) as u64;
    let schema_version = std::env::var("LLAMA_SCHEMA_VERSION")
        .unwrap_or_else(|_| "statement_v1".to_string());
    let prompt = openrouter_prompt(&schema_version);
    let pdf_data_url = format!(
        "data:application/pdf;base64,{}",
        STANDARD.encode(&request.bytes)
    );

    let payload = serde_json::json!({
        "model": model,
        "messages": [{
            "role":"user",
            "content": [
                {"type":"text","text": prompt},
                {"type":"file","file":{"filename": request.file_name, "file_data": pdf_data_url}}
            ]
        }],
        "plugins":[{"id":"file-parser","pdf":{"engine":"pdf-text"}}],
        "response_format": openrouter_response_format_schema(&schema_version)
    });

    let meta = HttpCallMeta {
        import_id: &request.import_id,
        file_name: &request.file_name,
        provider: provider_name,
        attempt_no,
        operation: "chat_completions".to_string(),
        method: "POST",
        url: endpoint.clone(),
        timeout_ms,
        request_body_meta: serde_json::json!({
            "model": model,
            "plugins": ["file-parser:pdf-text"],
            "file_name": request.file_name,
            "file_size_bytes": request.bytes.len(),
        }),
    };
    let result = execute_http_call(http, meta, |client| {
        client
            .post(endpoint)
            .header("Authorization", format!("Bearer {api_key}"))
            .header("Content-Type", "application/json")
            .json(&payload)
            .timeout(Duration::from_millis(timeout_ms))
    })
    .await?;
    let status = StatusCode::from_u16(result.status).unwrap_or(StatusCode::OK);
    let raw = result.raw;

    if raw.trim().is_empty() {
        return Err(ProviderError {
            code: "MANAGED_PROVIDER_TIMEOUT".to_string(),
            message: "openrouter returned empty response body".to_string(),
            status_code: Some(status.as_u16()),
            raw_response: Some(raw),
            llama_state: None,
            llama_poll_count: 0,
        });
    }

    let value: Value = serde_json::from_str(&raw).map_err(|e| ProviderError {
        code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
        message: format!("openrouter response is not valid json: {e}"),
        status_code: Some(status.as_u16()),
        raw_response: Some(raw.clone()),
        llama_state: None,
        llama_poll_count: 0,
    })?;

    // OpenRouter nested content can be JSON string in choices[0].message.content.
    let maybe_content = value
        .get("choices")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.get("message"))
        .and_then(|v| v.get("content"));
    let rows = if let Some(content) = maybe_content {
        if let Some(text) = content.as_str() {
            let parsed: Value = serde_json::from_str(text).map_err(|e| ProviderError {
                code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
                message: format!("openrouter content not json schema payload: {e}"),
                status_code: Some(status.as_u16()),
                raw_response: Some(raw.clone()),
                llama_state: None,
                llama_poll_count: 0,
            })?;
            extract_rows(&parsed).map_err(|e| ProviderError {
                code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
                message: e.to_string(),
                status_code: Some(status.as_u16()),
                raw_response: Some(raw.clone()),
                llama_state: None,
                llama_poll_count: 0,
            })?
        } else if let Some(parts) = content.as_array() {
            let merged = parts
                .iter()
                .filter_map(|part| part.get("text").and_then(|v| v.as_str()))
                .collect::<Vec<_>>()
                .join("");
            let parsed: Value = serde_json::from_str(&merged).map_err(|e| ProviderError {
                code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
                message: format!("openrouter array content not json schema payload: {e}"),
                status_code: Some(status.as_u16()),
                raw_response: Some(raw.clone()),
                llama_state: None,
                llama_poll_count: 0,
            })?;
            extract_rows(&parsed).map_err(|e| ProviderError {
                code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
                message: e.to_string(),
                status_code: Some(status.as_u16()),
                raw_response: Some(raw.clone()),
                llama_state: None,
                llama_poll_count: 0,
            })?
        } else {
            extract_rows(&value).map_err(|e| ProviderError {
                code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
                message: e.to_string(),
                status_code: Some(status.as_u16()),
                raw_response: Some(raw.clone()),
                llama_state: None,
                llama_poll_count: 0,
            })?
        }
    } else {
        extract_rows(&value).map_err(|e| ProviderError {
            code: "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
            message: e.to_string(),
            status_code: Some(status.as_u16()),
            raw_response: Some(raw.clone()),
            llama_state: None,
            llama_poll_count: 0,
        })?
    };

    Ok(ProviderCallSuccess {
        status: status.as_u16(),
        raw,
        rows,
        diagnostics: serde_json::json!({ "provider": "openrouter_pdf_text", "model": model }),
        llama_poll_count: 0,
        llama_last_state: None,
    })
}

fn extract_rows(value: &Value) -> anyhow::Result<Vec<ProviderRow>> {
    if let Some(rows) = value.get("rows").and_then(|v| v.as_array()) {
        let parsed: Vec<ProviderRow> = rows
            .iter()
            .map(parse_provider_row)
            .collect::<anyhow::Result<_>>()?;
        return Ok(parsed);
    }
    if let Some(rows) = value.get("transactions").and_then(|v| v.as_array()) {
        let parsed: Vec<ProviderRow> = rows
            .iter()
            .map(parse_provider_row)
            .collect::<anyhow::Result<_>>()?;
        return Ok(parsed);
    }
    if let Some(rows) = value
        .get("output")
        .and_then(|v| v.get("rows"))
        .and_then(|v| v.as_array())
    {
        let parsed: Vec<ProviderRow> = rows
            .iter()
            .map(parse_provider_row)
            .collect::<anyhow::Result<_>>()?;
        return Ok(parsed);
    }
    if let Some(rows) = value
        .get("output")
        .and_then(|v| v.get("transactions"))
        .and_then(|v| v.as_array())
    {
        let parsed: Vec<ProviderRow> = rows
            .iter()
            .map(parse_provider_row)
            .collect::<anyhow::Result<_>>()?;
        return Ok(parsed);
    }
    Err(anyhow!("provider payload missing rows[]"))
}

fn parse_provider_row(item: &Value) -> anyhow::Result<ProviderRow> {
    if item.get("transaction_date").is_some() || item.get("details").is_some() {
        let booked_at = item
            .get("transaction_date")
            .and_then(|v| v.as_str())
            .unwrap_or("1970-01-01")
            .to_string();
        let description = item
            .get("details")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown transaction")
            .to_string();
        let amount = item
            .get("amount")
            .and_then(as_f64_from_value)
            .unwrap_or(0.0);
        let amount_cents = (amount * 100.0).round() as i64;
        return Ok(ProviderRow {
            booked_at,
            description,
            amount_cents: Some(amount_cents),
            amount: None,
            confidence: item.get("confidence").and_then(|v| v.as_f64()),
            direction: item
                .get("type")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            direction_confidence: item.get("direction_confidence").and_then(|v| v.as_f64()),
            running_balance_cents: item.get("running_balance_cents").and_then(|v| v.as_i64()),
            transaction_type_raw: item
                .get("transaction_type_raw")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            counterparty: item
                .get("counterparty")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            reference_id: item
                .get("reference_id")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            meta: item.get("meta").cloned(),
        });
    }

    let parsed: ProviderRow = serde_json::from_value(item.clone())?;
    Ok(parsed)
}

fn extract_rows_with_pages_fallback(value: &Value) -> anyhow::Result<Vec<ProviderRow>> {
    if let Ok(rows) = extract_rows(value) {
        if !rows.is_empty() {
            return Ok(rows);
        }
    }

    let Some(pages) = value.get("pages").and_then(|v| v.as_array()) else {
        return Err(anyhow!("provider payload missing rows[] and pages[]"));
    };

    let mut rows = Vec::new();
    for page in pages {
        let text = page.get("text").and_then(|v| v.as_str()).unwrap_or("");
        let md = page.get("md").and_then(|v| v.as_str()).unwrap_or("");
        let year_hint = infer_statement_year(md).or_else(|| infer_statement_year(text));
        rows.extend(parse_markdown_table_rows(md, year_hint));
        rows.extend(parse_candidate_rows_from_text(md, year_hint));
        rows.extend(parse_candidate_rows_from_text(text, year_hint));
    }

    if rows.is_empty() {
        return Err(anyhow!("no parseable transaction rows found in pages[]"));
    }
    Ok(rows)
}

fn parse_candidate_rows_from_text(text: &str, year_hint: Option<i32>) -> Vec<ProviderRow> {
    let mut rows = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }

        let Some(booked_at) = normalize_date_token(parts[0], year_hint) else {
            continue;
        };

        let amount_raw = parts[parts.len() - 1];
        let amount_cents = match parse_amount_cents(amount_raw) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let description = parts[1..parts.len() - 1].join(" ");
        if description.is_empty() {
            continue;
        }

        rows.push(ProviderRow {
            booked_at,
            description,
            amount_cents: Some(amount_cents),
            amount: None,
            confidence: Some(0.7),
            direction: None,
            direction_confidence: None,
            running_balance_cents: None,
            transaction_type_raw: None,
            counterparty: None,
            reference_id: None,
            meta: None,
        });
    }
    rows
}

fn parse_markdown_table_rows(md: &str, year_hint: Option<i32>) -> Vec<ProviderRow> {
    let mut rows = Vec::new();
    let mut last_idx: Option<usize> = None;

    for raw_line in md.lines() {
        let line = raw_line.trim();
        if !line.contains('|') {
            continue;
        }
        if line.contains("---") || line.starts_with("| Date") || line.starts_with("| ------") {
            continue;
        }
        let mut columns = line.split('|').map(str::trim).collect::<Vec<_>>();
        if columns.first().is_some_and(|v| v.is_empty()) {
            columns.remove(0);
        }
        if columns.last().is_some_and(|v| v.is_empty()) {
            columns.pop();
        }
        if columns.len() < 3 {
            continue;
        }

        let date_col = columns.first().copied().unwrap_or_default();
        let desc_col = columns.get(1).copied().unwrap_or_default();
        let withdrawn_col = columns.get(2).copied().unwrap_or_default();
        let deposited_col = columns.get(3).copied().unwrap_or_default();

        let is_noise = desc_col.eq_ignore_ascii_case("opening balance")
            || desc_col.eq_ignore_ascii_case("closing balance")
            || desc_col.eq_ignore_ascii_case("date")
            || desc_col.eq_ignore_ascii_case("transactions");
        if is_noise {
            continue;
        }

        let booked_at = normalize_date_token(date_col, year_hint);
        let withdrawn = parse_amount_cents(withdrawn_col).ok();
        let deposited = parse_amount_cents(deposited_col).ok();

        match (booked_at, withdrawn, deposited) {
            (Some(date), Some(w), _) => {
                let amount = -w.abs();
                rows.push(ProviderRow {
                    booked_at: date,
                    description: desc_col.to_string(),
                    amount_cents: Some(amount),
                    amount: None,
                    confidence: Some(0.85),
                    direction: Some("debit".to_string()),
                    direction_confidence: Some(0.85),
                    running_balance_cents: None,
                    transaction_type_raw: None,
                    counterparty: None,
                    reference_id: None,
                    meta: None,
                });
                last_idx = Some(rows.len() - 1);
            }
            (Some(date), _, Some(d)) => {
                let amount = d.abs();
                rows.push(ProviderRow {
                    booked_at: date,
                    description: desc_col.to_string(),
                    amount_cents: Some(amount),
                    amount: None,
                    confidence: Some(0.85),
                    direction: Some("credit".to_string()),
                    direction_confidence: Some(0.85),
                    running_balance_cents: None,
                    transaction_type_raw: None,
                    counterparty: None,
                    reference_id: None,
                    meta: None,
                });
                last_idx = Some(rows.len() - 1);
            }
            (None, None, None) if !desc_col.is_empty() => {
                if let Some(idx) = last_idx {
                    let existing = rows[idx].description.clone();
                    rows[idx].description = format!("{existing} {}", desc_col).trim().to_string();
                }
            }
            _ => {}
        }
    }

    rows
}

fn normalize_date_token(token: &str, year_hint: Option<i32>) -> Option<String> {
    let token = token.trim_matches(|c: char| c == ':' || c == ',');
    if is_iso_date(token) {
        return Some(token.to_string());
    }

    let chunks: Vec<&str> = token.split('/').collect();
    if chunks.len() == 3
        && chunks
            .iter()
            .all(|p| p.chars().all(|ch| ch.is_ascii_digit()))
        && chunks[0].len() == 2
        && chunks[1].len() == 2
        && chunks[2].len() == 4
    {
        return Some(format!("{}-{}-{}", chunks[2], chunks[0], chunks[1]));
    }

    let month_day = token.split_whitespace().collect::<Vec<_>>();
    if month_day.len() == 2 {
        let month = month_to_number(month_day[0])?;
        let day = month_day[1]
            .trim_matches(|c: char| c == ',' || c == '.')
            .parse::<u32>()
            .ok()?;
        let year = year_hint?;
        return Some(format!("{year:04}-{month:02}-{day:02}"));
    }
    None
}

fn infer_statement_year(text: &str) -> Option<i32> {
    let mut years = Vec::new();
    for token in text.split_whitespace() {
        let cleaned = token.trim_matches(|c: char| !c.is_ascii_digit());
        if cleaned.len() == 4 && cleaned.starts_with("20") {
            if let Ok(y) = cleaned.parse::<i32>() {
                if (2000..=2100).contains(&y) {
                    years.push(y);
                }
            }
        }
    }
    years.into_iter().max()
}

fn month_to_number(value: &str) -> Option<u32> {
    match value.to_ascii_lowercase().as_str() {
        "jan" | "january" => Some(1),
        "feb" | "february" => Some(2),
        "mar" | "march" => Some(3),
        "apr" | "april" => Some(4),
        "may" => Some(5),
        "jun" | "june" => Some(6),
        "jul" | "july" => Some(7),
        "aug" | "august" => Some(8),
        "sep" | "sept" | "september" => Some(9),
        "oct" | "october" => Some(10),
        "nov" | "november" => Some(11),
        "dec" | "december" => Some(12),
        _ => None,
    }
}

fn openrouter_prompt(schema_version: &str) -> &'static str {
    if schema_version == "statement_v2" {
        "Extract a bank statement as strict JSON using this exact shape: statement_period {start_date,end_date}, statement_date, account_details, due_this_statement, account_summary, interest_information, transactions, transaction_subtotals. Each transaction must include transaction_date (YYYY-MM-DD), details, amount (signed number), and type (credit|debit). Credits are non-negative; debits are non-positive."
    } else {
        "Extract bank statement transactions as strict JSON with key rows. Each row: booked_at (YYYY-MM-DD), description, amount_cents (integer)."
    }
}

fn openrouter_response_format_schema(schema_version: &str) -> Value {
    if schema_version == "statement_v2" {
        return serde_json::json!({
            "type":"json_schema",
            "json_schema":{
                "name":"statement_v2_rows",
                "strict":true,
                "schema":{
                    "type":"object",
                    "properties":{
                        "statement_period":{
                            "type":"object",
                            "properties":{
                                "start_date":{"type":"string"},
                                "end_date":{"type":"string"}
                            },
                            "required":["start_date","end_date"],
                            "additionalProperties":false
                        },
                        "statement_date":{"type":"string"},
                        "account_details":{
                            "type":"object",
                            "properties":{
                                "account_type":{"type":"string"},
                                "account_number_ending":{"type":"string"},
                                "customer_name":{"type":"string"}
                            },
                            "required":["account_type","account_number_ending","customer_name"],
                            "additionalProperties":false
                        },
                        "due_this_statement":{
                            "type":"object",
                            "properties":{
                                "payment_due_date":{"type":"string"},
                                "total_minimum_payment":{"type":"number"}
                            },
                            "required":["payment_due_date","total_minimum_payment"],
                            "additionalProperties":false
                        },
                        "account_summary":{
                            "type":"object",
                            "properties":{
                                "interest_charged":{"type":"number"},
                                "account_balance":{"type":"number"},
                                "credit_limit":{"type":"number"},
                                "available_credit":{"type":"number"}
                            },
                            "required":[
                                "interest_charged",
                                "account_balance",
                                "credit_limit",
                                "available_credit"
                            ],
                            "additionalProperties":false
                        },
                        "interest_information":{
                            "anyOf":[
                                {
                                    "type":"object",
                                    "properties":{
                                        "estimated_payoff_time":{
                                            "type":"object",
                                            "properties":{
                                                "years":{"type":"integer"},
                                                "months":{"type":"integer"}
                                            },
                                            "required":["years","months"],
                                            "additionalProperties":false
                                        }
                                    },
                                    "required":["estimated_payoff_time"],
                                    "additionalProperties":false
                                },
                                {"type":"null"}
                            ]
                        },
                        "transactions":{
                            "type":"array",
                            "items":{
                                "type":"object",
                                "properties":{
                                    "transaction_date":{"type":"string"},
                                    "details":{"type":"string"},
                                    "amount":{"type":"number"},
                                    "type":{"type":"string","enum":["credit","debit"]}
                                },
                                "required":["transaction_date","details","amount","type"],
                                "additionalProperties":false
                            }
                        },
                        "transaction_subtotals":{
                            "type":"object",
                            "properties":{
                                "credits_total":{"type":"number"},
                                "debits_total":{"type":"number"}
                            },
                            "required":["credits_total","debits_total"],
                            "additionalProperties":false
                        }
                    },
                    "required":[
                        "statement_period",
                        "statement_date",
                        "account_details",
                        "due_this_statement",
                        "account_summary",
                        "interest_information",
                        "transactions",
                        "transaction_subtotals"
                    ],
                    "additionalProperties":false
                }
            }
        });
    }

    serde_json::json!({
        "type":"json_schema",
        "json_schema":{
            "name":"statement_rows",
            "strict":true,
            "schema":{
                "type":"object",
                "properties":{
                    "rows":{
                        "type":"array",
                        "items":{
                            "type":"object",
                            "properties":{
                                "booked_at":{"type":"string"},
                                "description":{"type":"string"},
                                "amount_cents":{"type":"integer"},
                                "confidence":{"type":"number"}
                            },
                            "required":["booked_at","description","amount_cents","confidence"],
                            "additionalProperties":false
                        }
                    }
                },
                "required":["rows"],
                "additionalProperties":false
            }
        }
    })
}

fn map_row(row_index: i64, account_id: &str, row: ProviderRow) -> anyhow::Result<ExtractedRow> {
    let amount_cents = match (row.amount_cents, row.amount) {
        (Some(v), _) => v,
        (None, Some(value)) => parse_amount_cents(&value)
            .map_err(|e| anyhow!("invalid amount in provider row: {e}"))?,
        (None, None) => return Err(anyhow!("provider row missing amount")),
    };
    let description = normalize_description(&row.description);
    let normalized_txn_hash =
        compute_row_hash(account_id, &row.booked_at, amount_cents, &description);
    let confidence = row.confidence.unwrap_or(0.9);

    let mut parse_errors = Vec::new();
    if !is_iso_date(&row.booked_at) {
        parse_errors.push("date format not ISO (YYYY-MM-DD), review required".to_string());
    }
    let mut direction = TransactionDirection::from_value(row.direction.as_deref())
        .unwrap_or(TransactionDirection::Unknown);
    let direction_confidence = row.direction_confidence.unwrap_or(0.0);
    if direction == TransactionDirection::Debit && amount_cents >= 0 {
        parse_errors.push("direction_sign_conflict: debit requires negative amount".to_string());
        direction = TransactionDirection::Unknown;
    }
    if direction == TransactionDirection::Credit && amount_cents <= 0 {
        parse_errors.push("direction_sign_conflict: credit requires positive amount".to_string());
        direction = TransactionDirection::Unknown;
    }
    if row.direction.is_none() {
        parse_errors.push("missing_or_invalid_direction; review required".to_string());
    }
    if direction_confidence > 0.0 && direction_confidence < 0.5 {
        parse_errors.push("low_direction_confidence; review required".to_string());
        direction = TransactionDirection::Unknown;
    }

    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "direction".to_string(),
        Value::String(direction.as_str().to_string()),
    );
    if direction_confidence > 0.0 {
        metadata.insert(
            "direction_confidence".to_string(),
            Value::from(direction_confidence),
        );
    }
    if let Some(value) = row.running_balance_cents {
        metadata.insert("running_balance_cents".to_string(), Value::from(value));
    }
    if let Some(value) = row.transaction_type_raw {
        metadata.insert("transaction_type_raw".to_string(), Value::String(value));
    }
    if let Some(value) = row.counterparty {
        metadata.insert("counterparty".to_string(), Value::String(value));
    }
    if let Some(value) = row.reference_id {
        metadata.insert("reference_id".to_string(), Value::String(value));
    }
    if let Some(value) = row.meta {
        metadata.insert("meta".to_string(), value);
    }
    metadata.insert(
        "direction_source".to_string(),
        Value::String("model".to_string()),
    );

    let parse_error = if parse_errors.is_empty() {
        None
    } else {
        Some(parse_errors.join("; "))
    };

    Ok(ExtractedRow {
        row_index,
        booked_at: row.booked_at,
        amount_cents,
        description,
        confidence,
        parse_error,
        normalized_txn_hash,
        metadata: Some(Value::Object(metadata)),
    })
}

fn dedupe_mapped_rows(rows: Vec<ExtractedRow>) -> Vec<ExtractedRow> {
    let mut deduped = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for mut row in rows {
        if seen.insert(row.normalized_txn_hash.clone()) {
            row.row_index = deduped.len() as i64 + 1;
            deduped.push(row);
        }
    }
    deduped
}

fn with_attempt_diagnostics(
    base: Value,
    attempts: &[ProviderAttempt],
    llama_diag: &LlamaPhaseDiagnostics,
) -> Value {
    let mut by_provider = serde_json::Map::new();
    for attempt in attempts {
        let entry = by_provider
            .entry(attempt.provider.clone())
            .or_insert_with(|| Value::from(0));
        let current = entry.as_i64().unwrap_or(0);
        *entry = Value::from(current + 1);
    }

    let retry_trail = attempts
        .iter()
        .map(|attempt| {
            serde_json::json!({
                "provider": attempt.provider,
                "attempt_no": attempt.attempt_no,
                "outcome": attempt.outcome,
                "error_code": attempt.error_code,
                "retry_decision": attempt.retry_decision,
            })
        })
        .collect::<Vec<_>>();

    let last_error = attempts
        .iter()
        .rev()
        .find_map(|attempt| attempt.error_code.clone());

    serde_json::json!({
        "base": base,
        "provider_attempts_count": attempts.len(),
        "provider_counts": by_provider,
        "last_error_classification": last_error,
        "retry_fallback_trail": retry_trail,
        "llama_phase_started_at": llama_diag.llama_phase_started_at,
        "llama_phase_deadline_at": llama_diag.llama_phase_deadline_at,
        "llama_poll_count": llama_diag.llama_poll_count,
        "llama_last_state": llama_diag.llama_last_state,
        "fallback_reason": llama_diag.fallback_reason,
        "terminal_failure_classification": llama_diag.terminal_failure_classification,
    })
}

fn can_fallback_to_openrouter(
    terminal_reason: &Option<LlamaTerminalReason>,
    managed_fallback_enabled: bool,
) -> FallbackGateDecision {
    if !managed_fallback_enabled {
        return FallbackGateDecision::Blocked;
    }
    if terminal_reason.is_some() {
        return FallbackGateDecision::Allowed;
    }
    FallbackGateDecision::Blocked
}

fn classify_llama_terminal_reason(
    err: &ProviderError,
    budget: &LlamaPhaseBudget,
) -> LlamaTerminalReason {
    if budget.is_exhausted() || err.code == "MANAGED_PROVIDER_TIMEOUT" {
        return LlamaTerminalReason::ReadyWindowExceeded;
    }
    if err.code == "MANAGED_PROVIDER_SCHEMA_INVALID" {
        return LlamaTerminalReason::SchemaParseFailure;
    }
    if is_retryable(&err.code) {
        return LlamaTerminalReason::ProviderReportedFailure;
    }
    LlamaTerminalReason::NonRetryableFailure
}

fn llama_terminal_reason_label(reason: &LlamaTerminalReason) -> &'static str {
    match reason {
        LlamaTerminalReason::ReadyWindowExceeded => "ready_window_exceeded",
        LlamaTerminalReason::ProviderReportedFailure => "provider_reported_failure",
        LlamaTerminalReason::NonRetryableFailure => "non_retryable_failure",
        LlamaTerminalReason::SchemaParseFailure => "schema_parse_failure",
    }
}

async fn execute_http_call<F>(
    http: &reqwest::Client,
    meta: HttpCallMeta<'_>,
    build_request: F,
) -> Result<HttpCallSuccess, ProviderError>
where
    F: FnOnce(&reqwest::Client) -> reqwest::RequestBuilder,
{
    let started = Instant::now();
    let response = build_request(http).send().await;
    match response {
        Ok(response) => {
            let status = response.status();
            let response_headers = redact_headers(response.headers());
            let raw = response.text().await.unwrap_or_default();
            let latency_ms = started.elapsed().as_millis() as i64;
            let raw_event = ExternalApiRawEvent {
                ts_utc: chrono::Utc::now().to_rfc3339(),
                kind: "external_api_raw",
                import_id: Some(meta.import_id.to_string()),
                file_name: Some(meta.file_name.to_string()),
                provider: meta.provider.to_string(),
                attempt_no: Some(meta.attempt_no),
                operation: meta.operation.clone(),
                method: meta.method.to_string(),
                url: meta.url.clone(),
                request_body_meta: Some(meta.request_body_meta.clone()),
                status_code: Some(status.as_u16()),
                response_headers_redacted: response_headers.clone(),
                response_body_raw: Some(raw.clone()),
                error_message: None,
            };
            log_external_api_raw_event(&raw_event);
            let (raw_out, truncated) = truncate_raw(raw.clone());
            let event = HttpCallEvent {
                ts_utc: chrono::Utc::now().to_rfc3339(),
                kind: "http_call",
                import_id: meta.import_id.to_string(),
                file_name: meta.file_name.to_string(),
                provider: meta.provider.to_string(),
                attempt_no: meta.attempt_no,
                operation: meta.operation,
                method: meta.method.to_string(),
                url: meta.url,
                timeout_ms: meta.timeout_ms,
                request_headers_redacted: redacted_default_headers(),
                request_body_meta: meta.request_body_meta,
                status_code: Some(status.as_u16()),
                response_headers_redacted: response_headers,
                raw_response: Some(raw_out.clone()),
                truncated,
                latency_ms,
                error_code: None,
                error_message: None,
            };
            log_http_call_event(&event);
            if !status.is_success() {
                return Err(status_error(status, raw_out));
            }

            Ok(HttpCallSuccess {
                status: status.as_u16(),
                raw,
            })
        }
        Err(err) => {
            let mapped = map_network_error(err);
            let latency_ms = started.elapsed().as_millis() as i64;
            let raw_event = ExternalApiRawEvent {
                ts_utc: chrono::Utc::now().to_rfc3339(),
                kind: "external_api_raw",
                import_id: Some(meta.import_id.to_string()),
                file_name: Some(meta.file_name.to_string()),
                provider: meta.provider.to_string(),
                attempt_no: Some(meta.attempt_no),
                operation: meta.operation.clone(),
                method: meta.method.to_string(),
                url: meta.url.clone(),
                request_body_meta: Some(meta.request_body_meta.clone()),
                status_code: mapped.status_code,
                response_headers_redacted: serde_json::json!({}),
                response_body_raw: mapped.raw_response.clone(),
                error_message: Some(mapped.message.clone()),
            };
            log_external_api_raw_event(&raw_event);
            let event = HttpCallEvent {
                ts_utc: chrono::Utc::now().to_rfc3339(),
                kind: "http_call",
                import_id: meta.import_id.to_string(),
                file_name: meta.file_name.to_string(),
                provider: meta.provider.to_string(),
                attempt_no: meta.attempt_no,
                operation: meta.operation,
                method: meta.method.to_string(),
                url: meta.url,
                timeout_ms: meta.timeout_ms,
                request_headers_redacted: redacted_default_headers(),
                request_body_meta: meta.request_body_meta,
                status_code: mapped.status_code,
                response_headers_redacted: serde_json::json!({}),
                raw_response: mapped.raw_response.clone(),
                truncated: false,
                latency_ms,
                error_code: Some(mapped.code.clone()),
                error_message: Some(mapped.message.clone()),
            };
            log_http_call_event(&event);
            Err(mapped)
        }
    }
}

fn map_network_error(err: reqwest::Error) -> ProviderError {
    let code = if err.is_timeout() {
        "MANAGED_PROVIDER_TIMEOUT"
    } else {
        "MANAGED_PROVIDER_NETWORK"
    };
    ProviderError {
        code: code.to_string(),
        message: err.to_string(),
        status_code: None,
        raw_response: None,
        llama_state: None,
        llama_poll_count: 0,
    }
}

fn status_error(status: StatusCode, raw: String) -> ProviderError {
    let code = match status {
        StatusCode::TOO_MANY_REQUESTS => "MANAGED_PROVIDER_RATE_LIMITED",
        s if s.is_server_error() => "MANAGED_PROVIDER_SERVER_ERROR",
        _ => "MANAGED_PROVIDER_BAD_REQUEST",
    };
    let (raw_out, _) = truncate_raw(raw);
    ProviderError {
        code: code.to_string(),
        message: format!("provider returned status {}", status.as_u16()),
        status_code: Some(status.as_u16()),
        raw_response: Some(raw_out),
        llama_state: None,
        llama_poll_count: 0,
    }
}

fn redacted_default_headers() -> Value {
    serde_json::json!({
        "authorization": "REDACTED",
        "content-type": "REDACTED",
        "accept": "REDACTED",
    })
}

fn redact_headers(headers: &reqwest::header::HeaderMap) -> Value {
    let mut out = serde_json::Map::new();
    for name in headers.keys() {
        out.insert(
            name.as_str().to_string(),
            Value::String("REDACTED".to_string()),
        );
    }
    Value::Object(out)
}

fn is_retryable(code: &str) -> bool {
    matches!(
        code,
        "MANAGED_PROVIDER_TIMEOUT"
            | "MANAGED_PROVIDER_NETWORK"
            | "MANAGED_PROVIDER_RATE_LIMITED"
            | "MANAGED_PROVIDER_SERVER_ERROR"
    )
}

fn backoff_for_attempt(attempt: i64) -> Duration {
    match attempt {
        1 => Duration::from_millis(250),
        2 => Duration::from_millis(750),
        _ => Duration::from_millis(1500),
    }
}

fn adaptive_poll_delay(poll_no: i64) -> Duration {
    match poll_no {
        1 => Duration::from_secs(1),
        2 => Duration::from_secs(2),
        _ => Duration::from_secs(3),
    }
}

fn is_iso_date(value: &str) -> bool {
    let chunks: Vec<&str> = value.split('-').collect();
    chunks.len() == 3
        && chunks[0].len() == 4
        && chunks[1].len() == 2
        && chunks[2].len() == 2
        && chunks
            .iter()
            .all(|chunk| chunk.chars().all(|ch| ch.is_ascii_digit()))
}

fn env_var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn truncate_raw(raw: String) -> (String, bool) {
    let max_bytes = env::var("EXTRACTION_LOG_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(262_144);
    if raw.len() <= max_bytes {
        return (raw, false);
    }
    let mut out = raw;
    out.truncate(max_bytes);
    (out, true)
}

fn log_provider_attempt(import_id: &str, attempt: &ProviderAttempt, file_name: &str) {
    if !should_log_full_response() {
        return;
    }
    let log_path = extraction_log_path();
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(log_path) else {
        return;
    };

    let payload = serde_json::json!({
        "ts_utc": chrono::Utc::now().to_rfc3339(),
        "kind": "provider_attempt",
        "import_id": import_id,
        "file_name": file_name,
        "attempt": attempt,
    });
    let _ = writeln!(file, "{payload}");
}

fn log_http_call_event(event: &HttpCallEvent) {
    if !should_log_full_response() {
        return;
    }
    let log_path = extraction_log_path();
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(log_path) else {
        return;
    };
    if let Ok(line) = serde_json::to_string(event) {
        let _ = writeln!(file, "{line}");
    }
}

fn log_decision_event(
    import_id: &str,
    file_name: &str,
    provider: &str,
    attempt_no: i64,
    decision: &str,
    reason: &str,
) {
    if !should_log_full_response() {
        return;
    }
    let log_path = extraction_log_path();
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(log_path) else {
        return;
    };
    let event = DecisionEvent {
        ts_utc: chrono::Utc::now().to_rfc3339(),
        kind: "decision",
        import_id: import_id.to_string(),
        file_name: file_name.to_string(),
        provider: provider.to_string(),
        attempt_no,
        decision: decision.to_string(),
        reason: reason.to_string(),
    };
    if let Ok(line) = serde_json::to_string(&event) {
        let _ = writeln!(file, "{line}");
    }
}

fn extraction_log_path() -> PathBuf {
    if let Ok(explicit) = env::var("EXPENSE_EXTRACTION_LOG_PATH") {
        return PathBuf::from(explicit);
    }
    expense_core::default_app_data_dir()
        .join("logs")
        .join("extraction-provider.log")
}

fn external_api_raw_log_path() -> PathBuf {
    if let Ok(explicit) = env::var("EXPENSE_EXTERNAL_API_RAW_LOG_PATH") {
        return PathBuf::from(explicit);
    }
    expense_core::default_app_data_dir()
        .join("logs")
        .join("external-api-raw.log")
}

fn should_log_full_response() -> bool {
    // Intentional for local development and debugging provider behavior.
    // Revisit before production to avoid storing sensitive financial payloads by default.
    !env::var("EXTRACTION_LOG_FULL_RESPONSE")
        .ok()
        .map(|v| v == "false" || v == "0")
        .unwrap_or(false)
}

fn log_external_api_raw_event(event: &ExternalApiRawEvent) {
    let log_path = external_api_raw_log_path();
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(log_path) else {
        return;
    };
    if let Ok(line) = serde_json::to_string(event) {
        let _ = writeln!(file, "{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_error_codes_are_classified() {
        assert!(is_retryable("MANAGED_PROVIDER_TIMEOUT"));
        assert!(is_retryable("MANAGED_PROVIDER_SERVER_ERROR"));
        assert!(!is_retryable("MANAGED_PROVIDER_SCHEMA_INVALID"));
    }

    #[test]
    fn backoff_profile_matches_retry_plan() {
        assert_eq!(backoff_for_attempt(1), Duration::from_millis(250));
        assert_eq!(backoff_for_attempt(2), Duration::from_millis(750));
        assert_eq!(backoff_for_attempt(3), Duration::from_millis(1500));
        assert_eq!(backoff_for_attempt(99), Duration::from_millis(1500));
    }

    #[test]
    fn pages_fallback_extracts_candidate_rows() {
        let payload = serde_json::json!({
            "pages": [
                { "text": "2026-03-01 Coffee Shop -12.40\n03/02/2026 Grocery 45.10" }
            ]
        });
        let rows = extract_rows_with_pages_fallback(&payload).expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].booked_at, "2026-03-01");
        assert_eq!(rows[1].booked_at, "2026-03-02");
    }

    #[test]
    fn pages_fallback_uses_markdown_field_when_text_missing() {
        let payload = serde_json::json!({
            "pages": [
                { "md": "03/15/2026 Transit -4.25" }
            ]
        });
        let rows = extract_rows_with_pages_fallback(&payload).expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].booked_at, "2026-03-15");
        assert_eq!(rows[0].amount_cents, Some(-425));
    }

    #[test]
    fn pages_fallback_ignores_non_transaction_lines() {
        let payload = serde_json::json!({
            "pages": [
                { "text": "Opening Balance\nRandom Header\nNot a row\n2026-03-04 Lunch 14.00" }
            ]
        });
        let rows = extract_rows_with_pages_fallback(&payload).expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].description, "Lunch");
        assert_eq!(rows[0].amount_cents, Some(1400));
    }

    #[test]
    fn normalize_date_token_handles_expected_formats() {
        assert_eq!(
            normalize_date_token("2026-03-01", None),
            Some("2026-03-01".to_string())
        );
        assert_eq!(
            normalize_date_token("03/01/2026", None),
            Some("2026-03-01".to_string())
        );
        assert_eq!(
            normalize_date_token("Jan 24", Some(2026)),
            Some("2026-01-24".to_string())
        );
        assert_eq!(normalize_date_token("March", None), None);
    }

    #[test]
    fn extract_rows_with_pages_fallback_errors_when_no_rows_found() {
        let payload = serde_json::json!({
            "pages": [
                { "text": "Opening Balance\nNo transaction lines here" }
            ]
        });
        let err = extract_rows_with_pages_fallback(&payload)
            .expect_err("expected no parseable row error");
        assert!(err.to_string().contains("no parseable transaction rows"));
    }

    #[test]
    fn extract_rows_with_pages_fallback_prefers_rows_array_when_present() {
        let payload = serde_json::json!({
            "rows": [
                {
                    "booked_at":"2026-04-01",
                    "description":"Direct Row",
                    "amount_cents":123
                }
            ],
            "pages":[
                { "text":"2026-03-01 Ignored 99.00" }
            ]
        });
        let rows = extract_rows_with_pages_fallback(&payload).expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].description, "Direct Row");
        assert_eq!(rows[0].amount_cents, Some(123));
    }

    #[test]
    fn openrouter_schema_requires_all_declared_fields() {
        let schema = openrouter_response_format_schema("statement_v1");
        let required = schema
            .get("json_schema")
            .and_then(|v| v.get("schema"))
            .and_then(|v| v.get("properties"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.get("items"))
            .and_then(|v| v.get("required"))
            .and_then(|v| v.as_array())
            .expect("required array");

        let as_strings: Vec<String> = required
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        assert!(as_strings.contains(&"booked_at".to_string()));
        assert!(as_strings.contains(&"description".to_string()));
        assert!(as_strings.contains(&"amount_cents".to_string()));
        assert!(as_strings.contains(&"confidence".to_string()));
    }

    #[test]
    fn openrouter_statement_v2_schema_requires_statement_period_and_transactions() {
        let schema = openrouter_response_format_schema("statement_v2");
        let required = schema
            .get("json_schema")
            .and_then(|v| v.get("schema"))
            .and_then(|v| v.get("required"))
            .and_then(|v| v.as_array())
            .expect("root required");
        let root_required: Vec<String> = required
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(root_required.contains(&"statement_period".to_string()));
        assert!(root_required.contains(&"transactions".to_string()));

        let tx_required = schema
            .get("json_schema")
            .and_then(|v| v.get("schema"))
            .and_then(|v| v.get("properties"))
            .and_then(|v| v.get("transactions"))
            .and_then(|v| v.get("items"))
            .and_then(|v| v.get("required"))
            .and_then(|v| v.as_array())
            .expect("tx required");
        let tx_required_keys: Vec<String> = tx_required
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(tx_required_keys.contains(&"transaction_date".to_string()));
        assert!(tx_required_keys.contains(&"type".to_string()));
    }

    #[test]
    fn map_statement_row_flags_sign_conflict_but_keeps_direction() {
        let row = StatementRowCandidate {
            transaction_date: Some("2026-03-10".to_string()),
            details: Some("Salary".to_string()),
            amount: Some(-50.0),
            confidence: Some(0.9),
            tx_type: Some("credit".to_string()),
        };
        let mapped = map_statement_row(1, "acct-1", row);
        let direction = mapped
            .metadata
            .as_ref()
            .and_then(|v| v.get("direction"))
            .and_then(|v| v.as_str());
        assert_eq!(direction, Some("credit"));
        assert!(mapped
            .parse_error
            .as_deref()
            .unwrap_or_default()
            .contains("type_sign_conflict"));
    }

    #[test]
    fn map_statement_row_marks_unknown_when_type_missing() {
        let row = StatementRowCandidate {
            transaction_date: Some("2026-03-10".to_string()),
            details: Some("Txn".to_string()),
            amount: Some(50.0),
            confidence: Some(0.9),
            tx_type: None,
        };
        let mapped = map_statement_row(1, "acct-1", row);
        let direction = mapped
            .metadata
            .as_ref()
            .and_then(|v| v.get("direction"))
            .and_then(|v| v.as_str());
        assert_eq!(direction, Some("unknown"));
        assert!(mapped.parse_error.is_some());
    }

    #[test]
    fn not_ready_llama_status_is_retryable_timeout() {
        let err = ProviderError {
            code: "MANAGED_PROVIDER_TIMEOUT".to_string(),
            message: "llamaparse result not ready within polling window".to_string(),
            status_code: Some(404),
            raw_response: Some("not found".to_string()),
            llama_state: Some("processing".to_string()),
            llama_poll_count: 1,
        };
        assert!(is_retryable(&err.code));
    }

    #[test]
    fn empty_openrouter_body_should_map_to_retryable_timeout() {
        let err = ProviderError {
            code: "MANAGED_PROVIDER_TIMEOUT".to_string(),
            message: "openrouter returned empty response body".to_string(),
            status_code: Some(200),
            raw_response: Some(String::new()),
            llama_state: None,
            llama_poll_count: 0,
        };
        assert!(is_retryable(&err.code));
    }

    #[test]
    fn attempt_diagnostics_include_provider_counts_and_last_error() {
        let attempts = vec![
            ProviderAttempt {
                provider: "llamaparse".to_string(),
                attempt_no: 1,
                status_code: Some(504),
                outcome: "error".to_string(),
                error_code: Some("MANAGED_PROVIDER_TIMEOUT".to_string()),
                error_message: Some("timed out".to_string()),
                latency_ms: 1000,
                retry_decision: "retry".to_string(),
                raw_response: None,
                truncated: false,
            },
            ProviderAttempt {
                provider: "openrouter_pdf_text".to_string(),
                attempt_no: 1,
                status_code: Some(400),
                outcome: "error".to_string(),
                error_code: Some("MANAGED_PROVIDER_SCHEMA_INVALID".to_string()),
                error_message: Some("bad schema".to_string()),
                latency_ms: 800,
                retry_decision: "stop".to_string(),
                raw_response: None,
                truncated: false,
            },
        ];

        let diagnostics = with_attempt_diagnostics(
            serde_json::json!({ "provider": "openrouter_pdf_text" }),
            &attempts,
            &LlamaPhaseDiagnostics {
                llama_phase_started_at: "2026-03-04T00:00:00Z".to_string(),
                llama_phase_deadline_at: "2026-03-04T00:03:00Z".to_string(),
                llama_poll_count: 9,
                llama_last_state: "failed".to_string(),
                fallback_reason: Some("llama terminal failure".to_string()),
                terminal_failure_classification: Some(
                    "MANAGED_PROVIDER_SCHEMA_INVALID".to_string(),
                ),
            },
        );
        assert_eq!(
            diagnostics
                .get("provider_attempts_count")
                .and_then(|v| v.as_i64())
                .unwrap_or_default(),
            2
        );
        assert_eq!(
            diagnostics
                .get("last_error_classification")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
            "MANAGED_PROVIDER_SCHEMA_INVALID"
        );
        assert_eq!(
            diagnostics
                .get("llama_poll_count")
                .and_then(|v| v.as_i64())
                .unwrap_or_default(),
            9
        );
    }

    #[test]
    fn fallback_gate_blocks_until_llama_terminal() {
        assert_eq!(
            can_fallback_to_openrouter(&None, true),
            FallbackGateDecision::Blocked
        );
        assert_eq!(
            can_fallback_to_openrouter(&Some(LlamaTerminalReason::SchemaParseFailure), true),
            FallbackGateDecision::Allowed
        );
        assert_eq!(
            can_fallback_to_openrouter(&Some(LlamaTerminalReason::ReadyWindowExceeded), false),
            FallbackGateDecision::Blocked
        );
    }

    #[test]
    fn markdown_table_parsing_handles_month_day_and_multiline_description() {
        let md = r#"
| Date   | Transactions                    | Amounts withdrawn ($) | Amounts deposited ($) | Balance ($) |
| ------ | ------------------------------- | --------------------- | --------------------- | ----------- |
| Jan 24 | Point of sale purchase          | 1.05                  |                       | 484.76      |
|        | Opos Uberdirectca_Pass Toronto  |                       |                       |             |
| Jan 26 | Error correction                |                       | 1.05                  | 435.10      |
"#;
        let rows = parse_markdown_table_rows(md, Some(2026));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].booked_at, "2026-01-24");
        assert_eq!(rows[0].amount_cents, Some(-105));
        assert!(rows[0].description.contains("Opos Uberdirectca_Pass"));
        assert_eq!(rows[1].amount_cents, Some(105));
    }

    #[test]
    fn redact_headers_masks_values() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-test", "value".parse().expect("header"));
        let redacted = redact_headers(&headers);
        assert_eq!(
            redacted
                .get("x-test")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
            "REDACTED"
        );
    }

    #[tokio::test]
    async fn local_ocr_stub_returns_expected_error() {
        let result = local_ocr_stub(&ExtractionRequest {
            import_id: "i1".to_string(),
            account_id: "a1".to_string(),
            file_name: "statement.pdf".to_string(),
            bytes: vec![],
            max_provider_retries: 3,
            timeout_ms: 30_000,
            managed_fallback_enabled: true,
        })
        .await
        .expect("stub result");

        assert!(result
            .errors
            .contains(&"LOCAL_OCR_NOT_IMPLEMENTED".to_string()));
    }

    #[test]
    fn dedupe_mapped_rows_uses_normalized_hash() {
        let rows = vec![
            ExtractedRow {
                row_index: 8,
                booked_at: "2026-01-01".to_string(),
                amount_cents: 100,
                description: "A".to_string(),
                confidence: 0.9,
                parse_error: None,
                normalized_txn_hash: "h1".to_string(),
                metadata: None,
            },
            ExtractedRow {
                row_index: 9,
                booked_at: "2026-01-01".to_string(),
                amount_cents: 100,
                description: "A".to_string(),
                confidence: 0.8,
                parse_error: None,
                normalized_txn_hash: "h1".to_string(),
                metadata: None,
            },
            ExtractedRow {
                row_index: 10,
                booked_at: "2026-01-02".to_string(),
                amount_cents: 200,
                description: "B".to_string(),
                confidence: 0.95,
                parse_error: None,
                normalized_txn_hash: "h2".to_string(),
                metadata: None,
            },
        ];

        let deduped = dedupe_mapped_rows(rows);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].row_index, 1);
        assert_eq!(deduped[1].row_index, 2);
    }

    #[test]
    fn reconciliation_passes_within_tolerance_and_fails_when_outside() {
        let rows = vec![
            ExtractedRow {
                row_index: 1,
                booked_at: "2026-01-01".to_string(),
                amount_cents: -1000,
                description: "Debit".to_string(),
                confidence: 0.9,
                parse_error: None,
                normalized_txn_hash: "h1".to_string(),
                metadata: Some(serde_json::json!({
                    "direction": "debit"
                })),
            },
            ExtractedRow {
                row_index: 2,
                booked_at: "2026-01-02".to_string(),
                amount_cents: 500,
                description: "Credit".to_string(),
                confidence: 0.9,
                parse_error: None,
                normalized_txn_hash: "h2".to_string(),
                metadata: Some(serde_json::json!({
                    "direction": "credit"
                })),
            },
        ];
        let pass = compute_reconciliation(
            &serde_json::json!({
                "opening_balance_cents": 10_000,
                "closing_balance_cents": 9_500,
                "total_debits_cents": 1_000,
                "total_credits_cents": 500
            }),
            &rows,
            1,
        );
        assert_eq!(pass.get("failed").and_then(|v| v.as_bool()), Some(false));

        let fail = compute_reconciliation(
            &serde_json::json!({
                "opening_balance_cents": 10_000,
                "closing_balance_cents": 9_700,
                "total_debits_cents": 1_000,
                "total_credits_cents": 500
            }),
            &rows,
            1,
        );
        assert_eq!(fail.get("failed").and_then(|v| v.as_bool()), Some(true));
    }

    #[test]
    fn fallback_enriches_unknown_direction_by_running_balance_delta() {
        let rows = vec![
            ExtractedRow {
                row_index: 1,
                booked_at: "2026-01-01".to_string(),
                amount_cents: -2000,
                description: "POS".to_string(),
                confidence: 0.8,
                parse_error: None,
                normalized_txn_hash: "x1".to_string(),
                metadata: Some(serde_json::json!({
                    "direction": "unknown",
                    "direction_source": "model",
                    "running_balance_cents": 8_000
                })),
            },
            ExtractedRow {
                row_index: 2,
                booked_at: "2026-01-02".to_string(),
                amount_cents: -1000,
                description: "Unknown".to_string(),
                confidence: 0.8,
                parse_error: None,
                normalized_txn_hash: "x2".to_string(),
                metadata: Some(serde_json::json!({
                    "direction": "unknown",
                    "direction_source": "model",
                    "running_balance_cents": 7_000
                })),
            },
        ];

        let enriched = enrich_unresolved_directions(rows);
        assert_eq!(
            enriched[1]
                .metadata
                .as_ref()
                .and_then(|v| v.get("direction"))
                .and_then(|v| v.as_str()),
            Some("debit")
        );
        assert_eq!(
            enriched[1]
                .metadata
                .as_ref()
                .and_then(|v| v.get("direction_source"))
                .and_then(|v| v.as_str()),
            Some("balance_delta")
        );
    }

    #[test]
    fn fallback_does_not_override_manual_direction() {
        let rows = vec![ExtractedRow {
            row_index: 1,
            booked_at: "2026-01-01".to_string(),
            amount_cents: 2000,
            description: "salary".to_string(),
            confidence: 0.8,
            parse_error: None,
            normalized_txn_hash: "x1".to_string(),
            metadata: Some(serde_json::json!({
                "direction": "debit",
                "direction_source": "manual"
            })),
        }];
        let enriched = enrich_unresolved_directions(rows);
        assert_eq!(
            enriched[0]
                .metadata
                .as_ref()
                .and_then(|v| v.get("direction"))
                .and_then(|v| v.as_str()),
            Some("debit")
        );
        assert_eq!(
            enriched[0]
                .metadata
                .as_ref()
                .and_then(|v| v.get("direction_source"))
                .and_then(|v| v.as_str()),
            Some("manual")
        );
    }

    #[test]
    fn versioned_agent_name_uses_expected_pattern() {
        assert_eq!(
            versioned_agent_name("spendora-statement-agent", "statement_v1"),
            "spendora-statement-agent--statement_v1"
        );
    }

    #[test]
    fn extract_agent_id_by_name_handles_data_array_shape() {
        let payload = serde_json::json!({
            "data": [
                { "id": "a1", "name": "one" },
                { "id": "a2", "name": "two" }
            ]
        });
        assert_eq!(
            extract_agent_id_by_name(&payload, "two").as_deref(),
            Some("a2")
        );
    }

    #[test]
    fn scoped_url_ignores_invalid_optional_scope_ids() {
        let cfg = ExtractionRuntimeConfig {
            llama_cloud_api_key: "k".to_string(),
            llama_agent_name: "agent".to_string(),
            llama_schema_version: "statement_v1".to_string(),
            llama_cloud_organization_id: Some("...".to_string()),
            llama_cloud_project_id: Some("not-a-uuid".to_string()),
        };
        let url = scoped_url(
            "https://api.cloud.llamaindex.ai",
            "/api/v1/extraction/extraction-agents",
            &cfg,
        )
        .expect("scoped url");
        let rendered = url.as_str().to_string();
        assert!(!rendered.contains("organization_id="));
        assert!(!rendered.contains("project_id="));
    }

    #[test]
    fn classify_job_status_maps_known_buckets() {
        assert!(matches!(
            classify_job_status("SUCCESS"),
            JobStatusClass::TerminalSuccess(JobsTerminalStatus::Success)
        ));
        assert!(matches!(
            classify_job_status("PARTIAL_SUCCESS"),
            JobStatusClass::TerminalSuccess(JobsTerminalStatus::PartialSuccess)
        ));
        assert!(matches!(
            classify_job_status("ERROR"),
            JobStatusClass::TerminalFailure(JobsTerminalStatus::Error)
        ));
        assert!(matches!(
            classify_job_status("RUNNING"),
            JobStatusClass::InProgress
        ));
        assert!(matches!(classify_job_status("UNSEEN"), JobStatusClass::Unknown));
    }

    #[test]
    fn validate_jobs_result_payload_requires_transactions_array() {
        let payload = serde_json::json!({
            "result": {
                "period_start": "2026-03-01",
                "period_end": "2026-03-31"
            }
        });
        let err = validate_jobs_result_payload(&payload, "statement_v1")
            .expect_err("missing transactions");
        assert!(err.to_string().contains("MANAGED_PROVIDER_SCHEMA_INVALID"));
    }

    #[test]
    fn validate_jobs_result_payload_v2_requires_account_summary() {
        let payload = serde_json::json!({
            "result": {
                "statement_date": "2026-03-31",
                "statement_period": {
                    "start_date": "2026-03-01",
                    "end_date": "2026-03-31"
                },
                "account_details": {
                    "account_type": "Card",
                    "account_number_ending": "1234",
                    "customer_name": "Test User"
                },
                "due_this_statement": {
                    "payment_due_date": "2026-04-10",
                    "total_minimum_payment": 25.0
                },
                "interest_information": null,
                "transactions": [
                    {
                        "transaction_date":"2026-03-10",
                        "details":"x",
                        "amount":1.0,
                        "type":"credit"
                    }
                ],
                "transaction_subtotals": {
                    "credits_total": 1.0,
                    "debits_total": 0.0
                }
            }
        });
        let err = validate_jobs_result_payload(&payload, "statement_v2")
            .expect_err("missing account summary");
        assert!(err.to_string().contains("missing account_summary"));
    }

    #[test]
    fn resolve_period_derives_when_missing() {
        let rows = vec![
            StatementRowCandidate {
                transaction_date: Some("2026-03-01".to_string()),
                details: Some("a".to_string()),
                amount: Some(0.1),
                confidence: Some(0.8),
                tx_type: None,
            },
            StatementRowCandidate {
                transaction_date: Some("2026-03-31".to_string()),
                details: Some("b".to_string()),
                amount: Some(0.2),
                confidence: Some(0.8),
                tx_type: None,
            },
        ];
        let (start, end, derived) = resolve_period(None, None, &rows).expect("derived");
        assert_eq!(start, "2026-03-01");
        assert_eq!(end, "2026-03-31");
        assert!(derived);
    }
}
