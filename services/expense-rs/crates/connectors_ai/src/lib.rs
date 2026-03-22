use anyhow::anyhow;
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use expense_core::{
    compute_row_hash, normalize_description, parse_amount_cents, ExtractionRuntimeConfig,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
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

    if response.status().is_success() {
        log_bootstrap_event(serde_json::json!({
            "ts_utc": chrono::Utc::now().to_rfc3339(),
            "kind": "bootstrap_schema_validation_success",
            "status_code": response.status().as_u16(),
        }));
        return Ok(());
    }

    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
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

    if !response.status().is_success() {
        let status_code = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        return Err(LlamaAgentBootstrapError {
            code: "EXTRACTION_AGENT_BOOTSTRAP_API_UNREACHABLE".to_string(),
            message: format!("agent list failed ({status_code}): {body}"),
            status_code: Some(status_code),
        });
    }

    let body: Value = response
        .json()
        .await
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

    if !response.status().is_success() {
        let status_code = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        return Err(LlamaAgentBootstrapError {
            code: "EXTRACTION_AGENT_BOOTSTRAP_API_UNREACHABLE".to_string(),
            message: format!("agent create failed ({status_code}): {body}"),
            status_code: Some(status_code),
        });
    }

    let body: Value = response
        .json()
        .await
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
    let prompt = "Extract bank statement transactions as strict JSON with key rows. Each row: booked_at (YYYY-MM-DD), description, amount_cents (integer).";
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
        "response_format": openrouter_response_format_schema()
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
            .map(|item| serde_json::from_value(item.clone()))
            .collect::<Result<_, _>>()?;
        return Ok(parsed);
    }
    if let Some(rows) = value
        .get("output")
        .and_then(|v| v.get("rows"))
        .and_then(|v| v.as_array())
    {
        let parsed: Vec<ProviderRow> = rows
            .iter()
            .map(|item| serde_json::from_value(item.clone()))
            .collect::<Result<_, _>>()?;
        return Ok(parsed);
    }
    Err(anyhow!("provider payload missing rows[]"))
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

fn openrouter_response_format_schema() -> Value {
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

    let parse_error = if is_iso_date(&row.booked_at) {
        None
    } else {
        Some("date format not ISO (YYYY-MM-DD), review required".to_string())
    };

    Ok(ExtractedRow {
        row_index,
        booked_at: row.booked_at,
        amount_cents,
        description,
        confidence,
        parse_error,
        normalized_txn_hash,
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

fn should_log_full_response() -> bool {
    // Intentional for local development and debugging provider behavior.
    // Revisit before production to avoid storing sensitive financial payloads by default.
    !env::var("EXTRACTION_LOG_FULL_RESPONSE")
        .ok()
        .map(|v| v == "false" || v == "0")
        .unwrap_or(false)
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
        let schema = openrouter_response_format_schema();
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
            },
            ExtractedRow {
                row_index: 9,
                booked_at: "2026-01-01".to_string(),
                amount_cents: 100,
                description: "A".to_string(),
                confidence: 0.8,
                parse_error: None,
                normalized_txn_hash: "h1".to_string(),
            },
            ExtractedRow {
                row_index: 10,
                booked_at: "2026-01-02".to_string(),
                amount_cents: 200,
                description: "B".to_string(),
                confidence: 0.95,
                parse_error: None,
                normalized_txn_hash: "h2".to_string(),
            },
        ];

        let deduped = dedupe_mapped_rows(rows);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].row_index, 1);
        assert_eq!(deduped[1].row_index, 2);
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
}
