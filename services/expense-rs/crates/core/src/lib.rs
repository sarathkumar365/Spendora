use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::hash_map::DefaultHasher, env, hash::Hasher, path::PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub service: &'static str,
    pub status: &'static str,
    pub now_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImportStatus {
    Queued,
    Parsing,
    PendingCardResolution,
    ReviewRequired,
    ReadyToCommit,
    Committed,
    Failed,
}

impl ImportStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Parsing => "parsing",
            Self::PendingCardResolution => "pending_card_resolution",
            Self::ReviewRequired => "review_required",
            Self::ReadyToCommit => "ready_to_commit",
            Self::Committed => "committed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransactionSource {
    Plaid,
    Manual,
}

impl TransactionSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plaid => "plaid",
            Self::Manual => "manual",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClassificationSource {
    Manual,
    Rule,
    Agent,
}

impl ClassificationSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Rule => "rule",
            Self::Agent => "agent",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportSession {
    pub id: String,
    pub file_name: String,
    pub parser_type: String,
    pub status: String,
    pub review_required_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportRow {
    pub id: String,
    pub import_id: String,
    pub row_index: i64,
    pub booked_at: String,
    pub amount_cents: i64,
    pub description: String,
    pub confidence: f64,
    pub parse_error: Option<String>,
    pub normalized_txn_hash: String,
    pub approved: bool,
    pub rejection_reason: Option<String>,
    pub account_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("resource not found")]
    NotFound,
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("parse failure: {0}")]
    Parse(String),
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),
    #[error("commit conflict: {0}")]
    CommitConflict(String),
    #[error("duplicate ignored: {0}")]
    DuplicateIgnored(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractionRuntimeConfig {
    pub llama_cloud_api_key: String,
    pub llama_agent_name: String,
    pub llama_schema_version: String,
    pub llama_cloud_organization_id: Option<String>,
    pub llama_cloud_project_id: Option<String>,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ExtractionRuntimeConfigError {
    #[error("EXTRACTION_CONFIG_MISSING_REQUIRED_ENV: {0}")]
    MissingRequiredEnv(&'static str),
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum BlueprintSchemaError {
    #[error("EXTRACTION_SCHEMA_VERSION_NOT_FOUND: {0}")]
    VersionNotFound(String),
    #[error("EXTRACTION_SCHEMA_INVALID_JSON: {0}")]
    InvalidJson(String),
    #[error("EXTRACTION_SCHEMA_INVALID_CONTRACT: {0}")]
    InvalidContract(String),
}

pub fn new_health_status(service: &'static str) -> HealthStatus {
    HealthStatus {
        service,
        status: "ok",
        now_utc: Utc::now(),
    }
}

pub fn new_idempotency_key() -> String {
    Uuid::new_v4().to_string()
}

pub fn normalize_description(input: &str) -> String {
    input
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

pub fn parse_amount_cents(input: &str) -> Result<i64, DomainError> {
    let cleaned = input
        .trim()
        .replace('$', "")
        .replace(',', "")
        .replace('(', "-")
        .replace(')', "");
    let value: f64 = cleaned
        .parse()
        .map_err(|_| DomainError::Validation(format!("invalid amount: {input}")))?;
    Ok((value * 100.0).round() as i64)
}

pub fn compute_row_hash(
    account_id: &str,
    booked_at: &str,
    amount_cents: i64,
    description: &str,
) -> String {
    let mut hasher = DefaultHasher::new();
    hasher.write(account_id.as_bytes());
    hasher.write(booked_at.as_bytes());
    hasher.write(amount_cents.to_string().as_bytes());
    hasher.write(normalize_description(description).as_bytes());
    format!("{:x}", hasher.finish())
}

pub fn compute_source_hash(bytes: &[u8]) -> String {
    let mut hasher = DefaultHasher::new();
    hasher.write(bytes);
    format!("{:x}", hasher.finish())
}

pub fn default_app_data_dir() -> PathBuf {
    if let Ok(explicit) = env::var("EXPENSE_APP_DATA_DIR") {
        return PathBuf::from(explicit);
    }

    #[cfg(target_os = "windows")]
    {
        if let Ok(app_data) = env::var("APPDATA") {
            return PathBuf::from(app_data).join("SpendoraDesktop");
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home)
                .join("Library")
                .join("Application Support")
                .join("SpendoraDesktop");
        }
    }

    if let Ok(home) = env::var("HOME") {
        return PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("SpendoraDesktop");
    }

    PathBuf::from(".").join("data")
}

pub fn load_extraction_runtime_config_from_env(
) -> Result<ExtractionRuntimeConfig, ExtractionRuntimeConfigError> {
    Ok(ExtractionRuntimeConfig {
        llama_cloud_api_key: required_env("LLAMA_CLOUD_API_KEY")?,
        llama_agent_name: required_env("LLAMA_AGENT_NAME")?,
        llama_schema_version: required_env("LLAMA_SCHEMA_VERSION")?,
        llama_cloud_organization_id: optional_env("LLAMA_CLOUD_ORGANIZATION_ID"),
        llama_cloud_project_id: optional_env("LLAMA_CLOUD_PROJECT_ID"),
    })
}

pub fn load_statement_blueprint_schema(version: &str) -> Result<Value, BlueprintSchemaError> {
    let raw = match version {
        "statement_v1" => include_str!("../../../schemas/statement_v1.json"),
        "statement_v2" => include_str!("../../../schemas/statement_v2.json"),
        _ => {
            return Err(BlueprintSchemaError::VersionNotFound(version.to_string()));
        }
    };

    let schema: Value =
        serde_json::from_str(raw).map_err(|e| BlueprintSchemaError::InvalidJson(e.to_string()))?;
    validate_statement_schema_contract(&schema, version)?;
    Ok(schema)
}

fn required_env(name: &'static str) -> Result<String, ExtractionRuntimeConfigError> {
    optional_env(name).ok_or(ExtractionRuntimeConfigError::MissingRequiredEnv(name))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn validate_statement_schema_contract(
    schema: &Value,
    version: &str,
) -> Result<(), BlueprintSchemaError> {
    let root_required = schema
        .get("required")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            BlueprintSchemaError::InvalidContract("root required[] missing".to_string())
        })?;
    let root_required_keys = if version == "statement_v2" {
        vec![
            "statement_period",
            "statement_date",
            "account_details",
            "due_this_statement",
            "account_summary",
            "interest_information",
            "transactions",
            "transaction_subtotals",
        ]
    } else {
        vec!["period_start", "period_end", "transactions"]
    };
    for key in root_required_keys {
        let has_key = root_required
            .iter()
            .filter_map(|v| v.as_str())
            .any(|item| item == key);
        if !has_key {
            return Err(BlueprintSchemaError::InvalidContract(format!(
                "root required[] missing {key}"
            )));
        }
    }

    let tx_required = schema
        .get("properties")
        .and_then(|v| v.get("transactions"))
        .and_then(|v| v.get("items"))
        .and_then(|v| v.get("required"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            BlueprintSchemaError::InvalidContract(
                "transactions.items.required[] missing".to_string(),
            )
        })?;

    let tx_required_keys = if version == "statement_v2" {
        vec!["transaction_date", "details", "amount", "type"]
    } else {
        vec!["booked_at", "description", "amount_cents"]
    };
    for key in tx_required_keys {
        let has_key = tx_required
            .iter()
            .filter_map(|v| v.as_str())
            .any(|item| item == key);
        if !has_key {
            return Err(BlueprintSchemaError::InvalidContract(format!(
                "transactions.items.required[] missing {key}"
            )));
        }
    }

    let root_is_strict = schema
        .get("additionalProperties")
        .and_then(|v| v.as_bool())
        .is_some_and(|value| !value);
    if !root_is_strict {
        return Err(BlueprintSchemaError::InvalidContract(
            "root additionalProperties must be false".to_string(),
        ));
    }

    let tx_item_is_strict = schema
        .get("properties")
        .and_then(|v| v.get("transactions"))
        .and_then(|v| v.get("items"))
        .and_then(|v| v.get("additionalProperties"))
        .and_then(|v| v.as_bool())
        .is_some_and(|value| !value);
    if !tx_item_is_strict {
        return Err(BlueprintSchemaError::InvalidContract(
            "transactions.items.additionalProperties must be false".to_string(),
        ));
    }

    if version == "statement_v2" {
        let required_blocks = vec![
            ("statement_period", vec!["start_date", "end_date"]),
            (
                "account_details",
                vec!["account_type", "account_number_ending", "customer_name"],
            ),
            (
                "due_this_statement",
                vec!["payment_due_date", "total_minimum_payment"],
            ),
            (
                "account_summary",
                vec![
                    "interest_charged",
                    "account_balance",
                    "credit_limit",
                    "available_credit",
                ],
            ),
            (
                "transaction_subtotals",
                vec!["credits_total", "debits_total"],
            ),
        ];

        for (block, keys) in required_blocks {
            let required = schema
                .get("properties")
                .and_then(|v| v.get(block))
                .and_then(|v| v.get("required"))
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    BlueprintSchemaError::InvalidContract(format!("{block}.required[] missing"))
                })?;
            for key in keys {
                let has_key = required
                    .iter()
                    .filter_map(|v| v.as_str())
                    .any(|item| item == key);
                if !has_key {
                    return Err(BlueprintSchemaError::InvalidContract(format!(
                        "{block}.required[] missing {key}"
                    )));
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn health_status_is_ok_and_namespaced() {
        let status = new_health_status("expense-api");
        assert_eq!(status.service, "expense-api");
        assert_eq!(status.status, "ok");
    }

    #[test]
    fn idempotency_key_generation_is_unique() {
        let mut keys = HashSet::new();
        for _ in 0..100 {
            let inserted = keys.insert(new_idempotency_key());
            assert!(inserted, "duplicate idempotency key generated");
        }
    }

    #[test]
    fn normalize_and_hash_are_deterministic() {
        let hash_a = compute_row_hash("acct", "2026-03-01", 1234, "  Coffee  shop ");
        let hash_b = compute_row_hash("acct", "2026-03-01", 1234, "coffee shop");
        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn parse_amount_handles_currency_decorations() {
        assert_eq!(parse_amount_cents("$12.34").expect("parse amount"), 1234);
        assert_eq!(parse_amount_cents("(5.00)").expect("parse amount"), -500);
    }

    #[test]
    fn app_data_dir_uses_expected_suffix() {
        let path = default_app_data_dir();
        let rendered = path.display().to_string();
        assert!(
            rendered.contains("SpendoraDesktop")
                || rendered.ends_with("./data")
                || rendered.contains(".runtime"),
            "unexpected app data path: {rendered}"
        );
    }

    #[test]
    fn extraction_runtime_config_requires_all_mandatory_envs() {
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

        let err = load_extraction_runtime_config_from_env().expect_err("expected missing env");
        assert_eq!(
            err,
            ExtractionRuntimeConfigError::MissingRequiredEnv("LLAMA_CLOUD_API_KEY")
        );
    }

    #[test]
    fn extraction_runtime_config_accepts_optional_envs_missing() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe {
            std::env::set_var("LLAMA_CLOUD_API_KEY", "api-key");
            std::env::set_var("LLAMA_AGENT_NAME", "statement-agent");
            std::env::set_var("LLAMA_SCHEMA_VERSION", "statement_v1");
            std::env::remove_var("LLAMA_CLOUD_ORGANIZATION_ID");
            std::env::remove_var("LLAMA_CLOUD_PROJECT_ID");
        }

        let value = load_extraction_runtime_config_from_env().expect("config");
        assert_eq!(value.llama_cloud_api_key, "api-key");
        assert_eq!(value.llama_agent_name, "statement-agent");
        assert_eq!(value.llama_schema_version, "statement_v1");
        assert!(value.llama_cloud_organization_id.is_none());
        assert!(value.llama_cloud_project_id.is_none());

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
    fn statement_schema_loader_rejects_unknown_version() {
        let err =
            load_statement_blueprint_schema("does_not_exist").expect_err("expected version error");
        assert_eq!(
            err,
            BlueprintSchemaError::VersionNotFound("does_not_exist".to_string())
        );
    }

    #[test]
    fn statement_schema_loader_validates_required_contract() {
        let schema = load_statement_blueprint_schema("statement_v1").expect("statement_v1 schema");
        let tx_required = schema
            .get("properties")
            .and_then(|v| v.get("transactions"))
            .and_then(|v| v.get("items"))
            .and_then(|v| v.get("required"))
            .and_then(|v| v.as_array())
            .expect("required fields");
        let required_keys: Vec<&str> = tx_required.iter().filter_map(|v| v.as_str()).collect();
        assert!(required_keys.contains(&"booked_at"));
        assert!(required_keys.contains(&"description"));
        assert!(required_keys.contains(&"amount_cents"));
    }

    #[test]
    fn statement_v2_schema_loader_validates_required_contract() {
        let schema = load_statement_blueprint_schema("statement_v2").expect("statement_v2 schema");
        let root_required = schema
            .get("required")
            .and_then(|v| v.as_array())
            .expect("root required fields");
        let root_required_keys: Vec<&str> =
            root_required.iter().filter_map(|v| v.as_str()).collect();
        assert!(root_required_keys.contains(&"statement_period"));
        assert!(root_required_keys.contains(&"transaction_subtotals"));

        let tx_required = schema
            .get("properties")
            .and_then(|v| v.get("transactions"))
            .and_then(|v| v.get("items"))
            .and_then(|v| v.get("required"))
            .and_then(|v| v.as_array())
            .expect("tx required fields");
        let tx_required_keys: Vec<&str> = tx_required.iter().filter_map(|v| v.as_str()).collect();
        assert!(tx_required_keys.contains(&"transaction_date"));
        assert!(tx_required_keys.contains(&"type"));
    }
}
