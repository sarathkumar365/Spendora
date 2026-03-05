use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

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
}
