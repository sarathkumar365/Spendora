use anyhow::{anyhow, Context};
use expense_core::{
    compute_row_hash, new_idempotency_key, ClassificationSource, ImportStatus, TransactionSource,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    ConnectOptions, Pool, Row, Sqlite,
};
use std::path::Path;
use std::str::FromStr;

pub type SqlitePool = Pool<Sqlite>;
pub const DEFAULT_EXTRACTION_MODE: &str = "managed";
pub const DEFAULT_MAX_PROVIDER_RETRIES: i64 = 3;
pub const DEFAULT_PROVIDER_TIMEOUT_MS: i64 = 180_000;
pub const MIN_PROVIDER_TIMEOUT_MS: i64 = 1_000;
pub const MAX_PROVIDER_TIMEOUT_MS: i64 = 180_000;

#[derive(Debug, Clone)]
pub struct CreateImportInput {
    pub file_name: String,
    pub parser_type: String,
    pub content_base64: String,
    pub source_hash: String,
    pub extraction_mode: Option<String>,
    pub resolved_account_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportStatusView {
    pub import_id: String,
    pub file_name: String,
    pub parser_type: String,
    pub status: String,
    pub extraction_mode: String,
    pub effective_provider: Option<String>,
    pub provider_attempts: Vec<serde_json::Value>,
    pub diagnostics: serde_json::Value,
    pub review_required_count: i64,
    pub summary: serde_json::Value,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub resolved_account_id: Option<String>,
    pub card_resolution_status: String,
    pub card_resolution_reason: Option<String>,
    pub card_resolution_metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedRowInput {
    pub row_index: i64,
    pub normalized_json: serde_json::Value,
    pub confidence: f64,
    pub parse_error: Option<String>,
    pub normalized_txn_hash: String,
    pub account_id: Option<String>,
    pub statement_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewRow {
    pub row_id: String,
    pub row_index: i64,
    pub normalized_json: serde_json::Value,
    pub direction: String,
    pub direction_confidence: Option<f64>,
    pub direction_source: String,
    pub confidence: f64,
    pub parse_error: Option<String>,
    pub approved: bool,
    pub rejection_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewDecision {
    pub row_id: String,
    pub approved: bool,
    pub rejection_reason: Option<String>,
    pub direction: Option<String>,
    pub direction_confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionListItem {
    pub id: String,
    pub account_id: String,
    pub details: Option<String>,
    pub amount: Option<String>,
    pub transaction_date: Option<String>,
    pub source: String,
    pub classification_source: String,
    pub confidence: f64,
    pub explanation: String,
    pub last_sync_at: String,
    pub import_id: Option<String>,
    pub statement_id: Option<String>,
    pub tx_type: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionQuery {
    pub q: Option<String>,
    pub account_id: Option<String>,
    pub source: Option<String>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountItem {
    pub id: String,
    pub name: String,
    pub currency_code: String,
    pub account_type: Option<String>,
    pub account_number_ending: Option<String>,
    pub customer_name: Option<String>,
    pub metadata_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct CreateAccountCardInput {
    pub name: String,
    pub currency_code: String,
    pub account_type: Option<String>,
    pub account_number_ending: Option<String>,
    pub customer_name: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct JobRun {
    pub id: String,
    pub payload_json: String,
    pub attempts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResult {
    pub inserted_count: i64,
    pub duplicate_count: i64,
}

#[derive(Debug, Clone)]
pub struct ImportBlob {
    pub parser_type: String,
    pub content_base64: String,
    pub extraction_mode: String,
    pub file_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionSettings {
    pub default_extraction_mode: String,
    pub managed_fallback_enabled: bool,
    pub max_provider_retries: i64,
    pub provider_timeout_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LlamaAgentCache {
    pub agent_id: String,
    pub schema_version: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LlamaAgentReadinessState {
    Configured,
    Missing,
    SchemaInvalid,
    ApiUnreachable,
}

impl LlamaAgentReadinessState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Configured => "configured",
            Self::Missing => "missing",
            Self::SchemaInvalid => "schema_invalid",
            Self::ApiUnreachable => "api_unreachable",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LlamaAgentReadiness {
    pub state: LlamaAgentReadinessState,
    pub agent_name: String,
    pub schema_version: String,
    pub agent_id: Option<String>,
    pub checked_at: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatementRecord {
    pub id: String,
    pub account_id: String,
    pub period_start: String,
    pub period_end: String,
    pub statement_month: Option<String>,
    pub provider_name: Option<String>,
    pub provider_job_id: Option<String>,
    pub provider_run_id: Option<String>,
    pub provider_metadata_json: serde_json::Value,
    pub schema_version: String,
    pub statement_period_start: Option<String>,
    pub statement_period_end: Option<String>,
    pub statement_date: Option<String>,
    pub account_number_ending: Option<String>,
    pub customer_name: Option<String>,
    pub payment_due_date: Option<String>,
    pub total_minimum_payment: Option<f64>,
    pub interest_charged: Option<f64>,
    pub account_balance: Option<f64>,
    pub credit_limit: Option<f64>,
    pub available_credit: Option<f64>,
    pub estimated_payoff_years: Option<i64>,
    pub estimated_payoff_months: Option<i64>,
    pub credits_total: Option<f64>,
    pub debits_total: Option<f64>,
    pub statement_payload_json: serde_json::Value,
    pub opening_balance_cents: Option<i64>,
    pub opening_balance_date: Option<String>,
    pub closing_balance_cents: Option<i64>,
    pub closing_balance_date: Option<String>,
    pub total_debits_cents: Option<i64>,
    pub total_credits_cents: Option<i64>,
    pub account_type: Option<String>,
    pub account_number_masked: Option<String>,
    pub currency_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementListItem {
    pub id: String,
    pub account_id: String,
    pub period_start: String,
    pub period_end: String,
    pub statement_month: Option<String>,
    pub provider_name: Option<String>,
    pub provider_job_id: Option<String>,
    pub provider_run_id: Option<String>,
    pub schema_version: String,
    pub linked_txn_count: i64,
    pub statement_period_start: Option<String>,
    pub statement_period_end: Option<String>,
    pub statement_date: Option<String>,
    pub account_number_ending: Option<String>,
    pub customer_name: Option<String>,
    pub payment_due_date: Option<String>,
    pub total_minimum_payment: Option<f64>,
    pub interest_charged: Option<f64>,
    pub account_balance: Option<f64>,
    pub credit_limit: Option<f64>,
    pub available_credit: Option<f64>,
    pub estimated_payoff_years: Option<i64>,
    pub estimated_payoff_months: Option<i64>,
    pub credits_total: Option<f64>,
    pub debits_total: Option<f64>,
    pub statement_payload_json: serde_json::Value,
    pub opening_balance_cents: Option<i64>,
    pub opening_balance_date: Option<String>,
    pub closing_balance_cents: Option<i64>,
    pub closing_balance_date: Option<String>,
    pub total_debits_cents: Option<i64>,
    pub total_credits_cents: Option<i64>,
    pub account_type: Option<String>,
    pub account_number_masked: Option<String>,
    pub currency_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementCoverageMonth {
    pub year: i32,
    pub month: i32,
    pub statement_exists: bool,
    pub statement_id: Option<String>,
    pub statement_month: Option<String>,
    pub period_start: Option<String>,
    pub period_end: Option<String>,
    pub linked_txn_count: i64,
    pub manual_added_txn_count: i64,
    pub statement_period_start: Option<String>,
    pub statement_period_end: Option<String>,
    pub statement_date: Option<String>,
    pub account_number_ending: Option<String>,
    pub customer_name: Option<String>,
    pub payment_due_date: Option<String>,
    pub total_minimum_payment: Option<f64>,
    pub interest_charged: Option<f64>,
    pub account_balance: Option<f64>,
    pub credit_limit: Option<f64>,
    pub available_credit: Option<f64>,
    pub estimated_payoff_years: Option<i64>,
    pub estimated_payoff_months: Option<i64>,
    pub credits_total: Option<f64>,
    pub debits_total: Option<f64>,
    pub statement_payload_json: serde_json::Value,
    pub opening_balance_cents: Option<i64>,
    pub opening_balance_date: Option<String>,
    pub closing_balance_cents: Option<i64>,
    pub closing_balance_date: Option<String>,
    pub total_debits_cents: Option<i64>,
    pub total_credits_cents: Option<i64>,
    pub account_type: Option<String>,
    pub account_number_masked: Option<String>,
    pub currency_code: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct StatementSummaryInput {
    pub statement_period_start: Option<String>,
    pub statement_period_end: Option<String>,
    pub statement_date: Option<String>,
    pub account_number_ending: Option<String>,
    pub customer_name: Option<String>,
    pub payment_due_date: Option<String>,
    pub total_minimum_payment: Option<f64>,
    pub interest_charged: Option<f64>,
    pub account_balance: Option<f64>,
    pub credit_limit: Option<f64>,
    pub available_credit: Option<f64>,
    pub estimated_payoff_years: Option<i64>,
    pub estimated_payoff_months: Option<i64>,
    pub credits_total: Option<f64>,
    pub debits_total: Option<f64>,
    pub statement_payload_json: Option<serde_json::Value>,
    pub opening_balance_cents: Option<i64>,
    pub opening_balance_date: Option<String>,
    pub closing_balance_cents: Option<i64>,
    pub closing_balance_date: Option<String>,
    pub total_debits_cents: Option<i64>,
    pub total_credits_cents: Option<i64>,
    pub account_type: Option<String>,
    pub account_number_masked: Option<String>,
    pub currency_code: Option<String>,
}

impl Default for ExtractionSettings {
    fn default() -> Self {
        Self {
            default_extraction_mode: DEFAULT_EXTRACTION_MODE.to_string(),
            managed_fallback_enabled: true,
            max_provider_retries: DEFAULT_MAX_PROVIDER_RETRIES,
            provider_timeout_ms: DEFAULT_PROVIDER_TIMEOUT_MS,
        }
    }
}

pub async fn connect(db_path: &Path) -> anyhow::Result<SqlitePool> {
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create db directory: {}", parent.display()))?;
    }

    let connect_opts =
        SqliteConnectOptions::from_str(format!("sqlite://{}", db_path.display()).as_str())?
            .create_if_missing(true)
            .disable_statement_logging();
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect_with(connect_opts)
        .await
        .with_context(|| format!("failed to connect sqlite at {}", db_path.display()))?;

    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .context("failed to enable foreign keys")?;

    Ok(pool)
}

pub async fn run_migrations(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    )
    .execute(pool)
    .await?;

    for (version, sql) in MIGRATIONS {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
        )
        .bind(*version)
        .fetch_one(pool)
        .await?;

        if exists > 0 {
            continue;
        }

        apply_migration_sql(pool, sql).await?;
        sqlx::query("INSERT INTO schema_migrations (version) VALUES (?1)")
            .bind(*version)
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn apply_migration_sql(pool: &SqlitePool, sql: &str) -> anyhow::Result<()> {
    for statement in sql
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        if let Err(error) = sqlx::query(statement).execute(pool).await {
            if is_idempotent_sqlite_error(&error) {
                continue;
            }
            return Err(error.into());
        }
    }
    Ok(())
}

fn is_idempotent_sqlite_error(error: &sqlx::Error) -> bool {
    let Some(db_error) = error.as_database_error() else {
        return false;
    };
    let message = db_error.message().to_ascii_lowercase();
    message.contains("duplicate column name")
        || message.contains("already exists")
        || message.contains("duplicate key name")
}

pub async fn ensure_default_manual_account(pool: &SqlitePool) -> anyhow::Result<String> {
    let connection_id = "manual-connection";
    let account_id = "manual-default-account";

    sqlx::query(
        "INSERT OR IGNORE INTO connections (id, provider, status, external_ref) VALUES (?1, 'manual', 'active', 'manual-local')",
    )
    .bind(connection_id)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT OR IGNORE INTO accounts (id, connection_id, name, currency_code) VALUES (?1, ?2, 'Manual Imported Account', 'CAD')",
    )
    .bind(account_id)
    .bind(connection_id)
    .execute(pool)
    .await?;

    Ok(account_id.to_string())
}

pub async fn create_import(pool: &SqlitePool, input: CreateImportInput) -> anyhow::Result<String> {
    let import_id = new_idempotency_key();
    let extraction_mode = input
        .extraction_mode
        .unwrap_or_else(|| DEFAULT_EXTRACTION_MODE.to_string());
    let card_resolution_status = if input.resolved_account_id.is_some() {
        "resolved"
    } else {
        "pending"
    };
    sqlx::query(
        "INSERT INTO imports (id, source_type, status, file_name, parser_type, source_hash, content_base64, extraction_mode, resolved_account_id, card_resolution_status, card_resolution_metadata_json, card_resolved_at, updated_at) VALUES (?1, 'manual', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, '{}', CASE WHEN ?8 IS NULL THEN NULL ELSE CURRENT_TIMESTAMP END, CURRENT_TIMESTAMP)",
    )
    .bind(&import_id)
    .bind(ImportStatus::Queued.as_str())
    .bind(&input.file_name)
    .bind(&input.parser_type)
    .bind(&input.source_hash)
    .bind(&input.content_base64)
    .bind(extraction_mode)
    .bind(input.resolved_account_id)
    .bind(card_resolution_status)
    .execute(pool)
    .await?;

    let payload = serde_json::json!({ "import_id": import_id });
    enqueue_job(pool, "import_parse", &payload.to_string()).await?;

    Ok(import_id)
}

pub async fn create_reused_import(
    pool: &SqlitePool,
    input: CreateImportInput,
    summary: &serde_json::Value,
    diagnostics: &serde_json::Value,
) -> anyhow::Result<String> {
    let import_id = new_idempotency_key();
    let extraction_mode = input
        .extraction_mode
        .unwrap_or_else(|| DEFAULT_EXTRACTION_MODE.to_string());

    let card_resolution_status = if input.resolved_account_id.is_some() {
        "resolved"
    } else {
        "pending"
    };
    sqlx::query(
        "INSERT INTO imports (id, source_type, status, file_name, parser_type, source_hash, content_base64, extraction_mode, effective_provider, provider_attempts_json, extraction_diagnostics_json, provider_attempt_count, review_required_count, summary_json, errors_json, warnings_json, resolved_account_id, card_resolution_status, card_resolution_metadata_json, card_resolved_at, committed_at, updated_at) VALUES (?1, 'manual', ?2, ?3, ?4, ?5, ?6, ?7, 'reused_db', '[]', ?8, 0, 0, ?9, '[]', '[]', ?10, ?11, '{}', CASE WHEN ?10 IS NULL THEN NULL ELSE CURRENT_TIMESTAMP END, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(&import_id)
    .bind(ImportStatus::Committed.as_str())
    .bind(&input.file_name)
    .bind(&input.parser_type)
    .bind(&input.source_hash)
    .bind(&input.content_base64)
    .bind(extraction_mode)
    .bind(diagnostics.to_string())
    .bind(summary.to_string())
    .bind(input.resolved_account_id)
    .bind(card_resolution_status)
    .execute(pool)
    .await?;

    Ok(import_id)
}

pub async fn update_import_status(
    pool: &SqlitePool,
    import_id: &str,
    status: ImportStatus,
    summary: serde_json::Value,
    errors: Vec<String>,
    warnings: Vec<String>,
    review_required_count: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE imports SET status = ?2, summary_json = ?3, errors_json = ?4, warnings_json = ?5, review_required_count = ?6, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
    )
    .bind(import_id)
    .bind(status.as_str())
    .bind(summary.to_string())
    .bind(serde_json::to_string(&errors)?)
    .bind(serde_json::to_string(&warnings)?)
    .bind(review_required_count)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_import_extraction_result(
    pool: &SqlitePool,
    import_id: &str,
    effective_provider: Option<&str>,
    provider_attempts: &[serde_json::Value],
    diagnostics: &serde_json::Value,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE imports SET effective_provider = ?2, provider_attempts_json = ?3, extraction_diagnostics_json = ?4, provider_attempt_count = ?5, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
    )
    .bind(import_id)
    .bind(effective_provider)
    .bind(serde_json::to_string(provider_attempts)?)
    .bind(diagnostics.to_string())
    .bind(provider_attempts.len() as i64)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn find_high_confidence_account_match(
    pool: &SqlitePool,
    account_type: Option<&str>,
    account_number_ending: Option<&str>,
    customer_name: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let Some(account_type) = account_type.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let Some(account_number_ending) = account_number_ending
        .map(str::trim)
        .filter(|v| !v.is_empty())
    else {
        return Ok(None);
    };
    let Some(customer_name) = customer_name.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };

    let rows = sqlx::query(
        "SELECT id FROM accounts WHERE lower(trim(COALESCE(account_type, ''))) = lower(trim(?1)) AND lower(trim(COALESCE(account_number_ending, ''))) = lower(trim(?2)) AND lower(trim(COALESCE(customer_name, ''))) = lower(trim(?3)) ORDER BY created_at ASC LIMIT 2",
    )
    .bind(account_type)
    .bind(account_number_ending)
    .bind(customer_name)
    .fetch_all(pool)
    .await?;

    if rows.len() == 1 {
        return Ok(Some(rows[0].get("id")));
    }
    Ok(None)
}

pub async fn set_import_card_resolution(
    pool: &SqlitePool,
    import_id: &str,
    resolved_account_id: Option<&str>,
    reason: Option<&str>,
    metadata: &serde_json::Value,
) -> anyhow::Result<()> {
    let status = if resolved_account_id.is_some() {
        "resolved"
    } else {
        "pending"
    };

    sqlx::query(
        "UPDATE imports SET resolved_account_id = ?2, card_resolution_status = ?3, card_resolution_reason = ?4, card_resolution_metadata_json = ?5, card_resolved_at = CASE WHEN ?2 IS NULL THEN NULL ELSE CURRENT_TIMESTAMP END, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
    )
    .bind(import_id)
    .bind(resolved_account_id)
    .bind(status)
    .bind(reason)
    .bind(metadata.to_string())
    .execute(pool)
    .await?;

    if let Some(account_id) = resolved_account_id {
        sqlx::query(
            "UPDATE import_rows SET account_id = ?2 WHERE import_id = ?1 AND account_id IS NULL",
        )
        .bind(import_id)
        .bind(account_id)
        .execute(pool)
        .await?;
    }

    Ok(())
}

pub async fn create_account_card(
    pool: &SqlitePool,
    input: CreateAccountCardInput,
) -> anyhow::Result<AccountItem> {
    let connection_id = "manual-connection";
    sqlx::query(
        "INSERT OR IGNORE INTO connections (id, provider, status, external_ref) VALUES (?1, 'manual', 'active', 'manual-local')",
    )
    .bind(connection_id)
    .execute(pool)
    .await?;

    let account_id = new_idempotency_key();
    sqlx::query(
        "INSERT INTO accounts (id, connection_id, name, currency_code, account_type, account_number_ending, customer_name, metadata_json, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP)",
    )
    .bind(&account_id)
    .bind(connection_id)
    .bind(input.name.trim())
    .bind(if input.currency_code.trim().is_empty() {
        "CAD".to_string()
    } else {
        input.currency_code.trim().to_string()
    })
    .bind(input.account_type.and_then(trim_optional))
    .bind(input.account_number_ending.and_then(trim_optional))
    .bind(input.customer_name.and_then(trim_optional))
    .bind(
        input
            .metadata_json
            .unwrap_or_else(|| serde_json::json!({}))
            .to_string(),
    )
    .execute(pool)
    .await?;

    let row = sqlx::query(
        "SELECT id, name, currency_code, account_type, account_number_ending, customer_name, metadata_json FROM accounts WHERE id = ?1",
    )
    .bind(&account_id)
    .fetch_one(pool)
    .await?;

    Ok(AccountItem {
        id: row.get("id"),
        name: row.get("name"),
        currency_code: row.get("currency_code"),
        account_type: row.get("account_type"),
        account_number_ending: row.get("account_number_ending"),
        customer_name: row.get("customer_name"),
        metadata_json: serde_json::from_str(row.get::<String, _>("metadata_json").as_str())
            .unwrap_or_else(|_| serde_json::json!({})),
    })
}

fn trim_optional(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub async fn get_extraction_settings(pool: &SqlitePool) -> anyhow::Result<ExtractionSettings> {
    let row = sqlx::query("SELECT value_json FROM app_settings WHERE key = 'extraction_settings'")
        .fetch_optional(pool)
        .await?;

    let Some(row) = row else {
        return Ok(ExtractionSettings::default());
    };

    let raw: String = row.get("value_json");
    let mut value: ExtractionSettings =
        serde_json::from_str(&raw).unwrap_or_else(|_| ExtractionSettings::default());

    if value.max_provider_retries < 1 {
        value.max_provider_retries = 1;
    } else if value.max_provider_retries > DEFAULT_MAX_PROVIDER_RETRIES {
        value.max_provider_retries = DEFAULT_MAX_PROVIDER_RETRIES;
    }
    value.provider_timeout_ms = value
        .provider_timeout_ms
        .clamp(MIN_PROVIDER_TIMEOUT_MS, MAX_PROVIDER_TIMEOUT_MS);

    Ok(value)
}

pub async fn upsert_extraction_settings(
    pool: &SqlitePool,
    mut settings: ExtractionSettings,
) -> anyhow::Result<ExtractionSettings> {
    if settings.max_provider_retries < 1 {
        settings.max_provider_retries = 1;
    } else if settings.max_provider_retries > DEFAULT_MAX_PROVIDER_RETRIES {
        settings.max_provider_retries = DEFAULT_MAX_PROVIDER_RETRIES;
    }
    settings.provider_timeout_ms = settings
        .provider_timeout_ms
        .clamp(MIN_PROVIDER_TIMEOUT_MS, MAX_PROVIDER_TIMEOUT_MS);

    sqlx::query(
        "INSERT INTO app_settings (key, value_json, updated_at) VALUES ('extraction_settings', ?1, CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = CURRENT_TIMESTAMP",
    )
    .bind(serde_json::to_string(&settings)?)
    .execute(pool)
    .await?;

    Ok(settings)
}

pub async fn get_llama_agent_cache(pool: &SqlitePool) -> anyhow::Result<Option<LlamaAgentCache>> {
    let row = sqlx::query("SELECT value_json FROM app_settings WHERE key = 'llama_agent_cache'")
        .fetch_optional(pool)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let raw: String = row.get("value_json");
    let parsed: LlamaAgentCache = serde_json::from_str(&raw)
        .map_err(|e| anyhow!("invalid llama_agent_cache payload: {e}"))?;
    Ok(Some(parsed))
}

pub async fn upsert_llama_agent_cache(
    pool: &SqlitePool,
    agent_id: &str,
    schema_version: &str,
) -> anyhow::Result<LlamaAgentCache> {
    let payload = LlamaAgentCache {
        agent_id: agent_id.to_string(),
        schema_version: schema_version.to_string(),
        updated_at: chrono::Utc::now().to_rfc3339(),
    };

    sqlx::query(
        "INSERT INTO app_settings (key, value_json, updated_at) VALUES ('llama_agent_cache', ?1, CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = CURRENT_TIMESTAMP",
    )
    .bind(serde_json::to_string(&payload)?)
    .execute(pool)
    .await?;

    Ok(payload)
}

pub async fn get_llama_agent_readiness(
    pool: &SqlitePool,
) -> anyhow::Result<Option<LlamaAgentReadiness>> {
    let row =
        sqlx::query("SELECT value_json FROM app_settings WHERE key = 'llama_agent_readiness'")
            .fetch_optional(pool)
            .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let raw: String = row.get("value_json");
    let parsed: LlamaAgentReadiness = serde_json::from_str(&raw)
        .map_err(|e| anyhow!("invalid llama_agent_readiness payload: {e}"))?;
    Ok(Some(parsed))
}

pub async fn upsert_llama_agent_readiness(
    pool: &SqlitePool,
    readiness: &LlamaAgentReadiness,
) -> anyhow::Result<LlamaAgentReadiness> {
    sqlx::query(
        "INSERT INTO app_settings (key, value_json, updated_at) VALUES ('llama_agent_readiness', ?1, CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = CURRENT_TIMESTAMP",
    )
    .bind(serde_json::to_string(readiness)?)
    .execute(pool)
    .await?;

    Ok(readiness.clone())
}

pub async fn get_statement_by_account_period(
    pool: &SqlitePool,
    account_id: &str,
    period_start: &str,
    period_end: &str,
) -> anyhow::Result<Option<StatementRecord>> {
    let row = sqlx::query(
        "SELECT id, account_id, period_start, period_end, statement_month, provider_name, provider_job_id, provider_run_id, provider_metadata_json, schema_version, statement_period_start, statement_period_end, statement_date, account_number_ending, customer_name, payment_due_date, total_minimum_payment, interest_charged, account_balance, credit_limit, available_credit, estimated_payoff_years, estimated_payoff_months, credits_total, debits_total, statement_payload_json, opening_balance_cents, opening_balance_date, closing_balance_cents, closing_balance_date, total_debits_cents, total_credits_cents, account_type, account_number_masked, currency_code FROM statements WHERE account_id = ?1 AND period_start = ?2 AND period_end = ?3",
    )
    .bind(account_id)
    .bind(period_start)
    .bind(period_end)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let metadata_raw: String = row.get("provider_metadata_json");
    let payload_raw: String = row.get("statement_payload_json");
    Ok(Some(StatementRecord {
        id: row.get("id"),
        account_id: row.get("account_id"),
        period_start: row.get("period_start"),
        period_end: row.get("period_end"),
        statement_month: row.get("statement_month"),
        provider_name: row.get("provider_name"),
        provider_job_id: row.get("provider_job_id"),
        provider_run_id: row.get("provider_run_id"),
        provider_metadata_json: serde_json::from_str(metadata_raw.as_str())
            .unwrap_or_else(|_| serde_json::json!({})),
        schema_version: row.get("schema_version"),
        statement_period_start: row.get("statement_period_start"),
        statement_period_end: row.get("statement_period_end"),
        statement_date: row.get("statement_date"),
        account_number_ending: row.get("account_number_ending"),
        customer_name: row.get("customer_name"),
        payment_due_date: row.get("payment_due_date"),
        total_minimum_payment: row.get("total_minimum_payment"),
        interest_charged: row.get("interest_charged"),
        account_balance: row.get("account_balance"),
        credit_limit: row.get("credit_limit"),
        available_credit: row.get("available_credit"),
        estimated_payoff_years: row.get("estimated_payoff_years"),
        estimated_payoff_months: row.get("estimated_payoff_months"),
        credits_total: row.get("credits_total"),
        debits_total: row.get("debits_total"),
        statement_payload_json: serde_json::from_str(payload_raw.as_str())
            .unwrap_or_else(|_| serde_json::json!({})),
        opening_balance_cents: row.get("opening_balance_cents"),
        opening_balance_date: row.get("opening_balance_date"),
        closing_balance_cents: row.get("closing_balance_cents"),
        closing_balance_date: row.get("closing_balance_date"),
        total_debits_cents: row.get("total_debits_cents"),
        total_credits_cents: row.get("total_credits_cents"),
        account_type: row.get("account_type"),
        account_number_masked: row.get("account_number_masked"),
        currency_code: row.get("currency_code"),
    }))
}

pub async fn upsert_or_get_statement(
    pool: &SqlitePool,
    account_id: &str,
    period_start: &str,
    period_end: &str,
    statement_month: Option<&str>,
    provider_name: Option<&str>,
    provider_job_id: Option<&str>,
    provider_run_id: Option<&str>,
    provider_metadata_json: &serde_json::Value,
    schema_version: &str,
    summary: StatementSummaryInput,
) -> anyhow::Result<StatementRecord> {
    if let Some(existing) =
        get_statement_by_account_period(pool, account_id, period_start, period_end).await?
    {
        return Ok(existing);
    }

    let statement_id = new_idempotency_key();
    sqlx::query(
        "INSERT INTO statements (id, account_id, period_start, period_end, statement_month, provider_name, provider_job_id, provider_run_id, provider_metadata_json, schema_version, statement_period_start, statement_period_end, statement_date, account_number_ending, customer_name, payment_due_date, total_minimum_payment, interest_charged, account_balance, credit_limit, available_credit, estimated_payoff_years, estimated_payoff_months, credits_total, debits_total, statement_payload_json, opening_balance_cents, opening_balance_date, closing_balance_cents, closing_balance_date, total_debits_cents, total_credits_cents, account_type, account_number_masked, currency_code, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, CURRENT_TIMESTAMP)",
    )
    .bind(&statement_id)
    .bind(account_id)
    .bind(period_start)
    .bind(period_end)
    .bind(statement_month)
    .bind(provider_name)
    .bind(provider_job_id)
    .bind(provider_run_id)
    .bind(provider_metadata_json.to_string())
    .bind(schema_version)
    .bind(summary.statement_period_start)
    .bind(summary.statement_period_end)
    .bind(summary.statement_date)
    .bind(summary.account_number_ending)
    .bind(summary.customer_name)
    .bind(summary.payment_due_date)
    .bind(summary.total_minimum_payment)
    .bind(summary.interest_charged)
    .bind(summary.account_balance)
    .bind(summary.credit_limit)
    .bind(summary.available_credit)
    .bind(summary.estimated_payoff_years)
    .bind(summary.estimated_payoff_months)
    .bind(summary.credits_total)
    .bind(summary.debits_total)
    .bind(
        summary
            .statement_payload_json
            .unwrap_or_else(|| serde_json::json!({}))
            .to_string(),
    )
    .bind(summary.opening_balance_cents)
    .bind(summary.opening_balance_date)
    .bind(summary.closing_balance_cents)
    .bind(summary.closing_balance_date)
    .bind(summary.total_debits_cents)
    .bind(summary.total_credits_cents)
    .bind(summary.account_type)
    .bind(summary.account_number_masked)
    .bind(summary.currency_code)
    .execute(pool)
    .await?;

    get_statement_by_account_period(pool, account_id, period_start, period_end)
        .await?
        .ok_or_else(|| anyhow!("statement upsert load failed"))
}

pub async fn list_statements_for_account(
    pool: &SqlitePool,
    account_id: &str,
    year: Option<i32>,
    month: Option<i32>,
    date_from: Option<&str>,
    date_to: Option<&str>,
) -> anyhow::Result<Vec<StatementListItem>> {
    let mut sql = String::from(
        "SELECT s.id, s.account_id, s.period_start, s.period_end, s.statement_month, s.provider_name, s.provider_job_id, s.provider_run_id, s.schema_version, s.statement_period_start, s.statement_period_end, s.statement_date, s.account_number_ending, s.customer_name, s.payment_due_date, s.total_minimum_payment, s.interest_charged, s.account_balance, s.credit_limit, s.available_credit, s.estimated_payoff_years, s.estimated_payoff_months, s.credits_total, s.debits_total, s.statement_payload_json, s.opening_balance_cents, s.opening_balance_date, s.closing_balance_cents, s.closing_balance_date, s.total_debits_cents, s.total_credits_cents, s.account_type, s.account_number_masked, s.currency_code, COALESCE(COUNT(t.id), 0) AS linked_txn_count FROM statements s LEFT JOIN transactions t ON t.statement_id = s.id WHERE s.account_id = ?",
    );
    let mut binds: Vec<String> = vec![account_id.to_string()];

    if let Some(y) = year {
        sql.push_str(" AND substr(COALESCE(s.statement_month, s.period_start), 1, 4) = ?");
        binds.push(format!("{y:04}"));
    }
    if let Some(m) = month {
        sql.push_str(" AND substr(COALESCE(s.statement_month, s.period_start), 6, 2) = ?");
        binds.push(format!("{m:02}"));
    }
    if let Some(from) = date_from {
        sql.push_str(" AND date(s.period_end) >= date(?)");
        binds.push(from.to_string());
    }
    if let Some(to) = date_to {
        sql.push_str(" AND date(s.period_start) <= date(?)");
        binds.push(to.to_string());
    }

    sql.push_str(
        " GROUP BY s.id, s.account_id, s.period_start, s.period_end, s.statement_month, s.provider_name, s.provider_job_id, s.provider_run_id, s.schema_version, s.statement_period_start, s.statement_period_end, s.statement_date, s.account_number_ending, s.customer_name, s.payment_due_date, s.total_minimum_payment, s.interest_charged, s.account_balance, s.credit_limit, s.available_credit, s.estimated_payoff_years, s.estimated_payoff_months, s.credits_total, s.debits_total, s.statement_payload_json, s.opening_balance_cents, s.opening_balance_date, s.closing_balance_cents, s.closing_balance_date, s.total_debits_cents, s.total_credits_cents, s.account_type, s.account_number_masked, s.currency_code ORDER BY s.period_start DESC, s.period_end DESC",
    );

    let mut query = sqlx::query(&sql);
    for bind in binds {
        query = query.bind(bind);
    }
    let rows = query.fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|row| StatementListItem {
            id: row.get("id"),
            account_id: row.get("account_id"),
            period_start: row.get("period_start"),
            period_end: row.get("period_end"),
            statement_month: row.get("statement_month"),
            provider_name: row.get("provider_name"),
            provider_job_id: row.get("provider_job_id"),
            provider_run_id: row.get("provider_run_id"),
            schema_version: row.get("schema_version"),
            linked_txn_count: row.get("linked_txn_count"),
            statement_period_start: row.get("statement_period_start"),
            statement_period_end: row.get("statement_period_end"),
            statement_date: row.get("statement_date"),
            account_number_ending: row.get("account_number_ending"),
            customer_name: row.get("customer_name"),
            payment_due_date: row.get("payment_due_date"),
            total_minimum_payment: row.get("total_minimum_payment"),
            interest_charged: row.get("interest_charged"),
            account_balance: row.get("account_balance"),
            credit_limit: row.get("credit_limit"),
            available_credit: row.get("available_credit"),
            estimated_payoff_years: row.get("estimated_payoff_years"),
            estimated_payoff_months: row.get("estimated_payoff_months"),
            credits_total: row.get("credits_total"),
            debits_total: row.get("debits_total"),
            statement_payload_json: serde_json::from_str(
                row.get::<String, _>("statement_payload_json").as_str(),
            )
            .unwrap_or_else(|_| serde_json::json!({})),
            opening_balance_cents: row.get("opening_balance_cents"),
            opening_balance_date: row.get("opening_balance_date"),
            closing_balance_cents: row.get("closing_balance_cents"),
            closing_balance_date: row.get("closing_balance_date"),
            total_debits_cents: row.get("total_debits_cents"),
            total_credits_cents: row.get("total_credits_cents"),
            account_type: row.get("account_type"),
            account_number_masked: row.get("account_number_masked"),
            currency_code: row.get("currency_code"),
        })
        .collect())
}

pub async fn get_statement_coverage(
    pool: &SqlitePool,
    account_id: &str,
    year: Option<i32>,
    month: Option<i32>,
) -> anyhow::Result<Vec<StatementCoverageMonth>> {
    let statements = list_statements_for_account(pool, account_id, None, None, None, None).await?;
    let mut by_month: std::collections::BTreeMap<(i32, i32), StatementCoverageMonth> =
        std::collections::BTreeMap::new();

    for statement in statements {
        let month_token = statement.statement_month.clone().or_else(|| {
            statement
                .period_start
                .get(..7)
                .map(|value| value.to_string())
        });
        let Some((y, m)) = month_token.as_deref().and_then(parse_year_month_safe) else {
            continue;
        };
        let entry = by_month
            .entry((y, m))
            .or_insert_with(|| StatementCoverageMonth {
                year: y,
                month: m,
                statement_exists: true,
                statement_id: Some(statement.id.clone()),
                statement_month: statement.statement_month.clone(),
                period_start: Some(statement.period_start.clone()),
                period_end: Some(statement.period_end.clone()),
                linked_txn_count: 0,
                manual_added_txn_count: 0,
                statement_period_start: statement.statement_period_start.clone(),
                statement_period_end: statement.statement_period_end.clone(),
                statement_date: statement.statement_date.clone(),
                account_number_ending: statement.account_number_ending.clone(),
                customer_name: statement.customer_name.clone(),
                payment_due_date: statement.payment_due_date.clone(),
                total_minimum_payment: statement.total_minimum_payment,
                interest_charged: statement.interest_charged,
                account_balance: statement.account_balance,
                credit_limit: statement.credit_limit,
                available_credit: statement.available_credit,
                estimated_payoff_years: statement.estimated_payoff_years,
                estimated_payoff_months: statement.estimated_payoff_months,
                credits_total: statement.credits_total,
                debits_total: statement.debits_total,
                statement_payload_json: statement.statement_payload_json.clone(),
                opening_balance_cents: statement.opening_balance_cents,
                opening_balance_date: statement.opening_balance_date.clone(),
                closing_balance_cents: statement.closing_balance_cents,
                closing_balance_date: statement.closing_balance_date.clone(),
                total_debits_cents: statement.total_debits_cents,
                total_credits_cents: statement.total_credits_cents,
                account_type: statement.account_type.clone(),
                account_number_masked: statement.account_number_masked.clone(),
                currency_code: statement.currency_code.clone(),
            });
        entry.statement_exists = true;
        entry.linked_txn_count += statement.linked_txn_count;
        if entry.statement_id.is_none() {
            entry.statement_id = Some(statement.id.clone());
        }
        if entry.statement_month.is_none() {
            entry.statement_month = statement.statement_month.clone();
        }
        if let Some(existing) = entry.period_start.clone() {
            if statement.period_start < existing {
                entry.period_start = Some(statement.period_start.clone());
            }
        }
        if let Some(existing) = entry.period_end.clone() {
            if statement.period_end > existing {
                entry.period_end = Some(statement.period_end.clone());
            }
        }
        if entry.opening_balance_cents.is_none() {
            entry.opening_balance_cents = statement.opening_balance_cents;
        }
        if entry.opening_balance_date.is_none() {
            entry.opening_balance_date = statement.opening_balance_date.clone();
        }
        if entry.closing_balance_cents.is_none() {
            entry.closing_balance_cents = statement.closing_balance_cents;
        }
        if entry.closing_balance_date.is_none() {
            entry.closing_balance_date = statement.closing_balance_date.clone();
        }
        if entry.total_debits_cents.is_none() {
            entry.total_debits_cents = statement.total_debits_cents;
        }
        if entry.total_credits_cents.is_none() {
            entry.total_credits_cents = statement.total_credits_cents;
        }
        if entry.account_type.is_none() {
            entry.account_type = statement.account_type.clone();
        }
        if entry.statement_period_start.is_none() {
            entry.statement_period_start = statement.statement_period_start.clone();
        }
        if entry.statement_period_end.is_none() {
            entry.statement_period_end = statement.statement_period_end.clone();
        }
        if entry.statement_date.is_none() {
            entry.statement_date = statement.statement_date.clone();
        }
        if entry.account_number_ending.is_none() {
            entry.account_number_ending = statement.account_number_ending.clone();
        }
        if entry.customer_name.is_none() {
            entry.customer_name = statement.customer_name.clone();
        }
        if entry.payment_due_date.is_none() {
            entry.payment_due_date = statement.payment_due_date.clone();
        }
        if entry.total_minimum_payment.is_none() {
            entry.total_minimum_payment = statement.total_minimum_payment;
        }
        if entry.interest_charged.is_none() {
            entry.interest_charged = statement.interest_charged;
        }
        if entry.account_balance.is_none() {
            entry.account_balance = statement.account_balance;
        }
        if entry.credit_limit.is_none() {
            entry.credit_limit = statement.credit_limit;
        }
        if entry.available_credit.is_none() {
            entry.available_credit = statement.available_credit;
        }
        if entry.estimated_payoff_years.is_none() {
            entry.estimated_payoff_years = statement.estimated_payoff_years;
        }
        if entry.estimated_payoff_months.is_none() {
            entry.estimated_payoff_months = statement.estimated_payoff_months;
        }
        if entry.credits_total.is_none() {
            entry.credits_total = statement.credits_total;
        }
        if entry.debits_total.is_none() {
            entry.debits_total = statement.debits_total;
        }
        if entry.account_number_masked.is_none() {
            entry.account_number_masked = statement.account_number_masked.clone();
        }
        if entry.currency_code.is_none() {
            entry.currency_code = statement.currency_code.clone();
        }
    }

    // TODO(step4): Bucket manual rows by booked_at so backfilled entries land in
    // the statement month they belong to instead of the month they were created.
    let manual_rows = sqlx::query(
        "SELECT CAST(strftime('%Y', created_at) AS INTEGER) AS y, CAST(strftime('%m', created_at) AS INTEGER) AS m, COUNT(*) AS cnt FROM transactions WHERE account_id = ?1 AND statement_id IS NULL GROUP BY y, m",
    )
    .bind(account_id)
    .fetch_all(pool)
    .await?;

    for row in manual_rows {
        let y: i32 = row.get("y");
        let m: i32 = row.get("m");
        let cnt: i64 = row.get("cnt");
        let entry = by_month
            .entry((y, m))
            .or_insert_with(|| StatementCoverageMonth {
                year: y,
                month: m,
                statement_exists: false,
                statement_id: None,
                statement_month: None,
                period_start: None,
                period_end: None,
                linked_txn_count: 0,
                manual_added_txn_count: 0,
                statement_period_start: None,
                statement_period_end: None,
                statement_date: None,
                account_number_ending: None,
                customer_name: None,
                payment_due_date: None,
                total_minimum_payment: None,
                interest_charged: None,
                account_balance: None,
                credit_limit: None,
                available_credit: None,
                estimated_payoff_years: None,
                estimated_payoff_months: None,
                credits_total: None,
                debits_total: None,
                statement_payload_json: serde_json::json!({}),
                opening_balance_cents: None,
                opening_balance_date: None,
                closing_balance_cents: None,
                closing_balance_date: None,
                total_debits_cents: None,
                total_credits_cents: None,
                account_type: None,
                account_number_masked: None,
                currency_code: None,
            });
        entry.manual_added_txn_count = cnt;
    }

    Ok(by_month
        .into_values()
        .filter(|item| year.map(|v| v == item.year).unwrap_or(true))
        .filter(|item| month.map(|v| v == item.month).unwrap_or(true))
        .collect())
}

pub async fn list_transactions_for_statement(
    pool: &SqlitePool,
    statement_id: &str,
) -> anyhow::Result<Vec<TransactionListItem>> {
    let rows = sqlx::query(
        "SELECT t.id, t.account_id, COALESCE(t.details, t.description) AS details, COALESCE(t.amount, printf('%.2f', t.amount_cents / 100.0)) AS amount, COALESCE(t.transaction_date, t.booked_at) AS transaction_date, t.source, COALESCE(t.classification_source, 'manual') AS classification_source, COALESCE(t.confidence, 1.0) AS confidence, COALESCE(t.explanation, 'Imported transaction') AS explanation, t.updated_at AS last_sync_at, (SELECT ir.import_id FROM import_rows ir WHERE ir.normalized_txn_hash = t.external_txn_id LIMIT 1) AS import_id, t.statement_id, COALESCE(t.type, t.direction) AS type FROM transactions t WHERE t.statement_id = ?1 ORDER BY COALESCE(t.transaction_date, t.booked_at) DESC, t.created_at DESC",
    )
    .bind(statement_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| TransactionListItem {
            id: row.get("id"),
            account_id: row.get("account_id"),
            details: row.get("details"),
            amount: row.get("amount"),
            transaction_date: row.get("transaction_date"),
            source: row.get("source"),
            classification_source: row.get("classification_source"),
            confidence: row.get("confidence"),
            explanation: row.get("explanation"),
            last_sync_at: row.get("last_sync_at"),
            import_id: row.get("import_id"),
            statement_id: row.get("statement_id"),
            tx_type: row.get("type"),
        })
        .collect())
}

fn parse_year_month(input: &str) -> anyhow::Result<(i32, i32)> {
    let mut parts = input.split('-');
    let year = parts
        .next()
        .ok_or_else(|| anyhow!("missing year token"))?
        .parse::<i32>()?;
    let month = parts
        .next()
        .ok_or_else(|| anyhow!("missing month token"))?
        .parse::<i32>()?;
    if !(1..=12).contains(&month) {
        return Err(anyhow!("month out of range"));
    }
    Ok((year, month))
}

fn parse_year_month_safe(input: &str) -> Option<(i32, i32)> {
    parse_year_month(input).ok()
}

pub async fn get_import_content(pool: &SqlitePool, import_id: &str) -> anyhow::Result<ImportBlob> {
    let row = sqlx::query(
        "SELECT parser_type, content_base64, extraction_mode, file_name FROM imports WHERE id = ?1",
    )
    .bind(import_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow!("import not found"))?;

    Ok(ImportBlob {
        parser_type: row.get("parser_type"),
        content_base64: row.get("content_base64"),
        extraction_mode: row.get("extraction_mode"),
        file_name: row.get("file_name"),
    })
}

pub async fn clear_import_rows(pool: &SqlitePool, import_id: &str) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM import_rows WHERE import_id = ?1")
        .bind(import_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_import_rows(
    pool: &SqlitePool,
    import_id: &str,
    rows: Vec<ParsedRowInput>,
) -> anyhow::Result<()> {
    for row in rows {
        sqlx::query(
            "INSERT INTO import_rows (id, import_id, row_index, normalized_json, confidence, parse_error, normalized_txn_hash, approved, rejection_reason, account_id, statement_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )
        .bind(new_idempotency_key())
        .bind(import_id)
        .bind(row.row_index)
        .bind(row.normalized_json.to_string())
        .bind(row.confidence)
        .bind(row.parse_error)
        .bind(row.normalized_txn_hash)
        .bind(1_i64)
        .bind(Option::<String>::None)
        .bind(row.account_id)
        .bind(row.statement_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn get_import_status(
    pool: &SqlitePool,
    import_id: &str,
) -> anyhow::Result<ImportStatusView> {
    let row = sqlx::query(
        "SELECT id, file_name, parser_type, status, extraction_mode, effective_provider, provider_attempts_json, extraction_diagnostics_json, review_required_count, summary_json, errors_json, warnings_json, resolved_account_id, card_resolution_status, card_resolution_reason, card_resolution_metadata_json FROM imports WHERE id = ?1",
    )
    .bind(import_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow!("import not found"))?;

    Ok(ImportStatusView {
        import_id: row.get("id"),
        file_name: row.get("file_name"),
        parser_type: row.get("parser_type"),
        status: row.get("status"),
        extraction_mode: row
            .get::<Option<String>, _>("extraction_mode")
            .unwrap_or_else(|| DEFAULT_EXTRACTION_MODE.to_string()),
        effective_provider: row.get("effective_provider"),
        provider_attempts: serde_json::from_str(
            row.get::<String, _>("provider_attempts_json").as_str(),
        )
        .unwrap_or_else(|_| Vec::new()),
        diagnostics: serde_json::from_str(
            row.get::<String, _>("extraction_diagnostics_json").as_str(),
        )
        .unwrap_or_else(|_| serde_json::json!({})),
        review_required_count: row.get("review_required_count"),
        summary: serde_json::from_str(row.get::<String, _>("summary_json").as_str())
            .unwrap_or_else(|_| serde_json::json!({})),
        errors: serde_json::from_str(row.get::<String, _>("errors_json").as_str())
            .unwrap_or_else(|_| Vec::new()),
        warnings: serde_json::from_str(row.get::<String, _>("warnings_json").as_str())
            .unwrap_or_else(|_| Vec::new()),
        resolved_account_id: row.get("resolved_account_id"),
        card_resolution_status: row
            .get::<Option<String>, _>("card_resolution_status")
            .unwrap_or_else(|| "pending".to_string()),
        card_resolution_reason: row.get("card_resolution_reason"),
        card_resolution_metadata: serde_json::from_str(
            row.get::<String, _>("card_resolution_metadata_json")
                .as_str(),
        )
        .unwrap_or_else(|_| serde_json::json!({})),
    })
}

pub async fn list_import_rows_for_review(
    pool: &SqlitePool,
    import_id: &str,
) -> anyhow::Result<Vec<ReviewRow>> {
    let rows = sqlx::query(
        "SELECT id, row_index, normalized_json, confidence, parse_error, approved, rejection_reason FROM import_rows WHERE import_id = ?1 ORDER BY row_index ASC",
    )
    .bind(import_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let normalized_json: serde_json::Value =
                serde_json::from_str(row.get::<String, _>("normalized_json").as_str())
                    .unwrap_or_else(|_| serde_json::json!({}));
            ReviewRow {
                row_id: row.get("id"),
                row_index: row.get("row_index"),
                direction: normalized_json
                    .get("type")
                    .and_then(|v| v.as_str())
                    .or_else(|| normalized_json.get("direction").and_then(|v| v.as_str()))
                    .unwrap_or("unknown")
                    .to_string(),
                direction_confidence: normalized_json
                    .get("type_confidence")
                    .and_then(|v| v.as_f64()),
                direction_source: normalized_json
                    .get("type_source")
                    .and_then(|v| v.as_str())
                    .or_else(|| {
                        normalized_json
                            .get("direction_source")
                            .and_then(|v| v.as_str())
                    })
                    .unwrap_or("model")
                    .to_string(),
                normalized_json,
                confidence: row.get("confidence"),
                parse_error: row.get("parse_error"),
                approved: row.get::<i64, _>("approved") == 1,
                rejection_reason: row.get("rejection_reason"),
            }
        })
        .collect())
}

pub async fn apply_review_decisions(
    pool: &SqlitePool,
    import_id: &str,
    decisions: &[ReviewDecision],
) -> anyhow::Result<()> {
    fn is_supported_direction(value: &str) -> bool {
        matches!(value, "debit" | "credit" | "unknown")
    }

    for decision in decisions {
        let row =
            sqlx::query("SELECT normalized_json FROM import_rows WHERE import_id = ?1 AND id = ?2")
                .bind(import_id)
                .bind(&decision.row_id)
                .fetch_optional(pool)
                .await?;

        let normalized_json = if let Some(existing) = row {
            let raw: String = existing.get("normalized_json");
            let mut payload: serde_json::Value =
                serde_json::from_str(raw.as_str()).unwrap_or_else(|_| serde_json::json!({}));
            if let Some(direction) = decision.direction.as_ref() {
                if !is_supported_direction(direction.as_str()) {
                    return Err(anyhow!("invalid direction value: {}", direction));
                }
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert("type".to_string(), serde_json::json!(direction));
                    obj.insert("type_source".to_string(), serde_json::json!("manual"));
                    if let Some(confidence) = decision.direction_confidence {
                        obj.insert("type_confidence".to_string(), serde_json::json!(confidence));
                    } else {
                        obj.remove("type_confidence");
                    }
                }
            }
            payload.to_string()
        } else {
            "{}".to_string()
        };

        sqlx::query(
            "UPDATE import_rows SET approved = ?3, rejection_reason = ?4, normalized_json = ?5 WHERE import_id = ?1 AND id = ?2",
        )
        .bind(import_id)
        .bind(&decision.row_id)
        .bind(1_i64)
        .bind(Option::<String>::None)
        .bind(normalized_json)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn commit_import_rows(
    pool: &SqlitePool,
    import_id: &str,
) -> anyhow::Result<CommitResult> {
    let mut tx = pool.begin().await?;

    let import_row = sqlx::query(
        "SELECT resolved_account_id, card_resolution_status, extraction_diagnostics_json FROM imports WHERE id = ?1",
    )
    .bind(import_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| anyhow!("import not found"))?;

    let mut resolved_account_id = import_row.get::<Option<String>, _>("resolved_account_id");
    let card_resolution_status = import_row
        .get::<Option<String>, _>("card_resolution_status")
        .unwrap_or_else(|| "pending".to_string());
    let diagnostics: serde_json::Value = serde_json::from_str(
        import_row
            .get::<String, _>("extraction_diagnostics_json")
            .as_str(),
    )
    .unwrap_or_else(|_| serde_json::json!({}));

    let rows = sqlx::query(
        "SELECT id, normalized_json, normalized_txn_hash, confidence, account_id, statement_id FROM import_rows WHERE import_id = ?1",
    )
    .bind(import_id)
    .fetch_all(&mut *tx)
    .await?;
    if resolved_account_id.is_none() {
        let mut distinct = rows
            .iter()
            .filter_map(|row| row.get::<Option<String>, _>("account_id"))
            .collect::<std::collections::BTreeSet<_>>();
        if distinct.len() == 1 {
            let single = distinct.pop_first().expect("len checked");
            resolved_account_id = Some(single.clone());
            sqlx::query(
                "UPDATE imports SET resolved_account_id = ?2, card_resolution_status = 'resolved', card_resolution_reason = 'legacy_row_account', card_resolved_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
            )
            .bind(import_id)
            .bind(single)
            .execute(&mut *tx)
            .await?;
        }
    }
    if card_resolution_status != "resolved" && resolved_account_id.is_none() {
        return Err(anyhow!("IMPORT_CARD_RESOLUTION_REQUIRED"));
    }
    let resolved_account_id =
        resolved_account_id.ok_or_else(|| anyhow!("IMPORT_CARD_RESOLUTION_REQUIRED"))?;

    let import_statement_id =
        resolve_statement_for_import(&mut tx, &resolved_account_id, &diagnostics).await?;

    let mut inserted_count = 0_i64;
    let mut duplicate_count = 0_i64;

    for row in rows {
        let normalized_json: String = row.get("normalized_json");
        let payload: serde_json::Value = serde_json::from_str(&normalized_json)?;
        let account_id = row
            .get::<Option<String>, _>("account_id")
            .unwrap_or_else(|| resolved_account_id.clone());

        let details = payload
            .get("details")
            .and_then(|v| v.as_str())
            .filter(|v| !v.trim().is_empty())
            .or_else(|| payload.get("description").and_then(|v| v.as_str()))
            .map(|v| v.to_string());
        let amount = payload
            .get("amount")
            .and_then(|v| v.as_f64())
            .map(|v| format!("{v:.2}"))
            .or_else(|| {
                payload
                    .get("amount_cents")
                    .and_then(|v| v.as_i64())
                    .map(|v| format!("{:.2}", v as f64 / 100.0))
            });
        let amount_cents = amount
            .as_deref()
            .and_then(|v| v.parse::<f64>().ok())
            .map(|v| (v * 100.0).round() as i64)
            .or_else(|| payload.get("amount_cents").and_then(|v| v.as_i64()))
            .unwrap_or(0);
        let transaction_date = payload
            .get("transaction_date")
            .and_then(|v| v.as_str())
            .filter(|v| is_valid_iso_date(v))
            .or_else(|| {
                payload
                    .get("booked_at")
                    .and_then(|v| v.as_str())
                    .filter(|v| is_valid_iso_date(v))
            })
            .map(|v| v.to_string());
        let tx_type = payload
            .get("type")
            .and_then(|v| v.as_str())
            .or_else(|| payload.get("direction").and_then(|v| v.as_str()))
            .map(|v| v.to_string());
        let details_legacy = details
            .clone()
            .unwrap_or_else(|| "Unknown transaction".to_string());
        let booked_at_legacy = transaction_date
            .clone()
            .unwrap_or_else(|| "1970-01-01".to_string());
        let direction_legacy = tx_type.clone().unwrap_or_else(|| "unknown".to_string());
        let normalized_txn_hash = if row.get::<Option<String>, _>("account_id").is_none() {
            let hash_date = transaction_date.as_deref().unwrap_or("1970-01-01");
            let hash_details = details.as_deref().unwrap_or("unknown transaction");
            compute_row_hash(&account_id, hash_date, amount_cents, hash_details)
        } else {
            row.get::<String, _>("normalized_txn_hash")
        };
        let statement_id = row
            .get::<Option<String>, _>("statement_id")
            .or_else(|| import_statement_id.clone());

        let result = sqlx::query(
            "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, amount, details, transaction_date, source, classification_source, confidence, explanation, statement_id, direction, type, updated_at) VALUES (?1, ?2, ?3, ?4, 'CAD', ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, CURRENT_TIMESTAMP) ON CONFLICT(account_id, external_txn_id) DO NOTHING",
        )
        .bind(new_idempotency_key())
        .bind(&account_id)
        .bind(normalized_txn_hash)
        .bind(amount_cents)
        .bind(details_legacy)
        .bind(booked_at_legacy)
        .bind(amount)
        .bind(details)
        .bind(transaction_date)
        .bind(TransactionSource::Manual.as_str())
        .bind(ClassificationSource::Manual.as_str())
        .bind(row.get::<f64, _>("confidence"))
        .bind("Imported from statement")
        .bind(statement_id)
        .bind(direction_legacy)
        .bind(tx_type)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 1 {
            inserted_count += 1;
        } else {
            duplicate_count += 1;
        }
    }

    sqlx::query(
        "UPDATE imports SET status = ?2, committed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
    )
    .bind(import_id)
    .bind(ImportStatus::Committed.as_str())
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(CommitResult {
        inserted_count,
        duplicate_count,
    })
}

async fn resolve_statement_for_import(
    tx: &mut sqlx::Transaction<'_, Sqlite>,
    account_id: &str,
    diagnostics: &serde_json::Value,
) -> anyhow::Result<Option<String>> {
    let statement_context = diagnostics
        .get("statement_context")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let statement_summary = diagnostics
        .get("statement_summary")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let payload_snapshot = diagnostics
        .get("provider_diagnostics")
        .and_then(|v| v.get("payload_snapshot"))
        .cloned()
        .or_else(|| diagnostics.get("payload_snapshot").cloned())
        .unwrap_or_else(|| serde_json::json!({}));
    let lineage = diagnostics
        .get("provider_lineage")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    let period_start = statement_context
        .get("period_start")
        .and_then(|v| v.as_str());
    let period_end = statement_context.get("period_end").and_then(|v| v.as_str());
    let (Some(period_start), Some(period_end)) = (period_start, period_end) else {
        return Ok(None);
    };
    if period_start.trim().is_empty() || period_end.trim().is_empty() {
        return Ok(None);
    }

    let existing_statement_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM statements WHERE account_id = ?1 AND period_start = ?2 AND period_end = ?3 LIMIT 1",
    )
    .bind(account_id)
    .bind(period_start)
    .bind(period_end)
    .fetch_optional(tx.as_mut())
    .await?;
    let statement_id = if let Some(existing) = existing_statement_id {
        existing
    } else {
        let new_id = new_idempotency_key();
        sqlx::query(
            "INSERT INTO statements (id, account_id, period_start, period_end, statement_month, provider_name, provider_job_id, provider_run_id, provider_metadata_json, schema_version, statement_period_start, statement_period_end, statement_date, account_number_ending, customer_name, payment_due_date, total_minimum_payment, interest_charged, account_balance, credit_limit, available_credit, estimated_payoff_years, estimated_payoff_months, credits_total, debits_total, statement_payload_json, opening_balance_cents, opening_balance_date, closing_balance_cents, closing_balance_date, total_debits_cents, total_credits_cents, account_type, account_number_masked, currency_code, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, 'llamaextract_jobs', ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, CURRENT_TIMESTAMP)",
        )
        .bind(&new_id)
        .bind(account_id)
        .bind(period_start)
        .bind(period_end)
        .bind(statement_context.get("statement_month").and_then(|v| v.as_str()))
        .bind(lineage.get("job_id").and_then(|v| v.as_str()))
        .bind(lineage.get("run_id").and_then(|v| v.as_str()))
        .bind(lineage.to_string())
        .bind(
            statement_context
                .get("schema_version")
                .and_then(|v| v.as_str())
                .unwrap_or("statement_v2"),
        )
        .bind(statement_context.get("period_start").and_then(|v| v.as_str()))
        .bind(statement_context.get("period_end").and_then(|v| v.as_str()))
        .bind(payload_snapshot.get("statement_date").and_then(|v| v.as_str()))
        .bind(
            payload_snapshot
                .get("account_details")
                .and_then(|v| v.get("account_number_ending"))
                .and_then(|v| v.as_str()),
        )
        .bind(
            payload_snapshot
                .get("account_details")
                .and_then(|v| v.get("customer_name"))
                .and_then(|v| v.as_str()),
        )
        .bind(
            payload_snapshot
                .get("due_this_statement")
                .and_then(|v| v.get("payment_due_date"))
                .and_then(|v| v.as_str()),
        )
        .bind(
            payload_snapshot
                .get("due_this_statement")
                .and_then(|v| v.get("total_minimum_payment"))
                .and_then(|v| v.as_f64()),
        )
        .bind(statement_summary.get("interest_charged").and_then(|v| v.as_f64()))
        .bind(statement_summary.get("account_balance").and_then(|v| v.as_f64()))
        .bind(statement_summary.get("credit_limit").and_then(|v| v.as_f64()))
        .bind(statement_summary.get("available_credit").and_then(|v| v.as_f64()))
        .bind(
            payload_snapshot
                .get("interest_information")
                .and_then(|v| v.get("estimated_payoff_time"))
                .and_then(|v| v.get("years"))
                .and_then(|v| v.as_i64()),
        )
        .bind(
            payload_snapshot
                .get("interest_information")
                .and_then(|v| v.get("estimated_payoff_time"))
                .and_then(|v| v.get("months"))
                .and_then(|v| v.as_i64()),
        )
        .bind(
            payload_snapshot
                .get("transaction_subtotals")
                .and_then(|v| v.get("credits_total"))
                .and_then(|v| v.as_f64()),
        )
        .bind(
            payload_snapshot
                .get("transaction_subtotals")
                .and_then(|v| v.get("debits_total"))
                .and_then(|v| v.as_f64()),
        )
        .bind(payload_snapshot.to_string())
        .bind(statement_summary.get("opening_balance_cents").and_then(|v| v.as_i64()))
        .bind(
            statement_summary
                .get("opening_balance_date")
                .and_then(|v| v.as_str()),
        )
        .bind(statement_summary.get("closing_balance_cents").and_then(|v| v.as_i64()))
        .bind(
            statement_summary
                .get("closing_balance_date")
                .and_then(|v| v.as_str()),
        )
        .bind(statement_summary.get("total_debits_cents").and_then(|v| v.as_i64()))
        .bind(statement_summary.get("total_credits_cents").and_then(|v| v.as_i64()))
        .bind(
            payload_snapshot
                .get("account_details")
                .and_then(|v| v.get("account_type"))
                .and_then(|v| v.as_str()),
        )
        .bind(statement_summary.get("account_number_masked").and_then(|v| v.as_str()))
        .bind(statement_summary.get("currency_code").and_then(|v| v.as_str()))
        .execute(tx.as_mut())
        .await?;
        new_id
    };
    Ok(Some(statement_id))
}

fn is_valid_iso_date(value: &str) -> bool {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}

pub async fn query_transactions(
    pool: &SqlitePool,
    query: TransactionQuery,
) -> anyhow::Result<Vec<TransactionListItem>> {
    let mut base = String::from(
        "SELECT t.id, t.account_id, COALESCE(t.details, t.description) AS details, COALESCE(t.amount, printf('%.2f', t.amount_cents / 100.0)) AS amount, COALESCE(t.transaction_date, t.booked_at) AS transaction_date, t.source, COALESCE(t.classification_source, 'manual') AS classification_source, COALESCE(t.confidence, 1.0) AS confidence, COALESCE(t.explanation, 'Imported transaction') AS explanation, t.updated_at AS last_sync_at, (SELECT ir.import_id FROM import_rows ir WHERE ir.normalized_txn_hash = t.external_txn_id LIMIT 1) AS import_id, t.statement_id, COALESCE(t.type, t.direction) AS type FROM transactions t WHERE 1=1",
    );

    let mut binds: Vec<String> = Vec::new();

    if let Some(q) = query.q {
        base.push_str(" AND lower(COALESCE(t.details, t.description, '')) LIKE lower(?)");
        binds.push(format!("%{q}%"));
    }
    if let Some(account_id) = query.account_id {
        base.push_str(" AND t.account_id = ?");
        binds.push(account_id);
    }
    if let Some(source) = query.source {
        base.push_str(" AND t.source = ?");
        binds.push(source);
    }
    if let Some(date_from) = query.date_from {
        base.push_str(" AND date(COALESCE(t.transaction_date, t.booked_at)) >= date(?)");
        binds.push(date_from);
    }
    if let Some(date_to) = query.date_to {
        base.push_str(" AND date(COALESCE(t.transaction_date, t.booked_at)) <= date(?)");
        binds.push(date_to);
    }

    base.push_str(" ORDER BY COALESCE(t.transaction_date, t.booked_at) DESC, t.created_at DESC LIMIT ? OFFSET ?");

    let mut sql = sqlx::query(&base);
    for bind in binds {
        sql = sql.bind(bind);
    }
    sql = sql.bind(query.limit.max(1));
    sql = sql.bind(query.offset.max(0));

    let rows = sql.fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|row| TransactionListItem {
            id: row.get("id"),
            account_id: row.get("account_id"),
            details: row.get("details"),
            amount: row.get("amount"),
            transaction_date: row.get("transaction_date"),
            source: row.get("source"),
            classification_source: row.get("classification_source"),
            confidence: row.get("confidence"),
            explanation: row.get("explanation"),
            last_sync_at: row.get("last_sync_at"),
            import_id: row.get("import_id"),
            statement_id: row.get("statement_id"),
            tx_type: row.get("type"),
        })
        .collect())
}

pub async fn list_accounts(pool: &SqlitePool) -> anyhow::Result<Vec<AccountItem>> {
    let rows = sqlx::query(
        "SELECT id, name, currency_code, account_type, account_number_ending, customer_name, COALESCE(metadata_json, '{}') AS metadata_json FROM accounts ORDER BY name ASC",
    )
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| AccountItem {
            id: row.get("id"),
            name: row.get("name"),
            currency_code: row.get("currency_code"),
            account_type: row.get("account_type"),
            account_number_ending: row.get("account_number_ending"),
            customer_name: row.get("customer_name"),
            metadata_json: serde_json::from_str(row.get::<String, _>("metadata_json").as_str())
                .unwrap_or_else(|_| serde_json::json!({})),
        })
        .collect())
}

pub async fn enqueue_job(
    pool: &SqlitePool,
    job_type: &str,
    payload_json: &str,
) -> anyhow::Result<String> {
    let job_id = new_idempotency_key();
    sqlx::query(
        "INSERT INTO job_runs (id, job_type, payload_json, status, idempotency_key, attempts, next_run_at, updated_at) VALUES (?1, ?2, ?3, 'pending', ?4, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(&job_id)
    .bind(job_type)
    .bind(payload_json)
    .bind(new_idempotency_key())
    .execute(pool)
    .await?;
    Ok(job_id)
}

pub async fn claim_pending_job(
    pool: &SqlitePool,
    job_type: &str,
) -> anyhow::Result<Option<JobRun>> {
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "SELECT id, payload_json, attempts FROM job_runs WHERE job_type = ?1 AND status = 'pending' AND next_run_at <= CURRENT_TIMESTAMP ORDER BY created_at ASC LIMIT 1",
    )
    .bind(job_type)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(row) = row else {
        tx.commit().await?;
        return Ok(None);
    };

    let id: String = row.get("id");
    let updated = sqlx::query(
        "UPDATE job_runs SET status = 'running', attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?1 AND status = 'pending'",
    )
    .bind(&id)
    .execute(&mut *tx)
    .await?;

    if updated.rows_affected() == 0 {
        tx.commit().await?;
        return Ok(None);
    }

    tx.commit().await?;

    Ok(Some(JobRun {
        id,
        payload_json: row.get("payload_json"),
        attempts: row.get("attempts"),
    }))
}

pub async fn mark_job_completed(pool: &SqlitePool, job_id: &str) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE job_runs SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
    )
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_job_failed(
    pool: &SqlitePool,
    job_id: &str,
    attempts: i64,
    error_message: &str,
) -> anyhow::Result<()> {
    if attempts >= 3 {
        sqlx::query(
            "UPDATE job_runs SET status = 'failed', last_error = ?2, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
        )
        .bind(job_id)
        .bind(error_message)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(
            "UPDATE job_runs SET status = 'pending', next_run_at = datetime('now', '+30 seconds'), last_error = ?2, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
        )
        .bind(job_id)
        .bind(error_message)
        .execute(pool)
        .await?;
    }
    Ok(())
}

const MIGRATIONS: &[(&str, &str)] = &[
    (
        "0001_init",
        include_str!("../../../migrations/0001_init.sql"),
    ),
    (
        "0002_import_pipeline",
        include_str!("../../../migrations/0002_import_pipeline.sql"),
    ),
    (
        "0003_extraction_settings",
        include_str!("../../../migrations/0003_extraction_settings.sql"),
    ),
    (
        "0004_statement_foundation",
        include_str!("../../../migrations/0004_statement_foundation.sql"),
    ),
    (
        "0005_import_rows_statement_link",
        include_str!("../../../migrations/0005_import_rows_statement_link.sql"),
    ),
    (
        "0006_direction_and_statement_summary",
        include_str!("../../../migrations/0006_direction_and_statement_summary.sql"),
    ),
    (
        "0007_statement_v2_schema_first",
        include_str!("../../../migrations/0007_statement_v2_schema_first.sql"),
    ),
    (
        "0008_import_card_resolution",
        include_str!("../../../migrations/0008_import_card_resolution.sql"),
    ),
];

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_db_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock moved backwards")
            .as_nanos();
        let count = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::current_dir()
            .expect("cwd should exist")
            .join(".tmp")
            .join("storage-tests");
        std::fs::create_dir_all(&base).expect("create temp test directory");
        base.join(format!("expense-test-{nanos}-{count}.db"))
    }

    #[tokio::test]
    async fn connect_creates_db_and_enables_foreign_keys() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");

        let fk = sqlx::query("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .expect("pragma read should succeed");
        let fk_value: i64 = fk.get(0);
        assert_eq!(fk_value, 1);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn run_migrations_recovers_when_schema_migrations_is_truncated() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("initial migrations should pass");

        sqlx::query("DELETE FROM schema_migrations")
            .execute(&pool)
            .await
            .expect("should clear schema_migrations");

        run_migrations(&pool)
            .await
            .expect("rerun should tolerate already-added columns");

        let applied = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM schema_migrations")
            .fetch_one(&pool)
            .await
            .expect("read migration rows");
        assert_eq!(applied as usize, MIGRATIONS.len());

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn commit_is_idempotent_for_duplicate_hashes() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "sample.pdf".to_string(),
                parser_type: "pdf".to_string(),
                content_base64: "".to_string(),
                source_hash: "hash-1".to_string(),
                extraction_mode: None,
                resolved_account_id: None,
            },
        )
        .await
        .expect("create import");

        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");
        let normalized = serde_json::json!({
            "booked_at": "2026-03-04",
            "amount_cents": 1234,
            "description": "coffee",
            "direction": "credit"
        });

        insert_import_rows(
            &pool,
            &import_id,
            vec![
                ParsedRowInput {
                    row_index: 1,
                    normalized_json: normalized.clone(),
                    confidence: 0.9,
                    parse_error: None,
                    normalized_txn_hash: "same-hash".to_string(),
                    account_id: Some(account_id.clone()),
                    statement_id: None,
                },
                ParsedRowInput {
                    row_index: 2,
                    normalized_json: normalized,
                    confidence: 0.9,
                    parse_error: None,
                    normalized_txn_hash: "same-hash".to_string(),
                    account_id: Some(account_id),
                    statement_id: None,
                },
            ],
        )
        .await
        .expect("insert rows");

        let result = commit_import_rows(&pool, &import_id)
            .await
            .expect("commit import rows");
        assert_eq!(result.inserted_count, 1);
        assert_eq!(result.duplicate_count, 1);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn commit_skips_rows_with_parse_error_or_rejected_decision() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "sample.pdf".to_string(),
                parser_type: "pdf".to_string(),
                content_base64: "".to_string(),
                source_hash: "hash-2".to_string(),
                extraction_mode: None,
                resolved_account_id: None,
            },
        )
        .await
        .expect("create import");

        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        insert_import_rows(
            &pool,
            &import_id,
            vec![
                ParsedRowInput {
                    row_index: 1,
                    normalized_json: serde_json::json!({
                        "booked_at": "2026-03-05",
                        "amount_cents": 3300,
                        "description": "salary",
                        "direction": "credit"
                    }),
                    confidence: 0.99,
                    parse_error: None,
                    normalized_txn_hash: "hash-ok".to_string(),
                    account_id: Some(account_id.clone()),
                    statement_id: None,
                },
                ParsedRowInput {
                    row_index: 2,
                    normalized_json: serde_json::json!({
                        "booked_at": "2026-03-05",
                        "amount_cents": -1200,
                        "description": "unknown",
                        "direction": "debit"
                    }),
                    confidence: 0.4,
                    parse_error: Some("failed parse".to_string()),
                    normalized_txn_hash: "hash-err".to_string(),
                    account_id: Some(account_id.clone()),
                    statement_id: None,
                },
            ],
        )
        .await
        .expect("insert rows");

        let rows = list_import_rows_for_review(&pool, &import_id)
            .await
            .expect("list review rows");
        let second_row = rows
            .iter()
            .find(|r| r.row_index == 2)
            .expect("row 2 should exist");
        apply_review_decisions(
            &pool,
            &import_id,
            &[ReviewDecision {
                row_id: second_row.row_id.clone(),
                approved: false,
                rejection_reason: Some("invalid row".to_string()),
                direction: None,
                direction_confidence: None,
            }],
        )
        .await
        .expect("apply review decision");

        let result = commit_import_rows(&pool, &import_id)
            .await
            .expect("commit import rows");
        assert_eq!(result.inserted_count, 2);
        assert_eq!(result.duplicate_count, 0);

        let txs = query_transactions(
            &pool,
            TransactionQuery {
                q: Some("salary".to_string()),
                account_id: None,
                source: Some("manual".to_string()),
                date_from: None,
                date_to: None,
                limit: 20,
                offset: 0,
            },
        )
        .await
        .expect("query transactions");
        assert_eq!(txs.len(), 1);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn query_transactions_respects_filters() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        sqlx::query(
            "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation) VALUES (?1, ?2, ?3, 1200, 'CAD', 'Coffee Shop', '2026-03-01', 'manual', 'manual', 0.9, 'manual entry')",
        )
        .bind("tx-filter-1")
        .bind(&account_id)
        .bind("hash-filter-1")
        .execute(&pool)
        .await
        .expect("insert tx1");

        sqlx::query(
            "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation) VALUES (?1, ?2, ?3, 9900, 'CAD', 'Salary', '2026-03-02', 'manual', 'manual', 1.0, 'manual entry')",
        )
        .bind("tx-filter-2")
        .bind(&account_id)
        .bind("hash-filter-2")
        .execute(&pool)
        .await
        .expect("insert tx2");

        let only_coffee = query_transactions(
            &pool,
            TransactionQuery {
                q: Some("coffee".to_string()),
                account_id: Some(account_id),
                source: Some("manual".to_string()),
                date_from: Some("2026-03-01".to_string()),
                date_to: Some("2026-03-01".to_string()),
                limit: 50,
                offset: 0,
            },
        )
        .await
        .expect("query transactions");

        assert_eq!(only_coffee.len(), 1);
        assert_eq!(only_coffee[0].details.as_deref(), Some("Coffee Shop"));

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn extraction_settings_roundtrip_clamps_retry_limit() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let stored = upsert_extraction_settings(
            &pool,
            ExtractionSettings {
                default_extraction_mode: "managed".to_string(),
                managed_fallback_enabled: true,
                max_provider_retries: 99,
                provider_timeout_ms: 500_000,
            },
        )
        .await
        .expect("save settings");

        assert_eq!(stored.max_provider_retries, 3);
        assert_eq!(stored.provider_timeout_ms, 180_000);

        let loaded = get_extraction_settings(&pool).await.expect("load settings");
        assert_eq!(loaded.max_provider_retries, 3);
        assert_eq!(loaded.provider_timeout_ms, 180_000);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn extraction_result_fields_persist_in_import_status() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "sample.pdf".to_string(),
                parser_type: "pdf".to_string(),
                content_base64: "".to_string(),
                source_hash: "hash-3".to_string(),
                extraction_mode: Some("managed".to_string()),
                resolved_account_id: None,
            },
        )
        .await
        .expect("create import");

        let attempts = vec![serde_json::json!({
            "provider": "llamaparse",
            "attempt_no": 1,
            "outcome": "success"
        })];
        let diagnostics = serde_json::json!({
            "provider": "llamaparse",
            "rows": 3
        });
        update_import_extraction_result(
            &pool,
            &import_id,
            Some("llamaparse"),
            &attempts,
            &diagnostics,
        )
        .await
        .expect("update extraction result");

        let status = get_import_status(&pool, &import_id)
            .await
            .expect("get import status");
        assert_eq!(status.extraction_mode, "managed");
        assert_eq!(status.effective_provider.as_deref(), Some("llamaparse"));
        assert_eq!(status.provider_attempts.len(), 1);
        assert_eq!(
            status
                .diagnostics
                .get("provider")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
            "llamaparse"
        );

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn statement_foundation_migration_adds_expected_columns_and_constraints() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        let txn_cols = sqlx::query("PRAGMA table_info(transactions)")
            .fetch_all(&pool)
            .await
            .expect("table info");
        let has_statement_id = txn_cols
            .iter()
            .any(|row| row.get::<String, _>("name") == "statement_id");
        assert!(has_statement_id, "transactions.statement_id should exist");

        sqlx::query(
            "INSERT INTO statements (id, account_id, period_start, period_end, statement_month, schema_version) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind("stmt-1")
        .bind(&account_id)
        .bind("2026-03-01")
        .bind("2026-03-31")
        .bind("2026-03")
        .bind("statement_v1")
        .execute(&pool)
        .await
        .expect("insert statement");

        let duplicate = sqlx::query(
            "INSERT INTO statements (id, account_id, period_start, period_end, statement_month, schema_version) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind("stmt-2")
        .bind(&account_id)
        .bind("2026-03-01")
        .bind("2026-03-31")
        .bind("2026-03")
        .bind("statement_v1")
        .execute(&pool)
        .await;
        assert!(
            duplicate.is_err(),
            "duplicate account+period statement should fail"
        );

        let inserted = sqlx::query(
            "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, statement_id) VALUES (?1, ?2, ?3, ?4, 'CAD', ?5, ?6, 'manual', 'manual', 1.0, 'manual entry', ?7)",
        )
        .bind("tx-statement-1")
        .bind(&account_id)
        .bind("hash-statement-1")
        .bind(1250_i64)
        .bind("Coffee")
        .bind("2026-03-10")
        .bind(Option::<String>::None)
        .execute(&pool)
        .await
        .expect("insert tx with nullable statement_id");
        assert_eq!(inserted.rows_affected(), 1);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn llama_agent_cache_roundtrip_works() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let initial = get_llama_agent_cache(&pool).await.expect("read cache");
        assert!(initial.is_none());

        let saved = upsert_llama_agent_cache(&pool, "agent-123", "statement_v1")
            .await
            .expect("save cache");
        assert_eq!(saved.agent_id, "agent-123");
        assert_eq!(saved.schema_version, "statement_v1");
        assert!(!saved.updated_at.is_empty());

        let loaded = get_llama_agent_cache(&pool)
            .await
            .expect("reload cache")
            .expect("cache value");
        assert_eq!(loaded.agent_id, "agent-123");
        assert_eq!(loaded.schema_version, "statement_v1");

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn llama_agent_readiness_roundtrip_works() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let initial = get_llama_agent_readiness(&pool)
            .await
            .expect("read readiness");
        assert!(initial.is_none());

        let payload = LlamaAgentReadiness {
            state: LlamaAgentReadinessState::Configured,
            agent_name: "agent--statement_v1".to_string(),
            schema_version: "statement_v1".to_string(),
            agent_id: Some("agent-123".to_string()),
            checked_at: chrono::Utc::now().to_rfc3339(),
            error_code: None,
            error_message: None,
        };
        let saved = upsert_llama_agent_readiness(&pool, &payload)
            .await
            .expect("save readiness");
        assert_eq!(saved.state, LlamaAgentReadinessState::Configured);

        let loaded = get_llama_agent_readiness(&pool)
            .await
            .expect("reload readiness")
            .expect("readiness value");
        assert_eq!(loaded.agent_name, "agent--statement_v1");
        assert_eq!(loaded.schema_version, "statement_v1");
        assert_eq!(loaded.agent_id.as_deref(), Some("agent-123"));

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn upsert_or_get_statement_returns_same_record_for_same_period() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        let first = upsert_or_get_statement(
            &pool,
            &account_id,
            "2026-03-01",
            "2026-03-31",
            Some("2026-03"),
            Some("llamaextract_jobs"),
            Some("job-1"),
            Some("run-1"),
            &serde_json::json!({"job_id":"job-1","run_id":"run-1"}),
            "statement_v1",
            StatementSummaryInput::default(),
        )
        .await
        .expect("first upsert");
        let second = upsert_or_get_statement(
            &pool,
            &account_id,
            "2026-03-01",
            "2026-03-31",
            Some("2026-03"),
            Some("llamaextract_jobs"),
            Some("job-2"),
            Some("run-2"),
            &serde_json::json!({"job_id":"job-2","run_id":"run-2"}),
            "statement_v1",
            StatementSummaryInput::default(),
        )
        .await
        .expect("second upsert");

        assert_eq!(first.id, second.id);
        assert_eq!(first.period_start, "2026-03-01");
        assert_eq!(first.period_end, "2026-03-31");

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn commit_import_rows_persists_statement_id_linkage() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "sample.pdf".to_string(),
                parser_type: "pdf".to_string(),
                content_base64: "".to_string(),
                source_hash: "hash-link".to_string(),
                extraction_mode: None,
                resolved_account_id: None,
            },
        )
        .await
        .expect("create import");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");
        let statement = upsert_or_get_statement(
            &pool,
            &account_id,
            "2026-04-01",
            "2026-04-30",
            Some("2026-04"),
            Some("llamaextract_jobs"),
            Some("job-link"),
            Some("run-link"),
            &serde_json::json!({"job_id":"job-link","run_id":"run-link"}),
            "statement_v1",
            StatementSummaryInput::default(),
        )
        .await
        .expect("upsert statement");

        insert_import_rows(
            &pool,
            &import_id,
            vec![ParsedRowInput {
                row_index: 1,
                normalized_json: serde_json::json!({
                    "booked_at": "2026-04-10",
                    "amount_cents": 4200,
                    "description": "linked row",
                    "direction": "credit"
                }),
                confidence: 0.9,
                parse_error: None,
                normalized_txn_hash: "link-hash".to_string(),
                account_id: Some(account_id.clone()),
                statement_id: Some(statement.id.clone()),
            }],
        )
        .await
        .expect("insert import row");

        let committed = commit_import_rows(&pool, &import_id).await.expect("commit");
        assert_eq!(committed.inserted_count, 1);

        let saved = sqlx::query(
            "SELECT statement_id FROM transactions WHERE account_id = ?1 AND external_txn_id = ?2",
        )
        .bind(&account_id)
        .bind("link-hash")
        .fetch_one(&pool)
        .await
        .expect("load tx");
        let saved_statement_id: Option<String> = saved.get("statement_id");
        assert_eq!(saved_statement_id.as_deref(), Some(statement.id.as_str()));

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn commit_import_rows_allows_unknown_direction_rows() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "sample.pdf".to_string(),
                parser_type: "pdf".to_string(),
                content_base64: "".to_string(),
                source_hash: "hash-direction".to_string(),
                extraction_mode: None,
                resolved_account_id: None,
            },
        )
        .await
        .expect("create import");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        insert_import_rows(
            &pool,
            &import_id,
            vec![
                ParsedRowInput {
                    row_index: 1,
                    normalized_json: serde_json::json!({
                        "booked_at": "2026-04-10",
                        "amount_cents": -4200,
                        "description": "debit row",
                        "direction": "debit",
                        "direction_confidence": 0.91,
                        "direction_source": "model"
                    }),
                    confidence: 0.9,
                    parse_error: None,
                    normalized_txn_hash: "dir-hash-1".to_string(),
                    account_id: Some(account_id.clone()),
                    statement_id: None,
                },
                ParsedRowInput {
                    row_index: 2,
                    normalized_json: serde_json::json!({
                        "booked_at": "2026-04-11",
                        "amount_cents": 2500,
                        "description": "legacy row"
                    }),
                    confidence: 0.9,
                    parse_error: None,
                    normalized_txn_hash: "dir-hash-2".to_string(),
                    account_id: Some(account_id.clone()),
                    statement_id: None,
                },
            ],
        )
        .await
        .expect("insert import rows");

        let result = commit_import_rows(&pool, &import_id)
            .await
            .expect("unknown direction rows should commit");
        assert_eq!(result.inserted_count, 2);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn commit_import_rows_applies_defaults_for_missing_structural_fields() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "sample.pdf".to_string(),
                parser_type: "pdf".to_string(),
                content_base64: "".to_string(),
                source_hash: "hash-defaults".to_string(),
                extraction_mode: None,
                resolved_account_id: None,
            },
        )
        .await
        .expect("create import");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        insert_import_rows(
            &pool,
            &import_id,
            vec![ParsedRowInput {
                row_index: 1,
                normalized_json: serde_json::json!({
                    "direction": "debit",
                    "direction_source": "manual"
                }),
                confidence: 0.3,
                parse_error: Some("missing core fields".to_string()),
                normalized_txn_hash: "dir-hash-default".to_string(),
                account_id: Some(account_id.clone()),
                statement_id: None,
            }],
        )
        .await
        .expect("insert import row");

        commit_import_rows(&pool, &import_id).await.expect("commit");

        let row = sqlx::query(
            "SELECT booked_at, amount_cents, description, direction, direction_source FROM transactions WHERE account_id = ?1 AND external_txn_id = 'dir-hash-default'",
        )
        .bind(&account_id)
        .fetch_one(&pool)
        .await
        .expect("load committed defaults row");
        let booked_at: String = row.get("booked_at");
        let amount_cents: i64 = row.get("amount_cents");
        let description: String = row.get("description");
        let direction: String = row.get("direction");
        let direction_source: String = row.get("direction_source");
        assert_eq!(booked_at, "1970-01-01");
        assert_eq!(amount_cents, 0);
        assert_eq!(description, "Unknown transaction");
        assert_eq!(direction, "debit");
        assert_eq!(direction_source, "legacy");

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn apply_review_decisions_can_override_direction_metadata() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let import_id = create_import(
            &pool,
            CreateImportInput {
                file_name: "sample.pdf".to_string(),
                parser_type: "pdf".to_string(),
                content_base64: "".to_string(),
                source_hash: "hash-review-direction".to_string(),
                extraction_mode: None,
                resolved_account_id: None,
            },
        )
        .await
        .expect("create import");

        insert_import_rows(
            &pool,
            &import_id,
            vec![ParsedRowInput {
                row_index: 1,
                normalized_json: serde_json::json!({
                    "booked_at": "2026-04-10",
                    "amount_cents": 1000,
                    "description": "row",
                    "direction": "unknown"
                }),
                confidence: 0.9,
                parse_error: None,
                normalized_txn_hash: "review-dir-hash".to_string(),
                account_id: None,
                statement_id: None,
            }],
        )
        .await
        .expect("insert import row");

        let rows = list_import_rows_for_review(&pool, &import_id)
            .await
            .expect("review rows");
        let row_id = rows.first().expect("row").row_id.clone();
        apply_review_decisions(
            &pool,
            &import_id,
            &[ReviewDecision {
                row_id,
                approved: true,
                rejection_reason: None,
                direction: Some("credit".to_string()),
                direction_confidence: Some(0.88),
            }],
        )
        .await
        .expect("apply decision");

        let updated = list_import_rows_for_review(&pool, &import_id)
            .await
            .expect("updated rows");
        assert_eq!(updated[0].direction, "credit");
        assert_eq!(updated[0].direction_source, "manual");
        assert_eq!(updated[0].direction_confidence, Some(0.88));

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn upsert_statement_persists_summary_columns() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        let statement = upsert_or_get_statement(
            &pool,
            &account_id,
            "2026-11-01",
            "2026-11-30",
            Some("2026-11"),
            Some("llamaextract_jobs"),
            Some("job-summary"),
            Some("run-summary"),
            &serde_json::json!({}),
            "statement_v2",
            StatementSummaryInput {
                opening_balance_cents: Some(100_000),
                opening_balance_date: Some("2026-11-01".to_string()),
                closing_balance_cents: Some(95_000),
                closing_balance_date: Some("2026-11-30".to_string()),
                total_debits_cents: Some(15_000),
                total_credits_cents: Some(10_000),
                account_type: Some("chequing".to_string()),
                account_number_masked: Some("****1234".to_string()),
                currency_code: Some("CAD".to_string()),
                ..StatementSummaryInput::default()
            },
        )
        .await
        .expect("upsert statement");

        assert_eq!(statement.opening_balance_cents, Some(100_000));
        assert_eq!(statement.closing_balance_cents, Some(95_000));
        assert_eq!(statement.account_number_masked.as_deref(), Some("****1234"));
        assert_eq!(statement.currency_code.as_deref(), Some("CAD"));

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn statement_coverage_marks_statement_and_manual_months() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        let statement = upsert_or_get_statement(
            &pool,
            &account_id,
            "2026-05-01",
            "2026-05-31",
            Some("2026-05"),
            Some("llamaextract_jobs"),
            Some("job-cov"),
            Some("run-cov"),
            &serde_json::json!({}),
            "statement_v1",
            StatementSummaryInput::default(),
        )
        .await
        .expect("statement upsert");

        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, statement_id) VALUES ('tx-cov-1', ?1, 'hash-cov-1', 1100, 'CAD', 'Linked Tx', '2026-05-10', 'manual', 'manual', 1.0, 'manual', ?2)")
            .bind(&account_id)
            .bind(&statement.id)
            .execute(&pool)
            .await
            .expect("insert linked tx");

        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, created_at, statement_id) VALUES ('tx-cov-2', ?1, 'hash-cov-2', 1300, 'CAD', 'Manual Tx', '2026-06-03', 'manual', 'manual', 1.0, 'manual', '2026-06-04 12:00:00', NULL)")
            .bind(&account_id)
            .execute(&pool)
            .await
            .expect("insert manual tx");

        let coverage = get_statement_coverage(&pool, &account_id, None, None)
            .await
            .expect("coverage");

        let may = coverage
            .iter()
            .find(|item| item.year == 2026 && item.month == 5)
            .expect("may bucket");
        assert!(may.statement_exists);
        assert_eq!(may.linked_txn_count, 1);

        let june = coverage
            .iter()
            .find(|item| item.year == 2026 && item.month == 6)
            .expect("june bucket");
        assert!(!june.statement_exists);
        assert_eq!(june.manual_added_txn_count, 1);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn statement_coverage_skips_malformed_statement_period_start() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        sqlx::query(
            "INSERT INTO statements (id, account_id, period_start, period_end, statement_month, schema_version) VALUES ('st-mal-1', ?1, 'bad', '2026-05-31', NULL, 'statement_v1')",
        )
        .bind(&account_id)
        .execute(&pool)
        .await
        .expect("insert malformed statement");

        let coverage = get_statement_coverage(&pool, &account_id, None, None)
            .await
            .expect("coverage should not fail");
        assert!(coverage.is_empty());

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn list_transactions_for_statement_returns_only_linked_rows() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");
        let statement = upsert_or_get_statement(
            &pool,
            &account_id,
            "2026-07-01",
            "2026-07-31",
            Some("2026-07"),
            Some("llamaextract_jobs"),
            Some("job-list"),
            Some("run-list"),
            &serde_json::json!({}),
            "statement_v1",
            StatementSummaryInput::default(),
        )
        .await
        .expect("statement upsert");

        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, statement_id) VALUES ('tx-list-1', ?1, 'hash-list-1', 1500, 'CAD', 'Linked 1', '2026-07-03', 'manual', 'manual', 1.0, 'manual', ?2)")
            .bind(&account_id)
            .bind(&statement.id)
            .execute(&pool)
            .await
            .expect("insert linked tx");
        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation) VALUES ('tx-list-2', ?1, 'hash-list-2', 1600, 'CAD', 'Unlinked', '2026-07-04', 'manual', 'manual', 1.0, 'manual')")
            .bind(&account_id)
            .execute(&pool)
            .await
            .expect("insert unlinked tx");

        let rows = list_transactions_for_statement(&pool, &statement.id)
            .await
            .expect("list statement tx");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].details.as_deref(), Some("Linked 1"));
        assert_eq!(rows[0].statement_id.as_deref(), Some(statement.id.as_str()));

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }
}
