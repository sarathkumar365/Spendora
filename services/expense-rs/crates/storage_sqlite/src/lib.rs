use anyhow::{anyhow, Context};
use expense_core::{new_idempotency_key, ClassificationSource, ImportStatus, TransactionSource};
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionListItem {
    pub id: String,
    pub account_id: String,
    pub description: String,
    pub amount_cents: i64,
    pub booked_at: String,
    pub source: String,
    pub classification_source: String,
    pub confidence: f64,
    pub explanation: String,
    pub last_sync_at: String,
    pub import_id: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

        sqlx::query(sql).execute(pool).await?;
        sqlx::query("INSERT INTO schema_migrations (version) VALUES (?1)")
            .bind(*version)
            .execute(pool)
            .await?;
    }
    Ok(())
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
    sqlx::query(
        "INSERT INTO imports (id, source_type, status, file_name, parser_type, source_hash, content_base64, extraction_mode, updated_at) VALUES (?1, 'manual', ?2, ?3, ?4, ?5, ?6, ?7, CURRENT_TIMESTAMP)",
    )
    .bind(&import_id)
    .bind(ImportStatus::Queued.as_str())
    .bind(&input.file_name)
    .bind(&input.parser_type)
    .bind(&input.source_hash)
    .bind(&input.content_base64)
    .bind(extraction_mode)
    .execute(pool)
    .await?;

    let payload = serde_json::json!({ "import_id": import_id });
    enqueue_job(pool, "import_parse", &payload.to_string()).await?;

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

pub async fn get_llama_agent_cache(
    pool: &SqlitePool,
) -> anyhow::Result<Option<LlamaAgentCache>> {
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
    let row = sqlx::query("SELECT value_json FROM app_settings WHERE key = 'llama_agent_readiness'")
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
        "SELECT id, account_id, period_start, period_end, statement_month, provider_name, provider_job_id, provider_run_id, provider_metadata_json, schema_version FROM statements WHERE account_id = ?1 AND period_start = ?2 AND period_end = ?3",
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
) -> anyhow::Result<StatementRecord> {
    if let Some(existing) =
        get_statement_by_account_period(pool, account_id, period_start, period_end).await?
    {
        return Ok(existing);
    }

    let statement_id = new_idempotency_key();
    sqlx::query(
        "INSERT INTO statements (id, account_id, period_start, period_end, statement_month, provider_name, provider_job_id, provider_run_id, provider_metadata_json, schema_version, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, CURRENT_TIMESTAMP)",
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
    .execute(pool)
    .await?;

    get_statement_by_account_period(pool, account_id, period_start, period_end)
        .await?
        .ok_or_else(|| anyhow!("statement upsert load failed"))
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
        let has_parse_error = row.parse_error.is_some();
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
        .bind(if has_parse_error { 0 } else { 1 })
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
        "SELECT id, file_name, parser_type, status, extraction_mode, effective_provider, provider_attempts_json, extraction_diagnostics_json, review_required_count, summary_json, errors_json, warnings_json FROM imports WHERE id = ?1",
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
        .map(|row| ReviewRow {
            row_id: row.get("id"),
            row_index: row.get("row_index"),
            normalized_json: serde_json::from_str(row.get::<String, _>("normalized_json").as_str())
                .unwrap_or_else(|_| serde_json::json!({})),
            confidence: row.get("confidence"),
            parse_error: row.get("parse_error"),
            approved: row.get::<i64, _>("approved") == 1,
            rejection_reason: row.get("rejection_reason"),
        })
        .collect())
}

pub async fn apply_review_decisions(
    pool: &SqlitePool,
    import_id: &str,
    decisions: &[ReviewDecision],
) -> anyhow::Result<()> {
    for decision in decisions {
        sqlx::query(
            "UPDATE import_rows SET approved = ?3, rejection_reason = ?4 WHERE import_id = ?1 AND id = ?2",
        )
        .bind(import_id)
        .bind(&decision.row_id)
        .bind(if decision.approved { 1 } else { 0 })
        .bind(&decision.rejection_reason)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn commit_import_rows(
    pool: &SqlitePool,
    import_id: &str,
) -> anyhow::Result<CommitResult> {
    let default_account_id = ensure_default_manual_account(pool).await?;
    let mut tx = pool.begin().await?;

    let rows = sqlx::query(
        "SELECT id, normalized_json, normalized_txn_hash, confidence, account_id, statement_id FROM import_rows WHERE import_id = ?1 AND parse_error IS NULL AND approved = 1",
    )
    .bind(import_id)
    .fetch_all(&mut *tx)
    .await?;

    let mut inserted_count = 0_i64;
    let mut duplicate_count = 0_i64;

    for row in rows {
        let normalized_json: String = row.get("normalized_json");
        let payload: serde_json::Value = serde_json::from_str(&normalized_json)?;
        let account_id = row
            .get::<Option<String>, _>("account_id")
            .unwrap_or_else(|| default_account_id.clone());

        let description = payload
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("Imported transaction");
        let amount_cents = payload
            .get("amount_cents")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow!("normalized row missing amount_cents"))?;
        let booked_at = payload
            .get("booked_at")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("normalized row missing booked_at"))?;

        let result = sqlx::query(
            "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, statement_id, updated_at) VALUES (?1, ?2, ?3, ?4, 'CAD', ?5, ?6, ?7, ?8, ?9, ?10, ?11, CURRENT_TIMESTAMP) ON CONFLICT(account_id, external_txn_id) DO NOTHING",
        )
        .bind(new_idempotency_key())
        .bind(&account_id)
        .bind(row.get::<String, _>("normalized_txn_hash"))
        .bind(amount_cents)
        .bind(description)
        .bind(booked_at)
        .bind(TransactionSource::Manual.as_str())
        .bind(ClassificationSource::Manual.as_str())
        .bind(row.get::<f64, _>("confidence"))
        .bind("Imported from statement")
        .bind(row.get::<Option<String>, _>("statement_id"))
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

pub async fn query_transactions(
    pool: &SqlitePool,
    query: TransactionQuery,
) -> anyhow::Result<Vec<TransactionListItem>> {
    let mut base = String::from(
        "SELECT t.id, t.account_id, t.description, t.amount_cents, t.booked_at, t.source, COALESCE(t.classification_source, 'manual') AS classification_source, COALESCE(t.confidence, 1.0) AS confidence, COALESCE(t.explanation, 'Imported transaction') AS explanation, t.updated_at AS last_sync_at, (SELECT ir.import_id FROM import_rows ir WHERE ir.normalized_txn_hash = t.external_txn_id LIMIT 1) AS import_id FROM transactions t WHERE 1=1",
    );

    let mut binds: Vec<String> = Vec::new();

    if let Some(q) = query.q {
        base.push_str(" AND lower(t.description) LIKE lower(?)");
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
        base.push_str(" AND date(t.booked_at) >= date(?)");
        binds.push(date_from);
    }
    if let Some(date_to) = query.date_to {
        base.push_str(" AND date(t.booked_at) <= date(?)");
        binds.push(date_to);
    }

    base.push_str(" ORDER BY t.booked_at DESC, t.created_at DESC LIMIT ? OFFSET ?");

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
            description: row.get("description"),
            amount_cents: row.get("amount_cents"),
            booked_at: row.get("booked_at"),
            source: row.get("source"),
            classification_source: row.get("classification_source"),
            confidence: row.get("confidence"),
            explanation: row.get("explanation"),
            last_sync_at: row.get("last_sync_at"),
            import_id: row.get("import_id"),
        })
        .collect())
}

pub async fn list_accounts(pool: &SqlitePool) -> anyhow::Result<Vec<AccountItem>> {
    let rows = sqlx::query("SELECT id, name, currency_code FROM accounts ORDER BY name ASC")
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| AccountItem {
            id: row.get("id"),
            name: row.get("name"),
            currency_code: row.get("currency_code"),
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
            "description": "coffee"
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
                        "description": "salary"
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
                        "description": "unknown"
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
            }],
        )
        .await
        .expect("apply review decision");

        let result = commit_import_rows(&pool, &import_id)
            .await
            .expect("commit import rows");
        assert_eq!(result.inserted_count, 1);
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
        assert_eq!(only_coffee[0].description, "Coffee Shop");

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

        let initial = get_llama_agent_cache(&pool)
            .await
            .expect("read cache");
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
                    "description": "linked row"
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

        let committed = commit_import_rows(&pool, &import_id)
            .await
            .expect("commit");
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
}
