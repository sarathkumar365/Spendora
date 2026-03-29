use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use expense_core::{compute_source_hash, ImportStatus};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use storage_sqlite::{
    apply_review_decisions, commit_import_rows, create_import, create_reused_import,
    get_import_status, get_statement_coverage, list_import_rows_for_review, update_import_status,
    CreateImportInput, ReviewDecision,
};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct CreateImportRequest {
    pub file_name: Option<String>,
    pub parser_type: Option<String>,
    pub content_base64: Option<String>,
    pub extraction_mode: Option<String>,
    pub account_id: Option<String>,
    pub year: Option<i32>,
    pub month: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct CreateImportResponse {
    pub import_id: String,
    pub status: String,
    pub reused: bool,
}

#[derive(Debug, Deserialize)]
pub struct ReviewUpdateRequest {
    pub decisions: Vec<ReviewDecision>,
}

#[derive(Debug, Serialize)]
pub struct ImportStatusEnvelope {
    pub import_id: String,
    pub status: String,
    pub extraction_mode: String,
    pub effective_provider: Option<String>,
    pub provider_attempts: Vec<serde_json::Value>,
    pub diagnostics: serde_json::Value,
    pub summary: serde_json::Value,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub review_required_count: i64,
}

pub async fn create_import_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateImportRequest>,
) -> Result<(StatusCode, Json<CreateImportResponse>), (StatusCode, String)> {
    let parser_type = payload.parser_type.unwrap_or_else(|| "pdf".to_string());
    if parser_type != "pdf" && parser_type != "csv" {
        return Err((
            StatusCode::BAD_REQUEST,
            "parser_type must be pdf or csv".to_string(),
        ));
    }

    let extraction_mode = payload
        .extraction_mode
        .unwrap_or_else(|| "managed".to_string());
    if extraction_mode != "managed" && extraction_mode != "local_ocr" {
        return Err((
            StatusCode::BAD_REQUEST,
            "extraction_mode must be managed or local_ocr".to_string(),
        ));
    }

    let selected_year = payload.year;
    let selected_month = payload.month;
    let has_month_selection = selected_year.is_some() || selected_month.is_some();
    if has_month_selection && (selected_year.is_none() || selected_month.is_none()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "year and month must be provided together".to_string(),
        ));
    }
    if let Some(month) = selected_month {
        if !(1..=12).contains(&month) {
            return Err((StatusCode::BAD_REQUEST, "month must be 1..12".to_string()));
        }
    }

    if let (Some(account_id), Some(year), Some(month)) =
        (payload.account_id.clone(), selected_year, selected_month)
    {
        let coverage = get_statement_coverage(&state.db, account_id.as_str(), Some(year), Some(month))
            .await
            .map_err(internal_error)?;

        if let Some(hit) = coverage.iter().find(|item| item.statement_exists) {
            let file_name = payload
                .file_name
                .clone()
                .unwrap_or_else(|| format!("reused-{year:04}-{month:02}.pdf"));
            let content_base64 = payload.content_base64.clone().unwrap_or_default();
            let source_hash = compute_source_hash(content_base64.as_bytes());
            let summary = serde_json::json!({
                "reused": true,
                "statement_id": hit.statement_id,
                "statement_month": hit.statement_month,
                "period_start": hit.period_start,
                "period_end": hit.period_end,
                "linked_txn_count": hit.linked_txn_count,
                "manual_added_txn_count": hit.manual_added_txn_count,
                "review_required_count": 0,
                "inserted_count": 0,
                "duplicate_count": 0
            });
            let diagnostics = serde_json::json!({
                "reuse_mode": "statement_db",
                "reused": true,
                "account_id": account_id,
                "year": year,
                "month": month,
                "policy_note": "statement exists; extraction skipped"
            });

            let import_id = create_reused_import(
                &state.db,
                CreateImportInput {
                    file_name,
                    parser_type,
                    content_base64,
                    source_hash,
                    extraction_mode: Some(extraction_mode),
                },
                &summary,
                &diagnostics,
            )
            .await
            .map_err(internal_error)?;

            return Ok((
                StatusCode::CREATED,
                Json(CreateImportResponse {
                    import_id,
                    status: ImportStatus::Committed.as_str().to_string(),
                    reused: true,
                }),
            ));
        }
    }

    let file_name = payload.file_name.unwrap_or_default();
    if file_name.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "file_name is required".to_string()));
    }

    let content_base64 = payload.content_base64.unwrap_or_default();
    if content_base64.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "content_base64 is required".to_string(),
        ));
    }

    let source_hash = compute_source_hash(content_base64.as_bytes());

    let import_id = create_import(
        &state.db,
        CreateImportInput {
            file_name,
            parser_type,
            content_base64,
            source_hash,
            extraction_mode: Some(extraction_mode),
        },
    )
    .await
    .map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(CreateImportResponse {
            import_id,
            status: ImportStatus::Queued.as_str().to_string(),
            reused: false,
        }),
    ))
}

pub async fn get_import_status_handler(
    State(state): State<Arc<AppState>>,
    Path(import_id): Path<String>,
) -> Result<Json<ImportStatusEnvelope>, (StatusCode, String)> {
    let status = get_import_status(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;

    Ok(Json(ImportStatusEnvelope {
        import_id: status.import_id,
        status: status.status,
        extraction_mode: status.extraction_mode,
        effective_provider: status.effective_provider,
        provider_attempts: status.provider_attempts,
        diagnostics: status.diagnostics,
        summary: status.summary,
        errors: status.errors,
        warnings: status.warnings,
        review_required_count: status.review_required_count,
    }))
}

pub async fn get_import_review_handler(
    State(state): State<Arc<AppState>>,
    Path(import_id): Path<String>,
) -> Result<Json<Vec<storage_sqlite::ReviewRow>>, (StatusCode, String)> {
    let rows = list_import_rows_for_review(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;
    Ok(Json(rows))
}

pub async fn update_import_review_handler(
    State(state): State<Arc<AppState>>,
    Path(import_id): Path<String>,
    Json(payload): Json<ReviewUpdateRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let existing_status = get_import_status(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;

    apply_review_decisions(&state.db, &import_id, &payload.decisions)
        .await
        .map_err(internal_error)?;

    let rows = list_import_rows_for_review(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;
    let review_required_count = rows
        .iter()
        .filter(|row| row.approved && (row.parse_error.is_some() || row.confidence < 0.75))
        .count() as i64;

    let status = if review_required_count > 0 {
        ImportStatus::ReviewRequired
    } else {
        ImportStatus::ReadyToCommit
    };

    update_import_status(
        &state.db,
        &import_id,
        status,
        serde_json::json!({ "rows": rows.len() }),
        existing_status.errors,
        existing_status.warnings,
        review_required_count,
    )
    .await
    .map_err(internal_error)?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn commit_import_handler(
    State(state): State<Arc<AppState>>,
    Path(import_id): Path<String>,
) -> Result<Json<storage_sqlite::CommitResult>, (StatusCode, String)> {
    let result = commit_import_rows(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;
    Ok(Json(result))
}

fn internal_error(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

fn not_found_or_internal(err: anyhow::Error) -> (StatusCode, String) {
    let text = err.to_string();
    if text.contains("not found") {
        (StatusCode::NOT_FOUND, text)
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;
    use axum::extract::{Path, State};
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use std::sync::Arc;
    use storage_sqlite::{
        connect, get_import_status, run_migrations, upsert_or_get_statement,
    };

    fn temp_db_path() -> std::path::PathBuf {
        std::env::current_dir()
            .expect("cwd")
            .join(".tmp")
            .join(format!(
                "api-imports-test-{}.db",
                expense_core::new_idempotency_key()
            ))
    }

    #[tokio::test]
    async fn create_import_validates_required_fields() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let result = create_import_handler(
            State(state),
            Json(CreateImportRequest {
                file_name: Some("".to_string()),
                parser_type: Some("pdf".to_string()),
                content_base64: Some("".to_string()),
                extraction_mode: None,
                account_id: None,
                year: None,
                month: None,
            }),
        )
        .await;
        assert!(result.is_err());

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn review_and_commit_flow_returns_expected_statuses() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        storage_sqlite::ensure_default_manual_account(&pool)
            .await
            .expect("default account");
        let state = Arc::new(AppState { db: pool.clone() });

        let raw = b"2026-03-01|Coffee|12.40\n03/01/2026|Broken Date|5.10";
        let encoded = STANDARD.encode(raw);
        let created = create_import_handler(
            State(state.clone()),
            Json(CreateImportRequest {
                file_name: Some("statement.pdf".to_string()),
                parser_type: Some("pdf".to_string()),
                content_base64: Some(encoded),
                extraction_mode: Some("managed".to_string()),
                account_id: None,
                year: None,
                month: None,
            }),
        )
        .await
        .expect("create import")
        .1
         .0;
        let import_id = created.import_id.clone();

        storage_sqlite::insert_import_rows(
            &pool,
            &import_id,
            vec![
                storage_sqlite::ParsedRowInput {
                    row_index: 1,
                    normalized_json: serde_json::json!({
                        "booked_at": "2026-03-01",
                        "amount_cents": 1240,
                        "description": "Coffee"
                    }),
                    confidence: 0.92,
                    parse_error: None,
                    normalized_txn_hash: "hash-a".to_string(),
                    account_id: Some("manual-default-account".to_string()),
                    statement_id: None,
                },
                storage_sqlite::ParsedRowInput {
                    row_index: 2,
                    normalized_json: serde_json::json!({
                        "booked_at": "2026-03-01",
                        "amount_cents": 510,
                        "description": "Broken Date"
                    }),
                    confidence: 0.4,
                    parse_error: Some("invalid date format".to_string()),
                    normalized_txn_hash: "hash-b".to_string(),
                    account_id: Some("manual-default-account".to_string()),
                    statement_id: None,
                },
            ],
        )
        .await
        .expect("insert import rows");

        update_import_status(
            &pool,
            &import_id,
            ImportStatus::ReviewRequired,
            serde_json::json!({ "rows": 2 }),
            Vec::new(),
            Vec::new(),
            1,
        )
        .await
        .expect("status update");

        let rows = get_import_review_handler(State(state.clone()), Path(import_id.clone()))
            .await
            .expect("review rows")
            .0;
        assert_eq!(rows.len(), 2);

        let decisions = rows
            .iter()
            .map(|row| storage_sqlite::ReviewDecision {
                row_id: row.row_id.clone(),
                approved: row.row_index == 1,
                rejection_reason: if row.row_index == 2 {
                    Some("bad date".to_string())
                } else {
                    None
                },
            })
            .collect::<Vec<_>>();

        update_import_review_handler(
            State(state.clone()),
            Path(import_id.clone()),
            Json(ReviewUpdateRequest { decisions }),
        )
        .await
        .expect("update review");

        let status_after_review = get_import_status(&pool, &import_id)
            .await
            .expect("status lookup");
        assert_eq!(
            status_after_review.status,
            ImportStatus::ReadyToCommit.as_str()
        );

        let _commit = commit_import_handler(State(state), Path(import_id.clone()))
            .await
            .expect("commit response")
            .0;

        let status_after_commit = get_import_status_handler(
            State(Arc::new(AppState { db: pool.clone() })),
            Path(import_id),
        )
        .await
        .expect("status response")
        .0;
        assert_eq!(status_after_commit.status, ImportStatus::Committed.as_str());

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn create_import_rejects_invalid_extraction_mode() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let result = create_import_handler(
            State(state),
            Json(CreateImportRequest {
                file_name: Some("statement.pdf".to_string()),
                parser_type: Some("pdf".to_string()),
                content_base64: Some(STANDARD.encode(b"fake")),
                extraction_mode: Some("not-real".to_string()),
                account_id: None,
                year: None,
                month: None,
            }),
        )
        .await;
        assert!(result.is_err());

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn create_import_reuses_when_statement_exists_for_selected_month() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let account_id = storage_sqlite::ensure_default_manual_account(&pool)
            .await
            .expect("default account");
        upsert_or_get_statement(
            &pool,
            &account_id,
            "2026-04-01",
            "2026-04-30",
            Some("2026-04"),
            Some("llamaextract_jobs"),
            Some("job-1"),
            Some("run-1"),
            &serde_json::json!({}),
            "statement_v1",
        )
        .await
        .expect("upsert statement");

        let state = Arc::new(AppState { db: pool.clone() });
        let created = create_import_handler(
            State(state),
            Json(CreateImportRequest {
                file_name: None,
                parser_type: Some("pdf".to_string()),
                content_base64: None,
                extraction_mode: Some("managed".to_string()),
                account_id: Some(account_id),
                year: Some(2026),
                month: Some(4),
            }),
        )
        .await
        .expect("create import response")
        .1
        .0;

        assert!(created.reused);
        assert_eq!(created.status, ImportStatus::Committed.as_str());

        let status = get_import_status(&pool, &created.import_id)
            .await
            .expect("status lookup");
        assert_eq!(status.status, ImportStatus::Committed.as_str());
        assert_eq!(
            status
                .summary
                .get("reused")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            true
        );
        let queued_parse_jobs = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM job_runs WHERE job_type = 'import_parse' AND status = 'pending'",
        )
        .fetch_one(&pool)
        .await
        .expect("load parse jobs");
        assert_eq!(
            queued_parse_jobs, 0,
            "reuse path should not enqueue import_parse jobs"
        );

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }
}
