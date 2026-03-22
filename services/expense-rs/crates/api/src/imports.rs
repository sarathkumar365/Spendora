use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use expense_core::{compute_source_hash, ImportStatus};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use storage_sqlite::{
    apply_review_decisions, commit_import_rows, create_import, get_import_status,
    list_import_rows_for_review, update_import_status, CreateImportInput, ReviewDecision,
};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct CreateImportRequest {
    pub file_name: String,
    pub parser_type: Option<String>,
    pub content_base64: String,
    pub extraction_mode: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateImportResponse {
    pub import_id: String,
    pub status: String,
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
    if payload.file_name.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "file_name is required".to_string()));
    }

    if payload.content_base64.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "content_base64 is required".to_string(),
        ));
    }

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
    let source_hash = compute_source_hash(payload.content_base64.as_bytes());

    let import_id = create_import(
        &state.db,
        CreateImportInput {
            file_name: payload.file_name,
            parser_type,
            content_base64: payload.content_base64,
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
        .filter(|row| !row.approved || row.parse_error.is_some() || row.confidence < 0.75)
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
    use storage_sqlite::{connect, get_import_status, run_migrations};

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
                file_name: "".to_string(),
                parser_type: Some("pdf".to_string()),
                content_base64: "".to_string(),
                extraction_mode: None,
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
                file_name: "statement.pdf".to_string(),
                parser_type: Some("pdf".to_string()),
                content_base64: encoded,
                extraction_mode: None,
            }),
        )
        .await
        .expect("create import")
        .1;
        let import_id = created.import_id.clone();

        // Simulate worker parsed output by inserting parsed rows and moving status forward.
        storage_sqlite::insert_import_rows(
            &pool,
            &import_id,
            vec![
                storage_sqlite::ParsedRowInput {
                    row_index: 1,
                    normalized_json: serde_json::json!({
                        "booked_at": "2026-03-01",
                        "amount_cents": 1240,
                        "description": "coffee"
                    }),
                    confidence: 0.9,
                    parse_error: None,
                    normalized_txn_hash: "h1".to_string(),
                    account_id: Some("manual-default-account".to_string()),
                    statement_id: None,
                },
                storage_sqlite::ParsedRowInput {
                    row_index: 2,
                    normalized_json: serde_json::json!({
                        "booked_at": "03/01/2026",
                        "amount_cents": 510,
                        "description": "broken date"
                    }),
                    confidence: 0.6,
                    parse_error: Some(
                        "date format not ISO (YYYY-MM-DD), review required".to_string(),
                    ),
                    normalized_txn_hash: "h2".to_string(),
                    account_id: Some("manual-default-account".to_string()),
                    statement_id: None,
                },
            ],
        )
        .await
        .expect("insert rows");

        update_import_status(
            &pool,
            &import_id,
            ImportStatus::ReviewRequired,
            serde_json::json!({"parsed_rows": 2}),
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

        let low_confidence = rows
            .iter()
            .find(|r| r.row_index == 2)
            .expect("second row exists");
        update_import_review_handler(
            State(state.clone()),
            Path(import_id.clone()),
            Json(ReviewUpdateRequest {
                decisions: vec![ReviewDecision {
                    row_id: low_confidence.row_id.clone(),
                    approved: false,
                    rejection_reason: Some("manual reject".to_string()),
                }],
            }),
        )
        .await
        .expect("save review");

        let status_after_review = get_import_status(&pool, &import_id)
            .await
            .expect("status lookup");
        assert_eq!(
            status_after_review.status,
            ImportStatus::ReviewRequired.as_str()
        );

        let commit = commit_import_handler(State(state.clone()), Path(import_id.clone()))
            .await
            .expect("commit")
            .0;
        assert_eq!(commit.inserted_count, 1);
        assert_eq!(commit.duplicate_count, 0);

        let status_after_commit = get_import_status_handler(State(state), Path(import_id))
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
                file_name: "statement.pdf".to_string(),
                parser_type: Some("pdf".to_string()),
                content_base64: STANDARD.encode("2026-03-01,Coffee,12.40"),
                extraction_mode: Some("invalid".to_string()),
            }),
        )
        .await;
        assert!(result.is_err());

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }
}
