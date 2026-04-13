use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use expense_core::{compute_source_hash, ImportStatus};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use storage_sqlite::{
    apply_review_decisions, commit_import_rows, create_account_card, create_import,
    create_reused_import, get_import_status, get_statement_coverage, list_accounts,
    list_import_rows_for_review, set_import_card_resolution, update_import_status,
    CreateAccountCardInput, CreateImportInput, ReviewDecision, StatementSummaryInput,
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
    pub resolved_account_id: Option<String>,
    pub card_resolution_status: String,
    pub card_resolution_reason: Option<String>,
    pub card_resolution_metadata: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct ResolveImportCardRequest {
    pub account_id: Option<String>,
    pub new_account: Option<NewAccountRequest>,
}

#[derive(Debug, Deserialize)]
pub struct NewAccountRequest {
    pub name: String,
    pub currency_code: Option<String>,
    pub account_type: Option<String>,
    pub account_number_ending: Option<String>,
    pub customer_name: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ImportCardResolutionEnvelope {
    pub import_id: String,
    pub card_resolution_status: String,
    pub resolved_account_id: Option<String>,
    pub card_resolution_reason: Option<String>,
    pub card_resolution_metadata: serde_json::Value,
    pub candidate_accounts: Vec<storage_sqlite::AccountItem>,
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
        let coverage =
            get_statement_coverage(&state.db, account_id.as_str(), Some(year), Some(month))
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
                    resolved_account_id: Some(account_id),
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
            resolved_account_id: payload.account_id.filter(|v| !v.trim().is_empty()),
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
    let mut diagnostics = status.diagnostics.clone();
    if let Some(obj) = diagnostics.as_object_mut() {
        if !obj.contains_key("quality_metrics") {
            if let Some(metrics) = status.summary.get("quality_metrics").cloned() {
                obj.insert("quality_metrics".to_string(), metrics);
            }
        }
        if !obj.contains_key("reconciliation") {
            if let Some(reconciliation) = status.summary.get("reconciliation").cloned() {
                obj.insert("reconciliation".to_string(), reconciliation);
            }
        }
    }

    Ok(Json(ImportStatusEnvelope {
        import_id: status.import_id,
        status: status.status,
        extraction_mode: status.extraction_mode,
        effective_provider: status.effective_provider,
        provider_attempts: status.provider_attempts,
        diagnostics,
        summary: status.summary,
        errors: status.errors,
        warnings: status.warnings,
        review_required_count: status.review_required_count,
        resolved_account_id: status.resolved_account_id,
        card_resolution_status: status.card_resolution_status,
        card_resolution_reason: status.card_resolution_reason,
        card_resolution_metadata: status.card_resolution_metadata,
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
    fn ratio(numerator: i64, denominator: i64) -> f64 {
        if denominator <= 0 {
            return 0.0;
        }
        numerator as f64 / denominator as f64
    }

    let existing_status = get_import_status(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;

    apply_review_decisions(&state.db, &import_id, &payload.decisions)
        .await
        .map_err(internal_error)?;

    let rows = list_import_rows_for_review(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;
    let unknown_count = rows.iter().filter(|row| row.direction == "unknown").count() as i64;
    let review_required_count = unknown_count;
    let rows_total = rows.len() as i64;
    let manual_override_count = rows
        .iter()
        .filter(|row| row.direction_source == "manual")
        .count() as i64;
    let direction_review_row_count = existing_status
        .summary
        .get("quality_metrics")
        .and_then(|v| v.get("direction_review_row_count"))
        .and_then(|v| v.as_i64())
        .or_else(|| {
            existing_status
                .diagnostics
                .get("quality_metrics")
                .and_then(|v| v.get("direction_review_row_count"))
                .and_then(|v| v.as_i64())
        })
        .unwrap_or(rows_total.max(1));
    let conflict_count = existing_status
        .summary
        .get("quality_metrics")
        .and_then(|v| v.get("conflict_count"))
        .and_then(|v| v.as_i64())
        .or_else(|| {
            existing_status
                .diagnostics
                .get("quality_metrics")
                .and_then(|v| v.get("conflict_count"))
                .and_then(|v| v.as_i64())
        })
        .unwrap_or(0);
    let reconciliation_fail_count = existing_status
        .summary
        .get("reconciliation")
        .and_then(|v| v.get("fail_count"))
        .and_then(|v| v.as_i64())
        .or_else(|| {
            existing_status
                .diagnostics
                .get("reconciliation")
                .and_then(|v| v.get("fail_count"))
                .and_then(|v| v.as_i64())
        })
        .unwrap_or(0);
    let reconciliation_total_checks = existing_status
        .summary
        .get("reconciliation")
        .and_then(|v| v.get("total_checks"))
        .and_then(|v| v.as_i64())
        .or_else(|| {
            existing_status
                .diagnostics
                .get("reconciliation")
                .and_then(|v| v.get("total_checks"))
                .and_then(|v| v.as_i64())
        })
        .unwrap_or(0);

    let status = if review_required_count > 0 {
        ImportStatus::ReviewRequired
    } else if existing_status.card_resolution_status != "resolved"
        || existing_status.resolved_account_id.is_none()
    {
        ImportStatus::PendingCardResolution
    } else {
        ImportStatus::ReadyToCommit
    };
    let quality_metrics = serde_json::json!({
        "rows_total": rows_total,
        "direction_review_row_count": direction_review_row_count,
        "unknown_count": unknown_count,
        "unknown_rate": ratio(unknown_count, rows_total),
        "conflict_count": conflict_count,
        "conflict_rate": ratio(conflict_count, rows_total),
        "manual_override_count": manual_override_count,
        "manual_override_rate": ratio(manual_override_count, direction_review_row_count),
        "reconciliation_fail_count": reconciliation_fail_count,
        "reconciliation_fail_rate": ratio(reconciliation_fail_count, reconciliation_total_checks),
    });
    let mut summary = existing_status.summary.clone();
    if let Some(obj) = summary.as_object_mut() {
        obj.insert("rows".to_string(), serde_json::json!(rows_total));
        obj.insert(
            "unresolved_direction_count".to_string(),
            serde_json::json!(review_required_count),
        );
        obj.insert("quality_metrics".to_string(), quality_metrics);
        if !obj.contains_key("reconciliation") {
            obj.insert(
                "reconciliation".to_string(),
                existing_status
                    .diagnostics
                    .get("reconciliation")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({})),
            );
        }
    }

    update_import_status(
        &state.db,
        &import_id,
        status,
        summary,
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
        .map_err(|err| {
            let text = err.to_string();
            if text.contains("IMPORT_REVIEW_REQUIRED_UNKNOWN_DIRECTION") {
                (StatusCode::CONFLICT, text)
            } else if text.contains("IMPORT_CARD_RESOLUTION_REQUIRED") {
                (StatusCode::CONFLICT, text)
            } else {
                not_found_or_internal(err)
            }
        })?;
    Ok(Json(result))
}

pub async fn get_import_card_resolution_handler(
    State(state): State<Arc<AppState>>,
    Path(import_id): Path<String>,
) -> Result<Json<ImportCardResolutionEnvelope>, (StatusCode, String)> {
    let status = get_import_status(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;
    let accounts = list_accounts(&state.db).await.map_err(internal_error)?;
    Ok(Json(ImportCardResolutionEnvelope {
        import_id: status.import_id,
        card_resolution_status: status.card_resolution_status,
        resolved_account_id: status.resolved_account_id,
        card_resolution_reason: status.card_resolution_reason,
        card_resolution_metadata: status.card_resolution_metadata,
        candidate_accounts: accounts,
    }))
}

pub async fn resolve_import_card_handler(
    State(state): State<Arc<AppState>>,
    Path(import_id): Path<String>,
    Json(payload): Json<ResolveImportCardRequest>,
) -> Result<Json<ImportCardResolutionEnvelope>, (StatusCode, String)> {
    let choose_existing = payload
        .account_id
        .as_ref()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false);
    let create_new = payload.new_account.is_some();
    if choose_existing == create_new {
        return Err((
            StatusCode::BAD_REQUEST,
            "provide exactly one of account_id or new_account".to_string(),
        ));
    }

    let selected_account_id = if let Some(account_id) = payload.account_id {
        account_id.trim().to_string()
    } else {
        let new_account = payload.new_account.expect("checked above");
        if new_account.name.trim().is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "new_account.name is required".to_string(),
            ));
        }
        let created = create_account_card(
            &state.db,
            CreateAccountCardInput {
                name: new_account.name,
                currency_code: new_account
                    .currency_code
                    .unwrap_or_else(|| "CAD".to_string()),
                account_type: new_account.account_type,
                account_number_ending: new_account.account_number_ending,
                customer_name: new_account.customer_name,
                metadata_json: new_account.metadata_json,
            },
        )
        .await
        .map_err(internal_error)?;
        created.id
    };

    let current = get_import_status(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;
    set_import_card_resolution(
        &state.db,
        &import_id,
        Some(selected_account_id.as_str()),
        Some("manual_selection"),
        &current.card_resolution_metadata,
    )
    .await
    .map_err(internal_error)?;

    let rows = list_import_rows_for_review(&state.db, &import_id)
        .await
        .map_err(not_found_or_internal)?;
    let review_required_count = rows.iter().filter(|row| row.direction == "unknown").count() as i64;
    let status = if review_required_count > 0 {
        ImportStatus::ReviewRequired
    } else {
        ImportStatus::ReadyToCommit
    };
    let mut summary = current.summary.clone();
    if let Some(obj) = summary.as_object_mut() {
        obj.insert(
            "unresolved_direction_count".to_string(),
            serde_json::json!(review_required_count),
        );
    }
    update_import_status(
        &state.db,
        &import_id,
        status,
        summary,
        current.errors,
        current.warnings,
        review_required_count,
    )
    .await
    .map_err(internal_error)?;

    get_import_card_resolution_handler(State(state), Path(import_id)).await
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
    use storage_sqlite::{connect, get_import_status, run_migrations, upsert_or_get_statement};

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
                account_id: Some("manual-default-account".to_string()),
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
                        "description": "Coffee",
                        "direction": "credit"
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
                        "description": "Broken Date",
                        "direction": "unknown"
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
                approved: true,
                rejection_reason: None,
                direction: if row.row_index == 2 {
                    Some("credit".to_string())
                } else {
                    None
                },
                direction_confidence: None,
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
            StatementSummaryInput::default(),
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

    #[tokio::test]
    async fn review_update_sets_pending_card_resolution_when_directions_are_resolved() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let created = create_import_handler(
            State(state.clone()),
            Json(CreateImportRequest {
                file_name: Some("statement.pdf".to_string()),
                parser_type: Some("pdf".to_string()),
                content_base64: Some(STANDARD.encode(b"fake")),
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
            vec![storage_sqlite::ParsedRowInput {
                row_index: 1,
                normalized_json: serde_json::json!({
                    "transaction_date": "2026-04-10",
                    "amount": 10.0,
                    "details": "Coffee",
                    "type": "credit"
                }),
                confidence: 0.9,
                parse_error: None,
                normalized_txn_hash: "pending-card-hash".to_string(),
                account_id: None,
                statement_id: None,
            }],
        )
        .await
        .expect("insert import rows");

        update_import_review_handler(
            State(state.clone()),
            Path(import_id.clone()),
            Json(ReviewUpdateRequest {
                decisions: vec![ReviewDecision {
                    row_id: storage_sqlite::list_import_rows_for_review(&pool, &import_id)
                        .await
                        .expect("review rows")
                        .first()
                        .expect("row exists")
                        .row_id
                        .clone(),
                    approved: true,
                    rejection_reason: None,
                    direction: None,
                    direction_confidence: None,
                }],
            }),
        )
        .await
        .expect("update review");

        let status = get_import_status(&pool, &import_id)
            .await
            .expect("status lookup");
        assert_eq!(status.status, ImportStatus::PendingCardResolution.as_str());
        assert_eq!(status.card_resolution_status, "pending");
        assert!(status.resolved_account_id.is_none());

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn resolve_import_card_select_existing_sets_ready_to_commit() {
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

        let import_id = create_import_handler(
            State(state.clone()),
            Json(CreateImportRequest {
                file_name: Some("statement.pdf".to_string()),
                parser_type: Some("pdf".to_string()),
                content_base64: Some(STANDARD.encode(b"fake")),
                extraction_mode: Some("managed".to_string()),
                account_id: None,
                year: None,
                month: None,
            }),
        )
        .await
        .expect("create import")
        .1
         .0
        .import_id;

        let resolved = resolve_import_card_handler(
            State(state.clone()),
            Path(import_id.clone()),
            Json(ResolveImportCardRequest {
                account_id: Some("manual-default-account".to_string()),
                new_account: None,
            }),
        )
        .await
        .expect("resolve import card")
        .0;

        assert_eq!(resolved.card_resolution_status, "resolved");
        assert_eq!(
            resolved.resolved_account_id.as_deref(),
            Some("manual-default-account")
        );

        let status = get_import_status(&pool, &import_id)
            .await
            .expect("status lookup");
        assert_eq!(status.status, ImportStatus::ReadyToCommit.as_str());
        assert_eq!(status.card_resolution_status, "resolved");
        assert_eq!(
            status.resolved_account_id.as_deref(),
            Some("manual-default-account")
        );

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn resolve_import_card_create_new_card_sets_ready_to_commit() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let import_id = create_import_handler(
            State(state.clone()),
            Json(CreateImportRequest {
                file_name: Some("statement.pdf".to_string()),
                parser_type: Some("pdf".to_string()),
                content_base64: Some(STANDARD.encode(b"fake")),
                extraction_mode: Some("managed".to_string()),
                account_id: None,
                year: None,
                month: None,
            }),
        )
        .await
        .expect("create import")
        .1
         .0
        .import_id;

        let resolved = resolve_import_card_handler(
            State(state.clone()),
            Path(import_id.clone()),
            Json(ResolveImportCardRequest {
                account_id: None,
                new_account: Some(NewAccountRequest {
                    name: "Scotia Visa".to_string(),
                    currency_code: Some("CAD".to_string()),
                    account_type: Some("Scotiabank Scene+ Visa Card".to_string()),
                    account_number_ending: Some("1234".to_string()),
                    customer_name: Some("Jane Doe".to_string()),
                    metadata_json: Some(serde_json::json!({"source":"test"})),
                }),
            }),
        )
        .await
        .expect("resolve with new card")
        .0;

        assert_eq!(resolved.card_resolution_status, "resolved");
        let resolved_account_id = resolved
            .resolved_account_id
            .as_deref()
            .expect("resolved account id");
        assert_ne!(resolved_account_id, "manual-default-account");

        let accounts = storage_sqlite::list_accounts(&pool)
            .await
            .expect("list accounts");
        assert!(accounts
            .iter()
            .any(|a| a.id == resolved_account_id && a.name == "Scotia Visa"));

        let status = get_import_status(&pool, &import_id)
            .await
            .expect("status lookup");
        assert_eq!(status.status, ImportStatus::ReadyToCommit.as_str());
        assert_eq!(status.card_resolution_status, "resolved");

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn commit_import_returns_conflict_when_card_not_resolved() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let import_id = create_import_handler(
            State(state.clone()),
            Json(CreateImportRequest {
                file_name: Some("statement.pdf".to_string()),
                parser_type: Some("pdf".to_string()),
                content_base64: Some(STANDARD.encode(b"fake")),
                extraction_mode: Some("managed".to_string()),
                account_id: None,
                year: None,
                month: None,
            }),
        )
        .await
        .expect("create import")
        .1
         .0
        .import_id;

        storage_sqlite::insert_import_rows(
            &pool,
            &import_id,
            vec![storage_sqlite::ParsedRowInput {
                row_index: 1,
                normalized_json: serde_json::json!({
                    "transaction_date": "2026-04-10",
                    "amount": -12.0,
                    "details": "Coffee",
                    "type": "debit"
                }),
                confidence: 0.95,
                parse_error: None,
                normalized_txn_hash: "blocked-commit-hash".to_string(),
                account_id: None,
                statement_id: None,
            }],
        )
        .await
        .expect("insert row");

        let err = commit_import_handler(State(state), Path(import_id))
            .await
            .expect_err("should require card resolution");
        assert_eq!(err.0, StatusCode::CONFLICT);
        assert!(err.1.contains("IMPORT_CARD_RESOLUTION_REQUIRED"));

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }
}
