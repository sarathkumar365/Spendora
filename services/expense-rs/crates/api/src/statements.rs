use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use storage_sqlite::{
    get_statement_coverage, list_statements_for_account, list_transactions_for_statement,
    StatementCoverageMonth, StatementListItem, TransactionListItem,
};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct StatementsQueryParams {
    pub account_id: String,
    pub year: Option<i32>,
    pub month: Option<i32>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CoverageQueryParams {
    pub account_id: String,
    pub year: Option<i32>,
    pub month: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct CoverageMonthView {
    pub month: i32,
    pub statement_exists: bool,
    pub statement_id: Option<String>,
    pub statement_month: Option<String>,
    pub period_start: Option<String>,
    pub period_end: Option<String>,
    pub linked_txn_count: i64,
    pub manual_added_txn_count: i64,
}

#[derive(Debug, Serialize)]
pub struct CoverageYearView {
    pub year: i32,
    pub months: Vec<CoverageMonthView>,
}

#[derive(Debug, Serialize)]
pub struct CoverageSelectedView {
    pub year: i32,
    pub month: i32,
    pub reusable: bool,
    pub statement_exists: bool,
    pub has_linked_txns: bool,
    pub has_manual_added_txns_only: bool,
    pub policy_note: &'static str,
    pub statement_id: Option<String>,
    pub statement_month: Option<String>,
    pub period_start: Option<String>,
    pub period_end: Option<String>,
    pub linked_txn_count: i64,
    pub manual_added_txn_count: i64,
}

#[derive(Debug, Serialize)]
pub struct CoverageResponse {
    pub account_id: String,
    pub years: Vec<CoverageYearView>,
    pub selected: Option<CoverageSelectedView>,
}

pub async fn list_statements_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<StatementsQueryParams>,
) -> Result<Json<Vec<StatementListItem>>, (StatusCode, String)> {
    if params.account_id.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "account_id is required".to_string(),
        ));
    }
    if let Some(month) = params.month {
        if !(1..=12).contains(&month) {
            return Err((StatusCode::BAD_REQUEST, "month must be 1..12".to_string()));
        }
    }

    let rows = list_statements_for_account(
        &state.db,
        params.account_id.trim(),
        params.year,
        params.month,
        params.date_from.as_deref(),
        params.date_to.as_deref(),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(rows))
}

pub async fn get_statement_coverage_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CoverageQueryParams>,
) -> Result<Json<CoverageResponse>, (StatusCode, String)> {
    if params.account_id.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "account_id is required".to_string(),
        ));
    }
    if (params.year.is_some() && params.month.is_none())
        || (params.year.is_none() && params.month.is_some())
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "year and month must be provided together".to_string(),
        ));
    }
    if let Some(month) = params.month {
        if !(1..=12).contains(&month) {
            return Err((StatusCode::BAD_REQUEST, "month must be 1..12".to_string()));
        }
    }

    let coverage = get_statement_coverage(
        &state.db,
        params.account_id.trim(),
        params.year,
        params.month,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut grouped: BTreeMap<i32, Vec<CoverageMonthView>> = BTreeMap::new();
    for item in &coverage {
        grouped
            .entry(item.year)
            .or_default()
            .push(CoverageMonthView {
                month: item.month,
                statement_exists: item.statement_exists,
                statement_id: item.statement_id.clone(),
                statement_month: item.statement_month.clone(),
                period_start: item.period_start.clone(),
                period_end: item.period_end.clone(),
                linked_txn_count: item.linked_txn_count,
                manual_added_txn_count: item.manual_added_txn_count,
            });
    }

    let years = grouped
        .into_iter()
        .map(|(year, mut months)| {
            months.sort_by(|a, b| a.month.cmp(&b.month));
            CoverageYearView { year, months }
        })
        .collect::<Vec<_>>();

    let selected = match (params.year, params.month) {
        (Some(year), Some(month)) => {
            let selected_item = coverage
                .iter()
                .find(|item| item.year == year && item.month == month)
                .cloned()
                .unwrap_or_else(|| StatementCoverageMonth {
                    year,
                    month,
                    statement_exists: false,
                    statement_id: None,
                    statement_month: None,
                    period_start: None,
                    period_end: None,
                    linked_txn_count: 0,
                    manual_added_txn_count: 0,
                });

            Some(CoverageSelectedView {
                year,
                month,
                reusable: selected_item.statement_exists,
                statement_exists: selected_item.statement_exists,
                has_linked_txns: selected_item.linked_txn_count > 0,
                has_manual_added_txns_only: !selected_item.statement_exists
                    && selected_item.manual_added_txn_count > 0,
                policy_note: if selected_item.statement_exists {
                    "statement exists; extraction can be skipped"
                } else if selected_item.manual_added_txn_count > 0 {
                    "manual-added month only; continue normal import"
                } else {
                    "no statement coverage; continue normal import"
                },
                statement_id: selected_item.statement_id,
                statement_month: selected_item.statement_month,
                period_start: selected_item.period_start,
                period_end: selected_item.period_end,
                linked_txn_count: selected_item.linked_txn_count,
                manual_added_txn_count: selected_item.manual_added_txn_count,
            })
        }
        _ => None,
    };

    Ok(Json(CoverageResponse {
        account_id: params.account_id,
        years,
        selected,
    }))
}

pub async fn list_statement_transactions_handler(
    State(state): State<Arc<AppState>>,
    Path(statement_id): Path<String>,
) -> Result<Json<Vec<TransactionListItem>>, (StatusCode, String)> {
    if statement_id.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "statement_id is required".to_string(),
        ));
    }

    let rows = list_transactions_for_statement(&state.db, statement_id.trim())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::{create_import_handler, CreateImportRequest};
    use crate::state::AppState;
    use axum::extract::{Path, Query, State};
    use std::sync::Arc;
    use storage_sqlite::{
        connect, ensure_default_manual_account, run_migrations, upsert_or_get_statement,
    };

    fn temp_db_path() -> std::path::PathBuf {
        std::env::current_dir()
            .expect("cwd")
            .join(".tmp")
            .join(format!(
                "api-statements-test-{}.db",
                expense_core::new_idempotency_key()
            ))
    }

    #[tokio::test]
    async fn coverage_requires_account_id() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let result = get_statement_coverage_handler(
            State(state),
            Query(CoverageQueryParams {
                account_id: "".to_string(),
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
    async fn statement_transactions_returns_rows_for_statement() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");
        let statement = upsert_or_get_statement(
            &pool,
            &account_id,
            "2026-08-01",
            "2026-08-31",
            Some("2026-08"),
            Some("llamaextract_jobs"),
            Some("job-api"),
            Some("run-api"),
            &serde_json::json!({}),
            "statement_v1",
        )
        .await
        .expect("statement upsert");
        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, statement_id) VALUES ('tx-api-st-1', ?1, 'hash-api-st-1', 2100, 'CAD', 'Statement Row', '2026-08-08', 'manual', 'manual', 1.0, 'manual', ?2)")
            .bind(&account_id)
            .bind(&statement.id)
            .execute(&pool)
            .await
            .expect("insert tx");

        let state = Arc::new(AppState { db: pool.clone() });
        let rows = list_statement_transactions_handler(State(state), Path(statement.id))
            .await
            .expect("statement tx response")
            .0;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].description, "Statement Row");

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn coverage_selected_for_txn_only_month_is_not_reusable() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");
        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, created_at, statement_id) VALUES ('tx-api-manual-1', ?1, 'hash-api-manual-1', 2200, 'CAD', 'Manual Only', '2026-09-05', 'manual', 'manual', 1.0, 'manual', '2026-09-06 09:00:00', NULL)")
            .bind(&account_id)
            .execute(&pool)
            .await
            .expect("insert manual tx");

        let state = Arc::new(AppState { db: pool.clone() });
        let payload = get_statement_coverage_handler(
            State(state),
            Query(CoverageQueryParams {
                account_id,
                year: Some(2026),
                month: Some(9),
            }),
        )
        .await
        .expect("coverage response")
        .0;

        let selected = payload.selected.expect("selected payload");
        assert!(!selected.reusable);
        assert!(!selected.statement_exists);
        assert!(selected.has_manual_added_txns_only);
        assert_eq!(selected.manual_added_txn_count, 1);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn step4_reuse_flow_supports_statement_clickthrough_transactions() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");
        let statement = upsert_or_get_statement(
            &pool,
            &account_id,
            "2026-10-01",
            "2026-10-31",
            Some("2026-10"),
            Some("llamaextract_jobs"),
            Some("job-flow"),
            Some("run-flow"),
            &serde_json::json!({}),
            "statement_v1",
        )
        .await
        .expect("statement upsert");
        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, statement_id) VALUES ('tx-api-flow-1', ?1, 'hash-api-flow-1', 2500, 'CAD', 'Flow Linked Tx', '2026-10-08', 'manual', 'manual', 1.0, 'manual', ?2)")
            .bind(&account_id)
            .bind(&statement.id)
            .execute(&pool)
            .await
            .expect("insert linked tx");

        let state = Arc::new(AppState { db: pool.clone() });
        let coverage = get_statement_coverage_handler(
            State(state.clone()),
            Query(CoverageQueryParams {
                account_id: account_id.clone(),
                year: Some(2026),
                month: Some(10),
            }),
        )
        .await
        .expect("coverage response")
        .0;
        assert!(
            coverage
                .selected
                .as_ref()
                .map(|v| v.reusable)
                .unwrap_or(false)
        );

        let created = create_import_handler(
            State(state.clone()),
            Json(CreateImportRequest {
                file_name: None,
                parser_type: Some("pdf".to_string()),
                content_base64: None,
                extraction_mode: Some("managed".to_string()),
                account_id: Some(account_id.clone()),
                year: Some(2026),
                month: Some(10),
            }),
        )
        .await
        .expect("reused import")
        .1
        .0;
        assert!(created.reused);
        assert_eq!(created.status, "committed");

        let statement_rows = list_statements_handler(
            State(state.clone()),
            Query(StatementsQueryParams {
                account_id: account_id.clone(),
                year: Some(2026),
                month: Some(10),
                date_from: None,
                date_to: None,
            }),
        )
        .await
        .expect("statements list")
        .0;
        assert!(!statement_rows.is_empty());

        let tx_rows = list_statement_transactions_handler(State(state), Path(statement.id))
            .await
            .expect("statement tx list")
            .0;
        assert_eq!(tx_rows.len(), 1);
        assert_eq!(tx_rows[0].description, "Flow Linked Tx");

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }
}
