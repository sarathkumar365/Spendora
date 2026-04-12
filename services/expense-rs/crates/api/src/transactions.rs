use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use storage_sqlite::{query_transactions, TransactionListItem, TransactionQuery};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct TransactionsQueryParams {
    pub q: Option<String>,
    pub account_id: Option<String>,
    pub source: Option<String>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SafeSummaryQueryParams {
    pub account_id: String,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub include_unknown: Option<bool>,
}

#[derive(Debug, Serialize, Default, PartialEq)]
pub struct SafeSummaryResponse {
    pub inflow_cents: i64,
    pub outflow_cents: i64,
    pub net_cents: i64,
    pub included_count: i64,
    pub excluded_unknown_count: i64,
    pub safety_note: String,
}

pub async fn get_transactions_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TransactionsQueryParams>,
) -> Result<Json<Vec<TransactionListItem>>, (axum::http::StatusCode, String)> {
    let result = query_transactions(
        &state.db,
        TransactionQuery {
            q: params.q,
            account_id: params.account_id,
            source: params.source,
            date_from: params.date_from,
            date_to: params.date_to,
            limit: params.limit.unwrap_or(100),
            offset: params.offset.unwrap_or(0),
        },
    )
    .await
    .map_err(|err| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )
    })?;

    Ok(Json(result))
}

pub async fn get_transactions_safe_summary_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SafeSummaryQueryParams>,
) -> Result<Json<SafeSummaryResponse>, (axum::http::StatusCode, String)> {
    if params.account_id.trim().is_empty() {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "account_id is required".to_string(),
        ));
    }

    let include_unknown = params.include_unknown.unwrap_or(false);
    let mut offset = 0_i64;
    let limit = 1_000_i64;
    let mut all = Vec::<TransactionListItem>::new();
    loop {
        let batch = query_transactions(
            &state.db,
            TransactionQuery {
                q: None,
                account_id: Some(params.account_id.trim().to_string()),
                source: None,
                date_from: params.date_from.clone(),
                date_to: params.date_to.clone(),
                limit,
                offset,
            },
        )
        .await
        .map_err(|err| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            )
        })?;

        let batch_len = batch.len() as i64;
        all.extend(batch);
        if batch_len < limit {
            break;
        }
        offset += limit;
    }

    let mut response = SafeSummaryResponse {
        safety_note: if include_unknown {
            "unknown direction rows are included by amount sign".to_string()
        } else {
            "unknown direction rows are excluded by default".to_string()
        },
        ..SafeSummaryResponse::default()
    };

    for item in all {
        match item.direction.as_str() {
            "credit" => {
                response.inflow_cents += item.amount_cents.abs();
                response.included_count += 1;
            }
            "debit" => {
                response.outflow_cents += item.amount_cents.abs();
                response.included_count += 1;
            }
            "unknown" if include_unknown => {
                if item.amount_cents >= 0 {
                    response.inflow_cents += item.amount_cents.abs();
                } else {
                    response.outflow_cents += item.amount_cents.abs();
                }
                response.included_count += 1;
            }
            "unknown" => {
                response.excluded_unknown_count += 1;
            }
            _ => {}
        }
    }

    response.net_cents = response.inflow_cents - response.outflow_cents;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;
    use axum::extract::{Query, State};
    use std::sync::Arc;
    use storage_sqlite::{connect, ensure_default_manual_account, run_migrations};

    fn temp_db_path() -> std::path::PathBuf {
        std::env::current_dir()
            .expect("cwd")
            .join(".tmp")
            .join(format!(
                "api-transactions-test-{}.db",
                expense_core::new_idempotency_key()
            ))
    }

    #[tokio::test]
    async fn transactions_handler_applies_filters() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation) VALUES ('tx-api-1', ?1, 'h-api-1', 1250, 'CAD', 'Coffee Run', '2026-03-10', 'manual', 'manual', 0.9, 'manual import')")
            .bind(&account_id)
            .execute(&pool)
            .await
            .expect("insert tx1");
        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation) VALUES ('tx-api-2', ?1, 'h-api-2', 8200, 'CAD', 'Salary', '2026-03-11', 'manual', 'manual', 1.0, 'manual import')")
            .bind(&account_id)
            .execute(&pool)
            .await
            .expect("insert tx2");

        let state = Arc::new(AppState { db: pool.clone() });
        let resp = get_transactions_handler(
            State(state),
            Query(TransactionsQueryParams {
                q: Some("coffee".to_string()),
                account_id: Some(account_id),
                source: Some("manual".to_string()),
                date_from: Some("2026-03-10".to_string()),
                date_to: Some("2026-03-10".to_string()),
                limit: Some(10),
                offset: Some(0),
            }),
        )
        .await
        .expect("handler success")
        .0;

        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].description, "Coffee Run");
        assert_eq!(resp[0].direction, "unknown");
        assert_eq!(resp[0].direction_source, "legacy");

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn safe_summary_excludes_unknown_by_default_and_can_include() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let account_id = ensure_default_manual_account(&pool)
            .await
            .expect("default account");

        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, direction, direction_source) VALUES ('tx-safe-1', ?1, 'h-safe-1', -1200, 'CAD', 'Debit', '2026-03-10', 'manual', 'manual', 1.0, 'manual import', 'debit', 'model')")
            .bind(&account_id)
            .execute(&pool)
            .await
            .expect("insert tx1");
        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, direction, direction_source) VALUES ('tx-safe-2', ?1, 'h-safe-2', 3000, 'CAD', 'Credit', '2026-03-11', 'manual', 'manual', 1.0, 'manual import', 'credit', 'model')")
            .bind(&account_id)
            .execute(&pool)
            .await
            .expect("insert tx2");
        sqlx::query("INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source, classification_source, confidence, explanation, direction, direction_source) VALUES ('tx-safe-3', ?1, 'h-safe-3', -800, 'CAD', 'Unknown', '2026-03-12', 'manual', 'manual', 1.0, 'manual import', 'unknown', 'legacy')")
            .bind(&account_id)
            .execute(&pool)
            .await
            .expect("insert tx3");

        let state = Arc::new(AppState { db: pool.clone() });
        let excluded = get_transactions_safe_summary_handler(
            State(state.clone()),
            Query(SafeSummaryQueryParams {
                account_id: account_id.clone(),
                date_from: None,
                date_to: None,
                include_unknown: None,
            }),
        )
        .await
        .expect("summary excluded")
        .0;
        assert_eq!(excluded.inflow_cents, 3000);
        assert_eq!(excluded.outflow_cents, 1200);
        assert_eq!(excluded.excluded_unknown_count, 1);

        let included = get_transactions_safe_summary_handler(
            State(state),
            Query(SafeSummaryQueryParams {
                account_id,
                date_from: None,
                date_to: None,
                include_unknown: Some(true),
            }),
        )
        .await
        .expect("summary included")
        .0;
        assert_eq!(included.inflow_cents, 3000);
        assert_eq!(included.outflow_cents, 2000);
        assert_eq!(included.excluded_unknown_count, 0);
        assert_eq!(included.net_cents, 1000);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }
}
