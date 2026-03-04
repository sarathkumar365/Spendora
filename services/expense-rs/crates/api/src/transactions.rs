use axum::{
    extract::{Query, State},
    Json,
};
use serde::Deserialize;
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

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }
}
