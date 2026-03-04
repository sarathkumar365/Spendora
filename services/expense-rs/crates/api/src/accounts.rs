use axum::{extract::State, Json};
use std::sync::Arc;
use storage_sqlite::list_accounts;

use crate::state::AppState;

pub async fn get_accounts_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<storage_sqlite::AccountItem>>, (axum::http::StatusCode, String)> {
    let result = list_accounts(&state.db).await.map_err(|err| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )
    })?;
    Ok(Json(result))
}
