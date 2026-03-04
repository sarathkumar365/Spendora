use axum::{http::StatusCode, Json};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct DeferredResponse {
    pub message: String,
}

pub async fn deferred_plaid() -> (StatusCode, Json<DeferredResponse>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(DeferredResponse {
            message: "Plaid deferred to future phase".to_string(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deferred_plaid_returns_501_and_clear_message() {
        let (status, Json(body)) = deferred_plaid().await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(body.message, "Plaid deferred to future phase");
    }
}
