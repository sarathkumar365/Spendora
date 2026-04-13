use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use storage_sqlite::{
    get_extraction_settings, upsert_extraction_settings, ExtractionSettings,
    DEFAULT_MAX_PROVIDER_RETRIES, MAX_PROVIDER_TIMEOUT_MS, MIN_PROVIDER_TIMEOUT_MS,
};

use crate::state::AppState;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExtractionSettingsPayload {
    pub default_extraction_mode: String,
    pub managed_fallback_enabled: bool,
    pub max_provider_retries: i64,
    pub provider_timeout_ms: i64,
}

pub async fn get_extraction_settings_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ExtractionSettingsPayload>, (StatusCode, String)> {
    let value = get_extraction_settings(&state.db)
        .await
        .map_err(internal_error)?;
    Ok(Json(ExtractionSettingsPayload {
        default_extraction_mode: value.default_extraction_mode,
        managed_fallback_enabled: value.managed_fallback_enabled,
        max_provider_retries: value.max_provider_retries,
        provider_timeout_ms: value.provider_timeout_ms,
    }))
}

pub async fn put_extraction_settings_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ExtractionSettingsPayload>,
) -> Result<Json<ExtractionSettingsPayload>, (StatusCode, String)> {
    if payload.default_extraction_mode != "managed"
        && payload.default_extraction_mode != "local_ocr"
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "default_extraction_mode must be managed or local_ocr".to_string(),
        ));
    }
    if payload.max_provider_retries < 1
        || payload.max_provider_retries > DEFAULT_MAX_PROVIDER_RETRIES
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("max_provider_retries must be between 1 and {DEFAULT_MAX_PROVIDER_RETRIES}"),
        ));
    }
    if payload.provider_timeout_ms < MIN_PROVIDER_TIMEOUT_MS
        || payload.provider_timeout_ms > MAX_PROVIDER_TIMEOUT_MS
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "provider_timeout_ms must be between {MIN_PROVIDER_TIMEOUT_MS} and {MAX_PROVIDER_TIMEOUT_MS}"
            ),
        ));
    }

    let saved = upsert_extraction_settings(
        &state.db,
        ExtractionSettings {
            default_extraction_mode: payload.default_extraction_mode,
            managed_fallback_enabled: payload.managed_fallback_enabled,
            max_provider_retries: payload.max_provider_retries,
            provider_timeout_ms: payload.provider_timeout_ms,
        },
    )
    .await
    .map_err(internal_error)?;

    Ok(Json(ExtractionSettingsPayload {
        default_extraction_mode: saved.default_extraction_mode,
        managed_fallback_enabled: saved.managed_fallback_enabled,
        max_provider_retries: saved.max_provider_retries,
        provider_timeout_ms: saved.provider_timeout_ms,
    }))
}

fn internal_error(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;
    use axum::extract::State;
    use std::sync::Arc;
    use storage_sqlite::{connect, run_migrations, DEFAULT_PROVIDER_TIMEOUT_MS};

    fn temp_db_path() -> std::path::PathBuf {
        std::env::current_dir()
            .expect("cwd")
            .join(".tmp")
            .join(format!(
                "api-settings-test-{}.db",
                expense_core::new_idempotency_key()
            ))
    }

    #[tokio::test]
    async fn settings_get_returns_defaults_when_unset() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let result = get_extraction_settings_handler(State(state))
            .await
            .expect("get settings");
        assert_eq!(result.default_extraction_mode, "managed");
        assert_eq!(result.max_provider_retries, DEFAULT_MAX_PROVIDER_RETRIES);
        assert_eq!(result.provider_timeout_ms, DEFAULT_PROVIDER_TIMEOUT_MS);
        assert!(result.managed_fallback_enabled);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn settings_put_rejects_retry_cap_above_three() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let result = put_extraction_settings_handler(
            State(state),
            Json(ExtractionSettingsPayload {
                default_extraction_mode: "managed".to_string(),
                managed_fallback_enabled: true,
                max_provider_retries: 4,
                provider_timeout_ms: 1000,
            }),
        )
        .await;
        assert!(result.is_err());

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn settings_put_rejects_timeout_above_max() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let result = put_extraction_settings_handler(
            State(state),
            Json(ExtractionSettingsPayload {
                default_extraction_mode: "managed".to_string(),
                managed_fallback_enabled: true,
                max_provider_retries: 3,
                provider_timeout_ms: 180_001,
            }),
        )
        .await;
        assert!(result.is_err());

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn settings_put_accepts_timeout_at_upper_bound() {
        let db_path = temp_db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        let state = Arc::new(AppState { db: pool.clone() });

        let result = put_extraction_settings_handler(
            State(state),
            Json(ExtractionSettingsPayload {
                default_extraction_mode: "managed".to_string(),
                managed_fallback_enabled: true,
                max_provider_retries: 3,
                provider_timeout_ms: 180_000,
            }),
        )
        .await
        .expect("settings update should succeed");
        assert_eq!(result.provider_timeout_ms, 180_000);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }
}
