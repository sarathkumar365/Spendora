mod accounts;
mod imports;
mod plaid;
mod settings;
mod state;
mod transactions;

use axum::{
    http::{header, HeaderName, HeaderValue, Method},
    extract::State,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use expense_core::{
    default_app_data_dir, load_extraction_runtime_config_from_env, load_statement_blueprint_schema,
    new_health_status, HealthStatus,
};
use serde::Serialize;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use storage_sqlite::{
    connect, ensure_default_manual_account, get_llama_agent_readiness, run_migrations,
    LlamaAgentReadiness, SqlitePool,
};
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
use tracing::info;

use state::AppState;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = default_db_path())]
    db_path: String,
    #[arg(long, default_value_t = 8081)]
    port: u16,
    #[arg(long, default_value_t = true)]
    migrate: bool,
}

#[derive(Serialize)]
struct Diagnostics {
    service: &'static str,
    sqlite: &'static str,
    llama_agent_readiness: Option<LlamaAgentReadiness>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    validate_extraction_runtime_contract()?;

    let db_path = PathBuf::from(args.db_path);
    let pool = connect(&db_path).await?;

    if args.migrate {
        run_migrations(&pool).await?;
    }

    ensure_default_manual_account(&pool).await?;

    let state = Arc::new(AppState { db: pool });
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/health", get(health))
        .route("/api/v1/diagnostics", get(diagnostics))
        .route("/api/v1/imports", post(imports::create_import_handler))
        .route(
            "/api/v1/imports/:id/status",
            get(imports::get_import_status_handler),
        )
        .route(
            "/api/v1/imports/:id/review",
            get(imports::get_import_review_handler).post(imports::update_import_review_handler),
        )
        .route(
            "/api/v1/imports/:id/commit",
            post(imports::commit_import_handler),
        )
        .route(
            "/api/v1/transactions",
            get(transactions::get_transactions_handler),
        )
        .route("/api/v1/accounts", get(accounts::get_accounts_handler))
        .route(
            "/api/v1/settings/extraction",
            get(settings::get_extraction_settings_handler)
                .put(settings::put_extraction_settings_handler),
        )
        .route(
            "/api/v1/connections/plaid/link-token",
            post(plaid::deferred_plaid),
        )
        .route(
            "/api/v1/connections/plaid/exchange",
            post(plaid::deferred_plaid),
        )
        .layer(build_cors_layer()?)
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], args.port));
    info!(%addr, "expense-api listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn validate_extraction_runtime_contract() -> anyhow::Result<()> {
    let extraction_config = load_extraction_runtime_config_from_env()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    load_statement_blueprint_schema(&extraction_config.llama_schema_version)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(())
}

async fn health() -> Json<HealthStatus> {
    Json(new_health_status("expense-api"))
}

async fn diagnostics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let ping = sqlite_ping_status(&state.db).await;
    let readiness = get_llama_agent_readiness(&state.db).await.ok().flatten();

    Json(Diagnostics {
        service: "expense-api",
        sqlite: ping,
        llama_agent_readiness: readiness,
    })
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "api=info,axum=info".into()),
        )
        .compact()
        .init();
}

fn default_db_path() -> String {
    default_app_data_dir()
        .join("expense.db")
        .display()
        .to_string()
}

async fn sqlite_ping_status(db: &SqlitePool) -> &'static str {
    sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(db)
        .await
        .map(|_| "ok")
        .unwrap_or("error")
}

fn build_cors_layer() -> anyhow::Result<CorsLayer> {
    let allow_origins = parse_allowed_origins()?;
    let allow_methods = parse_allowed_methods()?;
    let allow_headers = parse_allowed_headers()?;

    Ok(CorsLayer::new()
        .allow_origin(AllowOrigin::list(allow_origins))
        .allow_methods(AllowMethods::list(allow_methods))
        .allow_headers(AllowHeaders::list(allow_headers)))
}

fn parse_allowed_origins() -> anyhow::Result<Vec<HeaderValue>> {
    let raw = std::env::var("CORS_ALLOWED_ORIGINS").unwrap_or_else(|_| {
        // Desktop + local dev defaults.
        "http://127.0.0.1:1420,http://localhost:1420".to_string()
    });
    let origins = raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            validate_origin(item).and_then(|valid| {
                if !valid {
                    anyhow::bail!("invalid value in CORS_ALLOWED_ORIGINS: {item}");
                }
                HeaderValue::from_str(item).map_err(|_| {
                    anyhow::anyhow!("invalid value in CORS_ALLOWED_ORIGINS: {item}")
                })
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if origins.is_empty() {
        anyhow::bail!("CORS_ALLOWED_ORIGINS must include at least one origin");
    }
    Ok(origins)
}

fn parse_allowed_methods() -> anyhow::Result<Vec<Method>> {
    let raw = std::env::var("CORS_ALLOWED_METHODS")
        .unwrap_or_else(|_| "GET,POST,PUT,DELETE,OPTIONS".to_string());
    let methods = raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            Method::from_bytes(item.as_bytes())
                .map_err(|_| anyhow::anyhow!("invalid HTTP method in CORS_ALLOWED_METHODS: {item}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if methods.is_empty() {
        anyhow::bail!("CORS_ALLOWED_METHODS must include at least one method");
    }
    Ok(methods)
}

fn parse_allowed_headers() -> anyhow::Result<Vec<HeaderName>> {
    let raw = std::env::var("CORS_ALLOWED_HEADERS")
        .unwrap_or_else(|_| "Content-Type,Authorization".to_string());
    let headers = raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse::<HeaderName>()
                .map_err(|_| anyhow::anyhow!("invalid header name in CORS_ALLOWED_HEADERS: {item}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if headers.is_empty() {
        anyhow::bail!("CORS_ALLOWED_HEADERS must include at least one header");
    }
    Ok(headers)
}

fn validate_origin(value: &str) -> anyhow::Result<bool> {
    let uri = value
        .parse::<axum::http::Uri>()
        .map_err(|_| anyhow::anyhow!("failed to parse origin"))?;
    Ok(uri.scheme().is_some() && uri.authority().is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::{Mutex, OnceLock};
    use storage_sqlite::{upsert_llama_agent_readiness, LlamaAgentReadinessState};
    use tower::util::ServiceExt;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[tokio::test]
    async fn health_returns_ok_payload() {
        let Json(status) = health().await;
        assert_eq!(status.service, "expense-api");
        assert_eq!(status.status, "ok");
    }

    #[tokio::test]
    async fn diagnostics_returns_llama_agent_readiness_when_present() {
        let db_path = std::env::current_dir()
            .expect("cwd")
            .join(".tmp")
            .join(format!("api-diagnostics-test-{}.db", expense_core::new_idempotency_key()));
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let pool = connect(&db_path).await.expect("connect");
        run_migrations(&pool).await.expect("migrate");
        ensure_default_manual_account(&pool).await.expect("default account");

        upsert_llama_agent_readiness(
            &pool,
            &LlamaAgentReadiness {
                state: LlamaAgentReadinessState::Configured,
                agent_name: "agent--statement_v1".to_string(),
                schema_version: "statement_v1".to_string(),
                agent_id: Some("agent-123".to_string()),
                checked_at: chrono::Utc::now().to_rfc3339(),
                error_code: None,
                error_message: None,
            },
        )
        .await
        .expect("save readiness");

        let state = Arc::new(AppState { db: pool.clone() });
        let response = diagnostics(State(state)).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(
            payload
                .get("llama_agent_readiness")
                .and_then(|v| v.get("state"))
                .and_then(|v| v.as_str()),
            Some("configured")
        );

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[test]
    fn default_db_path_points_to_expense_db_file() {
        let value = default_db_path();
        assert!(
            value.ends_with("expense.db"),
            "default db path should end with expense.db, got: {value}"
        );
    }

    #[test]
    fn cors_origin_parser_rejects_invalid_values() {
        unsafe { std::env::set_var("CORS_ALLOWED_ORIGINS", "http://ok,not-an-origin") };
        let err = parse_allowed_origins()
            .expect_err("invalid origin should fail");
        assert!(err.to_string().contains("invalid value in CORS_ALLOWED_ORIGINS"));
        unsafe { std::env::remove_var("CORS_ALLOWED_ORIGINS") };
    }

    #[test]
    fn cors_method_parser_rejects_invalid_values() {
        unsafe { std::env::set_var("CORS_ALLOWED_METHODS", "GET,B@D") };
        let err = parse_allowed_methods().expect_err("invalid method should fail");
        assert!(err
            .to_string()
            .contains("invalid HTTP method in CORS_ALLOWED_METHODS"));
        unsafe { std::env::remove_var("CORS_ALLOWED_METHODS") };
    }

    #[test]
    fn cors_header_parser_rejects_invalid_values() {
        unsafe { std::env::set_var("CORS_ALLOWED_HEADERS", "Content-Type, bad header") };
        let err = parse_allowed_headers().expect_err("invalid header should fail");
        assert!(err
            .to_string()
            .contains("invalid header name in CORS_ALLOWED_HEADERS"));
        unsafe { std::env::remove_var("CORS_ALLOWED_HEADERS") };
    }

    #[tokio::test]
    async fn cors_allows_configured_origin_and_blocks_unknown_origin() {
        unsafe {
            std::env::set_var("CORS_ALLOWED_ORIGINS", "http://127.0.0.1:1420");
            std::env::set_var("CORS_ALLOWED_METHODS", "GET,OPTIONS");
            std::env::set_var("CORS_ALLOWED_HEADERS", "Content-Type,Authorization");
        }

        let app = Router::new()
            .route("/api/v1/health", get(health))
            .layer(build_cors_layer().expect("cors layer"));

        let allowed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/health")
                    .header(header::ORIGIN, "http://127.0.0.1:1420")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            allowed.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("http://127.0.0.1:1420"))
        );
        assert!(allowed
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
            .is_none());

        let blocked = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/health")
                    .header(header::ORIGIN, "https://evil.example")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(blocked
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none());

        unsafe {
            std::env::remove_var("CORS_ALLOWED_ORIGINS");
            std::env::remove_var("CORS_ALLOWED_METHODS");
            std::env::remove_var("CORS_ALLOWED_HEADERS");
        }
    }

    #[tokio::test]
    async fn cors_preflight_allows_expected_header_and_rejects_unknown_header() {
        unsafe {
            std::env::set_var("CORS_ALLOWED_ORIGINS", "http://127.0.0.1:1420");
            std::env::set_var("CORS_ALLOWED_METHODS", "GET,OPTIONS");
            std::env::set_var("CORS_ALLOWED_HEADERS", "Content-Type,Authorization");
        }

        let app = Router::new()
            .route("/api/v1/transactions", get(|| async { "ok" }))
            .layer(build_cors_layer().expect("cors layer"));

        let allowed_preflight = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/v1/transactions")
                    .header(header::ORIGIN, "http://127.0.0.1:1420")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(allowed_preflight.status(), axum::http::StatusCode::OK);
        assert!(allowed_preflight
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
            .is_some());

        let denied_preflight = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/v1/transactions")
                    .header(header::ORIGIN, "http://127.0.0.1:1420")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "x-secret-header")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(denied_preflight.status(), axum::http::StatusCode::OK);
        let denied_allow_headers = denied_preflight
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            !denied_allow_headers.contains("x-secret-header"),
            "disallowed header should not be present in Access-Control-Allow-Headers"
        );

        unsafe {
            std::env::remove_var("CORS_ALLOWED_ORIGINS");
            std::env::remove_var("CORS_ALLOWED_METHODS");
            std::env::remove_var("CORS_ALLOWED_HEADERS");
        }
    }

    #[test]
    fn extraction_runtime_contract_fails_when_required_env_missing() {
        let _guard = env_lock().lock().expect("env lock");
        for key in [
            "LLAMA_CLOUD_API_KEY",
            "LLAMA_AGENT_NAME",
            "LLAMA_SCHEMA_VERSION",
            "LLAMA_CLOUD_ORGANIZATION_ID",
            "LLAMA_CLOUD_PROJECT_ID",
        ] {
            unsafe { std::env::remove_var(key) };
        }
        let err = validate_extraction_runtime_contract().expect_err("expected missing env");
        assert!(err
            .to_string()
            .contains("EXTRACTION_CONFIG_MISSING_REQUIRED_ENV"));
    }

    #[test]
    fn extraction_runtime_contract_succeeds_with_required_env() {
        let _guard = env_lock().lock().expect("env lock");
        unsafe {
            std::env::set_var("LLAMA_CLOUD_API_KEY", "x");
            std::env::set_var("LLAMA_AGENT_NAME", "agent");
            std::env::set_var("LLAMA_SCHEMA_VERSION", "statement_v1");
            std::env::remove_var("LLAMA_CLOUD_ORGANIZATION_ID");
            std::env::remove_var("LLAMA_CLOUD_PROJECT_ID");
        }

        validate_extraction_runtime_contract().expect("runtime contract should validate");

        for key in [
            "LLAMA_CLOUD_API_KEY",
            "LLAMA_AGENT_NAME",
            "LLAMA_SCHEMA_VERSION",
            "LLAMA_CLOUD_ORGANIZATION_ID",
            "LLAMA_CLOUD_PROJECT_ID",
        ] {
            unsafe { std::env::remove_var(key) };
        }
    }
}
