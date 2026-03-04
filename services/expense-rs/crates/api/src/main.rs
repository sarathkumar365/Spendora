use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use clap::Parser;
use expense_core::{default_app_data_dir, new_health_status, HealthStatus};
use serde::Serialize;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use storage_sqlite::{connect, run_migrations, SqlitePool};
use tracing::info;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = default_db_path())]
    db_path: String,
    #[arg(long, default_value_t = 8081)]
    port: u16,
    #[arg(long, default_value_t = true)]
    migrate: bool,
}

#[derive(Clone)]
struct AppState {
    db: SqlitePool,
}

#[derive(Serialize)]
struct Diagnostics {
    service: &'static str,
    sqlite: &'static str,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();

    let db_path = PathBuf::from(args.db_path);
    let pool = connect(&db_path).await?;

    if args.migrate {
        run_migrations(&pool).await?;
    }

    let state = Arc::new(AppState { db: pool });
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/health", get(health))
        .route("/api/v1/diagnostics", get(diagnostics))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], args.port));
    info!(%addr, "expense-api listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<HealthStatus> {
    Json(new_health_status("expense-api"))
}

async fn diagnostics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let ping = sqlite_ping_status(&state.db).await;

    Json(Diagnostics {
        service: "expense-api",
        sqlite: ping,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_returns_ok_payload() {
        let Json(status) = health().await;
        assert_eq!(status.service, "expense-api");
        assert_eq!(status.status, "ok");
    }

    #[tokio::test]
    async fn sqlite_ping_status_returns_ok_for_valid_pool() {
        let db_path = std::env::temp_dir().join(format!(
            "expense-api-test-{}.db",
            expense_core::new_idempotency_key()
        ));
        let pool = storage_sqlite::connect(&db_path)
            .await
            .expect("db should connect");
        let ping = sqlite_ping_status(&pool).await;
        assert_eq!(ping, "ok");
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
}
