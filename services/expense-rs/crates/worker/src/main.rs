use axum::{routing::get, Json, Router};
use clap::Parser;
use expense_core::{default_app_data_dir, new_health_status, HealthStatus};
use std::{net::SocketAddr, path::PathBuf};
use storage_sqlite::{connect, run_migrations};
use tokio::time::{sleep, Duration};
use tracing::{error, info};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = default_db_path())]
    db_path: String,
    #[arg(long, default_value_t = 8082)]
    port: u16,
    #[arg(long, default_value_t = true)]
    migrate: bool,
    #[arg(long, default_value_t = 10)]
    poll_seconds: u64,
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

    let health_addr = SocketAddr::from(([127, 0, 0, 1], args.port));
    tokio::spawn(async move {
        let app = Router::new()
            .route("/health", get(health))
            .route("/api/v1/health", get(health));

        match tokio::net::TcpListener::bind(health_addr).await {
            Ok(listener) => {
                info!(%health_addr, "expense-worker health endpoint listening");
                if let Err(err) = axum::serve(listener, app).await {
                    error!(error = %err, "worker health server failed");
                }
            }
            Err(err) => error!(error = %err, "worker health bind failed"),
        }
    });

    info!(poll_seconds = args.poll_seconds, "worker loop started");
    loop {
        info!("worker heartbeat: no jobs yet");
        sleep(Duration::from_secs(args.poll_seconds)).await;
    }
}

async fn health() -> Json<HealthStatus> {
    Json(new_health_status("expense-worker"))
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "worker=info,axum=info".into()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_returns_ok_payload() {
        let Json(status) = health().await;
        assert_eq!(status.service, "expense-worker");
        assert_eq!(status.status, "ok");
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
