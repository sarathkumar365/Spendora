use anyhow::Context;
use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite};
use std::path::Path;

pub type SqlitePool = Pool<Sqlite>;

pub async fn connect(db_path: &Path) -> anyhow::Result<SqlitePool> {
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create db directory: {}", parent.display()))?;
    }

    let url = format!("sqlite://{}", db_path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await
        .with_context(|| format!("failed to connect sqlite at {}", db_path.display()))?;

    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .context("failed to enable foreign keys")?;

    Ok(pool)
}

pub async fn run_migrations(pool: &SqlitePool) -> anyhow::Result<()> {
    // File-based migration runner for predictable desktop bootstrap.
    for sql in MIGRATIONS {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

const MIGRATIONS: &[&str] = &[include_str!("../../../migrations/0001_init.sql")];

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_db_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock moved backwards")
            .as_nanos();
        std::env::temp_dir().join(format!("expense-test-{nanos}.db"))
    }

    #[tokio::test]
    async fn connect_creates_db_and_enables_foreign_keys() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");

        let fk = sqlx::query("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .expect("pragma read should succeed");
        let fk_value: i64 = fk.get(0);
        assert_eq!(fk_value, 1);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn migrations_are_idempotent_and_create_core_tables() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");

        run_migrations(&pool)
            .await
            .expect("first migration run should succeed");
        run_migrations(&pool)
            .await
            .expect("second migration run should succeed");

        let row = sqlx::query(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('transactions', 'job_runs', 'audit_events')",
        )
        .fetch_one(&pool)
        .await
        .expect("table presence check should succeed");
        let count: i64 = row.get(0);
        assert_eq!(count, 3);

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn unique_constraint_on_transactions_is_enforced() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        sqlx::query(
            "INSERT INTO connections (id, provider, status) VALUES ('conn-1', 'plaid', 'active')",
        )
        .execute(&pool)
        .await
        .expect("insert connection");

        sqlx::query(
            "INSERT INTO accounts (id, connection_id, name, currency_code) VALUES ('acct-1', 'conn-1', 'Checking', 'CAD')",
        )
        .execute(&pool)
        .await
        .expect("insert account");

        let insert_sql = "INSERT INTO transactions (id, account_id, external_txn_id, amount_cents, currency_code, description, booked_at, source) VALUES (?1, 'acct-1', 'txn-dup', 1000, 'CAD', 'Coffee', '2026-01-01T00:00:00Z', 'manual')";
        sqlx::query(insert_sql)
            .bind("tx-1")
            .execute(&pool)
            .await
            .expect("first insert should pass");

        let duplicate = sqlx::query(insert_sql).bind("tx-2").execute(&pool).await;
        assert!(
            duplicate.is_err(),
            "duplicate txn should violate unique constraint"
        );

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn unique_constraint_on_job_runs_idempotency_key_is_enforced() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let insert_sql = "INSERT INTO job_runs (id, job_type, payload_json, status, idempotency_key) VALUES (?1, 'sync', '{}', 'pending', 'idem-key-1')";
        sqlx::query(insert_sql)
            .bind("job-1")
            .execute(&pool)
            .await
            .expect("first insert should pass");

        let duplicate = sqlx::query(insert_sql).bind("job-2").execute(&pool).await;
        assert!(
            duplicate.is_err(),
            "duplicate idempotency key should violate unique constraint"
        );

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[tokio::test]
    async fn foreign_key_constraint_on_accounts_is_enforced() {
        let db_path = temp_db_path();
        let pool = connect(&db_path).await.expect("connect should succeed");
        run_migrations(&pool)
            .await
            .expect("migration should succeed");

        let orphan_account = sqlx::query(
            "INSERT INTO accounts (id, connection_id, name, currency_code) VALUES ('acct-orphan', 'missing-conn', 'Checking', 'CAD')",
        )
        .execute(&pool)
        .await;
        assert!(
            orphan_account.is_err(),
            "account insert should fail for missing connection FK"
        );

        drop(pool);
        let _ = tokio::fs::remove_file(db_path).await;
    }
}
