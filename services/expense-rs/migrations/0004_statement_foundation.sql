CREATE TABLE IF NOT EXISTS statements (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  period_start TEXT NOT NULL,
  period_end TEXT NOT NULL,
  statement_month TEXT,
  provider_name TEXT,
  provider_job_id TEXT,
  provider_run_id TEXT,
  provider_metadata_json TEXT NOT NULL DEFAULT '{}',
  schema_version TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_statements_account_period_unique
  ON statements(account_id, period_start, period_end);

ALTER TABLE transactions ADD COLUMN statement_id TEXT REFERENCES statements(id);

CREATE INDEX IF NOT EXISTS idx_transactions_statement_booked_at
  ON transactions(statement_id, booked_at);
