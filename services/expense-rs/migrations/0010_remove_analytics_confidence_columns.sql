ALTER TABLE import_rows RENAME TO import_rows_old;

CREATE TABLE import_rows (
  id TEXT PRIMARY KEY,
  import_id TEXT NOT NULL,
  row_index INTEGER NOT NULL,
  normalized_json TEXT NOT NULL,
  parse_error TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  normalized_txn_hash TEXT,
  approved INTEGER NOT NULL DEFAULT 1,
  rejection_reason TEXT,
  account_id TEXT,
  statement_id TEXT REFERENCES statements(id),
  FOREIGN KEY (import_id) REFERENCES imports(id)
);

INSERT INTO import_rows (
  id,
  import_id,
  row_index,
  normalized_json,
  parse_error,
  created_at,
  normalized_txn_hash,
  approved,
  rejection_reason,
  account_id,
  statement_id
)
SELECT
  id,
  import_id,
  row_index,
  normalized_json,
  parse_error,
  created_at,
  normalized_txn_hash,
  approved,
  rejection_reason,
  account_id,
  statement_id
FROM import_rows_old;

DROP TABLE import_rows_old;

CREATE INDEX IF NOT EXISTS idx_import_rows_import_id ON import_rows(import_id);
CREATE INDEX IF NOT EXISTS idx_import_rows_hash ON import_rows(normalized_txn_hash);
CREATE INDEX IF NOT EXISTS idx_import_rows_statement_id ON import_rows(statement_id);

ALTER TABLE transactions RENAME TO transactions_old;

CREATE TABLE transactions (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  external_txn_id TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  currency_code TEXT NOT NULL DEFAULT 'CAD',
  description TEXT NOT NULL,
  booked_at TEXT NOT NULL,
  source TEXT NOT NULL,
  classification_source TEXT,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  statement_id TEXT REFERENCES statements(id),
  direction TEXT NOT NULL DEFAULT 'unknown',
  direction_confidence REAL,
  direction_source TEXT NOT NULL DEFAULT 'legacy',
  amount TEXT,
  details TEXT,
  transaction_date TEXT,
  type TEXT,
  UNIQUE (account_id, external_txn_id),
  FOREIGN KEY (account_id) REFERENCES accounts(id)
);

INSERT INTO transactions (
  id,
  account_id,
  external_txn_id,
  amount_cents,
  currency_code,
  description,
  booked_at,
  source,
  classification_source,
  explanation,
  created_at,
  updated_at,
  statement_id,
  direction,
  direction_confidence,
  direction_source,
  amount,
  details,
  transaction_date,
  type
)
SELECT
  id,
  account_id,
  external_txn_id,
  amount_cents,
  currency_code,
  description,
  booked_at,
  source,
  classification_source,
  explanation,
  created_at,
  updated_at,
  statement_id,
  direction,
  direction_confidence,
  direction_source,
  amount,
  details,
  transaction_date,
  type
FROM transactions_old;

DROP TABLE transactions_old;

CREATE INDEX IF NOT EXISTS idx_transactions_account_booked_at ON transactions(account_id, booked_at);
CREATE INDEX IF NOT EXISTS idx_transactions_source ON transactions(source);
CREATE INDEX IF NOT EXISTS idx_transactions_statement_booked_at ON transactions(statement_id, booked_at);
CREATE INDEX IF NOT EXISTS idx_transactions_account_booked_at_direction
  ON transactions(account_id, booked_at, direction);
CREATE INDEX IF NOT EXISTS idx_transactions_account_transaction_date
  ON transactions(account_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_statement_transaction_date
  ON transactions(statement_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_account_transaction_date_type
  ON transactions(account_id, transaction_date, type);
