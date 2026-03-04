ALTER TABLE imports ADD COLUMN parser_type TEXT NOT NULL DEFAULT 'pdf';
ALTER TABLE imports ADD COLUMN source_hash TEXT;
ALTER TABLE imports ADD COLUMN review_required_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE imports ADD COLUMN committed_at TEXT;
ALTER TABLE imports ADD COLUMN summary_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE imports ADD COLUMN errors_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE imports ADD COLUMN warnings_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE imports ADD COLUMN content_base64 TEXT NOT NULL DEFAULT '';

ALTER TABLE import_rows ADD COLUMN normalized_txn_hash TEXT;
ALTER TABLE import_rows ADD COLUMN approved INTEGER NOT NULL DEFAULT 1;
ALTER TABLE import_rows ADD COLUMN rejection_reason TEXT;
ALTER TABLE import_rows ADD COLUMN account_id TEXT;

CREATE INDEX IF NOT EXISTS idx_import_rows_import_id ON import_rows(import_id);
CREATE INDEX IF NOT EXISTS idx_import_rows_hash ON import_rows(normalized_txn_hash);
CREATE INDEX IF NOT EXISTS idx_transactions_account_booked_at ON transactions(account_id, booked_at);
CREATE INDEX IF NOT EXISTS idx_transactions_source ON transactions(source);
