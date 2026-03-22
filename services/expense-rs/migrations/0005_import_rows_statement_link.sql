ALTER TABLE import_rows ADD COLUMN statement_id TEXT REFERENCES statements(id);

CREATE INDEX IF NOT EXISTS idx_import_rows_statement_id
  ON import_rows(statement_id);
