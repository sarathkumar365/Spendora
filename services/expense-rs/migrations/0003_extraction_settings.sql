ALTER TABLE imports ADD COLUMN extraction_mode TEXT NOT NULL DEFAULT 'managed';
ALTER TABLE imports ADD COLUMN managed_provider_preference TEXT;
ALTER TABLE imports ADD COLUMN effective_provider TEXT;
ALTER TABLE imports ADD COLUMN provider_attempts_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE imports ADD COLUMN extraction_diagnostics_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE imports ADD COLUMN provider_attempt_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS app_settings (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
