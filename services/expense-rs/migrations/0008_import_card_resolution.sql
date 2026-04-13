ALTER TABLE accounts ADD COLUMN account_type TEXT;
ALTER TABLE accounts ADD COLUMN account_number_ending TEXT;
ALTER TABLE accounts ADD COLUMN customer_name TEXT;
ALTER TABLE accounts ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE imports ADD COLUMN resolved_account_id TEXT REFERENCES accounts(id);
ALTER TABLE imports ADD COLUMN card_resolution_status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE imports ADD COLUMN card_resolution_reason TEXT;
ALTER TABLE imports ADD COLUMN card_resolution_metadata_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE imports ADD COLUMN card_resolved_at TEXT;

UPDATE imports
SET resolved_account_id = (
  SELECT ir.account_id
  FROM import_rows ir
  WHERE ir.import_id = imports.id
    AND ir.account_id IS NOT NULL
  ORDER BY ir.row_index ASC
  LIMIT 1
)
WHERE resolved_account_id IS NULL;

UPDATE imports
SET card_resolution_status = CASE
  WHEN resolved_account_id IS NOT NULL THEN 'resolved'
  ELSE 'pending'
END
WHERE card_resolution_status IS NULL
   OR card_resolution_status NOT IN ('pending', 'resolved');

UPDATE imports
SET card_resolution_metadata_json = '{}'
WHERE card_resolution_metadata_json IS NULL OR trim(card_resolution_metadata_json) = '';

CREATE INDEX IF NOT EXISTS idx_imports_card_resolution_status
  ON imports(card_resolution_status);

CREATE INDEX IF NOT EXISTS idx_imports_resolved_account_id
  ON imports(resolved_account_id);

CREATE INDEX IF NOT EXISTS idx_accounts_card_metadata
  ON accounts(account_type, account_number_ending, customer_name);
