DELETE FROM transactions
WHERE account_id = 'manual-default-account';

DELETE FROM statements
WHERE account_id = 'manual-default-account';

DELETE FROM import_rows
WHERE account_id = 'manual-default-account';

UPDATE imports
SET
  resolved_account_id = NULL,
  card_resolution_status = 'pending',
  card_resolution_reason = 'legacy_account_removed',
  card_resolved_at = NULL,
  updated_at = CURRENT_TIMESTAMP
WHERE resolved_account_id = 'manual-default-account';

DELETE FROM accounts
WHERE id = 'manual-default-account';
