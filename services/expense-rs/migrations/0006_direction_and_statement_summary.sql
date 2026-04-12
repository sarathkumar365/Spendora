ALTER TABLE transactions ADD COLUMN direction TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE transactions ADD COLUMN direction_confidence REAL;
ALTER TABLE transactions ADD COLUMN direction_source TEXT NOT NULL DEFAULT 'legacy';

ALTER TABLE statements ADD COLUMN opening_balance_cents INTEGER;
ALTER TABLE statements ADD COLUMN opening_balance_date TEXT;
ALTER TABLE statements ADD COLUMN closing_balance_cents INTEGER;
ALTER TABLE statements ADD COLUMN closing_balance_date TEXT;
ALTER TABLE statements ADD COLUMN total_debits_cents INTEGER;
ALTER TABLE statements ADD COLUMN total_credits_cents INTEGER;
ALTER TABLE statements ADD COLUMN account_type TEXT;
ALTER TABLE statements ADD COLUMN account_number_masked TEXT;
ALTER TABLE statements ADD COLUMN currency_code TEXT;

CREATE INDEX IF NOT EXISTS idx_transactions_account_booked_at_direction
  ON transactions(account_id, booked_at, direction);

CREATE INDEX IF NOT EXISTS idx_statements_account_period
  ON statements(account_id, period_start, period_end);
