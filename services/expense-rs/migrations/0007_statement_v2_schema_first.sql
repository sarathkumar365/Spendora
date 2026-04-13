ALTER TABLE transactions ADD COLUMN amount TEXT;
ALTER TABLE transactions ADD COLUMN details TEXT;
ALTER TABLE transactions ADD COLUMN transaction_date TEXT;
ALTER TABLE transactions ADD COLUMN type TEXT;

UPDATE transactions
SET
  amount = CASE WHEN amount IS NULL THEN printf('%.2f', amount_cents / 100.0) ELSE amount END,
  details = CASE WHEN details IS NULL THEN description ELSE details END,
  transaction_date = CASE WHEN transaction_date IS NULL THEN booked_at ELSE transaction_date END,
  type = CASE
    WHEN type IS NULL AND direction IN ('credit', 'debit') THEN direction
    ELSE type
  END;

CREATE INDEX IF NOT EXISTS idx_transactions_account_transaction_date
  ON transactions(account_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_statement_transaction_date
  ON transactions(statement_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_account_transaction_date_type
  ON transactions(account_id, transaction_date, type);

ALTER TABLE statements ADD COLUMN statement_period_start TEXT;
ALTER TABLE statements ADD COLUMN statement_period_end TEXT;
ALTER TABLE statements ADD COLUMN statement_date TEXT;
ALTER TABLE statements ADD COLUMN account_number_ending TEXT;
ALTER TABLE statements ADD COLUMN customer_name TEXT;
ALTER TABLE statements ADD COLUMN payment_due_date TEXT;
ALTER TABLE statements ADD COLUMN total_minimum_payment REAL;
ALTER TABLE statements ADD COLUMN interest_charged REAL;
ALTER TABLE statements ADD COLUMN account_balance REAL;
ALTER TABLE statements ADD COLUMN credit_limit REAL;
ALTER TABLE statements ADD COLUMN available_credit REAL;
ALTER TABLE statements ADD COLUMN estimated_payoff_years INTEGER;
ALTER TABLE statements ADD COLUMN estimated_payoff_months INTEGER;
ALTER TABLE statements ADD COLUMN credits_total REAL;
ALTER TABLE statements ADD COLUMN debits_total REAL;
ALTER TABLE statements ADD COLUMN statement_payload_json TEXT NOT NULL DEFAULT '{}';
