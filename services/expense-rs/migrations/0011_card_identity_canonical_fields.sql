ALTER TABLE accounts ADD COLUMN account_number_last4_canonical TEXT;
ALTER TABLE accounts ADD COLUMN customer_name_canonical TEXT;
ALTER TABLE accounts ADD COLUMN account_descriptor_canonical TEXT;

CREATE INDEX IF NOT EXISTS idx_accounts_card_metadata_canonical
  ON accounts(account_number_last4_canonical, customer_name_canonical, account_descriptor_canonical);
