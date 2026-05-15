-- Phase 5a: Category Intelligence layer.
-- Adds merchant_signatures, merchant_category_assignments, category_resolution_history.
-- Extends categories with a stable `slug` column and seeds default categories.

-- 1. Extend categories with a slug for stable code-side lookup
ALTER TABLE categories ADD COLUMN slug TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_slug ON categories(slug) WHERE slug IS NOT NULL;

-- 2. Seed default categories (idempotent: INSERT OR IGNORE)
INSERT OR IGNORE INTO categories (id, name, slug, created_at, updated_at) VALUES
  ('cat-groceries',     'Groceries',            'groceries',     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-dining',        'Dining & Restaurants', 'dining',        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-transit',       'Transit & Fuel',       'transit',       CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-utilities',     'Utilities & Bills',    'utilities',     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-entertainment', 'Entertainment',        'entertainment', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-shopping',      'Shopping',             'shopping',      CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-subscriptions', 'Subscriptions',        'subscriptions', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-healthcare',    'Healthcare',           'healthcare',    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-travel',        'Travel',               'travel',        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-income',        'Income',               'income',        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-transfers',     'Transfers',            'transfers',     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-fees',          'Fees & Interest',      'fees',          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-other',         'Other',                'other',         CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- 3. merchant_signatures: one row per normalized merchant string
CREATE TABLE IF NOT EXISTS merchant_signatures (
  id TEXT PRIMARY KEY,
  normalized_key TEXT NOT NULL UNIQUE,
  display_label TEXT NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  txn_count INTEGER NOT NULL DEFAULT 0,
  total_cents INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_merchant_signatures_normalized_key
  ON merchant_signatures(normalized_key);

-- 4. merchant_category_assignments: current category per (merchant, category)
CREATE TABLE IF NOT EXISTS merchant_category_assignments (
  id TEXT PRIMARY KEY,
  merchant_signature_id TEXT NOT NULL REFERENCES merchant_signatures(id),
  category_id TEXT NOT NULL REFERENCES categories(id),
  source TEXT NOT NULL CHECK (source IN ('llm_suggested', 'user_confirmed', 'user_overridden')),
  included INTEGER NOT NULL DEFAULT 1,
  confidence REAL,
  confirmed_by_user_at TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(merchant_signature_id, category_id)
);

CREATE INDEX IF NOT EXISTS idx_mca_category_source_included
  ON merchant_category_assignments(category_id, source, included);

CREATE INDEX IF NOT EXISTS idx_mca_merchant
  ON merchant_category_assignments(merchant_signature_id);

-- 5. category_resolution_history: append-only audit log for future learning
CREATE TABLE IF NOT EXISTS category_resolution_history (
  id TEXT PRIMARY KEY,
  merchant_signature_id TEXT NOT NULL REFERENCES merchant_signatures(id),
  category_id TEXT NOT NULL REFERENCES categories(id),
  source TEXT NOT NULL,
  user_action TEXT,
  occurred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crh_merchant
  ON category_resolution_history(merchant_signature_id);

CREATE INDEX IF NOT EXISTS idx_crh_category_time
  ON category_resolution_history(category_id, occurred_at);
