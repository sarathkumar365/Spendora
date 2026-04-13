## Step-Phased Implementation Plan: Statement V2 Schema-First Migration

### Summary
Implement the migration in 4 phases so we can safely move from the current internal transaction model to your schema-native model (`transaction_date`, `details`, `amount`, `type`) while persisting all statement fields and enabling account-per-card tracking via `accounts`.

### Step 1: Contract and Ingestion Layer
- Replace `statement_v2` schema with your LlamaParse schema as the single canonical contract.
- Update extraction payload parsing to read your field names directly (no alias mapping to old names).
- Change row validation policy: extraction never hard-fails on missing transaction fields; missing fields are recorded as row-level parse/review metadata.
- Keep sign semantics aligned with your requirement: incoming money positive, outgoing money negative.

### Step 2: Database Migration (Schema-First Storage)
- Ship a hard migration for transactions from old columns to new schema-native columns:
  - `booked_at -> transaction_date`
  - `description -> details`
  - `amount_cents -> amount` (decimal text, signed)
  - `direction -> type` (`credit|debit`)
- Add optional statement columns for all new schema sections:
  - statement period/date, account details, due/payment, account summary values, interest info, transaction subtotals.
- Add one raw `statement_payload_json` column for full schema snapshot persistence.
- Remove runtime dependence on legacy transaction direction/confidence/source fields in Statement V2 path.

### Step 3: Account/Card Tracking and Linking
- Use `accounts` as card entities (no new cards table).
- Keep extracted account metadata on statements/imports (`account_type`, `account_number_ending`, `customer_name`) for matching context and audit.
- Do not use `account_number_ending` (last4) as a standalone identity key for auto-linking.
- Add explicit card-resolution workflow in import pipeline:
  - if extraction metadata maps with high confidence to one existing account, auto-link;
  - otherwise mark import as `pending_card_resolution` and require user action: `select existing card` or `add new card`.
- Do not create fallback/unknown-card records when account metadata is missing.
- Persist resolution decision (`resolved_account_id`, resolution source, timestamp) and then link statement + transaction rows to that account.

### Step 4: API, Review/Commit Behavior, and Hardening
- Update statements APIs to expose all new optional schema fields immediately.
- Update transactions APIs to schema-native fields (`transaction_date`, `details`, `amount`, `type`).
- Allow commit with partial transaction rows (nullable transaction fields), preserving parse flags so users decide based on available details.
- Add API support for card resolution flow:
  - list existing cards/accounts for selection,
  - create new card/account from user input,
  - resolve a pending import to an account before commit/finalization,
  - expose import resolution status and required action.
- Add regression coverage for:
  - schema contract loading,
  - migration integrity,
  - partial-row extraction + commit,
  - pending card-resolution state and account linkage after user decision,
  - API response shape changes.

### Test Scenarios
- Full valid statement import persists all sections and links to correct account/card.
- Partial statement import (missing transaction fields) still extracts and commits.
- Import with low-confidence/no account metadata is held in pending resolution until user selects/adds a card.
- After user card resolution, statement + transactions link to selected/new `account_id`.
- API responses include new statement fields and renamed transaction fields.

### Assumptions
- `transactions.amount` is stored as exact decimal text.
- `type` domain is `credit|debit` for Statement V2.
- New fields are optional in DB/API even if extraction schema marks them required.
- Desktop app is single-user; account/card identity is local to that user context.
