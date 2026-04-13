# Revised Plan: Card-Only Import Model + Global Data View

## Summary
Replace legacy managed-account behavior with a strict card-only model in `accounts`, auto-resolve imports to cards when metadata is complete, and make Data page read across all cards (no filter for now). This fixes:
1. Empty statements/transactions after commit
2. Wrong managed-account option in import card resolution

## Implementation Changes

1. **Remove legacy managed account row and clean old data**
- Add DB migration (next version) to:
  - Delete `manual-default-account` from `accounts`
  - Delete linked `transactions`, `statements`, `import_rows` for that account
  - Reset any `imports.resolved_account_id = manual-default-account` to unresolved/pending card resolution state
- Keep app-level root identity in `app_user` + `connections` only (no root row in `accounts`).

2. **Stop creating legacy default account at startup**
- Remove API startup call that ensures `manual-default-account`.
- Keep `connections.manual-connection` creation inside card/account creation path as needed.

3. **Card resolution policy (auto-first, manual fallback)**
- Use exact match key: `account_type + account_number_ending + customer_name`.
- On import processing:
  - If exact single match exists: resolve to that card (`auto_high_confidence_match`).
  - If no match and all 3 fields exist: auto-create card via `create_account_card`, resolve import, and continue.
  - If any of the 3 fields missing: keep `pending_card_resolution`, require user card input.
- Keep ambiguous multi-match as manual resolution (no auto-pick).

4. **Import UI behavior**
- Card-resolution existing-card dropdown must show **cards only** (no legacy managed account).
- For auto-created card, show non-blocking info message in import results:
  - “No matching card found; created new card and linked this import.”
- Manual resolution form remains for insufficient metadata cases.

5. **Global data read mode (for now)**
- Make statements and coverage APIs support global mode when `account_id` is absent:
  - `GET /api/v1/statements` returns all-card statements
  - `GET /api/v1/statements/coverage` returns all-card coverage
- UI Data page:
  - Remove dependency on selecting first account from `/api/v1/accounts`
  - Load statements/coverage in global mode
  - Continue statement drill-down by `statement_id`

## API / Interface Updates
- `GET /api/v1/statements`: `account_id` optional (global if missing).
- `GET /api/v1/statements/coverage`: `account_id` optional (global if missing).
- `GET /api/v1/imports/:id/card-resolution`: same shape; candidate semantics are cards-only.
- No change to commit endpoint shape; commit gating remains card-resolution aware.

## Test Plan
1. **Migration tests**
- Legacy `manual-default-account` and linked rows are removed/reset correctly.
- Fresh DB has no default account row created on startup.

2. **Card resolution tests**
- Exact match resolves automatically.
- No match + complete metadata auto-creates and resolves.
- Missing any of type/last4/name stays `pending_card_resolution`.
- Ambiguous matches remain manual.

3. **Data API tests**
- Statements endpoint returns rows globally with no `account_id`.
- Coverage endpoint returns global coverage with no `account_id`.
- Existing account-filtered behavior still works when `account_id` is provided.

4. **UI tests**
- Data view after commit shows imported statement(s)/transactions without account preselection.
- Card-resolution list excludes managed account.
- Auto-create banner appears when a new card is created automatically.

## Assumptions
- Clearing existing legacy managed-account data is acceptable.
- Card filtering UI is deferred; global Data view is temporary desired behavior.
- Card identity remains strict on `type + last4 + customer_name` for this phase.
