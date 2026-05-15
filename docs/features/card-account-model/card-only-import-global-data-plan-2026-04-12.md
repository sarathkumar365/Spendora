# Card-Only Import + Global Data (Two-Pass Implementation)

## Summary
Implement in 2 passes:
1. Pass 1 (stability-first): migration + backend/API behavior + core UI wiring + tests.
2. Pass 2 (cleanup): import-page UX cleanup and regression hardening.

Locked defaults:
- Migration policy: hard-delete legacy `manual-default-account` data.
- Scope: full import-page cleanup now (after core behavior is stable).

## Key Changes

1. **Database + storage**
- Add migration `0009` to:
  - delete `manual-default-account` from `accounts`
  - delete linked `transactions`, `statements`, `import_rows`
  - reset `imports.resolved_account_id = manual-default-account` to unresolved (`resolved_account_id = NULL`, pending card resolution)
- Remove startup dependency on `ensure_default_manual_account`.
- Keep `manual-connection` creation only in card-account creation paths.

2. **Import resolution pipeline**
- Enforce card-only match key: `account_type + account_number_ending + customer_name`.
- On import processing:
  - single exact match -> auto-resolve (`auto_high_confidence_match`)
  - no match + complete metadata -> auto-create via `create_account_card`, then resolve
  - missing required metadata -> `pending_card_resolution`
  - ambiguous multi-match -> manual resolution
- Ensure `/api/v1/imports/:id/card-resolution` returns card candidates only.

3. **Statements/Coverage API global mode**
- Make `account_id` optional for:
  - `GET /api/v1/statements`
  - `GET /api/v1/statements/coverage`
- If `account_id` is absent, return aggregated all-card data.
- Keep account-filtered behavior unchanged when `account_id` is present.

4. **Desktop UI**
- Remove first-account bootstrap dependency on `/api/v1/accounts` for Data view.
- Load statements/coverage in global mode.
- Keep statement drill-down by `statement_id`.
- Card-resolution panel:
  - existing-card selector shows cards only
  - non-blocking info message when auto-created card is used
- Full cleanup:
  - remove duplicate/legacy account creation and resolution blocks
  - simplify import action flow and status messaging
  - preserve review/commit functional behavior

## Pass Breakdown

1. **Pass 1**
- Migration, storage, API contract updates, minimal UI wiring, and backend/UI tests green.

2. **Pass 2**
- Full import-page cleanup/refactor, UX message cleanup, focused regression suite, final end-to-end smoke flow:
  import -> review -> card resolution -> commit -> data visibility.

## Test Plan
1. Migration/storage tests:
- legacy row and linked records removed/reset as intended
- fresh startup does not create `manual-default-account`

2. Import resolution tests:
- exact-match auto-resolve
- complete metadata + no match -> auto-create + resolve
- missing metadata -> `pending_card_resolution`
- ambiguous matches -> manual required

3. API tests:
- statements works with and without `account_id`
- coverage works with and without `account_id`
- existing account-filtered behavior unchanged

4. UI tests:
- Data page shows committed statements/transactions without account preselection
- card-resolution candidates exclude legacy managed account
- auto-create info banner appears when applicable
- cleanup flow remains commit-safe end-to-end

## Assumptions
- Hard-delete of legacy managed-account data is acceptable.
- Global Data mode is temporary and intentionally unfiltered for now.
- Card identity remains strict on `type + last4 + customer_name` for this phase.
