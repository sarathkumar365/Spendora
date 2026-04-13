# Import Issues Handoff (2026-04-12)

## Context
- User observed these issues during statement import + commit flow.
- Work paused for the day; continue tomorrow from this list.

## Issue 1: Duplicate account creation path in import flow
- On import page, user sees two account creation paths:
  - existing "managed account" (app-level account)
  - actual credit-card account creation
- Expected:
  - credit card imports should map to card/account entities only.
  - do **not** use/manage statement card data through the legacy managed app-level account path.
- Impact:
  - confusing UX and likely wrong account linkage.

## Issue 2: Commit success but no statements/transactions shown
- After save/create + commit import, app navigates to transactions page.
- User sees no transactions/statements.
- Expected:
  - committed import should be queryable immediately in statements/transactions views.
- Suspected area:
  - commit-to-storage linkage and/or query filters for new schema-first fields.

## Issue 3: Import page is cluttered
- Import page currently feels overloaded and hard to use.
- Expected:
  - cleanup/rework to simplify actions and remove redundant blocks (especially account selection/creation duplication).

## Next-session starting checklist
1. Reproduce all three issues with one fresh import.
2. Trace account resolution + commit pipeline (import -> resolved account/card -> statement -> transactions).
3. Verify API responses used by transactions/statements page after commit.
4. Draft targeted UI cleanup list for import page.
