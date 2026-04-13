# Known Issues

## Production app startup shows "No account available" with `TypeError: Load failed`

### Date observed
- March 29, 2026

### Symptoms
- In production app run (`Spendora Desktop.app`), startup/loading completes but UI shows:
  - `No account available`
  - `TypeError: Load failed`

### What was verified
- Desktop shell process is running.
- API and worker processes are running and listening on expected ports:
  - API: `127.0.0.1:8081`
  - Worker: `127.0.0.1:8082`
- API endpoint responds successfully:
  - `GET /api/v1/accounts` returns `200` and account payload.

### Root cause
- CORS origin mismatch in production mode.
- API CORS allows dev origins (for example `http://127.0.0.1:1420`) but does not allow production app origin (`tauri://localhost`).
- Evidence:
  - Request with `Origin: tauri://localhost` returns no `access-control-allow-origin` header.
  - Request with `Origin: http://127.0.0.1:1420` returns `access-control-allow-origin` header.

### Impact
- Frontend account bootstrap request is blocked by WebView CORS policy in production.
- Import/Data flows are effectively blocked at startup gate.

### Notes for future fix
- Update API CORS configuration to explicitly support production Tauri origin(s) in addition to dev origins.
- Keep dev and prod origin handling explicit and environment-aware.

## Inflow/outflow direction is unreliable in extracted transactions

### Date observed
- March 29, 2026

### Symptoms
- Import/review and saved transactions expose `amount_cents` but no explicit `direction` field.
- Many rows that look like outflow by description (for example `withdrawal`, `point of sale purchase`, `mb-transfer to`) are stored with positive amounts.
- This makes inflow/outflow totals inaccurate if computed from sign only.

### What was verified
- Current extraction schema (`statement_v1`) requires `booked_at`, `description`, `amount_cents` only; no direction enum is present.
- Worker pipeline stores provider `amount_cents` as-is into `import_rows`, then into `transactions` on commit.
- Current dataset snapshot (repo runtime DB):
  - `transactions`: 115 rows
  - positive: 104, negative: 11
  - `withdrawal` rows observed with positive values
  - `error correction` appears as both positive and negative
- Endpoint responses used by UI (`/api/v1/imports/:id/review`, `/api/v1/statements/:id/transactions`, `/api/v1/transactions`) do not provide direction metadata.

### Root cause
- Direction semantics are not part of the extraction contract.
- Managed extraction output does not consistently enforce signed `amount_cents` for inflow/outflow.
- Downstream API/UI currently depend on raw amount sign without a fallback direction signal.

### Impact
- Inflow/outflow classification in UI and analytics can be wrong.
- Users cannot reliably infer money-in vs money-out from current output in all cases.

### Resolutions researched
- **Short-term (safe, no schema break):**
  - Add deterministic description-based direction rules (`deposit/payroll/transfer from` => inflow, `withdrawal/purchase/transfer to/fees` => outflow, `error correction/credit memo` => reversal).
  - Only auto-assign when rule confidence is high; label remaining rows as `unknown` for review.
- **Medium-term (recommended contract fix):**
  - Update extraction blueprint/schema and prompts to require explicit direction semantics:
    - either signed `amount_cents` rule (`outflow < 0`, `inflow > 0`) with validation
    - or explicit `direction` field (`inflow|outflow|reversal|transfer`) plus signed consistency checks.
- **Hybrid ML path:**
  - Use local NLP/ML (spaCy + matcher rules, optional local LLM fallback) for unresolved rows.
  - Persist user corrections and retrain/tune for higher precision on recurring merchant patterns.
- **Quality controls:**
  - Add confidence gating and manual review for low-confidence labels.
  - Maintain a small gold dataset for regression precision/recall checks.
