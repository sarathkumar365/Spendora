# Expense API v1 Contract Stubs

Base URL (local): `http://127.0.0.1:8081`

## Health
- `GET /health`
- `GET /api/v1/health`
- `GET /api/v1/diagnostics`

## CORS Policy
- CORS uses an explicit origin allowlist configured via `CORS_ALLOWED_ORIGINS`.
- No wildcard origins are used.
- No credentialed CORS (`Access-Control-Allow-Credentials` is not enabled).
- Optional env overrides:
  - `CORS_ALLOWED_METHODS` (default: `GET,POST,PUT,DELETE,OPTIONS`)
  - `CORS_ALLOWED_HEADERS` (default: `Content-Type,Authorization`)

## Planned core endpoints
### Implemented in Step 2
- `POST /api/v1/imports`
- `GET /api/v1/imports/:id/status`
- `GET /api/v1/imports/:id/review`
- `POST /api/v1/imports/:id/review`
- `POST /api/v1/imports/:id/commit`
- `GET /api/v1/accounts`
- `GET /api/v1/transactions`

### Implemented in Step 2.1
- `GET /api/v1/settings/extraction`
- `PUT /api/v1/settings/extraction`

Step 2.1 import contract additions:
- `POST /api/v1/imports` optional request fields:
  - `extraction_mode`: `managed` or `local_ocr`
- `GET /api/v1/imports/:id/status` additional fields:
  - `extraction_mode`
  - `effective_provider`
  - `provider_attempts[]`
  - `diagnostics`

### Implemented in Step 4
- `GET /api/v1/statements/coverage`
- `GET /api/v1/statements`
- `GET /api/v1/statements/:statement_id/transactions`

Step 4 import contract additions:
- `POST /api/v1/imports` optional request fields:
  - `account_id`
  - `year`
  - `month`
- `POST /api/v1/imports` response additions:
  - `reused` (boolean)

### Deferred (returns 501)
- `POST /api/v1/connections/plaid/link-token`
- `POST /api/v1/connections/plaid/exchange`

### Backlog
- `GET /api/v1/connections`
- `DELETE /api/v1/connections/:id`
- `PATCH /api/v1/transactions/:id/category`
- `GET /api/v1/insights/monthly`
- `POST /api/v1/assistant/query`
