# Expense API v1 Contract Stubs

Base URL (local): `http://127.0.0.1:8081`

## Health
- `GET /health`
- `GET /api/v1/health`
- `GET /api/v1/diagnostics`

## Planned core endpoints
- `POST /api/v1/connections/plaid/link-token`
- `POST /api/v1/connections/plaid/exchange`
- `GET /api/v1/connections`
- `DELETE /api/v1/connections/:id`
- `GET /api/v1/accounts`
- `GET /api/v1/transactions`
- `PATCH /api/v1/transactions/:id/category`
- `POST /api/v1/imports`
- `GET /api/v1/imports/:id/status`
- `GET /api/v1/insights/monthly`
- `POST /api/v1/assistant/query`
