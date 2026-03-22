# Local Bootstrap Runbook

## Prerequisites
- Node.js 20+
- Rust stable toolchain
- Tauri desktop prerequisites installed

## Install
- `npm install`

## Start API and worker manually
- `npm run rs:api`
- `npm run rs:worker`

## Start desktop shell
- `npm run tauri:dev`

## Health checks
- API: `http://127.0.0.1:8081/health`
- API diagnostics: `http://127.0.0.1:8081/api/v1/diagnostics`
- Worker: `http://127.0.0.1:8082/health`

## CORS (strict allowlist)
- API CORS is allowlist-based (no wildcard `*` and no credentialed CORS).
- Default local allowed origins:
  - `http://127.0.0.1:1420`
  - `http://localhost:1420`
- Configure additional known web domains via:
  - `CORS_ALLOWED_ORIGINS` (comma-separated)
- Optional overrides:
  - `CORS_ALLOWED_METHODS` (default: `GET,POST,PUT,DELETE,OPTIONS`)
  - `CORS_ALLOWED_HEADERS` (default: `Content-Type,Authorization`)

Troubleshooting preflight:
- If browser requests fail with CORS, check the API process env and ensure origin is listed.
- Quick preflight check:
  - `curl -i -X OPTIONS http://127.0.0.1:8081/api/v1/transactions -H 'Origin: http://localhost:1420' -H 'Access-Control-Request-Method: GET' -H 'Access-Control-Request-Headers: content-type'`

## Database
- Default DB file path is OS app-data directory + `expense.db`.
- On first boot, migrations from `services/expense-rs/migrations` are applied.

## Extraction Settings and Provider Config

### API settings endpoints
- `GET /api/v1/settings/extraction`
- `PUT /api/v1/settings/extraction`

Settings payload:
- `default_extraction_mode`: `managed` or `local_ocr`
- `managed_fallback_enabled`: `true` or `false`
- `max_provider_retries`: `1..3` (hard cap at 3)
- `provider_timeout_ms`: request timeout in milliseconds

### Import override fields
`POST /api/v1/imports` accepts optional:
- `extraction_mode`: `managed` or `local_ocr`

Mode resolution order:
1. per-import override
2. global extraction settings
3. default `managed`

### Managed providers
- Primary: LlamaParse
- Fallback: OpenRouter `pdf-text`
- Retryable failures: timeout/network, HTTP 429, HTTP 5xx
- Non-retryable: schema/validation and permanent 4xx failures

### Required environment variables
- `LLAMAPARSE_API_KEY` for Llama primary path
- `OPENROUTER_API_KEY` for OpenRouter fallback path
- Step 1 startup contract (API + worker fail fast when missing):
  - `LLAMA_CLOUD_API_KEY`
  - `LLAMA_AGENT_NAME`
  - `LLAMA_SCHEMA_VERSION` (use `statement_v1` for this phase)
- Optional:
  - `OPENROUTER_MODEL` (default in code if not set)
  - `LLAMAPARSE_ENDPOINT` (override endpoint)
  - `OPENROUTER_ENDPOINT` (override endpoint)
  - `LLAMA_CLOUD_ORGANIZATION_ID`
  - `LLAMA_CLOUD_PROJECT_ID`

Optional scope-ID behavior:
- `LLAMA_CLOUD_ORGANIZATION_ID` and `LLAMA_CLOUD_PROJECT_ID` are optional.
- If present but invalid UUIDs, they are ignored (non-blocking) and a warning event is written to bootstrap logs.

### Response logging (full raw provider responses)
By default, full provider responses are written for each attempt.

Controls:
- `EXTRACTION_LOG_FULL_RESPONSE=true` (default behavior)
- `EXTRACTION_LOG_MAX_BYTES=262144` (default max response bytes per entry)
- `EXPENSE_EXTRACTION_LOG_PATH` (optional explicit path)
- `EXPENSE_BOOTSTRAP_LOG_PATH` (optional explicit path for bootstrap/gate events)

Default log paths:
- provider attempts/http: `~/Library/Application Support/SpendoraDesktop/logs/extraction-provider.log`
- bootstrap/readiness/gate: `~/Library/Application Support/SpendoraDesktop/logs/extraction-bootstrap.log`

Security warning:
- Logs can include sensitive financial statement data when full-response logging is enabled.
