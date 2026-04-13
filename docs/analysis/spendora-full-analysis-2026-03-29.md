# Spendora Full Repository Analysis Report

Date: 2026-03-29
Repository: `/Users/sarathkumar/Projects/spendora`
Analyst: Codex

## 1) Scope and Method

This document captures a full technical analysis of the current Spendora report/codebase state by reviewing:

1. All files under `docs/`
2. All currently implemented API and worker endpoints
3. All DB tables/migrations/indexes/constraints and cross-table connections
4. Runtime/process behavior and orchestration
5. External provider integration surfaces (Llama/OpenRouter)
6. Gaps and mismatches between plans/docs and implementation

This analysis was done by reading source files directly and cross-checking contracts against implementation.

---

## 2) Full `docs/` Inventory Reviewed

Total doc lines reviewed: 2,103

- `docs/reference/DEVELOPER_NOTES.md` (419)
- `docs/plans/archive/desktop-rust-plan.md` (329)
- `docs/issues/known-issues.md` (78)
- `docs/plans/archive/lama-extraction-0.2.md` (285)
- `docs/plans/archive/runtime-path-enforcement-plan.md` (73)
- `docs/plans/archive/version-0.2-plan.md` (276)
- `docs/expense/README.md` (9)
- `docs/expense/ai-pdf-extraction-research.md` (301)
- `docs/expense/api-contract.md` (63)
- `docs/expense/openapi-v1.stub.yaml` (25)
- `docs/expense/plaid-deferred-checklist.md` (27)
- `docs/expense/runbook-local.md` (116)
- `docs/expense/testing-step1.md` (102)

---

## 3) Architecture and Runtime Model (As Implemented)

### 3.1 Components

Current runtime is desktop-first and local-first with these active pieces:

1. Tauri desktop shell process
2. React desktop UI in webview
3. Local Rust API process (Axum)
4. Local Rust worker process (loop + health HTTP)
5. Local SQLite DB

### 3.2 Ownership split

- Tauri: process lifecycle and startup orchestration
- API: HTTP interface, contracts, reads/writes to storage layer
- Worker: async import parsing/extraction execution and job lifecycle
- Storage crate: migrations and DB repository logic
- Connectors crates: parsing/extraction provider integrations

### 3.3 Startup and service control

Implemented Tauri commands:

- `start_services`
- `stop_services`
- `service_status`
- `startup_status`

Key behavior:

- Auto startup on app setup thread
- Retry up to 3 attempts
- Timeout-based readiness checks
- Service stop on app exit
- Dev runtime path currently uses `.runtime` under services root

---

## 4) Implemented HTTP Endpoints (Local API/Worker)

Base API host in docs: `http://127.0.0.1:8081`
Worker health default: `127.0.0.1:8082`

### 4.1 API endpoints currently registered

From API router wiring:

1. `GET /health`
2. `GET /api/v1/health`
3. `GET /api/v1/diagnostics`
4. `POST /api/v1/imports`
5. `GET /api/v1/imports/:id/status`
6. `GET /api/v1/imports/:id/review`
7. `POST /api/v1/imports/:id/review`
8. `POST /api/v1/imports/:id/commit`
9. `GET /api/v1/transactions`
10. `GET /api/v1/accounts`
11. `GET /api/v1/statements`
12. `GET /api/v1/statements/coverage`
13. `GET /api/v1/statements/:statement_id/transactions`
14. `GET /api/v1/settings/extraction`
15. `PUT /api/v1/settings/extraction`
16. `POST /api/v1/connections/plaid/link-token` (deferred 501)
17. `POST /api/v1/connections/plaid/exchange` (deferred 501)

### 4.2 Worker health endpoints

1. `GET /health`
2. `GET /api/v1/health`

### 4.3 CORS policy implementation

- Explicit allowlist origin policy (no wildcard)
- Default origins in code:
  - `http://127.0.0.1:1420`
  - `http://localhost:1420`
- Methods default: `GET,POST,PUT,DELETE,OPTIONS`
- Headers default: `Content-Type,Authorization`
- No credentials enablement

### 4.4 Deferred/not implemented endpoints

- Plaid endpoints return HTTP 501 with message `Plaid deferred to future phase`
- Backlog in docs includes:
  - `GET /api/v1/connections`
  - `DELETE /api/v1/connections/:id`
  - `PATCH /api/v1/transactions/:id/category`
  - `GET /api/v1/insights/monthly`
  - `POST /api/v1/assistant/query`

---

## 5) Endpoint-Level Behavioral Analysis

### 5.1 Health and diagnostics

- `/health` and `/api/v1/health` return `HealthStatus` with `service/status/now_utc`
- `/api/v1/diagnostics` returns:
  - service
  - sqlite ping status (`SELECT 1`)
  - `llama_agent_readiness` from `app_settings` when present

### 5.2 Imports flow (`POST /api/v1/imports`)

Request fields accepted:

- `file_name` (required unless reuse path)
- `parser_type` (`pdf`/`csv`)
- `content_base64` (required unless reuse path)
- `extraction_mode` (`managed`/`local_ocr`)
- `account_id` (optional)
- `year` and `month` (optional, must be paired)

Behavior:

1. Validates parser/extraction mode and month range
2. If `account_id + year + month` supplied:
   - checks statement coverage
   - if statement exists: creates reused import immediately in committed state
   - skips queueing parse job
3. Otherwise:
   - inserts import row
   - enqueues `job_runs` `import_parse`
   - returns queued status

### 5.3 Import status/review/commit endpoints

- `GET /imports/:id/status`
  - returns status envelope with extraction diagnostics, attempts, summary, errors/warnings
- `GET /imports/:id/review`
  - lists review rows from `import_rows`
- `POST /imports/:id/review`
  - applies row approval/rejection decisions
  - recomputes `review_required_count` and updates import status to `review_required` or `ready_to_commit`
- `POST /imports/:id/commit`
  - commits approved parseable rows to `transactions` with dedupe

### 5.4 Transactions listing

`GET /api/v1/transactions` query params:

- `q`
- `account_id`
- `source`
- `date_from`
- `date_to`
- `limit` (default 100)
- `offset` (default 0)

SQL behavior:

- case-insensitive description filter
- source/account/date filters
- sorted by `booked_at DESC, created_at DESC`
- includes derived `import_id` via subquery against `import_rows.normalized_txn_hash`

### 5.5 Statements coverage/listing

`GET /api/v1/statements`

- requires `account_id`
- supports `year`, `month`, `date_from`, `date_to`
- returns statement metadata + linked transaction count

`GET /api/v1/statements/coverage`

- requires `account_id`
- optional `year+month` pair
- aggregates coverage month-wise
- selected month response includes reusability policy info

`GET /api/v1/statements/:statement_id/transactions`

- returns transactions directly linked by `statement_id`

### 5.6 Extraction settings endpoints

`GET /api/v1/settings/extraction`

- returns persisted settings or defaults

`PUT /api/v1/settings/extraction`

Validation:

- mode must be `managed` or `local_ocr`
- retries must be 1..3
- timeout must be 1000..180000 ms

---

## 6) Database Schema, Tables, Constraints, and Connections

This section reflects additive migrations `0001` to `0005`.

### 6.1 Core tables

1. `app_user`
2. `connections`
3. `accounts`
4. `transactions_raw`
5. `transactions`
6. `categories`
7. `classification_results`
8. `rules`
9. `imports`
10. `import_rows`
11. `job_runs`
12. `audit_events`
13. `app_settings` (added in `0003`)
14. `statements` (added in `0004`)
15. `schema_migrations` (created by migration runner)

### 6.2 Added columns over time

`imports` additions:

- `parser_type`
- `source_hash`
- `review_required_count`
- `committed_at`
- `summary_json`
- `errors_json`
- `warnings_json`
- `content_base64`
- `extraction_mode`
- `managed_provider_preference`
- `effective_provider`
- `provider_attempts_json`
- `extraction_diagnostics_json`
- `provider_attempt_count`

`import_rows` additions:

- `normalized_txn_hash`
- `approved`
- `rejection_reason`
- `account_id`
- `statement_id`

`transactions` additions:

- `statement_id`

### 6.3 Foreign keys and unique constraints

- `accounts.connection_id -> connections.id`
- `transactions.account_id -> accounts.id`
- `classification_results.transaction_id -> transactions.id`
- `classification_results.category_id -> categories.id`
- `rules.category_id -> categories.id`
- `import_rows.import_id -> imports.id`
- `transactions.statement_id -> statements.id`
- `import_rows.statement_id -> statements.id`
- `statements.account_id -> accounts.id`

Unique constraints/indexes:

- `transactions UNIQUE(account_id, external_txn_id)`
- `job_runs.idempotency_key UNIQUE`
- `categories.name UNIQUE`
- `statements unique index(account_id, period_start, period_end)`

### 6.4 Indexes

- `idx_import_rows_import_id`
- `idx_import_rows_hash`
- `idx_transactions_account_booked_at`
- `idx_transactions_source`
- `idx_transactions_statement_booked_at`
- `idx_import_rows_statement_id`

### 6.5 Effective relationship graph

1. `connections -> accounts -> transactions`
2. `imports -> import_rows`
3. `statements -> transactions`
4. `statements -> import_rows`
5. `categories -> classification_results` and `transactions -> classification_results`
6. `job_runs` as async task queue for worker
7. `app_settings` as key-value config/readiness store

---

## 7) Worker Pipeline and Job Model

### 7.1 Job processing

- Worker polls and claims pending `job_runs` of type `import_parse`
- Sets import status to `parsing`
- Loads import content/settings
- Chooses parser path by parser type and extraction mode

### 7.2 Parsing/extraction paths

1. `csv` -> local text parser (`connectors_manual::parse_csv`)
2. `pdf` + `local_ocr` -> explicit stub failure (`LOCAL_OCR_NOT_IMPLEMENTED`)
3. `pdf` + `managed` -> managed extraction flow:
   - readiness gate for Llama agent
   - flow mode `new|legacy` via `EXTRACTION_MANAGED_FLOW_MODE`

### 7.3 Managed flow (`new`)

Sequence:

1. Resolve agent readiness/cache
2. Upload file (with endpoint compatibility fallbacks)
3. Create extraction job
4. Poll job status until terminal or timeout
5. Fetch result payload
6. Validate `transactions[]` contract
7. Resolve/derive statement period
8. Upsert `statements`
9. Insert `import_rows` with optional `statement_id`
10. Update import extraction diagnostics and status

### 7.4 Status transitions

Import status progression:

- `queued` -> `parsing` -> (`review_required` or `ready_to_commit`) -> `committed`
- failures -> `failed`

### 7.5 Retry and failure policy

- Job failure retry logic in `job_runs`:
  - if attempts < 3: requeue after +30s
  - else terminal `failed`

### 7.6 Observability and logs

Log files used/documented:

- `extraction-provider.log`
- `external-api-raw.log`
- `extraction-bootstrap.log`
- `startup-metrics.log` (desktop startup)

Worker writes bootstrap/decision/status events; provider attempts include metadata and retry decisions.

---

## 8) Connectors and External Endpoint Surface

### 8.1 LlamaExtract Jobs integration (new managed path)

Used endpoints:

- File upload candidates:
  - `/api/v1/beta/files`
  - `/api/v1/files` (`upload_file` and `file` multipart key compatibility)
- Jobs:
  - `/api/v1/extraction/jobs` (create)
  - `/api/v1/extraction/jobs/{job_id}` (poll)
  - `/api/v1/extraction/jobs/{job_id}/result` (result)

### 8.2 Agent bootstrap endpoints

- `/api/v1/extraction/extraction-agents/schema/validation`
- `/api/v1/extraction/extraction-agents` (list/create)

### 8.3 Legacy managed fallback path

- LlamaParse upload/poll endpoints
- OpenRouter chat completions (`/api/v1/chat/completions`) using `file-parser` plugin (`pdf-text`)

### 8.4 Compatibility handling implemented

- field drift handling for jobs create (`extraction_agent_id` vs `agent_id`)
- upload endpoint/path + multipart key fallbacks
- status classification handles multiple in-progress tokens and unknown-state logic

### 8.5 Hard caps and timeouts

- provider timeout clamped to `1000..180000 ms`
- jobs poll attempt clamp and hard cap of 3 minutes

---

## 9) Schema Contract for Statement Extraction

Current blueprint schema: `statement_v1`

Required top-level fields:

- `period_start` (date)
- `period_end` (date)
- `transactions` (array)

Each transaction required fields:

- `booked_at` (date)
- `description` (string)
- `amount_cents` (integer)

Optional transaction fields:

- `confidence`
- `meta`

Strictness:

- `additionalProperties: false` at root and item levels

---

## 10) Desktop Runtime/Path Behavior

### 10.1 Current implementation

- Tauri spawns API/worker via `cargo run -p <package>`
- DB/log path currently rooted at `services/expense-rs/.runtime`
- sets `EXPENSE_APP_DATA_DIR` to runtime dir for children

### 10.2 Planned but not fully applied

`docs/plans/archive/runtime-path-enforcement-plan.md` defines prod-vs-dev path enforcement with production app-data path correction. Current Tauri spawn path code is still `.runtime`-oriented.

---

## 11) Testing and Validation Surface

### 11.1 Documented testing

- Rust tests (`core`, `storage`, API/worker modules)
- smoke tests (`tests/step1/smoke.sh`)
- stress-lite tests (`tests/step1/stress-lite.sh`)
- manual Tauri checks (`tests/step1/tauri-manual-checklist.md`)

### 11.2 Coverage themes

- migration idempotency
- health/diagnostics readiness
- import review/commit behavior
- settings validation bounds
- statement coverage/transaction listing
- worker managed gate behavior

---

## 12) Document-vs-Implementation Alignment

### 12.1 Aligned

1. Endpoint set in expense API contract largely matches API router.
2. Step status in `version-0.2-plan.md` aligns with present code state:
   - step 1/2/3 complete
   - step 4 partial
3. Statement reuse and statement linkage are implemented.
4. Llama agent readiness is persisted and surfaced in diagnostics.

### 12.2 Partial or mismatch

1. OpenAPI stub is minimal and does not reflect implemented route surface.
2. Runtime path enforcement plan indicates mode-aware prod path correction, but spawn code remains `.runtime` based.
3. Some docs discuss future cloud/plaid/assistant/insights that remain unimplemented or deferred.

---

## 13) Known Issues Confirmed by Code

### 13.1 Production CORS origin issue

- Docs report `tauri://localhost` not allowed.
- Code defaults allow only local dev HTTP origins unless overridden by env.
- Impact: production webview requests can fail despite API being healthy.

### 13.2 Inflow/outflow direction ambiguity

- Extraction schema has `amount_cents` but no explicit `direction` field.
- Commit path stores `amount_cents` as extracted.
- API responses don’t expose direction metadata.
- Downstream reporting relying on sign can be incorrect.

---

## 14) Data-Flow End-to-End Connections

### 14.1 Import parse lifecycle

1. API creates `imports` record and optionally `job_runs` entry.
2. Worker claims pending `import_parse` job.
3. Worker writes extraction diagnostics to `imports`.
4. Worker inserts `import_rows`.
5. API review endpoint reads/modifies `import_rows.approved/rejection_reason`.
6. API commit endpoint inserts into `transactions` with dedupe.
7. Statement-linked rows propagate `statement_id` into transactions.

### 14.2 Statement reuse lifecycle

1. User provides account+year+month on import request.
2. API checks coverage map.
3. If statement exists, API creates committed reused import with diagnostics:
   - `reuse_mode: statement_db`
4. No extraction/provider call occurs for that request.

### 14.3 Diagnostics lineage

Import status envelope carries:

- extraction mode
- effective provider
- provider attempts
- diagnostics
- summary/errors/warnings
- review required count

Managed `new` flow diagnostics include:

- managed flow mode
- provider lineage (`file_id`, `job_id`, `run_id`, `agent_id`)
- poll status trail
- statement context (period fields, derivation marker)
- agent readiness snapshot

---

## 15) Security/Privacy/Operational Notes

1. Full external API raw responses can be logged; this can include sensitive financial data.
2. CORS is explicit allowlist; safe by default for dev, but requires prod origin configuration.
3. Secrets are env-driven; extraction startup contract fails when required env values missing.
4. FK enforcement enabled with `PRAGMA foreign_keys = ON` at connection setup.

---

## 16) Current Backlog and Deferred Areas

1. Plaid real integration still deferred.
2. Insights/assistant/category patch/connections list-delete endpoints remain backlog.
3. Local OCR mode remains explicit stub.
4. Runtime path mode enforcement work remains to be completed in Tauri path resolver.

---

## 17) High-Value Risks and Gaps

1. Production startup/user bootstrap risk from CORS origin mismatch.
2. Financial reporting correctness risk from missing explicit inflow/outflow direction semantics.
3. Contract drift risk due to sparse OpenAPI spec vs actual implementation.
4. Potential linkage ambiguity from hash-derived transaction -> import mapping (not FK).
5. Coverage month bucketing currently uses `created_at` for manual unlinked rows (not booked date), explicitly marked TODO.

---

## 18) Source Files Cross-Referenced During Analysis

### Docs

- `/Users/sarathkumar/Projects/spendora/docs/reference/DEVELOPER_NOTES.md`
- `/Users/sarathkumar/Projects/spendora/docs/plans/archive/desktop-rust-plan.md`
- `/Users/sarathkumar/Projects/spendora/docs/issues/known-issues.md`
- `/Users/sarathkumar/Projects/spendora/docs/plans/archive/lama-extraction-0.2.md`
- `/Users/sarathkumar/Projects/spendora/docs/plans/archive/runtime-path-enforcement-plan.md`
- `/Users/sarathkumar/Projects/spendora/docs/plans/archive/version-0.2-plan.md`
- `/Users/sarathkumar/Projects/spendora/docs/expense/README.md`
- `/Users/sarathkumar/Projects/spendora/docs/expense/ai-pdf-extraction-research.md`
- `/Users/sarathkumar/Projects/spendora/docs/expense/api-contract.md`
- `/Users/sarathkumar/Projects/spendora/docs/expense/openapi-v1.stub.yaml`
- `/Users/sarathkumar/Projects/spendora/docs/expense/plaid-deferred-checklist.md`
- `/Users/sarathkumar/Projects/spendora/docs/expense/runbook-local.md`
- `/Users/sarathkumar/Projects/spendora/docs/expense/testing-step1.md`

### Backend/API/Worker/Storage/Connectors/Schema

- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/api/src/main.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/api/src/imports.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/api/src/transactions.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/api/src/accounts.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/api/src/statements.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/api/src/settings.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/api/src/plaid.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/worker/src/main.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/storage_sqlite/src/lib.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/connectors_ai/src/lib.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/connectors_manual/src/lib.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/crates/core/src/lib.rs`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/schemas/statement_v1.json`

### Migrations

- `/Users/sarathkumar/Projects/spendora/services/expense-rs/migrations/0001_init.sql`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/migrations/0002_import_pipeline.sql`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/migrations/0003_extraction_settings.sql`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/migrations/0004_statement_foundation.sql`
- `/Users/sarathkumar/Projects/spendora/services/expense-rs/migrations/0005_import_rows_statement_link.sql`

### Desktop shell

- `/Users/sarathkumar/Projects/spendora/apps/expense-desktop-tauri/src-tauri/src/main.rs`

---

## 19) Final Summary

The current report/codebase is a functional desktop-local financial import pipeline with:

1. Working API + worker + storage foundation
2. Statement-based managed extraction flow and DB reuse path
3. Full import/review/commit lifecycle
4. Deferred Plaid and insight/assistant surfaces
5. Clear next hardening needs around production CORS, direction semantics, runtime-path mode enforcement, and OpenAPI contract completeness
