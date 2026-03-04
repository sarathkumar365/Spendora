# Desktop-First Personal Expense Tracker (Rust) - Decision-Complete Build Plan

## Summary
Build a native desktop expense tracker for macOS and Windows with:
1. Tauri desktop shell + React UI,
2. Rust-first backend/workers running locally,
3. SQLite storage (single-user),
4. Plaid-first Scotiabank ingestion with manual statement fallback (CSV/PDF),
5. Agent workflows for classification, reconciliation, and insights.

This plan is optimized for single-user reliability, low ops overhead, and future migration to cloud/multi-user.

## 1) Architecture Decisions (Locked)

1. App type
- Native desktop app (not browser-only), shipped as installers for Mac/Windows.

2. Runtime topology
- Tauri app process (UI container)
- Local API process (`expense-api`, Rust, localhost)
- Local worker process (`expense-worker`, Rust, localhost/internal)
- Shared SQLite DB file in OS app data path

3. Data and jobs
- SQLite for application state + ledger + job queue (`job_runs` table)
- No RabbitMQ/Redis/Postgres in v1

4. Ingestion strategy
- Primary: Plaid
- Fallback: CSV/PDF statement import
- Connector abstraction to add Flinks/direct APIs later

5. Agent strategy
- Deterministic rules first
- AI model assistance second (confidence-scored, explainable, overridable)

## 2) Repository and Workspace Layout

- `apps/expense-desktop-ui/` React + TS + Vite
- `apps/expense-desktop-tauri/` Tauri app shell + native config
- `services/expense-rs/` Cargo workspace
- `services/expense-rs/crates/core/` domain types + shared logic
- `services/expense-rs/crates/storage_sqlite/` DB layer + migrations
- `services/expense-rs/crates/connectors_plaid/` Plaid adapter
- `services/expense-rs/crates/connectors_manual/` CSV/PDF ingest
- `services/expense-rs/crates/api/` HTTP API (axum)
- `services/expense-rs/crates/worker/` job runner + workers
- `services/expense-rs/crates/agent/` classification/insight logic
- `docs/expense/` ADRs, contracts, runbooks, release checklist

## 3) Module Design (Product + Engineering)

### A) `core`
Responsibilities:
- Domain models (`Connection`, `Account`, `Transaction`, `Category`, `Rule`)
- Canonical enums/status types
- Error taxonomy
- Idempotency key and dedupe helpers

### B) `storage_sqlite`
Responsibilities:
- Migration scripts (forward-only)
- Repository interfaces and concrete SQLx implementations
- Transaction-safe writes for sync + imports
- Read models for fast UI queries

### C) `connectors_plaid`
Responsibilities:
- Link token generation
- Public token exchange and secure storage reference
- Accounts fetch and cursor-based transaction sync
- Webhook event normalization

### D) `connectors_manual`
Responsibilities:
- CSV parser pipeline
- PDF extraction pipeline
- Confidence scoring + parse diagnostics
- Normalization into canonical transaction shape

### E) `api`
Responsibilities:
- Auth/session (single-user local)
- Connection endpoints
- Accounts and transactions query endpoints
- Rules CRUD
- Import upload/session endpoints
- Insights endpoints
- Assistant query endpoints
- Health and diagnostics endpoints

### F) `worker`
Responsibilities:
- DB-backed scheduler loop
- Sync worker (incremental + idempotent)
- Import parse worker
- Categorization worker
- Reconciliation worker
- Insight refresh worker
- Retry/backoff/dead-letter semantics in table state

### G) `agent`
Responsibilities:
- Rule engine (deterministic)
- AI-assisted categorization fallback
- Explanation synthesis
- “Why this category?” and “What changed?” reasoning
- Assistant query planner (read-only default)

### H) `desktop shell (tauri)`
Responsibilities:
- Process lifecycle (start/stop local services)
- Secure storage integration
- Auto-update plumbing (optional in v1.1)
- Native notifications for sync/import issues

## 4) API and Interface Contracts

### API namespace
- `/api/v1/...` versioned from day one

### Key endpoints
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

### Internal connector trait
- `create_session`
- `exchange_credentials`
- `sync`
- `list_accounts`
- `disconnect`

### Output invariants
Every transaction response must include:
- source (`plaid`/`manual`)
- classification source (`manual`/`rule`/`agent`)
- confidence
- explanation string
- last_sync_at

## 5) Database Schema (SQLite v1)

Tables:
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

Critical constraints:
- Unique `(account_id, external_txn_id)` on `transactions`
- Unique `idempotency_key` on `job_runs`
- FK constraints on all child entities
- Raw data append-only policy

## 6) Job Model (No External Queue)

### `job_runs` fields
- `id`, `job_type`, `payload_json`, `status`, `attempts`, `next_run_at`, `idempotency_key`, `last_error`, `created_at`, `updated_at`

### Worker behavior
1. Poll `pending` jobs due by `next_run_at`
2. Acquire lease/lock atomically
3. Execute with tracing context
4. On success, mark `completed`
5. On failure, increment attempts, compute backoff, reschedule
6. Move terminal failures to `failed` and emit user-visible notification

## 7) AI Workflow Design in Rust

### Classification policy
1. Check manual override
2. Apply matching rules (priority order)
3. If unresolved, run AI categorization call
4. Store confidence + rationale
5. If confidence below threshold, set `needs_review`

### Assistant policy
- Read-only by default
- Must cite source transactions used in response
- No autonomous mutation without explicit user action endpoint
- Token/cost controls and rate limiting for model calls

## 8) PDF/Import Strategy (Risk-Managed)

1. CSV support first (highest reliability)
2. PDF text extraction second with robust fallback
3. OCR sidecar optional for scanned PDFs (feature flag)
4. Unknown format path:
- partial parse
- line-by-line review UI
- user confirmation before commit

## 9) Security and Privacy Baseline

1. Token secrets stored using OS keychain + encrypted references
2. SQLite file in app-data directory with restricted permissions
3. Redaction in logs for sensitive fields
4. Signed upload validation and MIME checks
5. Immutable audit trail for key actions
6. Local encrypted backup export/import option

## 10) Desktop UX and Interaction Model

Primary screens:
1. Onboarding (connect bank or import statement)
2. Transactions feed (search/filter/edit category)
3. Rules builder (preview impacted rows before save)
4. Review queue (low-confidence categorizations/import rows)
5. Insights dashboard (monthly, trends, anomalies)
6. Assistant panel (answers + evidence)

UX principles:
- Fast first paint (<1.5s on warm start)
- Explicit sync state and last updated time
- One-click correction flows for trust/recovery
- No hidden AI decisions without explanation

## 11) Implementation Plan (5 Steps)

### Step 1: Bootstrap Monorepo + Runtime Skeleton
- Create workspace structure (`apps/...`, `services/expense-rs/crates/...`, `docs/expense/...`).
- Stand up Tauri shell, React UI scaffold, Rust API, and Rust worker processes with local process management.
- Add shared config, structured logging/tracing, health endpoints, and SQLite migration runner.
- Define baseline versioned API contracts (`/api/v1`) with OpenAPI stubs.
- Exit criteria: app launches on macOS, API/worker boot reliably, DB initializes from migrations.

### Step 2: Core Ledger + Plaid Sync Path
Status: Done for current delivery scope. Live Plaid integration remains deferred; see `docs/expense/plaid-deferred-checklist.md`.

- Implement `core` domain models, error taxonomy, and idempotency/dedupe helpers.
- Implement `storage_sqlite` repositories and schema constraints (`connections`, `accounts`, `transactions`, `job_runs`, etc.).
- Implement `connectors_plaid` (link token, exchange, accounts list, cursor sync) and API endpoints.
- Build transactions feed UI (list, filters, sync state, last updated).
- Exit criteria: Plaid sandbox connects, sync works end-to-end, no duplicate transactions across repeated sync.

### Step 3: Manual Import Pipeline + Review UX
- Implement `connectors_manual` CSV parser first; PDF parser baseline second with diagnostics.
- Add import session flow (`POST /api/v1/imports`, status tracking, staging tables).
- Build review queue UI for low-confidence rows and partial parse handling.
- Merge approved import rows into canonical `transactions` with provenance metadata.
- Exit criteria: CSV path is reliable end-to-end; PDF baseline works with explicit review fallback.

### Step 4: Rules, Agent, Reconciliation, and Insights
- Build rule engine with priority ordering, preview impact, CRUD UI, and deterministic precedence.
- Add agent fallback for unresolved classifications with confidence/rationale and `needs_review` flow.
- Implement reconciliation heuristics (duplicates, refunds, transfers) and monthly/anomaly insight jobs.
- Build assistant endpoint/UI as read-only by default with evidence citations.
- Exit criteria: manual overrides always win, explanations are visible, and insights refresh from real ledger data.

### Step 5: Hardening, Test, Package, and Release
- Complete worker reliability: leasing, retries, exponential backoff, dead-letter states, and user notifications.
- Add security/privacy baseline: keychain secret handling, redacted logs, immutable audit events, backup export/import.
- Execute test matrix (unit, integration, E2E desktop, restart/crash recovery).
- Package macOS and Windows installers; validate install/upgrade/uninstall paths.
- Exit criteria: release candidate passes acceptance targets (no duplicate transactions, manual overrides respected, Plaid and import flows usable end-to-end, p95 transaction query <300ms on 50k rows).

## 12) Test Strategy and Acceptance Criteria

### Unit tests
- Rule matcher and precedence
- Idempotency and dedupe
- Parsing normalization and confidence thresholds

### Integration tests
- Plaid sandbox sync lifecycle
- Import pipelines with fixture datasets
- Worker retry/backoff semantics

### E2E tests
- Connect -> sync -> categorize -> insights
- Import -> review -> merge -> insights
- Restart app -> data/session integrity

### Desktop release validation
- Clean install on macOS and Windows
- Upgrade path preserves DB and config
- App recovers from API/worker crash on restart

Acceptance:
- No duplicate txns across repeated sync
- Manual override always respected
- Import and Plaid paths both usable end-to-end
- p95 transaction query <300ms on 50k rows local dataset

## 13) Risks and Mitigations

1. Scotiabank/Plaid availability variance  
Mitigation: manual import first-class, connection health checks, reconnect UX.

2. PDF format inconsistency  
Mitigation: CSV-first, parser confidence + review queue, optional OCR path.

3. AI misclassification trust gap  
Mitigation: deterministic rules precedence, confidence thresholds, explicit explanations.

4. Desktop packaging complexity  
Mitigation: early CI packaging smoke builds from week 2 onward.

## 14) Migration Path (Post-v1)

1. Swap SQLite -> Postgres with repository abstraction unchanged
2. Replace DB job runner with Redis/Rabbit when needed
3. Add optional cloud sync account and encrypted remote backup
4. Multi-user tenancy behind same `/api/v1` contracts

## Assumptions and Defaults

1. Single user only in v1
2. CAD default currency
3. Local-first, no mandatory cloud account
4. Plaid primary connector; manual import always available
5. Tauri selected over Electron for footprint/perf
