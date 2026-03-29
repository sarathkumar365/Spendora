# Version 0.2 — 5-Step Technical Implementation Plan

## Implementation Status (Updated March 29, 2026)

- `Step 1` is complete and validated in the current codebase.
- `Step 2` is complete and validated in the current codebase.
- `Step 3` is complete and validated in the current codebase.
- `Step 4` is partially complete (startup + UI shell foundation implemented; insights layer still pending).

### Session Update (March 29, 2026) — Startup Revision + UI Foundation

Implemented:
- Deterministic desktop startup orchestration in Tauri:
  - auto-start API + worker from Tauri `.setup(...)`,
  - bounded retry flow (3 attempts) with startup state tracking,
  - readiness now requires both TCP-open and HTTP health (`/health` or `/api/v1/health`),
  - startup failure remains hard-blocking (no degraded shell entry).
- Tauri command surface preserved:
  - `start_services`, `stop_services`, `service_status` unchanged,
  - added `startup_status` command for UI loading gate polling.
- Process lifecycle hardening on app exit:
  - child processes are terminated on Tauri exit,
  - graceful TERM window with forced kill fallback.
- Startup observability additions:
  - new runtime metrics log:
    `services/expense-rs/.runtime/logs/startup-metrics.log`,
  - per startup attempt logs include:
    `total startup time took = <secs>s (<ms>ms) | mode=<auto|manual> | result=<healthy|failed> | attempts=<n>`.
- UI shell rebuilt from blank baseline:
  - minimal startup loading/error gate wired to Tauri startup state,
  - 3-page navigation scaffold added:
    `AI Interaction` (default stub), `Import` (stub), `View Your Data` (statements + transactions wired to existing APIs),
  - pastel modern-minimal styling baseline added.
- UI stack foundation aligned to shadcn/tailwind direction:
  - Tailwind + PostCSS config added,
  - `components.json` and path alias scaffolding added,
  - utility helpers added for future shadcn component growth.
- Warning cleanup completed:
  - removed Rust compile warnings in API/worker startup paths,
  - optional invalid scope IDs removed from local `.env` to avoid repeated bootstrap warning events.

Validated:
- Tauri startup test suite expanded and passing:
  - retry/backoff logic,
  - startup status state updates,
  - HTTP health probe behavior,
  - readiness requiring HTTP health (not just open TCP),
  - process termination behavior on shutdown.
- Test run result:
  - `cargo test --manifest-path apps/expense-desktop-tauri/src-tauri/Cargo.toml` -> pass.
- Build/compile checks:
  - `npm run test:ui-build` -> pass,
  - `cargo check --manifest-path apps/expense-desktop-tauri/src-tauri/Cargo.toml` -> pass,
  - `cargo check --manifest-path services/expense-rs/Cargo.toml -p api -p worker` -> pass.

Important current behavior:
- In dev mode, cold startup can still include Rust compile time due to `cargo run` process launches.
- Startup-time measurement now captures full standup duration every run in `startup-metrics.log`.
- AI page is intentionally a stub; insight-generation/product intelligence views remain future scope.

### Step 1 Completion Record (for Step 2 handoff)

Implemented:
- Additive migration `0004_statement_foundation`:
  - created `statements` table,
  - added nullable `transactions.statement_id`,
  - added statement/account-period indexes and unique period constraint.
- Startup runtime contract (API + worker fail-fast):
  - required envs: `LLAMA_CLOUD_API_KEY`, `LLAMA_AGENT_NAME`, `LLAMA_SCHEMA_VERSION`,
  - optional envs: `LLAMA_CLOUD_ORGANIZATION_ID`, `LLAMA_CLOUD_PROJECT_ID`.
- Local versioned blueprint schema added and validated at startup:
  - `services/expense-rs/schemas/statement_v1.json`.
- Agent metadata cache contract added in existing `app_settings`:
  - key `llama_agent_cache` with `{ agent_id, schema_version, updated_at }`,
  - storage helpers added for get/upsert.

Validated:
- Rust workspace tests pass (`npm run test:rs`).
- Runtime DB now includes:
  - migration version `0004_statement_foundation`,
  - `statements` table,
  - `transactions.statement_id` column.

Important Step 1 snapshot (pre-Step 2 implementation):
- `load_statement_blueprint_schema` is currently a startup guardrail only.
- Provider extraction responses are not yet validated against full `statement_v1` at ingest time.
- No agent auto-ensure logic exists yet (this is Step 2 scope).

### Step 2 Completion Record (for Step 3 handoff)

Implemented:
- Worker-owned Llama agent bootstrap and readiness persistence:
  - startup `ensure_llama_agent_ready` added after DB connect/migrations,
  - readiness persisted in `app_settings` key `llama_agent_readiness`,
  - states implemented: `configured | missing | schema_invalid | api_unreachable`.
- Llama agent bootstrap client for Step 2:
  - schema validation call,
  - list-by-name resolution using versioned name `${LLAMA_AGENT_NAME}--${LLAMA_SCHEMA_VERSION}`,
  - create-on-miss path.
- Managed extraction readiness gate in worker:
  - managed PDF path now blocks when agent readiness is not configured,
  - deterministic failure code: `EXTRACTION_AGENT_NOT_READY:<state>`,
  - import status diagnostics include `agent_readiness` snapshot.
- API diagnostics extended:
  - `GET /api/v1/diagnostics` now includes `llama_agent_readiness`.
- Observability/logging additions:
  - bootstrap + gate events written to `extraction-bootstrap.log`,
  - provider-level attempt/http logs continue in `extraction-provider.log`.
- Optional scope ID hardening:
  - invalid `LLAMA_CLOUD_ORGANIZATION_ID` / `LLAMA_CLOUD_PROJECT_ID` values are ignored (non-blocking),
  - warning event logged (`bootstrap_scope_id_ignored`).

Validated:
- Rust workspace tests pass (`cargo test --workspace`).
- Runtime diagnostics confirms readiness visibility via API.
- Managed imports fail fast with structured readiness error when not ready.

Important current behavior (explicit for Step 3):
- Managed extraction path is now dual-mode:
  - `EXTRACTION_MANAGED_FLOW_MODE=new` (default): LlamaExtract Jobs flow.
  - `EXTRACTION_MANAGED_FLOW_MODE=legacy`: previous managed provider flow.

### Step 3 Completion Record (for future agent handoff)

Implemented:
- Managed-flow router in worker with `EXTRACTION_MANAGED_FLOW_MODE`:
  - supported values: `new | legacy`,
  - default/invalid fallback: `new` with warning.
- Step 2 readiness gate preserved in-place (unchanged contract):
  - blocks managed extraction when not ready,
  - deterministic error remains `EXTRACTION_AGENT_NOT_READY:<state>`.
- New LlamaExtract Jobs flow in connectors:
  - file upload + job create + status polling + result fetch,
  - compatibility handling for endpoint/field drift (`/api/v1/beta/files` and `/api/v1/files` variants).
- Strict response validation and row mapping:
  - `transactions[]` required,
  - statement period resolution and derivation markers in diagnostics.
- Statement persistence and linkage:
  - `upsert_or_get_statement(...)`,
  - `import_rows.statement_id`,
  - commit path populates `transactions.statement_id`.
- Additive migration:
  - `0005_import_rows_statement_link`.
- Diagnostics expansion on import status:
  - `managed_flow_mode`,
  - `provider_lineage`,
  - `poll_status_trail`,
  - `statement_context`,
  - `agent_readiness`.
- Observability updates:
  - status transition events (`parsing`, `review_required|ready_to_commit`, `failed`),
  - structured Jobs lifecycle logs,
  - common raw external API response log:
    `~/Library/Application Support/SpendoraDesktop/logs/external-api-raw.log`.
- Polling guardrail:
  - hard cap of 3 minutes per Jobs polling lifecycle.

Validated:
- Rust workspace tests pass (`cargo test --workspace`).
- Managed `new` flow produces extracted rows and import transitions.
- Legacy managed flow remains selectable via env fallback mode.

## Summary

Implement blueprint-based statement ingestion using LlamaExtract Jobs with strict validation, additive DB migrations, DB-first reuse for free-tier control, and full test coverage, while preserving current manual upload/review/commit UX.

## Step 1 — Schema + Config Foundation (DB and env contracts)

- Add additive migrations (no new DB):
  - Create `statements` table with statement identity, period metadata, provider lineage, and schema version.
  - Add `transactions.statement_id` nullable FK and indexes for account/date and statement joins.
  - Add uniqueness constraint on statement period per account (`account_id + period_start + period_end`).
- Extend settings contract for runtime config:
  - `LLAMA_CLOUD_API_KEY`, `LLAMA_AGENT_NAME`, `LLAMA_SCHEMA_VERSION`, optional org/project IDs.
  - Persist resolved agent metadata in app settings (cached `agent_id`, schema version, `updated_at`).
- Define versioned local extraction blueprint schema file (e.g., `statement_v1`) mapped to DB target fields:
  - statement period fields,
  - `transactions[]` with required keys (`booked_at`, `description`, `amount_cents`, optional confidence/meta).
- Acceptance criteria:
  - migrations apply on existing DB without data loss,
  - app boots with missing optional env values and fails fast with clear error when required extraction env missing.

## Step 2 — Llama Agent Bootstrap + Validation Gate

- Implement startup “auto-ensure agent” routine:
  - Load schema by `LLAMA_SCHEMA_VERSION`.
  - Call schema validation endpoint; fail startup/bootstrap path on invalid schema.
  - Resolve existing agent by configured `LLAMA_AGENT_NAME` + schema version metadata.
  - Create/update agent when missing or stale; cache resulting `agent_id`.
- Add explicit runtime health/diagnostic visibility:
  - agent readiness state (`configured`, `missing`, `schema_invalid`, `api_unreachable`) exposed via existing diagnostics/status flow.
- Preserve strict validation requirements:
  - no extraction job submission unless agent is validated and resolvable.
- Acceptance criteria:
  - cold start can create agent once and reuse subsequently,
  - schema version bump triggers deterministic update/replace path,
  - failures are surfaced with structured error codes.

## Step 3 — Managed Extraction Pipeline Replacement (Jobs flow)

- Replace current managed parse-first provider call path with LlamaExtract Jobs:
  - Upload file (prefer `/api/v1/beta/files`, fallback compatibility handling for `/api/v1/files` drift where needed).
  - Submit extraction job using ensured `agent_id`.
  - Poll job state with terminal handling:
    - result-bearing: `SUCCESS`, `PARTIAL_SUCCESS`
    - failure: `ERROR`, `CANCELLED`
  - Fetch job result and persist provider lineage (`job_id`, `run_id`, metadata/payload).
- Implement response validator before row mapping:
  - validate top-level result envelope,
  - validate `transactions[]` presence and required keys,
  - validate/normalize `booked_at` to `YYYY-MM-DD`,
  - map invalid rows to review/errors with structured diagnostics (never silent drop).
- Statement-period derivation:
  - prefer explicit period from response schema,
  - fallback to min/max transaction dates when absent, with derivation marker in diagnostics.
- Acceptance criteria:
  - managed path no longer depends on markdown/table heuristics as primary behavior,
  - imports transition through existing statuses with equivalent UX semantics,
  - partial-success payloads still produce reviewable rows.

## Step 4 — DB-First Reuse + UI/Query Extensions (cost control + history)

Current implementation note (latest):
- Desktop startup now auto-starts API/Worker from Tauri shell boot.
- UI was hard-reset to a blank foundation to rebuild Step 4 UX cleanly.
- Step 4 backend/query capabilities remain in place; UI wiring is pending redesign.

- Add import preflight for selected account + timeframe:
  - UI collects start/end date or month.
  - backend checks `statements` coverage before Llama call.
  - if found: skip extraction, reuse existing statement/transactions, return reuse indicator.
  - if not found: continue normal extraction.
- Extend API payloads:
  - include statement summary (`statement_id`, `period_start`, `period_end`, `statement_month`, `reused`).
  - add statement listing/query support for timeframe browsing.
- Commit path updates:
  - commit approved rows only,
  - preserve dedupe insert behavior,
  - write `transactions.statement_id`,
  - persist inserted/duplicate/review counters.
- Clarify row semantics in code/docs:
  - `booked_at` = transaction posting date; statement period stored separately.
- Acceptance criteria:
  - re-uploading existing month does not invoke Llama by default,
  - historical month/date browsing works from DB only,
  - transaction lineage to statement is queryable.

## Step 5 — Full Verification Matrix and Rollout Guardrails

- Unit test suite additions:
  - env parsing + agent ensure decision tree,
  - schema validation pass/fail,
  - result validation (missing keys, invalid dates, partial rows),
  - period derivation and statement uniqueness enforcement.
- Integration test suite additions:
  - mocked end-to-end Llama Jobs flow (upload→job→poll→result→persist),
  - existing-period short-circuit reuse (no provider call),
  - commit dedupe + `statement_id` linkage correctness.
- Regression and compatibility tests:
  - existing manual import/review/commit UI still functions,
  - existing transaction filter endpoints continue working with new joins/fields.
- Operational checks:
  - structured diagnostics for provider and validation failures,
  - migration safety on populated DB,
  - documented fallback behavior and env requirements.
- Acceptance criteria:
  - all new tests pass,
  - no regression in current flow,
  - rollout-ready with deterministic startup and clear failure reporting.

## Assumptions and Defaults

- Keep existing single-database architecture (SQLite).
- Keep non-destructive overlap policy (dedupe + insert new only).
- Agent lifecycle is env-governed auto-ensure at startup.
- Existing-period default is skip Llama and reuse DB data.
