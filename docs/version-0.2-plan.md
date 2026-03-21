# Version 0.2 Plan

## Summary

This plan upgrades manual statement ingestion to a schema-blueprint extraction flow using LlamaExtract Jobs while preserving the current working pipeline shape (manual upload, review, commit, transaction browsing). The goal is to improve extraction quality and reliability, reduce heuristic parsing, minimize free-tier usage, and store statement-period data for month-over-month continuity.

Key constraints and decisions already agreed:

- Reuse the existing SQLite database file; no new DB.
- Keep and strengthen validation; do not remove it.
- Configure extraction-agent behavior from `.env`.
- Use startup auto-ensure logic for extraction agent existence/compatibility.
- If statement data for selected timeframe already exists in DB, skip Llama calls.
- Add UI support for start/end date (or month) filtering and statement existence checks.
- Add comprehensive tests for all new behavior.

---

## Product Intent and Target Behavior

### Big-picture data model intent

- Each statement upload must become a first-class statement record with statement period boundaries.
- The system must accumulate statement history month-over-month.
- All transactions from all uploaded months should remain queryable in DB.
- Users should be able to retrieve data by date range/month without re-extracting old statements.

### Cost-control intent (free-tier aware)

- Before invoking Llama for a selected timeframe/account, check DB for existing statement coverage.
- If statement already exists for that period, skip extraction and reuse stored results.

---

## Final Decisions Locked

### Agent lifecycle strategy

- **Auto Ensure (startup/bootstrap):** app validates schema + ensures agent exists and is usable at startup.
- Agent identity and schema governance come from env config.

### Statement uniqueness/sequencing

- **Canonical uniqueness:** one statement per `account_id + period_start + period_end`.
- Enforce with DB unique constraint.

### Overlap handling

- **Policy:** deduplicate and insert only new transactions.
- Never delete overlapping historical rows by default.

### Existing-period upload behavior

- **Policy:** skip Llama call and reuse existing statement/transactions when matching period exists.

### Agent env mode

- **Mode:** `agent name + schema version` in env.
- Runtime resolves actual agent ID by ensure logic and persists cached ID/settings.

---

## Implementation Changes

## 1) Database and Persistence (Migration-Only, Existing DB)

No separate database is introduced. Existing schema is evolved via additive migrations.

### New/extended schema

- Add `statements` table with at least:
  - `id`
  - `account_id`
  - `source_import_id`
  - `file_name`
  - `file_hash`
  - `period_start`
  - `period_end`
  - `statement_month` (derived canonical month key, e.g. `YYYY-MM`)
  - `provider`
  - `provider_job_id`
  - `provider_run_id`
  - `provider_payload_json`
  - `provider_metadata_json`
  - `schema_version`
  - timestamps
- Constraint:
  - `UNIQUE(account_id, period_start, period_end)`
- Extend `transactions`:
  - add `statement_id` (nullable FK)
  - keep current idempotent insert behavior (`ON CONFLICT(account_id, external_txn_id) DO NOTHING`)
- Add indexes for timeframe checks and query performance:
  - statement period lookups by account and date range
  - transaction lookup by account/date and optional statement link

### Data lineage guarantees

- Each committed transaction should be attributable to statement/import source.
- Keep import-level and provider-level artifacts for troubleshooting/audit.

---

## 2) Extraction Integration Upgrade (Managed Mode)

Replace current managed parse-first internals with LlamaExtract Jobs as canonical extraction path.

### Canonical Llama flow

1. Upload file (`/api/v1/beta/files`, with fallback handling for `/api/v1/files` doc drift if needed per tenant behavior).
2. Ensure extraction agent is available and schema-compatible.
3. Create extraction job (`/api/v1/extraction/jobs`).
4. Poll job status until terminal.
5. Fetch result (`/api/v1/extraction/jobs/{job_id}/result`).

### Terminal state handling

- Result-bearing terminal states: `SUCCESS`, `PARTIAL_SUCCESS`
- Failure terminal states: `ERROR`, `CANCELLED`

### Managed parsing fallback behavior

- Heuristic pages/markdown parsing is no longer primary for managed mode.
- It may remain behind an explicit internal fallback flag only for controlled recovery, not default.

---

## 3) Validation (Explicitly Preserved and Strengthened)

Validation remains mandatory at both schema and response layers.

### A) Agent/schema validation

- Validate blueprint schema using Llama schema validation endpoint before agent create/update/use.
- Fail fast startup/bootstrap if schema invalid.

### B) Runtime response validation

Validate extraction result before row mapping/DB writes:

- Required envelope fields present (including extract result payload block).
- Required row fields exist for each transaction row:
  - `booked_at`
  - `description`
  - `amount_cents`
- `booked_at` format normalization/validation to `YYYY-MM-DD`.
- Invalid/missing fields are surfaced as structured import errors/review items (not silently dropped).

### C) Statement-period validation

- If schema includes explicit statement period fields, validate and use them.
- If absent, derive from transaction min/max dates with explicit diagnostic marker.

---

## 4) Agent Configuration via Environment

Add/standardize env keys:

- `LLAMA_CLOUD_API_KEY` (already available)
- `LLAMA_AGENT_NAME`
- `LLAMA_SCHEMA_VERSION`
- optional:
  - `LLAMA_CLOUD_PROJECT_ID`
  - `LLAMA_CLOUD_ORGANIZATION_ID`

### Startup auto-ensure sequence

1. Read env config.
2. Load local versioned schema blueprint (e.g., statement schema v1 file).
3. Validate schema remotely.
4. Resolve existing agent by configured name/version metadata.
5. Create/update agent when needed.
6. Persist resolved `agent_id` and schema version in app settings for runtime usage.

This avoids manual repeated setup while still allowing controlled schema versioning.

---

## 5) UI/Workflow Changes

### Import form additions

- Add statement period selection/visibility (`start` / `end` date or month).
- On submit, perform DB existence check for account+period.

### DB-first short-circuit behavior

- If period exists:
  - skip Llama call
  - return/reuse existing statement and related transactions
  - mark flow as reused/no-op extraction path
- If period does not exist:
  - proceed with Llama extraction flow

### Query UX

- Add start/end filter support in UI for transaction browsing.
- Add month-oriented browsing option backed by statement periods.

---

## 6) Commit and Save Semantics

- Continue review/approval gating before final commit (existing flow preserved).
- Commit inserts approved, parse-valid rows only.
- Use dedupe guard to avoid duplicate transaction insertion.
- Link inserted transactions to statement via `statement_id`.
- Persist final import/statement summary counts:
  - inserted
  - duplicate
  - review-required
  - failed rows

---

## 7) Clarification: `booked_at`

- `booked_at` is the **transaction posting date** for each transaction row extracted from the statement.
- It is not the statement month key.
- Statement period/month are stored separately at statement level.

---

## API/Interface Impact

Planned API behavior adjustments/additions (high level):

- Extend import status payload to include statement summary fields (`statement_id`, `period_start`, `period_end`, reuse indicator).
- Add statement existence check endpoint (or equivalent import preflight behavior).
- Add/list statements endpoint for period-level history.
- Keep existing transactions endpoint and augment by date/month filter compatibility.

---

## Testing Plan (All New Behavior Must Be Covered)

### Unit tests

- Env parsing and agent ensure logic:
  - create path
  - reuse path
  - schema-version mismatch update path
- Schema validation handling and error propagation.
- Runtime result validation:
  - missing required keys
  - invalid date formats
  - partial success mapping
- Statement period derivation and uniqueness behavior.

### Integration tests

- Full managed import with mocked Llama Jobs lifecycle:
  - upload -> job create -> poll -> result -> row mapping -> status updates
- Existing-period short-circuit:
  - DB has statement for period -> skip Llama -> return reused result
- Commit behavior:
  - insert new rows only
  - duplicates ignored
  - statement linkage persisted

### Regression tests

- Existing manual flow remains functional.
- Review, status progression, and commit endpoints remain backward compatible for current UI expectations.

---

## Acceptance Criteria

- User can upload monthly statements and build month-over-month historical data.
- Re-uploading existing period does not consume Llama calls by default.
- All extracted rows written to DB are schema-validated and date-normalized.
- Statement records are queryable by timeframe and linked to committed transactions.
- Full automated test coverage exists for all newly introduced logic paths.

---

## Rollout Notes

- Implement as additive migration + feature-complete backend + UI filter updates.
- Keep existing import flow UI semantics (`Create Import`, review, `Commit`) to minimize UX disruption.
- Prefer configuration-driven agent/schema evolution via env and settings cache.

