# Version 0.2 — 5-Step Technical Implementation Plan

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
