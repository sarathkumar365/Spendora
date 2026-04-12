# Spendora Statement v2 Upgrade Plan (3 Phases)

Date: 2026-04-11
Owner: Product + Backend + Extraction
Status: Planning only (no implementation in this document)

## 1) Summary

This plan upgrades Spendora extraction and data contracts so debit/credit direction and statement balances become reliable source-of-truth for the personal finance assistant.

Chosen defaults:
1. Canonical transaction model: signed `amount_cents` + explicit `direction`.
2. Direction enum: `debit | credit | transfer | reversal | unknown`.
3. Metadata scope: core statement metadata only.
4. Ambiguity policy: `unknown` + review-required (no forced auto-resolution).
5. PII policy: mask and retain essentials only (no full holder address/name persistence).

---

## 2) Compatibility Check with Llama Pipeline

### Current state verified
1. Spendora currently validates and consumes `statement_v1` shape (`booked_at`, `description`, `amount_cents`) and strict schema rules.
2. Worker/runtime schema selection is driven by `LLAMA_SCHEMA_VERSION` and agent bootstrap.
3. Current mapping paths are v1-oriented; direction/balance metadata is not propagated end-to-end.

### LlamaExtract compatibility conclusion
1. LlamaExtract is schema-driven and supports nested objects, arrays, enums, and nullable fields (`anyOf` + `null`) in practice.
2. Proposed `statement_v2` shape is compatible with Llama extraction style, but Spendora parser/mapping/storage must be upgraded to consume it.

### Rollout compatibility decision
1. Keep v1 read compatibility for historical imports.
2. Introduce v2 contract and switch runtime default to `statement_v2` after Phase 1 validation.

---

## 3) Phase 1 — Extraction Contract + Llama Mapping

Goal: make direction and statement balances extractable and normalized before broad DB/API rollout.

### 3.1 Schema
Add `statement_v2` contract with:

1. Root required fields:
- `period_start`
- `period_end`
- `account_summary`
- `transactions`

2. `account_summary` core fields:
- `opening_balance_cents`
- `opening_balance_date`
- `closing_balance_cents`
- `closing_balance_date`
- `total_debits_cents`
- `total_credits_cents`
- `account_type`
- `account_number_masked`
- optional `currency_code`

3. `transactions[]` required fields:
- `booked_at`
- `description`
- `amount_cents` (signed)
- `direction` (`debit|credit|transfer|reversal|unknown`)

4. `transactions[]` optional useful metadata:
- `direction_confidence`
- `running_balance_cents`
- `transaction_type_raw`
- `counterparty`
- `reference_id`
- `meta`

### 3.2 Extraction rules
1. Update extraction instructions/prompts to require explicit `direction` and signed amount consistency.
2. If evidence is weak or contradictory, require `direction=unknown` instead of guessing.

### 3.3 Normalization rules
1. Enforce consistency:
- `debit` implies `amount_cents < 0`
- `credit` implies `amount_cents > 0`
2. Conflicts are flagged and routed to review (`unknown` fallback).
3. Persist per-row diagnostics for direction conflicts and parse quality.

### 3.4 Acceptance criteria
1. Worker can bootstrap and run with `LLAMA_SCHEMA_VERSION=statement_v2`.
2. Extracted rows include direction where determinable, unknown where ambiguous.
3. Statement summary values are captured in diagnostics payload.
4. Existing v1 flows remain readable.

---

## 4) Phase 2 — Persistence + API + Review Contract

Goal: persist and expose v2 truth model end-to-end.

### 4.1 Persistence model additions
1. Transaction-level persisted fields:
- `direction`
- `direction_confidence`
- `direction_source` (`model|rule|balance_delta|manual`)

2. Statement-level persisted fields:
- opening/closing balances and dates
- total debits/credits
- masked account metadata
- schema version

### 4.2 Pipeline propagation
1. Import row normalized payload carries direction fields.
2. Commit path writes direction fields into transactions.
3. Historical rows with missing direction remain valid and map to `unknown`.

### 4.3 API additions
1. Import review/status payloads expose direction/confidence/source and conflict flags.
2. Transactions endpoints expose direction fields in list/detail responses.
3. Statements endpoints expose account summary metadata (opening/closing/totals).
4. Changes are additive to avoid client breakage.

### 4.4 Review behavior
1. Reviewer can override direction and amount sign.
2. Manual override is highest-precedence truth (`direction_source=manual`).

### 4.5 Acceptance criteria
1. New imports persist direction and balances end-to-end.
2. Existing v1 data remains queryable.
3. UI/API no longer depend on sign-only inference for inflow/outflow.

---

## 5) Phase 3 — Reconciliation, Quality Controls, and Rollout

Goal: make direction/balance data production-trustworthy for assistant insights.

### 5.1 Reconciliation checks
1. Verify statement equation:
- `opening_balance + net_movement ~= closing_balance` (tolerance policy)
2. Verify totals:
- sum of debit rows vs `total_debits_cents`
- sum of credit rows vs `total_credits_cents`

### 5.2 Deterministic fallback enrichers
For unresolved rows only:
1. Withdrawal/deposit column cues when available.
2. Running-balance delta cues.
3. Rule hints from transaction wording/patterns.

Rule: fallbacks may classify unresolved rows but never override manual corrections.

### 5.3 Quality gates
1. Low-confidence/conflicting rows remain `unknown` and review-required.
2. Metrics to track:
- unknown-rate
- conflict-rate
- manual-override-rate
- reconciliation-fail-rate

### 5.4 Assistant safety behavior
1. Insights layer uses explicit direction first.
2. Rows with `unknown` direction are excluded or called out clearly in calculations.

### 5.5 Acceptance criteria
1. Reconciliation pass rate meets target threshold.
2. Unknown/conflict metrics are visible and improving.
3. Agent insights operate safely with mixed v1/v2 historical data.

---

## 6) Test Plan

1. Contract tests:
- `statement_v2` required keys, enums, and nullability behavior.

2. Extraction parser tests:
- valid v2 payload mapping,
- direction-sign conflict handling,
- unknown fallback behavior.

3. Storage/API tests:
- new fields persisted and returned,
- additive response compatibility.

4. End-to-end tests:
- import -> review -> commit -> transactions list,
- statement summary retrieval,
- override precedence.

5. Regression tests:
- v1 historical data remains functional,
- schema-version bootstrap/readiness behavior remains deterministic.

---

## 7) Useful Metadata to Save (Recommended)

These are useful for future AI insights while staying practical in v2:

1. Transaction metadata:
- `direction`
- `direction_confidence`
- `direction_source`
- `running_balance_cents` (if present)
- `transaction_type_raw`
- `merchant_normalized` (if derivable)
- `counterparty` (if derivable)
- `reference_id` (if present)

2. Statement metadata:
- `opening_balance_cents`
- `opening_balance_date`
- `closing_balance_cents`
- `closing_balance_date`
- `total_debits_cents`
- `total_credits_cents`
- `account_type`
- `account_number_masked`
- `currency_code`
- `schema_version`

3. Data quality metadata:
- per-row conflict flags
- parse warnings/errors
- reconciliation result markers

---

## 8) Assumptions

1. Backend/storage can be enhanced as needed (as requested).
2. Runtime can run one active extraction schema version at a time.
3. Historical data can remain mixed (`statement_v1` + `statement_v2`) with read compatibility.
4. No full PII persistence beyond masked essentials.

