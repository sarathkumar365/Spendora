# Phase 5 — Implementation Plan

Date: 2026-05-15
Status: Approved, ready to build
Picks up from Phase 4 (3 smart tools shipped, 22 agent tests green).

Two parallel threads:
- **5a–5e**: Category Intelligence (learning categorization layer)
- **5f**: Polish (token budget, error states, README, smoke script)

Total estimate: ~7 hours of continuous coding.

---

## Sub-phase 5a — Migration & schema (45 min)

### Files
- `services/expense-rs/migrations/0012_category_intelligence.sql` (new)
- `services/expense-rs/crates/storage_sqlite/src/lib.rs` (add helpers)

### Migration contents
```sql
-- merchant_signatures: one row per normalized merchant
CREATE TABLE merchant_signatures (
  id TEXT PRIMARY KEY,
  normalized_key TEXT NOT NULL UNIQUE,
  display_label TEXT NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  txn_count INTEGER NOT NULL DEFAULT 0,
  total_cents INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_merchant_signatures_normalized_key ON merchant_signatures(normalized_key);

-- merchant_category_assignments: one current category per merchant per category-slot
-- We allow multiple categories per merchant (e.g. Walmart could later split groceries + shopping),
-- but enforce UNIQUE per (merchant, category) so confirmations are idempotent.
CREATE TABLE merchant_category_assignments (
  id TEXT PRIMARY KEY,
  merchant_signature_id TEXT NOT NULL REFERENCES merchant_signatures(id),
  category_id TEXT NOT NULL REFERENCES categories(id),
  source TEXT NOT NULL CHECK (source IN ('llm_suggested', 'user_confirmed', 'user_overridden')),
  included INTEGER NOT NULL DEFAULT 1,           -- 1 = include in category; 0 = explicit "no"
  confidence REAL,
  confirmed_by_user_at TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(merchant_signature_id, category_id)
);
CREATE INDEX idx_mca_category ON merchant_category_assignments(category_id, source, included);
CREATE INDEX idx_mca_merchant ON merchant_category_assignments(merchant_signature_id);

-- category_resolution_history: append-only audit log
CREATE TABLE category_resolution_history (
  id TEXT PRIMARY KEY,
  merchant_signature_id TEXT NOT NULL REFERENCES merchant_signatures(id),
  category_id TEXT NOT NULL REFERENCES categories(id),
  source TEXT NOT NULL,
  user_action TEXT,                              -- 'included' | 'excluded' | NULL for non-user
  occurred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_crh_merchant ON category_resolution_history(merchant_signature_id);

-- Seed default categories (idempotent via INSERT OR IGNORE)
INSERT OR IGNORE INTO categories (id, name, created_at, updated_at)
VALUES
  ('cat-groceries',     'Groceries',           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-dining',        'Dining & Restaurants',CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-transit',       'Transit & Fuel',      CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-utilities',     'Utilities & Bills',   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-entertainment', 'Entertainment',       CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-shopping',      'Shopping',            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-subscriptions', 'Subscriptions',       CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-healthcare',    'Healthcare',          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-travel',        'Travel',              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-income',        'Income',              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-transfers',     'Transfers',           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-fees',          'Fees & Interest',     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('cat-other',         'Other',               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
```

Note: the `categories.slug` column doesn't currently exist in the original schema (just `name`). Use deterministic `id` (`cat-groceries`) as the stable identifier instead of adding a column.

### Storage helpers to add
- `upsert_merchant_signature(pool, normalized_key, display_label, txn_count, total_cents) -> id`
- `list_merchants_in_window(pool, date_from, date_to, account_id?, direction) -> Vec<MerchantSeen>` (joins signatures + transactions, populates new signatures lazily)
- `load_assignments_for_category(pool, category_id) -> Vec<Assignment>`
- `upsert_assignment(pool, merchant_id, category_id, source, included, confidence)`
- `append_history(pool, merchant_id, category_id, source, user_action)`
- `list_categories(pool) -> Vec<Category>` (for the system prompt)

### Done when
- `npm run test:rs` passes
- A test runs the migration on a fresh DB + inserts a few transactions + calls `list_merchants_in_window` → returns deduped merchants with counts.

---

## Sub-phase 5b — Refactor Tool trait → AgentDeps (30 min)

### Why
The classifier needs LLM access. Current trait only has `&SqlitePool`. One atomic refactor.

### Files
- `services/expense-rs/crates/agent/src/tools/mod.rs` (trait change)
- `services/expense-rs/crates/agent/src/runtime.rs` (pass deps to invoke)
- `services/expense-rs/crates/agent/src/tools/*.rs` (every existing tool: `accounts`, `query`, `aggregate`, `compare`, `recurring`, `detail`, `echo`)
- `services/expense-rs/crates/api/src/agent_chat.rs` (build AgentDeps)

### Change
```rust
pub struct AgentDeps<'a> {
    pub db: &'a SqlitePool,
    pub llm: &'a dyn LlmProvider,
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn parameters_schema(&self) -> Value;
    async fn invoke(&self, deps: AgentDeps<'_>, args: Value) -> Result<ToolOutput>;
    // ...
}
```

Existing tools just rename their first arg and ignore `deps.llm`. Trivial.

### Done when
- All 22 existing tests pass after the refactor.
- `cargo build -p api` clean.

---

## Sub-phase 5c — `resolve_category_intent` tool (90 min)

### Files
- `services/expense-rs/crates/agent/src/tools/resolve_category_tool.rs` (new)
- Register in `tools/mod.rs::build_default_registry`

### Behavior
1. Resolve `category` arg (string like "groceries") to `category_id` via name match (case-insensitive). Error if no match.
2. Default window: last 12 months from today. Honor `date_from`/`date_to` if provided.
3. Call `list_merchants_in_window` — lazily populates `merchant_signatures` for any unseen merchants. Cap result at 200, sorted by `txn_count DESC`.
4. Load existing assignments for `category_id`:
   - `user_confirmed && included=true` → goes into `confirmed` output
   - `user_overridden && included=false` → goes into `excluded` output (don't re-suggest)
   - `llm_suggested` → expires after 24h, re-suggest
5. Compute the "to classify" set: merchants in window minus already-classified-for-this-category.
6. If "to classify" is empty → return immediately. Skip LLM.
7. Otherwise: call the LLM (`deps.llm`) with a dedicated, structured prompt:
   - System: "You are a merchant classifier. For each merchant, output JSON `{merchant_key: confidence_0_to_1}`. Only the merchants that match category=X."
   - User: list of merchant display labels (one per line)
   - Parse JSON response. Robust to formatting glitches (extract JSON block via regex).
8. Filter LLM output by `confidence >= 0.4` threshold (configurable).
9. Persist suggestions: upsert `merchant_category_assignments` with `source='llm_suggested'`, `confidence=N`, `included=true`.
10. Return:
    ```json
    {
      "category": { "id": "cat-groceries", "name": "Groceries" },
      "date_from": "...", "date_to": "...",
      "confirmed": [ { merchant_signature_id, label, txn_count, total_cents, sample_descriptions, confirmed_by_user_at } ],
      "suggested": [ { merchant_signature_id, label, confidence, txn_count, total_cents, sample_descriptions } ],
      "excluded":  [ { merchant_signature_id, label } ],
      "requires_user_confirmation": (suggested.len() > 0)
    }
    ```

### LLM prompt template
```
You classify merchant strings into a specific category.

Category: GROCERIES (food/grocery stores, supermarkets)

Merchants (one per line):
- Loblaws
- Tim Hortons
- Walmart Supercentre
- Uber
- Metro
- Netflix
- ...

Respond with ONLY valid JSON in this exact shape:
{"loblaws": 0.95, "metro": 0.95, "walmart": 0.60, "tim_hortons": 0.08, ...}

Use the lowercase, underscore-joined merchant name as the key.
Confidence 0.0 = definitely not, 1.0 = definitely yes.
Include EVERY merchant in your response. No prose, no markdown — JSON only.
```

### Edge cases
- Window has 0 transactions → return empty confirmed/suggested. Agent says "no transactions in that window."
- LLM returns malformed JSON → wrap in tool error with retry guidance. Runtime already handles tool errors gracefully.
- Same merchant appears in multiple categories (Walmart = groceries OR shopping) → both rows allowed (UNIQUE is on merchant+category, not merchant alone).

### Tests
- Empty window
- Window with only confirmed merchants → no LLM call
- Window with unknown merchants → LLM call mocked → suggestions persisted
- Excluded merchants don't reappear in suggestions

### Done when
- 4+ tests for the tool pass
- Integration: agent can call it via the chat endpoint and get back a structured response

---

## Sub-phase 5d — `confirm_category_assignments` + array filters (60 min)

### `confirm_category_assignments` tool

#### Files
- `services/expense-rs/crates/agent/src/tools/confirm_category_tool.rs` (new)

#### Input
```json
{
  "category": "groceries",
  "assignments": [
    { "merchant_signature_id": "...", "included": true },
    { "merchant_signature_id": "...", "included": false }
  ]
}
```

#### Behavior
1. Resolve category by name.
2. For each assignment:
   - If `included=true` → upsert with `source='user_confirmed'`, `confirmed_by_user_at=now()`, history row with `user_action='included'`
   - If `included=false` → upsert with `source='user_overridden'`, `included=0`, history row with `user_action='excluded'`
3. Return the updated set: `{ confirmed: [...], excluded: [...] }` so the agent can use it directly without re-calling `resolve_category_intent`.

### Array filters on existing tools

#### Files
- `services/expense-rs/crates/agent/src/tools/query_tool.rs`
- `services/expense-rs/crates/agent/src/tools/aggregate_tool.rs`
- `services/expense-rs/crates/agent/src/tools/compare_tool.rs`

#### Change
Add optional `merchant_substrings: Vec<String>` to each. Generate SQL like:
```sql
AND (LOWER(t.description) LIKE '%loblaws%'
  OR LOWER(t.description) LIKE '%metro%'
  OR LOWER(t.description) LIKE '%walmart%')
```
when the array is non-empty. Keep `merchant_substring` (singular) for back-compat. If both passed, agent gets a 400-style tool error.

### System prompt addition
Section "Category & intent resolution":
> For category questions (groceries, dining, transit, utilities, entertainment, subscriptions, healthcare, travel, shopping, income, transfers, fees):
> 1. Call `resolve_category_intent` first.
> 2. If `requires_user_confirmation=true`, EMIT this exact line as your final message: `CATEGORY_CONFIRMATION_NEEDED: <category_slug>`. The UI will render a confirmation card and the user's reply will arrive as the next turn.
> 3. When the user's confirmation reply arrives, call `confirm_category_assignments` with their picks.
> 4. Call `aggregate_transactions` (or `query_transactions`) with `merchant_substrings` set to the final merchant list from confirmed assignments.
> 5. In the answer, name the merchants you included.

### Tests
- `confirm_category_assignments` writes user_confirmed and user_overridden correctly
- `aggregate_transactions` with `merchant_substrings` array returns correct totals
- `query_transactions` with array filter

### Done when
- 5+ new tests pass
- Existing 22 tests still green

---

## Sub-phase 5e — UI confirmation card + new SSE event (90 min)

### Files
- `apps/expense-desktop-ui/src/chat/ChatPanel.tsx` (new event handling + card rendering)
- `apps/expense-desktop-ui/src/styles.css` (card styles)

### New SSE event protocol
The runtime needs a new event kind: `category_confirmation_needed`. Emitted when the agent's final message is `CATEGORY_CONFIRMATION_NEEDED: groceries` — we parse it in the runtime, look up the latest `resolve_category_intent` tool result by tool call id, and emit:

```typescript
{
  kind: "category_confirmation_needed",
  category: { id: "cat-groceries", name: "Groceries" },
  confirmed: [...],   // pre-checked, can't be unchecked
  suggested: [...],   // user toggles
  excluded:  [...]    // hidden by default, shown under "Show excluded"
}
```

### Runtime change
In `runtime.rs`, when the assistant final message matches `^CATEGORY_CONFIRMATION_NEEDED: (\w+)$`:
1. Find the most recent `resolve_category_intent` ToolCallResult in the current run.
2. Build a `CategoryConfirmationNeeded` event from its `data` field.
3. Emit that event instead of `AssistantMessage`.
4. Stop the run (the user's reply will start a new one).

### UI card
Rendered inside the assistant bubble. Layout:
```
┌─────────────────────────────────────────────────────────┐
│ Quick check — which of these are Groceries?            │
│                                                         │
│ Auto-included (you confirmed earlier):                  │
│  ✓ Loblaws       4 txns · $310.00                       │
│  ✓ Metro         2 txns · $84.00                        │
│                                                         │
│ Might be Groceries:                                     │
│  ☐ Walmart       3 txns · $187.00  (LLM: 65% confident) │
│  ☐ Costco        1 txn  · $142.00  (LLM: 55% confident) │
│                                                         │
│ ▸ Show excluded (1)                                     │
│                                                         │
│                                          [   Apply   ]  │
└─────────────────────────────────────────────────────────┘
```

### On Apply
Send a structured user message to the chat:
```
I want groceries to include: Loblaws, Metro, Walmart. Exclude: Costco.
```
The agent reads it, calls `confirm_category_assignments`, then aggregates.

(Why a plain text follow-up vs a structured action? Keeps the chat history coherent — replayable, persistable. The agent parses it.)

### Tests
- Manual smoke: ask "how much on groceries last month" → card appears → tick → see answer
- Verify localStorage persists the confirmation card state so reload works

### Done when
- End-to-end works on real DB
- Manual test pass for 3 categories: groceries, dining, subscriptions

---

## Sub-phase 5f — Polish (90 min)

### Token-budget guard (30 min)

In `runtime.rs`:
- Before passing a tool result back to the LLM, if `data` JSON > 16 KB, truncate. Add a `[truncated: X bytes]` note.
- Cap total messages to ~50 (rare to hit).
- If a `query_transactions` tool returns >100 rows, replace `transactions` array in the LLM-bound copy with a summary only — the UI still gets the full list.

### UI error states (30 min)

In `ChatPanel.tsx`:
- 429 from server → "Hit a rate limit. Try again in a moment."
- 5xx → "Something went wrong on the local API. Check the API logs."
- Network failure → "Couldn't reach the agent. Is the API running?"
- LLM provider misconfigured → already handled (`ctxError`)
- Generic fallback unchanged

### README (15 min)

`docs/features/agent-financial-awareness/README.md`:
- Overview + screenshots placeholder
- Env vars needed (`OPENAI_API_KEY`, `OPENAI_MODEL`, optional `AGENT_LLM_PROVIDER=local`)
- How to run (`npm run tauri:dev`)
- How to test (`cargo test -p agent`, `npm run test:agent`)
- Architecture pointer to the diagram MDs
- Roadmap pointer to deferred items

### Smoke test script (15 min)

`tests/agent/smoke.sh`:
- Asserts the API is running
- Hits `/api/v1/agent/context`
- Hits `/api/v1/agent/chat` with "list my accounts"
- Verifies SSE stream contains `tool_call_start`, `tool_call_result`, `assistant_message`, `done`

Wire into `package.json` as `npm run test:agent`.

### Done when
- All builds green
- 3 manual error-state tests pass (kill API, hit rate limit, bad key)

---

## Validation gates (whole Phase 5)

- [ ] `cargo build --workspace` clean
- [ ] `cargo test -p agent` passes (≥35 tests after additions)
- [ ] `cargo test --workspace` passes
- [ ] `npm run test:ui-build` clean
- [ ] Manual: ask "how much on groceries last month" → card → confirm → answer
- [ ] Manual: ask "how much on groceries this month" (after first confirm) → instant answer, no card
- [ ] Manual: ask "how much on dining last month" → independent card for that category
- [ ] Manual: ask "how much on subscriptions" → integrates with existing `find_recurring`-style answer or new category-card
- [ ] LocalStorage chat survives reload with confirmation card mid-flow
- [ ] `npm run test:agent` (smoke) passes
- [ ] Token-budget guard prevents context overflow on a 500-txn aggregate call

## Build order

Strict sequence (each phase depends on the previous):
1. 5a — migration (no dependencies)
2. 5b — trait refactor (depends on 5a's storage helpers)
3. 5c — resolve_category tool (depends on 5b)
4. 5d — confirm tool + array filters (depends on 5c)
5. 5e — UI card + SSE event (depends on 5d)
6. 5f — polish (independent, can run alongside 5e)

## What this leaves unfinished

Explicitly deferred — track in a follow-up doc:
- Manual category-management UI
- Subcategories
- Custom user-defined categories
- Bulk re-classification jobs
- Embedding-based / heuristic classifier
- Splitting a single merchant across multiple categories (Walmart = part groceries + part shopping)
- Auto-categorization on import (no chat needed)
