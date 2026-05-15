# Phases — Agent Financial Awareness

Each phase is independently reviewable, mergeable, and leaves the app in a usable state.

## Status at a glance

| Phase | Theme | Status |
|---|---|---|
| 1 | Foundation — chat shell + agent loop + dummy tool | ✅ shipped |
| 2 | Core data tools — query, aggregate, accounts | ✅ shipped |
| 3 | Citations + context priming + UX polish | ✅ shipped |
| 4 | Smart tools — compare, recurring, detail | ✅ shipped |
| **5** | **Category Intelligence + final polish** | **🚧 detailed in [phase-5-implementation.md](phase-5-implementation.md)** |

Phase 5 supersedes the original "Polish & dogfood" Phase 5 below. The original section is preserved at the bottom for reference but is **not the current plan** — its scope (token-budget guard, error states, README, smoke test) is now Sub-phase 5f inside the new doc, alongside 5a–5e for the learning category classifier.

---

## Phase 1 — Foundation (chat shell + agent loop) ✅ SHIPPED

**Goal:** End-to-end wire from React chat input → Rust endpoint → OpenRouter → streamed text back. One dummy tool proves the tool-call loop works.

**Backend (`services/expense-rs`)**
- Expand `crates/agent`:
  - `AgentRequest` / `AgentResponse` types
  - Tool trait + registry
  - Multi-turn loop with iteration cap (default 6)
  - **`LlmProvider` trait** with OpenAI Chat Completions semantics (messages, tools, tool_calls, streaming).
  - **`OpenAiProvider`** — direct call to `https://api.openai.com/v1/chat/completions`. Env: `OPENAI_API_KEY`, `OPENAI_MODEL` (default `gpt-4o-mini`).
  - **`LocalOpenAiCompatibleProvider`** — same wire format, different base URL. Env: `LOCAL_LLM_BASE_URL` (e.g. `http://127.0.0.1:11434/v1` for Ollama), `LOCAL_LLM_MODEL` (e.g. `llama3.1:8b-instruct`), optional `LOCAL_LLM_API_KEY`.
  - **Provider selector**: `AGENT_LLM_PROVIDER=openai|local` (default `openai`) read once at startup.
- Add to `crates/api`:
  - `POST /api/v1/agent/chat` — streaming (SSE)
  - `GET /api/v1/agent/context` — accounts, categories, today's date, currency
- One dummy tool: `echo(text)` to prove the loop end-to-end.

**Frontend (`apps/expense-desktop-ui`)**
- New route/tab: `/chat`
- Components: `ChatPage`, `MessageList`, `MessageInput`, `StreamingAssistantMessage`
- SSE client hook
- Basic styling consistent with existing UI

**Done when:**
- User opens chat tab, types "say hi", agent streams a greeting (via `OPENAI_API_KEY`).
- User types "echo hello world", agent calls the echo tool and returns the result.
- Tool-call cap prevents runaway (test by forcing a loop).
- Setting `AGENT_LLM_PROVIDER=local` + a running local server (Ollama/LM Studio) routes through the local provider with no code change.

**Out of scope this phase:** real tools, citations, system prompt priming, follow-ups.

---

## Phase 2 — Core data tools ✅ SHIPPED

**Goal:** Agent can read your real money data.

**Tools to implement (all read-only SQL):**
- `list_accounts_and_cards()` → `[{id, name, mask, last4, customer_name, currency}]`
- `query_transactions(filters)` → rows + `transaction_ids[]`. Filters: `date_from`, `date_to`, `account_id?`, `category?`, `merchant_substring?`, `amount_min?`, `amount_max?`, `direction?` (in/out), `limit` (default 100, cap 500).
- `aggregate_transactions(filters, group_by, metric)` → `[{group, value}]` + `transaction_ids[]`. `group_by`: category | merchant | account | day | week | month. `metric`: sum | count | avg | min | max.

**Backend**
- Implement in `crates/agent/src/tools/`.
- Each tool: typed input struct, typed output struct, JSON schema for the LLM, unit test against a fixture DB or the dev DB.
- Register all three in the loop.

**Frontend**
- Render tool-call "chips" while agent is thinking ("Querying transactions…").

**Done when:**
- "How much did I spend last month?" → correct number from your real data.
- "List my accounts." → returns all cards with masks.
- "How much on dining in April?" → correct number; agent picks `aggregate` with category filter.

---

## Phase 3 — Citations, context priming, UX shell ✅ SHIPPED

**Goal:** Trust + first-impression polish. The features that make it *feel* like a product.

**Backend**
- `GET /api/v1/agent/context` returns: accounts, categories, currency, today's date, conversation hint metadata.
- System prompt builder: injects context payload, today's date, schema knowledge, formatting rules (currency, dates).
- Citation safety: the chat endpoint tracks all `transaction_ids` returned by tool calls in the turn; any IDs the LLM mentions in its final answer are validated against this set and rendered as chips. IDs not in the set are stripped.

**Frontend**
- **Citation chips** below each assistant message; click → drawer with the transactions (reuse existing transactions table component if possible).
- **Starter prompts**: empty-state with 5 buttons:
  - "Where did my money go last month?"
  - "What did I spend at Amazon this year?"
  - "List all my accounts"
  - "How much on dining vs groceries?"
  - "Compare April to March"
- **Smart follow-up chips**: agent asked (via system prompt) to suggest 2–3 follow-ups; rendered as one-click buttons that send the suggestion as the next message.

**Done when:**
- Every numeric answer has clickable citation chips → opens correct transactions.
- Cold-start screen shows starter prompts.
- After every answer, 2–3 follow-up chips appear and work.
- "How much did I spend on my Amex?" routes correctly using context-aware multi-account awareness.

---

## Phase 4 — Smart tools ✅ SHIPPED

Built `compare_periods`, `find_recurring`, `transaction_detail`. Deliberately skipped `top_merchants` / `search_freetext` (already covered by `aggregate_transactions` + `query_transactions`).

**Goal:** Cover the remaining product features.

**Tools to add:**
- `compare_periods(filters, window_a, window_b, group_by?, metric)` → diff aggregation. Handles "April vs March", "this month vs same month last year".
- `top_merchants(filters, limit=10)` and `top_categories(filters, limit=10)` → ranked rollups + IDs.
- `find_recurring(merchant?, lookback_months=6)` → detected subscriptions: `[{merchant, cadence_days, last_charge, next_estimated, sample_txn_ids}]`. Algorithm: group by normalized merchant; if ≥3 charges with similar amount and ~regular interval, flag as recurring.
- `transaction_detail(transaction_id)` → the txn + merchant history + 10 similar txns + recurrence flag.
- `search_transactions_freetext(query, limit=50)` → LIKE search on description + merchant; ranks by recency.

**Done when each of these prompts answers correctly:**
- "April vs March" → period comparison output.
- "Top 5 merchants this year" → ranked list with chips.
- "What subscriptions do I have?" → recurring list with cadence.
- "Explain this $89.99 charge" (paste an ID or describe) → detail view.
- "Find that Toronto charge around $80 in March" → freetext search result.

---

## Phase 5 — Category Intelligence + Polish 🚧 SEE [phase-5-implementation.md](phase-5-implementation.md)

The original Phase 5 below ("Polish & dogfood") is **superseded**. The new Phase 5 ships a self-learning merchant categorization layer (sub-phases 5a–5e) plus the polish work (now 5f). Full details in [phase-5-implementation.md](phase-5-implementation.md). Supporting design docs:

- [category-intelligence-plan.md](category-intelligence-plan.md) — high-level rationale
- [architecture-diagram.md](architecture-diagram.md) — where the new layer plugs in
- [flow-first-time.md](flow-first-time.md) — sequence when the system has to learn
- [flow-learned.md](flow-learned.md) — sequence after the user has confirmed
- [data-model.md](data-model.md) — new SQLite tables (ER diagram)
- [learning-over-time.md](learning-over-time.md) — how it stabilizes

### Sub-phase summary

| | Sub-phase | Time |
|---|---|---|
| 5a | Migration 0012 + storage helpers + seed categories | 45 min |
| 5b | Refactor `Tool` trait → `AgentDeps { db, llm }`, update all existing tools | 30 min |
| 5c | `resolve_category_intent` tool with LLM classifier + assignment persistence | 90 min |
| 5d | `confirm_category_assignments` tool + extend query/aggregate with `merchant_substrings` array | 60 min |
| 5e | UI confirmation card + new SSE event handling + tests | 90 min |
| 5f | Polish: token-budget guard, UI error states, README, smoke test | 90 min |

**Strict build order:** 5a → 5b → 5c → 5d → 5e. 5f can run alongside 5e.

---

## Phase 5 (original) — Polish & dogfood ⛔ SUPERSEDED

**Goal:** Make it not embarrassing.

**Tasks**
- Tool failures: wrap each tool call so errors return as JSON to the agent (not crash). Agent can retry or apologize gracefully.
- Token-budget guard: cap total tokens per request; truncate long tool results.
- Streaming polish: cursor animation, "thinking…" indicator while tools run, abort button.
- Error states in UI: network failure, server 500, rate limit.
- Model A/B: spend 30 min running the same 10-question battery on `gpt-4o-mini` vs `gpt-4o` (and optionally one local model). Pick default via env.
- Local-provider sanity pass: confirm tool-calling actually works against at least one local model (e.g. `qwen2.5:7b-instruct` or `llama3.1:8b-instruct` on Ollama). Document the recommended local model in the feature README.
- Latency check: p95 first-token <3s, full-answer <12s on a 3-tool question.
- README section under `docs/features/agent-financial-awareness/` documenting how to run/test.

**Done when:**
- 15-minute dogfood session with 10+ questions: no crashes, no hallucinated IDs, no broken citations.
- Smoke test script: `npm run test:agent` (new) runs each tool against the dev DB.

---

## Stretch (only after Phase 5 ships clean)
- **S1.** Persist chat history to SQLite (new `agent_conversations` + `agent_messages` tables).
- **S2.** Tool: `detect_anomalies` (txns >2× merchant rolling avg).
- **S3.** Starter prompt: "Give me my monthly narrative" → pre-composed multi-tool query.
- **S4.** First mutation tool: `categorize_transactions(ids, category)` with confirmation step in UI.

---

## Validation Checklist (whole feature)
- [ ] Chat tab opens and accepts input
- [ ] Streaming response works
- [ ] All 8 tools callable by the agent
- [ ] Citation chips render and open correct txns
- [ ] Starter prompts work
- [ ] Smart follow-ups appear
- [ ] Multi-account questions disambiguate correctly
- [ ] Subscription finder returns sensible recurrings on real data
- [ ] No hallucinated transaction IDs in any answer (10-question test pass)
- [ ] p95 first-token <3s
- [ ] Tool failures don't crash the loop
- [ ] `agent.md` quality gates: Rust tests pass, UI build passes, step1 smoke passes
