# Agent — Financial Awareness (MVP Plan)

Date: 2026-05-15 (updated 2026-05-15)
Owner: Sarathkumar
Status: Phases 1–6 shipped (Category Intelligence + Audit Trail complete)
Timeline: ~2 days of continuous coding

## Goal
Ship a chat-first agent inside Spendora that lets the user ask any question about their money and get a trustworthy, cited answer grounded in their local SQLite data — including category questions powered by a self-learning merchant classifier.

## User-Facing Features (the product)
1. **Ask Anything Chat** — full-screen chat tab, multi-turn. ✅ shipped
2. **Cited Answers** — every figure links back to the underlying transactions. ✅ shipped
3. **Spending Lookups** — totals by merchant, account, date range. ✅ shipped
4. **Period Comparisons** — month-over-month, year-over-year, custom windows. ✅ shipped
5. **Top Merchants** — ranked views on demand (via aggregate group_by). ✅ shipped
6. **Subscription Finder** — detect recurring charges with cadence + next-due estimate. ✅ shipped
7. **Transaction Explainer** — deep-dive on any single charge with history + similar txns. ✅ shipped
8. **Multi-Account Awareness** — agent disambiguates accounts/cards without IDs. ✅ shipped
9. **Smart Follow-ups** — 2–3 one-click next questions after every answer. ✅ shipped
10. **Starter Prompts** — cold-start screen with prebuilt questions. ✅ shipped
11. **Category Intelligence** — "how much on groceries / dining / utilities" with a self-learning merchant classifier. 🚧 Phase 5

## Non-Goals (explicit deferrals)
- Forecasting, "safe to spend today", cash-flow projection
- Goal tracking / budgeting
- Persistent agent memory across sessions beyond chat history + learned categories
- Sankey/flow visualization or charts rendered by the agent
- Proactive dashboard cards (chat-first decision; revisit post-MVP)
- Onboarding interview agent
- Plaid / live sync
- Rule creation by chat (mutation tools deferred)
- Custom user-defined categories / subcategories / split-merchant categorization
- Auto-categorization on import (today: lazy via chat questions)
- LLM-loop end-to-end automated tests (manual dogfooding only for MVP)

## Architecture (high level)
- **Backend**: `crates/agent` expanded with multi-turn runtime, tool registry, LLM provider trait. Exposes `POST /api/v1/agent/chat` (SSE) + `GET /api/v1/agent/context` via the `api` crate.
- **LLM**: provider-pluggable. Default = **OpenAI direct** (`OPENAI_API_KEY` + `OPENAI_MODEL`, default `gpt-4o-mini`). Second provider = **local** OpenAI-compatible endpoint (Ollama, LM Studio) via `LOCAL_LLM_BASE_URL` + `LOCAL_LLM_MODEL`. Selection via `AGENT_LLM_PROVIDER=openai|local`. Both providers must support OpenAI tool-calling.
- **Tool runtime**: capped multi-turn loop (≤6 iterations). Each tool receives `AgentDeps { db, llm }` (refactored in Phase 5b so tools can call the LLM for classification).
- **Data access**: read-only SQL via `sqlx` against existing schema + 3 new tables (Phase 5a) for category intelligence.
- **UI**: chat tab in `apps/expense-desktop-ui`. SSE streaming. Inline confirmation cards for category resolution.
- **Context priming**: on chat open, UI fetches `/agent/context` with accounts list, today's date, currency, data range. Injected into every system prompt.

## Tool Catalog (the agent's hands)

| Tool | Purpose | Status |
|---|---|---|
| `list_accounts_and_cards` | Enumerate accounts/cards with last4, names | ✅ |
| `query_transactions` | Filter by date/account/merchant/amount/direction | ✅ |
| `aggregate_transactions` | group_by × metric × window | ✅ |
| `compare_periods` | Two windows, diff aggregations | ✅ |
| `find_recurring` | Detect subscriptions with cadence + next-due | ✅ |
| `transaction_detail` | Single-txn deep dive with similar charges | ✅ |
| `echo` | Debug-only round-trip tool | ✅ |
| `resolve_category_intent` | First-time classifier + suggestion gathering | 🚧 5c |
| `confirm_category_assignments` | Persist user confirm/override choices | 🚧 5d |

All tools return `{ summary, data, transaction_ids }` so the UI can render citation chips and cache rows for the drawer uniformly.

## Phasing

See [phases.md](phases.md) for the original Phase 1–4 plan (now shipped). See [phase-5-implementation.md](phase-5-implementation.md) for the detailed Phase 5 plan (Category Intelligence + polish).

| Phase | Theme | Status |
|---|---|---|
| 1 | Foundation — chat endpoint, tool loop, UI shell, dummy tool | ✅ shipped |
| 2 | Core data tools — query, aggregate, accounts | ✅ shipped |
| 3 | Citations + context priming + UX shell | ✅ shipped |
| 4 | Smart tools — compare, recurring, detail | ✅ shipped |
| 5a | Migration 0012: merchant_signatures + assignments + history; seed categories | 🚧 next |
| 5b | Refactor `Tool` trait → `AgentDeps { db, llm }`; update all 7 existing tools | 🚧 |
| 5c | `resolve_category_intent` tool with LLM classifier + persistence | 🚧 |
| 5d | `confirm_category_assignments` tool + `merchant_substrings` array filter | 🚧 |
| 5e | UI confirmation card + new SSE `category_confirmation_needed` event | 🚧 |
| 5f | Polish: token-budget guard, UI error states, README, smoke test | 🚧 |

## Risks & Mid-Flight Signals

| Risk | Signal | Mitigation |
|---|---|---|
| Chosen model picks wrong tool sequences on compound questions | Manual dogfood reveals wonky reasoning | Swap model via env (`OPENAI_MODEL=gpt-4o` or larger) |
| Local models have weaker tool-calling than OpenAI | Tool args malformed or ignored on local provider | Document minimum recommended local models; keep OpenAI as default |
| Tool-loop latency >10s feels broken | First end-to-end test | Stream tool-call chips immediately so user sees progress |
| LLM hallucinates transaction IDs in answers | Citation chip clicks fail | Server-side: only IDs tools actually returned can be cited; cache rows in UI |
| Tool trait refactor (5b) breaks existing tools | `cargo test -p agent` fails | Atomic refactor + run all 22 existing tests before moving to 5c |
| LLM-classified categories drift from user intent | Wrong merchants in answers | User confirmation card on first use of every category; `user_overridden` rows never re-suggested |
| Token budget exceeded on big aggregate result | LLM returns truncated/error | Phase 5f token guard: cap tool result size sent to LLM at 16 KB |
| Category taxonomy lock-in | Users want custom categories | v1 ships with 13 seeded categories; custom + subcategories deferred to follow-up |
| Cold-start friction on every new category | User annoyed by repeated cards | Card only appears for unclassified merchants; after a few uses, asymptotes to silent |

## Validation
- **Phase 1 done**: ✅ Chat tab streams an LLM reply; echo tool round-trips.
- **Phase 2 done**: ✅ "How much did I spend last month?" answers correctly with real data.
- **Phase 3 done**: ✅ Citation chips open the drawer; follow-up chips work; markdown renders.
- **Phase 4 done**: ✅ Compare/recurring/detail tools answer their representative prompts.
- **Phase 5 done**: see [phase-5-implementation.md § Validation gates](phase-5-implementation.md).

## Linked Decisions
- `LlmProvider` trait with two impls: `OpenAiProvider` (default) and `LocalOpenAiCompatibleProvider`. Existing OpenRouter client in `connectors_ai` left alone (different concern: statement extraction).
- Tool-calling uses OpenAI Chat Completions `tools` + `tool_calls` schema.
- Read-only data tools only in Phase 1–4. Phase 5 introduces **two mutation tools** for category assignments (with explicit user confirmation gating).
- Migration 0012 introduces 3 new tables. Backward-compatible — existing data untouched.
- Chat history persisted via localStorage (Phase 3). DB-backed chat history still deferred.
- LLM is the kernel of the category classifier today. Architecture lets us swap it for heuristics, embeddings, or a per-user classifier later without changing the tool API.

## Build Order Summary
Phase 1 → 2 → 3 → 4 (shipped) → 5a → 5b → 5c → 5d → 5e (sequential, each depends on prior). 5f (polish) can run alongside 5e.
