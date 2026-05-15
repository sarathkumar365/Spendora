# Agent — Financial Awareness

Chat-first agent inside Spendora that answers ad-hoc questions about your local transactions ("how much did I spend on groceries last month?") with cited answers, follow-up suggestions, and a self-learning category classifier.

All processing happens locally (your SQLite DB) except for the LLM call, which goes to OpenAI (or a local OpenAI-compatible endpoint you configure).

## Status

Phases 1–6 shipped. Plan + diagrams in this folder:
- [plan.md](plan.md) — features, non-goals, risks
- [phases.md](phases.md) — phase-level status
- [phase-5-implementation.md](phase-5-implementation.md) — Category Intelligence subsystem
- [phase-6-audit-trail-plan.md](phase-6-audit-trail-plan.md) — Audit Trail subsystem
- [architecture-diagram.md](architecture-diagram.md) — system shape
- [flow-first-time.md](flow-first-time.md) / [flow-learned.md](flow-learned.md) — user flows
- [data-model.md](data-model.md) — category tables (migration 0012)
- [learning-over-time.md](learning-over-time.md) — how the system stabilises

## Configuration

Set in `.env` (gitignored) at the repo root. Tauri loads it on startup and injects into the spawned API process.

| Var | Purpose |
|---|---|
| `AGENT_LLM_PROVIDER` | `openai` (default) or `local` |
| `OPENAI_API_KEY` | Required for `openai` provider |
| `OPENAI_MODEL` | Default `gpt-4o-mini` |
| `OPENAI_BASE_URL` | Optional. Use to point at OpenRouter or another OpenAI-compatible endpoint (`https://openrouter.ai/api/v1`) |
| `LOCAL_LLM_BASE_URL` | Required when `AGENT_LLM_PROVIDER=local` (e.g. `http://127.0.0.1:11434/v1` for Ollama) |
| `LOCAL_LLM_MODEL` | Required when `AGENT_LLM_PROVIDER=local` (e.g. `qwen2.5:7b-instruct`) |
| `LOCAL_LLM_API_KEY` | Optional; only needed if your local server requires auth |

**Local-model note:** tool-calling reliability varies. Recommended starting points: `qwen2.5:7b-instruct`, `llama-3.1-8b-instruct` or larger. Smaller models often mis-call tools.

## Running

```bash
npm run tauri:dev
```

The Tauri shell spawns the Rust API + worker. Open the "AI Interaction" tab and start asking questions. The agent provider + tool registry are built once at API startup, so the first question is no slower than the rest.

## Architecture (brief)

```
React UI (ChatPanel) ──SSE──▶ /api/v1/agent/chat (axum)
                                      │
                                      ▼
                            AgentRunner (multi-turn loop)
                            ├─ LlmProvider (OpenAI or local)
                            └─ ToolRegistry → 9 tools
                                              │
                                              ▼
                                       SQLite (local)
```

Each turn:
1. UI sends `{message, history}`
2. Runtime builds the system prompt with today's date, accounts list, data range, registered tools
3. Loop: ask the LLM → if tool calls, execute them (passing `AgentDeps { db, llm }`), append results, repeat (cap 6 iterations) → if no tool calls, emit final answer
4. SSE events: `started`, `tool_call_start`, `tool_call_result`, `assistant_message` (or `category_confirmation_needed`), `followups`, `done`
5. UI renders streaming tool chips, markdown answer, citation chip → drawer, follow-up chips, and the inline category confirmation card when needed

## Registered tools

| Tool | Purpose |
|---|---|
| `list_accounts_and_cards` | Enumerate accounts/cards |
| `query_transactions` | Filtered list with `merchant_substring` or `merchant_substrings[]` |
| `aggregate_transactions` | group_by × metric (sum/count/avg/min/max) × window |
| `compare_periods` | Two windows, diff + per-group breakdown |
| `find_recurring` | Subscription detector via cadence buckets |
| `transaction_detail` | Single-txn deep dive with similar charges |
| `resolve_category_intent` | LLM classifier for category questions (5c) |
| `confirm_category_assignments` | Persist user's per-merchant decisions (5d) |

`echo` is in the codebase for debugging but **not** registered in the default registry — gpt-4o-mini was calling it gratuitously.

## Category intelligence loop

When the user asks a category question (groceries, dining, transit, …):
1. Agent calls `resolve_category_intent` — lazily populates `merchant_signatures`, loads existing assignments, asks the LLM to classify only unclassified merchants.
2. If anything needs confirmation, agent emits `CATEGORY_CONFIRMATION_NEEDED: <slug>` → runtime turns this into a structured SSE event → UI renders an inline card.
3. User clicks Apply → UI sends a structured follow-up message.
4. Agent calls `confirm_category_assignments` (user_confirmed / user_overridden rows persist forever) → calls `aggregate_transactions` with `merchant_substrings: [<confirmed merchants>]` → final answer.

Next time the user asks about the same category, all merchants are already classified → no LLM classification call, no confirmation card.

## Audit trail (Phase 6)

Every chat run records its full lifecycle to `agent_events` (migration 0013):

- `run_started` — model, provider, user message excerpt
- `llm_call` — full request + response, tokens, latency, **cost in micro-dollars**
- `tool_call` — name, args, result, duration, ok/fail
- `assistant_message` / `followups` / `category_confirmation_needed` — UI events
- `error` / `truncated` (when applicable)
- `run_ended` — totals (status, iterations, tokens, cost)

Writes flow through a buffered tokio background task so the SSE stream never blocks on DB I/O. Audit failures log via `tracing::warn` but never break a run.

### Cost tracking

`agent::pricing` has a hardcoded price table for OpenAI models. `AGENT_PRICING_OVERRIDE` env var lets you add models or override rates:
```
AGENT_PRICING_OVERRIDE="openai:gpt-5=3000000,12000000;openai:custom=100,500"
```
Values are micro-dollars per 1M tokens (so `150_000` means $0.15/M).

### Inspecting the audit

**UI:** click the **Activity** tab. See cost rollups (7d / 30d / all-time), conversation summaries, recent runs, and full event replays.

**API:**
- `GET /api/v1/audit/conversations?limit=50` — per-session rollups (incl. per-session $)
- `GET /api/v1/audit/runs?limit=100` — recent completed runs
- `GET /api/v1/audit/runs/:run_id/events` — full event sequence for one run
- `GET /api/v1/audit/summary?days=7` — window totals; omit `days` for all-time

**SQL examples:**
```sql
-- Total cost in the last 7 days
SELECT printf('$%.4f', SUM(cost_micros) / 1000000.0) FROM agent_events
WHERE event_kind='llm_call' AND occurred_at >= date('now','-7 days');

-- Most expensive runs
SELECT run_id, cost_micros / 1000000.0 AS dollars FROM agent_events
WHERE event_kind='run_ended' ORDER BY cost_micros DESC LIMIT 10;

-- Tool call frequency
SELECT tool_name, COUNT(*) FROM agent_events
WHERE event_kind='tool_call' GROUP BY tool_name ORDER BY COUNT(*) DESC;
```

## Testing

```bash
# Rust workspace
cargo test --workspace                                    # all crates
cargo test -p agent                                       # agent only (~51 tests)
cargo clippy -p agent --all-targets                       # zero warnings

# UI
npm run test:ui-build                                     # vite build

# End-to-end smoke (requires API running)
npm run test:agent
```

## Known limits / deferred

- Custom user-defined categories (v1 ships with 13 seeded categories)
- Subcategories
- Splitting a single merchant across multiple categories (Walmart = groceries + shopping)
- Bulk re-classification UI
- Auto-categorize on import (today: lazy, via chat questions)
- DB-backed chat history across sessions (today: localStorage, capped at last 100 turns)
- Real token streaming from the LLM (today: emit tool chips during loop, full final answer at once)

## Troubleshooting

- **"agent llm provider not configured"** — set `OPENAI_API_KEY` (or `LOCAL_LLM_BASE_URL` + `LOCAL_LLM_MODEL`) and restart `tauri:dev`.
- **429 from OpenAI** — `OPENAI_API_KEY` is out of quota. Add billing or swap in OpenRouter via `OPENAI_BASE_URL`.
- **Tool calls feel slow** — likely the LLM, not your DB. Each tool round-trip is 1–3s with gpt-4o-mini.
- **Wrong merchants in a category answer** — say "Don't count X as groceries" in chat. The agent updates `user_overridden` and never re-suggests.
