# Phase 7 — Flow Fixes Plan

Date: 2026-05-15 (shipped 2026-05-15)
Status: ✅ Shipped (7a–7e)
Estimate: ~6 hours across 5 sub-phases

Address all four issues surfaced by the post-launch audit of the category confirmation flow, and restructure the flow itself so a category question is one continuous run instead of two with a JSON wart in between.

## Triggering audit findings

1. **🔴 Agent passes `merchant_signature_id` UUIDs into `merchant_substrings`** — every category aggregation returned $0 across 5 different months.
2. **🟡 Apply follow-up renders as a wall of raw JSON in chat** — the user sees an internal protocol message.
3. **🟡 LLM classifier over-includes merchants** — Dollarama, pharmacy ended up "approved as groceries".
4. **🟡 Agent doesn't notice "this month has no data"** — said $0 in May confidently with no caveat.

## Decisions locked

| Fix | Approach |
|---|---|
| #1 | Server-side: new `merchant_signature_ids: Vec<String>` arg on `query_transactions`, `aggregate_transactions`, `compare_periods`. Storage helper resolves IDs → normalized_keys → in-memory match against `normalize(t.description)`. |
| #2 | Structural: paused-run continuation. New `RunCoordinator` in `AppState` + `POST /api/v1/agent/runs/:run_id/continue` endpoint. Confirmation card POSTs to this — no chat message. SSE stays open. |
| #3 | Tighten classifier prompt (explicit exclusion rules). Raise pre-check threshold 0.7 → 0.85. |
| #4 | System-prompt rule + `aggregate_transactions` returns `window_has_any_data: bool` so the agent can distinguish empty windows from substring-misses. |

## Sub-phases

| | Sub-phase | Status | Commit |
|---|---|---|---|
| 7a | `merchant_signature_ids` filter on data tools | ✅ | `8e413cc` |
| 7b | Paused-run continuation (RunCoordinator + /continue endpoint) | ✅ | `0fe4189` |
| 7c | Tighter classifier prompt + UI threshold | ✅ | `f1f21f1` |
| 7d | UI Apply hits /continue, no JSON message | ✅ | `63fc90e` |
| 7e | Smoke extension + abandoned-run test | ✅ | (this commit) |

## Build order

`7a`, `7b`, `7c` are independent server-side changes — can land in any order. **`7d` depends on 7a + 7b** (needs both the new tool arg and the new endpoint). `7e` at the end.

## Non-goals (explicit deferrals)

- Denormalising `normalized_merchant_key` onto `transactions` table (faster matching, but schema change; in-memory is sufficient until ~10k txns)
- WebSocket replacement for SSE + POST coordination
- Server-restart recovery for paused runs (rare; status=abandoned suffices)
- Reclassifying already-confirmed grocery merchants (e.g. Dollarama) — user can override manually
- Cross-conversation analytics on the new audit data

## Risks & mitigations

| Risk | Signal | Mitigation |
|---|---|---|
| Coordinator memory leak from never-resumed runs | Lots of `running`-status conversations in audit | 5-min idle timeout writes `run_ended` with status=abandoned; coordinator entry dropped |
| Tab close during pause | SSE channel `events.is_closed()` fires | Existing cancellation path picks this up + coordinator cleanup |
| LLM still copies UUIDs to `merchant_substrings` | Aggregate returns 0 unexpectedly | New arg lives next to where the agent already has IDs naturally; tool docstring + prompt updated |
| Double-apply | Oneshot already consumed | Endpoint returns 409 Conflict with the original result idempotently |
| In-memory normalisation slow on large windows | Aggregate latency spikes | Cap window at 365 days; benchmark at 5k txns before considering schema change |

## Validation gates

- [ ] `cargo test --workspace` green (target ≥70 agent tests)
- [ ] `cargo clippy -p agent -p api --all-targets` clean
- [ ] `npm run test:ui-build` clean
- [ ] Manual E2E: "groceries last month" → card → Apply → real $ figure → **one run** in audit with `category_confirmation_needed` mid-sequence
- [ ] Manual edge: "groceries in May" (outside data range) → agent says so explicitly
- [ ] Manual edge: tab close during pause → run abandoned within 5 min
- [ ] Re-ask same category question → no card, instant answer
- [ ] SQL ground-truth check: aggregate answer matches `SELECT SUM(amount_cents) … WHERE booked_at … AND merchant matches` within ±1¢

## Done definition

Every category aggregate returns the correct figure. No JSON in chat. One run per question. Empty windows answered honestly. Coordinator cleans up on disconnect/timeout.
