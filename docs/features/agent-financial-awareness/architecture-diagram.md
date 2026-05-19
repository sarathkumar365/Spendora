# Architecture — Agent Financial Awareness

State as of Phase 7 (shipped). This is the current shape of the system after Phases 1–7.

## System topology

```mermaid
flowchart TB
    User(["User"])

    subgraph UI["React UI (apps/expense-desktop-ui)"]
        ChatPanel["ChatPanel<br/>chat transcript, SSE client,<br/>citation drawer, follow-ups"]
        ConfirmationCard["CategoryConfirmationCard<br/>(inline in assistant bubble)"]
        DetailsExpander["TurnDetailsExpander<br/>(per-turn cost + tokens)"]
        ActivityPanel["ActivityPanel<br/>(Conversations · Runs · Drawer)"]
    end

    subgraph API["Rust API (axum, services/expense-rs/crates/api)"]
        ChatEP["POST /api/v1/agent/chat<br/>(SSE stream)"]
        ContEP["POST /api/v1/agent/runs/<br/>:run_id/continue<br/>★ Phase 7b"]
        CtxEP["GET /api/v1/agent/context"]
        AuditEPs["GET /api/v1/audit/*<br/>conversations · runs · events · summary"]
    end

    subgraph AgentCrate["Agent crate (crates/agent)"]
        Runtime["AgentRunner<br/>multi-turn loop + cancellation"]
        Coordinator["RunCoordinator<br/>parks runs awaiting confirmation<br/>★ Phase 7b"]
        AuditSink["AuditSink trait<br/>DbAuditSink (background writer)<br/>★ Phase 6a"]
        Pricing["pricing.rs<br/>token → micro-dollars<br/>★ Phase 6b"]
        Deps["AgentDeps { db, llm }<br/>passed to every tool"]

        subgraph Tools["Tool Registry (9 tools)"]
            direction LR
            ReadTools["Read tools:<br/>list_accounts_and_cards<br/>query_transactions ★ +merchant_signature_ids<br/>aggregate_transactions ★ +merchant_signature_ids<br/>compare_periods ★ +merchant_signature_ids<br/>find_recurring<br/>transaction_detail"]
            CatTools["Category tools (Phase 5):<br/>resolve_category_intent<br/>(LLM-classifier, Phase 7c tighter prompt)<br/>confirm_category_assignments"]
        end
    end

    LLM[("OpenAI / OpenAI-compatible<br/>via LlmProvider trait")]

    subgraph SQLite["SQLite — local-only (storage_sqlite)"]
        CoreData["transactions<br/>accounts<br/>statements"]
        Categories["categories<br/>(13 seeded, Phase 5a)"]
        CatTables["merchant_signatures<br/>merchant_category_assignments<br/>category_resolution_history<br/>★ migration 0012 (Phase 5a)"]
        AuditTable["agent_events<br/>(single table, all event kinds)<br/>★ migration 0013 (Phase 6a)"]
    end

    User --> UI
    ChatPanel <-->|SSE| ChatEP
    ConfirmationCard -.->|Apply| ContEP
    DetailsExpander -.->|reads stats from Done event| ChatPanel
    ActivityPanel <-->|polls| AuditEPs

    ChatEP --> Runtime
    ContEP --> Coordinator
    Coordinator <-.->|park/resume| Runtime
    CtxEP --> Deps
    AuditEPs --> AuditTable

    Runtime --> Deps
    Runtime --> AuditSink
    Runtime -->|usage → cost| Pricing
    Deps --> Tools
    Runtime <-.->|chat completion + usage| LLM
    Tools --> CoreData
    Tools --> Categories
    Tools --> CatTables
    AuditSink -.->|background writer| AuditTable
    Pricing -.-> AuditSink

    style ContEP fill:#fef3c7,stroke:#d97706,stroke-width:2px
    style Coordinator fill:#fef3c7,stroke:#d97706,stroke-width:2px
    style AuditSink fill:#fef3c7,stroke:#d97706,stroke-width:2px
    style Pricing fill:#fef3c7,stroke:#d97706,stroke-width:2px
    style AuditTable fill:#fef3c7,stroke:#d97706,stroke-width:2px
    style CatTables fill:#fef3c7,stroke:#d97706,stroke-width:2px
    style ActivityPanel fill:#fef3c7,stroke:#d97706,stroke-width:2px
    style DetailsExpander fill:#fef3c7,stroke:#d97706,stroke-width:2px
```

★ = added or changed in Phases 5–7.

---

## End-to-end flow — "How much on groceries last month?"

This is the canonical category question. Shows how Phases 5, 6, and 7 collaborate in one run.

```mermaid
sequenceDiagram
    autonumber
    actor U as User
    participant UI as ChatPanel
    participant API as /agent/chat (SSE)
    participant R as AgentRunner
    participant Coord as RunCoordinator
    participant Audit as DbAuditSink
    participant L as OpenAI
    participant DB as SQLite

    U->>UI: "groceries last month?"
    UI->>API: POST + open SSE
    API->>R: spawn run (run_id=R1)
    R->>Audit: record run_started
    R->>L: chat completion #1
    L-->>R: tool_call: resolve_category_intent
    R->>DB: read merchant_signatures + assignments
    R->>L: classifier sub-call (per-category prompt)
    L-->>R: confidence map
    R->>DB: persist llm_suggested rows
    R->>Audit: tool_call + llm_call events
    R->>L: chat completion #2
    L-->>R: "CATEGORY_CONFIRMATION_NEEDED: groceries"
    R->>UI: SSE event: category_confirmation_needed (run_id=R1)
    R->>Audit: category_confirmation_needed
    R->>Coord: park("R1") — awaits oneshot
    Note over R,Coord: ★ same run still alive — SSE stream stays open

    UI->>U: render inline card<br/>(confirmed / suggested / excluded)
    U->>UI: tick boxes, click Apply

    UI->>API: POST /agent/runs/R1/continue<br/>{ category_slug, assignments }
    API->>Coord: resume("R1", continuation)
    Coord-->>R: oneshot delivers
    R->>Audit: category_confirmation_received
    R->>L: chat completion #3 (with user message in history)
    L-->>R: tool_call: confirm_category_assignments
    R->>DB: write user_confirmed / user_overridden + history
    L-->>R: tool_call: aggregate_transactions<br/>(merchant_signature_ids=[ids])
    R->>DB: resolve IDs → keys → SQL fetch → filter in Rust
    R->>L: chat completion #4
    L-->>R: "You spent $581 on groceries — Loblaws, Metro…"
    R->>UI: SSE: tool_call_*, assistant_message, followups, done
    R->>Audit: llm_call + assistant_message + run_ended (status=done, totals)
    UI->>U: final answer + citations + Details expander
```

Key things this diagram makes visible:

- **One run.** `run_id=R1` is used start-to-finish. The Activity tab shows it as a single entry with one cost rollup.
- **`merchant_signature_ids`, not substrings.** The `aggregate_transactions` call (step 26) sends UUIDs that the server resolves to canonical merchant keys. No more `%uuid%` LIKE failures.
- **Audit runs in parallel.** Every event hits `DbAuditSink` → background tokio task → `agent_events` table. The hot path never blocks.
- **The Coordinator owns the pause.** Without `coordinator: Some(...)` on the runner the sentinel still works the old way (used by tests).

---

## Failure modes the architecture handles

| Scenario | Handling |
|---|---|
| User closes tab during pause | SSE channel close → runtime detects via `events.closed()` → `coord.cancel()` → `run_ended` (status=cancelled) |
| User never clicks Apply | 5-min timeout in the runtime's `tokio::select!` → `coord.cancel()` → Truncated event + `run_ended` (status=abandoned) |
| Double-click Apply | First POST consumes the oneshot → second POST gets 409 Conflict; UI surfaces error inside card |
| Audit DB write fails | `DbAuditSink` logs `tracing::warn`, drops the event. Run continues. |
| LLM provider misconfigured | `/agent/chat` returns 503 with actionable message at handler level |
| LLM returns malformed JSON in classifier | `parse_classifier_json` extracts the JSON block from prose/fences; on failure tool returns an error the agent reads as a Tool message and can retry |
| Empty data window | Tools return `window_has_any_data: false`; system prompt requires the agent to say so explicitly (Phase 7c) |

---

## What ships per phase

| Phase | What appeared in this diagram |
|---|---|
| 1–4 | Runtime, LlmProvider, ReadTools, basic UI |
| 5a | `merchant_signatures` + `merchant_category_assignments` + `category_resolution_history` |
| 5b | `AgentDeps` (the box that lets tools call the LLM) |
| 5c | `resolve_category_intent` tool with classifier sub-call |
| 5d | `confirm_category_assignments` + `merchant_substrings` array |
| 5e | `CategoryConfirmationCard` in UI |
| 6a | `AuditSink` trait + `DbAuditSink` background writer + `agent_events` table |
| 6b | `pricing.rs` for token → cost |
| 6c | Runtime instrumentation: every event recorded |
| 6d | `/audit/*` endpoints |
| 6e | `TurnDetailsExpander` per-turn UI |
| 6f | `ActivityPanel` cross-conversation UI |
| 7a | `merchant_signature_ids` filter on `query` / `aggregate` / `compare` |
| 7b | `RunCoordinator` + `POST /agent/runs/:id/continue` + runtime park/resume |
| 7c | Tighter classifier prompt + per-category exclusion rules |
| 7d | UI Apply hits `/continue` (no JSON message in chat) |
| 7e | Abandoned-run test + smoke for `/continue` endpoint |

---

## What is NOT in this diagram (deferred)

- Custom user-defined categories
- Subcategories
- Splitting a single merchant across multiple categories
- Auto-categorisation on import (today: lazy via chat)
- Persistent chat history in SQLite (today: localStorage, capped at 100 turns)
- Real LLM-token streaming (today: full responses emitted at once; tool chips show progress mid-loop)
- WebSocket transport (SSE + separate POST coordination is sufficient at our scale)
