# Architecture — where the new layer plugs in

The agent runtime stays the same. We add 3 new tools and 3 new SQLite tables (highlighted in yellow).

```mermaid
flowchart TB
    User(["User"])
    UI["Chat UI<br/>(ChatPanel + ConfirmationCard)"]
    API["Rust API<br/>POST /api/v1/agent/chat (SSE)"]

    subgraph AgentCrate["Agent crate"]
        Runtime["AgentRunner<br/>multi-turn loop"]
        Deps["AgentDeps { db, llm }<br/>passed to every tool"]

        subgraph Tools["Tool Registry"]
            direction LR
            Existing["Existing tools:<br/>list_accounts<br/>query_transactions<br/>aggregate_transactions<br/>compare_periods<br/>find_recurring<br/>transaction_detail"]
            NewTools["NEW:<br/>resolve_category_intent<br/>confirm_category_assignments<br/>list_unique_merchants"]
        end
    end

    LLM[("OpenAI<br/>gpt-4o-mini")]

    subgraph SQLite["SQLite (local)"]
        OldDB["transactions<br/>accounts<br/>statements"]
        Cats["categories<br/>(seeded: groceries, dining,<br/>transit, utilities, etc.)"]
        NewDB["NEW (migration 0012):<br/>merchant_signatures<br/>merchant_category_assignments<br/>category_resolution_history"]
    end

    User --> UI
    UI <-->|SSE stream| API
    API --> Runtime
    Runtime --> Deps
    Deps --> Tools
    Runtime -.->|chat completion| LLM
    Tools --> SQLite

    style NewTools fill:#fef3c7,stroke:#d97706,stroke-width:2px
    style NewDB fill:#fef3c7,stroke:#d97706,stroke-width:2px
```

## What's changing

- **Tool trait refactor**: every tool now receives `AgentDeps { db, llm }` instead of just `&SqlitePool`. This lets new tools call the LLM for classification.
- **3 new tools** that together implement the learning loop.
- **3 new tables** for merchant signatures, current category assignments, and an audit history for future learning.

## What's NOT changing

- The SSE event protocol stays the same shape, with one new event kind: `category_confirmation_needed`.
- Existing tools work exactly as before after the trait refactor.
- No changes to the transactions, accounts, or statements tables.
