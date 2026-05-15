# Flow — After the category is learned (silent path)

The second time the user asks about "groceries", no LLM classification is needed and no confirmation is shown. The agent goes straight to the answer.

```mermaid
sequenceDiagram
    autonumber
    actor U as User
    participant UI as Chat UI
    participant A as Agent (LLM)
    participant T as Tools
    participant DB as SQLite

    U->>UI: "How much on groceries this month?"
    UI->>A: chat request

    A->>T: resolve_category_intent("groceries", May)
    T->>DB: load confirmed mappings for category=groceries
    DB-->>T: [Loblaws, Metro, Walmart] (user_confirmed)
    T->>DB: load merchants seen in May
    Note over T: All May merchants already classified.<br/>No unknowns → no LLM call needed.
    T-->>A: {confirmed:[Loblaws, Metro, Walmart],<br/>suggested:[], unknown:[]}

    Note over A: No confirmation card needed.<br/>Skip straight to aggregation.

    A->>T: aggregate_transactions(<br/>merchant_substrings=[loblaws, metro, walmart],<br/>May, debit, sum)
    T-->>A: $412.50, 7 transactions

    A-->>UI: "You spent **$412.50** on groceries in May."
    UI-->>U: Instant answer (1 tool round-trip)
```

## What makes this fast

- **No LLM classification call** — every merchant in May was already classified from prior interactions.
- **No user confirmation card** — `suggested` is empty, so the agent skips it.
- **Just 1 aggregate query** — same as a non-category question.

## What happens if a NEW merchant appears in May

Example: user used a new grocery store in May called "Farm Boy".

```mermaid
sequenceDiagram
    participant T as Tools
    participant DB as SQLite
    participant L as OpenAI

    T->>DB: load confirmed mappings for category=groceries
    DB-->>T: [Loblaws, Metro, Walmart]
    T->>DB: load May merchants
    DB-->>T: [Loblaws, Metro, Walmart, **Farm Boy**, ...]
    Note over T: Farm Boy has no assignment yet → unknown
    T->>L: "Is Farm Boy groceries? (and other unknowns)"
    L-->>T: {Farm Boy: 0.92}
    T->>DB: write llm_suggested for Farm Boy
    Note over T: Returns suggested=[Farm Boy] → triggers confirmation card
```

So you only get re-prompted for *truly new* merchants. The system stabilizes quickly.
