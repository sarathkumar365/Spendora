# Flow — First time asking about a category (learning happens)

When the user asks about "groceries" for the first time, the system has nothing learned. It asks the LLM to suggest, then asks the user to confirm. The user's confirmation is persisted.

```mermaid
sequenceDiagram
    autonumber
    actor U as User
    participant UI as Chat UI
    participant A as Agent (LLM)
    participant T as Tools
    participant DB as SQLite
    participant L as OpenAI

    U->>UI: "How much on groceries last month?"
    UI->>A: chat request

    Note over A: Parses intent:<br/>category=groceries<br/>window=2026-04-01..30

    A->>T: resolve_category_intent("groceries", April)
    T->>DB: lazy-populate merchant_signatures from April txns
    T->>DB: load confirmed mappings for category=groceries
    Note over DB: First time — nothing confirmed yet
    T->>L: "Which of these merchants are groceries?<br/>[Loblaws, Tim Hortons, Walmart, Uber, Metro,<br/>Costco, Netflix, ...]"
    L-->>T: {Loblaws: 0.95, Metro: 0.95, Walmart: 0.65,<br/>Costco: 0.50, others: <0.2}
    T->>DB: write llm_suggested rows<br/>(not yet user_confirmed)
    T-->>A: {confirmed:[], suggested:[Loblaws, Metro,<br/>Walmart, Costco], unknown:[...]}

    A->>UI: SSE event: category_confirmation_needed

    UI->>U: Shows card:<br/>Loblaws (4 txns, $310) checked<br/>Metro (2 txns, $84) checked<br/>Walmart (3 txns, $187) toggleable<br/>Costco (1 txn, $142) toggleable

    U->>UI: Toggles Walmart ON, leaves Costco OFF<br/>Clicks "Apply"

    UI->>A: structured follow-up:<br/>"Include: Loblaws, Metro, Walmart.<br/>Exclude: Costco."

    A->>T: confirm_category_assignments(...)
    T->>DB: UPDATE rows to user_confirmed<br/>(Costco → user_overridden, included=false)
    T->>DB: append to category_resolution_history

    A->>T: aggregate_transactions(<br/>merchant_substrings=[loblaws, metro, walmart],<br/>direction=debit, April, metric=sum)
    T->>DB: SQL aggregation
    T-->>A: $581.00, 9 transactions

    A-->>UI: "You spent **$581.00** on groceries in April<br/>across Loblaws, Metro, and Walmart (9 txns)."<br/>+ citation chip + followups
    UI-->>U: Renders answer
```

## Key moments

| Step | What's happening |
|---|---|
| 5 | Only unclassified merchants are sent to the LLM — we don't re-ask about merchants already in the assignments table. |
| 6 | LLM returns confidence scores. We don't show low-confidence ones (< 0.2) to the user. |
| 7 | Even before the user confirms, we store the LLM's guesses with `source='llm_suggested'`. Lets us mine these later or show suggestions faster next time. |
| 9 | The confirmation card is rendered **inside** the assistant message bubble — not a modal — so it feels like part of the conversation. |
| 12 | User overrides (Costco → not groceries) are first-class. Stored as `user_overridden`, never re-suggested. |
| 13 | The audit log captures every confirmation/override so we can train better classifiers later. |
| 14 | The actual aggregation happens with the user's final list. This is what produces the citation chips. |
