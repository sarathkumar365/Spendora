# Learning over time — how the system gets smarter

Each interaction reduces the number of merchants the LLM has to classify next time. After a couple weeks of use, the LLM is rarely called.

```mermaid
flowchart LR
    subgraph Day1["Day 1<br/>'groceries last month?'"]
        D1["LLM classifies 200 merchants.<br/>User confirms 4, rejects 1.<br/>5 merchants now learned."]
    end

    subgraph Day2["Day 2<br/>'how much on dining?'"]
        D2["LLM classifies 196 unclassified.<br/>User confirms 8, rejects 2.<br/>15 merchants learned total."]
    end

    subgraph Day7["Day 7<br/>'transit this week?'"]
        D7["Only 3 new merchants since Day 2.<br/>LLM classifies just those 3.<br/>User confirms 2."]
    end

    subgraph Day30["Day 30+"]
        D30["Nearly all merchants categorized.<br/>New imports auto-classified.<br/>LLM almost never called for categories."]
    end

    Day1 --> Day2 --> Day7 --> Day30

    style Day1 fill:#fee2e2,stroke:#dc2626
    style Day2 fill:#fed7aa,stroke:#ea580c
    style Day7 fill:#fef3c7,stroke:#d97706
    style Day30 fill:#d1fae5,stroke:#16a34a
```

## What this means economically

- Day 1: ~$0.005 per category question (one LLM classification call)
- Day 30+: ~$0.000 per category question (no classification needed)

The cost asymptotes to zero as the user's merchant pool stabilizes.

## What this means for accuracy

- Day 1: accuracy depends on the LLM's world knowledge and the user spotting misses
- Day 30+: accuracy is whatever the user confirmed it to be. The system can't drift.

## Future improvements unlocked by `category_resolution_history`

```mermaid
flowchart TD
    History[("category_resolution_history<br/>audit log of every<br/>confirm/override")]

    Now["Today:<br/>The agent uses the<br/>LLM classifier."]

    Later1["Improvement 1:<br/>Heuristic priors<br/>('always Costco for this user<br/>→ groceries, skip LLM')"]

    Later2["Improvement 2:<br/>Per-user embedding model<br/>trained on user's own<br/>confirmation patterns"]

    Later3["Improvement 3:<br/>Federated insights<br/>('most users mark Costco as<br/>groceries — confidence boost')"]

    History --> Now
    History --> Later1
    History --> Later2
    History --> Later3

    style Now fill:#d1fae5
    style Later1 fill:#fef3c7
    style Later2 fill:#fef3c7
    style Later3 fill:#fef3c7
```

All three improvements drop in **without changing the tool API** (`resolve_category_intent` keeps the same input/output). They just change *how* the suggestions are computed. That's the value of the trait + table abstraction.
