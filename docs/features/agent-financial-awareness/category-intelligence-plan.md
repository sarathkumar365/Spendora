# Category Intelligence — Plan

Date: 2026-05-15
Status: Awaiting approval
Estimated build: ~5–6 hours across 5 sub-phases

A persistent, self-learning merchant-to-category mapping. The LLM is the kernel today; future improvements (heuristics, embeddings, user-trained classifier) can replace it without breaking the contract.

See the diagrams in this folder:
- [architecture-diagram.md](architecture-diagram.md) — where the new layer sits in the system
- [flow-first-time.md](flow-first-time.md) — user flow the **first** time a category is asked
- [flow-learned.md](flow-learned.md) — user flow **after** the category is learned
- [data-model.md](data-model.md) — new tables + ER diagram
- [learning-over-time.md](learning-over-time.md) — how it compounds

## What it solves

Today's gap: ask "how much on groceries last month" and the agent has no idea which merchant strings in your DB (`LOBLAWS GREAT FOOD`, `METRO #455`, `WALMART SUPERCENTRE`) belong to the groceries bucket.

The fix: a learning layer that
1. Asks the LLM the first time
2. Asks the user to confirm/correct
3. Persists the answer
4. Never asks again for that merchant + category pair

## New tools

| Tool | What |
|---|---|
| `resolve_category_intent(category, date_from?, date_to?)` | Returns `{ confirmed, suggested, unknown }`. Populates `merchant_signatures` lazily. Calls the LLM only for merchants without a current assignment. |
| `confirm_category_assignments(assignments)` | Persists user choices as `user_confirmed` or `user_overridden`. Appends to history. |
| `list_unique_merchants(date_from?, date_to?, limit=200)` | Helper. Returns deduped merchants in a window with txn counts + totals. |

## Architecture change

The current `Tool` trait takes `(&SqlitePool, args)`. The classifier needs LLM access. Refactor to:

```rust
pub struct AgentDeps<'a> {
    pub db: &'a SqlitePool,
    pub llm: &'a dyn LlmProvider,
}

#[async_trait]
pub trait Tool: Send + Sync {
    async fn invoke(&self, deps: AgentDeps<'_>, args: Value) -> Result<ToolOutput>;
    // ...
}
```

One-time change. Touches every existing tool. Lands atomically with tests.

## Build order

| Sub-phase | What | Time |
|---|---|---|
| 5a | Migration `0012`: `merchant_signatures`, `merchant_category_assignments`, `category_resolution_history`. Seed default categories. | 45 min |
| 5b | Refactor `Tool` trait → `AgentDeps`. Update all existing tools. | 30 min |
| 5c | `resolve_category_intent` tool with LLM classifier + persistence. | 90 min |
| 5d | `confirm_category_assignments` + extend `query` / `aggregate` with `merchant_substrings` (array OR). | 60 min |
| 5e | UI confirmation card + new SSE event handling + tests. | 90 min |

## Seeded categories (v1, hardcoded)

`groceries`, `dining`, `transit`, `utilities`, `entertainment`, `shopping`, `income`, `transfers`, `fees`, `subscriptions`, `healthcare`, `travel`, `other`.

Custom user-defined categories deferred to a later feature.

## Risk flags

1. **LLM classification cost** — ~$0.005 per category question on 200-merchant batch with gpt-4o-mini. Acceptable. Avoided entirely once confirmed.
2. **Cold-start friction** — first time you ask any category, you'll see a confirmation card. Once confirmed, silent forever.
3. **Tool trait refactor** — touches every tool. Must land cleanly. Tests cover all six existing tools after the refactor.
4. **Taxonomy lock-in** — fixed seed list means custom categories don't exist in v1.

## Deferred

- Manual category-management UI (chat-driven is enough for now)
- Subcategories
- Custom user categories
- Bulk re-classification
- Embedding-based classifier (LLM is fine until you have ≥10k merchants)
- Phase 5 polish (token-budget, README, smoke script) — after this lands
