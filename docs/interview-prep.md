# Spendora — Interview Preparation Guide

> A complete, project-specific cheat sheet for backend / AI / full-stack / LLM-integration interviews.
> All answers are grounded in the actual Spendora codebase.

---

## 0. The 60-second Pitch ("Tell me about this project")

> "Spendora is a **local-first desktop expense tracker** I built to learn the intersection of systems engineering and LLM application design. It's a Tauri desktop app: a **React + TypeScript UI** rendered inside a native window, talking to **two Rust services** — an HTTP API on port 8081 and a background worker on 8082 — that the Tauri shell spawns and supervises as child processes. Both services share a local **SQLite database** managed through versioned migrations.
>
> The interesting part is the **AI layer**. Users drop in PDF or CSV bank statements; the worker parses them with a hybrid pipeline — deterministic parsers for known formats, and an **LLM-powered extractor** (OpenAI-compatible, swappable with a local model) for messy statements. On top of that, there's an **agent** the user can chat with: it has nine tools — query, aggregate, compare, recurring detection, category resolution, etc. — wired through OpenAI-style function calling. Every agent turn is persisted in an **audit table** with token usage and cost, so the user can replay any conversation in the Activity tab.
>
> What I'm proudest of: the agent **pauses mid-run** when it needs a human decision (e.g., 'confirm this category mapping'), using a `tokio::oneshot` channel keyed by `run_id` — so from the LLM's perspective, the entire human-in-the-loop flow is **one continuous run**, not two separate turns. Total stack: React + Vite + Tauri + Axum + sqlx + SQLite + OpenAI-compatible LLMs."

Length: ~210 words, ~70 seconds spoken. Memorize the structure (stack → AI layer → one unique thing).

---

## 1. Architecture Summary

### 1.1 Process model
```
┌─────────────────────────────────────────┐
│ Tauri desktop window (single binary)    │
│  ├─ React UI (Vite)                     │
│  └─ Rust shell (process supervisor)     │
│        spawn ────▶ expense-api  :8081   │  (Axum HTTP)
│        spawn ────▶ expense-worker :8082 │  (polling job runner)
│                       │                 │
│        both ─▶ SQLite expense.db        │
└─────────────────────────────────────────┘
                  │ HTTPS
                  ▼
    OpenAI / local LLM     Plaid (deferred)
```

### 1.2 Layers
| Layer | Tech | Crate / Folder |
|---|---|---|
| UI | React 18 + TS + Vite | `apps/expense-desktop-ui` |
| Shell / IPC | Tauri (Rust) | `apps/expense-desktop-tauri` |
| HTTP API | Axum 0.7, tokio | `services/expense-rs/crates/api` |
| Worker | tokio polling loop | `crates/worker` |
| Persistence | sqlx 0.8 + SQLite | `crates/storage_sqlite` |
| Domain types | pure Rust | `crates/core` |
| LLM agent | OpenAI-compat function-calling | `crates/agent` |
| Extractors | CSV/PDF + AI | `crates/connectors_manual`, `connectors_ai` |
| External | Plaid (deferred) | `crates/connectors_plaid` |

### 1.3 Key design decisions
1. **Local-first**: privacy + offline; no Spendora-owned backend.
2. **Two-process Rust split**: API stays responsive while the worker does CPU/LLM-heavy work.
3. **Polling-based queue**: zero infra dependencies (no Redis/Kafka); trade-off is up-to-5s latency.
4. **Pluggable LLM provider**: OpenAI ↔ local (Ollama / vLLM) via a single `LlmProvider` trait.
5. **Function-calling agent with audit-on-every-turn**: deterministic tool routing instead of free-form code execution.
6. **Schema-versioned SQLite**: migrations `0001`…`0013` baked into the binary; auto-applied on startup.

---

## 2. Request Flows

### 2.1 UI → API typical fetch
1. React component calls `apiFetchJson<T>('/api/v1/transactions')`.
2. Helper resolves base URL → `http://127.0.0.1:8081`.
3. Axum router matches route → handler in `transactions.rs`.
4. Handler pulls from `Arc<AppState>` (sqlx pool + LLM provider + run coordinator).
5. sqlx executes prepared statement against SQLite.
6. JSON serialized via serde → response.

### 2.2 Statement import (the most interesting flow)
```
UI                 API (8081)             Worker (8082)        DB
 │ base64 PDF       │                       │                   │
 ├─ POST /imports ─▶│                       │                   │
 │                  ├── INSERT import,      │                   │
 │                  │    enqueue job_run ──────────────────────▶│
 │ {import_id}      │                       │                   │
 │◀─────────────────┤                       │                   │
 │                  │            ◀── poll every 5s ─────────────┤
 │                  │                       │ claim_pending_job │
 │                  │                       │ (atomic UPDATE)   │
 │                  │                       │                   │
 │                  │                       │ parse PDF/CSV     │
 │                  │                       │  or call LLM      │
 │                  │                       │ extractor         │
 │                  │                       │ INSERT import_rows│
 │ GET /imports/:id │                       │                   │
 │   /status (poll) │                       │                   │
 │                  │ → "pending_card_      │                   │
 │                  │    resolution"        │                   │
 │ GET card-        │                       │                   │
 │   resolution    ─▶ candidates from       │                   │
 │                  │  canonicalized match  │                   │
 │ POST resolve ───▶│                       │                   │
 │ GET review ─────▶│ direction overrides   │                   │
 │ POST commit ────▶│ insert into           │                   │
 │                  │ transactions          │                   │
```

### 2.3 Agent chat (function-calling loop with human-in-the-loop pause)
```
POST /api/v1/agent/chat { conversation_id, user_msg }
  │
  ▼
build messages = [system, ...history, user_msg]
  │
  ▼
┌──── loop ────────────────────────────────────┐
│ LLM.complete(messages, tools=[9 functions])  │
│   ↓                                          │
│ if finish_reason == "stop": return content   │
│ if tool_calls exist:                         │
│    for each tc:                              │
│      execute tool (query/aggregate/...)      │
│      append Tool message                     │
│    continue loop                             │
│ if tool result == CATEGORY_CONFIRMATION:     │
│    park run: RunCoordinator.register(run_id) │
│    return 202 { needs_confirmation, run_id } │
└──────────────────────────────────────────────┘
  │
  ▼ (later)
POST /api/v1/agent/runs/:run_id/continue
  → oneshot::Sender.send(continuation)
  → loop resumes from inside the SAME run
  │
  ▼
audit_sink writes every turn (input/output/tokens/cost)
```

---

## 3. Per-Component Deep Dive

For each component: **what / why / alternatives / trade-offs / scale / security / perf**.

### 3.1 Tauri shell
- **What**: Native window + Rust process supervisor exposing `start_services`, `stop_services`, `startup_status` IPC commands.
- **Why**: Need OS-level capability to spawn child processes and access filesystem — browser sandbox can't do that.
- **Alternatives**: Electron (heavier, Chromium + Node), native Swift/Kotlin (no cross-platform), pure CLI (no UX).
- **Trade-offs**: Tauri bundle is small (~10 MB vs Electron's ~100 MB) but ecosystem is younger; Rust IPC adds a learning curve.
- **Scale**: Single-machine by design; not a "scale" concern in the cloud sense.
- **Security**: All IPC is allow-listed in `tauri.conf.json`. Risk: `lsof | kill` of stale ports could in theory kill an unrelated process listening on 8081 — could mitigate by checking PID's command line.
- **Perf**: 800ms health-poll on startup; 90s total timeout. Spawning `cargo run` in dev is slow (~5s cold); release binaries are sub-second.

### 3.2 React + Vite UI
- **What**: Single large `main.tsx` (~1500 lines) with `ChatPanel` and `ActivityPanel` components. State via hooks; no Redux.
- **Why**: Single-developer project; hooks are sufficient for the state size.
- **Alternatives**: Zustand / Redux Toolkit / Tanstack Query for cache + dedup.
- **Trade-offs**: Hooks-only is fast to build but the 1500-line file is the main code-smell; refactor into feature folders + Tanstack Query would help.
- **Perf**: No memoization → polling causes re-renders. A 1.4k-line component re-rendering every 800ms during startup is wasteful.
- **Security**: No XSS surface to worry about (no untrusted HTML), but file content is base64 → JSON → API; should cap upload size client-side.

### 3.3 Axum API
- **What**: Stateful Axum router with 18+ routes covering imports, statements, transactions, accounts, agent chat, audit, settings.
- **Why Axum**: Tower middleware ecosystem, tokio-native, zero-cost typed extractors.
- **Alternatives**: Actix-web (faster on micro-benchmarks but heavier types), Rocket (synchronous flavor), warp (filter combinators, harder to read).
- **Trade-offs**: Axum routing is great, but compile times are notorious — full rebuild ~30s on this project.
- **Scale**: Binds to `127.0.0.1` — no horizontal scale. To go multi-user you'd add auth middleware, bind 0.0.0.0, and switch SQLite → Postgres.
- **Security gaps**: No auth, no rate limiting, no request-size cap. Acceptable on loopback; would be CVE-bait if exposed.
- **Perf**: sqlx prepared statements + WAL-mode SQLite → ~50k QPS theoretical. Bottleneck is the LLM, not the DB.

### 3.4 Worker
- **What**: Polling loop (default 5s) that `SELECT … FOR UPDATE`-equivalent claims a `job_runs` row, runs the extraction, and updates status.
- **Why polling**: Zero infra dependencies. Idempotent retries fall out for free (just leave status='pending').
- **Alternatives**:
  - LISTEN/NOTIFY (Postgres only) — sub-second latency.
  - SQLite + `inotify`/file watcher trigger from API.
  - Redis Streams / RabbitMQ / NATS — overkill here.
- **Trade-offs**: Polling wastes wakeups when idle; adds up to 5s of latency to every job. A simple fix: API can `tokio::process::Command` "kick" the worker via an HTTP `POST /trigger` after enqueue, falling back to poll.
- **Scale**: One worker, serial processing. Concurrency would need either (a) multiple workers + row-level locking, or (b) a tokio job pool inside one worker.
- **Security**: Health endpoint only; no auth.

### 3.5 SQLite + sqlx
- **What**: Single file at `~/Library/Application Support/SpendoraDesktop/expense.db`. sqlx pool, prepared statements, compile-time query checking via `query!` macro.
- **Why SQLite**: Local-first, zero-ops, transactional, fast for single-user.
- **Alternatives**: Postgres (multi-user), DuckDB (analytics), Sled (Rust-native KV).
- **Trade-offs**: SQLite single-writer constraint — concurrent writes serialize. With one user this is invisible; with N workers it would matter.
- **Scale**: SQLite handles ~10k writes/sec in WAL mode. Read scaling is trivial. To scale users, swap driver to Postgres.
- **Security**: **Data at rest is unencrypted.** Mitigations: SQLCipher (encrypted SQLite) or OS-level FileVault/BitLocker. Currently relies on the latter.
- **Migrations**: 13 forward-only migrations, auto-applied on boot via `--migrate`. **No rollback** — a known weakness.

### 3.6 Agent (the LLM layer)
- **What**: A coordinator loop in `crates/agent/src/runtime.rs` that takes user messages, calls an `LlmProvider` (OpenAI-compatible), parses tool_calls, dispatches to one of 9 tools, appends results, loops until the model emits a final text.
- **Why function-calling over ReAct / code interpreter**: Deterministic, auditable, no sandboxed execution risk, structured arguments.
- **Provider abstraction**:
  ```rust
  trait LlmProvider {
      async fn complete(&self, msgs: &[ChatMessage], tools: &[ToolDef])
          -> Result<LlmResponse>;
  }
  ```
  Two impls: `OpenAiProvider` (gpt-4o-mini default), `LocalOpenAiCompatibleProvider` (Ollama/vLLM, longer timeout).
- **Tools** (`crates/agent/src/tools/`):
  | Tool | Purpose |
  |---|---|
  | `query_tool` | SQL-backed filter/search over transactions |
  | `aggregate_tool` | sum/avg/count grouped by category/account/period |
  | `compare_tool` | period-over-period diffs |
  | `detail_tool` | hydrate a single transaction |
  | `recurring_tool` | merchant_signature-based subscription detection |
  | `accounts_tool` | list/lookup accounts and balances |
  | `resolve_category_tool` | propose category for unclassified txns |
  | `confirm_category_tool` | **parks run for human approval** |
  | `echo_tool` | debug |
- **Human-in-the-loop pause**: `RunCoordinator` is a `Mutex<HashMap<RunId, oneshot::Sender<Continuation>>>`. `confirm_category_tool` registers a receiver and `.await`s it; the API endpoint resumes via the sender. The LLM never sees this — to it, the tool simply "took a long time."
- **Audit trail**: `DbAuditSink` runs as a tokio task receiving `AuditEvent`s and inserting into `agent_audit` table. Stores prompt, completion, tool args, tool result, prompt/completion tokens, computed USD cost (`pricing.rs`).
- **Hallucination mitigation**:
  - Tools, not free text, do the data work — model can't fabricate a transaction.
  - Tool results are appended verbatim into context.
  - Category confirmation requires explicit human ack.
- **Token / cost handling**: per-turn token usage logged, dollar cost computed from a model→price map in `pricing.rs`. No hard budget cap yet — a gap.
- **No RAG / no embeddings / no vector DB**: deliberate — the dataset is bounded (one user's transactions, ~10k rows max) and structured. SQL retrieval is more accurate than vector search for this domain. *Be ready to defend this in an interview; see Q&A below.*

### 3.7 Connectors
- **connectors_manual**: deterministic CSV (header detection) + PDF (text extraction) parsers per bank format.
- **connectors_ai**: LLM-based extractor that handles unknown formats. Triggered when `EXTRACTION_MANAGED_FLOW_MODE=new`.
- **connectors_plaid**: stubbed routes; access tokens stored in `connections` table; OAuth-style link flow not yet implemented.

---

## 4. Database Schema Walkthrough

Tables (from migrations 0001–0013):
- `app_user` (single row, single-user)
- `connections` (provider link metadata: plaid/manual)
- `accounts` (FK→connections; mask, currency)
- `transactions_raw` (immutable source payload — audit trail)
- `transactions` (canonical ledger; UNIQUE(account_id, external_txn_id))
- `categories`
- `classification_results` (1-many with transactions; source = rule|agent, confidence, rationale)
- `rules` (user-defined pattern → category)
- `imports` (file_name, status, source_type)
- `import_rows` (parsed rows pre-commit; normalized_json + parse_error)
- `job_runs` (job_type, status, attempts, next_run_at, idempotency_key)
- `statements` (statement v2: period coverage, opening/closing balance)
- `merchant_signatures` (canonicalized merchant names for recurring detection)
- `card_identity_*` (canonical fields for matching imported cards to existing accounts)
- `agent_audit` / `agent_conversations` / `agent_runs` / `agent_events` (audit trail)

### Flow: from PDF to ledger
`imports` (raw blob) → `import_rows` (extracted, reviewable) → user commits → `transactions` (immutable canonical) → `classification_results` (agent or rules tag categories).

---

## 5. Weaknesses Interviewers Will Probe

| Weakness | Probe | Honest answer |
|---|---|---|
| No API auth | "What if I run this on a coffee-shop network?" | Bound to loopback only; for multi-user we'd add bearer-token middleware and switch to TLS. |
| 5s job latency | "How does the user feel waiting 5s for parsing to start?" | UX bandage: optimistic UI. Real fix: API kicks worker via HTTP after enqueue. |
| Single worker | "What if a PDF takes 60s and the user uploads 5?" | Today: serial. Fix: `tokio::spawn` per job inside the worker with a semaphore for concurrency, plus row-level claim. |
| 1500-line `main.tsx` | "Walk me through your frontend architecture" | Acknowledged tech debt; would split by feature with Tanstack Query for fetch dedup. |
| No rollback migrations | "How do you handle a bad migration in prod?" | Forward-only by policy; for prod-grade I'd add reversible migrations + a backup-before-migrate step. |
| Secrets in `.env` | "Where do you store the OpenAI key?" | Env var, loaded by Tauri. Better: OS keychain (`keyring` crate) — flagged on the roadmap. |
| No streaming LLM output | "Why doesn't the chat stream?" | Simplicity for v1; SSE/WS would be the obvious upgrade. |
| No request-size limit | "What stops me from uploading a 10 GB file?" | Today: nothing. Tower `RequestBodyLimitLayer` is one line; should add. |
| SQLite unencrypted | "PCI / privacy?" | Relies on OS disk encryption; SQLCipher is the upgrade path. |
| No token budget | "What if the agent costs $50 in one chat?" | Per-call cost logged but no cap. Need per-conversation budget enforcement. |
| No tests for full import flow | "How do you know it actually works?" | Unit tests per crate + a `tests/step1` smoke. Missing: integration test from upload → commit. |

---

## 6. "What would you improve?" — structured answer

1. **Streaming agent responses** via SSE — biggest UX win.
2. **Trigger-on-enqueue** (API → worker `POST /trigger`) — kills the 5s latency without losing polling fallback.
3. **Per-conversation token/cost budget** with hard cutoff.
4. **OS keychain integration** for API keys.
5. **Refactor `main.tsx`** → feature folders + Tanstack Query.
6. **Integration tests** covering CSV import, PDF import, agent confirmation flow.
7. **SQLCipher** for at-rest encryption.
8. **Replace polling with `LISTEN/NOTIFY`** if/when we move to Postgres for multi-user.
9. **Embeddings index on merchant descriptions** to improve `resolve_category_tool` over time.
10. **Reversible migrations** with `up.sql` / `down.sql` per version.

## 7. "How would you scale this?"

The product is local-first, so "scale" means "what if we made it multi-user / cloud-hosted":

1. **DB**: SQLite → Postgres (single-writer → multi-writer; row-level locking for the job claim).
2. **Job queue**: polling → Postgres `LISTEN/NOTIFY` (cheap) or Redis Streams / SQS (decoupled).
3. **API**: stateless Axum behind a load balancer; share state via Postgres + Redis (for `RunCoordinator` continuations).
4. **Worker**: horizontal scale with a `worker_id` lease on `job_runs.claimed_by`; tokio concurrency for I/O-bound (LLM) work.
5. **AuthN/Z**: bearer tokens (JWT) + per-user data isolation via `app_user_id` on every row.
6. **LLM**: route to a self-hosted vLLM cluster for cost; use OpenAI as fallback. Cache embeddings.
7. **Observability**: structured logs (tracing), metrics (Prometheus), traces (OpenTelemetry) per agent run.
8. **Deployment**: Docker images for API/worker, served via k8s / Fly.io. Tauri client downloads stay the same but point at the cloud API.

## 8. "What challenges did you face?"

Pick 2–3, tell the story:

1. **The human-in-the-loop pause.** First attempt: agent finishes turn, returns "need confirmation", UI sends a *new* user message saying "confirmed". Problem — the LLM saw two separate conversations and lost causal context. Fix: park the tokio task on a `oneshot::Receiver`, expose `POST /runs/:id/continue`. The agent sees one logical tool result. *(Talks to: concurrency, async Rust, UX-driven design.)*

2. **Card identity matching.** Importing the same Visa from two different PDFs produced duplicate accounts because "Visa Infinite ****1234" and "VISA INFINITE 1234" look different. Built canonicalization helpers (`canonicalize_account_descriptor`, `canonicalize_account_number_last4`) and a card-resolution UI step that surfaces fuzzy matches to the user. *(Talks to: data quality, edge-case thinking.)*

3. **Compile-time SQL with sqlx vs migration drift.** sqlx checks queries at compile time against a live DB schema; meant migrations had to be auto-applied or builds broke. Solution: `cargo sqlx prepare` snapshot + `--migrate` on boot. *(Talks to: tooling discipline.)*

---

## 9. AI / LLM Pipeline — deep-dive answers

**Prompt flow**: system prompt embeds (a) app instructions, (b) tool list (auto-generated from `ToolDef` schemas), (c) recent context (accounts, categories) from `get_agent_context_handler`. User message appended → call LLM → parse `tool_calls` → execute → append `Tool` messages → loop.

**Tool calling**: OpenAI function-calling format — JSON schema per tool defined in each `tools/*_tool.rs` file. Arguments JSON-deserialized into typed Rust structs (serde) before tool body runs. Errors surface as `Tool` message content so the LLM can self-correct.

**RAG**: not used. Rationale: bounded structured dataset; SQL is more precise than vector search for "show me restaurant spending in March." Where RAG *would* help: free-form merchant categorization across millions of merchants — future work.

**Embeddings / vector DB**: none today. Candidates for future: pgvector or sqlite-vec for merchant similarity.

**Model selection**: gpt-4o-mini default — cheap, fast, supports function calling well. Configurable via `OPENAI_MODEL`. Local path uses Llama-class models via Ollama.

**Token handling**: token counts returned in API response; persisted in `agent_audit.prompt_tokens` / `.completion_tokens`. Cost computed via per-model rates in `pricing.rs`. No proactive trimming yet — context limited only by history depth.

**Hallucination mitigation**:
- Tools (not the LLM) execute all data access — model can't invent numbers.
- Tool results echo back verbatim, so the LLM is summarizing real data.
- High-stakes mutations (category confirmation) require explicit user click.

**Backend orchestration**: agent runtime is `tokio`-async, awaits LLM + tools concurrently when possible, persists every turn through a non-blocking `mpsc` → `DbAuditSink` task.

---

## 10. Final Mock Interview — 20 likely questions

> Format: **Q** → concise → detailed → what they're evaluating → likely follow-up.

### Q1. "Give me a 60-second overview of Spendora."
- **Concise**: Local-first desktop expense tracker. Tauri shell + React UI + two Rust services + SQLite. LLM agent with function-calling does categorization and Q&A.
- **Detailed**: Use the pitch in §0.
- **Evaluating**: Can you summarize a system clearly under time pressure?
- **Follow-up**: "Why local-first?"

### Q2. "Why two separate Rust services instead of one?"
- **Concise**: Separation of concerns — keep the API responsive while CPU/LLM-heavy extraction runs out of band.
- **Detailed**: An API is a request/response contract with tight latency budgets. Parsing a 30-page PDF or calling an LLM can take 5–60 seconds. Putting that on the request path would either time out clients or pin tokio executor threads. The worker also gives us a natural place to retry, rate-limit, and parallelize without polluting the API surface. Communication happens through a shared `job_runs` table — at-most-once claim, idempotent retries.
- **Evaluating**: Do you understand process isolation, blocking-vs-non-blocking, queue semantics?
- **Follow-up**: "Why not just use `tokio::spawn` inside the API process?"
  - *Answer*: You can, but then a crash in extraction takes down the API, deploying them independently becomes harder, and shared state (e.g., LLM SDK clients with their own thread pools) gets messy. Process boundary = blast-radius boundary.

### Q3. "Walk me through what happens when I upload a PDF statement."
- **Concise**: UI base64-uploads → API stores import + enqueues job → worker polls, claims, parses (deterministic or LLM), writes `import_rows` → UI polls status → user reviews directions and resolves card → API commits to `transactions`.
- **Detailed**: See §2.2 flow diagram.
- **Evaluating**: Can you narrate an end-to-end flow under questioning?
- **Follow-up**: "What happens if the worker crashes mid-job?"
  - *Answer*: The row stays in `status='claimed'` with an `attempts` counter. A reaper (or restart-based) re-claims rows whose `claimed_at` is older than a threshold. With `attempts >= max_attempts`, it moves to a dead state — to be added (current gap).

### Q4. "Why SQLite over Postgres?"
- **Concise**: Local-first, zero-ops, transactional, fast enough for one user.
- **Detailed**: SQLite ships in-process — no daemon, no network, perfect for a desktop app. WAL mode gives concurrent readers + one writer. The whole DB is a single file you can back up by copying. Trade-off: doesn't scale to multi-user concurrency. If we ever go multi-tenant, sqlx makes the swap to Postgres mostly a connection-string change.
- **Evaluating**: Do you pick tech for the problem, not the résumé?
- **Follow-up**: "Where does SQLite break down?" → concurrent writers, big-team replication, > a few hundred GB.

### Q5. "How does your agent's tool calling work?"
- **Concise**: OpenAI function-calling spec. 9 tools defined as JSON schemas. Loop: call model → if `tool_calls`, execute and append result → repeat until plain text.
- **Detailed**: See §3.6.
- **Evaluating**: Understanding of structured outputs, multi-turn orchestration, error handling.
- **Follow-up**: "What happens if the model invents a tool name?"
  - *Answer*: Dispatch returns an error message; we append it as the Tool result so the model self-corrects on the next turn. OpenAI's API also enforces that tool names match the supplied schema, so it's rare in practice.

### Q6. "Where's the RAG?"
- **Concise**: Intentionally none.
- **Detailed**: The data is small, structured, and queryable with SQL — vector search would be less accurate than a `WHERE merchant ILIKE '%starbucks%'`. RAG shines on unstructured corpora; this isn't one. Where I *would* add embeddings: merchant→category similarity to bootstrap new users. Honest answer beats checkbox AI.
- **Evaluating**: Do you actually understand when RAG is the right tool?
- **Follow-up**: "Convince me RAG would never help."
  - *Answer*: It would — for fuzzy merchant matching ("STAR\*BUX #1234" → Starbucks). I'd add sqlite-vec embeddings on merchant strings; deterministic SQL remains for time/amount queries.

### Q7. "How does the human-in-the-loop confirmation work without breaking the LLM context?"
- **Concise**: A tokio `oneshot` channel keyed by `run_id` parks the agent task; `POST /runs/:id/continue` resumes it. The LLM sees one logical tool result.
- **Detailed**: `RunCoordinator` holds `HashMap<RunId, oneshot::Sender<Continuation>>`. The `confirm_category_tool` registers a receiver, returns `Future`, the agent loop `.await`s it. UI hits the continue endpoint with the user's decision → `sender.send(continuation)` → tool's await resolves → tool result is appended as a normal `Tool` message → loop continues. No synthetic user message, single conversation, single run row in the audit.
- **Evaluating**: async Rust, channel primitives, UX-driven backend design.
- **Follow-up**: "What if the user never clicks?"
  - *Answer*: Run times out after N minutes — `select!` on the receiver vs a `tokio::time::sleep`. Audit row marked `timed_out`.

### Q8. "How do you prevent hallucinated transactions?"
- **Concise**: The LLM never produces data — tools do. Model only chooses which tool to call.
- **Detailed**: All numbers, dates, and merchant names come from SQL queries inside tools. The model summarizes results. For the AI *extractor* (PDF → rows), we keep the raw PDF text in `transactions_raw` and require a human review step before any row lands in `transactions`. Confidence scores ride along on every extracted row.
- **Evaluating**: Maturity around LLM failure modes.
- **Follow-up**: "What about the extractor inventing a $0.01 entry?"
  - *Answer*: That's why the review step is mandatory. Future work: cross-check sum-of-rows vs the statement's printed total, flag mismatches.

### Q9. "How do you track LLM cost?"
- **Concise**: Token usage logged per turn; cost computed via a per-model price table in `pricing.rs`; stored in `agent_audit`.
- **Detailed**: OpenAI returns `usage.prompt_tokens` / `completion_tokens`. We multiply by `(input_price_per_1k, output_price_per_1k)` for the active model. Aggregated in the Activity tab. **Gap**: no enforcement — a runaway loop could cost $X. Fix: per-conversation `max_cost_usd`, abort on exceed.
- **Evaluating**: Production LLM awareness.
- **Follow-up**: "Tokens for local models?" → often missing in the response; we fall back to `tiktoken`-style estimation, currently log `None`.

### Q10. "Why polling instead of a real queue?"
- **Concise**: Zero infra dependencies for a desktop app.
- **Detailed**: Redis/RabbitMQ would mean another binary the user has to install or that we bundle. SQLite is already there. Trade-off: ≤5s latency. For multi-user cloud, I'd switch to Postgres `LISTEN/NOTIFY` (sub-100ms) or SQS.
- **Evaluating**: Pragmatism vs over-engineering.
- **Follow-up**: "How would you cut the latency without changing infra?" → API hits a `POST /trigger` on the worker after enqueue, falling back to poll on failure.

### Q11. "How do you handle SQL migrations?"
- **Concise**: Forward-only numbered `.sql` files; auto-run on startup when `--migrate` flag is set.
- **Detailed**: 13 migrations live in `services/expense-rs/migrations`. sqlx's migrator records applied versions in `_sqlx_migrations`. **Weaknesses**: no `down.sql` rollback, no pre-flight backup. Production fix: backup-then-migrate wrapper + reversible migrations.
- **Evaluating**: Ops awareness.
- **Follow-up**: "How would you handle a failed migration in a deployed desktop app?"
  - *Answer*: Snapshot `expense.db` to `expense.db.bak.<timestamp>` before migrating; on failure, swap back, surface a "please update" message.

### Q12. "Where are the security weaknesses?"
- **Concise**: No API auth, no rate limit, no request size cap, secrets in `.env`, DB unencrypted at rest.
- **Detailed**: Mitigated by binding to loopback + single-user model. Each one has a known fix path (§5 table). Threat model assumes "trusted machine"; if that ever changes, all bets are off.
- **Evaluating**: Honesty + ability to threat-model.
- **Follow-up**: "What's the most urgent one?"
  - *Answer*: Secret storage — `OPENAI_API_KEY` in a plaintext `.env` is the kind of thing that ends up in screenshots. Move to OS keychain via the `keyring` crate.

### Q13. "How would you scale this to 100k users?"
- **Concise**: SQLite → Postgres, polling → `LISTEN/NOTIFY` or SQS, stateless Axum behind LB, worker autoscaling, auth, observability.
- **Detailed**: See §7.
- **Evaluating**: Distributed-systems thinking.
- **Follow-up**: "Where does the architecture break first?"
  - *Answer*: `RunCoordinator` is in-memory — fails as soon as you have >1 API replica. Move continuation state into Redis with a TTL.

### Q14. "Walk me through your error handling philosophy."
- **Concise**: `Result<T, E>` everywhere, typed error enums per crate, propagated to API as structured JSON.
- **Detailed**: Each crate has its own `Error` enum (thiserror). The API converts to HTTP via an `IntoResponse` impl. Tool errors surface as Tool message content so the LLM can recover. Frontend treats any non-2xx as user-visible error text. **Gap**: error codes aren't fully stable — clients parse strings, not codes.
- **Evaluating**: Rust idiomatic-ness + production polish.
- **Follow-up**: "How would you make errors machine-parseable?" → add a stable `code` field (`IMPORT_PARSE_FAILED`, etc.) in the JSON body.

### Q15. "Why Tauri over Electron?"
- **Concise**: Bundle size + Rust-native shell.
- **Detailed**: Electron ships Chromium (~80 MB compressed); Tauri uses the OS webview (~10 MB). I already had Rust services, so the shell language was Rust for free. Trade-off: webview differences across OSes (Safari on macOS, Edge WebView2 on Windows, WebKitGTK on Linux) — CSS quirks happen.
- **Evaluating**: Tooling judgement.
- **Follow-up**: "What broke across OSes?" → fonts, scrollbar styling; mitigated with a normalized CSS reset.

### Q16. "What tests do you have, and what's missing?"
- **Concise**: Unit tests per crate + a `tests/step1` smoke runner; integration coverage is light.
- **Detailed**: `coordinator.rs` has 4 unit tests for park/resume/cancel. Storage has tests for `claim_pending_job` race semantics and canonicalization. Missing: full upload → commit integration test, UI e2e, load tests.
- **Evaluating**: Self-awareness about quality.
- **Follow-up**: "What would you add first?" → an integration test that boots an in-memory SQLite, fires `POST /imports`, runs the worker once synchronously, asserts `transactions` rows.

### Q17. "How does the worker avoid claiming the same job twice?"
- **Concise**: Atomic `UPDATE … WHERE status='pending' RETURNING …`.
- **Detailed**: `claim_pending_job` issues a transactional `UPDATE job_runs SET status='claimed', claimed_at=... WHERE id=(SELECT id FROM job_runs WHERE status='pending' ORDER BY id LIMIT 1) RETURNING *`. SQLite's serializable default ensures two concurrent claims can't both win. With multiple workers in Postgres you'd use `FOR UPDATE SKIP LOCKED`.
- **Evaluating**: DB concurrency literacy.
- **Follow-up**: "If a worker crashes between claim and complete?" → claimed rows older than `now() - T` are recovered by the next poller.

### Q18. "How does the agent know what tools are available?"
- **Concise**: Tools are registered in a `Vec<ToolDef>` (name + JSON schema) and serialized into the OpenAI `tools` parameter on every request.
- **Detailed**: Each tool implements a trait that exposes `name()`, `schema()`, and `execute(args, ctx)`. The agent runtime builds the tool list once per run from the registry. JSON schemas are validated server-side before dispatch so the LLM can't break invariants.
- **Evaluating**: Cleanness of the abstraction.
- **Follow-up**: "How would you add a new tool?" → implement the trait, register, ship — no agent loop changes needed.

### Q19. "Where would you most like to put real-time / streaming?"
- **Concise**: LLM responses → UI via SSE.
- **Detailed**: Today the API returns the full agent reply at once; with long chains of tool calls this can be 10+ seconds of silence. SSE per token (passthrough from OpenAI's stream) would make the chat feel instant. Implementation: `axum::response::sse::Sse<impl Stream>`, frontend uses `EventSource`.
- **Evaluating**: UX instincts + comfort with async streams.
- **Follow-up**: "How do you stream tool calls?" → OpenAI streams `delta.tool_calls.function.arguments` as JSON fragments; buffer until `finish_reason`, then execute.

### Q20. "What did you learn from this project?"
- **Concise**: That the boring choices (SQLite, polling, function calling, single binary) compound into a working product; cleverness is for the 5% that actually needs it.
- **Detailed**: Three lessons I'd take to any team:
  1. **Process boundaries are blast-radius boundaries** — splitting API and worker paid back the first time a PDF parser panicked.
  2. **Let the LLM choose, not compute** — function calling + tools is dramatically more reliable than asking the model to do math on JSON.
  3. **Audit everything** — being able to replay an agent run with full token / cost / tool-arg history turned debugging from guessing into reading.
- **Evaluating**: Reflection and growth mindset.
- **Follow-up**: "If you started over?" → keep the architecture; refactor `main.tsx` into feature modules from day one; pick OS-keychain over `.env` from day one.

---

## 11. Quick-reference cheat card (memorize these numbers)

- **Ports**: API 8081, Worker 8082, Vite dev 1420
- **Poll interval**: 5s default
- **Health-check timeout**: 90s total startup
- **Default LLM**: `gpt-4o-mini`, 90s timeout (OpenAI) / 180s (local)
- **Migrations**: 13 (0001 → 0013)
- **Agent tools**: 9
- **Workspace crates**: 8 (`api`, `worker`, `agent`, `core`, `storage_sqlite`, `connectors_plaid`, `connectors_manual`, `connectors_ai`)

---

## 12. Behavioral & ownership angles

- **Ownership**: "I'm the only contributor — I shipped the schema, the agent runtime, the IPC layer, the React UI."
- **Trade-off articulation**: pick polling-vs-queue or SQLite-vs-Postgres — both let you show structured thinking.
- **Failure story**: the human-in-the-loop pause re-design (§8.1) is the strongest narrative — initial design, why it broke, what you redesigned, what you learned.
- **Future direction**: streaming + cost budgets + multi-user cloud — frame as a roadmap, not regrets.

Good luck.
