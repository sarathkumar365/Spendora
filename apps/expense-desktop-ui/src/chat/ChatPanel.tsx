import React from "react";
import ReactMarkdown from "react-markdown";

type ChatRole = "user" | "assistant";

type CachedTransaction = {
  id: string;
  account_id?: string;
  account_name?: string | null;
  amount_cents: number;
  currency: string;
  description: string;
  booked_at: string;
  direction: string;
};

type ToolEvent = {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
  result?: {
    ok: boolean;
    summary: string;
    transactionIds: string[];
    error?: string | null;
  };
};

type CategoryMerchant = {
  merchant_signature_id: string;
  label: string;
  normalized_key: string;
  txn_count: number;
  total_cents: number;
  sample_descriptions: string[];
  confidence: number | null;
};

type CategoryConfirmation = {
  categorySlug: string;
  categoryName: string;
  confirmed: CategoryMerchant[];
  suggested: CategoryMerchant[];
  excluded: CategoryMerchant[];
  /// Set of merchant_signature_ids the user has chosen to include from the suggested list.
  selectedIds: Set<string>;
  /// Once Apply is clicked, the card disables itself.
  applied: boolean;
};

type RunStats = {
  runId: string;
  conversationId: string;
  iterations: number;
  model: string;
  promptTokens: number;
  completionTokens: number;
  costMicros: number;
  llmDurationMs: number;
};

type ChatTurn = {
  id: string;
  role: ChatRole;
  content: string;
  toolEvents?: ToolEvent[];
  followups?: string[];
  citedIds?: string[];
  truncated?: string | null;
  error?: string | null;
  categoryConfirmation?: CategoryConfirmation;
  runStats?: RunStats;
};

type AgentEventBase = { kind: string };
type StartedEvent = AgentEventBase & {
  kind: "started";
  model: string;
  provider: string;
};
type ToolCallStartEvent = AgentEventBase & {
  kind: "tool_call_start";
  id: string;
  name: string;
  arguments: Record<string, unknown>;
};
type ToolCallResultEvent = AgentEventBase & {
  kind: "tool_call_result";
  id: string;
  name: string;
  ok: boolean;
  summary: string;
  transaction_ids: string[];
  data: unknown;
  error: string | null;
};
type AssistantMessageEvent = AgentEventBase & {
  kind: "assistant_message";
  content: string;
};
type FollowupsEvent = AgentEventBase & {
  kind: "followups";
  suggestions: string[];
};
type TruncatedEvent = AgentEventBase & { kind: "truncated"; reason: string };
type ErrorEvent = AgentEventBase & { kind: "error"; message: string };
type DoneEvent = AgentEventBase & {
  kind: "done";
  iterations: number;
  cited_transaction_ids: string[];
  run_id: string;
  conversation_id: string;
  model: string;
  total_prompt_tokens: number;
  total_completion_tokens: number;
  total_cost_micros: number;
  total_llm_duration_ms: number;
};
type CategoryConfirmationNeededEvent = AgentEventBase & {
  kind: "category_confirmation_needed";
  category_slug: string;
  payload: {
    category: { id?: string; name?: string; slug?: string };
    confirmed: CategoryMerchant[];
    suggested: CategoryMerchant[];
    excluded: CategoryMerchant[];
  };
};

type AgentEvent =
  | StartedEvent
  | ToolCallStartEvent
  | ToolCallResultEvent
  | AssistantMessageEvent
  | FollowupsEvent
  | TruncatedEvent
  | ErrorEvent
  | DoneEvent
  | CategoryConfirmationNeededEvent;

type AccountSummary = {
  id: string;
  name: string;
  currency: string;
  account_type: string | null;
  last4: string | null;
  customer_name: string | null;
};

type AgentContextResponse = {
  today: string;
  timezone: string;
  currency_default: string;
  provider: string;
  model: string;
  registered_tools: string[];
  accounts: AccountSummary[];
  data_range: {
    earliest_booked_at: string | null;
    latest_booked_at: string | null;
    transaction_count: number;
  };
};

type Props = {
  apiBaseUrl: string;
};

const STORAGE_KEY = "spendora.chat.v1";
const MAX_PERSISTED_TURNS = 100;

function newId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function loadPersistedTurns(): ChatTurn[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as ChatTurn[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function persistTurns(turns: ChatTurn[]) {
  try {
    // Persist only final-state turns (skip in-flight assistant with empty content),
    // and cap to the most recent MAX_PERSISTED_TURNS so localStorage doesn't grow forever.
    const stable = turns
      .filter((t) => t.role === "user" || (t.role === "assistant" && t.content))
      .slice(-MAX_PERSISTED_TURNS);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(stable));
  } catch {
    /* localStorage full / disabled — ignore */
  }
}

function formatToolLabel(name: string, args: Record<string, unknown>): string {
  switch (name) {
    case "list_accounts_and_cards":
      return "Listing accounts";
    case "query_transactions": {
      const parts: string[] = ["Querying transactions"];
      if (args.account_id) parts.push("(one account)");
      if (args.merchant_substring) parts.push(`matching "${args.merchant_substring}"`);
      if (args.direction) parts.push(`(${args.direction})`);
      if (args.date_from || args.date_to) {
        parts.push(`${args.date_from ?? "…"} → ${args.date_to ?? "…"}`);
      }
      return parts.join(" ");
    }
    case "aggregate_transactions": {
      const parts: string[] = ["Aggregating"];
      parts.push(`${args.metric ?? "?"} by ${args.group_by ?? "?"}`);
      if (args.direction) parts.push(`(${args.direction})`);
      if (args.merchant_substring) parts.push(`matching "${args.merchant_substring}"`);
      if (args.date_from || args.date_to) {
        parts.push(`${args.date_from ?? "…"} → ${args.date_to ?? "…"}`);
      }
      return parts.join(" ");
    }
    case "compare_periods": {
      const parts: string[] = ["Comparing periods"];
      if (args.label_a && args.label_b) {
        parts.push(`${args.label_a} vs ${args.label_b}`);
      }
      if (args.metric) parts.push(`(${args.metric})`);
      if (args.group_by) parts.push(`by ${args.group_by}`);
      if (args.direction) parts.push(`(${args.direction})`);
      return parts.join(" ");
    }
    case "find_recurring": {
      const parts: string[] = ["Detecting recurring charges"];
      if (args.lookback_months) parts.push(`(last ${args.lookback_months}mo)`);
      if (args.merchant_substring) parts.push(`matching "${args.merchant_substring}"`);
      return parts.join(" ");
    }
    case "transaction_detail":
      return `Looking up transaction ${(args.transaction_id as string | undefined)?.slice(0, 8) ?? ""}…`;
    case "echo":
      return "Echo (debug)";
    default:
      return name;
  }
}

function formatAmount(cents: number, currency: string): string {
  const dollars = Math.abs(cents) / 100;
  return `${currency} ${dollars.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  })}`;
}

export function ChatPanel({ apiBaseUrl }: Props) {
  const [turns, setTurns] = React.useState<ChatTurn[]>(() => loadPersistedTurns());
  const [input, setInput] = React.useState("");
  const [running, setRunning] = React.useState(false);
  const [ctx, setCtx] = React.useState<AgentContextResponse | null>(null);
  const [ctxError, setCtxError] = React.useState<string | null>(null);
  // One conversation_id per UI session, sent on every chat request so the audit log groups
  // every turn from this session under one conversation row.
  const conversationIdRef = React.useRef<string>(newId());
  const [drawerCitedIds, setDrawerCitedIds] = React.useState<string[] | null>(null);
  const abortRef = React.useRef<AbortController | null>(null);
  const transcriptRef = React.useRef<HTMLDivElement | null>(null);
  // Cache of all transaction rows seen via tool_call_result events.
  const txnCache = React.useRef<Map<string, CachedTransaction>>(new Map());

  React.useEffect(() => {
    void fetchContext();
    return () => abortRef.current?.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  React.useEffect(() => {
    transcriptRef.current?.scrollTo({
      top: transcriptRef.current.scrollHeight,
      behavior: "smooth"
    });
  }, [turns]);

  React.useEffect(() => {
    persistTurns(turns);
  }, [turns]);

  async function fetchContext() {
    setCtxError(null);
    try {
      const res = await fetch(`${apiBaseUrl}/api/v1/agent/context`);
      if (!res.ok) {
        const text = await res.text();
        throw new Error(friendlyHttpError(res.status, text));
      }
      const json = (await res.json()) as AgentContextResponse;
      setCtx(json);
    } catch (err) {
      setCtxError(err instanceof Error ? err.message : String(err));
      setCtx(null);
    }
  }

  function clearChat() {
    abortRef.current?.abort();
    setTurns([]);
    txnCache.current.clear();
    conversationIdRef.current = newId();
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch {
      /* ignore */
    }
  }

  async function sendMessage(text: string) {
    const trimmed = text.trim();
    if (!trimmed || running) return;

    const userTurn: ChatTurn = {
      id: newId(),
      role: "user",
      content: trimmed
    };
    const assistantTurn: ChatTurn = {
      id: newId(),
      role: "assistant",
      content: "",
      toolEvents: []
    };

    const history = turns.flatMap<{ role: ChatRole; content: string }>((t) =>
      t.role === "user" || (t.role === "assistant" && t.content)
        ? [{ role: t.role, content: t.content }]
        : []
    );

    setTurns((prev) => [...prev, userTurn, assistantTurn]);
    setInput("");
    setRunning(true);

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const res = await fetch(`${apiBaseUrl}/api/v1/agent/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
        body: JSON.stringify({
          message: trimmed,
          history,
          conversation_id: conversationIdRef.current
        }),
        signal: controller.signal
      });

      if (!res.ok || !res.body) {
        const text = await res.text();
        throw new Error(friendlyHttpError(res.status, text));
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        let idx;
        while ((idx = buffer.indexOf("\n\n")) !== -1) {
          const rawEvent = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 2);
          const event = parseSse(rawEvent);
          if (event) applyEvent(assistantTurn.id, event);
        }
      }
    } catch (err) {
      if ((err as { name?: string })?.name === "AbortError") {
        applyError(assistantTurn.id, "Cancelled.");
      } else if (err instanceof TypeError) {
        // `fetch` throws TypeError for network/DNS errors with no HTTP response.
        applyError(
          assistantTurn.id,
          "Couldn't reach the local agent service. Make sure Spendora's API process is running."
        );
      } else {
        applyError(assistantTurn.id, err instanceof Error ? err.message : String(err));
      }
    } finally {
      setRunning(false);
      abortRef.current = null;
    }
  }

  function cacheTransactionsFromToolResult(event: ToolCallResultEvent) {
    if (!event.ok || !event.data) return;
    const data = event.data as { transactions?: CachedTransaction[] };
    const txns = data.transactions;
    if (!Array.isArray(txns)) return;
    for (const t of txns) {
      if (t && t.id) txnCache.current.set(t.id, t);
    }
  }

  function applyEvent(turnId: string, event: AgentEvent) {
    if (event.kind === "tool_call_result") {
      cacheTransactionsFromToolResult(event);
    }
    setTurns((prev) =>
      prev.map((t) => {
        if (t.id !== turnId) return t;
        switch (event.kind) {
          case "tool_call_start": {
            const tools = t.toolEvents ?? [];
            return {
              ...t,
              toolEvents: [
                ...tools,
                {
                  id: event.id,
                  name: event.name,
                  arguments: (event.arguments ?? {}) as Record<string, unknown>
                }
              ]
            };
          }
          case "tool_call_result": {
            const tools = (t.toolEvents ?? []).map((te) =>
              te.id === event.id
                ? {
                    ...te,
                    result: {
                      ok: event.ok,
                      summary: event.summary,
                      transactionIds: event.transaction_ids ?? [],
                      error: event.error
                    }
                  }
                : te
            );
            return { ...t, toolEvents: tools };
          }
          case "assistant_message":
            return { ...t, content: event.content };
          case "category_confirmation_needed": {
            const p = event.payload ?? { confirmed: [], suggested: [], excluded: [] };
            const categoryName =
              p.category?.name ?? event.category_slug.charAt(0).toUpperCase() + event.category_slug.slice(1);
            return {
              ...t,
              categoryConfirmation: {
                categorySlug: event.category_slug,
                categoryName,
                confirmed: p.confirmed ?? [],
                suggested: p.suggested ?? [],
                excluded: p.excluded ?? [],
                // Pre-select only very high-confidence suggestions (>=0.85). Phase 7c
                // raised this from 0.7 after the audit showed Dollarama / pharmacy slipping
                // in unnoticed when the user clicked Apply without ticking through.
                selectedIds: new Set(
                  (p.suggested ?? [])
                    .filter((m) => (m.confidence ?? 0) >= 0.85)
                    .map((m) => m.merchant_signature_id)
                ),
                applied: false
              }
            };
          }
          case "followups":
            return { ...t, followups: event.suggestions ?? [] };
          case "done":
            return {
              ...t,
              citedIds: event.cited_transaction_ids ?? [],
              runStats: {
                runId: event.run_id,
                conversationId: event.conversation_id,
                iterations: event.iterations,
                model: event.model,
                promptTokens: event.total_prompt_tokens,
                completionTokens: event.total_completion_tokens,
                costMicros: event.total_cost_micros,
                llmDurationMs: event.total_llm_duration_ms
              }
            };
          case "truncated":
            return { ...t, truncated: event.reason };
          case "error":
            return { ...t, error: event.message };
          case "started":
          default:
            return t;
        }
      })
    );
  }

  function applyError(turnId: string, message: string) {
    setTurns((prev) =>
      prev.map((t) => (t.id === turnId ? { ...t, error: message } : t))
    );
  }

  function toggleCategoryMerchant(turnId: string, merchantId: string) {
    setTurns((prev) =>
      prev.map((t) => {
        if (t.id !== turnId || !t.categoryConfirmation || t.categoryConfirmation.applied) {
          return t;
        }
        const next = new Set(t.categoryConfirmation.selectedIds);
        if (next.has(merchantId)) {
          next.delete(merchantId);
        } else {
          next.add(merchantId);
        }
        return {
          ...t,
          categoryConfirmation: { ...t.categoryConfirmation, selectedIds: next }
        };
      })
    );
  }

  async function applyCategoryConfirmation(turnId: string) {
    const turn = turns.find((t) => t.id === turnId);
    const card = turn?.categoryConfirmation;
    if (!card || card.applied) return;

    // Always include user-confirmed merchants. For suggested ones, the selectedIds set decides.
    const include: string[] = [];
    const exclude: string[] = [];
    for (const m of card.confirmed) include.push(m.merchant_signature_id);
    for (const m of card.suggested) {
      if (card.selectedIds.has(m.merchant_signature_id)) {
        include.push(m.merchant_signature_id);
      } else {
        exclude.push(m.merchant_signature_id);
      }
    }

    const followup =
      `[User confirmed category "${card.categorySlug}"]\n` +
      `Call confirm_category_assignments now with this exact payload:\n` +
      JSON.stringify(
        {
          category: card.categorySlug,
          assignments: [
            ...include.map((id) => ({ merchant_signature_id: id, included: true })),
            ...exclude.map((id) => ({ merchant_signature_id: id, included: false }))
          ]
        },
        null,
        2
      ) +
      `\nThen compute the original spending question for the confirmed merchants ` +
      `(use aggregate_transactions with merchant_substrings set to their normalized_key values) ` +
      `and answer in the same format you would have used originally.`;

    // Mark the card applied so it disables.
    setTurns((prev) =>
      prev.map((t) =>
        t.id === turnId && t.categoryConfirmation
          ? { ...t, categoryConfirmation: { ...t.categoryConfirmation, applied: true } }
          : t
      )
    );

    await sendMessage(followup);
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    void sendMessage(input);
  }

  function handleStop() {
    abortRef.current?.abort();
  }

  const starters = ctx?.accounts && ctx.accounts.length > 0
    ? [
        "How much did I spend last month?",
        "Top 10 merchants this year",
        "Show my biggest 5 transactions",
        "How much inflow vs outflow last month?",
        "List Amazon charges this year",
        ctx.accounts[0]?.name
          ? `How much spent on ${ctx.accounts[0].name}?`
          : "What accounts do I have?"
      ]
    : [
        "What accounts do I have?",
        "How much did I spend last month?",
        "List my biggest 5 transactions",
        "How much inflow vs outflow last month?"
      ];

  return (
    <section className="panel page chat-page">
      <div className="chat-header">
        <div>
          <p className="eyebrow">Agent</p>
          <h2>Ask anything about your money</h2>
          <p className="muted small">
            {ctx ? (
              <>
                <code>{ctx.model}</code> · {ctx.accounts.length} account{ctx.accounts.length === 1 ? "" : "s"} ·{" "}
                {ctx.data_range.transaction_count} transactions{ctx.data_range.earliest_booked_at ? (
                  <>
                    {" "}({ctx.data_range.earliest_booked_at} → {ctx.data_range.latest_booked_at})
                  </>
                ) : null}
              </>
            ) : ctxError ? (
              <>Agent not configured: {ctxError}</>
            ) : (
              <>Loading agent context…</>
            )}
          </p>
        </div>
        {turns.length > 0 ? (
          <button className="button ghost small-button" onClick={clearChat}>
            Clear chat
          </button>
        ) : null}
      </div>

      <div className="chat-transcript" ref={transcriptRef}>
        {turns.length === 0 ? (
          <div className="chat-empty">
            <p>Ask anything about your spending. A few starters:</p>
            <div className="chat-starter-grid">
              {starters.map((s) => (
                <button
                  key={s}
                  className="button ghost"
                  onClick={() => void sendMessage(s)}
                  disabled={running}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        ) : (
          turns.map((t) => (
            <TurnView
              key={t.id}
              turn={t}
              onCitationClick={(ids) => setDrawerCitedIds(ids)}
              onFollowupClick={(q) => void sendMessage(q)}
              onCategoryToggle={(turnId, merchantId) => toggleCategoryMerchant(turnId, merchantId)}
              onCategoryApply={(turnId) => void applyCategoryConfirmation(turnId)}
              runningLast={running && t.id === turns[turns.length - 1]?.id}
            />
          ))
        )}
        {running ? <div className="chat-thinking">Thinking…</div> : null}
      </div>

      <form className="chat-input-row" onSubmit={handleSubmit}>
        <input
          type="text"
          className="chat-input"
          placeholder="Ask about your spending, accounts, or trends…"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          disabled={running || !!ctxError}
        />
        {running ? (
          <button type="button" className="button" onClick={handleStop}>
            Stop
          </button>
        ) : (
          <button
            type="submit"
            className="button"
            disabled={!input.trim() || !!ctxError}
          >
            Send
          </button>
        )}
      </form>

      {drawerCitedIds ? (
        <CitationDrawer
          ids={drawerCitedIds}
          cache={txnCache.current}
          onClose={() => setDrawerCitedIds(null)}
        />
      ) : null}
    </section>
  );
}

function TurnView({
  turn,
  onCitationClick,
  onFollowupClick,
  onCategoryToggle,
  onCategoryApply,
  runningLast
}: {
  turn: ChatTurn;
  onCitationClick: (ids: string[]) => void;
  onFollowupClick: (q: string) => void;
  onCategoryToggle: (turnId: string, merchantId: string) => void;
  onCategoryApply: (turnId: string) => void;
  runningLast: boolean;
}) {
  if (turn.role === "user") {
    return (
      <div className="chat-turn user">
        <div className="chat-bubble user">{turn.content}</div>
      </div>
    );
  }

  const citedIds = turn.citedIds ?? [];

  return (
    <div className="chat-turn assistant">
      <div className="chat-bubble assistant">
        {turn.toolEvents && turn.toolEvents.length > 0 ? (
          <div className="chat-tool-events">
            {turn.toolEvents.map((te) => (
              <div
                key={te.id}
                className={`chat-tool-event ${te.result ? (te.result.ok ? "ok" : "err") : "running"}`}
              >
                <span className="chat-tool-event-label">
                  {formatToolLabel(te.name, te.arguments)}
                </span>
                {te.result ? (
                  <span className="chat-tool-event-summary">· {te.result.summary}</span>
                ) : (
                  <span className="chat-tool-event-summary">…</span>
                )}
              </div>
            ))}
          </div>
        ) : null}

        {turn.content ? (
          <div className="chat-assistant-text">
            <ReactMarkdown>{turn.content}</ReactMarkdown>
          </div>
        ) : !turn.error && !turn.truncated && runningLast && !turn.categoryConfirmation ? (
          <div className="chat-assistant-text muted">…</div>
        ) : null}

        {turn.categoryConfirmation ? (
          <CategoryConfirmationCard
            turnId={turn.id}
            card={turn.categoryConfirmation}
            onToggle={onCategoryToggle}
            onApply={onCategoryApply}
          />
        ) : null}

        {citedIds.length > 0 ? (
          <button
            className="chat-citation-chip"
            onClick={() => onCitationClick(citedIds)}
          >
            View {citedIds.length} transaction{citedIds.length === 1 ? "" : "s"}
          </button>
        ) : null}

        {turn.followups && turn.followups.length > 0 && !runningLast ? (
          <div className="chat-followups">
            {turn.followups.map((f) => (
              <button
                key={f}
                className="chat-followup-chip"
                onClick={() => onFollowupClick(f)}
              >
                {f}
              </button>
            ))}
          </div>
        ) : null}

        {turn.truncated ? (
          <div className="chat-warning">Stopped early: {turn.truncated}</div>
        ) : null}
        {turn.error ? <div className="chat-error">Error: {turn.error}</div> : null}

        {turn.runStats ? <TurnDetailsExpander stats={turn.runStats} toolEvents={turn.toolEvents} /> : null}
      </div>
    </div>
  );
}

function TurnDetailsExpander({
  stats,
  toolEvents
}: {
  stats: RunStats;
  toolEvents?: ToolEvent[];
}) {
  const [open, setOpen] = React.useState(false);

  const costDollars = stats.costMicros / 1_000_000;
  const costLabel =
    stats.costMicros > 0
      ? `$${costDollars.toFixed(costDollars >= 0.01 ? 4 : 6)}`
      : "(no cost — provider didn't report usage)";

  return (
    <details
      className="chat-details"
      open={open}
      onToggle={(e) => setOpen((e.target as HTMLDetailsElement).open)}
    >
      <summary className="chat-details-summary">
        Details · {stats.iterations} iteration{stats.iterations === 1 ? "" : "s"} ·{" "}
        {stats.llmDurationMs} ms LLM · {costLabel}
      </summary>
      <div className="chat-details-body">
        <dl className="chat-details-grid">
          <dt>Model</dt>
          <dd><code>{stats.model}</code></dd>
          <dt>Prompt tokens</dt>
          <dd>{stats.promptTokens.toLocaleString()}</dd>
          <dt>Completion tokens</dt>
          <dd>{stats.completionTokens.toLocaleString()}</dd>
          <dt>Estimated cost</dt>
          <dd>{costLabel}</dd>
          <dt>LLM time</dt>
          <dd>{stats.llmDurationMs} ms across {stats.iterations} call{stats.iterations === 1 ? "" : "s"}</dd>
          <dt>Run id</dt>
          <dd><code className="chat-details-id">{stats.runId}</code></dd>
          <dt>Conversation id</dt>
          <dd><code className="chat-details-id">{stats.conversationId}</code></dd>
        </dl>

        {toolEvents && toolEvents.length > 0 ? (
          <div className="chat-details-tools">
            <p className="muted small">Tool calls ({toolEvents.length}):</p>
            <ul>
              {toolEvents.map((te) => (
                <li key={te.id}>
                  <code>{te.name}</code>{" "}
                  {te.result ? (
                    <span className={te.result.ok ? "muted small" : "chat-error"}>
                      — {te.result.summary}
                    </span>
                  ) : (
                    <span className="muted small">— …</span>
                  )}
                </li>
              ))}
            </ul>
          </div>
        ) : null}
      </div>
    </details>
  );
}

function CategoryConfirmationCard({
  turnId,
  card,
  onToggle,
  onApply
}: {
  turnId: string;
  card: CategoryConfirmation;
  onToggle: (turnId: string, merchantId: string) => void;
  onApply: (turnId: string) => void;
}) {
  const includedCount = card.confirmed.length + card.selectedIds.size;
  return (
    <div className={`chat-category-card${card.applied ? " applied" : ""}`}>
      <header className="chat-category-card-header">
        <strong>Quick check — which of these are {card.categoryName}?</strong>
        <span className="muted small">
          {includedCount} merchant{includedCount === 1 ? "" : "s"} selected
        </span>
      </header>

      {card.confirmed.length > 0 ? (
        <div className="chat-category-card-section">
          <p className="muted small">Already confirmed — auto-included</p>
          <ul>
            {card.confirmed.map((m) => (
              <li key={m.merchant_signature_id} className="chat-category-row confirmed">
                <span className="chat-category-row-check">✓</span>
                <span className="chat-category-row-label">{m.label}</span>
                <span className="chat-category-row-meta muted small">
                  {m.txn_count} txn{m.txn_count === 1 ? "" : "s"} · {formatAmount(m.total_cents, "CAD")}
                </span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {card.suggested.length > 0 ? (
        <div className="chat-category-card-section">
          <p className="muted small">Maybe {card.categoryName.toLowerCase()} — toggle as needed</p>
          <ul>
            {card.suggested.map((m) => {
              const checked = card.selectedIds.has(m.merchant_signature_id);
              return (
                <li
                  key={m.merchant_signature_id}
                  className={`chat-category-row suggested${checked ? " checked" : ""}`}
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    disabled={card.applied}
                    onChange={() => onToggle(turnId, m.merchant_signature_id)}
                  />
                  <span className="chat-category-row-label">{m.label}</span>
                  <span className="chat-category-row-meta muted small">
                    {m.txn_count} txn{m.txn_count === 1 ? "" : "s"} · {formatAmount(m.total_cents, "CAD")}
                    {m.confidence != null ? ` · ${Math.round(m.confidence * 100)}% confident` : null}
                  </span>
                </li>
              );
            })}
          </ul>
        </div>
      ) : null}

      {card.confirmed.length === 0 && card.suggested.length === 0 ? (
        <p className="muted small">No matching merchants found in this window.</p>
      ) : null}

      <div className="chat-category-card-actions">
        <button
          className="button"
          onClick={() => onApply(turnId)}
          disabled={card.applied || (card.confirmed.length === 0 && card.selectedIds.size === 0)}
        >
          {card.applied ? "Applied" : "Apply"}
        </button>
      </div>
    </div>
  );
}

function CitationDrawer({
  ids,
  cache,
  onClose
}: {
  ids: string[];
  cache: Map<string, CachedTransaction>;
  onClose: () => void;
}) {
  const txns = ids.map((id) => cache.get(id)).filter(Boolean) as CachedTransaction[];
  const missing = ids.length - txns.length;

  return (
    <div className="chat-drawer-backdrop" onClick={onClose}>
      <aside className="chat-drawer" onClick={(e) => e.stopPropagation()}>
        <header className="chat-drawer-header">
          <h3>Cited transactions ({ids.length})</h3>
          <button className="button ghost small-button" onClick={onClose}>
            Close
          </button>
        </header>
        {missing > 0 ? (
          <p className="muted small">
            {missing} transaction{missing === 1 ? "" : "s"} not in cache (not yet fetched in this
            session). Ask the agent to show them and they'll appear here.
          </p>
        ) : null}
        <ul className="chat-drawer-list">
          {txns.map((t) => (
            <li key={t.id} className="chat-drawer-row">
              <div className="chat-drawer-row-main">
                <span className="chat-drawer-merchant">{t.description}</span>
                <span
                  className={`chat-drawer-amount ${
                    t.direction === "credit" ? "is-credit" : "is-debit"
                  }`}
                >
                  {t.direction === "credit" ? "+" : "−"}
                  {formatAmount(t.amount_cents, t.currency)}
                </span>
              </div>
              <div className="chat-drawer-row-sub muted small">
                {t.booked_at} · {t.account_name ?? t.account_id ?? "—"} · {t.direction}
              </div>
            </li>
          ))}
        </ul>
      </aside>
    </div>
  );
}

function friendlyHttpError(status: number, body: string): string {
  const snippet = body.trim().slice(0, 200);
  switch (status) {
    case 429:
      return "Hit a rate limit on the LLM provider. Wait a moment and try again.";
    case 401:
    case 403:
      return "The LLM provider rejected the API key. Check OPENAI_API_KEY in your .env.";
    case 503:
      return snippet || "The agent service is unavailable — provider may not be configured.";
    case 500:
    case 502:
    case 504:
      return `Local API error (HTTP ${status})${snippet ? `: ${snippet}` : ""}`;
    default:
      return snippet || `Request failed (HTTP ${status})`;
  }
}

function parseSse(raw: string): AgentEvent | null {
  let data = "";
  for (const line of raw.split("\n")) {
    if (line.startsWith("data:")) {
      data += line.slice(5).trimStart();
    }
  }
  if (!data) return null;
  try {
    return JSON.parse(data) as AgentEvent;
  } catch {
    return null;
  }
}
