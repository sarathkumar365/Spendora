import React from "react";

type ConversationSummary = {
  conversation_id: string;
  started_at: string;
  last_active_at: string;
  run_count: number;
  total_cost_micros: number;
  total_prompt_tokens: number;
  total_completion_tokens: number;
  first_question: string | null;
};

type RunSummary = {
  run_id: string;
  conversation_id: string;
  occurred_at: string;
  status: string | null;
  model: string | null;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  cost_micros: number | null;
  error_message: string | null;
};

type AuditSummary = {
  window_days: number | null;
  since_iso: string;
  total_cost_micros: number;
  total_cost_dollars: number;
  total_prompt_tokens: number;
  total_completion_tokens: number;
  llm_call_count: number;
};

type AgentEventRecord = {
  id: string;
  conversation_id: string;
  run_id: string;
  sequence: number;
  event_kind: string;
  occurred_at: string;
  duration_ms: number | null;
  payload_json: string;
  status: string | null;
  model: string | null;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  cost_micros: number | null;
  user_message_excerpt: string | null;
  tool_name: string | null;
  ok: boolean | null;
  error_message: string | null;
};

type View = "conversations" | "runs";

type Props = {
  apiBaseUrl: string;
};

function dollars(micros: number | null | undefined): string {
  const m = micros ?? 0;
  if (m === 0) return "—";
  const d = m / 1_000_000;
  return d >= 0.01 ? `$${d.toFixed(4)}` : `$${d.toFixed(6)}`;
}

function fmtTimestamp(iso: string): string {
  // SQLite default is "YYYY-MM-DD HH:MM:SS" UTC.
  // Render local-ish without seconds for compactness.
  const safe = iso.replace(" ", "T");
  const d = new Date(safe + (safe.endsWith("Z") || safe.includes("+") ? "" : "Z"));
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
}

export function ActivityPanel({ apiBaseUrl }: Props) {
  const [view, setView] = React.useState<View>("conversations");
  const [conversations, setConversations] = React.useState<ConversationSummary[]>([]);
  const [runs, setRuns] = React.useState<RunSummary[]>([]);
  const [summary7, setSummary7] = React.useState<AuditSummary | null>(null);
  const [summary30, setSummary30] = React.useState<AuditSummary | null>(null);
  const [summaryAll, setSummaryAll] = React.useState<AuditSummary | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);
  const [drawerRunId, setDrawerRunId] = React.useState<string | null>(null);

  React.useEffect(() => {
    void loadAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function loadAll() {
    setLoading(true);
    setErr(null);
    try {
      const [c, r, s7, s30, sAll] = await Promise.all([
        fetchJson<ConversationSummary[]>(`${apiBaseUrl}/api/v1/audit/conversations?limit=50`),
        fetchJson<RunSummary[]>(`${apiBaseUrl}/api/v1/audit/runs?limit=100`),
        fetchJson<AuditSummary>(`${apiBaseUrl}/api/v1/audit/summary?days=7`),
        fetchJson<AuditSummary>(`${apiBaseUrl}/api/v1/audit/summary?days=30`),
        fetchJson<AuditSummary>(`${apiBaseUrl}/api/v1/audit/summary`)
      ]);
      setConversations(c);
      setRuns(r);
      setSummary7(s7);
      setSummary30(s30);
      setSummaryAll(sAll);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="panel page activity-page">
      <div className="activity-header">
        <div>
          <p className="eyebrow">Activity</p>
          <h2>Audit trail</h2>
          <p className="muted small">
            Every agent run, LLM call, and tool execution is recorded locally. Click any row
            for the full event sequence.
          </p>
        </div>
        <button className="button ghost small-button" onClick={() => void loadAll()} disabled={loading}>
          {loading ? "Loading…" : "Refresh"}
        </button>
      </div>

      <div className="activity-summary-row">
        <SummaryCard label="Last 7 days" s={summary7} />
        <SummaryCard label="Last 30 days" s={summary30} />
        <SummaryCard label="All time" s={summaryAll} />
      </div>

      {err ? <div className="chat-error">Error: {err}</div> : null}

      <div className="activity-view-toggle">
        <button
          className={view === "conversations" ? "button active" : "button ghost"}
          onClick={() => setView("conversations")}
        >
          Conversations ({conversations.length})
        </button>
        <button
          className={view === "runs" ? "button active" : "button ghost"}
          onClick={() => setView("runs")}
        >
          Runs ({runs.length})
        </button>
      </div>

      {view === "conversations" ? (
        <ConversationsTable
          conversations={conversations}
          onSelectConversation={(_id) => {
            // For v1, clicking a conversation row just switches to the runs view; the user
            // sees all recent runs there and can drill into one. Filtering by conversation
            // would need a new endpoint — deferred.
            setView("runs");
          }}
        />
      ) : (
        <RunsTable runs={runs} onSelectRun={(rid) => setDrawerRunId(rid)} />
      )}

      {drawerRunId ? (
        <RunEventsDrawer
          runId={drawerRunId}
          apiBaseUrl={apiBaseUrl}
          onClose={() => setDrawerRunId(null)}
        />
      ) : null}
    </section>
  );
}

function SummaryCard({ label, s }: { label: string; s: AuditSummary | null }) {
  return (
    <div className="activity-summary-card">
      <p className="muted small">{label}</p>
      <p className="activity-summary-cost">
        {s ? `$${(s.total_cost_micros / 1_000_000).toFixed(4)}` : "—"}
      </p>
      <p className="muted small">
        {s
          ? `${s.llm_call_count} call${s.llm_call_count === 1 ? "" : "s"} · ${s.total_prompt_tokens.toLocaleString()} in / ${s.total_completion_tokens.toLocaleString()} out`
          : ""}
      </p>
    </div>
  );
}

function ConversationsTable({
  conversations,
  onSelectConversation
}: {
  conversations: ConversationSummary[];
  onSelectConversation: (id: string) => void;
}) {
  if (conversations.length === 0) {
    return <p className="muted small">No conversations yet. Ask the agent something in the AI tab.</p>;
  }
  return (
    <table className="activity-table">
      <thead>
        <tr>
          <th>First question</th>
          <th className="num">Runs</th>
          <th className="num">Cost</th>
          <th className="num">Tokens (in / out)</th>
          <th>Last active</th>
        </tr>
      </thead>
      <tbody>
        {conversations.map((c) => (
          <tr key={c.conversation_id} onClick={() => onSelectConversation(c.conversation_id)}>
            <td>{c.first_question ?? <span className="muted small">(no first question)</span>}</td>
            <td className="num">{c.run_count}</td>
            <td className="num">{dollars(c.total_cost_micros)}</td>
            <td className="num muted small">
              {c.total_prompt_tokens.toLocaleString()} / {c.total_completion_tokens.toLocaleString()}
            </td>
            <td className="muted small">{fmtTimestamp(c.last_active_at)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function RunsTable({
  runs,
  onSelectRun
}: {
  runs: RunSummary[];
  onSelectRun: (id: string) => void;
}) {
  if (runs.length === 0) {
    return <p className="muted small">No completed runs yet.</p>;
  }
  return (
    <table className="activity-table">
      <thead>
        <tr>
          <th>When</th>
          <th>Status</th>
          <th>Model</th>
          <th className="num">Tokens (in / out)</th>
          <th className="num">Cost</th>
          <th>Run id</th>
        </tr>
      </thead>
      <tbody>
        {runs.map((r) => (
          <tr key={r.run_id} onClick={() => onSelectRun(r.run_id)}>
            <td className="muted small">{fmtTimestamp(r.occurred_at)}</td>
            <td>
              <span className={`status-pill status-${r.status ?? "unknown"}`}>{r.status ?? "—"}</span>
            </td>
            <td className="muted small"><code>{r.model ?? "—"}</code></td>
            <td className="num muted small">
              {(r.prompt_tokens ?? 0).toLocaleString()} / {(r.completion_tokens ?? 0).toLocaleString()}
            </td>
            <td className="num">{dollars(r.cost_micros)}</td>
            <td className="muted small"><code className="chat-details-id">{r.run_id.slice(0, 8)}…</code></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function RunEventsDrawer({
  runId,
  apiBaseUrl,
  onClose
}: {
  runId: string;
  apiBaseUrl: string;
  onClose: () => void;
}) {
  const [events, setEvents] = React.useState<AgentEventRecord[] | null>(null);
  const [err, setErr] = React.useState<string | null>(null);

  React.useEffect(() => {
    void (async () => {
      try {
        const data = await fetchJson<AgentEventRecord[]>(
          `${apiBaseUrl}/api/v1/audit/runs/${encodeURIComponent(runId)}/events`
        );
        setEvents(data);
      } catch (e) {
        setErr(e instanceof Error ? e.message : String(e));
      }
    })();
  }, [runId, apiBaseUrl]);

  return (
    <div className="chat-drawer-backdrop" onClick={onClose}>
      <aside className="chat-drawer activity-drawer" onClick={(e) => e.stopPropagation()}>
        <header className="chat-drawer-header">
          <h3>Run replay · <code className="chat-details-id">{runId.slice(0, 12)}…</code></h3>
          <button className="button ghost small-button" onClick={onClose}>Close</button>
        </header>
        {err ? <div className="chat-error">Error: {err}</div> : null}
        {!events && !err ? <p className="muted small">Loading…</p> : null}
        {events ? (
          <ol className="activity-event-list">
            {events.map((e) => (
              <li key={e.id} className={`activity-event kind-${e.event_kind}`}>
                <div className="activity-event-header">
                  <code className="activity-event-kind">{e.event_kind}</code>
                  <span className="muted small">
                    #{e.sequence} · {fmtTimestamp(e.occurred_at)}
                    {e.duration_ms != null ? ` · ${e.duration_ms} ms` : null}
                  </span>
                </div>
                {e.tool_name ? (
                  <p className="activity-event-meta">
                    tool: <code>{e.tool_name}</code> · {e.ok ? "ok" : <span className="chat-error">failed</span>}
                  </p>
                ) : null}
                {e.model && e.event_kind === "llm_call" ? (
                  <p className="activity-event-meta muted small">
                    {e.model} · {(e.prompt_tokens ?? 0).toLocaleString()} in / {(e.completion_tokens ?? 0).toLocaleString()} out · {dollars(e.cost_micros)}
                  </p>
                ) : null}
                {e.event_kind === "run_started" && e.user_message_excerpt ? (
                  <p className="activity-event-meta">"{e.user_message_excerpt}"</p>
                ) : null}
                {e.error_message ? (
                  <p className="chat-error">{e.error_message}</p>
                ) : null}
                <details>
                  <summary className="activity-event-summary">payload</summary>
                  <pre className="activity-event-payload">{prettyJson(e.payload_json)}</pre>
                </details>
              </li>
            ))}
          </ol>
        ) : null}
      </aside>
    </div>
  );
}

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Request failed (HTTP ${res.status})`);
  }
  return (await res.json()) as T;
}

function prettyJson(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}
