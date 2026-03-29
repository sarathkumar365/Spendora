import React from "react";
import ReactDOM from "react-dom/client";
import { invoke } from "@tauri-apps/api/core";
import "./styles.css";

type ServiceStartupState = "starting" | "healthy" | "failed";
type AppSection = "ai" | "import" | "data";

type StartupStatus = {
  state: ServiceStartupState;
  phase: string;
  attempt: number;
  message?: string | null;
};

type ServiceStatus = {
  api_running: boolean;
  worker_running: boolean;
};

type AccountItem = {
  id: string;
  name: string;
  currency_code: string;
};

type StatementItem = {
  id: string;
  statement_month?: string | null;
  period_start: string;
  period_end: string;
  linked_txn_count: number;
};

type TransactionItem = {
  id: string;
  description: string;
  booked_at: string;
  amount_cents: number;
  source: string;
};

function resolveApiBaseUrl() {
  const configured = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim();
  if (configured) {
    return configured.replace(/\/+$/, "");
  }

  const isTauriRuntime =
    typeof window !== "undefined" &&
    Object.prototype.hasOwnProperty.call(window, "__TAURI_INTERNALS__");
  // Tauri production has no Vite dev proxy, so route directly to the local API.
  return isTauriRuntime ? "http://127.0.0.1:8081" : "";
}

const API_BASE_URL = resolveApiBaseUrl();

function apiUrl(path: string) {
  return API_BASE_URL ? `${API_BASE_URL}${path}` : path;
}

function phaseLabel(phase: string) {
  switch (phase) {
    case "api":
      return "Starting API";
    case "worker":
      return "Starting Worker";
    case "retry":
      return "Retrying startup";
    case "healthy":
      return "Services healthy";
    case "failed_terminal":
      return "Startup failed";
    default:
      return "Initializing";
  }
}

function formatMoney(cents: number) {
  return `$${(cents / 100).toFixed(2)}`;
}

function App() {
  const [section, setSection] = React.useState<AppSection>("ai");
  const [startup, setStartup] = React.useState<StartupStatus>({
    state: "starting",
    phase: "idle",
    attempt: 0,
    message: "Starting local services..."
  });

  const [accounts, setAccounts] = React.useState<AccountItem[]>([]);
  const [accountId, setAccountId] = React.useState<string>("");
  const [statements, setStatements] = React.useState<StatementItem[]>([]);
  const [selectedStatementId, setSelectedStatementId] = React.useState<string>("");
  const [statementTransactions, setStatementTransactions] = React.useState<TransactionItem[]>([]);

  React.useEffect(() => {
    let mounted = true;
    const loadStatus = async () => {
      try {
        const status = await invoke<StartupStatus>("startup_status");
        if (mounted) {
          setStartup(status);
        }
      } catch (error) {
        if (mounted) {
          setStartup({
            state: "failed",
            phase: "failed_terminal",
            attempt: 0,
            message: `Unable to read startup status: ${String(error)}`
          });
        }
      }
    };

    void loadStatus();
    const timer = window.setInterval(loadStatus, 800);
    return () => {
      mounted = false;
      window.clearInterval(timer);
    };
  }, []);

  const retryStartup = React.useCallback(async () => {
    setStartup({
      state: "starting",
      phase: "retry",
      attempt: 1,
      message: "Retrying local services startup..."
    });
    try {
      const result = await invoke<ServiceStatus>("start_services");
      if (!result.api_running || !result.worker_running) {
        throw new Error("Services reported not running after retry.");
      }
      setStartup({
        state: "healthy",
        phase: "healthy",
        attempt: 1,
        message: "Services are healthy."
      });
    } catch (error) {
      setStartup({
        state: "failed",
        phase: "failed_terminal",
        attempt: 1,
        message: String(error)
      });
    }
  }, []);

  React.useEffect(() => {
    if (startup.state !== "healthy") {
      return;
    }
    let mounted = true;
    const run = async () => {
      try {
        const response = await fetch(apiUrl("/api/v1/accounts"));
        if (!response.ok) {
          throw new Error(`accounts request failed: ${response.status}`);
        }
        const payload = (await response.json()) as AccountItem[];
        if (!mounted) {
          return;
        }
        setAccounts(payload);
        if (payload.length > 0) {
          setAccountId((current) => current || payload[0].id);
        }
      } catch {
        if (mounted) {
          setAccounts([]);
        }
      }
    };
    void run();
    return () => {
      mounted = false;
    };
  }, [startup.state]);

  React.useEffect(() => {
    if (startup.state !== "healthy" || !accountId) {
      return;
    }
    let mounted = true;
    const run = async () => {
      try {
        const response = await fetch(
          apiUrl(`/api/v1/statements?account_id=${encodeURIComponent(accountId)}`)
        );
        if (!response.ok) {
          throw new Error(`statements request failed: ${response.status}`);
        }
        const payload = (await response.json()) as StatementItem[];
        if (!mounted) {
          return;
        }
        setStatements(payload);
        if (payload.length === 0) {
          setSelectedStatementId("");
          setStatementTransactions([]);
          return;
        }
        setSelectedStatementId((current) => {
          if (current && payload.some((item) => item.id === current)) {
            return current;
          }
          return payload[0].id;
        });
      } catch {
        if (mounted) {
          setStatements([]);
          setSelectedStatementId("");
          setStatementTransactions([]);
        }
      }
    };
    void run();
    return () => {
      mounted = false;
    };
  }, [accountId, startup.state]);

  React.useEffect(() => {
    if (startup.state !== "healthy" || !selectedStatementId) {
      return;
    }
    let mounted = true;
    const run = async () => {
      try {
        const response = await fetch(
          apiUrl(`/api/v1/statements/${encodeURIComponent(selectedStatementId)}/transactions`)
        );
        if (!response.ok) {
          throw new Error(`transactions request failed: ${response.status}`);
        }
        const payload = (await response.json()) as TransactionItem[];
        if (mounted) {
          setStatementTransactions(payload);
        }
      } catch {
        if (mounted) {
          setStatementTransactions([]);
        }
      }
    };
    void run();
    return () => {
      mounted = false;
    };
  }, [selectedStatementId, startup.state]);

  if (startup.state !== "healthy") {
    return (
      <main className="screen">
        <section className="panel gate">
          <p className="eyebrow">Spendora Desktop</p>
          <h1>Starting local services...</h1>
          <p className="muted">
            {phaseLabel(startup.phase)}
            {startup.attempt > 0 ? ` · Attempt ${startup.attempt}` : ""}
          </p>
          {startup.message ? <pre className="status-message">{startup.message}</pre> : null}
          {startup.state === "failed" ? (
            <button className="button" onClick={() => void retryStartup()}>
              Retry Startup
            </button>
          ) : (
            <div className="spinner" aria-label="starting" />
          )}
        </section>
      </main>
    );
  }

  return (
    <main className="screen">
      <div className="app-shell">
        <header className="panel topbar">
          <h1>Spendora</h1>
          <nav>
            <button
              className={section === "ai" ? "button active" : "button ghost"}
              onClick={() => setSection("ai")}
            >
              AI Interaction
            </button>
            <button
              className={section === "import" ? "button active" : "button ghost"}
              onClick={() => setSection("import")}
            >
              Import
            </button>
            <button
              className={section === "data" ? "button active" : "button ghost"}
              onClick={() => setSection("data")}
            >
              View Your Data
            </button>
          </nav>
        </header>

        {section === "ai" ? (
          <section className="panel page">
            <p className="eyebrow">AI-First Home</p>
            <h2>Copilot workspace (stub)</h2>
            <p className="muted">
              This will become the default intelligence workspace for insights and guidance from
              your saved financial data.
            </p>
          </section>
        ) : null}

        {section === "import" ? (
          <section className="panel page">
            <p className="eyebrow">Import</p>
            <h2>Import pipeline</h2>
            <p className="muted">
              Import remains available as a supporting workflow. The primary product direction is
              insight delivery from existing data.
            </p>
          </section>
        ) : null}

        {section === "data" ? (
          <section className="panel page">
            <p className="eyebrow">Saved Statements</p>
            <h2>View your data</h2>

            <label className="field">
              <span>Account</span>
              <select
                value={accountId}
                onChange={(event) => setAccountId(event.target.value)}
                className="input"
              >
                {accounts.map((account) => (
                  <option key={account.id} value={account.id}>
                    {account.name} ({account.currency_code})
                  </option>
                ))}
              </select>
            </label>

            <div className="split">
              <div className="panel inset">
                <h3>Statements</h3>
                {statements.length === 0 ? (
                  <p className="muted">No statements available.</p>
                ) : (
                  <ul className="list">
                    {statements.map((statement) => (
                      <li key={statement.id}>
                        <button
                          className={
                            selectedStatementId === statement.id
                              ? "button list-item active"
                              : "button list-item ghost"
                          }
                          onClick={() => setSelectedStatementId(statement.id)}
                        >
                          <span>{statement.statement_month || statement.period_start.slice(0, 7)}</span>
                          <small>{statement.linked_txn_count} txns</small>
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
              </div>

              <div className="panel inset">
                <h3>Transactions</h3>
                {statementTransactions.length === 0 ? (
                  <p className="muted">No transactions for selected statement.</p>
                ) : (
                  <ul className="list">
                    {statementTransactions.map((transaction) => (
                      <li key={transaction.id} className="txn-row">
                        <div>
                          <strong>{transaction.description}</strong>
                          <small>{transaction.booked_at}</small>
                        </div>
                        <div className="amount">{formatMoney(transaction.amount_cents)}</div>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          </section>
        ) : null}
      </div>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
