import React from "react";
import ReactDOM from "react-dom/client";
import { invoke } from "@tauri-apps/api/core";
import "./styles.css";

type ServiceStatus = {
  api_running: boolean;
  worker_running: boolean;
};

type Account = {
  id: string;
  name: string;
  currency_code: string;
};

type Transaction = {
  id: string;
  account_id: string;
  description: string;
  amount_cents: number;
  booked_at: string;
  source: string;
  classification_source: string;
  confidence: number;
  explanation: string;
  last_sync_at: string;
  import_id?: string | null;
};

type ImportStatus = {
  import_id: string;
  status: string;
  extraction_mode: string;
  effective_provider?: string | null;
  provider_attempts?: Array<{
    provider: string;
    attempt_no: number;
    outcome: string;
    error_code?: string | null;
    retry_decision?: string | null;
  }>;
  diagnostics?: Record<string, unknown>;
  summary: Record<string, unknown>;
  errors: string[];
  warnings: string[];
  review_required_count: number;
};
type ExtractionSettings = {
  default_extraction_mode: "managed" | "local_ocr";
  managed_fallback_enabled: boolean;
  max_provider_retries: number;
  provider_timeout_ms: number;
};

const DEFAULT_EXTRACTION_SETTINGS: ExtractionSettings = {
  default_extraction_mode: "managed",
  managed_fallback_enabled: true,
  max_provider_retries: 3,
  provider_timeout_ms: 180000,
};

type ReviewRow = {
  row_id: string;
  row_index: number;
  normalized_json: { booked_at?: string; amount_cents?: number; description?: string };
  confidence: number;
  parse_error?: string | null;
  approved: boolean;
  rejection_reason?: string | null;
};

const IS_HTTP_CONTEXT =
  typeof window !== "undefined" &&
  (window.location.protocol === "http:" || window.location.protocol === "https:");
const API_BASE = IS_HTTP_CONTEXT ? "/api/v1" : "http://127.0.0.1:8081/api/v1";

function normalizeFetchError(err: unknown): string {
  const text = String(err);
  if (text.includes("TypeError: Load failed") || text.includes("Failed to fetch")) {
    return "Could not reach local API. Start services and retry.";
  }
  return text;
}

function App() {
  const [serviceStatus, setServiceStatus] = React.useState<ServiceStatus | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const [accounts, setAccounts] = React.useState<Account[]>([]);
  const [transactions, setTransactions] = React.useState<Transaction[]>([]);

  const [search, setSearch] = React.useState("");
  const [sourceFilter, setSourceFilter] = React.useState("all");
  const [accountFilter, setAccountFilter] = React.useState("all");

  const [fileName, setFileName] = React.useState("");
  const [fileBase64, setFileBase64] = React.useState("");
  const [parserType, setParserType] = React.useState("pdf");
  const [importModeOverride, setImportModeOverride] = React.useState("default");
  const [settings, setSettings] = React.useState<ExtractionSettings>(
    DEFAULT_EXTRACTION_SETTINGS
  );
  const [activeImportStatus, setActiveImportStatus] = React.useState<ImportStatus | null>(null);
  const [reviewRows, setReviewRows] = React.useState<ReviewRow[]>([]);

  async function runServiceAction(action: "start_services" | "stop_services" | "service_status") {
    try {
      setError(null);
      const next = await invoke<ServiceStatus>(action);
      setServiceStatus(next);
      if (next.api_running && (action === "start_services" || action === "service_status")) {
        await refreshData(next);
      }
    } catch (err) {
      setError(normalizeFetchError(err));
    }
  }

  async function refreshData(currentStatus?: ServiceStatus | null) {
    const status = currentStatus ?? serviceStatus;
    if (!status?.api_running) {
      setError("API is not running. Click Start Services.");
      return;
    }

    try {
      setError(null);
      const accountsRes = await fetch(`${API_BASE}/accounts`);
      if (accountsRes.ok) {
        setAccounts(await accountsRes.json());
      }
      const settingsRes = await fetch(`${API_BASE}/settings/extraction`);
      if (settingsRes.ok) {
        setSettings(await settingsRes.json());
      }

      const params = new URLSearchParams();
      if (search.trim()) params.set("q", search.trim());
      if (sourceFilter !== "all") params.set("source", sourceFilter);
      if (accountFilter !== "all") params.set("account_id", accountFilter);

      const txRes = await fetch(`${API_BASE}/transactions?${params.toString()}`);
      if (txRes.ok) {
        setTransactions(await txRes.json());
      }
    } catch (err) {
      setError(normalizeFetchError(err));
    }
  }

  async function handleFileChange(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (!file) return;

    setFileName(file.name);

    const arrayBuffer = await file.arrayBuffer();
    const bytes = new Uint8Array(arrayBuffer);
    let binary = "";
    for (let i = 0; i < bytes.length; i += 1) {
      binary += String.fromCharCode(bytes[i]);
    }
    setFileBase64(btoa(binary));
  }

  async function createImport() {
    if (!fileName || !fileBase64) {
      setError("Please select a statement file first.");
      return;
    }

    try {
      setError(null);
      const res = await fetch(`${API_BASE}/imports`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          file_name: fileName,
          parser_type: parserType,
          content_base64: fileBase64,
          extraction_mode:
            importModeOverride === "default" ? undefined : importModeOverride,
        }),
      });

      if (!res.ok) {
        throw new Error(await res.text());
      }

      const payload = (await res.json()) as { import_id: string };
      await refreshImportStatus(payload.import_id);
    } catch (err) {
      setError(normalizeFetchError(err));
    }
  }

  async function refreshImportStatus(importId: string) {
    try {
      const res = await fetch(`${API_BASE}/imports/${importId}/status`);
      if (!res.ok) {
        throw new Error(await res.text());
      }

      const payload = (await res.json()) as ImportStatus;
      setActiveImportStatus(payload);

      const reviewRes = await fetch(`${API_BASE}/imports/${importId}/review`);
      if (reviewRes.ok) {
        setReviewRows(await reviewRes.json());
      }

      await refreshData();
    } catch (err) {
      setError(normalizeFetchError(err));
    }
  }

  async function saveReviewDecisions() {
    if (!activeImportStatus) return;

    const decisions = reviewRows.map((row) => ({
      row_id: row.row_id,
      approved: row.approved,
      rejection_reason: row.rejection_reason ?? null,
    }));

    const res = await fetch(`${API_BASE}/imports/${activeImportStatus.import_id}/review`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ decisions }),
    });

    if (!res.ok) {
      setError(await res.text());
      return;
    }

    await refreshImportStatus(activeImportStatus.import_id);
  }

  async function commitImport() {
    if (!activeImportStatus) return;

    const res = await fetch(`${API_BASE}/imports/${activeImportStatus.import_id}/commit`, {
      method: "POST",
    });

    if (!res.ok) {
      setError(await res.text());
      return;
    }

    await refreshImportStatus(activeImportStatus.import_id);
    await refreshData();
  }

  async function saveExtractionSettings() {
    try {
      setError(null);
      const res = await fetch(`${API_BASE}/settings/extraction`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(settings),
      });
      if (!res.ok) {
        throw new Error(await res.text());
      }
      setSettings(await res.json());
    } catch (err) {
      setError(normalizeFetchError(err));
    }
  }

  React.useEffect(() => {
    void runServiceAction("service_status");
  }, []);

  React.useEffect(() => {
    if (!serviceStatus?.api_running || !activeImportStatus) {
      return;
    }

    const status = activeImportStatus.status.toLowerCase();
    if (status !== "queued" && status !== "parsing") {
      return;
    }

    const timer = window.setInterval(() => {
      void refreshImportStatus(activeImportStatus.import_id);
    }, 2000);

    return () => window.clearInterval(timer);
  }, [serviceStatus?.api_running, activeImportStatus?.import_id, activeImportStatus?.status]);

  return (
    <main className="app-shell">
      <header>
        <h1>Expense Tracker</h1>
        <p>Step 2 manual ingestion flow (PDF-first, Plaid deferred).</p>
      </header>

      <section className="panel">
        <h2>Service Control</h2>
        <div className="actions">
          <button onClick={() => void runServiceAction("start_services")}>Start Services</button>
          <button onClick={() => void runServiceAction("stop_services")}>Stop Services</button>
          <button onClick={() => void runServiceAction("service_status")}>Refresh Status</button>
          <button onClick={() => void refreshData()}>Refresh Data</button>
        </div>
        {serviceStatus && (
          <p>
            API: <strong>{serviceStatus.api_running ? "running" : "stopped"}</strong> | Worker: <strong>{serviceStatus.worker_running ? "running" : "stopped"}</strong>
          </p>
        )}
      </section>

      <section className="panel">
        <h2>Import Statement</h2>
        <div className="actions">
          <select
            value={settings.default_extraction_mode}
            onChange={(e) =>
              setSettings((prev) => ({
                ...prev,
                default_extraction_mode: e.target.value as "managed" | "local_ocr",
              }))
            }
          >
            <option value="managed">Default: Managed API</option>
            <option value="local_ocr">Default: Local OCR (stub)</option>
          </select>
          <button onClick={() => void saveExtractionSettings()}>Save Extraction Settings</button>
        </div>
        <div className="actions">
          <select value={parserType} onChange={(e) => setParserType(e.target.value)}>
            <option value="pdf">PDF</option>
            <option value="csv">CSV</option>
          </select>
          <select value={importModeOverride} onChange={(e) => setImportModeOverride(e.target.value)}>
            <option value="default">Use app default extraction mode</option>
            <option value="managed">Managed API</option>
            <option value="local_ocr">Local OCR (stub)</option>
          </select>
          <input type="file" onChange={handleFileChange} />
          <button onClick={() => void createImport()}>Create Import</button>
        </div>
        {activeImportStatus && (
          <div className="status-box">
            <p>Import ID: <code>{activeImportStatus.import_id}</code></p>
            <p>Status: <strong>{activeImportStatus.status}</strong></p>
            <p>Extraction mode: <strong>{activeImportStatus.extraction_mode}</strong></p>
            <p>Effective provider: <strong>{activeImportStatus.effective_provider ?? "-"}</strong></p>
            <p>Review required rows: {activeImportStatus.review_required_count}</p>
            {!!activeImportStatus.errors.length && <p className="error">Errors: {activeImportStatus.errors.join(" | ")}</p>}
            {!!activeImportStatus.warnings.length && <p>Warnings: {activeImportStatus.warnings.join(" | ")}</p>}
            {!!activeImportStatus.provider_attempts?.length && (
              <p>
                Attempts:{" "}
                {activeImportStatus.provider_attempts
                  .map((a) => `${a.provider}#${a.attempt_no}:${a.outcome}${a.error_code ? `(${a.error_code})` : ""}`)
                  .join(" | ")}
              </p>
            )}
            <div className="actions">
              <button onClick={() => void refreshImportStatus(activeImportStatus.import_id)}>Refresh Import</button>
              <button onClick={() => void saveReviewDecisions()}>Save Review</button>
              <button onClick={() => void commitImport()}>Commit Import</button>
            </div>
          </div>
        )}
      </section>

      {!!reviewRows.length && (
        <section className="panel">
          <h2>Review Rows</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>Date</th>
                  <th>Description</th>
                  <th>Amount</th>
                  <th>Confidence</th>
                  <th>Error</th>
                  <th>Approved</th>
                </tr>
              </thead>
              <tbody>
                {reviewRows.map((row, idx) => (
                  <tr key={row.row_id}>
                    <td>{row.row_index}</td>
                    <td>{row.normalized_json.booked_at ?? "-"}</td>
                    <td>{row.normalized_json.description ?? "-"}</td>
                    <td>{((row.normalized_json.amount_cents ?? 0) / 100).toFixed(2)}</td>
                    <td>{row.confidence.toFixed(2)}</td>
                    <td>{row.parse_error ?? "-"}</td>
                    <td>
                      <input
                        type="checkbox"
                        checked={row.approved}
                        onChange={(e) => {
                          const next = [...reviewRows];
                          next[idx] = { ...next[idx], approved: e.target.checked };
                          setReviewRows(next);
                        }}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <section className="panel">
        <h2>Transactions</h2>
        <div className="actions">
          <input
            placeholder="Search description"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <select value={sourceFilter} onChange={(e) => setSourceFilter(e.target.value)}>
            <option value="all">All sources</option>
            <option value="manual">Manual</option>
            <option value="plaid">Plaid</option>
          </select>
          <select value={accountFilter} onChange={(e) => setAccountFilter(e.target.value)}>
            <option value="all">All accounts</option>
            {accounts.map((account) => (
              <option key={account.id} value={account.id}>{account.name}</option>
            ))}
          </select>
          <button onClick={() => void refreshData()}>Apply Filters</button>
        </div>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Description</th>
                <th>Amount</th>
                <th>Source</th>
                <th>Confidence</th>
                <th>Import ID</th>
              </tr>
            </thead>
            <tbody>
              {transactions.map((tx) => (
                <tr key={tx.id}>
                  <td>{tx.booked_at}</td>
                  <td>{tx.description}</td>
                  <td>{(tx.amount_cents / 100).toFixed(2)}</td>
                  <td>{tx.source}</td>
                  <td>{tx.confidence.toFixed(2)}</td>
                  <td>{tx.import_id ?? "-"}</td>
                </tr>
              ))}
              {!transactions.length && (
                <tr>
                  <td colSpan={6}>No transactions yet.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      {error && <p className="error">Error: {error}</p>}
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
