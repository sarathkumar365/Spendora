import React from "react";
import ReactDOM from "react-dom/client";
import { invoke } from "@tauri-apps/api/core";
import { Settings2 } from "lucide-react";
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

type CoverageSelected = {
  year: number;
  month: number;
  reusable: boolean;
  statement_exists: boolean;
  has_linked_txns: boolean;
  has_manual_added_txns_only: boolean;
  policy_note: string;
  statement_id?: string | null;
  statement_month?: string | null;
  period_start?: string | null;
  period_end?: string | null;
  linked_txn_count: number;
  manual_added_txn_count: number;
};

type CoverageResponse = {
  account_id: string;
  years: Array<{
    year: number;
    months: Array<{
      month: number;
      statement_exists: boolean;
      statement_id?: string | null;
      statement_month?: string | null;
      period_start?: string | null;
      period_end?: string | null;
      linked_txn_count: number;
      manual_added_txn_count: number;
    }>;
  }>;
  selected?: CoverageSelected | null;
};

type ImportStatus =
  | "queued"
  | "parsing"
  | "review_required"
  | "ready_to_commit"
  | "committed"
  | "failed";

type ImportStatusEnvelope = {
  import_id: string;
  status: ImportStatus;
  extraction_mode: string;
  effective_provider?: string | null;
  provider_attempts: unknown[];
  diagnostics: Record<string, unknown>;
  summary: Record<string, unknown>;
  errors: string[];
  warnings: string[];
  review_required_count: number;
};

type ReviewRow = {
  row_id: string;
  row_index: number;
  normalized_json: {
    booked_at?: string;
    description?: string;
    amount_cents?: number;
    [key: string]: unknown;
  };
  confidence: number;
  parse_error?: string | null;
  approved: boolean;
  rejection_reason?: string | null;
};

type CommitResult = {
  inserted_count: number;
  duplicate_count: number;
};

type CreateImportResponse = {
  import_id: string;
  status: string;
  reused: boolean;
};

type ImportStage = "idle" | "polling" | "results" | "failed";

function resolveApiBaseUrl() {
  const configured = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim();
  if (configured) {
    return configured.replace(/\/+$/, "");
  }

  const isTauriRuntime =
    typeof window !== "undefined" &&
    Object.prototype.hasOwnProperty.call(window, "__TAURI_INTERNALS__");
  return isTauriRuntime ? "http://127.0.0.1:8081" : "";
}

const API_BASE_URL = resolveApiBaseUrl();

function apiUrl(path: string) {
  return API_BASE_URL ? `${API_BASE_URL}${path}` : path;
}

async function apiFetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(apiUrl(path), init);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `request failed (${response.status})`);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  const text = await response.text();
  if (!text.trim()) {
    return undefined as T;
  }
  return JSON.parse(text) as T;
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

function importStatusTitle(status: ImportStatus | null) {
  switch (status) {
    case "queued":
      return "Queued";
    case "parsing":
      return "Extracting transactions";
    case "review_required":
      return "Review required";
    case "ready_to_commit":
      return "Ready to commit";
    case "committed":
      return "Committed";
    case "failed":
      return "Import failed";
    default:
      return "Preparing import";
  }
}

function importStatusMessage(status: ImportStatusEnvelope | null) {
  if (!status) {
    return "Preparing extraction...";
  }
  if (status.status === "queued") {
    return "Your file is queued for the worker to process.";
  }
  if (status.status === "parsing") {
    const mode = typeof status.diagnostics?.managed_flow_mode === "string" ? status.diagnostics.managed_flow_mode : status.extraction_mode;
    return `Parsing statement in ${String(mode)} mode...`;
  }
  if (status.status === "review_required") {
    return "Extraction finished. Please review flagged rows before commit.";
  }
  if (status.status === "ready_to_commit") {
    return "Extraction finished. Ready to commit reviewed rows.";
  }
  if (status.status === "committed") {
    return "Import committed successfully.";
  }
  const firstError = status.errors[0];
  return firstError || "Import failed. Check diagnostics below.";
}

function isTerminalImportStatus(status: ImportStatus) {
  return status === "review_required" || status === "ready_to_commit" || status === "failed" || status === "committed";
}

function monthLabel(month: number) {
  return new Date(2000, month - 1, 1).toLocaleString(undefined, { month: "long" });
}

function formatMoney(cents: number) {
  return `$${(cents / 100).toFixed(2)}`;
}

function inferParserType(fileName: string) {
  return fileName.toLowerCase().endsWith(".csv") ? "csv" : "pdf";
}

function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = reader.result;
      if (typeof result !== "string") {
        reject(new Error("failed to read file"));
        return;
      }
      const base64 = result.includes(",") ? result.split(",", 2)[1] : result;
      resolve(base64);
    };
    reader.onerror = () => reject(new Error("failed to read file"));
    reader.readAsDataURL(file);
  });
}

function toStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((item): item is string => typeof item === "string");
}

export function App() {
  const isProductionBuild = import.meta.env.PROD;
  const [section, setSection] = React.useState<AppSection>("ai");
  const [startup, setStartup] = React.useState<StartupStatus>({
    state: "starting",
    phase: "idle",
    attempt: 0,
    message: "Starting local services..."
  });

  const [accountId, setAccountId] = React.useState<string>("");
  const [accountsLoading, setAccountsLoading] = React.useState(false);
  const [accountLoadError, setAccountLoadError] = React.useState<string | null>(null);
  const [accountRetryKey, setAccountRetryKey] = React.useState(0);

  const [statements, setStatements] = React.useState<StatementItem[]>([]);
  const [selectedStatementId, setSelectedStatementId] = React.useState<string>("");
  const [statementTransactions, setStatementTransactions] = React.useState<TransactionItem[]>([]);
  const [dataYearFilter, setDataYearFilter] = React.useState<number | null>(null);
  const [dataMonthFilter, setDataMonthFilter] = React.useState<number | null>(null);

  const [isImportOptionsOpen, setIsImportOptionsOpen] = React.useState(false);
  const [importYear, setImportYear] = React.useState<number | null>(null);
  const [importMonth, setImportMonth] = React.useState<number | null>(null);
  const [coverage, setCoverage] = React.useState<CoverageSelected | null>(null);
  const [coverageLoading, setCoverageLoading] = React.useState(false);
  const [coverageError, setCoverageError] = React.useState<string | null>(null);

  const [selectedFile, setSelectedFile] = React.useState<File | null>(null);
  const [importStage, setImportStage] = React.useState<ImportStage>("idle");
  const [activeImportId, setActiveImportId] = React.useState<string | null>(null);
  const [activeImportStatus, setActiveImportStatus] = React.useState<ImportStatusEnvelope | null>(null);
  const [reviewRows, setReviewRows] = React.useState<ReviewRow[]>([]);
  const [commitResult, setCommitResult] = React.useState<CommitResult | null>(null);
  const [importError, setImportError] = React.useState<string | null>(null);
  const [isSubmittingImport, setIsSubmittingImport] = React.useState(false);
  const [isSavingReview, setIsSavingReview] = React.useState(false);
  const [isCommittingImport, setIsCommittingImport] = React.useState(false);

  const coverageExists = coverage?.statement_exists ?? false;
  const selectedMonthToken = importYear !== null && importMonth !== null ? `${importYear}-${String(importMonth).padStart(2, "0")}` : null;

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

  const loadSingleAccount = React.useCallback(async () => {
    setAccountsLoading(true);
    setAccountLoadError(null);
    try {
      const payload = await apiFetchJson<AccountItem[]>("/api/v1/accounts");
      if (payload.length === 0) {
        setAccountId("");
        setAccountLoadError("No account available. Retry after API bootstrap finishes.");
        return;
      }
      setAccountId(payload[0].id);
    } catch (error) {
      setAccountId("");
      setAccountLoadError(String(error));
    } finally {
      setAccountsLoading(false);
    }
  }, []);

  React.useEffect(() => {
    if (startup.state !== "healthy") {
      return;
    }
    void loadSingleAccount();
  }, [startup.state, accountRetryKey, loadSingleAccount]);

  React.useEffect(() => {
    if (startup.state !== "healthy" || !accountId) {
      return;
    }
    if (importYear === null || importMonth === null) {
      setCoverage(null);
      setCoverageError(null);
      return;
    }

    let mounted = true;
    const run = async () => {
      setCoverageLoading(true);
      setCoverageError(null);
      try {
        const params = new URLSearchParams({
          account_id: accountId,
          year: String(importYear),
          month: String(importMonth)
        });
        const payload = await apiFetchJson<CoverageResponse>(`/api/v1/statements/coverage?${params.toString()}`);
        if (!mounted) {
          return;
        }
        setCoverage(payload.selected ?? null);
      } catch (error) {
        if (mounted) {
          setCoverage(null);
          setCoverageError(String(error));
        }
      } finally {
        if (mounted) {
          setCoverageLoading(false);
        }
      }
    };

    void run();
    return () => {
      mounted = false;
    };
  }, [startup.state, accountId, importYear, importMonth]);

  const fetchStatements = React.useCallback(async () => {
    if (startup.state !== "healthy" || !accountId) {
      return;
    }

    const params = new URLSearchParams({ account_id: accountId });
    if (dataYearFilter !== null) {
      params.set("year", String(dataYearFilter));
    }
    if (dataMonthFilter !== null) {
      params.set("month", String(dataMonthFilter));
    }

    try {
      const payload = await apiFetchJson<StatementItem[]>(`/api/v1/statements?${params.toString()}`);
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
      setStatements([]);
      setSelectedStatementId("");
      setStatementTransactions([]);
    }
  }, [accountId, dataMonthFilter, dataYearFilter, startup.state]);

  React.useEffect(() => {
    if (section !== "data") {
      return;
    }
    void fetchStatements();
  }, [section, fetchStatements]);

  React.useEffect(() => {
    if (section !== "data" || startup.state !== "healthy" || !selectedStatementId) {
      return;
    }
    let mounted = true;

    const run = async () => {
      try {
        const payload = await apiFetchJson<TransactionItem[]>(
          `/api/v1/statements/${encodeURIComponent(selectedStatementId)}/transactions`
        );
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
  }, [section, selectedStatementId, startup.state]);

  const loadReviewRows = React.useCallback(async (importId: string) => {
    const rows = await apiFetchJson<ReviewRow[]>(`/api/v1/imports/${encodeURIComponent(importId)}/review`);
    setReviewRows(rows);
  }, []);

  const refreshImportStatus = React.useCallback(
    async (importId: string) => {
      const status = await apiFetchJson<ImportStatusEnvelope>(
        `/api/v1/imports/${encodeURIComponent(importId)}/status`
      );
      setActiveImportStatus(status);

      if (status.status === "review_required" || status.status === "ready_to_commit") {
        await loadReviewRows(importId);
        setImportStage("results");
      } else if (status.status === "failed") {
        setImportStage("failed");
        setImportError(status.errors[0] ?? "Import failed.");
      } else if (status.status === "committed") {
        setImportStage("results");
      }

      return status;
    },
    [loadReviewRows]
  );

  React.useEffect(() => {
    if (!activeImportId || importStage !== "polling") {
      return;
    }

    let cancelled = false;
    const poll = async () => {
      try {
        const status = await refreshImportStatus(activeImportId);
        if (!cancelled && isTerminalImportStatus(status.status)) {
          return;
        }
      } catch (error) {
        if (!cancelled) {
          setImportStage("failed");
          setImportError(String(error));
        }
      }
    };

    void poll();
    const timer = window.setInterval(() => {
      void poll();
    }, 1000);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [activeImportId, importStage, refreshImportStatus]);

  const resetImportSession = React.useCallback(() => {
    setImportStage("idle");
    setActiveImportId(null);
    setActiveImportStatus(null);
    setReviewRows([]);
    setCommitResult(null);
    setImportError(null);
    setSelectedFile(null);
    setIsSavingReview(false);
    setIsCommittingImport(false);
  }, []);

  const openViewDataWithMonthContext = React.useCallback(() => {
    setSection("data");
    if (importYear !== null && importMonth !== null) {
      setDataYearFilter(importYear);
      setDataMonthFilter(importMonth);
    }
  }, [importMonth, importYear]);

  const handleCreateImport = React.useCallback(async () => {
    if (!selectedFile || !accountId) {
      return;
    }
    if (coverageExists) {
      return;
    }

    setIsSubmittingImport(true);
    setImportError(null);
    setCommitResult(null);

    try {
      const contentBase64 = await fileToBase64(selectedFile);
      const includeMonthContext = importYear !== null && importMonth !== null;

      const payload: Record<string, unknown> = {
        file_name: selectedFile.name,
        parser_type: inferParserType(selectedFile.name),
        content_base64: contentBase64,
        extraction_mode: "managed"
      };

      if (includeMonthContext) {
        payload.account_id = accountId;
        payload.year = importYear;
        payload.month = importMonth;
      }

      const created = await apiFetchJson<CreateImportResponse>("/api/v1/imports", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });

      setActiveImportId(created.import_id);
      if (created.reused) {
        setImportStage("idle");
        await openViewDataWithMonthContext();
        return;
      }
      setImportStage("polling");
    } catch (error) {
      setImportStage("failed");
      setImportError(String(error));
    } finally {
      setIsSubmittingImport(false);
    }
  }, [accountId, coverageExists, importMonth, importYear, openViewDataWithMonthContext, selectedFile]);

  const saveReviewDecisions = React.useCallback(async () => {
    if (!activeImportId) {
      return;
    }

    setIsSavingReview(true);
    setImportError(null);
    try {
      await apiFetchJson<unknown>(`/api/v1/imports/${encodeURIComponent(activeImportId)}/review`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          decisions: reviewRows.map((row) => ({
            row_id: row.row_id,
            approved: row.approved,
            rejection_reason: row.approved ? null : row.rejection_reason || null
          }))
        })
      });
      await refreshImportStatus(activeImportId);
    } catch (error) {
      setImportError(String(error));
    } finally {
      setIsSavingReview(false);
    }
  }, [activeImportId, refreshImportStatus, reviewRows]);

  const commitImport = React.useCallback(async () => {
    if (!activeImportId) {
      return;
    }

    setIsCommittingImport(true);
    setImportError(null);
    try {
      const result = await apiFetchJson<CommitResult>(`/api/v1/imports/${encodeURIComponent(activeImportId)}/commit`, {
        method: "POST"
      });
      setCommitResult(result);
      await openViewDataWithMonthContext();
    } catch (error) {
      setImportError(String(error));
    } finally {
      setIsCommittingImport(false);
    }
  }, [activeImportId, openViewDataWithMonthContext]);

  if (startup.state !== "healthy") {
    if (isProductionBuild) {
      return (
        <main className="screen">
          <section className="panel gate brand-startup-gate">
            <h1 className="brand-startup-title">Spendora</h1>
            <div className="loading-line" aria-label="loading" />
            {startup.state === "failed" ? (
              <button className="button" onClick={() => void retryStartup()}>
                Retry
              </button>
            ) : null}
          </section>
        </main>
      );
    }

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

  if (!accountsLoading && !accountId) {
    return (
      <main className="screen">
        <section className="panel gate" data-testid="no-account-state">
          <p className="eyebrow">Spendora Desktop</p>
          <h1>No account available</h1>
          <p className="muted">{accountLoadError || "Account bootstrap is still in progress."}</p>
          <button className="button" onClick={() => setAccountRetryKey((v) => v + 1)}>
            Retry
          </button>
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
          <section className="panel page import-page" data-testid="import-page">
            <div className="import-header">
              <div>
                <p className="eyebrow">Import</p>
                <h2>Upload your statement</h2>
              </div>
              <button
                className="icon-button"
                aria-label="toggle import options"
                onClick={() => setIsImportOptionsOpen((value) => !value)}
              >
                <Settings2 size={18} />
              </button>
            </div>

            {isImportOptionsOpen ? (
              <section className="panel import-options" data-testid="import-options-panel">
                <label className="field compact">
                  <span>Year</span>
                  <select
                    className="input"
                    value={importYear ?? ""}
                    onChange={(event) => {
                      const next = Number(event.target.value);
                      setImportYear(Number.isNaN(next) || next <= 0 ? null : next);
                    }}
                  >
                    <option value="">Select year</option>
                    {Array.from({ length: 7 }).map((_, idx) => {
                      const year = new Date().getFullYear() - idx;
                      return (
                        <option key={year} value={year}>
                          {year}
                        </option>
                      );
                    })}
                  </select>
                </label>
                <label className="field compact">
                  <span>Month</span>
                  <select
                    className="input"
                    value={importMonth ?? ""}
                    onChange={(event) => {
                      const next = Number(event.target.value);
                      setImportMonth(Number.isNaN(next) || next <= 0 ? null : next);
                    }}
                  >
                    <option value="">Select month</option>
                    {Array.from({ length: 12 }).map((_, idx) => {
                      const month = idx + 1;
                      return (
                        <option key={month} value={month}>
                          {monthLabel(month)}
                        </option>
                      );
                    })}
                  </select>
                </label>
              </section>
            ) : null}

            {coverageLoading ? <p className="muted">Checking month coverage...</p> : null}
            {coverageError ? <p className="error-text">{coverageError}</p> : null}

            {coverageExists ? (
              <section className="panel reuse-banner" data-testid="coverage-hit-banner">
                <h3>Statement already exists for {selectedMonthToken}</h3>
                <p className="muted">
                  {coverage?.policy_note || "This month has statement coverage. Reuse existing data."}
                </p>
                <button className="button" onClick={() => void openViewDataWithMonthContext()}>
                  Show me statements
                </button>
              </section>
            ) : null}

            {importStage === "polling" ? (
              <div className="import-center-status" data-testid="import-polling-stage">
                <div className="spinner" aria-label="import-progress" />
                <h3>{importStatusTitle(activeImportStatus?.status ?? null)}</h3>
                <p className="muted">{importStatusMessage(activeImportStatus)}</p>
              </div>
            ) : null}

            {importStage === "idle" && !coverageExists ? (
              <div className="import-idle-center">
                <div className="upload-blob" data-testid="upload-blob">
                  <h3>Upload</h3>
                  <input
                    data-testid="import-file-input"
                    id="import-file"
                    type="file"
                    accept=".pdf,.csv"
                    className="hidden-input"
                    onChange={(event) => {
                      const file = event.target.files?.[0] ?? null;
                      setSelectedFile(file);
                      setImportError(null);
                    }}
                  />
                  <div className="upload-actions">
                    <label className="button ghost" htmlFor="import-file">
                      Choose File
                    </label>
                    {selectedFile ? (
                      <button
                        className="button"
                        disabled={coverageExists || isSubmittingImport || coverageLoading || !accountId}
                        onClick={() => void handleCreateImport()}
                      >
                        {isSubmittingImport ? "Starting..." : "Start Extraction"}
                      </button>
                    ) : null}
                  </div>
                  <p className="muted upload-file-name">{selectedFile ? selectedFile.name : "No file selected"}</p>
                  {importError ? <p className="error-text">{importError}</p> : null}
                </div>
              </div>
            ) : null}

            {importStage === "failed" ? (
              <section className="panel import-failed" data-testid="import-failed-stage">
                <h3>Import failed</h3>
                <p className="muted">{importError || importStatusMessage(activeImportStatus)}</p>
                {activeImportStatus?.errors?.length ? (
                  <ul className="list compact-list">
                    {activeImportStatus.errors.map((error) => (
                      <li key={error}>{error}</li>
                    ))}
                  </ul>
                ) : null}
                <button className="button" onClick={resetImportSession}>
                  Back to upload
                </button>
              </section>
            ) : null}

            {importStage === "results" ? (
              <section className="results-stack" data-testid="import-results-stage">
                <article className="panel results-summary" data-testid="results-summary">
                  <p className="eyebrow">Import Summary</p>
                  <h3>{importStatusTitle(activeImportStatus?.status ?? null)}</h3>
                  <div className="summary-grid">
                    <div>
                      <span>Parsed Rows</span>
                      <strong>{Number(activeImportStatus?.summary?.parsed_rows ?? 0)}</strong>
                    </div>
                    <div>
                      <span>Review Required</span>
                      <strong>{activeImportStatus?.review_required_count ?? 0}</strong>
                    </div>
                    <div>
                      <span>Warnings</span>
                      <strong>{activeImportStatus?.warnings?.length ?? 0}</strong>
                    </div>
                    <div>
                      <span>Errors</span>
                      <strong>{activeImportStatus?.errors?.length ?? 0}</strong>
                    </div>
                    <div>
                      <span>Provider</span>
                      <strong>{activeImportStatus?.effective_provider || "n/a"}</strong>
                    </div>
                    <div>
                      <span>Mode</span>
                      <strong>{activeImportStatus?.extraction_mode || "managed"}</strong>
                    </div>
                  </div>

                  {commitResult ? (
                    <p className="success-text" data-testid="commit-summary">
                      Committed. Inserted {commitResult.inserted_count} · Duplicates {commitResult.duplicate_count}
                    </p>
                  ) : null}

                  {toStringArray(activeImportStatus?.warnings).length ? (
                    <div className="result-messages">
                      <h4>Warnings</h4>
                      <ul className="list compact-list">
                        {toStringArray(activeImportStatus?.warnings).map((warning) => (
                          <li key={warning}>{warning}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}

                  {toStringArray(activeImportStatus?.errors).length ? (
                    <div className="result-messages">
                      <h4>Errors</h4>
                      <ul className="list compact-list">
                        {toStringArray(activeImportStatus?.errors).map((error) => (
                          <li key={error}>{error}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}

                  {importError ? <p className="error-text">{importError}</p> : null}

                  <div className="upload-actions">
                    <button className="button ghost" onClick={resetImportSession}>
                      Create New Import
                    </button>
                    <button className="button ghost" onClick={() => void saveReviewDecisions()} disabled={isSavingReview}>
                      {isSavingReview ? "Saving..." : "Save Review Decisions"}
                    </button>
                    <button
                      className="button"
                      onClick={() => void commitImport()}
                      disabled={isCommittingImport || activeImportStatus?.status === "failed"}
                    >
                      {isCommittingImport ? "Committing..." : "Commit Import"}
                    </button>
                  </div>
                </article>

                <article className="panel results-rows" data-testid="results-rows">
                  <h3>Transactions</h3>
                  {reviewRows.length === 0 ? (
                    <p className="muted">No review rows available.</p>
                  ) : (
                    <ul className="list">
                      {reviewRows.map((row) => (
                        <li key={row.row_id} className="review-row">
                          <div className="review-row-main">
                            <strong>
                              #{row.row_index} · {row.normalized_json.description || "(no description)"}
                            </strong>
                            <small>
                              {row.normalized_json.booked_at || "(no date)"} · {formatMoney(Number(row.normalized_json.amount_cents || 0))} · confidence {row.confidence.toFixed(2)}
                            </small>
                            {row.parse_error ? <small className="error-text">Parse error: {row.parse_error}</small> : null}
                          </div>

                          <div className="review-actions">
                            <label className="review-approve-label">
                              <input
                                className="review-checkbox"
                                type="checkbox"
                                checked={row.approved}
                                onChange={(event) => {
                                  const approved = event.target.checked;
                                  setReviewRows((current) =>
                                    current.map((item) =>
                                      item.row_id === row.row_id
                                        ? {
                                            ...item,
                                            approved,
                                            rejection_reason: approved ? null : item.rejection_reason
                                          }
                                        : item
                                    )
                                  );
                                }}
                              />
                              <span>Approve</span>
                            </label>
                            {!row.approved ? (
                              <input
                                className="input"
                                placeholder="Rejection reason"
                                value={row.rejection_reason || ""}
                                onChange={(event) => {
                                  const reason = event.target.value;
                                  setReviewRows((current) =>
                                    current.map((item) =>
                                      item.row_id === row.row_id
                                        ? {
                                            ...item,
                                            rejection_reason: reason
                                          }
                                        : item
                                    )
                                  );
                                }}
                              />
                            ) : null}
                          </div>
                        </li>
                      ))}
                    </ul>
                  )}
                </article>
              </section>
            ) : null}
          </section>
        ) : null}

        {section === "data" ? (
          <section className="panel page view-data-page" data-testid="view-data-page">
            <p className="eyebrow">Saved Statements</p>
            <h2>View your data</h2>
            {dataYearFilter !== null && dataMonthFilter !== null ? (
              <p className="muted">
                Showing {dataYearFilter}-{String(dataMonthFilter).padStart(2, "0")}
              </p>
            ) : null}

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

const rootElement = document.getElementById("root");
if (rootElement && import.meta.env.MODE !== "test") {
  ReactDOM.createRoot(rootElement).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>
  );
}
