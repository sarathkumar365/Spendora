import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { vi, beforeEach, afterEach, describe, it, expect } from "vitest";

const invokeMock = vi.fn();
vi.mock("@tauri-apps/api/core", () => ({
  invoke: (...args: unknown[]) => invokeMock(...args)
}));

import { App } from "./main";

type FetchRoute = (url: string, init?: RequestInit) => Promise<Response>;

function jsonResponse(payload: unknown, status = 200) {
  return Promise.resolve(
    new Response(JSON.stringify(payload), {
      status,
      headers: { "Content-Type": "application/json" }
    })
  );
}

function toUrl(input: RequestInfo | URL) {
  return typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
}

function withDefaultRoutes(extra?: FetchRoute): FetchRoute {
  return async (url, init) => {
    const method = (init?.method || "GET").toUpperCase();

    if (url.includes("/api/v1/accounts") && method === "GET") {
      return jsonResponse([
        { id: "manual-default-account", name: "Manual Imported Account", currency_code: "CAD" }
      ]);
    }

    if (url.includes("/api/v1/statements/") && url.includes("/transactions") && method === "GET") {
      return jsonResponse([]);
    }

    if (url.includes("/api/v1/statements?") && method === "GET") {
      return jsonResponse([]);
    }

    if (url.includes("/api/v1/statements/coverage") && method === "GET") {
      return jsonResponse({ account_id: "manual-default-account", years: [], selected: null });
    }

    if (extra) {
      return extra(url, init);
    }

    return jsonResponse({ error: `unhandled route ${method} ${url}` }, 500);
  };
}

beforeEach(() => {
  invokeMock.mockReset();
  invokeMock.mockImplementation(async (command: string) => {
    if (command === "startup_status") {
      return { state: "healthy", phase: "healthy", attempt: 1, message: "ok" };
    }
    if (command === "start_services") {
      return { api_running: true, worker_running: true };
    }
    throw new Error(`Unhandled invoke ${command}`);
  });

  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    return withDefaultRoutes()(toUrl(input), init);
  });
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Import page revamp", () => {
  it("renders upload blob and toggles advanced month options", async () => {
    const user = userEvent.setup();
    render(<App />);

    await user.click(await screen.findByRole("button", { name: "Import" }));
    expect(screen.getByTestId("upload-blob")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "toggle import options" }));
    expect(screen.getByTestId("import-options-panel")).toBeInTheDocument();
  });

  it("checks coverage for selected month and blocks upload when statement exists", async () => {
    const user = userEvent.setup();
    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;

    fetchMock.mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = toUrl(input);
      const method = (init?.method || "GET").toUpperCase();

      if (url.includes("/api/v1/statements/coverage") && method === "GET") {
        return jsonResponse({
          account_id: "manual-default-account",
          years: [],
          selected: {
            year: 2026,
            month: 4,
            reusable: true,
            statement_exists: true,
            has_linked_txns: true,
            has_manual_added_txns_only: false,
            policy_note: "statement exists; extraction can be skipped",
            statement_id: "stmt-1",
            statement_month: "2026-04",
            period_start: "2026-04-01",
            period_end: "2026-04-30",
            linked_txn_count: 12,
            manual_added_txn_count: 0
          }
        });
      }

      return withDefaultRoutes()(url, init);
    });

    render(<App />);
    await user.click(await screen.findByRole("button", { name: "Import" }));
    await user.click(screen.getByRole("button", { name: "toggle import options" }));
    await screen.findByTestId("import-options-panel");

    await user.selectOptions(screen.getByLabelText("Year"), "2026");
    await user.selectOptions(screen.getByLabelText("Month"), "4");

    expect(await screen.findByTestId("coverage-hit-banner")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Start Extraction" })).not.toBeInTheDocument();
    expect(screen.queryByText("Choose File")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Show me statements" })).toBeInTheDocument();

    const postImportCalls = fetchMock.mock.calls.filter((call) => {
      const url = toUrl(call[0]);
      const method = ((call[1] as RequestInit | undefined)?.method || "GET").toUpperCase();
      return url.includes("/api/v1/imports") && method === "POST";
    });
    expect(postImportCalls).toHaveLength(0);
  });

  it("navigates to View Data when show me statements is clicked", async () => {
    const user = userEvent.setup();
    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;

    fetchMock.mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = toUrl(input);
      const method = (init?.method || "GET").toUpperCase();

      if (url.includes("/api/v1/statements/coverage") && method === "GET") {
        return jsonResponse({
          account_id: "manual-default-account",
          years: [],
          selected: {
            year: 2026,
            month: 3,
            reusable: true,
            statement_exists: true,
            has_linked_txns: true,
            has_manual_added_txns_only: false,
            policy_note: "statement exists; extraction can be skipped",
            statement_id: "stmt-2",
            statement_month: "2026-03",
            period_start: "2026-03-01",
            period_end: "2026-03-31",
            linked_txn_count: 8,
            manual_added_txn_count: 0
          }
        });
      }

      if (url.includes("/api/v1/statements?") && method === "GET") {
        return jsonResponse([
          {
            id: "stmt-2",
            statement_month: "2026-03",
            period_start: "2026-03-01",
            period_end: "2026-03-31",
            linked_txn_count: 8
          }
        ]);
      }

      if (url.includes("/api/v1/statements/stmt-2/transactions") && method === "GET") {
        return jsonResponse([
          {
            id: "txn-1",
            description: "Coffee",
            booked_at: "2026-03-02",
            amount_cents: 450,
            source: "manual"
          }
        ]);
      }

      return withDefaultRoutes()(url, init);
    });

    render(<App />);
    await user.click(await screen.findByRole("button", { name: "Import" }));
    await user.click(screen.getByRole("button", { name: "toggle import options" }));
    await screen.findByTestId("import-options-panel");
    await user.selectOptions(screen.getByLabelText("Year"), "2026");
    await user.selectOptions(screen.getByLabelText("Month"), "3");

    await user.click(await screen.findByRole("button", { name: "Show me statements" }));
    expect(await screen.findByTestId("view-data-page")).toBeInTheDocument();
    expect(screen.getByText(/Showing 2026-03/)).toBeInTheDocument();
  });

  it("shows centered polling status, then results summary above transactions", async () => {
    const user = userEvent.setup();
    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;

    let statusCalls = 0;
    fetchMock.mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = toUrl(input);
      const method = (init?.method || "GET").toUpperCase();

      if (url.includes("/api/v1/imports") && method === "POST") {
        return jsonResponse({ import_id: "imp-1", status: "queued", reused: false }, 201);
      }

      if (url.includes("/api/v1/imports/imp-1/status") && method === "GET") {
        statusCalls += 1;
        if (statusCalls === 1) {
          return jsonResponse({
            import_id: "imp-1",
            status: "queued",
            extraction_mode: "managed",
            effective_provider: null,
            provider_attempts: [],
            diagnostics: {},
            summary: {},
            errors: [],
            warnings: [],
            review_required_count: 0
          });
        }
        return jsonResponse({
          import_id: "imp-1",
          status: "review_required",
          extraction_mode: "managed",
          effective_provider: "llamaextract_jobs",
          provider_attempts: [],
          diagnostics: { managed_flow_mode: "new" },
          summary: { parsed_rows: 2 },
          errors: [],
          warnings: ["low confidence row"],
          review_required_count: 1
        });
      }

      if (url.includes("/api/v1/imports/imp-1/review") && method === "GET") {
        return jsonResponse([
          {
            row_id: "row-1",
            row_index: 1,
            normalized_json: {
              booked_at: "2026-03-03",
              description: "Coffee",
              amount_cents: 530
            },
            confidence: 0.66,
            parse_error: null,
            approved: true,
            rejection_reason: null
          }
        ]);
      }

      return withDefaultRoutes()(url, init);
    });

    render(<App />);
    await user.click(await screen.findByRole("button", { name: "Import" }));

    const fileInput = screen.getByTestId("import-file-input") as HTMLInputElement;
    const file = new File(["dummy"], "statement.pdf", { type: "application/pdf" });
    fireEvent.change(fileInput, { target: { files: [file] } });
    await user.click(screen.getByRole("button", { name: "Start Extraction" }));

    expect(await screen.findByTestId("import-polling-stage")).toBeInTheDocument();
    const summary = await screen.findByTestId("results-summary");
    const rows = await screen.findByTestId("results-rows");
    expect(summary.compareDocumentPosition(rows) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("shows blocking no-account state when account list is empty", async () => {
    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    fetchMock.mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = toUrl(input);
      const method = (init?.method || "GET").toUpperCase();
      if (url.includes("/api/v1/accounts") && method === "GET") {
        return jsonResponse([]);
      }
      return withDefaultRoutes()(url, init);
    });

    render(<App />);
    expect(await screen.findByTestId("no-account-state")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });

  it("submits review decisions and commits import", async () => {
    const user = userEvent.setup();
    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;

    let statusCalls = 0;
    fetchMock.mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = toUrl(input);
      const method = (init?.method || "GET").toUpperCase();

      if (url.includes("/api/v1/imports") && method === "POST") {
        return jsonResponse({ import_id: "imp-2", status: "queued", reused: false }, 201);
      }

      if (url.includes("/api/v1/imports/imp-2/status") && method === "GET") {
        statusCalls += 1;
        const status = statusCalls <= 1 ? "review_required" : "ready_to_commit";
        return jsonResponse({
          import_id: "imp-2",
          status,
          extraction_mode: "managed",
          effective_provider: "llamaextract_jobs",
          provider_attempts: [],
          diagnostics: {},
          summary: { parsed_rows: 1 },
          errors: [],
          warnings: [],
          review_required_count: status === "review_required" ? 1 : 0
        });
      }

      if (url.includes("/api/v1/imports/imp-2/review") && method === "GET") {
        return jsonResponse([
          {
            row_id: "row-2",
            row_index: 1,
            direction: "credit",
            direction_source: "model",
            direction_confidence: 0.93,
            normalized_json: {
              booked_at: "2026-04-10",
              description: "Store",
              amount_cents: 1200
            },
            confidence: 0.95,
            parse_error: null
          }
        ]);
      }

      if (url.includes("/api/v1/imports/imp-2/review") && method === "POST") {
        return jsonResponse({}, 204);
      }

      if (url.includes("/api/v1/imports/imp-2/commit") && method === "POST") {
        return jsonResponse({ inserted_count: 1, duplicate_count: 0 });
      }

      return withDefaultRoutes()(url, init);
    });

    render(<App />);
    await user.click(await screen.findByRole("button", { name: "Import" }));

    const fileInput = screen.getByTestId("import-file-input") as HTMLInputElement;
    fireEvent.change(fileInput, {
      target: { files: [new File(["dummy"], "statement.pdf", { type: "application/pdf" })] }
    });
    await user.click(screen.getByRole("button", { name: "Start Extraction" }));

    await screen.findByTestId("import-results-stage");
    await user.click(screen.getByRole("button", { name: "Save Review Decisions" }));
    await user.click(screen.getByRole("button", { name: "Commit Import" }));

    await waitFor(() => expect(screen.getByTestId("view-data-page")).toBeInTheDocument());

    const reviewPostCalls = fetchMock.mock.calls.filter((call) => {
      const url = toUrl(call[0]);
      const method = ((call[1] as RequestInit | undefined)?.method || "GET").toUpperCase();
      return url.includes("/api/v1/imports/imp-2/review") && method === "POST";
    });
    expect(reviewPostCalls.length).toBeGreaterThan(0);
  });

  it("resets to fresh upload state when create new import is clicked", async () => {
    const user = userEvent.setup();
    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;

    let statusCalls = 0;
    fetchMock.mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = toUrl(input);
      const method = (init?.method || "GET").toUpperCase();

      if (url.includes("/api/v1/imports") && method === "POST") {
        return jsonResponse({ import_id: "imp-3", status: "queued", reused: false }, 201);
      }

      if (url.includes("/api/v1/imports/imp-3/status") && method === "GET") {
        statusCalls += 1;
        if (statusCalls === 1) {
          return jsonResponse({
            import_id: "imp-3",
            status: "queued",
            extraction_mode: "managed",
            effective_provider: "llamaextract_jobs",
            provider_attempts: [],
            diagnostics: {},
            summary: {},
            errors: [],
            warnings: [],
            review_required_count: 0
          });
        }
        return jsonResponse({
          import_id: "imp-3",
          status: "ready_to_commit",
          extraction_mode: "managed",
          effective_provider: "llamaextract_jobs",
          provider_attempts: [],
          diagnostics: {},
          summary: { parsed_rows: 1 },
          errors: [],
          warnings: [],
          review_required_count: 0
        });
      }

      if (url.includes("/api/v1/imports/imp-3/review") && method === "GET") {
        return jsonResponse([
          {
            row_id: "row-3",
            row_index: 1,
            normalized_json: {
              booked_at: "2026-04-10",
              description: "Store",
              amount_cents: 1200
            },
            confidence: 0.95,
            parse_error: null,
            approved: true,
            rejection_reason: null
          }
        ]);
      }

      return withDefaultRoutes()(url, init);
    });

    render(<App />);
    await user.click(await screen.findByRole("button", { name: "Import" }));

    const fileInput = screen.getByTestId("import-file-input") as HTMLInputElement;
    fireEvent.change(fileInput, {
      target: { files: [new File(["dummy"], "statement.pdf", { type: "application/pdf" })] }
    });
    await user.click(screen.getByRole("button", { name: "Start Extraction" }));

    await screen.findByTestId("import-results-stage");
    await user.click(screen.getByRole("button", { name: "Create New Import" }));

    expect(await screen.findByTestId("upload-blob")).toBeInTheDocument();
    expect(screen.queryByTestId("import-results-stage")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Start Extraction" })).not.toBeInTheDocument();
  });
});
