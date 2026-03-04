import React from "react";
import ReactDOM from "react-dom/client";
import { invoke } from "@tauri-apps/api/core";
import "./styles.css";

type Status = {
  api_running: boolean;
  worker_running: boolean;
};

function App() {
  const [status, setStatus] = React.useState<Status | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  async function run(action: "start_services" | "stop_services" | "service_status") {
    try {
      setError(null);
      const next = await invoke<Status>(action);
      setStatus(next);
    } catch (err) {
      setError(String(err));
    }
  }

  React.useEffect(() => {
    void run("service_status");
  }, []);

  return (
    <main className="app-shell">
      <header>
        <h1>Expense Tracker</h1>
        <p>Desktop bootstrap is ready.</p>
      </header>
      <section className="actions">
        <button onClick={() => void run("start_services")}>Start Services</button>
        <button onClick={() => void run("stop_services")}>Stop Services</button>
        <button onClick={() => void run("service_status")}>Refresh Status</button>
      </section>
      {status && (
        <p>
          API: <strong>{status.api_running ? "running" : "stopped"}</strong> | Worker:{" "}
          <strong>{status.worker_running ? "running" : "stopped"}</strong>
        </p>
      )}
      {error && <p className="error">Error: {error}</p>}
      <section className="status-grid">
        <article>
          <h2>UI</h2>
          <p>Running with React + Vite.</p>
        </article>
        <article>
          <h2>API</h2>
          <p>Expected on <code>http://127.0.0.1:8081</code>.</p>
        </article>
        <article>
          <h2>Worker</h2>
          <p>Background processor bootstrapped.</p>
        </article>
      </section>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
