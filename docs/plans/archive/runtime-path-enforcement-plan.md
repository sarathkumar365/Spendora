# Enforce Runtime Data Paths by Mode (Dev vs Production)

## Summary
Implement a single source of truth for runtime paths in Tauri startup so:
- Dev mode uses repo-local `services/expense-rs/.runtime`
- Production mode always uses OS app data directory (for DB + logs)
- Production automatically corrects any accidental runtime-path use, without migrating legacy runtime DB data

## Implementation Changes
1. **Add explicit runtime mode + resolved paths model**
- Introduce a small internal struct (for example `RuntimePaths`) with:
  - `mode`: `Dev` | `Production`
  - `base_dir`
  - `db_path`
  - `logs_dir`
- Add a resolver function used by startup and logging code:
  - Dev base dir: `services_root().join(".runtime")`
  - Production base dir: `app.path().app_data_dir()` (or equivalent Tauri app data resolver), then `.../logs` and `.../expense.db`

2. **Decide mode once during app setup and store in managed state**
- During `.setup(...)`, compute mode + paths and `manage(...)` it as shared state.
- Mode rule:
  - `cfg!(debug_assertions)` => `Dev`
  - otherwise => `Production`
- Keep all service-launch and log-path code reading from this managed runtime-path state, not from `services_root().join(".runtime")`.

3. **Refactor service spawn + logging to use resolved paths**
- Update `spawn_service(...)` to accept/use resolved `db_path` and `base_dir`.
- Pass `--db-path <resolved_db_path>` and `EXPENSE_APP_DATA_DIR=<resolved_base_dir>` consistently.
- Update `service_log_path`, `startup_metrics_log_path`, `read_log_tail`, and `clear_runtime_logs_for_dev` to use resolved `logs_dir`.
- Ensure `clear_runtime_logs_for_dev` runs only in `Dev`.

4. **Production auto-correct behavior**
- In production mode, never read/write under `.runtime` for DB/logs.
- If a runtime path is detected in any legacy helper path, auto-correct to app-data path before process spawn and log a clear correction message (single line with old path -> new path).
- No runtime DB migration; old `.runtime/expense.db` remains untouched.

5. **Operational visibility**
- On startup, log one canonical line for effective mode and file paths:
  - mode
  - db path
  - logs path
- This prevents ambiguity during support/debugging.

## Public APIs / Interfaces
- No backend HTTP API changes.
- Tauri command signatures can remain unchanged unless needed internally.
- Internal interface addition: managed runtime path state object consumed by startup/service/logging helpers.

## Test Plan
1. **Unit tests for path resolution**
- Dev mode resolves to `services_root/.runtime`.
- Production mode resolves to app data dir.
- Production resolution never yields `.runtime`-based DB/log path.

2. **Unit tests for spawn/log helper wiring**
- `spawn_service` uses resolved `--db-path` and `EXPENSE_APP_DATA_DIR`.
- `service_log_path` and `startup_metrics_log_path` use resolved `logs_dir`.

3. **Behavior tests (non-mutating integration-style)**
- Dev startup clears/truncates runtime logs only in dev.
- Production startup does not call dev log-clear behavior.
- Production correction path logs expected “corrected runtime path” message.

4. **Regression checks**
- Existing startup retry/status flow remains unchanged.
- Manual retry (`start_services`) still updates startup status correctly.
- No changes to import/view-data API interactions.

## Assumptions and Defaults
- “Production” is defined as non-debug build (`!cfg!(debug_assertions)`).
- Production enforcement policy: auto-correct to app-data paths (not hard-fail).
- Legacy runtime DB is not migrated automatically.
