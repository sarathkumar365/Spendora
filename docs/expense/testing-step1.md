# Step 1 Test Strategy and Execution

This document defines validation for all Step 1 deliverables: monorepo scaffold, Rust services, SQLite migrations, UI baseline, and Tauri process lifecycle integration.

## Test Locations
- Rust unit/integration-style tests:
  - `services/expense-rs/crates/core/src/lib.rs`
  - `services/expense-rs/crates/storage_sqlite/src/lib.rs`
- Automated smoke tests:
  - `tests/step1/smoke.sh`
  - `tests/step1/stress-lite.sh`
  - `tests/step1/run-all.sh`
- Manual desktop validation:
  - `tests/step1/tauri-manual-checklist.md`

## Coverage Matrix
1. Core helpers (`expense_core`)
- `new_health_status` returns expected service/status fields.
- `new_idempotency_key` generates unique keys.
- `default_app_data_dir` resolves expected platform path suffix.

2. SQLite storage bootstrap (`storage_sqlite`)
- DB connection creates DB path when missing.
- Foreign key pragma is enabled.
- Migration runner executes and is idempotent.
- Critical tables exist after migration.

3. API baseline (`api`)
- Process boots with custom DB path and port.
- `/health` and `/api/v1/health` return healthy payload.
- `/api/v1/diagnostics` confirms sqlite availability.

4. Worker baseline (`worker`)
- Process boots with custom DB path and port.
- `/health` and `/api/v1/health` return healthy payload.
- Worker runs loop without crashing during smoke window.

5. UI/Tauri baseline
- UI compiles in production mode.
- Tauri shell starts and presents service controls.
- Start/stop/status actions interact with spawned API/worker.

## Commands
Run from repo root.

1. Rust tests only:
- `npm run test:rs`

2. UI build validation only:
- `npm run test:ui-build`

3. API/worker smoke only:
- `npm run test:step1:smoke`

4. Full automated Step 1 suite:
- `npm run test:step1`

5. Stress-lite reliability suite:
- `npm run test:step1:stress`

6. Manual desktop shell validation:
- Follow `tests/step1/tauri-manual-checklist.md`

## CI Recommendation
For CI, use this order:
1. `npm ci`
2. `npm run test:rs`
3. `npm run test:ui-build`
4. `npm run test:step1:smoke`
5. `npm run test:step1:stress`

## Expected Artifacts During Smoke
- Temp DB path under `.tmp/step1-smoke/expense-smoke.db`
- API log: `.tmp/step1-smoke/api.log`
- Worker log: `.tmp/step1-smoke/worker.log`

## Expected Artifacts During Stress-lite
- Temp DB path under `.tmp/step1-stress/expense-stress.db`
- API log: `.tmp/step1-stress/api.log`
- Worker log: `.tmp/step1-stress/worker.log`
- Invalid-path checks:
  - `.tmp/step1-stress/api-invalid.log`
  - `.tmp/step1-stress/worker-invalid.log`

## Stress-lite Scenarios
1. Restart churn
- Repeats service start/probe/stop cycles (`RESTART_CYCLES`, default `4`).
- Verifies service startup stability across repeated process lifecycle operations.

2. Probe pressure
- Repeats API and worker health checks (`HEALTH_PROBES`, default `25`).
- Includes diagnostics checks while services are running.

3. Negative path validation
- Starts API/worker with an intentionally invalid DB path (`/dev/null/expense-invalid.db`).
- Expects both services to fail startup (ensures failures are not silently ignored).

## Pass Criteria
- All automated scripts exit with code 0.
- Health and diagnostics endpoints return HTTP 200 with expected JSON fields.
- Migration-created DB contains critical bootstrap tables.
- Manual Tauri checklist passes without lifecycle or startup failures.
