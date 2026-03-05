# Step 1 Tauri Manual Validation Checklist

Use this checklist after `npm install` and successful Rust dependency resolution.

## Start Desktop Shell
- Run `npm run tauri:dev` from repo root.
- Confirm app window opens with the Spendora bootstrap screen.

## Process Lifecycle Commands
- Click `Start Services`.
- Confirm UI status shows API and Worker as `running`.
- Click `Refresh Status` and confirm status stays `running`.
- Click `Stop Services` and confirm both status values return to `stopped`.

## Endpoint Validation During Runtime
- With services running, verify:
  - `curl -sSf http://127.0.0.1:8081/health`
  - `curl -sSf http://127.0.0.1:8081/api/v1/diagnostics`
  - `curl -sSf http://127.0.0.1:8082/health`

## Pass Criteria
- Tauri shell launches without crash.
- Start/stop/status controls behave as expected.
- API and worker health endpoints return HTTP 200 with expected JSON fields.
