# Local Bootstrap Runbook

## Prerequisites
- Node.js 20+
- Rust stable toolchain
- Tauri desktop prerequisites installed

## Install
- `npm install`

## Start API and worker manually
- `npm run rs:api`
- `npm run rs:worker`

## Start desktop shell
- `npm run tauri:dev`

## Health checks
- API: `http://127.0.0.1:8081/health`
- API diagnostics: `http://127.0.0.1:8081/api/v1/diagnostics`
- Worker: `http://127.0.0.1:8082/health`

## Database
- Default DB file path is OS app-data directory + `expense.db`.
- On first boot, migrations from `services/expense-rs/migrations` are applied.
