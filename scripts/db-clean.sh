#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-runtime}"
RUNTIME_DB="${EXPENSE_DB_PATH:-services/expense-rs/.runtime/expense.db}"
APP_DB="${SPENDORA_APP_DB_PATH:-$HOME/Library/Application Support/SpendoraDesktop/expense.db}"

print_usage() {
  echo "Usage: scripts/db-clean.sh [status|runtime|app|all]"
  echo "  status  - show known DB file paths and whether they exist"
  echo "  runtime - delete runtime DB (default)"
  echo "  app     - delete app-support DB"
  echo "  all     - delete runtime + app-support DBs"
}

print_db_status() {
  local label="$1"
  local path="$2"
  if [[ -f "$path" ]]; then
    local size
    size="$(ls -lh "$path" | awk '{print $5}')"
    echo "[$label] present: $path ($size)"
  else
    echo "[$label] missing: $path"
  fi
}

delete_db() {
  local label="$1"
  local path="$2"

  echo "[$label] deleting: $path"
  rm -f "$path" "$path-shm" "$path-wal"

  if [[ -f "$path" ]]; then
    echo "[$label] failed: file still exists"
    return 1
  fi

  echo "[$label] cleared"
}

case "$MODE" in
  status)
    print_db_status "runtime" "$RUNTIME_DB"
    print_db_status "app" "$APP_DB"
    ;;
  runtime)
    delete_db "runtime" "$RUNTIME_DB"
    ;;
  app)
    delete_db "app" "$APP_DB"
    ;;
  all)
    delete_db "runtime" "$RUNTIME_DB"
    delete_db "app" "$APP_DB"
    ;;
  *)
    print_usage
    exit 1
    ;;
esac

