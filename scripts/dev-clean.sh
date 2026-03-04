#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-clean}"
PATTERN='target/debug/(api|worker|expense-desktop-tauri)|tauri dev|[[:space:]]vite($|[[:space:]])'

list_matches() {
  ps -ax -o pid=,command= \
    | rg -e "$PATTERN" \
    | rg -v "rg -e|scripts/dev-clean.sh" || true
}

print_usage() {
  echo "Usage: scripts/dev-clean.sh [list|clean]"
  echo "  list  - show matching dev processes"
  echo "  clean - kill matching dev processes (default)"
}

case "$MODE" in
  list)
    echo "[dev-clean] matching processes:"
    list_matches
    ;;
  clean)
    MATCHES="$(list_matches)"
    if [[ -z "$MATCHES" ]]; then
      echo "[dev-clean] no matching processes found."
      exit 0
    fi

    echo "[dev-clean] killing processes:"
    echo "$MATCHES"
    PIDS="$(echo "$MATCHES" | awk '{print $1}')"
    # shellcheck disable=SC2086
    kill $PIDS || true
    sleep 0.2

    REMAINING="$(list_matches)"
    if [[ -n "$REMAINING" ]]; then
      echo "[dev-clean] force killing remaining processes:"
      echo "$REMAINING"
      RPIDS="$(echo "$REMAINING" | awk '{print $1}')"
      # shellcheck disable=SC2086
      kill -9 $RPIDS || true
    fi

    echo "[dev-clean] done."
    ;;
  *)
    print_usage
    exit 1
    ;;
esac
