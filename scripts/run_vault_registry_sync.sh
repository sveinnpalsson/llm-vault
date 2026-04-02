#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VAULT_OPS_CLI="$ROOT/vault-ops"
LOGDIR="${VAULT_LOG_DIR:-$ROOT/logs}"
LOGFILE="$LOGDIR/vault_registry_sync.log"
mkdir -p "$LOGDIR"

# Ensure local tool installs are visible when they live under the caller's HOME.
if [[ -n "${HOME:-}" && -d "$HOME/.local/bin" ]]; then
  export PATH="$HOME/.local/bin:$PATH"
fi

# Summary model: avoid unstable generic "default" alias for cron ingestion.
export VAULT_SUMMARY_MODEL="${VAULT_SUMMARY_MODEL:-qwen3-14b}"

run_and_log() {
  local label="$1"
  shift

  if [[ -t 1 ]]; then
    {
      echo "[$(date -Is)] $label"
      "$@"
    } 2>&1 | tee -a "$LOGFILE"
  else
    {
      echo "[$(date -Is)] $label"
      "$@"
    } >> "$LOGFILE" 2>&1
  fi
}

run_and_log \
  "vault-ops update (summary_model=$VAULT_SUMMARY_MODEL)" \
  "$VAULT_OPS_CLI" update --max 600 --verbose
