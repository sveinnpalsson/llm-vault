#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VAULT_OPS="$ROOT/vault-ops"

MAX_ITEMS="${MAX_ITEMS:-${MAX_SECONDS:-300}}"
QUERY_TEXT="${QUERY_TEXT:-tax receipt}"
SUMMARY_BACKFILL="${SUMMARY_BACKFILL:-50}"
PHOTO_BACKFILL="${PHOTO_BACKFILL:-0}"
LONGRUN_VERBOSE="${LONGRUN_VERBOSE:-1}"

LONGRUN_FLAGS=()
if [[ "$LONGRUN_VERBOSE" != "0" ]]; then
  LONGRUN_FLAGS+=(--verbose)
fi

if [[ -z "${LLM_VAULT_DB_PASSWORD:-}" ]]; then
  echo "error: LLM_VAULT_DB_PASSWORD is required" >&2
  exit 2
fi

echo "[acceptance] status before"
"$VAULT_OPS" status --json

echo "[acceptance] search redacted"
"$VAULT_OPS" search "$QUERY_TEXT" --top-k 5 --json

echo "[acceptance] search full"
"$VAULT_OPS" search "$QUERY_TEXT" --top-k 5 --clearance full --json

echo "[acceptance] bounded update (max=$MAX_ITEMS)"
"$VAULT_OPS" update --max "$MAX_ITEMS" "${LONGRUN_FLAGS[@]}"

echo "[acceptance] bounded repair (summary backfill=$SUMMARY_BACKFILL)"
"$VAULT_OPS" repair --max "$MAX_ITEMS" --reprocess-missing-summaries "$SUMMARY_BACKFILL" "${LONGRUN_FLAGS[@]}"

if [[ "$PHOTO_BACKFILL" != "0" ]]; then
  echo "[acceptance] bounded repair photo backfill (photo backfill=$PHOTO_BACKFILL)"
  "$VAULT_OPS" repair --max "$MAX_ITEMS" --reprocess-missing-photo-analysis "$PHOTO_BACKFILL" "${LONGRUN_FLAGS[@]}"
fi

echo "[acceptance] status after"
"$VAULT_OPS" status --json

echo "[acceptance] done"
