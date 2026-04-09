#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${LLM_VAULT_REPO_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
LOG_FILE="${LOG_FILE:-$REPO_DIR/logs/vault-update-15m.log}"
SECRETS_FILE="${SECRETS_FILE:-$HOME/.config/llm-vault/secrets.env}"
VAULT_OPS_BIN="${VAULT_OPS_BIN:-$REPO_DIR/vault-ops}"

mkdir -p "$REPO_DIR/logs"
cd "$REPO_DIR"

if [[ -n "${HOME:-}" && -d "$HOME/.local/bin" ]]; then
  export PATH="$HOME/.local/bin:$PATH"
fi

resolve_config_path() {
  if [[ -n "${CONFIG:-}" ]]; then
    printf '%s\n' "$CONFIG"
    return 0
  fi
  if [[ -n "${VAULT_OPS_CONFIG:-}" ]]; then
    printf '%s\n' "$VAULT_OPS_CONFIG"
    return 0
  fi
  if [[ -f "$REPO_DIR/vault-ops.toml" ]]; then
    printf '%s\n' "$REPO_DIR/vault-ops.toml"
    return 0
  fi
  if [[ -f "$REPO_DIR/.vault-ops.toml" ]]; then
    printf '%s\n' "$REPO_DIR/.vault-ops.toml"
    return 0
  fi
  return 1
}

get_mail_bridge_password_env() {
  local config_path="$1"
  [[ -n "$config_path" && -f "$config_path" ]] || return 1
  python3 -c '
import pathlib
import sys
import tomllib

path = pathlib.Path(sys.argv[1])
with path.open("rb") as fh:
    data = tomllib.load(fh)
mail = data.get("mail_bridge")
if not (isinstance(mail, dict) and mail.get("enabled") is True):
    raise SystemExit(1)
env_name = mail.get("password_env")
if isinstance(env_name, str) and env_name.strip():
    print(env_name.strip())
else:
    print("INBOX_VAULT_DB_PASSWORD")
' "$config_path"
}

ts() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log_info() {
  echo "[$(ts)] $*" | tee -a "$LOG_FILE"
}

log_error() {
  echo "[$(ts)] $*" | tee -a "$LOG_FILE" >&2
}

CONFIG_PATH=""
if CONFIG_PATH="$(resolve_config_path)"; then
  :
fi

MAIL_BRIDGE_ENABLED=0
MAIL_BRIDGE_PASSWORD_ENV="INBOX_VAULT_DB_PASSWORD"
if [[ -n "$CONFIG_PATH" ]]; then
  if mail_bridge_password_env="$(get_mail_bridge_password_env "$CONFIG_PATH")"; then
    MAIL_BRIDGE_ENABLED=1
    MAIL_BRIDGE_PASSWORD_ENV="$mail_bridge_password_env"
  fi
fi

if [[ -f "$SECRETS_FILE" ]]; then
  if [[ -z "${LLM_VAULT_DB_PASSWORD:-}" || ( "$MAIL_BRIDGE_ENABLED" -eq 1 && -z "${!MAIL_BRIDGE_PASSWORD_ENV:-}" ) ]]; then
    # shellcheck disable=SC1090
    source "$SECRETS_FILE"
  fi
fi

if [[ -z "${LLM_VAULT_DB_PASSWORD:-}" ]]; then
  log_error "ERROR missing LLM_VAULT_DB_PASSWORD"
  exit 2
fi

if [[ "$MAIL_BRIDGE_ENABLED" -eq 1 && -z "${!MAIL_BRIDGE_PASSWORD_ENV:-}" ]]; then
  if [[ -n "$CONFIG_PATH" ]]; then
    log_error "ERROR mail_bridge is enabled in $CONFIG_PATH but $MAIL_BRIDGE_PASSWORD_ENV is missing"
  else
    log_error "ERROR mail_bridge is enabled but $MAIL_BRIDGE_PASSWORD_ENV is missing"
  fi
  exit 2
fi

cmd=("$VAULT_OPS_BIN")
if [[ -n "$CONFIG_PATH" ]]; then
  cmd+=(--config "$CONFIG_PATH")
fi
cmd+=(update "$@")

log_info "START vault update"
"${cmd[@]}" 2>&1 | tee -a "$LOG_FILE"
status=${PIPESTATUS[0]}
if [[ $status -ne 0 ]]; then
  log_error "FAIL vault update exit=$status"
  exit "$status"
fi

log_info "OK vault update"
