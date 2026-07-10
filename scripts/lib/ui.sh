#!/usr/bin/env bash
# Shared, intentionally dependency-free terminal UI for repository scripts.
# Set VERBOSE=1 to stream command output instead of using the compact UI.

if [ -n "${ZERO_CACHE_UI_LOADED:-}" ]; then
  return 0
fi
ZERO_CACHE_UI_LOADED=1

UI_SCRIPT_NAME="${UI_SCRIPT_NAME:-$(basename "$0" .sh)}"
UI_LOG_ROOT="${UI_LOG_ROOT:-target/script-logs}"
UI_LOG_DIR="${UI_LOG_DIR:-$UI_LOG_ROOT/${UI_SCRIPT_NAME}-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
mkdir -p "$UI_LOG_DIR"

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ] && [ "${TERM:-dumb}" != "dumb" ]; then
  UI_TTY=1
  UI_BOLD=$'\033[1m'
  UI_DIM=$'\033[2m'
  UI_BLUE=$'\033[34m'
  UI_GREEN=$'\033[32m'
  UI_YELLOW=$'\033[33m'
  UI_RED=$'\033[31m'
  UI_RESET=$'\033[0m'
else
  UI_TTY=0
  UI_BOLD=""
  UI_DIM=""
  UI_BLUE=""
  UI_GREEN=""
  UI_YELLOW=""
  UI_RED=""
  UI_RESET=""
fi

ui_banner() {
  printf '\n%s%s%s\n' "$UI_BOLD" "$1" "$UI_RESET"
  [ -z "${2:-}" ] || printf '%s%s%s\n' "$UI_DIM" "$2" "$UI_RESET"
  printf '\n'
}

ui_note() { printf '%s•%s %s\n' "$UI_BLUE" "$UI_RESET" "$*"; }
ui_warn() { printf '%s!%s %s\n' "$UI_YELLOW" "$UI_RESET" "$*"; }
ui_error() { printf '%s✗%s %s\n' "$UI_RED" "$UI_RESET" "$*" >&2; }
ui_success() { printf '%s✓%s %s\n' "$UI_GREEN" "$UI_RESET" "$*"; }

ui_log_path() {
  local label=$1
  label=$(printf '%s' "$label" | tr '[:upper:]' '[:lower:]' | tr -cs '[:alnum:]' '-' | sed 's/^-//; s/-$//')
  printf '%s/%s.log' "$UI_LOG_DIR" "${label:-command}"
}

ui_run() {
  local label=$1 log start elapsed pid frame status
  shift
  log=$(ui_log_path "$label")
  start=$SECONDS

  if [ "${VERBOSE:-0}" = "1" ]; then
    printf '%s…%s %s\n' "$UI_BLUE" "$UI_RESET" "$label"
    if "$@" 2>&1 | tee "$log"; then
      elapsed=$((SECONDS - start))
      ui_success "$label (${elapsed}s)"
      return 0
    else
      status=$?
      ui_error "$label failed (exit $status)"
      return "$status"
    fi
  fi

  "$@" >"$log" 2>&1 &
  pid=$!
  if [ "$UI_TTY" = "1" ]; then
    while kill -0 "$pid" >/dev/null 2>&1; do
      for frame in '⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏'; do
        kill -0 "$pid" >/dev/null 2>&1 || break
        printf '\r%s%s%s %s' "$UI_BLUE" "$frame" "$UI_RESET" "$label"
        sleep 0.08
      done
    done
    printf '\r\033[K'
  else
    printf '… %s\n' "$label"
  fi

  if wait "$pid"; then
    elapsed=$((SECONDS - start))
    ui_success "$label (${elapsed}s)"
    return 0
  else
    status=$?
    elapsed=$((SECONDS - start))
    ui_error "$label failed after ${elapsed}s (exit $status)"
    printf '%sLast output:%s\n' "$UI_DIM" "$UI_RESET" >&2
    tail -n "${UI_ERROR_LINES:-40}" "$log" >&2 || true
    printf '%sFull log: %s%s\n' "$UI_DIM" "$log" "$UI_RESET" >&2
    return "$status"
  fi
}

ui_poll() {
  local timeout=$1 interval=$2 start
  shift 2
  start=$SECONDS
  until "$@"; do
    [ $((SECONDS - start)) -lt "$timeout" ] || return 1
    sleep "$interval"
  done
}

ui_wait_for() {
  local label=$1 timeout=$2 interval=$3
  shift 3
  ui_run "$label" ui_poll "$timeout" "$interval" "$@"
}

ui_logs_note() {
  ui_note "Detailed logs: $UI_LOG_DIR"
}
