#!/usr/bin/env bash
# Run a sustained, production-shaped Zero deployment simulation locally.
#
# Usage:
#   scripts/simulate-production.sh          # ~15 minute default scenario
#   scripts/simulate-production.sh --quick  # ~90 second smoke scenario
#   scripts/simulate-production.sh --keep-up
#
# Workload phases use clients:seconds pairs, for example:
#   SIM_PHASES=50:60,400:180,1000:300,150:180 scripts/simulate-production.sh

set -euo pipefail
cd "$(dirname "$0")/.."
UI_SCRIPT_NAME=simulate-production
# shellcheck source=scripts/lib/ui.sh
source scripts/lib/ui.sh

QUICK=0
KEEP_UP="${KEEP_UP:-0}"
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    --keep-up) KEEP_UP=1 ;;
    -h|--help)
      sed -n '2,10p' "$0" | sed 's/^# //; s/^#$//'
      exit 0
      ;;
    *) ui_error "Unknown argument: $arg"; exit 2 ;;
  esac
done

export SIM_PROJECT="${SIM_PROJECT:-zero-prod-sim}"
export SIM_PORT="${SIM_PORT:-4848}"
export SIM_HAPROXY_STATS_PORT="${SIM_HAPROXY_STATS_PORT:-8404}"
export SIM_MIN_REPLICAS="${SIM_MIN_REPLICAS:-1}"
export SIM_MAX_REPLICAS="${SIM_MAX_REPLICAS:-6}"
export SIM_TARGET_CONNECTIONS="${SIM_TARGET_CONNECTIONS:-180}"
export SIM_CONNECTIONS_PER_REPLICA="${SIM_CONNECTIONS_PER_REPLICA:-250}"
export SIM_COMPOSE_FILE="$PWD/simulation/docker-compose.yml"

if [ "$QUICK" = 1 ]; then
  PHASES="${SIM_PHASES:-25:15,180:25,450:25,40:15}"
  export SIM_SCALE_INTERVAL="${SIM_SCALE_INTERVAL:-3}"
  export SIM_SCALE_COOLDOWN="${SIM_SCALE_COOLDOWN:-6}"
  export SIM_SCALE_IN_STREAK="${SIM_SCALE_IN_STREAK:-2}"
else
  # Warm baseline -> traffic growth -> peak -> recovery -> quiet tail.
  PHASES="${SIM_PHASES:-75:90,350:150,900:240,1400:300,400:150,75:90}"
fi

COMPOSE=(docker compose -p "$SIM_PROJECT" -f "$SIM_COMPOSE_FILE")
RESULT_DIR="simulation/results/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RESULT_DIR"
export SIM_EVENT_LOG="$PWD/$RESULT_DIR/autoscaling.csv"
WRITER_PID=""
AUTOSCALER_PID=""
STACK_STARTED=0

terminate_tree() {
  local pid=$1 child
  [ -n "$pid" ] || return 0
  # A background loop can be waiting on docker/docker-compose. Terminate its
  # descendants too so an in-flight scale operation cannot recreate services
  # after the final `compose down` has started.
  for child in $(pgrep -P "$pid" 2>/dev/null || true); do
    terminate_tree "$child"
  done
  kill "$pid" >/dev/null 2>&1 || true
}

cleanup() {
  local code=$?
  trap - EXIT INT TERM
  for pid in "$WRITER_PID" "$AUTOSCALER_PID"; do
    if [ -n "$pid" ]; then
      terminate_tree "$pid"
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  if [ "$STACK_STARTED" != "1" ]; then
    :
  elif [ "$KEEP_UP" = 1 ]; then
    ui_note "Simulation stack left running"
    ui_note "Stop it with: docker compose -p $SIM_PROJECT -f simulation/docker-compose.yml down -v"
  else
    # Stop immediately before removal. Graceful draining is exercised by the
    # workload itself; cleanup must also be reliable after a partial startup.
    ui_run "Stop simulation stack" "${COMPOSE[@]}" kill || true
    ui_run "Remove simulation stack" "${COMPOSE[@]}" down -t 2 -v --remove-orphans || true
  fi
  exit "$code"
}
trap cleanup EXIT INT TERM

for command in docker cargo curl awk pgrep; do
  command -v "$command" >/dev/null 2>&1 || { ui_error "$command is required but was not found"; exit 1; }
done
docker info >/dev/null 2>&1 || { ui_error "Docker is not running"; exit 1; }

scenario="production scenario"
[ "$QUICK" = 1 ] && scenario="quick smoke scenario"
ui_banner "Production simulation" "$scenario · replicas $SIM_MIN_REPLICAS..$SIM_MAX_REPLICAS"

simulation_ready() {
  curl -fsS "http://127.0.0.1:${SIM_HAPROXY_STATS_PORT}/stats;csv" 2>/dev/null \
    | awk -F, '$1 == "zero_view_syncers" && $2 != "BACKEND" && $18 == "UP" { up++ } END { exit !(up > 0) }'
}

build_load_driver() { (cd bench/loadtest && cargo build --release --bin zero-loadtest); }

ui_run "Reset isolated project" "${COMPOSE[@]}" down -t 2 -v --remove-orphans || true

STACK_STARTED=1
ui_run "Build and start production topology" "${COMPOSE[@]}" up -d --build --scale view-syncer="$SIM_MIN_REPLICAS" \
  postgres change-streamer view-syncer load-balancer

if ! ui_wait_for "Wait for healthy worker pool" 180 2 simulation_ready; then
  "${COMPOSE[@]}" ps >&2
  "${COMPOSE[@]}" logs --tail=80 change-streamer view-syncer load-balancer >&2
  exit 1
fi

ui_run "Build protocol load driver" build_load_driver
ulimit -n 100000 2>/dev/null || true

ui_note "Starting continuous upstream writes"
(
  row=1
  while :; do
    "${COMPOSE[@]}" exec -T postgres psql -U postgres -d zero -v ON_ERROR_STOP=1 \
      -c "UPDATE issue SET rank = rank + 1 WHERE id = 'i${row}';" >/dev/null
    row=$((row % 1000 + 1))
    sleep "${SIM_WRITE_INTERVAL:-1}"
  done
) >"$UI_LOG_DIR/upstream-writer.log" 2>&1 &
WRITER_PID=$!

ui_note "Starting autoscaler · target $SIM_TARGET_CONNECTIONS sessions per worker"
simulation/autoscaler.sh >"$UI_LOG_DIR/autoscaler.log" 2>&1 &
AUTOSCALER_PID=$!

phase=0
failures=0
OLD_IFS=$IFS
IFS=,
set -- $PHASES
IFS=$OLD_IFS
for spec in "$@"; do
  phase=$((phase + 1))
  case "$spec" in
    *:*) ;;
    *) ui_error "Invalid phase '$spec'; expected clients:seconds"; exit 2 ;;
  esac
  clients=${spec%%:*}
  duration=${spec#*:}
  if [ -z "$clients" ] || [ -z "$duration" ]; then
    ui_error "Invalid phase '$spec'; expected clients:seconds"
    exit 2
  fi
  ramp=$((duration / 4))
  [ "$ramp" -lt 2 ] && ramp=2
  [ "$ramp" -gt 30 ] && ramp=30
  phase_label="Phase $phase · $clients clients · ${duration}s"
  if ui_run "$phase_label" bench/loadtest/target/release/zero-loadtest \
      --url "ws://127.0.0.1:${SIM_PORT}" \
      --clients "$clients" --duration "$duration" --ramp "$ramp" \
      --workload fanout --fanout-min-pokes 1; then
    cp "$(ui_log_path "$phase_label")" "$RESULT_DIR/phase-${phase}.log"
  else
    failures=$((failures + 1))
    cp "$(ui_log_path "$phase_label")" "$RESULT_DIR/phase-${phase}.log" 2>/dev/null || true
    ui_warn "Phase $phase degraded; report: $RESULT_DIR/phase-${phase}.log"
  fi
done

printf '\n'
ui_success "Simulation complete"
ui_note "Autoscaling timeline: $RESULT_DIR/autoscaling.csv"
ui_note "Phase reports: $RESULT_DIR/phase-*.log"
awk -F, 'NR > 1 {
  if (min == "" || $2 < min) min = $2
  if ($2 > max) max = $2
  if ($5 == "scale-out") out++
  if ($5 == "scale-in") scale_ins++
} END {
  printf "• Observed replicas: %d..%d (%d scale-outs, %d scale-ins)\n", min, max, out, scale_ins
}' "$SIM_EVENT_LOG"
if [ "$failures" -gt 0 ]; then
  ui_error "$failures phase(s) were degraded"
  exit 1
fi
ui_logs_note
