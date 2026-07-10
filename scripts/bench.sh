#!/usr/bin/env bash
# scripts/bench.sh — benchmarks zero-cache-rust against the official
# rocicorp/zero, head to head.
#
#   scripts/bench.sh [CLIENTS] [DURATION_S]      default: 2000 clients, 30s
#
# What it does:
#   1. starts TWO Postgres (logical replication) via bench/docker-compose.bench.yml
#      — a dedicated database per server so neither's replication interferes
#   2. seeds an identical dataset (bench/seed.sql) into BOTH before the servers start
#   3. starts BOTH sync servers, each against its own database:
#        - this Rust port          (built from ./Dockerfile)   on :4848
#        - official Zero v1.7.0 (commit 6863de5, protocol v51) on :4849
#   4. drives identical WebSocket load against both (bench/loadtest), speaking
#      the real @rocicorp/zero connect protocol (/sync/v51/connect)
#   5. prints a side-by-side report: connect success, ping latency
#      p50/p90/p99, throughput, and per-container peak CPU / memory
#
# Env:
#   KEEP_UP=1     leave the stack running afterwards (default: tear down)
#   ZERO_BENCH_{PG_RUST,PG_REF,RUST,REF,METRICS}_PORT
#                 override host ports when the defaults are already in use
#   LOAD_WORKLOAD=fanout
#                 run the data-path load mode; this script then drives matched
#                 source updates into both isolated Postgres databases
#   ZERO_BENCH_FANOUT_INTERVAL_S=.25
#                 delay between fan-out source updates (default .25 seconds)

set -euo pipefail
cd "$(dirname "$0")/.."
UI_SCRIPT_NAME=bench
# shellcheck source=scripts/lib/ui.sh
source scripts/lib/ui.sh

CLIENTS="${1:-2000}"
DURATION="${2:-30}"
RUST_PORT="${ZERO_BENCH_RUST_PORT:-4848}"
REF_PORT="${ZERO_BENCH_REF_PORT:-4849}"
METRICS_PORT="${ZERO_BENCH_METRICS_PORT:-9600}"
COMPOSE=(docker compose -f bench/docker-compose.bench.yml)
WORKLOAD="${LOAD_WORKLOAD:-ping}"
FANOUT_PID=""
STACK_STARTED=0

case "${1:-}" in
  -h|--help)
    printf 'Usage: scripts/bench.sh [CLIENTS] [DURATION_SECONDS]\n\nRun a quiet head-to-head benchmark against official Zero.\n'
    exit 0
    ;;
esac
[ "$#" -le 2 ] || { ui_error "Too many arguments"; exit 2; }

stop_fanout_writer() {
  if [ -n "$FANOUT_PID" ]; then
    kill "$FANOUT_PID" >/dev/null 2>&1 || true
    wait "$FANOUT_PID" >/dev/null 2>&1 || true
    FANOUT_PID=""
  fi
}

cleanup() {
  local code=$?
  trap - EXIT INT TERM
  stop_fanout_writer
  if [ "$STACK_STARTED" != "1" ]; then
    :
  elif [ "${KEEP_UP:-0}" = "1" ]; then
    ui_note "Stack left running (KEEP_UP=1)"
    ui_note "Stop it with: ${COMPOSE[*]} down -v"
  else
    ui_run "Remove benchmark stack" "${COMPOSE[@]}" down -v || true
  fi
  exit "$code"
}
trap cleanup EXIT INT TERM

case "$CLIENTS:$DURATION" in
  *[!0-9:]*|:*|*:) ui_error "CLIENTS and DURATION must be positive integers"; exit 2 ;;
esac
[ "$CLIENTS" -gt 0 ] && [ "$DURATION" -gt 0 ] || { ui_error "CLIENTS and DURATION must be greater than zero"; exit 2; }
command -v docker >/dev/null 2>&1 || { ui_error "Docker is required but was not found"; exit 1; }
command -v cargo >/dev/null 2>&1 || { ui_error "Cargo is required but was not found"; exit 1; }
command -v curl >/dev/null 2>&1 || { ui_error "curl is required but was not found"; exit 1; }
docker info >/dev/null 2>&1 || { ui_error "Docker is not running"; exit 1; }

ui_banner "Zero Cache benchmark" "$CLIENTS clients · ${DURATION}s · $WORKLOAD workload"

postgres_ready() {
  docker exec "$1" psql -U postgres -d zero -Atqc 'SELECT 1'
}

seed_databases() {
  local pg
  for pg in zero-bench-pg-rust zero-bench-pg-ref; do
    docker exec -i "$pg" psql -U postgres -d zero < bench/seed.sql
  done
}

build_load_driver() { (cd bench/loadtest && cargo build --release --bin zero-loadtest); }

run_benchmark() {
  (cd bench/loadtest && target/release/zero-loadtest \
    --clients "$CLIENTS" --duration "$DURATION" \
    --url "ws://127.0.0.1:${RUST_PORT}" --container zero-bench-rust \
    --compare --ref-url "ws://127.0.0.1:${REF_PORT}" --ref-container zero-bench-ref)
}

STACK_STARTED=1
ui_run "Start benchmark databases" "${COMPOSE[@]}" up -d postgres-rust postgres-ref
# The image first runs a temporary bootstrap server, which `pg_isready` can
# report as ready immediately before the entrypoint shuts it down. Wait for a
# real query against the final database instead so seeding cannot hit that
# shutdown window.
for pg in zero-bench-pg-rust zero-bench-pg-ref; do
  ui_wait_for "Wait for $pg" 90 1 postgres_ready "$pg"
done

ui_run "Seed identical datasets" seed_databases

ui_run "Build and start sync servers" "${COMPOSE[@]}" up -d --build zero-rust zero-ref

rust_ready() { curl -fsS "http://localhost:${METRICS_PORT}/readyz"; }
if ! ui_wait_for "Wait for Rust server" 120 2 rust_ready; then
  "${COMPOSE[@]}" logs --tail=40 zero-rust >&2 || true
  exit 1
fi

reference_ready() { curl -s -o /dev/null "http://localhost:${REF_PORT}/"; }
if ! ui_wait_for "Wait for official Zero" 120 2 reference_ready; then
  "${COMPOSE[@]}" logs --tail=40 zero-ref >&2 || true
  exit 1
fi

ui_run "Build load driver" build_load_driver

if [ "$WORKLOAD" = "fanout" ]; then
  # Fan-out needs real source changes; one stable row update per interval keeps
  # the two independently seeded databases logically equivalent while clients
  # hold their nonempty desired query. The driver itself verifies observed
  # pokes, and defaults to at least one per client in this mode.
  export LOAD_FANOUT_MIN_POKES="${LOAD_FANOUT_MIN_POKES:-1}"
  FANOUT_INTERVAL="${ZERO_BENCH_FANOUT_INTERVAL_S:-.25}"
  ui_note "Starting matched fan-out writes every ${FANOUT_INTERVAL}s"
  (
    i=1
    while :; do
      for pg in zero-bench-pg-rust zero-bench-pg-ref; do
        docker exec "$pg" psql -U postgres -d zero -v ON_ERROR_STOP=1 \
          -c "UPDATE issue SET rank = rank + 1 WHERE id = 'i${i}';" >/dev/null
      done
      i=$((i % 1000 + 1))
      sleep "$FANOUT_INTERVAL"
    done
  ) >"$UI_LOG_DIR/fanout-writer.log" 2>&1 &
  FANOUT_PID=$!
fi

ulimit -n 100000 || true
BENCH_LABEL="Run head-to-head benchmark"
ui_run "$BENCH_LABEL" run_benchmark
printf '\n%sBenchmark result%s\n' "$UI_BOLD" "$UI_RESET"
cat "$(ui_log_path "$BENCH_LABEL")"
printf '\n'
ui_success "Benchmark complete"
ui_logs_note
