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

CLIENTS="${1:-2000}"
DURATION="${2:-30}"
RUST_PORT="${ZERO_BENCH_RUST_PORT:-4848}"
REF_PORT="${ZERO_BENCH_REF_PORT:-4849}"
METRICS_PORT="${ZERO_BENCH_METRICS_PORT:-9600}"
COMPOSE="docker compose -f bench/docker-compose.bench.yml"
WORKLOAD="${LOAD_WORKLOAD:-ping}"
FANOUT_PID=""

stop_fanout_writer() {
  if [ -n "$FANOUT_PID" ]; then
    kill "$FANOUT_PID" >/dev/null 2>&1 || true
    wait "$FANOUT_PID" >/dev/null 2>&1 || true
    FANOUT_PID=""
  fi
}

cleanup() {
  stop_fanout_writer
  if [ "${KEEP_UP:-0}" = "1" ]; then
    echo "==> stack left running (KEEP_UP=1). Tear down with: $COMPOSE down -v"
  else
    echo "==> tearing down…"
    $COMPOSE down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> starting both Postgres databases…"
$COMPOSE up -d postgres-rust postgres-ref
# The image first runs a temporary bootstrap server, which `pg_isready` can
# report as ready immediately before the entrypoint shuts it down. Wait for a
# real query against the final database instead so seeding cannot hit that
# shutdown window.
for pg in zero-bench-pg-rust zero-bench-pg-ref; do
  until docker exec "$pg" psql -U postgres -d zero -Atqc 'SELECT 1' >/dev/null 2>&1; do sleep 1; done
done

echo "==> seeding identical dataset into both (before the servers start, so the publication exists)…"
for pg in zero-bench-pg-rust zero-bench-pg-ref; do
  docker exec -i "$pg" psql -U postgres -d zero < bench/seed.sql >/dev/null
done

echo "==> starting both sync servers…"
$COMPOSE up -d --build zero-rust zero-ref

echo "==> waiting for the Rust server (/readyz)…"
for i in $(seq 1 60); do
  curl -fsS "http://localhost:${METRICS_PORT}/readyz" >/dev/null 2>&1 && { echo "    rust ready."; break; }
  if [ "$i" = "60" ]; then
    echo "!! Rust server never became ready. Logs:" >&2
    $COMPOSE logs --tail=40 zero-rust >&2 || true
    [ "${KEEP_UP:-0}" = "1" ] || $COMPOSE down -v >/dev/null 2>&1 || true
    exit 1
  fi
  sleep 2
done

echo "==> waiting for the official zero server…"
for i in $(seq 1 60); do
  # Any HTTP response (even 404) means the dispatcher is up.
  if curl -s -o /dev/null "http://localhost:${REF_PORT}/"; then echo "    official zero up."; break; fi
  if [ "$i" = "60" ]; then
    echo "!! Official zero never came up. Logs:" >&2
    $COMPOSE logs --tail=40 zero-ref >&2 || true
    [ "${KEEP_UP:-0}" = "1" ] || $COMPOSE down -v >/dev/null 2>&1 || true
    exit 1
  fi
  sleep 2
done

echo "==> building the load driver…"
( cd bench/loadtest && cargo build --release --bin zero-loadtest )

if [ "$WORKLOAD" = "fanout" ]; then
  # Fan-out needs real source changes; one stable row update per interval keeps
  # the two independently seeded databases logically equivalent while clients
  # hold their nonempty desired query. The driver itself verifies observed
  # pokes, and defaults to at least one per client in this mode.
  export LOAD_FANOUT_MIN_POKES="${LOAD_FANOUT_MIN_POKES:-1}"
  FANOUT_INTERVAL="${ZERO_BENCH_FANOUT_INTERVAL_S:-.25}"
  echo "==> starting matched fan-out writer (every ${FANOUT_INTERVAL}s)…"
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
  ) &
  FANOUT_PID=$!
fi

echo "==> raising fd limit and running $WORKLOAD load ($CLIENTS clients, ${DURATION}s per target)…"
ulimit -n 100000 || true

( cd bench/loadtest && target/release/zero-loadtest \
    --clients "$CLIENTS" --duration "$DURATION" \
    --url "ws://127.0.0.1:${RUST_PORT}" --container zero-bench-rust \
    --compare --ref-url "ws://127.0.0.1:${REF_PORT}" --ref-container zero-bench-ref )
