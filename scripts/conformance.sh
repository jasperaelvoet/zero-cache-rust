#!/usr/bin/env bash
# Run the black-box WebSocket transcript corpus against this Rust server and
# the immutable official Zero v1.7.0 reference. The Compose project contains
# only disposable benchmark/conformance data; this script removes its volumes
# unless KEEP_UP=1 is set for debugging.

set -euo pipefail
cd "$(dirname "$0")/.."

RUST_PORT="${ZERO_BENCH_RUST_PORT:-4848}"
REF_PORT="${ZERO_BENCH_REF_PORT:-4849}"
METRICS_PORT="${ZERO_BENCH_METRICS_PORT:-9600}"
PG_RUST_PORT="${ZERO_BENCH_PG_RUST_PORT:-5432}"
PG_REF_PORT="${ZERO_BENCH_PG_REF_PORT:-5433}"
COMPOSE=(docker compose -f bench/docker-compose.bench.yml)

cleanup() {
  if [ "${KEEP_UP:-0}" != "1" ]; then
    "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  else
    echo "==> stack left running (KEEP_UP=1). Tear down with: ${COMPOSE[*]} down -v"
  fi
}
trap cleanup EXIT

# A new corpus run must not inherit replicas, CVR state, or replication slots
# from an earlier run. This only addresses resources declared in the benchmark
# Compose file.
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true

echo "==> starting isolated Postgres databases…"
"${COMPOSE[@]}" up -d postgres-rust postgres-ref
for pg in zero-bench-pg-rust zero-bench-pg-ref; do
  until docker exec "$pg" psql -U postgres -d zero -Atqc 'SELECT 1' >/dev/null 2>&1; do
    sleep 1
  done
done

echo "==> seeding identical source data…"
for pg in zero-bench-pg-rust zero-bench-pg-ref; do
  docker exec -i "$pg" psql -U postgres -d zero < bench/seed.sql >/dev/null
done

echo "==> starting Rust and pinned official Zero v1.7.0…"
"${COMPOSE[@]}" up -d --build zero-rust zero-ref

for i in $(seq 1 90); do
  if curl -fsS "http://localhost:${METRICS_PORT}/readyz" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" = "90" ]; then
    echo "Rust server never became ready:" >&2
    "${COMPOSE[@]}" logs --tail=80 zero-rust >&2 || true
    exit 1
  fi
  sleep 2
done

for i in $(seq 1 90); do
  # A TCP/HTTP response (including HTTP 404) proves that Zero's dispatcher is
  # listening; the actual corpus validates its WebSocket behavior next.
  if curl -s -o /dev/null "http://localhost:${REF_PORT}/"; then
    break
  fi
  if [ "$i" = "90" ]; then
    echo "Pinned reference server never became ready:" >&2
    "${COMPOSE[@]}" logs --tail=80 zero-ref >&2 || true
    exit 1
  fi
  sleep 2
done

# v1.7.0 reads deployed permissions from zero.permissions; ZERO_SCHEMA_JSON is
# configuration for schema validation, not a permission deployment command.
# Seed the same allow-all policy used by the benchmark schema after the
# reference creates its internal table, then let logical replication catch up.
docker exec zero-bench-pg-ref psql -U postgres -d zero -v ON_ERROR_STOP=1 -c \
  "INSERT INTO zero.permissions (permissions) VALUES ('{\"tables\":{\"issue\":{\"row\":{\"select\":[[\"allow\",{\"type\":\"and\",\"conditions\":[]}]]}}}}'::jsonb) ON CONFLICT (lock) DO UPDATE SET permissions = EXCLUDED.permissions;" >/dev/null
sleep 2

echo "==> comparing init, hydration, reconnect, and live update/delete transcripts…"
ZERO_CONFORMANCE_RUST_URL="ws://127.0.0.1:${RUST_PORT}" \
ZERO_CONFORMANCE_REFERENCE_URL="ws://127.0.0.1:${REF_PORT}" \
ZERO_CONFORMANCE_RUST_PG_URL="postgresql://postgres:postgres@127.0.0.1:${PG_RUST_PORT}/zero" \
ZERO_CONFORMANCE_REFERENCE_PG_URL="postgresql://postgres:postgres@127.0.0.1:${PG_REF_PORT}/zero" \
  cargo test -p zero-cache-server --test reference_conformance -- --ignored --nocapture
