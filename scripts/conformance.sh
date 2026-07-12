#!/usr/bin/env bash
# Run the black-box WebSocket transcript corpus against this Rust server and
# the immutable official Zero v1.7.0 reference. The Compose project contains
# only disposable benchmark/conformance data; this script removes its volumes
# unless KEEP_UP=1 is set for debugging.

set -euo pipefail
cd "$(dirname "$0")/.."
UI_SCRIPT_NAME=conformance
# shellcheck source=scripts/lib/ui.sh
source scripts/lib/ui.sh

RUST_PORT="${ZERO_BENCH_RUST_PORT:-4848}"
REF_PORT="${ZERO_BENCH_REF_PORT:-4849}"
PG_RUST_PORT="${ZERO_BENCH_PG_RUST_PORT:-5432}"
PG_REF_PORT="${ZERO_BENCH_PG_REF_PORT:-5433}"
COMPOSE=(docker compose -f bench/docker-compose.bench.yml)
STACK_STARTED=0

case "${1:-}" in
  -h|--help)
    printf 'Usage: scripts/conformance.sh\n\nCompare the black-box protocol corpus with official Zero v1.7.0.\n'
    exit 0
    ;;
  "") ;;
  *) ui_error "Unknown argument: $1"; exit 2 ;;
esac

cleanup() {
  local code=$?
  trap - EXIT INT TERM
  if [ "$STACK_STARTED" != "1" ]; then
    :
  elif [ "${KEEP_UP:-0}" != "1" ]; then
    ui_run "Remove conformance stack" "${COMPOSE[@]}" down -v || true
  else
    ui_note "Stack left running (KEEP_UP=1)"
    ui_note "Stop it with: ${COMPOSE[*]} down -v"
  fi
  exit "$code"
}
trap cleanup EXIT INT TERM

for command in docker cargo curl; do
  command -v "$command" >/dev/null 2>&1 || { ui_error "$command is required but was not found"; exit 1; }
done
docker info >/dev/null 2>&1 || { ui_error "Docker is not running"; exit 1; }
ui_banner "Protocol conformance" "Rust server vs official Zero v1.7.0"

postgres_ready() { docker exec "$1" psql -U postgres -d zero -Atqc 'SELECT 1'; }
seed_databases() {
  local pg
  for pg in zero-bench-pg-rust zero-bench-pg-ref; do
    docker exec -i "$pg" psql -U postgres -d zero < bench/seed.sql
  done
}
rust_ready() { curl -fsS "http://localhost:${RUST_PORT}/"; }
reference_ready() { curl -s -o /dev/null "http://localhost:${REF_PORT}/"; }
# The reference server responds on its HTTP port BEFORE it has finished creating
# its internal `zero.permissions` table (schema/replication init runs after the
# port opens). Waiting only on the port races the permissions INSERT below, which
# then fails with `relation "zero.permissions" does not exist`. Gate on the table
# actually existing.
reference_permissions_ready() {
  docker exec zero-bench-pg-ref psql -U postgres -d zero -Atqc \
    "SELECT to_regclass('zero.permissions') IS NOT NULL" | grep -q '^t$'
}

# A new corpus run must not inherit replicas, CVR state, or replication slots
# from an earlier run. This only addresses resources declared in the benchmark
# Compose file.
ui_run "Reset conformance stack" "${COMPOSE[@]}" down -v || true

STACK_STARTED=1
ui_run "Start isolated databases" "${COMPOSE[@]}" up -d postgres-rust postgres-ref
for pg in zero-bench-pg-rust zero-bench-pg-ref; do
  ui_wait_for "Wait for $pg" 90 1 postgres_ready "$pg"
done

ui_run "Seed identical source data" seed_databases

ui_run "Build and start both servers" "${COMPOSE[@]}" up -d --build zero-rust zero-ref

if ! ui_wait_for "Wait for Rust server" 180 2 rust_ready; then
  "${COMPOSE[@]}" logs --tail=80 zero-rust >&2 || true
  exit 1
fi

if ! ui_wait_for "Wait for official Zero" 180 2 reference_ready; then
  "${COMPOSE[@]}" logs --tail=80 zero-ref >&2 || true
  exit 1
fi

# v1.7.0 reads deployed permissions from zero.permissions; ZERO_SCHEMA_JSON is
# configuration for schema validation, not a permission deployment command.
# Seed the same allow-all policy used by the benchmark schema after the
# reference creates its internal table, then let logical replication catch up.
if ! ui_wait_for "Wait for reference permissions table" 120 2 reference_permissions_ready; then
  "${COMPOSE[@]}" logs --tail=80 zero-ref >&2 || true
  exit 1
fi
ui_run "Install reference permissions" docker exec zero-bench-pg-ref psql -U postgres -d zero -v ON_ERROR_STOP=1 -c \
  "INSERT INTO zero.permissions (permissions) VALUES ('{\"tables\":{\"issue\":{\"row\":{\"select\":[[\"allow\",{\"type\":\"and\",\"conditions\":[]}]]}}}}'::jsonb) ON CONFLICT (lock) DO UPDATE SET permissions = EXCLUDED.permissions;"
sleep 2

ui_run "Compare protocol transcripts" env \
  ZERO_CONFORMANCE_RUST_URL="ws://127.0.0.1:${RUST_PORT}" \
  ZERO_CONFORMANCE_REFERENCE_URL="ws://127.0.0.1:${REF_PORT}" \
  ZERO_CONFORMANCE_RUST_PG_URL="postgresql://postgres:postgres@127.0.0.1:${PG_RUST_PORT}/zero" \
  ZERO_CONFORMANCE_REFERENCE_PG_URL="postgresql://postgres:postgres@127.0.0.1:${PG_REF_PORT}/zero" \
  cargo test -p zero-cache-server --test reference_conformance -- --ignored --nocapture

printf '\n'
ui_success "All conformance scenarios passed"
ui_logs_note
