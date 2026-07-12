#!/usr/bin/env bash
# test.sh — runs the full zero-cache-rust test suite.
#
#   ./test.sh              unit + integration tests (live-Postgres tests skip
#                          gracefully when no test database is reachable)
#   ./test.sh --with-pg    also spin up a disposable Postgres in Docker so the
#                          live replication/CVR/mutation tests run for real
#
# Live tests share one database, so everything runs single-threaded
# (--test-threads=1); parallel runs would corrupt each other's state.

set -euo pipefail
cd "$(dirname "$0")/.."
UI_SCRIPT_NAME=test
# shellcheck source=scripts/lib/ui.sh
source scripts/lib/ui.sh

PG_CONTAINER=zero-test-pg
WITH_PG=0
PG_IMAGE="${ZERO_TEST_PG_IMAGE:-postgres:16}"
PG_PORT="${ZERO_TEST_PG_PORT:-54329}"
PG_STARTED=0
case "${1:-}" in
  --with-pg) WITH_PG=1 ;;
  -h|--help)
    printf 'Usage: scripts/test.sh [--with-pg]\n\nRun all tests serially. --with-pg adds live Postgres tests.\n'
    exit 0
    ;;
  "") ;;
  *) ui_error "Unknown argument: $1"; exit 2 ;;
esac

cleanup() {
  local code=$?
  trap - EXIT
  if [ "$PG_STARTED" = "1" ]; then
    if [ "${KEEP_UP:-0}" = "1" ]; then
      ui_note "Test Postgres left running: $PG_CONTAINER"
    else
      ui_run "Remove test Postgres" docker rm -f "$PG_CONTAINER" || true
    fi
  fi
  exit "$code"
}
trap cleanup EXIT

ui_banner "Test suite" "$([ "$WITH_PG" = 1 ] && printf 'Workspace + live Postgres' || printf 'Workspace tests')"

if [ "$WITH_PG" = "1" ]; then
  command -v docker >/dev/null 2>&1 || { ui_error "Docker is required for --with-pg"; exit 1; }
  docker info >/dev/null 2>&1 || { ui_error "Docker is not running"; exit 1; }
  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
  PG_STARTED=1
  # SSL is enabled (self-signed cert generated at container start) so the
  # live tests exercise the same TLS paths managed Postgres (RDS with
  # rds.force_ssl) requires: sslmode=require over both tokio-postgres and the
  # raw replication-protocol connection.
  ui_run "Start disposable Postgres" docker run -d --name "$PG_CONTAINER" \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p "$PG_PORT:5432" \
    "$PG_IMAGE" \
    bash -c "openssl req -new -x509 -days 3650 -nodes -subj /CN=localhost \
        -keyout /var/lib/postgresql/server.key -out /var/lib/postgresql/server.crt \
      && chown postgres:postgres /var/lib/postgresql/server.key /var/lib/postgresql/server.crt \
      && chmod 600 /var/lib/postgresql/server.key \
      && exec docker-entrypoint.sh postgres -c wal_level=logical \
        -c max_wal_senders=20 -c max_replication_slots=20 \
        -c ssl=on -c ssl_cert_file=/var/lib/postgresql/server.crt \
        -c ssl_key_file=/var/lib/postgresql/server.key"
  ui_wait_for "Wait for Postgres" 60 1 docker exec "$PG_CONTAINER" pg_isready -U postgres
  export ZERO_TEST_PG_URL="host=localhost port=$PG_PORT user=postgres dbname=postgres"
  export ZERO_TEST_PG="host=localhost port=$PG_PORT user=postgres dbname=postgres"
  export ZERO_TEST_PG_TCP="localhost:$PG_PORT"
fi

command -v cargo >/dev/null 2>&1 || { ui_error "Cargo is required but was not found"; exit 1; }
command -v node >/dev/null 2>&1 || { ui_error "Node.js is required for the official JS-client test"; exit 1; }
command -v npm >/dev/null 2>&1 || { ui_error "npm is required for the official JS-client test"; exit 1; }
ui_run "Install pinned JS client" npm --prefix js-client-e2e ci --ignore-scripts
ui_run "Run workspace + official JS-client tests" env ZERO_RUN_JS_CLIENT_E2E=1 cargo test --workspace -- --test-threads=1

run_loadtest_tests() { (cd bench/loadtest && cargo test); }
ui_run "Run load-driver tests" run_loadtest_tests

printf '\n'
ui_success "All tests passed"
if [ "$WITH_PG" = "0" ]; then
  ui_note "Live Postgres tests skipped; use scripts/test.sh --with-pg to include them"
fi
ui_logs_note
