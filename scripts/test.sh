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

PG_CONTAINER=zero-test-pg
WITH_PG=0
[ "${1:-}" = "--with-pg" ] && WITH_PG=1

cleanup() {
  if [ "$WITH_PG" = "1" ]; then
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ "$WITH_PG" = "1" ]; then
  echo "==> starting disposable test Postgres (logical replication enabled)…"
  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$PG_CONTAINER" \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p 54329:5432 \
    postgres:16 \
    postgres -c wal_level=logical -c max_wal_senders=20 -c max_replication_slots=20 \
    >/dev/null
  echo "==> waiting for Postgres…"
  until docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
  export ZERO_TEST_PG_URL="host=localhost port=54329 user=postgres dbname=postgres"
  export ZERO_TEST_PG_TCP="localhost:54329"
fi

echo "==> workspace tests (serial — live tests share one database)…"
cargo test --workspace -- --test-threads=1

echo "==> bench harness unit tests…"
( cd bench/loadtest && cargo test )

echo
echo "All tests passed."
if [ "$WITH_PG" = "0" ]; then
  echo "(live-Postgres tests were skipped — run ./test.sh --with-pg to include them)"
fi
