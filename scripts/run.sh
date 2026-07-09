#!/usr/bin/env bash
# scripts/run.sh — builds and runs the zero-cache server.
#
#   scripts/run.sh             run locally (cargo, release build); all ZERO_*
#                              env vars pass straight through, e.g.:
#                                ZERO_UPSTREAM_DB="postgres://…" \
#                                ZERO_REPLICA_FILE=./replica.db scripts/run.sh
#
#   scripts/run.sh --docker    quick-start Docker stack instead
#                              (docker-compose.yml: Postgres + server)
#
# Without ZERO_UPSTREAM_DB the server runs standalone (protocol only); with it,
# it replicates the upstream Postgres and serves real data.
# Ports: 4848 sync WebSocket, 9600 ops (/metrics /healthz /readyz).

set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${1:-}" = "--docker" ]; then
  exec docker compose up --build
fi

if [ -z "${ZERO_UPSTREAM_DB:-}" ]; then
  echo "note: ZERO_UPSTREAM_DB is not set — running standalone (protocol only)."
  echo "      set it to a Postgres URL/conn-string to serve real data."
fi

exec cargo run --release -p zero-cache-server --bin zero-cache-server
