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
UI_SCRIPT_NAME=run
# shellcheck source=scripts/lib/ui.sh
source scripts/lib/ui.sh

MODE=local
case "${1:-}" in
  --docker) MODE=docker ;;
  -h|--help)
    printf 'Usage: scripts/run.sh [--docker]\n\nBuild and run locally, or start the Docker Compose stack in the background.\n'
    exit 0
    ;;
  "") ;;
  *) ui_error "Unknown argument: $1"; exit 2 ;;
esac

ui_banner "Zero Cache" "Development server · $MODE mode"

if [ "$MODE" = "docker" ]; then
  command -v docker >/dev/null 2>&1 || { ui_error "Docker is required but was not found"; exit 1; }
  command -v curl >/dev/null 2>&1 || { ui_error "curl is required but was not found"; exit 1; }
  docker info >/dev/null 2>&1 || { ui_error "Docker is not running"; exit 1; }
  ui_run "Build and start Docker services" docker compose up -d --build
  docker_ready() { curl -fsS http://localhost:9600/readyz; }
  if ! ui_wait_for "Wait for server readiness" 180 2 docker_ready; then
    docker compose logs --tail=40 zero-cache >&2 || true
    exit 1
  fi
  printf '\n'
  ui_success "Docker stack is ready"
  ui_note "Sync ws://localhost:4848 · operations http://localhost:9600"
  ui_note "Stop it with: docker compose down"
  ui_logs_note
  exit 0
fi

if [ -z "${ZERO_UPSTREAM_DB:-}" ]; then
  ui_warn "ZERO_UPSTREAM_DB is not set; protocol-only mode will be used"
fi

command -v cargo >/dev/null 2>&1 || { ui_error "Cargo is required but was not found"; exit 1; }
ui_run "Build release server" cargo build --release -p zero-cache-server --bin zero-cache-server
ui_success "Server ready to start"
ui_note "Sync ws://localhost:4848 · operations http://localhost:9600"
ui_note "Press Ctrl-C to stop"
exec target/release/zero-cache-server
