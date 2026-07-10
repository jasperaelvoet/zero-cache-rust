#!/usr/bin/env bash
# Small local control loop analogous to an HPA. It reads active WebSocket
# sessions from HAProxy and worker CPU from Docker, then asks Compose to change
# the number of view-syncer replicas with scale-in stabilization and cooldowns.

set -euo pipefail

case "${1:-}" in
  -h|--help)
    printf 'Usage: simulation/autoscaler.sh\n\nRun the local simulation autoscaling control loop.\n'
    exit 0
    ;;
  "") ;;
  *) printf 'error: unknown argument: %s\n' "$1" >&2; exit 2 ;;
esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="${SIM_COMPOSE_FILE:-$ROOT/simulation/docker-compose.yml}"
PROJECT="${SIM_PROJECT:-zero-prod-sim}"
STATS_URL="http://127.0.0.1:${SIM_HAPROXY_STATS_PORT:-8404}/stats;csv"
MIN_REPLICAS="${SIM_MIN_REPLICAS:-1}"
MAX_REPLICAS="${SIM_MAX_REPLICAS:-6}"
TARGET_CONNECTIONS="${SIM_TARGET_CONNECTIONS:-180}"
SCALE_OUT_PERCENT="${SIM_SCALE_OUT_PERCENT:-80}"
SCALE_IN_PERCENT="${SIM_SCALE_IN_PERCENT:-35}"
CPU_HIGH="${SIM_CPU_HIGH:-85}"
CPU_LOW="${SIM_CPU_LOW:-35}"
INTERVAL="${SIM_SCALE_INTERVAL:-5}"
COOLDOWN="${SIM_SCALE_COOLDOWN:-20}"
LOW_STREAK_REQUIRED="${SIM_SCALE_IN_STREAK:-3}"
EVENT_LOG="${SIM_EVENT_LOG:-/dev/null}"

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

replica_count() {
  compose ps -q view-syncer | awk 'NF { n++ } END { print n + 0 }'
}

active_sessions() {
  curl -fsS "$STATS_URL" 2>/dev/null | awk -F, '
    $1 == "zero_view_syncers" && $2 != "BACKEND" { sessions += $5 }
    END { print sessions + 0 }
  '
}

average_cpu() {
  local ids
  ids="$(compose ps -q view-syncer)"
  if [ -z "$ids" ]; then
    echo 0
    return
  fi
  # shellcheck disable=SC2086 # docker expects one argument per container id.
  docker stats --no-stream --format '{{.CPUPerc}}' $ids 2>/dev/null | awk '
    { gsub(/%/, "", $1); total += $1; n++ }
    END { if (n) printf "%.1f", total / n; else print 0 }
  '
}

greater_or_equal() {
  awk -v left="$1" -v right="$2" 'BEGIN { exit !(left >= right) }'
}

less_than() {
  awk -v left="$1" -v right="$2" 'BEGIN { exit !(left < right) }'
}

record() {
  local line
  line="$(date -u +%Y-%m-%dT%H:%M:%SZ),$1,$2,$3,$4"
  echo "autoscaler: replicas=$1 sessions=$2 avg_cpu=${3}% action=$4"
  echo "$line" >> "$EVENT_LOG"
}

mkdir -p "$(dirname "$EVENT_LOG")"
if [ "$EVENT_LOG" != /dev/null ] && [ ! -s "$EVENT_LOG" ]; then
  echo "timestamp,replicas,active_sessions,avg_cpu_pct,action" > "$EVENT_LOG"
fi

last_scale=0
low_streak=0
while :; do
  replicas="$(replica_count)"
  sessions="$(active_sessions || echo 0)"
  cpu="$(average_cpu)"
  now="$(date +%s)"
  action="hold"

  if [ "$replicas" -lt "$MIN_REPLICAS" ]; then
    desired="$MIN_REPLICAS"
    action="restore-min"
  else
    out_threshold=$((replicas * TARGET_CONNECTIONS * SCALE_OUT_PERCENT / 100))
    # Scale-in compares against the capacity of one fewer replica so it does
    # not immediately reverse a recent scale-out near a single threshold.
    in_threshold=$(((replicas - 1) * TARGET_CONNECTIONS * SCALE_IN_PERCENT / 100))
    desired="$replicas"
    if [ "$replicas" -lt "$MAX_REPLICAS" ] && \
       { [ "$sessions" -ge "$out_threshold" ] || greater_or_equal "$cpu" "$CPU_HIGH"; }; then
      desired=$((replicas + 1))
      low_streak=0
      action="scale-out"
    elif [ "$replicas" -gt "$MIN_REPLICAS" ] && \
         [ "$sessions" -le "$in_threshold" ] && less_than "$cpu" "$CPU_LOW"; then
      low_streak=$((low_streak + 1))
      if [ "$low_streak" -ge "$LOW_STREAK_REQUIRED" ]; then
        desired=$((replicas - 1))
        low_streak=0
        action="scale-in"
      else
        action="stabilizing-${low_streak}/${LOW_STREAK_REQUIRED}"
      fi
    else
      low_streak=0
    fi
  fi

  if [ "$desired" -ne "$replicas" ]; then
    if [ $((now - last_scale)) -ge "$COOLDOWN" ]; then
      compose up -d --no-build --scale view-syncer="$desired" view-syncer >/dev/null 2>&1
      last_scale="$now"
      replicas="$desired"
    else
      action="cooldown"
    fi
  fi

  record "$replicas" "$sessions" "$cpu" "$action"
  sleep "$INTERVAL"
done
