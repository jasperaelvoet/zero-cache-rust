# Load + benchmark harness (`zero-loadtest`)

Drives **hundreds to thousands of concurrent WebSocket clients** against a
zero-cache server and reports latency percentiles, throughput, connection
success, and (optionally) per-container CPU/memory — with a `--compare` mode for
a head-to-head against the real `rocicorp/zero`.

The checked-in comparison stack pins its reference to upstream `zero/v1.7.0`
(commit `6863de5`, sync protocol v51), using an immutable OCI manifest digest.
Do not use `rocicorp/zero:latest` for compatibility or performance claims.

Standalone crate (its own workspace), so `cargo build --workspace` at the repo
root doesn't build it.

## Run

```sh
# 1. Start a server (standalone is enough for connect/ping load; use synced mode
#    with ZERO_UPSTREAM_DB for data workloads).
cargo run -p zero-cache-server --bin zero-cache-server &

# 2. Drive load (raise the fd limit first for large client counts).
ulimit -n 100000
cd bench/loadtest
cargo run --release --bin zero-loadtest -- \
  --url ws://127.0.0.1:4848/sync --clients 2000 --duration 30 --ramp 5
```

Each client: connect → wait for the `connected` greeting → `initConnection` →
the selected workload. Setup has its own timeout, and every successfully
initialized client receives the full requested sustain duration; a late cold
client group is never given a shortened run. The default `ping` workload
retains the original control-plane `ping`/`pong` loop; the data-path modes record
completed hydration pokes, reconnect handshakes, and live fan-out pokes as
appropriate. The report gives connect latency, hydration/reconnect latency
where applicable, ping RTT, throughput normalized to the equal per-client
sustain window, connection success, and an error breakdown. A ping client must
receive at least one pong before it counts as successfully initialized and
connected.

### Options (flag or env; flag wins)

| flag / env | default | meaning |
| --- | --- | --- |
| `--url` / `LOAD_URL` | `ws://127.0.0.1:4848/sync` | target server |
| `--clients` / `LOAD_CLIENTS` | `1000` | concurrent clients |
| `--duration` / `LOAD_DURATION` | `20` | seconds of sustained load |
| `--ramp` / `LOAD_RAMP` | `5` | seconds to stagger connects (avoid thundering herd) |
| `--setup-timeout` / `LOAD_SETUP_TIMEOUT` | `120` | timeout in seconds for each connect/greeting/hydration setup phase |
| `--ping-interval` / `LOAD_PING_MS` | `250` | ms between pings per client |
| `--burst` | off | all clients connect at once (thundering-herd stress) |
| `--workload` / `LOAD_WORKLOAD` | `ping` | `ping`, `hydrate`, `fanout`, or `reconnect`; data-path modes send a nonempty query |
| `--query-patch` / `LOAD_QUERY_PATCH` | seeded `issue` table query | JSON desired-query patch for data-path workloads |
| `--client-schema` / `LOAD_CLIENT_SCHEMA` | seeded `issue` schema | JSON client schema for a fresh client group |
| `--fanout-min-pokes` / `LOAD_FANOUT_MIN_POKES` | `0` | in `fanout` mode, require this many post-hydration pokes per client |
| `--container` / `LOAD_CONTAINER` | – | docker container name to sample CPU/mem via `docker stats` |
| `--compare` + `--ref-url` + `--ref-container` | – | also run against a reference server and print a side-by-side table |

At high client counts you **must** raise the file-descriptor limit
(`ulimit -n 100000`) — each client is one socket.

### Data-path workload examples

```sh
# Cold hydration: each client requests the nonempty seeded issue query and
# must receive a completed pokeEnd before it is counted as healthy.
cargo run --release --bin zero-loadtest -- \
  --workload hydrate --clients 500 --duration 20 --url ws://127.0.0.1:4848

# Reconnect/catch-up: hydrate, close, reconnect with the returned cookie, then
# keep the reconnected socket alive with pings.
cargo run --release --bin zero-loadtest -- \
  --workload reconnect --clients 500 --duration 20 --url ws://127.0.0.1:4848

# Live fan-out: run an external Postgres writer while clients hold the query.
# With a nonzero threshold each client must observe that many post-hydration
# pokeStart frames; this avoids treating an idle system as a fan-out result.
cargo run --release --bin zero-loadtest -- \
  --workload fanout --fanout-min-pokes 1 --clients 500 --duration 20 \
  --url ws://127.0.0.1:4848
```

`scripts/bench.sh` starts matched writers against both isolated source
databases automatically when `LOAD_WORKLOAD=fanout`; use
`ZERO_BENCH_FANOUT_INTERVAL_S` to tune their cadence. It also defaults
`LOAD_FANOUT_MIN_POKES=1` in that mode, so its result is not an idle fan-out
comparison.

The built-in data-path defaults target the deterministic `issue` table from
`bench/seed.sql`. For a different app, supply **both** its desired-query patch
and its client schema; do not accidentally benchmark an empty patch.

## Benchmark vs rocicorp/zero

Bring up both servers with the pinned stack, then:

```sh
KEEP_UP=1 scripts/bench.sh 1 1
```

This runs the same seed data through isolated Postgres instances (one per
server) and leaves the stack up. Then run:

```sh
cargo run --release --bin zero-loadtest -- \
  --clients 2000 --duration 30 \
  --url ws://127.0.0.1:4848/sync   --container zero-rust \
  --compare --ref-url ws://127.0.0.1:4849/sync --ref-container zero-ref
```

This prints each server's report plus a comparison table (connection success,
ping p50/p99, throughput, peak CPU, peak mem). Resource numbers require the
containers to be named (`--container`/`--ref-container`) and `docker` on PATH.

## Example (300 clients, local debug build)

```
── ws://127.0.0.1:4848/sync ──
  clients: 300  connected: 300 (100.0%)  duration: 7.2s
  connect ms:  p50 1.0  p99 3.8  max 8.0
  ping RTT ms: p50 0.58  p90 2.20  p99 8.03  max 11.59  (n=9052)
  throughput:  1258 pings/s   frames rcvd: 9352
```

## Tests

```sh
cargo test --lib    # stats/percentiles/resource-parsing, no server needed
```
