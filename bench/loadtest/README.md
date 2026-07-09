# Load + benchmark harness (`zero-loadtest`)

Drives **hundreds to thousands of concurrent WebSocket clients** against a
zero-cache server and reports latency percentiles, throughput, connection
success, and (optionally) per-container CPU/memory — with a `--compare` mode for
a head-to-head against the real `rocicorp/zero`.

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
sustained `ping`/`pong` loop (measuring RTT), counting any interim frames
(pokes). The report gives connect latency, ping RTT p50/p90/p99/max, throughput,
connection success rate, and an error breakdown.

### Options (flag or env; flag wins)

| flag / env | default | meaning |
| --- | --- | --- |
| `--url` / `LOAD_URL` | `ws://127.0.0.1:4848/sync` | target server |
| `--clients` / `LOAD_CLIENTS` | `1000` | concurrent clients |
| `--duration` / `LOAD_DURATION` | `20` | seconds of sustained load |
| `--ramp` / `LOAD_RAMP` | `5` | seconds to stagger connects (avoid thundering herd) |
| `--ping-interval` / `LOAD_PING_MS` | `250` | ms between pings per client |
| `--burst` | off | all clients connect at once (thundering-herd stress) |
| `--container` / `LOAD_CONTAINER` | – | docker container name to sample CPU/mem via `docker stats` |
| `--compare` + `--ref-url` + `--ref-container` | – | also run against a reference server and print a side-by-side table |

At high client counts you **must** raise the file-descriptor limit
(`ulimit -n 100000`) — each client is one socket.

## Benchmark vs rocicorp/zero

Bring up both servers (see `conformance/docker-compose.conformance.yml`, which
runs this Rust server on 4848 and `rocicorp/zero` on 4849 against one Postgres),
then:

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
