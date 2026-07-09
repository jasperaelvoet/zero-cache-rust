# Conformance harness

A **differential** test harness that drives this Rust `zero-cache` and the
official [`rocicorp/zero`](https://hub.docker.com/r/rocicorp/zero) with the
*identical* sequence of WebSocket client frames, normalizes away
non-deterministic values, and asserts the two servers behave the same.

It is transport-only — it speaks the wire protocol and compares what a real
client observes, not internals — so it works against any two `ws://` endpoints.

## How it works

- [`src/lib.rs`](src/lib.rs) — the harness:
  - `WsClient` — a thin tokio-tungstenite client (`send` / `recv` with a quiet
    timeout / `drain`).
  - `Scenario` — a scripted `Send` / `Expect(n)` sequence.
  - `run(url, scenario)` — replays a scenario and returns the **normalized**
    responses.
  - `normalize_frame` — parses each frame as JSON and masks volatile values
    (`wsid`, `timestamp`, `pokeID`, `cookie`, `lastMutationID`, …) with
    type-tagged placeholders, so only structural/ordering differences remain.
  - `diff(a, b)` — a readable per-step diff, or `None` when equivalent.
  - `scenarios()` — the battery (see below).
- [`src/main.rs`](src/main.rs) — a CLI that runs the battery against both
  servers and prints `PASS` / `DIFF`.
- [`tests/differential.rs`](tests/differential.rs) — the env-gated tests.

The harness's own logic (normalization, diffing, scenario building) is covered
by unit tests that run with **no servers**:

```sh
cargo test -p zero-conformance         # harness unit tests, always run
```

## Scenario battery

Protocol behaviors observable **without** a seeded schema/dataset, so they
compare across servers out of the box:

| scenario           | checks                                                    |
| ------------------ | --------------------------------------------------------- |
| `handshake`        | the `connected` greeting frame's shape                    |
| `ping_pong`        | `ping` is answered with `pong`                            |
| `init_then_ping`   | `initConnection` then `ping` → correct ordering/replies   |
| `data_before_init` | a data message before `initConnection` (protocol error)   |
| `malformed_frame`  | a non-JSON frame is handled the same way                  |

Data-level scenarios (queries → pokes) require both servers to be backed by the
**same schema and data**; add them once the reference container is seeded — the
harness (`Scenario` + normalization) already supports arbitrary send/expect
sequences.

## Running the real differential

Bring up both servers + Postgres:

```sh
docker compose -f conformance/docker-compose.conformance.yml up --build
```

Then, from the repo root:

```sh
ZERO_RUST_URL=ws://127.0.0.1:4848/sync \
ZERO_REF_URL=ws://127.0.0.1:4849/sync \
cargo test -p zero-conformance

# or the CLI, with a PASS/DIFF report:
ZERO_RUST_URL=ws://127.0.0.1:4848/sync \
ZERO_REF_URL=ws://127.0.0.1:4849/sync \
cargo run -p zero-conformance --bin conformance
```

With only `ZERO_RUST_URL` set, the harness runs a **self-check** (Rust vs Rust)
that validates determinism and the harness itself.

## Caveats (honest)

- The official `rocicorp/zero` requires an **app schema**, an **auth secret**,
  and a **replica file**; the compose env is a starting point — set
  `ZERO_AUTH_SECRET` / `ZERO_SCHEMA_*` to your app's values. Real zero may also
  require auth params in the connection URL, which the handshake scenarios don't
  supply.
- This Rust server's `main` currently serves the WebSocket protocol with a
  per-connection view-syncer handler over an in-memory replica; the supervised
  replicator loop is built and tested (see [`../PORTING.md`](../PORTING.md)) but
  not yet wired into `main`. So the **protocol/handshake** scenarios are the
  meaningful comparison today; **data-sync** scenarios become meaningful once
  the replicator is wired and both servers share seeded data.
