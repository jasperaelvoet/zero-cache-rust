# Zero v1.7 compatibility contract

This repository targets exactly:

- Rocicorp mono commit `6863de5f00a3c1e7dc09c83ea3263dec4a94ebee`
- tag `zero/v1.7.0`
- sync protocol v51
- `@rocicorp/zero-sqlite3@1.1.2`

The checked-out reference is available under `mono-src/`; behavior is derived
from that pinned commit rather than the checkout's current `HEAD`.

## Authoritative surfaces

The official implementation defines:

- accepted configuration names, defaults, validation, and normalization;
- public HTTP and WebSocket routing and message ordering;
- replica, change-log, CVR, and permissions schemas;
- replication, resumption, reset, backup, and worker lifecycle behavior;
- ZQL planning, hydration, incremental advancement, and row synchronization;
- authentication, authorization, custom query, and mutation semantics;
- logging, metrics, inspection, health, readiness, and shutdown behavior.

Existing Rust APIs are internal and may change. An older Rust behavior is not a
compatibility requirement when it differs from the pinned server.

## Native SQLite

`rusqlite` remains the high-level API. The workspace patches
`libsqlite3-sys` with the official Zero amalgamation and compile definitions.
Startup verifies the exact version, required compile options, and
`BEGIN CONCURRENT`; replica creation verifies WAL2. The server fails fast if a
system or vanilla SQLite is linked accidentally.

Source provenance and checksums are recorded in
`vendor/libsqlite3-sys/ZERO-SQLITE3-SOURCE.md`.

## Acceptance gate

Completion requires differential conformance with the pinned official server
and 100% successful `ping`, `hydrate`, `fanout`, and `reconnect` workloads.
For each workload, p99 latency, throughput, CPU, and peak memory must be within
10% of official Zero. A comparison is invalid if either target fails setup,
times out, resets a connection, or misses required live pokes.
