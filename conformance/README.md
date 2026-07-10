# Black-box reference conformance

`reference_conformance.rs` is deliberately an external-client test: it compares
the Rust binary with a real official Zero process over WebSockets, rather than
sharing any Rust implementation code with the system under test.

The reference is locked to this exact upstream release:

| field | value |
| --- | --- |
| upstream tag | `zero/v1.7.0` |
| upstream commit | `6863de5f00a3c1e7dc09c83ea3263dec4a94ebee` |
| sync protocol | v51 |
| OCI manifest | `rocicorp/zero@sha256:114363cd8ae889862c283dabc88fdb62fe183a46fe4ed0ea35ebeca8d325dba3` |

The test covers four deliberately small, reproducible scenarios:

1. A fresh client group receives `connected`, sends an `initConnection` with
   its mandatory `clientSchema`, and survives a `ping`/`pong`.
2. A fresh group requests the seeded `issue` table and must receive a complete
   hydration poke, not merely a successful WebSocket greeting.
3. That group reconnects using the cookie from its first `pokeEnd`; the trace
   preserves the cookie relationship while treating the opaque cookie value as
   server-generated.
4. A subscribed row is updated and then deleted in each target's Postgres;
   both servers must emit the same incremental row `put` and `del` sequence.

The comparison normalizes only `wsid`, timestamps, poke IDs, and opaque
cookies. Rows, query patches, nullable-vs-omitted fields, frame order, and
close/error behavior stay observable. A server closing during initialization
therefore fails the run instead of producing a misleading comparison.

## Run

```sh
scripts/conformance.sh
```

The script brings up two isolated Postgres databases, seeds both with
`bench/seed.sql`, starts the Rust server and the pinned reference, runs the
ignored integration test, and removes its disposable volumes. To inspect the
stack after a failure, set `KEEP_UP=1`.

To point the test at an already-running pair:

```sh
ZERO_CONFORMANCE_RUST_URL=ws://127.0.0.1:4848 \
ZERO_CONFORMANCE_REFERENCE_URL=ws://127.0.0.1:4849 \
ZERO_CONFORMANCE_RUST_PG_URL=postgresql://postgres:postgres@127.0.0.1:5432/zero \
ZERO_CONFORMANCE_REFERENCE_PG_URL=postgresql://postgres:postgres@127.0.0.1:5433/zero \
  cargo test -p zero-cache-server --test reference_conformance -- --ignored --nocapture
```

Set `ZERO_CONFORMANCE_SCENARIOS` to a comma-separated subset such as
`live-update-delete` while iterating.

This is a compatibility gate, not a benchmark. A difference is useful output:
fix the observed protocol behavior or explicitly narrow the product claim; do
not update the normalizer merely to conceal a semantic difference.

For app-side mutation replay and out-of-order batches, see the pinned
[mutation-ID contract](mutation-id-contract.md). The default stack has no
application mutate endpoint, so that transaction-level behavior is covered by
the Rust mutagen Postgres regression rather than silently faked in the
WebSocket-only corpus.
