# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
# Alpine/musl build. `rust:1-alpine` targets *-unknown-linux-musl by default and
# links libc statically (+crt-static), so the server is a single self-contained
# binary with no dynamic loader — the runtime image needs no shared libraries.
#
# The bundled SQLite (the vendored `libsqlite3-sys`, which carries Zero v1.7's
# exact SQLITE_ENABLE_STMT_SCANSTATUS build) and jemalloc (`jemalloc-sys`) both
# compile from C source, so the musl C toolchain must be present: `build-base`
# pulls in gcc + make + musl-dev. reqwest and tokio-postgres use rustls (ring),
# so there is deliberately no OpenSSL build dependency.
FROM rust:1-alpine AS builder

RUN apk add --no-cache build-base

WORKDIR /build
COPY . .

# musl statically links the MAIN thread's stack at 128 KiB (glibc instead gives
# the initial thread the kernel's ~8 MiB rlimit). `block_on` drives `async_main`
# on the main thread, and the planner / IVM / SQLite paths recurse deep, so size
# the main-thread stack to 8 MiB via PT_GNU_STACK at link time. This is a
# link-only flag: it does NOT touch the SQLite compile options the vendored
# `libsqlite3-sys` pins, so database semantics are byte-for-byte unchanged.
# (Child threads are covered by RUST_MIN_STACK in the runtime stage.)
ENV RUSTFLAGS="-C link-arg=-Wl,-z,stack-size=8388608"

# Build only the server binary in release mode. rust:alpine's host target is the
# musl triple, so the artifact lands in target/release (not target/<triple>).
RUN --mount=type=cache,target=/build/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --bin zero-cache-server && \
    cp target/release/zero-cache-server /usr/local/bin/zero-cache-server

# ---- Runtime stage ---------------------------------------------------------
# alpine:3.23 matches the upstream rocicorp/zero base (hunting-game's
# Dockerfile.zero), so the port and the official image share an OS baseline. The
# static musl binary needs no runtime libraries; ca-certificates covers the OTLP
# exporter's HTTPS pushes and the custom mutate/query calls, curl is for
# container healthchecks.
FROM alpine:3.23

RUN apk add --no-cache ca-certificates curl

# Bundle the litestream binary so ZERO_LITESTREAM_BACKUP_URL works out of the box
# (S3/object-store continuous backup + restore). TARGETARCH is amd64/arm64,
# matching litestream's release asset names; the releases are statically-linked
# Go, so they run on musl unchanged.
#
# NOTE on backup-format compatibility: apps deploying the upstream rocicorp/zero
# image bundle rocicorp's litestream FORK (0.3.13+z0.0.9) plus litestream v5
# (0.5.11) for the newer replica format. To RESTORE an EXISTING backup produced
# by those binaries, build/copy the matching binary here instead of the stock
# release below. If the format doesn't match, `litestream::restore` simply
# reports no replica and this server falls back to a full Postgres initial-sync
# (correct, just a slower cold start) — restore is an optimization, never a
# correctness dependency.
ARG TARGETARCH
ARG LITESTREAM_VERSION=v0.3.13
RUN curl -fsSL \
      "https://github.com/benbjohnson/litestream/releases/download/${LITESTREAM_VERSION}/litestream-${LITESTREAM_VERSION}-linux-${TARGETARCH}.tar.gz" \
      | tar -xz -C /usr/local/bin litestream \
    && litestream version

COPY --from=builder /usr/local/bin/zero-cache-server /usr/local/bin/zero-cache-server

# Rust's std sets a 2 MiB stack on threads it spawns (on musl and glibc alike),
# overriding musl's tiny 128 KiB pthread default — so the load-bearing musl fix
# is the main-thread link arg above, not this. RUST_MIN_STACK is headroom
# insurance: the replicator / view-syncer / shadow-sync-canary threads (spawned
# via std::thread::Builder with no explicit stack_size) run the deep SQLite + IVM
# call stacks, so give them 8 MiB. Tokio worker threads use Tokio's own default.
#
# _RJEM_MALLOC_CONF tunes jemalloc (the global allocator linked into the binary):
# dirty_decay_ms:1000 + muzzy_decay_ms:0 return freed pages to the OS ~1s after
# they go idle, so RSS falls back after the initial-sync allocation spike instead
# of being retained. background_thread:true purges proactively where supported;
# on a musl static binary jemalloc may fall back to foreground purging — harmless,
# decay still applies on subsequent allocator activity.
ENV ZERO_PORT=4848 \
    RUST_MIN_STACK=8388608 \
    _RJEM_MALLOC_CONF="background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0"

EXPOSE 4848 4849

ENTRYPOINT ["zero-cache-server"]
