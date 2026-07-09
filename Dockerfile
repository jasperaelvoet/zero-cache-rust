# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
# The workspace `.cargo/config.toml` sets
# LIBSQLITE3_FLAGS=SQLITE_ENABLE_STMT_SCANSTATUS, so the bundled SQLite is built
# with the scanstatus API the query-planner cost model uses. rusqlite's
# `bundled` feature compiles SQLite from source (needs a C toolchain, present in
# the rust image); reqwest uses rustls (no OpenSSL dev headers required).
FROM rust:1-bookworm AS builder

WORKDIR /build
COPY . .

# Build only the server binary in release mode.
RUN --mount=type=cache,target=/build/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --bin zero-cache-server && \
    cp target/release/zero-cache-server /usr/local/bin/zero-cache-server

# ---- Runtime stage ---------------------------------------------------------
# Slim runtime: only needs glibc/libgcc (dynamically linked) plus
# ca-certificates for the OTLP exporter's HTTPS pushes.
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Bundle the litestream binary so ZERO_LITESTREAM_BACKUP_URL works out of the
# box (S3/object-store continuous backup + restore). TARGETARCH is amd64/arm64,
# matching litestream's release asset names.
#
# NOTE on backup-format compatibility: apps deploying the upstream rocicorp/zero
# image (e.g. hunting-game's Dockerfile.zero) bundle rocicorp's litestream FORK
# (0.3.13+z0.0.9) plus litestream v5 (0.5.11) for the newer replica format. To
# RESTORE an EXISTING backup produced by those binaries, build/copy the matching
# binary here instead of the stock release below. If the format doesn't match,
# `litestream::restore` simply reports no replica and this server falls back to
# a full Postgres initial-sync (correct, just a slower cold start) — restore is
# an optimization, never a correctness dependency.
ARG TARGETARCH
ARG LITESTREAM_VERSION=v0.3.13
RUN curl -fsSL \
      "https://github.com/benbjohnson/litestream/releases/download/${LITESTREAM_VERSION}/litestream-${LITESTREAM_VERSION}-linux-${TARGETARCH}.tar.gz" \
      | tar -xz -C /usr/local/bin litestream \
    && litestream version

COPY --from=builder /usr/local/bin/zero-cache-server /usr/local/bin/zero-cache-server

# Defaults (override via `docker run -e` / compose `environment`).
ENV ZERO_PORT=4848 \
    ZERO_METRICS_ADDR=0.0.0.0:9600 \
    ZERO_FANOUT_CAPACITY=1024

EXPOSE 4848 9600

ENTRYPOINT ["zero-cache-server"]
