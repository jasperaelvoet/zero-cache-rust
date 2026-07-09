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
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/zero-cache-server /usr/local/bin/zero-cache-server

# Defaults (override via `docker run -e` / compose `environment`).
ENV ZERO_LISTEN_ADDR=0.0.0.0:4848 \
    ZERO_FANOUT_CAPACITY=1024

EXPOSE 4848

ENTRYPOINT ["zero-cache-server"]
