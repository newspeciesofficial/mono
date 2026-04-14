# Multi-stage build for zero-cache-rs
# Stage 1: Build the Rust binary
# Stage 2: Minimal runtime image (~20MB vs Node.js 432MB)

# ---------------------------------------------------------------------------
# Builder: compile on Debian with all build tools
# ---------------------------------------------------------------------------
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Cache dependencies by building a dummy project first
COPY Cargo.toml Cargo.lock* rust-toolchain.toml ./
COPY crates/types/Cargo.toml crates/types/Cargo.toml
COPY crates/config/Cargo.toml crates/config/Cargo.toml
COPY crates/observability/Cargo.toml crates/observability/Cargo.toml
COPY crates/db/Cargo.toml crates/db/Cargo.toml
COPY crates/replicator/Cargo.toml crates/replicator/Cargo.toml
COPY crates/streamer/Cargo.toml crates/streamer/Cargo.toml
COPY crates/sync/Cargo.toml crates/sync/Cargo.toml
COPY crates/server/Cargo.toml crates/server/Cargo.toml

# Create stub src files so cargo can resolve the workspace
RUN mkdir -p crates/types/src && echo "" > crates/types/src/lib.rs && \
    mkdir -p crates/config/src && echo "" > crates/config/src/lib.rs && \
    mkdir -p crates/observability/src && echo "" > crates/observability/src/lib.rs && \
    mkdir -p crates/db/src && echo "" > crates/db/src/lib.rs && \
    mkdir -p crates/replicator/src && echo "" > crates/replicator/src/lib.rs && \
    mkdir -p crates/streamer/src && echo "" > crates/streamer/src/lib.rs && \
    mkdir -p crates/sync/src && echo "" > crates/sync/src/lib.rs && \
    mkdir -p crates/server/src && echo 'fn main() {}' > crates/server/src/main.rs

# Build dependencies only (cached layer)
RUN cargo build --release -p zero-cache-server 2>/dev/null || true

# Now copy the real source
COPY crates/ crates/
COPY benches/ benches/

# Touch all source files to invalidate the cache for actual sources
RUN find crates -name "*.rs" -exec touch {} +

# Build the release binary
RUN cargo build --release -p zero-cache-server

# ---------------------------------------------------------------------------
# Runtime: minimal Debian slim (~80MB base, binary adds ~5MB)
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false zero

WORKDIR /opt/zero-cache

# Copy the binary
COPY --from=builder /build/target/release/zero-cache-server /usr/local/bin/zero-cache-server

# Default environment — matches TS server env vars exactly
ENV ZERO_PORT=4848
ENV ZERO_LOG_LEVEL=info
ENV ZERO_LOG_FORMAT=json
ENV ZERO_APP_ID=zero
ENV ZERO_SHARD_NUM=0
ENV ZERO_REPLICA_FILE=/var/zero/zero.db
ENV ZERO_IN_CONTAINER=1

# Create data directory
RUN mkdir -p /var/zero && chown zero:zero /var/zero

# Expose same ports as TS server
EXPOSE 4848 4849

USER zero

# Health check matching TS server
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${ZERO_PORT}/ || exit 1

ENTRYPOINT ["zero-cache-server"]
