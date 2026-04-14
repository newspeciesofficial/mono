# zero-cache-rs

Rust implementation of Zero's server-side sync engine (`zero-cache`).

## Quick Start

```bash
# Build
cargo build --release

# Run (requires PostgreSQL)
ZERO_UPSTREAM_DB="postgres://user:pass@localhost:5432/mydb" \
  cargo run --release -p zero-cache-server

# Test
cargo test --workspace

# Benchmark
cargo bench -p zero-cache-types --bench serialization
```

Default port: **5858** (TS server uses 4848 — no conflict).

## Architecture

Single-process async server using tokio, replacing Node.js multi-process model.

```
crates/
├── types/          # Wire types: Row, AST, protocol messages, CVR
├── config/         # ZERO_* env vars via clap derive
├── observability/  # JSON logging (tracing) + OTEL metrics (same names as TS)
├── db/             # PgPool (shared), SqliteWriter (dedicated thread), SqliteReaderPool
├── replicator/     # Notifier (watch channel), schema, incremental syncer
├── streamer/       # ChangeStreamer: forward-store-ack pipeline
├── sync/           # ViewSyncer, PipelineDriver, Snapshotter, CVR store
└── server/         # Binary: axum HTTP/WS, dispatcher, graceful shutdown
```

## Key Design Decisions

- **Strict safe Rust**: `#![deny(unsafe_code)]` enforced workspace-wide
- **Zero-copy IPC**: `Arc<Vec<RowChange>>` through tokio channels (no structured clone)
- **Dedicated SQLite writer thread**: `std::thread` + `crossbeam_channel` (not tokio — SQLite is synchronous)
- **Concurrent SQLite readers**: Pool of read-only connections + `tokio::task::spawn_blocking`
- **Global PG pool**: Single `deadpool-postgres::Pool` shared across all tasks (no per-syncer partitioning)
- **mimalloc allocator**: 5.3x faster than glibc for small allocations

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ZERO_UPSTREAM_DB` | (required) | PostgreSQL connection string |
| `ZERO_CVR_DB` | upstream_db | CVR database connection |
| `ZERO_PORT` | 5858 | Sync WebSocket port |
| `ZERO_REPLICA_FILE` | zero.db | SQLite replica path |
| `ZERO_NUM_SYNC_WORKERS` | cpus-1 | Number of syncer tasks |
| `ZERO_LOG_LEVEL` | info | Log level (debug/info/warn/error) |
| `ZERO_APP_ID` | zero | Application identifier |

Full list: see `crates/config/src/lib.rs` or run `cargo run -p zero-cache-server -- --help`.

## Testing

```bash
cargo test --workspace              # 138 tests
cargo test -p zero-cache-types      # Types (24 tests)
cargo test -p zero-cache-db         # Database (25 tests)
cargo test -p zero-cache-replicator # Replicator (21 tests)
cargo test -p zero-cache-server     # Server (22 tests)
cargo clippy --workspace            # Lint
cargo fmt --all                     # Format
```

## Benchmarks

```bash
cargo bench -p zero-cache-types --bench serialization
```

| Operation | Rust | TS (structured clone) |
|-----------|------|----------------------|
| Row 10 cols serialize | 182 ns | ~30-60 µs |
| Batch 20 RowChanges | 4.3 µs | 30-60 µs |
| Binary size | 4.6 MB | ~50 MB per V8 isolate |

## Running Alongside TS Server

```bash
# Terminal 1: TS (port 4848)
cd apps/zbugs && npm run zero-cache-dev

# Terminal 2: Rust (port 5858)
cd packages/zero-cache-rs
ZERO_UPSTREAM_DB="postgres://user:pass@localhost:5432/zbugs" cargo run --release -p zero-cache-server
```

Both read the same PostgreSQL and SQLite — no conflicts.

## Remaining Work

1. **Full IVM pipeline** — port ZQL operator graph (joins, filters, exists)
2. **Change source client** — WebSocket subscription to change-streamer
3. **CVR flush** — full PG transaction for committed view records
4. **Auth/JWT** — token validation
5. **Query transformation** — server-side AST rewriting for permissions

See `docs/ts-vs-rs-comparison.md` for detailed file-by-file comparison.
