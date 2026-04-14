# Multi-Replica Shards for Zero-Cache

## Problem

Zero-cache uses a single SQLite replica file that all sync workers read from. SQLite WAL2 mode has only **4 read-mark slots** shared across all readers. When multiple sync workers (each with Snapshotters opening concurrent transactions) compete for these 4 slots, `snapshotMs` increases from 0.1ms to 40ms+, creating a bottleneck.

On 30-core machines with many sync workers, this WAL contention limits throughput regardless of available CPU.

## Solution

Maintain **N independent SQLite replica files**, each with its own replicator process writing the same CDC data from the change-streamer. Sync workers are pinned to specific replica files, spreading WAL read-mark pressure across N × 4 slots.

```
                     PostgreSQL
                         │
                    (1 replication slot)
                         │
                  1 Change Streamer
                    │    │    │    │
                    ▼    ▼    ▼    ▼
               Replicator-0  Replicator-1  Replicator-2  Replicator-3
                    │         │              │              │
                    ▼         ▼              ▼              ▼
               replica.db  replica.db     replica.db     replica.db
                           -shard-1       -shard-2       -shard-3
                    │         │              │              │
                    ▼         ▼              ▼              ▼
               Syncer-0    Syncer-1       Syncer-2       Syncer-3
               (shard 0)   (shard 1)     (shard 2)      (shard 3)
```

### Key Properties

- **Only applies in zero-cache mode** (`numSyncWorkers > 0`). Replication-manager mode (`numSyncWorkers = 0`) is completely untouched.
- **Each shard replicator subscribes to the same change-streamer** via HTTP/WebSocket. Only ONE process connects to PostgreSQL (the change-streamer). Replicators are HTTP subscribers.
- **Each shard replicator is fully independent** — own process, own SQLite file, own write-worker thread, own watermark tracking. If one crashes, others continue.
- **`ZERO_NUM_REPLICA_SHARDS=1`** (default) gives exactly the old behavior — no code path changes at all.
- **Shards are clamped** to `min(numReplicaShards, numSyncWorkers)`. You can't have more shards than sync workers.

### Trade-offs

- **4× disk writes** (same CDC data written to 4 files)
- **4× disk space** for replica files
- **4× change-streamer subscriptions** (HTTP/WebSocket, lightweight)
- **WAL contention eliminated** — each file has its own 4 read-mark slots

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `ZERO_NUM_REPLICA_SHARDS` | `1` | Number of independent SQLite replica files. Clamped to `min(value, ZERO_NUM_SYNC_WORKERS)`. |
| `ZERO_NUM_SYNC_WORKERS` | `availableParallelism() - 1` | Number of sync worker processes. Each is pinned to shard `i % numReplicaShards`. |

### Example Configurations

**4 sync workers, 4 shards** (each syncer gets its own SQLite):
```bash
ZERO_NUM_SYNC_WORKERS=4
ZERO_NUM_REPLICA_SHARDS=4
```

**8 sync workers, 4 shards** (2 syncers per SQLite file):
```bash
ZERO_NUM_SYNC_WORKERS=8
ZERO_NUM_REPLICA_SHARDS=4
```

**2 sync workers, 4 shards** → clamped to 2 shards (can't have empty shards):
```bash
ZERO_NUM_SYNC_WORKERS=2
ZERO_NUM_REPLICA_SHARDS=4  # effective: 2
```

## Architecture Details

### How Replicators Subscribe

Each shard replicator creates its own `ChangeStreamerHttpClient` pointing to the same change-streamer URI. In zero-cache mode:

- **With external replicator** (`ZERO_CHANGE_STREAMER_URI` set): Each shard replicator subscribes to the external replication-manager via HTTP.
- **Without external replicator** (local, like zbugs): A local change-streamer is spawned. Each shard replicator subscribes to it at `http://localhost:{change-streamer-port}/`.

The change-streamer supports multiple subscribers — it fans out the same CDC stream to all.

### Shard File Initialization

Shard 0 uses the base replica file (e.g., `replica.db`), which is created by the change-streamer's initial sync. Shards 1+ use files like `replica.db-shard-1`, `replica.db-shard-2`, etc.

On first startup, shard files don't exist. The replicator for shard N > 0 copies the base file via `VACUUM INTO` before starting replication. This gives it a valid starting point with all existing data and schema. Subsequent restarts skip the copy since the file already exists.

**File**: `packages/zero-cache/src/server/replicator.ts` lines 50-62

### Client Routing (Dispatcher)

The dispatcher routes client groups to syncers with shard affinity:

1. **Hash client group to shard**: `shard = h32(taskID + '/' + clientGroupID) % numReplicaShards`
2. **Collect syncers for that shard**: syncer `i` belongs to shard `i % numReplicaShards`
3. **Pick within the shard's pool**: `h32(taskID + '/s/' + clientGroupID) % shardSyncerCount`

This ensures a client group always goes to a syncer reading from the same SQLite file, even across reconnections.

**File**: `packages/zero-cache/src/server/worker-dispatcher.ts` lines 73-97

### Notification Flow

Each shard replicator has its own notifier. Syncers subscribe only to their shard's notifier:

```
Replicator-0 → Notifier-0 → Syncer-0, Syncer-4, Syncer-8, ...
Replicator-1 → Notifier-1 → Syncer-1, Syncer-5, Syncer-9, ...
Replicator-2 → Notifier-2 → Syncer-2, Syncer-6, Syncer-10, ...
Replicator-3 → Notifier-3 → Syncer-3, Syncer-7, Syncer-11, ...
```

**File**: `packages/zero-cache/src/server/main.ts` lines 163-213

### Serving-Copy Mode

When `serving-copy` mode is active (litestream backup enabled), the shard suffix is applied to the base file BEFORE the `-serving-copy` suffix:

```
replica.db                     → replica.db-serving-copy           (shard 0)
replica.db-shard-1             → replica.db-shard-1-serving-copy   (shard 1)
replica.db-shard-2             → replica.db-shard-2-serving-copy   (shard 2)
```

**File**: `packages/zero-cache/src/workers/replicator.ts` lines 31-42

## Files Changed

### Modified (from Zero 1.0.0 base `5a5ea6b78`)

| File | Change |
|---|---|
| `packages/zero-cache/src/config/zero-config.ts` | Added `numReplicaShards` config option (default 1) |
| `packages/zero-cache/src/config/normalize.ts` | Added `numReplicaShards` to `NormalizedZeroConfig` type |
| `packages/zero-cache/src/server/main.ts` | Spawn N replicators, create N notifiers, pin syncers to shards, pass numReplicaShards to dispatcher |
| `packages/zero-cache/src/server/replicator.ts` | Parse `--shard-index=N` arg, copy base file for new shards via VACUUM INTO, use shard-specific file |
| `packages/zero-cache/src/server/syncer.ts` | Parse `--shard-index=N` arg, resolve shard-specific replica file |
| `packages/zero-cache/src/server/worker-dispatcher.ts` | Shard-aware routing: hash to shard first, then pick syncer within shard pool. Clamp shards to syncer count. |
| `packages/zero-cache/src/workers/replicator.ts` | `replicaFileName()` accepts optional `shardIndex`, applies shard suffix before mode suffix |

### NOT Changed

- PipelineDriver, Snapshotter, IVM, CVR, ChangeLog — all untouched
- Change-streamer — unchanged, naturally supports multiple subscribers
- Client protocol — no changes
- Replication-manager mode — untouched (only applies when `numSyncWorkers > 0`)

## Branch

- **Branch**: `feat/multi-replica-shards`
- **Base**: `zero/v1.0.0` (commit `5a5ea6b78`)
- **Repo**: `/Users/harsharanga/code/mono-v2`

## Testing

### Local (zbugs)

```bash
cd apps/zbugs
npm run db-up && npm run db-migrate && npm run db-seed

# Start with 4 shards
NODE_ENV=development ZERO_NUM_REPLICA_SHARDS=4 ZERO_NUM_SYNC_WORKERS=4 \
  node packages/zero/out/zero/src/cli.js

# Verify shard files created
ls -la zero.db*
# zero.db, zero.db-shard-1, zero.db-shard-2, zero.db-shard-3

# Verify in logs
# "started 4 replica shard(s) for 4 sync workers"
# "all workers ready"
```

### xyne-spaces-login (Docker)

1. **Build the Docker image** (from mono-v2 repo root):
```bash
npm --workspace=@rocicorp/zero run build
npm pack --workspace=@rocicorp/zero
cp rocicorp-zero-1.0.0.tgz packages/zero/pkgs/
cd packages/zero
docker build -t zero-cache-pool:local \
  --build-arg ZERO_VERSION=rocicorp-zero-1.0.0.tgz \
  --build-arg ZERO_SYNC_PROTOCOL_VERSION=49 \
  --build-arg ZERO_MIN_SUPPORTED_SYNC_PROTOCOL_VERSION=30 \
  .
```

2. **Add env var** in `docker-compose.dev.yml` under zero-cache service:
```yaml
- ZERO_NUM_REPLICA_SHARDS=4
```

3. **Restart zero-cache**:
```bash
docker compose -f docker-compose.dev.yml up -d --force-recreate zero-cache
```

4. **Verify**:
```bash
# Check all shards started
docker logs xyne-spaces-login-zero-cache 2>&1 | grep 'started.*shard'
# "started 2 replica shard(s) for 2 sync workers"  (clamped if shards > workers)

# Check shard files in container
docker exec xyne-spaces-login-zero-cache sh -c 'ls -la /var/zero/replica.db*'

# Check client routing
docker logs xyne-spaces-login-zero-cache 2>&1 | grep -o 'worker":"syncer-shard[0-9]*' | sort | uniq -c

# Check for errors
docker logs --since 5m xyne-spaces-login-zero-cache 2>&1 | grep '"level":"ERROR"'
```

### Verified Results (xyne-spaces-login)

- 2 sync workers, `ZERO_NUM_REPLICA_SHARDS=4` → clamped to 2 shards
- Both shards active: shard0 (239 log entries), shard1 (92 entries) in 2 minutes
- 24 queries hydrated, 441 rows flushed on first client connect
- **Zero errors, zero warnings**
- All workers ready in 1.7s

### Regression Test

Set `ZERO_NUM_REPLICA_SHARDS=1` (or don't set it at all). Behavior is identical to stock 1.0.0:
- 1 replicator, 1 notifier, all syncers on same file
- Dispatcher uses original `h32(taskID + '/' + clientGroupID) % syncers.length` routing
- No shard suffix on any file

## Gotchas & Learnings

### 1. Shards Must Be ≤ Sync Workers

If `numReplicaShards > numSyncWorkers`, some shards have no syncers. The dispatcher tries to route to an empty shard pool → `syncers[undefined]` → crash. Fixed by clamping in both `main.ts` and `worker-dispatcher.ts`.

### 2. Shard Files Don't Exist on First Startup

The change-streamer creates the base `replica.db` via initial PG sync. Shard files 1+ don't exist. The replicator's `upgradeReplica()` call fails on empty databases with "This should only be called for already synced replicas". Fixed by copying the base file via `VACUUM INTO` before starting the replicator for shard N > 0.

### 3. Shard Suffix Must Come Before Mode Suffix

For `serving-copy` mode, `setupReplica()` internally calls `replicaFileName(file, 'serving-copy')` to derive the copy location. If the shard suffix is after the mode suffix, double-suffixing occurs. The correct order is: `replica.db-shard-1-serving-copy` (shard first, then mode).

### 4. Stale Client Cookies From Different Branch

If a client was previously connected to a branch with a different cookie format (e.g., per-query cookies from pool-worker-threads), the server will reject the cookie with "Invalid version string". Fix: clear browser IndexedDB and reconnect.

### 5. Port Conflicts With OrbStack

OrbStack (Docker Desktop alternative) binds to ports like 4849. If zero-cache's change-streamer port (default: main port + 1) conflicts, use `ZERO_CHANGE_STREAMER_PORT` to pick a different port, or stop conflicting OrbStack services.

### 6. Disk Space

Each shard is a full copy of the replica. For a 4.3 MB replica with 4 shards, that's ~17 MB. For production with large datasets, plan for `N × replica_size` disk. Memory is NOT multiplied — sync workers share the same V8 heap characteristics as before.

## Future Considerations

- **Uneven shard loading**: Currently client groups are hash-distributed uniformly. If one client group is much heavier than others, its shard will be hotter. Could add weighted routing or shard rebalancing.
- **Combining with pool worker threads**: Multi-replica shards and pool worker threads are orthogonal. Shards reduce WAL contention, pool threads parallelize IVM computation. They can be used together.
- **Dynamic shard count**: Currently fixed at startup. Could add runtime shard scaling, but requires migrating client groups between shards (non-trivial).
