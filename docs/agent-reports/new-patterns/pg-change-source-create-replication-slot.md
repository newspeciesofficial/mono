# New pattern: CREATE_REPLICATION_SLOT on a replication-mode connection

## Category
E (Wire / protocol) — complements pgoutput parser (E4) but is a distinct SQL
subcommand sent over the "replication" sub-protocol.

## Where used
- `packages/zero-cache/src/services/change-source/pg/initial-sync.ts:84-88` —
  opens a `replication: 'database'` pgClient, then
- `packages/zero-cache/src/services/change-source/pg/initial-sync.ts:371-383`
  — `createReplicationSlot()` issues
  `CREATE_REPLICATION_SLOT "<name>" LOGICAL pgoutput` via `session.unsafe(...)`
  and returns `{slot_name, consistent_point, snapshot_name, output_plugin}`.
- `packages/zero-cache/src/services/change-source/pg/backfill-stream.ts:220-254`
  — `createSnapshotTransaction()` creates a short-lived replication slot to
  obtain a named snapshot pinned to a specific LSN, then uses
  `DROP_REPLICATION_SLOT "<name>"`.
- `packages/zero-cache/src/services/change-source/pg/initial-sync.ts:108-117,253`
  — handles `PG_INSUFFICIENT_PRIVILEGE` (auto-grants REPLICATION role) and
  drop-slot cleanup on failure.

## TS form
```ts
const replicationSession = pgClient(lc, upstreamURI, {
  fetch_types: false,                 // streaming protocol cannot prepare
  connection: {replication: 'database'},
});

const [slot] = await replicationSession.unsafe<ReplicationSlot[]>(
  `CREATE_REPLICATION_SLOT "${slotName}" LOGICAL pgoutput`,
);
// slot = { slot_name, consistent_point: LSN, snapshot_name, output_plugin }
```

## Proposed Rust form
```rust
use tokio_postgres::{Client, NoTls, Config};

let mut cfg: Config = url.parse()?;
cfg.replication_mode(tokio_postgres::config::ReplicationMode::Logical);
let (client, conn) = cfg.connect(NoTls).await?;
tokio::spawn(conn);

// The extended protocol is not available on a replication connection.
// Use simple_query (or `batch_execute`) — no parameter binding.
let rows = client
    .simple_query(&format!(
        r#"CREATE_REPLICATION_SLOT "{}" LOGICAL pgoutput"#,
        slot_name,
    ))
    .await?;
let row = rows.iter().find_map(|m| match m {
    tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
    _ => None,
}).unwrap();
let consistent_point = row.get("consistent_point").unwrap();   // LSN text
let snapshot_name    = row.get("snapshot_name").unwrap();
let output_plugin    = row.get("output_plugin").unwrap();
```

## Classification
Risky / spec-heavy. The command itself is a simple string, but:
- must be sent via `simple_query` — the extended query protocol is disabled
  on replication connections (same reason postgres.js sets `fetch_types:
  false`);
- the returned `consistent_point` is an LSN in `"H/L"` hex-slash form (see
  guide E5);
- the returned `snapshot_name` must later be consumed with
  `SET TRANSACTION SNAPSHOT '<name>'` inside a `REPEATABLE READ` transaction;
  that transaction is what the initial-sync COPY workers attach to.

## Caveats
- `tokio-postgres::replication` exists but its public API surfaces the
  streaming replication loop; the slot-creation SQL is something you send
  via a regular replication-mode `Client::simple_query` call.
- `postgres.js` (porsager) returns the slot info as a typed row; with
  `tokio-postgres` you must parse `SimpleQueryMessage::Row` columns by name.
- When `PG_INSUFFICIENT_PRIVILEGE` (SQLSTATE `42501`) is raised, the code
  retries after issuing `ALTER ROLE current_user WITH REPLICATION` on the
  non-replication connection. That must be preserved in Rust with a second
  `tokio_postgres::Client` on a normal connection.
- The slot must be explicitly dropped on failure
  (`DROP_REPLICATION_SLOT "..."`) to avoid WAL retention pile-up — this is
  just another `simple_query` call.

## Citation
- PostgreSQL streaming replication protocol —
  `CREATE_REPLICATION_SLOT … LOGICAL pgoutput`:
  https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-REPLICATION-SLOT
- `tokio-postgres::Config::replication_mode`:
  https://docs.rs/tokio-postgres/latest/tokio_postgres/config/enum.ReplicationMode.html
- `tokio-postgres::SimpleQueryMessage`:
  https://docs.rs/tokio-postgres/latest/tokio_postgres/enum.SimpleQueryMessage.html
