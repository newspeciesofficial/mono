# New pattern: upstream plpgsql event triggers + pg_logical_emit_message

## Category
E (Wire / protocol). The *upstream database* runs hand-written plpgsql
functions that emit JSON payloads through Postgres's logical-decoding
message channel (`pg_logical_emit_message`). The Rust port must install
these exact same SQL/plpgsql strings.

## Where used
- `packages/zero-cache/src/services/change-source/pg/schema/ddl.ts:143-313`
  — `createEventFunctionStatements()` emits:
  - `get_trigger_context()` (plpgsql) returning the current_query,
  - `schema_specs()` (plpgsql) returning `publishedTableQuery` +
    `indexDefinitionsQuery` as a single JSON blob,
  - `emit_ddl_start()` / `emit_ddl_end(tag)` event trigger functions which
    call `pg_logical_emit_message(true, '<appID>/<shardNum>[/ddl]',
    json_build_object(...)::text)`.
- `packages/zero-cache/src/services/change-source/pg/schema/ddl.ts:327-369`
  — `createEventTriggerStatements()` installs:
  - one `ON ddl_command_start` trigger covering `TAGS`,
  - one `ON ddl_command_end` trigger per tag (plus `COMMENT`).
- `packages/zero-cache/src/services/change-source/pg/schema/shard.ts:242-248,
  375-399` — `triggerSetup()` runs the above inside a SAVEPOINT; if it fails
  with `PG_INSUFFICIENT_PRIVILEGE` (42501), replication continues in
  `ddlDetection = false` (degraded) mode.
- `packages/zero-cache/src/services/change-source/pg/change-source.ts:753-765,
  794-854, 1107-1139` — consumer side: `Message` pgoutput message with
  `tag === 'message'` whose `prefix` matches `<appID>/<shardNum>[/ddl]` is
  routed to `#handleDdlMessage`, which parses the JSON against
  `replicationEventSchema` (valita) and diffs schema snapshots.

## TS form (abbreviated)
```ts
CREATE OR REPLACE FUNCTION {SCHEMA}.emit_ddl_end(tag TEXT)
RETURNS void AS $$
DECLARE ...
BEGIN
  ...
  PERFORM pg_logical_emit_message(
    true,
    '{appID}/{shardNum}' || event_prefix,
    json_build_object(
      'type', event_type,
      'version', {PROTOCOL_VERSION},
      'schema', schema_specs::json,
      'event', event::json,
      'context', {SCHEMA}.get_trigger_context()
    )::text
  );
END $$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER ... ON ddl_command_end WHEN TAG IN (...)
  EXECUTE PROCEDURE {SCHEMA}.emit_{tagID}();
```

## Proposed Rust form
```rust
// Treat the plpgsql source as opaque bytes and send it verbatim with
// simple_query. Do NOT rewrite the trigger bodies in Rust — they execute
// inside Postgres.
let script: String = render_trigger_sql(&shard);  // = TS createEventTriggerStatements()
client.batch_execute(&script).await?;

// On the consumer side, the pgoutput 'Message' record's prefix is
// compared, and the JSON `content` is deserialized with serde_json
// (see new-pattern: valita → serde).
```

## Classification
Direct, for the SQL install step (the SQL is a string constant; render
identifiers with `postgres-protocol::escape::escape_identifier`).

Risky, for the consumer path: routing depends on pgoutput `Message` records
which are part of the pgoutput parser work (guide E4).

## Caveats
- PG 15+ is required (checked in
  `initial-sync.ts:278-282`); the Rust side should carry the same guard.
- The shard's public SQL schema (`{appID}_{shardNum}.schema_specs()`,
  `{appID}_{shardNum}.emit_*()`) is intentionally **per-shard**
  (see `ddl.ts:124-141`); no cross-shard sharing of plpgsql functions.
  The Rust renderer must keep that isolation.
- The error path when the role lacks superuser: the trigger install runs
  inside a SAVEPOINT so a `42501` is caught and replication proceeds with
  `ddlDetection = false`. In the Rust port, use a nested transaction
  (`tokio_postgres` supports `SAVEPOINT` via `simple_query`/`Transaction`).
- `pg_logical_emit_message(true, ...)` = *transactional* message — the
  message arrives in the logical stream at the exact point the DDL
  committed. The consumer relies on that ordering.
- `pg-format`'s `literal(...)` wraps arrays (`TAGS`, `publications`) as
  properly-escaped PG literals. Port with
  `postgres-protocol::escape::escape_literal` or the `pg_escape` crate.

## Citation
- `pg_logical_emit_message`:
  https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-REPLICATION
- PostgreSQL EVENT TRIGGER:
  https://www.postgresql.org/docs/current/sql-createeventtrigger.html
- `postgres-protocol::escape::{escape_identifier, escape_literal}`:
  https://docs.rs/postgres-protocol/latest/postgres_protocol/escape/index.html
