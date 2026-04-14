# New pattern: version-bounded catchup-patch query against CVR tables

## Category
D (Data). The closest existing entry is D1 (`moka::future::Cache` for TTL +
LRU), but this is a different shape: a query that computes the set of
patches to apply between a client's `baseCookie` and the CVR's
`version`.

## Where used
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:637-715` —
  `catchupRowPatches` (delegates to `RowRecordCache`) and
  `catchupConfigPatches`. Both return patches ordered by `patchVersion`
  and bounded by `patchVersion > ${afterVersion}` and
  `patchVersion <= ${upTo}`; `catchupConfigPatches` first calls
  `checkVersion` to ensure the snapshot read sees the CVR at the
  expected version.
- `packages/zero-cache/src/services/view-syncer/schema/cvr.ts:141-144,
  186-191, 316-318` — indexes supporting the catchup query path:
  `queries_patch_version ON queries (patchVersion NULLS FIRST)`,
  `desires_patch_version ON desires (patchVersion)`,
  `row_patch_version ON rows (patchVersion)`.
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:1257-1272` —
  `checkVersion` (snapshot read with no row lock) is the read-only
  sibling of `#checkVersionAndOwnership`.

## TS form
```ts
async catchupConfigPatches(
  lc: LogContext,
  afterVersion: NullableCVRVersion,
  upToCVR: CVRSnapshot,
  current: CVRVersion,
): Promise<PatchToVersion[]> {
  if (cmpVersions(afterVersion, upToCVR.version) >= 0) {
    return [];
  }
  const start = afterVersion ? versionString(afterVersion) : '';
  const end = versionString(upToCVR.version);

  const reader = new TransactionPool(lc, Mode.READONLY).run(this.#db);
  try {
    await reader.processReadTask(tx =>
      checkVersion(tx, this.#schema, this.#id, current),
    );
    const [allDesires, queryRows] = await reader.processReadTask(tx =>
      Promise.all([
        tx<DesiresRow[]>`
          SELECT * FROM ${this.#cvr('desires')}
            WHERE "clientGroupID" = ${this.#id}
              AND "patchVersion" > ${start}
              AND "patchVersion" <= ${end}`,
        tx<Pick<QueriesRow, 'deleted' | 'queryHash' | 'patchVersion'>[]>`
          SELECT deleted, "queryHash", "patchVersion" FROM ${this.#cvr('queries')}
            WHERE "clientGroupID" = ${this.#id}
              AND "patchVersion" > ${start}
              AND "patchVersion" <= ${end}`,
      ]),
    );
    // ...project into PatchToVersion[] keyed off deleted flag ...
  } finally {
    reader.setDone();
  }
}
```

The CVR version string is lexicographic (stateVersion:configVersion,
base-36 lexi-encoded) — a straight string comparison in SQL gives the
correct ordering. See
`packages/zero-cache/src/services/view-syncer/schema/types.ts:296-324`.

## Proposed Rust form

```rust
pub async fn catchup_config_patches(
    pool: &deadpool_postgres::Pool,
    schema: &str,
    client_group_id: &str,
    after: Option<&CvrVersion>,
    up_to: &CvrSnapshot,
    current: &CvrVersion,   // must equal cvr.instances.version at snapshot time
) -> Result<Vec<PatchToVersion>> {
    if cmp_versions(after, Some(&up_to.version)) != Ordering::Less {
        return Ok(Vec::new());
    }
    let start: String = after.map(version_string).unwrap_or_default();
    let end: String   = version_string(&up_to.version);

    let client = pool.get().await?;
    let tx = client.build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;

    // Version guard: if the CVR has advanced under us, bail.
    let row = tx.query_one(
        &format!("SELECT version FROM {schema}.instances WHERE \"clientGroupID\" = $1"),
        &[&client_group_id],
    ).await?;
    let actual: String = row.get(0);
    if actual != version_string(current) {
        return Err(FlushError::ConcurrentModification { expected: version_string(current), actual });
    }

    // Pipeline the two SELECTs.
    let (desires, queries) = tokio::try_join!(
        tx.query(
            &format!(r#"
                SELECT "clientID", "queryHash", "patchVersion", COALESCE("deleted", false) AS deleted
                  FROM {schema}.desires
                  WHERE "clientGroupID" = $1
                    AND "patchVersion" > $2
                    AND "patchVersion" <= $3
            "#),
            &[&client_group_id, &start, &end],
        ),
        tx.query(
            &format!(r#"
                SELECT "queryHash", "patchVersion", COALESCE("deleted", false) AS deleted
                  FROM {schema}.queries
                  WHERE "clientGroupID" = $1
                    AND "patchVersion" > $2
                    AND "patchVersion" <= $3
            "#),
            &[&client_group_id, &start, &end],
        ),
    )?;
    tx.commit().await?;  // read-only commit is a no-op; closes the snapshot.

    let mut patches = Vec::with_capacity(desires.len() + queries.len());
    for r in queries {
        let id: String = r.get("queryHash");
        let ver = version_from_string(r.get::<_, String>("patchVersion"))?;
        let patch = if r.get::<_, bool>("deleted") {
            Patch::QueryDel { id }
        } else {
            Patch::QueryPut { id }
        };
        patches.push(PatchToVersion { patch, to_version: ver });
    }
    for r in desires {
        let client_id: String = r.get("clientID");
        let id: String = r.get("queryHash");
        let ver = version_from_string(r.get::<_, String>("patchVersion"))?;
        let patch = if r.get::<_, bool>("deleted") {
            Patch::QueryDelForClient { id, client_id }
        } else {
            Patch::QueryPutForClient { id, client_id }
        };
        patches.push(PatchToVersion { patch, to_version: ver });
    }
    Ok(patches)
}
```

## Classification
Idiom-swap. The SQL is unchanged. What is new compared to the guide is:
1. The **version cookie** (`stateVersion:configVersion`) is a
   sortable string by construction (see
   `packages/zero-cache/src/types/lexi-version.ts` — already on
   comparison row 1 / replicator row as "LexiVersion / Newtype-style,
   covered by A24").
2. The **open-lower, closed-upper** window `(baseCookie, cvr.version]`
   is repeated across three tables (`queries`, `desires`, `rows`);
   factor into a single helper.
3. The **snapshot read** uses a short-lived read-only transaction
   (`TransactionPool { Mode.READONLY }` in TS); in Rust, use
   `build_transaction().read_only(true).isolation_level(RepeatableRead)`
   to get a consistent snapshot.

## Caveats
- Row catchup (`catchupRowPatches` at cvr-store.ts:637) is streamed by
  `RowRecordCache` as an `AsyncGenerator<RowsRow[]>` — analogous to
  A16 + A22 (async generator / async iterator). In Rust this is
  `impl Stream<Item = Vec<RowsRow>>`, built with `async_stream::stream!`
  and paged by `tokio_postgres::Client::query_raw` + the portal cursor
  (`COPY` is not useful here; use a declared SQL cursor or
  `query_raw` + `try_next()`).
- For a large row-catchup set, TS uses `.cursor(N)` on the postgres.js
  query (see `row-record-cache.ts` — not in scope here, but called from
  cvr-store.ts:643). Port that to `tokio_postgres::Transaction::query_raw`
  with a portal, or to a `DECLARE ... CURSOR FOR ...; FETCH N FROM ...`
  loop.
- The `catchupRowPatches` implementation (not in my scope) also needs to
  merge with the in-memory `RowRecordCache`, which covers rows written
  in the current CVR version that haven't yet been flushed to `cvr.rows`.
  That dual-source merge is a key semantic that must be preserved to
  keep row-patches consistent with the CVR version being advertised.
- `patchVersion` for `queries` can be `NULL` — that means the query is
  "desired but not yet gotten". The `patchVersion > ${start}` predicate
  excludes `NULL`s (SQL `NULL > '...'` is UNKNOWN). Preserve that —
  `NULL`-patchVersion rows are never catchup-eligible.
- The catchup path is currently missing from the Rust port (comparison
  row 16 — "we skip the CVR row-diff path (we re-hydrate instead)").
  Porting this pattern is the critical fix for that row.

## Citation
- PostgreSQL `REPEATABLE READ` snapshot semantics:
  https://www.postgresql.org/docs/current/transaction-iso.html#XACT-REPEATABLE-READ
- `tokio_postgres::Client::build_transaction`:
  https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Client.html#method.build_transaction
- `async_stream::stream!` for paged streaming:
  https://docs.rs/async-stream/latest/async_stream/macro.stream.html
