//! Port of `packages/zqlite/src/sqlite-stat-fanout.ts`.
//!
//! Computes join fanout factors from SQLite's `sqlite_stat1` and
//! `sqlite_stat4` statistics tables. Used by [`super::sqlite_cost_model`] to
//! estimate join cardinality during query planning.
//!
//! ## Public API
//!
//! - [`SQLiteStatFanout`] — fanout calculator. Construct once per database;
//!   call [`SQLiteStatFanout::get_fanout`] for each `(table, columns)`
//!   query. Results are cached.
//! - [`FanoutResult`] — the value type returned: a fanout estimate plus a
//!   `source` and `confidence`.
//! - [`FanoutSource`] / [`FanoutConfidence`] — string-typed fields in TS,
//!   real enums here.
//!
//! ## Strategy mirror
//!
//! 1. `sqlite_stat4` (best): histogram with separate NULL/non-NULL samples.
//!    Returns the median fanout of non-NULL samples.
//! 2. `sqlite_stat1` (fallback): a single average per index. Includes NULL
//!    rows so it overestimates for sparse FKs.
//! 3. Default constant when statistics are unavailable.
//!
//! ## Compound indexes
//!
//! Constraint columns must appear, in any order, in the **first N**
//! positions of the index (gaps not allowed). Statistics at depth N give
//! the fanout for the combination of those N columns.
//!
//! ## Chokepoint
//!
//! All SQLite access funnels through the single private method
//! [`SQLiteStatFanout::query_rows`]. Future shadow-FFI / recording layers
//! hook this one method.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use rusqlite::types::{Value as SqlValue, ValueRef};

/// Source of a fanout estimate.
///
/// Mirrors TS string union `'stat4' | 'stat1' | 'default'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutSource {
    /// `sqlite_stat4` histogram (most accurate; excludes NULLs).
    Stat4,
    /// `sqlite_stat1` average (includes NULLs; may overestimate).
    Stat1,
    /// Constant fallback when statistics are unavailable.
    Default,
}

/// Confidence in the fanout estimate.
///
/// Mirrors TS string union `'high' | 'med' | 'none'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutConfidence {
    High,
    Med,
    None,
}

/// Result of a fanout calculation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FanoutResult {
    /// Average rows per distinct value of the join column(s).
    pub fanout: f64,
    pub confidence: FanoutConfidence,
    pub source: FanoutSource,
}

/// Internal: a decoded sqlite_stat4 sample.
struct DecodedSample {
    fanout: i64,
    is_null: bool,
}

/// Row view passed to the [`SQLiteStatFanout::query_rows`] mapper.
///
/// Wraps a `&[SqlValue]` column slice (produced by either rusqlite on the
/// live path or the replay-ctx adapter on the replay path) and exposes a
/// `get::<_, T>(idx)` / `get_ref(idx)` façade that mirrors the pieces of
/// the `rusqlite::Row` API this module uses.
#[allow(dead_code)]
pub(super) struct PositionalRow<'a> {
    cells: &'a [SqlValue],
}

impl<'a> PositionalRow<'a> {
    /// Mirror of `rusqlite::Row::get::<_, T>(idx)`.
    ///
    /// Accepts the same two-generic shape (`I = usize`, `T`) callers use
    /// against `rusqlite::Row` so the three callsites in this module
    /// don't need to change their mapping closures.
    pub(super) fn get<I, T>(&self, idx: I) -> rusqlite::Result<T>
    where
        I: RowIndex,
        T: rusqlite::types::FromSql,
    {
        let i = idx.idx();
        let vref = self.get_ref(i)?;
        T::column_result(vref)
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(i, type_of(&vref), Box::new(e)))
    }

    /// Mirror of `rusqlite::Row::get_ref(idx)`.
    pub(super) fn get_ref(&self, idx: usize) -> rusqlite::Result<ValueRef<'_>> {
        let v = self
            .cells
            .get(idx)
            .ok_or(rusqlite::Error::InvalidColumnIndex(idx))?;
        Ok(sql_value_as_ref(v))
    }
}

/// Helper trait mirroring `rusqlite::RowIndex` for the positional-only
/// wrapper. Only `usize` is needed — none of the three existing callsites
/// use string-based column names.
pub(super) trait RowIndex {
    fn idx(&self) -> usize;
}

impl RowIndex for usize {
    fn idx(&self) -> usize {
        *self
    }
}

fn sql_value_as_ref(v: &SqlValue) -> ValueRef<'_> {
    match v {
        SqlValue::Null => ValueRef::Null,
        SqlValue::Integer(i) => ValueRef::Integer(*i),
        SqlValue::Real(f) => ValueRef::Real(*f),
        SqlValue::Text(s) => ValueRef::Text(s.as_bytes()),
        SqlValue::Blob(b) => ValueRef::Blob(b),
    }
}

fn type_of(vref: &ValueRef<'_>) -> rusqlite::types::Type {
    match vref {
        ValueRef::Null => rusqlite::types::Type::Null,
        ValueRef::Integer(_) => rusqlite::types::Type::Integer,
        ValueRef::Real(_) => rusqlite::types::Type::Real,
        ValueRef::Text(_) => rusqlite::types::Type::Text,
        ValueRef::Blob(_) => rusqlite::types::Type::Blob,
    }
}

/// Fanout calculator.
///
/// Uses `Arc<Mutex<...>>` to allow sharing across the cost-model closure.
pub struct SQLiteStatFanout {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    conn: Connection,
    default_fanout: f64,
    /// Cache of fanout results.
    /// Key format: `"tableName:col1,col2,col3"` (sorted alphabetically).
    cache: HashMap<String, FanoutResult>,
}

impl SQLiteStatFanout {
    /// TS `new SQLiteStatFanout(db, defaultFanout = 3)`.
    ///
    /// Takes ownership of the rusqlite [`Connection`]. Use
    /// [`SQLiteStatFanout::with_path`] to open from a path or
    /// [`SQLiteStatFanout::open_in_memory`] for tests.
    pub fn new(conn: Connection, default_fanout: f64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                conn,
                default_fanout,
                cache: HashMap::new(),
            })),
        }
    }

    /// Convenience: construct with a connection borrowed from another
    /// caller, by opening a fresh in-memory database backed by the same
    /// schema. Intended for the cost-model integration where the model is
    /// constructed against an already-open `Connection`.
    ///
    /// In practice the cost model takes the same connection path as
    /// SQLiteStatFanout (the table source's database), so callers should
    /// share the underlying file via two separate [`Connection`]s.
    pub fn open_in_memory(default_fanout: f64) -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self::new(conn, default_fanout))
    }

    /// TS `getFanout(tableName, columns)`.
    ///
    /// Returns a cached result if one exists. Otherwise tries stat4 →
    /// stat1 → default.
    pub fn get_fanout(&self, table_name: &str, columns: &[String]) -> FanoutResult {
        // Cache key uses sorted columns for consistency with TS.
        let mut sorted: Vec<String> = columns.to_vec();
        sorted.sort();
        let cache_key = format!("{table_name}:{}", sorted.join(","));

        {
            let inner = self.inner.lock().expect("stat fanout mutex poisoned");
            if let Some(cached) = inner.cache.get(&cache_key) {
                return *cached;
            }
        }

        // Branch: stat4 first.
        if let Some(r) = self.get_fanout_from_stat4(table_name, columns) {
            self.insert_cache(cache_key, r);
            return r;
        }
        // Branch: stat1 fallback.
        if let Some(r) = self.get_fanout_from_stat1(table_name, columns) {
            self.insert_cache(cache_key, r);
            return r;
        }
        // Branch: default fallback.
        let default_fanout = self
            .inner
            .lock()
            .expect("stat fanout mutex poisoned")
            .default_fanout;
        let r = FanoutResult {
            fanout: default_fanout,
            confidence: FanoutConfidence::None,
            source: FanoutSource::Default,
        };
        self.insert_cache(cache_key, r);
        r
    }

    /// TS `clearCache()`.
    pub fn clear_cache(&self) {
        self.inner
            .lock()
            .expect("stat fanout mutex poisoned")
            .cache
            .clear();
    }

    fn insert_cache(&self, key: String, value: FanoutResult) {
        self.inner
            .lock()
            .expect("stat fanout mutex poisoned")
            .cache
            .insert(key, value);
    }

    /// TS `#getFanoutFromStat4(tableName, columns)`.
    fn get_fanout_from_stat4(&self, table_name: &str, columns: &[String]) -> Option<FanoutResult> {
        let index_info = self.find_index_for_columns(table_name, columns)?;

        // Query stat4 samples for this index.
        // Returns Vec<(neq, sample_bytes)>; we only need neq + the sample
        // blob to detect NULL.
        let rows: Vec<(String, Vec<u8>)> = self
            .query_rows(
                "SELECT neq, sample FROM sqlite_stat4 WHERE tbl = ? AND idx = ? ORDER BY nlt",
                &[
                    SqlValue::Text(table_name.to_string()),
                    SqlValue::Text(index_info.index_name.clone()),
                ],
                |row| {
                    let neq: String = row.get(0)?;
                    let sample = match row.get_ref(1)? {
                        ValueRef::Blob(b) => b.to_vec(),
                        ValueRef::Text(t) => t.to_vec(),
                        _ => Vec::new(),
                    };
                    Ok((neq, sample))
                },
            )
            .ok()?;

        if rows.is_empty() {
            return None;
        }

        // Use depth-1 for neq array index (depth is 1-based, array is 0-based).
        let neq_index = index_info.depth.saturating_sub(1);

        let decoded: Vec<DecodedSample> = rows
            .iter()
            .map(|(neq, sample)| {
                let parts: Vec<&str> = neq.split(' ').collect();
                let raw = parts
                    .get(neq_index)
                    .copied()
                    .unwrap_or_else(|| parts.first().copied().unwrap_or(""));
                let fanout: i64 = raw.parse().unwrap_or(0);
                DecodedSample {
                    fanout,
                    is_null: decode_sample_is_null(sample),
                }
            })
            .collect();

        let non_null: Vec<&DecodedSample> = decoded.iter().filter(|s| !s.is_null).collect();

        // Branch: all samples are NULL.
        if non_null.is_empty() {
            return Some(FanoutResult {
                fanout: 0.0,
                source: FanoutSource::Stat4,
                confidence: FanoutConfidence::High,
            });
        }

        // Median of non-NULL fanouts.
        let mut fanouts: Vec<i64> = non_null.iter().map(|s| s.fanout).collect();
        fanouts.sort_unstable();
        let median = if fanouts.len() % 2 == 0 {
            // Branch: even count → floor of average of two middle values.
            let lo = fanouts[fanouts.len() / 2 - 1];
            let hi = fanouts[fanouts.len() / 2];
            ((lo + hi) / 2) as f64
        } else {
            // Branch: odd count → middle value.
            fanouts[fanouts.len() / 2] as f64
        };

        Some(FanoutResult {
            fanout: median,
            source: FanoutSource::Stat4,
            confidence: FanoutConfidence::High,
        })
    }

    /// TS `#getFanoutFromStat1(tableName, columns)`.
    fn get_fanout_from_stat1(&self, table_name: &str, columns: &[String]) -> Option<FanoutResult> {
        let index_info = self.find_index_for_columns(table_name, columns)?;

        let rows: Vec<String> = self
            .query_rows(
                "SELECT stat FROM sqlite_stat1 WHERE tbl = ? AND idx = ?",
                &[
                    SqlValue::Text(table_name.to_string()),
                    SqlValue::Text(index_info.index_name.clone()),
                ],
                |row| row.get::<_, String>(0),
            )
            .ok()?;

        // Branch: no rows in sqlite_stat1.
        let stat = rows.into_iter().next()?;
        let parts: Vec<&str> = stat.split(' ').collect();

        // Branch: not enough parts for the requested depth.
        if parts.len() < index_info.depth + 1 {
            return None;
        }

        let raw = parts[index_info.depth];
        let fanout: i64 = raw.parse().ok()?;

        Some(FanoutResult {
            fanout: fanout as f64,
            source: FanoutSource::Stat1,
            confidence: FanoutConfidence::Med,
        })
    }

    /// TS `#findIndexForColumns(tableName, columns)`.
    ///
    /// Uses `pragma_index_list` + `pragma_index_info` to enumerate every
    /// index (user-created, PRIMARY KEY, UNIQUE).
    fn find_index_for_columns(&self, table_name: &str, columns: &[String]) -> Option<IndexInfo> {
        // pragma_index_list/info don't accept named bound parameters in
        // every rusqlite version; the table name comes from a trusted
        // schema map upstream, so we substitute it as a quoted SQL literal.
        // Defensive: forbid quotes/backslashes to keep the SQL well-formed.
        if table_name.contains('\'') || table_name.contains('"') || table_name.contains('\\') {
            return None;
        }
        let sql = format!(
            "SELECT il.name AS index_name, ii.seqno, ii.name AS column_name \
             FROM pragma_index_list('{}') il \
             JOIN pragma_index_info(il.name) ii \
             ORDER BY il.seq, ii.seqno",
            table_name
        );
        let rows: Vec<(String, i64, String)> = self
            .query_rows(&sql, &[], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .ok()?;

        // Group by index name, preserving column order via seqno.
        // pragma_index_info returns rows in seqno order already, but we
        // sorted by it explicitly above.
        let mut index_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for (name, _seq, col) in rows {
            let entry = index_map.entry(name.clone()).or_insert_with(|| {
                order.push(name.clone());
                Vec::new()
            });
            entry.push(col);
        }

        for index_name in order {
            let cols = index_map.get(&index_name).expect("just inserted");
            // Branch: prefix-match found.
            if is_prefix_match(columns, cols) {
                return Some(IndexInfo {
                    index_name,
                    depth: columns.len(),
                });
            }
        }
        None
    }

    /// **Single private chokepoint** for SQLite reads.
    ///
    /// The old signature exposed `F: FnMut(&rusqlite::Row) -> …` directly;
    /// that shape cannot be honoured in replay mode because constructing a
    /// synthetic `rusqlite::Row` is not part of the public rusqlite API.
    /// The compromise (documented in the task brief): dispatch through
    /// [`Self::query_rows_positional`] — the actual chokepoint — and have
    /// the generic adapter operate on the returned `Vec<SqlValue>`
    /// positional row via a thin [`PositionalRow`] accessor.
    ///
    /// Callers keep the same ergonomics (read by index) but the row value
    /// they see is a `PositionalRow` not a `&rusqlite::Row`.
    fn query_rows<T, F>(
        &self,
        sql: &str,
        params: &[SqlValue],
        mut map: F,
    ) -> rusqlite::Result<Vec<T>>
    where
        F: FnMut(&PositionalRow<'_>) -> rusqlite::Result<T>,
    {
        let rows = self.query_rows_positional(sql, params)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let view = PositionalRow { cells: row };
            out.push(map(&view)?);
        }
        Ok(out)
    }

    /// The chokepoint. Returns rows as `Vec<Vec<SqlValue>>` in column order.
    fn query_rows_positional(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> rusqlite::Result<Vec<Vec<SqlValue>>> {
        let inner = self.inner.lock().expect("stat fanout mutex poisoned");
        let mut stmt = inner.conn.prepare(sql)?;
        let n_cols = stmt.column_count();
        let mut rows = stmt.query(rusqlite::params_from_iter(params.iter()))?;
        let mut out: Vec<Vec<SqlValue>> = Vec::new();
        while let Some(r) = rows.next()? {
            let mut cols: Vec<SqlValue> = Vec::with_capacity(n_cols);
            for i in 0..n_cols {
                cols.push(r.get::<usize, SqlValue>(i)?);
            }
            out.push(cols);
        }
        Ok(out)
    }
}

#[derive(Debug, Clone)]
struct IndexInfo {
    index_name: String,
    depth: usize,
}

/// TS `#isPrefixMatch(queryColumns, indexColumns)`.
///
/// All `query_columns` must appear in `index_columns[0..query_columns.len()]`,
/// in any order. Case-insensitive.
fn is_prefix_match(query_columns: &[String], index_columns: &[String]) -> bool {
    // Branch: too many query columns.
    if query_columns.len() > index_columns.len() {
        return false;
    }
    let prefix: HashSet<String> = index_columns
        .iter()
        .take(query_columns.len())
        .map(|c| c.to_lowercase())
        .collect();
    query_columns
        .iter()
        .all(|q| prefix.contains(&q.to_lowercase()))
}

/// TS `#decodeSampleIsNull(sample)`.
///
/// SQLite record format: varint header size, then per-column serial type,
/// then data. Serial type 0 = NULL.
fn decode_sample_is_null(sample: &[u8]) -> bool {
    // Branch: empty buffer.
    if sample.is_empty() {
        return true;
    }
    let header_size = sample[0];
    // Branch: zero header or header overruns buffer.
    if header_size == 0 || (header_size as usize) >= sample.len() {
        return true;
    }
    let serial_type = sample[1];
    // Branch: serial type 0 = NULL.
    serial_type == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn open() -> SQLiteStatFanout {
        let conn = Connection::open_in_memory().expect("open in-memory");
        SQLiteStatFanout::new(conn, 3.0)
    }

    fn exec(s: &SQLiteStatFanout, sql: &str) {
        let inner = s.inner.lock().unwrap();
        inner.conn.execute_batch(sql).unwrap();
    }

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    // --- is_prefix_match branches ---

    // Branch: query longer than index.
    #[test]
    fn prefix_too_long() {
        assert!(!is_prefix_match(
            &cols(&["a", "b", "c"]),
            &cols(&["a", "b"])
        ));
    }

    // Branch: exact match.
    #[test]
    fn prefix_exact() {
        assert!(is_prefix_match(&cols(&["a", "b"]), &cols(&["a", "b"])));
    }

    // Branch: shorter prefix matches.
    #[test]
    fn prefix_short() {
        assert!(is_prefix_match(&cols(&["a"]), &cols(&["a", "b", "c"])));
    }

    // Branch: case-insensitive.
    #[test]
    fn prefix_case_insensitive() {
        assert!(is_prefix_match(&cols(&["MIXED"]), &cols(&["mixed"])));
    }

    // Branch: order-independent.
    #[test]
    fn prefix_order_independent() {
        assert!(is_prefix_match(&cols(&["b", "a"]), &cols(&["a", "b", "c"])));
    }

    // Branch: gap not allowed.
    #[test]
    fn prefix_gap_rejected() {
        assert!(!is_prefix_match(
            &cols(&["a", "c"]),
            &cols(&["a", "b", "c"])
        ));
    }

    // --- decode_sample_is_null branches ---

    // Branch: empty buffer.
    #[test]
    fn decode_empty() {
        assert!(decode_sample_is_null(&[]));
    }

    // Branch: zero header.
    #[test]
    fn decode_zero_header() {
        assert!(decode_sample_is_null(&[0, 1]));
    }

    // Branch: header overruns buffer.
    #[test]
    fn decode_header_overrun() {
        assert!(decode_sample_is_null(&[5, 1]));
    }

    // Branch: serial type 0 → NULL.
    #[test]
    fn decode_serial_zero_is_null() {
        assert!(decode_sample_is_null(&[2, 0, 0]));
    }

    // Branch: serial type non-zero → not NULL.
    #[test]
    fn decode_serial_one_not_null() {
        assert!(!decode_sample_is_null(&[2, 1, 42]));
    }

    // --- get_fanout: default fallback (no index) ---

    // Branch: no index → default.
    #[test]
    fn get_fanout_no_index_returns_default() {
        let s = open();
        exec(
            &s,
            "CREATE TABLE no_index (id INTEGER PRIMARY KEY, value INTEGER); ANALYZE;",
        );
        let r = s.get_fanout("no_index", &cols(&["value"]));
        assert_eq!(r.source, FanoutSource::Default);
        assert_eq!(r.fanout, 3.0);
        assert_eq!(r.confidence, FanoutConfidence::None);
    }

    // Branch: custom default.
    #[test]
    fn get_fanout_respects_custom_default() {
        let conn = Connection::open_in_memory().unwrap();
        let s = SQLiteStatFanout::new(conn, 10.0);
        let r = s.get_fanout("missing", &cols(&["value"]));
        assert_eq!(r.fanout, 10.0);
        assert_eq!(r.source, FanoutSource::Default);
    }

    // --- get_fanout: stat4 + stat1 (real ANALYZE) ---

    fn create_relational(s: &SQLiteStatFanout, parents: i64, children: i64, fk_count: i64) {
        exec(
            s,
            "CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE task (id INTEGER PRIMARY KEY, project_id INTEGER, name TEXT);
             CREATE INDEX idx_task_project_id ON task(project_id);",
        );
        let inner = s.inner.lock().unwrap();
        for i in 1..=parents {
            inner
                .conn
                .execute(
                    "INSERT INTO project (id, name) VALUES (?, ?)",
                    rusqlite::params![i, format!("p{}", i)],
                )
                .unwrap();
        }
        for i in 1..=children {
            let fk: Option<i64> = if i <= fk_count {
                Some(((i - 1) % parents) + 1)
            } else {
                None
            };
            inner
                .conn
                .execute(
                    "INSERT INTO task (id, project_id, name) VALUES (?, ?, ?)",
                    rusqlite::params![i, fk, format!("t{}", i)],
                )
                .unwrap();
        }
        inner.conn.execute_batch("ANALYZE").unwrap();
    }

    // Branch: stat4 path with sparse FK (NULL-heavy).
    #[test]
    fn get_fanout_stat4_sparse_fk() {
        let s = open();
        create_relational(&s, 5, 100, 20);
        let r = s.get_fanout("task", &cols(&["project_id"]));
        // We accept either stat4 (proper) or stat1 fallback depending on
        // the SQLite build's stat4 availability. Either way, source is not
        // 'default' and the fanout is well-defined.
        assert_ne!(r.source, FanoutSource::Default);
        // 20 non-null tasks / 5 distinct project_ids = 4. With NULLs
        // included (stat1) the value is much higher.
        if r.source == FanoutSource::Stat4 {
            assert_eq!(r.fanout, 4.0);
        }
    }

    // Branch: stat4 evenly distributed.
    #[test]
    fn get_fanout_stat4_even_distribution() {
        let s = open();
        create_relational(&s, 3, 30, 30);
        let r = s.get_fanout("task", &cols(&["project_id"]));
        assert_ne!(r.source, FanoutSource::Default);
        if r.source == FanoutSource::Stat4 {
            assert_eq!(r.fanout, 10.0);
        }
    }

    // Branch: all NULL → fanout 0 (stat4 only).
    #[test]
    fn get_fanout_all_null_returns_zero() {
        let s = open();
        exec(
            &s,
            "CREATE TABLE all_null (id INTEGER PRIMARY KEY, value INTEGER);
             CREATE INDEX idx_all_null ON all_null(value);",
        );
        {
            let inner = s.inner.lock().unwrap();
            for i in 1..=100 {
                inner
                    .conn
                    .execute(
                        "INSERT INTO all_null (id, value) VALUES (?, NULL)",
                        rusqlite::params![i],
                    )
                    .unwrap();
            }
            inner.conn.execute_batch("ANALYZE").unwrap();
        }
        let r = s.get_fanout("all_null", &cols(&["value"]));
        // If stat4 is available we get fanout=0. If only stat1, we get a
        // positive average (NULLs included).
        if r.source == FanoutSource::Stat4 {
            assert_eq!(r.fanout, 0.0);
            assert_eq!(r.confidence, FanoutConfidence::High);
        } else {
            assert_ne!(r.source, FanoutSource::Default);
        }
    }

    // Branch: case-insensitive column lookup.
    #[test]
    fn get_fanout_case_insensitive_columns() {
        let s = open();
        exec(
            &s,
            "CREATE TABLE case_test (id INTEGER PRIMARY KEY, \"MixedCase\" INTEGER);
             CREATE INDEX idx_mixed ON case_test(\"MixedCase\");",
        );
        {
            let inner = s.inner.lock().unwrap();
            for i in 1..=30 {
                inner
                    .conn
                    .execute(
                        "INSERT INTO case_test (id, \"MixedCase\") VALUES (?, ?)",
                        rusqlite::params![i, i % 3],
                    )
                    .unwrap();
            }
            inner.conn.execute_batch("ANALYZE").unwrap();
        }
        let r1 = s.get_fanout("case_test", &cols(&["MixedCase"]));
        s.clear_cache();
        let r2 = s.get_fanout("case_test", &cols(&["mixedcase"]));
        assert_ne!(r1.source, FanoutSource::Default);
        assert_ne!(r2.source, FanoutSource::Default);
    }

    // Branch: caching returns identical result for repeated query.
    #[test]
    fn get_fanout_caches_repeated_queries() {
        let s = open();
        exec(
            &s,
            "CREATE TABLE cached (id INTEGER PRIMARY KEY, value INTEGER);
             CREATE INDEX idx_cached ON cached(value);",
        );
        {
            let inner = s.inner.lock().unwrap();
            for i in 1..=100 {
                inner
                    .conn
                    .execute(
                        "INSERT INTO cached (id, value) VALUES (?, ?)",
                        rusqlite::params![i, i % 10],
                    )
                    .unwrap();
            }
            inner.conn.execute_batch("ANALYZE").unwrap();
        }
        let r1 = s.get_fanout("cached", &cols(&["value"]));
        let r2 = s.get_fanout("cached", &cols(&["value"]));
        assert_eq!(r1, r2);
    }

    // Branch: cache key is order-independent.
    #[test]
    fn get_fanout_cache_key_order_independent() {
        let s = open();
        exec(
            &s,
            "CREATE TABLE order_t (id INTEGER PRIMARY KEY, p INTEGER, q INTEGER);
             CREATE INDEX idx_pq ON order_t(p, q);",
        );
        {
            let inner = s.inner.lock().unwrap();
            for i in 1..=20 {
                inner
                    .conn
                    .execute(
                        "INSERT INTO order_t (id, p, q) VALUES (?, ?, ?)",
                        rusqlite::params![i, i % 2, i % 5],
                    )
                    .unwrap();
            }
            inner.conn.execute_batch("ANALYZE").unwrap();
        }
        let r1 = s.get_fanout("order_t", &cols(&["p", "q"]));
        let r2 = s.get_fanout("order_t", &cols(&["q", "p"]));
        assert_eq!(r1, r2);
    }

    // Branch: clear_cache invalidates.
    #[test]
    fn clear_cache_invalidates_results() {
        let s = open();
        exec(
            &s,
            "CREATE TABLE c (id INTEGER PRIMARY KEY, v INTEGER);
             CREATE INDEX idx_c ON c(v);",
        );
        {
            let inner = s.inner.lock().unwrap();
            for i in 1..=10 {
                inner
                    .conn
                    .execute(
                        "INSERT INTO c (id, v) VALUES (?, ?)",
                        rusqlite::params![i, i % 3],
                    )
                    .unwrap();
            }
            inner.conn.execute_batch("ANALYZE").unwrap();
        }
        let _ = s.get_fanout("c", &cols(&["v"]));
        s.clear_cache();
        // Should still return a meaningful value after clear.
        let r = s.get_fanout("c", &cols(&["v"]));
        assert_ne!(r.source, FanoutSource::Default);
    }

    // Branch: compound index, second column constrained.
    #[test]
    fn get_fanout_compound_index_two_cols() {
        let s = open();
        exec(
            &s,
            "CREATE TABLE evt (id INTEGER PRIMARY KEY, t INTEGER, u INTEGER);
             CREATE INDEX idx_tu ON evt(t, u);",
        );
        {
            let inner = s.inner.lock().unwrap();
            let mut id = 1;
            for t in 1..=2 {
                for u in 1..=5 {
                    for _ in 0..4 {
                        inner
                            .conn
                            .execute(
                                "INSERT INTO evt (id, t, u) VALUES (?, ?, ?)",
                                rusqlite::params![id, t, u],
                            )
                            .unwrap();
                        id += 1;
                    }
                }
            }
            inner.conn.execute_batch("ANALYZE").unwrap();
        }
        // Two cols → fanout 4 (rows per (t,u) pair).
        let r = s.get_fanout("evt", &cols(&["t", "u"]));
        assert_ne!(r.source, FanoutSource::Default);
        if r.source == FanoutSource::Stat4 {
            assert_eq!(r.fanout, 4.0);
        }
    }

    // Branch: partial prefix (gap) → default fallback.
    #[test]
    fn get_fanout_partial_prefix_gap_falls_back() {
        let s = open();
        exec(
            &s,
            "CREATE TABLE partial (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);
             CREATE INDEX idx_abc ON partial(a, b, c);",
        );
        {
            let inner = s.inner.lock().unwrap();
            for i in 1..=30 {
                inner
                    .conn
                    .execute(
                        "INSERT INTO partial (id, a, b, c) VALUES (?, ?, ?, ?)",
                        rusqlite::params![i, i % 2, i % 3, i % 5],
                    )
                    .unwrap();
            }
            inner.conn.execute_batch("ANALYZE").unwrap();
        }
        // (a, c) does not match (a, b, c) at any depth.
        let r = s.get_fanout("partial", &cols(&["a", "c"]));
        assert_eq!(r.source, FanoutSource::Default);
        assert_eq!(r.fanout, 3.0);
    }

    // Branch: defensive — table name with quote rejected (find returns None).
    #[test]
    fn get_fanout_rejects_unsafe_table_name() {
        let s = open();
        let r = s.get_fanout("bad'name", &cols(&["x"]));
        assert_eq!(r.source, FanoutSource::Default);
    }

}
