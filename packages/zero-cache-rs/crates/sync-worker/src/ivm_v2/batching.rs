//! Within-query batched emission — ivm_v2 milestone scaffold.
//!
//! Given an iterator of `Change`, drive it and flush to a chunk sink every
//! `DEFAULT_EMIT_CHUNK` items. Wiring into `shadow-ffi` / JS
//! `ThreadsafeFunction` is a separate integration step.
//!
//! Matches JS poke batcher size of 100.

use super::change::Change;

pub const DEFAULT_EMIT_CHUNK: usize = 100;

/// Read `DEFAULT_EMIT_CHUNK` (or env-override) from
/// `ZERO_IVM_EMIT_CHUNK_SIZE`. Env var is consulted per-call so tests
/// can flip it live.
pub fn emit_chunk_size() -> usize {
    std::env::var("ZERO_IVM_EMIT_CHUNK_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_EMIT_CHUNK)
}

/// Drive `iter` and call `on_chunk(rows, is_final)` every N items and once
/// at end with `is_final = true`. Returns the total count emitted.
///
/// Contract: `on_chunk` returning `false` means the consumer cancelled —
/// the driver drops the iterator (stopping upstream) and returns.
pub fn drive_in_chunks<I, F>(iter: I, mut on_chunk: F) -> usize
where
    I: IntoIterator<Item = Change>,
    F: FnMut(Vec<Change>, bool) -> bool,
{
    let chunk_size = emit_chunk_size();
    let mut total = 0usize;
    let mut buf: Vec<Change> = Vec::with_capacity(chunk_size);
    for c in iter {
        buf.push(c);
        total += 1;
        if buf.len() >= chunk_size {
            let chunk = std::mem::take(&mut buf);
            if !on_chunk(chunk, false) {
                return total;
            }
            buf.reserve(chunk_size);
        }
    }
    // Final flush — always signal is_final=true so downstream knows we're done.
    on_chunk(buf, true);
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::{AddChange, Change};
    use crate::ivm::data::Node;
    use indexmap::IndexMap;
    use serde_json::json;
    use zero_cache_types::value::Row;

    fn mk_add(id: i64) -> Change {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        Change::Add(AddChange {
            node: Node {
                row: r,
                relationships: IndexMap::new(),
            },
        })
    }

    #[test]
    fn emits_chunks_of_default_size() {
        let src: Vec<Change> = (0..250).map(mk_add).collect();
        let mut chunks: Vec<(usize, bool)> = Vec::new();
        let total = drive_in_chunks(src, |rows, is_final| {
            chunks.push((rows.len(), is_final));
            true
        });
        assert_eq!(total, 250);
        // 100 + 100 + 50(final)
        assert_eq!(chunks, vec![(100, false), (100, false), (50, true)]);
    }

    #[test]
    fn consumer_cancel_stops_driver() {
        let src: Vec<Change> = (0..250).map(mk_add).collect();
        let mut chunks_seen = 0;
        let total = drive_in_chunks(src, |_rows, _is_final| {
            chunks_seen += 1;
            // Cancel after the first chunk.
            false
        });
        // First chunk delivered (100); cancellation stops further work.
        assert_eq!(chunks_seen, 1);
        assert_eq!(total, 100);
    }

    #[test]
    fn empty_iter_still_emits_final() {
        let src: Vec<Change> = vec![];
        let mut got: Option<(usize, bool)> = None;
        let total = drive_in_chunks(src, |rows, is_final| {
            got = Some((rows.len(), is_final));
            true
        });
        assert_eq!(total, 0);
        assert_eq!(got, Some((0, true)));
    }

    // NOTE: env-var override is exercised at runtime; a test-time toggle
    // would require `unsafe { std::env::set_var }` which workspace lint
    // forbids. Verified manually via `ZERO_IVM_EMIT_CHUNK_SIZE=10 cargo run ...`.
}
