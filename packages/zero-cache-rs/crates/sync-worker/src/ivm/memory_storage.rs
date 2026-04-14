//! Port of `packages/zql/src/ivm/memory-storage.ts`.
//!
//! TS:
//! ```ts
//! export class MemoryStorage implements Storage {
//!   #data: BTreeSet<Entry> = new BTreeSet(comparator); // sorted by UTF-8 key
//!   set(key, value)       { this.#data.add([key, value]); }
//!   get(key, def?)        { return this.#data.get([key, null])?.[1] ?? def; }
//!   del(key)              { this.#data.delete([key, null]); }
//!   *scan({prefix}?)      { iterate from prefix, stop when key no longer startsWith(prefix) }
//!   cloneData()           { return structuredClone(Object.fromEntries(this.#data.values())); }
//! }
//! ```
//!
//! Simple in-memory implementation of the [`Storage`] trait for client
//! pipelines and tests. Keys are ordered UTF-8 bytewise, matching
//! `compareUTF8` in TS (Rust `String`'s default `Ord` is bytewise over UTF-8,
//! which is equivalent).
//!
//! Rust parity notes:
//!
//! - TS uses a `BTreeSet<[key, value]>` keyed by `key` alone. Rust uses
//!   `BTreeMap<String, JsonValue>` which enforces key-uniqueness natively
//!   and iterates in sorted order.
//! - TS `scan({prefix})` iterates from the first key `>= prefix` and stops
//!   at the first key that no longer starts with `prefix`. Rust uses
//!   `BTreeMap::range(prefix..)` and `take_while` with `starts_with`.
//! - TS `scan()` (no options) iterates all entries in key order.
//! - TS `get(key, def?)` returns `def` (or `undefined`) when key is absent.
//!   The Rust trait signature returns `Option<JsonValue>` — `None` only when
//!   key is absent **and** `def` is `None`.
//! - `cloneData` is not part of the `Storage` trait; it is an inherent
//!   convenience method matching the TS one.

use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::ivm::operator::{ScanOptions, Storage};
use crate::ivm::stream::Stream;

/// TS `MemoryStorage` — in-memory `Storage` backend.
#[derive(Debug, Default, Clone)]
pub struct MemoryStorage {
    data: BTreeMap<String, JsonValue>,
}

impl MemoryStorage {
    /// Constructs an empty `MemoryStorage`.
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    /// TS `cloneData()` — returns a deep clone of the current data as a
    /// plain map. Matches `structuredClone(Object.fromEntries(...))` in TS
    /// (JSON values cloned via `serde_json::Value::clone`).
    pub fn clone_data(&self) -> BTreeMap<String, JsonValue> {
        self.data.clone()
    }
}

impl Storage for MemoryStorage {
    /// TS `set(key, value)` — overwrites any prior value at `key`.
    fn set(&mut self, key: &str, value: JsonValue) {
        self.data.insert(key.to_owned(), value);
    }

    /// TS `get(key, def?)`. Returns the stored value if present, otherwise
    /// the caller-provided default. Returns `None` only when key is absent
    /// **and** `def` is `None`.
    fn get(&self, key: &str, def: Option<&JsonValue>) -> Option<JsonValue> {
        match self.data.get(key) {
            Some(v) => Some(v.clone()),
            None => def.cloned(),
        }
    }

    /// TS `scan({prefix}?)`. Iterates `(key, value)` pairs in key order.
    /// When `prefix` is set, starts at the first key `>= prefix` and stops
    /// at the first key that no longer starts with `prefix`.
    fn scan<'a>(&'a self, options: Option<ScanOptions>) -> Stream<'a, (String, JsonValue)> {
        match options.and_then(|o| o.prefix) {
            Some(prefix) => {
                // `range(prefix..)` yields all keys `>= prefix` in order. The
                // `take_while` mirrors TS: stop as soon as a key no longer
                // starts with the prefix (they are contiguous in sort order).
                let iter = self
                    .data
                    .range(prefix.clone()..)
                    .take_while(move |(k, _)| k.starts_with(&prefix))
                    .map(|(k, v)| (k.clone(), v.clone()));
                Box::new(iter)
            }
            None => {
                let iter = self.data.iter().map(|(k, v)| (k.clone(), v.clone()));
                Box::new(iter)
            }
        }
    }

    /// TS `del(key)` — no-op when key is absent.
    fn del(&mut self, key: &str) {
        self.data.remove(key);
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!   - `new()` / `Default::default()` — empty state.
    //!   - `set` inserts a new key.
    //!   - `set` on an existing key overwrites.
    //!   - `get` returns the stored value when key present, ignoring `def`.
    //!   - `get` returns `def` when key absent and `def` is `Some`.
    //!   - `get` returns `None` when key absent and `def` is `None`.
    //!   - `del` removes an existing key.
    //!   - `del` on a missing key is a no-op (no panic).
    //!   - `scan(None)` returns all entries in key order on a populated map.
    //!   - `scan(None)` returns empty iterator on an empty map.
    //!   - `scan(Some {prefix: None})` same as `scan(None)`.
    //!   - `scan(Some {prefix})` returns only keys starting with prefix,
    //!     stopping at the first non-matching key (sorted-order invariant).
    //!   - `scan(Some {prefix})` with no matches returns empty.
    //!   - `scan(Some {prefix: ""})` returns all entries.
    //!   - `clone_data()` returns a deep copy.

    use super::*;
    use serde_json::json;

    fn populated() -> MemoryStorage {
        let mut s = MemoryStorage::new();
        s.set("a", json!(1));
        s.set("b/1", json!("x"));
        s.set("b/2", json!("y"));
        s.set("c", json!(true));
        s
    }

    // Branch: new() starts empty — get on anything returns None.
    #[test]
    fn new_is_empty() {
        let s = MemoryStorage::new();
        assert!(s.get("anything", None).is_none());
        let out: Vec<_> = s.scan(None).collect();
        assert!(out.is_empty());
    }

    // Branch: Default impl matches new().
    #[test]
    fn default_is_empty() {
        let s = MemoryStorage::default();
        assert!(s.get("x", None).is_none());
    }

    // Branch: set inserts a new key.
    #[test]
    fn set_inserts_new_key() {
        let mut s = MemoryStorage::new();
        s.set("k", json!(42));
        assert_eq!(s.get("k", None), Some(json!(42)));
    }

    // Branch: set on existing key overwrites.
    #[test]
    fn set_overwrites_existing() {
        let mut s = MemoryStorage::new();
        s.set("k", json!(1));
        s.set("k", json!(2));
        assert_eq!(s.get("k", None), Some(json!(2)));
    }

    // Branch: get returns stored value (ignores def) when key present.
    #[test]
    fn get_present_ignores_default() {
        let mut s = MemoryStorage::new();
        s.set("k", json!("v"));
        let def = json!("default");
        assert_eq!(s.get("k", Some(&def)), Some(json!("v")));
    }

    // Branch: get returns def when key absent and def provided.
    #[test]
    fn get_absent_returns_default() {
        let s = MemoryStorage::new();
        let def = json!("default");
        assert_eq!(s.get("missing", Some(&def)), Some(json!("default")));
    }

    // Branch: get returns None when absent and def None.
    #[test]
    fn get_absent_no_default_returns_none() {
        let s = MemoryStorage::new();
        assert!(s.get("missing", None).is_none());
    }

    // Branch: del removes an existing key.
    #[test]
    fn del_removes_existing_key() {
        let mut s = MemoryStorage::new();
        s.set("k", json!(1));
        s.del("k");
        assert!(s.get("k", None).is_none());
    }

    // Branch: del on missing key is a silent no-op.
    #[test]
    fn del_missing_key_is_noop() {
        let mut s = MemoryStorage::new();
        s.del("never-set");
        // No panic, state still empty.
        assert!(s.get("never-set", None).is_none());
    }

    // Branch: scan(None) yields all entries in key order.
    #[test]
    fn scan_none_yields_all_in_order() {
        let s = populated();
        let out: Vec<_> = s.scan(None).collect();
        assert_eq!(
            out,
            vec![
                ("a".to_string(), json!(1)),
                ("b/1".to_string(), json!("x")),
                ("b/2".to_string(), json!("y")),
                ("c".to_string(), json!(true)),
            ]
        );
    }

    // Branch: scan on empty storage yields empty.
    #[test]
    fn scan_empty_storage() {
        let s = MemoryStorage::new();
        let out: Vec<_> = s.scan(Some(ScanOptions::default())).collect();
        assert!(out.is_empty());
    }

    // Branch: scan(Some{prefix: None}) behaves like scan(None).
    #[test]
    fn scan_some_with_no_prefix_yields_all() {
        let s = populated();
        let out: Vec<_> = s.scan(Some(ScanOptions { prefix: None })).collect();
        assert_eq!(out.len(), 4);
    }

    // Branch: scan with prefix yields matching keys and stops early.
    #[test]
    fn scan_with_prefix_filters_and_stops_early() {
        let s = populated();
        let out: Vec<_> = s
            .scan(Some(ScanOptions {
                prefix: Some("b/".into()),
            }))
            .collect();
        assert_eq!(
            out,
            vec![
                ("b/1".to_string(), json!("x")),
                ("b/2".to_string(), json!("y")),
            ]
        );
    }

    // Branch: prefix between two existing keys (no matches) yields empty.
    #[test]
    fn scan_with_prefix_no_matches() {
        let s = populated();
        let out: Vec<_> = s
            .scan(Some(ScanOptions {
                prefix: Some("zzz".into()),
            }))
            .collect();
        assert!(out.is_empty());
    }

    // Branch: empty-string prefix matches every key.
    #[test]
    fn scan_with_empty_prefix_yields_all() {
        let s = populated();
        let out: Vec<_> = s
            .scan(Some(ScanOptions {
                prefix: Some(String::new()),
            }))
            .collect();
        assert_eq!(out.len(), 4);
    }

    // Branch: scan stops at the first non-matching key after matches.
    // Guards the early-termination contract: with `x1`, `x2`, `y1` sorted,
    // prefix "x" must not visit `y1`.
    #[test]
    fn scan_with_prefix_stops_at_first_non_match() {
        let mut s = MemoryStorage::new();
        s.set("x1", json!(1));
        s.set("x2", json!(2));
        s.set("y1", json!(3));
        let out: Vec<_> = s
            .scan(Some(ScanOptions {
                prefix: Some("x".into()),
            }))
            .collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, "x1");
        assert_eq!(out[1].0, "x2");
    }

    // Branch: clone_data returns an independent deep copy.
    #[test]
    fn clone_data_deep_copy() {
        let mut s = MemoryStorage::new();
        s.set("k", json!({"nested": 1}));
        let copy = s.clone_data();
        // Mutating the original does not affect the clone.
        s.set("k", json!({"nested": 999}));
        assert_eq!(copy.get("k"), Some(&json!({"nested": 1})));
    }

    // Branch: UTF-8 ordering — multibyte keys sort lexicographically by
    // bytes (matches compareUTF8 in TS).
    #[test]
    fn scan_utf8_byte_order() {
        let mut s = MemoryStorage::new();
        s.set("b", json!(1));
        s.set("a", json!(2));
        s.set("ü", json!(3)); // multibyte, sorts after ASCII
        let keys: Vec<String> = s.scan(None).map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["a", "b", "ü"]);
    }
}
