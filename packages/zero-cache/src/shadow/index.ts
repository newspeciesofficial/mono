/**
 * Native-SQLite differential testing harness for the Rust port of
 * zero-cache. Each side opens its own connection to the same SQLite
 * replica file; WAL2 makes concurrent readers safe. The `diff` helper
 * compares TS and Rust IVM outputs row-by-row.
 *
 * The previous "record-and-replay" architecture (where TS recorded every
 * SQLite call into per-kind queues and Rust consumed from them) was
 * replaced once the Rust port got its own native SQLite via a forked
 * `libsqlite3-sys` pointing at the `@rocicorp/zero-sqlite3` amalgamation.
 * See `crates/libsqlite3-sys/` for details.
 */

export {diff} from './diff.ts';
export type {DiffResult} from './diff.ts';

export {tryLoadShadowNative, loadShadowNative} from './native.ts';
export type {ShadowNative, ShadowRowChange} from './native.ts';
