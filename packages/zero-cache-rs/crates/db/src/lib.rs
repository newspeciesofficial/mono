//! Database abstractions for zero-cache-rs.
//!
//! Provides connection pools and helpers for both PostgreSQL (via `deadpool-postgres`)
//! and SQLite (via `rusqlite`), including a dedicated writer thread and a
//! concurrent reader pool for SQLite.

pub mod migrations;
pub mod operator_storage;
pub mod pg;
pub mod sqlite_reader;
pub mod sqlite_writer;
pub mod transaction;

pub use pg::PgPool;
pub use sqlite_reader::SqliteReaderPool;
pub use sqlite_writer::SqliteWriter;
