//! An embedded key-value database for arbitrary byte keys and values.
//!
//! Mutations are synchronized to a write-ahead log before they become visible.
//! Opening a database loads the table indexes and replays its active log.

mod block_cache;
mod bloom;
mod compaction;
mod db;
mod error;
mod manifest;
mod memtable;
mod options;
mod sstable;
mod storage;
mod wal;

pub use db::MiniDB;
pub use error::{Error, Result};
pub use options::Options;
