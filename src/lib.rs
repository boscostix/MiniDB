//! An embedded key-value database for arbitrary byte keys and values.
//!
//! Mutations are synchronized to a write-ahead log before they become visible.
//! Opening a database replays its log into an in-memory table.

mod db;
mod error;
mod memtable;
mod options;
mod wal;

pub use db::MiniDB;
pub use error::{Error, Result};
pub use options::Options;
