//! An embedded key-value database for arbitrary byte keys and values.
//!
//! Each handle owns an independent in-memory database. Data lives for the
//! lifetime of the handle.

mod db;
mod error;
mod memtable;
mod options;

pub use db::MiniDB;
pub use error::{Error, Result};
pub use options::Options;
