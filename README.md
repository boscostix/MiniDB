# MiniDB

MiniDB is a Rust embedded key-value engine. Milestone 1 implements its in-memory MemTable and byte-oriented API using the Rust standard library. It requires Rust 1.88 or newer.

## Usage

```rust
use minidb::{MiniDB, Result};

fn main() -> Result<()> {
    let db = MiniDB::open("./data")?;
    db.put(b"user:1", b"Alice")?;
    assert_eq!(db.get(b"user:1")?.as_deref(), Some(b"Alice".as_slice()));
    db.delete(b"user:1")?;
    assert_eq!(db.get(b"user:1")?, None);
    db.close()
}
```

Keys and values can contain arbitrary bytes. Keys must have at least one byte, and a stored empty value is returned as `Some(Vec::new())`. `open_with_options` accepts optional key and value length limits in bytes. Both default to unlimited. Invalid mutations return an error before changing the table.

## Architecture

`MiniDB` validates inputs and delegates reads and mutations to a MemTable backed by a `BTreeMap` that orders keys lexicographically by byte. Each key maps to a value or a tombstone, which lets the MemTable distinguish a recorded deletion from a key it has never seen. The public `get` API returns `None` for both cases and returns owned bytes for a value.

A `RefCell` lets callers use the handle through `&self` for single-threaded access. Each call to `open` creates an independent empty database. The path is reserved for the persistent engine and is currently ignored. `close` consumes the handle and releases the data, and `flush` returns `Error::PersistenceUnavailable`. A deletion retains the key and its tombstone until overwritten or the handle is dropped.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
