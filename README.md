# MiniDB

MiniDB is a Rust embedded key-value engine. Milestone 2 adds a durable write-ahead log (WAL) and crash recovery for the in-memory MemTable. The crate requires Rust 1.88 or newer. Opening a database requires Unix advisory file locking and permission to open and synchronize the resolved database directory and every ancestor up to the root. Each of those directories must support directory synchronization.

## Usage

```rust
use minidb::{MiniDB, Result};

fn main() -> Result<()> {
    let db = MiniDB::open("./data")?;
    db.put(b"user:1", b"Alice")?;
    db.put(b"user:2", b"Bob")?;
    db.delete(b"user:2")?;
    db.close()?;

    let db = MiniDB::open("./data")?;
    assert_eq!(db.get(b"user:1")?.as_deref(), Some(b"Alice".as_slice()));
    assert_eq!(db.get(b"user:2")?, None);
    db.close()
}
```

Keys and values can contain arbitrary bytes. Keys must have at least one byte, and a stored empty value is returned as `Some(Vec::new())`. The WAL format limits each key and value to `u32::MAX` bytes. `open_with_options` accepts smaller per-handle limits in bytes. Limits apply to operations on that handle. Recovery applies existing records in order using the format limits.

## Architecture

`MiniDB::open` creates missing directories, acquires an exclusive advisory lock, and replays the WAL into a `BTreeMap` MemTable. The lock stays held until the handle closes or the process exits. Each key maps to a value or a tombstone. The public `get` API returns owned bytes for a value and `None` for a deleted or missing key.

Each successful `put` or `delete` appends and synchronizes one WAL record before changing the MemTable. After an append or synchronization error, reads still observe the current MemTable and the handle requires reopening before further mutations. Recovery discards an incomplete final record and reports corrupt data. A complete record can be recovered even when its call did not return success. See the [WAL format and durability model](docs/storage_format.md).

A `RefCell` provides single-threaded access through `&self`. The full logical state fits in memory, and the WAL grows with every successful mutation. `flush` returns `Error::PersistenceUnavailable` until persistent table storage is implemented.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```
