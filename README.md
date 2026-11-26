# MiniDB

MiniDB is a Rust embedded key-value engine. Milestone 3 adds immutable sorted tables (SSTables), a durable manifest, and WAL rotation to the in-memory MemTable and write-ahead log (WAL). The crate requires Rust 1.88 or newer. Opening a database requires Unix advisory file locking and permission to open and synchronize the resolved database directory and every ancestor up to the root. Each of those directories must support directory synchronization.

## Usage

```rust
use minidb::{MiniDB, Result};

fn main() -> Result<()> {
    let db = MiniDB::open("./data")?;
    db.put(b"user:1", b"Alice")?;
    db.put(b"user:2", b"Bob")?;
    db.delete(b"user:2")?;
    db.flush()?;
    db.close()?;

    let db = MiniDB::open("./data")?;
    assert_eq!(db.get(b"user:1")?.as_deref(), Some(b"Alice".as_slice()));
    assert_eq!(db.get(b"user:2")?, None);
    db.close()
}
```

Keys and values can contain arbitrary bytes. Keys must have at least one byte, and a stored empty value is returned as `Some(Vec::new())`. The formats limit each key and value to `u32::MAX` bytes. `open_with_options` accepts smaller per-handle limits. To configure the approximate MemTable byte threshold, call `MiniDB::open_with_flush_threshold(path, options, bytes)`. The default threshold is 4 MiB. A threshold of zero flushes after every successful mutation.

## Architecture

`MiniDB::open` acquires an exclusive directory lock, reads the manifest and live table indexes, and replays the active WAL into a `BTreeMap` MemTable. An existing WAL-only database is upgraded automatically. Keep using a manifest-aware version after upgrade because older writers ignore the manifest. Reads check the MemTable first, then search SSTables newest to oldest. Each candidate table reads at most one checksummed data block. The public `get` API returns owned bytes for a value and `None` for a deleted or missing key.

Each `put` or `delete` synchronizes the WAL record before changing the MemTable. `flush` synchronously persists the active MemTable to an SSTable and rotates the WAL. A write that reaches the threshold attempts this flush immediately. `Error::AutoFlushFailed` means the mutation is already durable and visible, and its automatic flush failed. A later valid mutation retries any over-threshold MemTable before appending its own record. A single large entry can exceed the threshold until it is flushed.

A `RefCell` provides single-threaded access through `&self`. The handle retains the active MemTable and one in-memory index per live table. A lookup opens an SSTable only while reading its candidate block. Without compaction, table count grows with each nonempty flush. See the [storage format and durability model](docs/storage_format.md) for publication order, error recovery, and compatibility details.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```
