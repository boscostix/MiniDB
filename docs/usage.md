# MiniDB Usage Guide

MiniDB is an embedded Rust key-value store for byte strings. Your application opens a database directory and calls the library directly. This guide shows how to add MiniDB to an application, store and recover data, configure the handle, and share it across threads.

## Add MiniDB to a Rust project

Use Rust 1.88 or newer on a Unix filesystem that supports advisory file locking and directory synchronization. The process needs to be able to open and synchronize the database directory and its ancestors.

With a checkout of MiniDB in a directory named `MiniDB`, run these commands from that directory to create a sibling application.

```sh
cd ..
cargo new --bin minidb-demo
cd minidb-demo
```

Add this line under `[dependencies]` in the application's `Cargo.toml`. Adjust the path if your checkout is elsewhere.

```toml
minidb = { path = "../MiniDB" }
```

To use an example below, replace the application's `src/main.rs` with the Rust block and run `cargo run` from the application directory. Each program uses a separate database directory relative to that working directory.

## Store data and reopen the database

`MiniDB::open` creates the directory if needed. `put` inserts or replaces a key, `get` returns an owned `Option<Vec<u8>>`, and `delete` records a deletion. A missing or deleted key returns `None`. A stored empty value returns `Some(Vec::new())`.

```rust
use minidb::{MiniDB, Result};

fn main() -> Result<()> {
    let path = "./data-basic";
    let db = MiniDB::open(path)?;

    db.put(b"user:42", b"Ada")?;
    db.put(b"user:42", b"Ada Lovelace")?;
    db.put(b"user:42:notes", b"")?;
    assert_eq!(db.get(b"user:42:notes")?, Some(Vec::new()));
    db.delete(b"user:42:notes")?;
    db.flush()?;

    db.put(b"status", b"ready")?;
    db.close()?;

    let db = MiniDB::open(path)?;
    assert_eq!(
        db.get(b"user:42")?.as_deref(),
        Some(b"Ada Lovelace".as_slice())
    );
    assert_eq!(db.get(b"user:42:notes")?, None);
    assert_eq!(db.get(b"status")?.as_deref(), Some(b"ready".as_slice()));
    println!("Recovered the user and status.");
    db.close()
}
```

MiniDB synchronizes the write-ahead log (WAL) before acknowledging each mutation. In this example, the explicit flush writes the user record and deletion to a sorted disk table. The final status write can be recovered directly from the WAL. `close` joins the background workers, reports worker errors, and releases the directory lock.

## Encode keys and configure limits

Keys must contain at least one byte. Keys and values can contain any bytes, including zeros. Use a consistent encoding for each logical key. For example, `42_u64.to_be_bytes()` always gives the same eight-byte key, while a text prefix such as `b"user:"` can distinguish one family of keys from another.

Configure limits with `MiniDB::open_with_options` to keep the default flush threshold. Use `MiniDB::open_with_flush_threshold` to set both limits and the threshold.

```rust
use minidb::{Error, MiniDB, Options, Result};

fn main() -> Result<()> {
    let options = Options {
        max_key_bytes: Some(64),
        max_value_bytes: Some(4 * 1024),
    };
    let db = MiniDB::open_with_flush_threshold("./data-limits", options, 1024 * 1024)?;

    let key = 42_u64.to_be_bytes();
    db.put(&key, b"binary key")?;
    assert_eq!(db.get(&key)?.as_deref(), Some(b"binary key".as_slice()));
    assert!(matches!(
        db.put(&key, &[0; 4097]),
        Err(Error::ValueTooLarge { .. })
    ));
    db.close()
}
```

Input limits apply to calls through the current handle. Recovery uses the storage format limits. For example, after reopening with a smaller key limit, reads and deletes using a longer existing key return `KeyTooLarge`. Adjust the limit on the next open to access that key.

The default flush threshold is approximately 4 MiB of MemTable entries. A mutation that reaches the threshold waits for the flush worker. A threshold of zero flushes after every successful mutation. Use `flush()` when you want pending entries in an SSTable at a specific point, such as the end of an import.

## Share one handle across threads

Share a single handle through `Arc<MiniDB>`. MiniDB serializes mutations and allows reads to overlap writes, flushes, and compaction. Once a write completes, a later read of the key can see that write or a newer update. Each call takes its own view of the database.

```rust
use std::{sync::Arc, thread};

use minidb::{MiniDB, Result};

fn main() -> Result<()> {
    let db = Arc::new(MiniDB::open("./data-shared")?);
    db.put(b"jobs:1", b"done")?;

    let writer_db = Arc::clone(&db);
    let writer = thread::spawn(move || writer_db.put(b"jobs:2", b"queued"));

    assert_eq!(db.get(b"jobs:1")?.as_deref(), Some(b"done".as_slice()));
    writer.join().expect("writer thread panicked")?;
    assert_eq!(db.get(b"jobs:2")?.as_deref(), Some(b"queued".as_slice()));

    let db = Arc::into_inner(db).expect("all other handles have been dropped");
    db.close()
}
```

Join the threads and release their copies of the `Arc` before calling `close()`. Opening the same directory a second time while the first handle is active returns `DatabaseLocked`.

## Handle errors and reopen safely

| Error or status | What it means for your application |
| --- | --- |
| `EmptyKey`, `KeyTooLarge`, `ValueTooLarge` | The call failed input validation. Correct the bytes or reopen with limits suited to the data. |
| `DatabaseLocked` | Another handle owns the directory lock. Share that handle within the process, or wait for it to close before opening the directory. |
| `AutoFlushFailed { cause }` | The calling mutation is already durable and visible. Inspect the flush error. A later valid mutation retries an over-threshold flush before writing its own record. |
| WAL append or sync error, followed by `WalWriteFailed` | Reopen before writing again. A complete record from a failed call may be recovered, so check the recovered state before retrying a mutation. |
| `FlushRequiresReopen` | Manifest publication may have reached disk. Close the handle and reopen the same directory so recovery can select the active files. |
| `CorruptWal`, `CorruptManifest`, `CorruptSstable` | MiniDB detected invalid stored data. Preserve the directory for inspection or restore from a known-good backup. |

Call `compaction_error()` to inspect a recorded background compaction failure while the handle is open. Explicit `close()` returns a recorded compaction error or a worker panic. Keep the database directory together during transfers. It contains the WAL, manifest, tables, and lock file. For an offline copy, close all users of the database and copy the whole directory. Details on recovery and the file formats are in [Storage formats and durability](storage_format.md).

## Run tests and benchmarks

From the MiniDB checkout, run the locked test suite with the following command.

```sh
cargo test --locked
```

The benchmark guide has commands for comparing the current build against the Milestone 5 baseline, along with measured throughput, latency, and storage costs. [Performance methodology and results](performance.md)
