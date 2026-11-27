# MiniDB

MiniDB is a Rust embedded key-value engine. It uses an in-memory MemTable, a write-ahead log (WAL), immutable sorted tables (SSTables), and a durable manifest. MiniDB supports concurrent access and runs flushes and compaction on background workers. The crate requires Rust 1.88 or newer. Opening a database requires Unix advisory file locking and permission to open and synchronize the resolved database directory and every ancestor up to the root. Each of those directories must support directory synchronization.

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

`MiniDB::open` acquires an exclusive directory lock, reads the manifest and live table indexes, and replays the active WAL into a `BTreeMap` MemTable. An existing WAL-only database is upgraded automatically. Keep using a manifest-aware version after upgrade because older writers ignore the manifest. Reads check the MemTable first, then search SSTables newest to oldest. Each candidate table needs at most one checksummed data block. Version 2 SSTable Bloom filters can skip a block on a definite miss. The per-handle cache reuses previously verified blocks. The public `get` API returns owned bytes for a value and `None` for a deleted or missing key.

Each `put` or `delete` synchronizes the WAL record before changing the MemTable. `flush` waits for the flush worker to persist the active MemTable to an SSTable and rotate the WAL. A write that reaches the threshold waits for the same worker. `Error::AutoFlushFailed` means the mutation is already durable and visible, and its automatic flush failed. A later valid mutation retries any over-threshold MemTable before appending its own record. A single large entry can exceed the threshold until it is flushed.

`MiniDB` is `Send + Sync`. Share it with `Arc<MiniDB>` or borrow it from scoped threads. Reads can overlap one another, a WAL sync, a flush, or compaction. A read checks the active MemTable and, on a miss, pins the current SSTable set under a shared lock. It releases that lock before probing the cache or opening the table for block I/O. Pinned lookups keep their files available until they finish.

Point operations are linearizable. Mutations and flush requests acquire one writer gate, and a mutation becomes visible when its synchronized WAL record is applied to the MemTable. A read overlapping that publication can see either the earlier or the new state. Reads begun after a successful write see that write or a later update to the same key. Independent calls each take their own view.

When at least four tables are live, a single background worker merges the four oldest tables. It keeps the newest entry for each key and removes tombstones whose older values were all covered by the merge. Reads and foreground flushes can proceed during the merge. The worker publishes through the durable manifest and leaves all tables outside the merge newer than the output. MiniDB can open Milestone 3 databases with version 1 manifests. New manifest publications use version 2.

A compaction error stops the worker for that handle. `compaction_error` reports the error once recorded, and `close` returns it after joining the worker. An error during manifest replacement or its directory sync also blocks further mutations and flushes until reopen. Both `close` and final drop join the flush and compaction workers before releasing the directory lock. Join users of a shared handle and recover ownership with `Arc::into_inner` to call `close` and receive worker errors. Queued merges can wait until the next open. Compaction can fall behind flushes and can repeatedly rewrite old data. See the [storage format and durability model](docs/storage_format.md) for publication and recovery details.

## Performance

New SSTables store checksummed per-block Bloom filters, including tombstones. Version 1 tables remain readable. A shared least-recently-used cache charges up to 4 MiB per opened handle for validated block allocations and estimated entry metadata. Concurrent reads can temporarily pin evicted blocks. Compaction streams its input blocks independently, and the cache budget excludes indexes, the active MemTable, compaction buffers, and caller-owned values.

Run the benchmark against the Milestone 5 baseline and the current worktree.

```sh
tools/benchmark.sh > minidb-performance.tsv
python3 tools/summarize_benchmark.py minidb-performance.tsv
```

The harness uses the same release-mode workload on each revision and reports throughput, p50/p95/p99 latency, requested allocations, peak RSS, and logical storage I/O. Workload sizes, cache state, repetition, the sync-every-write policy, and the group-commit experiment are in the [benchmark methodology and measured results](docs/performance.md).

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
cargo +1.88.0 test --locked
```

The GitHub Actions workflow runs formatting, strict Clippy, and locked tests on stable Rust and the minimum supported Rust version. It also runs a small release-mode benchmark smoke test.

## License

MiniDB is licensed under the [MIT License](LICENSE).
