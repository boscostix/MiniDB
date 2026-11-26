# MiniDB project guide

MiniDB is an embedded Rust key-value engine being built around an LSM tree. The goal is durable, concurrent byte storage with storage internals implemented in this repository. General-purpose crates are allowed. Implement the core storage engine here, without delegating it to RocksDB, LevelDB, sled, redb, or another database engine.

## Current state

Milestones 1 and 2 implement the in-memory byte API and durable WAL recovery. The crate uses Rust 2024 and requires Rust 1.88 or newer. It uses `crc32fast` for checksums and `fs2` for advisory file locking.

- `src/lib.rs` exports `MiniDB`, `Options`, `Error`, and `Result`.
- `src/db.rs` implements the public API, acquires the directory lock, and applies mutations to the MemTable after WAL synchronization. A `RefCell` allows single-threaded access through `&self`.
- `src/memtable.rs` uses a `BTreeMap` ordered lexicographically by key bytes. It distinguishes a stored value, a tombstone, and a missing key. The public `get` returns owned bytes for a stored value and `None` for a tombstone or missing key.
- `src/wal.rs` publishes, appends, and replays WAL version 1. Recovery validates record headers and payloads, truncates a recoverable incomplete tail, and reports corruption. See [WAL format and durability](docs/storage_format.md).
- `src/options.rs` and `src/error.rs` define optional per-handle input limits and typed errors. The WAL format bounds each field, and replay uses format limits. All key operations reject empty keys. Empty values are valid.
- `tests/basic.rs` covers the public API, and `tests/recovery.rs` covers restart, damaged logs, exclusive opens, and forced process termination. WAL and MemTable unit tests live alongside their implementations.

`open` requires Unix directory synchronization and holds the directory lock until the handle is dropped or the process exits. A failed WAL append or sync leaves the current MemTable intact and requires reopening for more writes. The full logical state lives in memory, and the WAL grows with every mutation. `flush` returns `Error::PersistenceUnavailable` until persistent table storage is implemented.

## Milestones

1. **Complete.** In-memory byte API, MemTable, typed errors, options, and tests.
2. **Complete.** Checksummed binary WAL, strict synchronization before mutation acknowledgment, ordered recovery, and an exclusive directory lock. Verified restart, incomplete tails, corruption, and forced process termination.
3. **Next, when explicitly requested.** Add versioned, indexed SSTables, MemTable flushing, safe WAL rotation, and a crash-consistent manifest. Publish metadata durably before removing superseded files.
4. Add background compaction using a sorted-stream merge. Preserve newest-value precedence and keep tombstones until dropping them cannot expose older values.
5. Add thread-safe access, document consistency and visibility, and validate concurrent operations and crash recovery with randomized testing and fault injection.
6. Add Bloom filters and a bounded block cache. Measure throughput, latency percentiles, memory use, and relevant storage tradeoffs with reproducible benchmarks.

## Working in this repository

- Work on one milestone at a time. Explain the design before coding and begin the next milestone only when requested.
- Preserve existing API behavior except where the active milestone explicitly changes it. Keep storage modules focused, avoid `unsafe` without a documented reason, and use typed errors for recoverable failures.
- Add tests for new behavior and failure paths. Document format versions, durability assumptions, and operation ordering as those guarantees are introduced.
- Update this file when a milestone lands. Record completed work, current guarantees and limitations, verification results, and the next milestone. Keep runnable usage and public-facing documentation in `README.md`.

Run these checks before marking a milestone complete.

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```

Milestone 2 passed formatting and strict Clippy on Rust 1.100. All 24 reported tests passed on Rust 1.100 and Rust 1.88 on macOS, including forced process termination and recovery. Report future test and benchmark results only after running them.

## Agent workflow

The coordinating agent follows this workflow for implementation work.

1. Explain the plan for the requested milestone or fix. Ask `minidb-implementer` to make the scoped changes and run relevant checks.
2. Once the implementer returns, ask `minidb-reviewer` to independently review the change. Supply the diff or changed paths, acceptance criteria, and check results. The reviewer suggests fixes without editing the implementation.
3. Stop when there are no actionable in-scope findings. Otherwise, send confirmed findings back to the implementer and ask the reviewer to check the resulting changes.

The coordinating agent tracks a maximum of **three runs per role per task**. The initial run counts, and so does every retry, resume, or follow-up assignment. Run the roles sequentially and count equivalent fallback agents against the same limit. If the limit leaves findings unresolved, report them to the user. The roles never delegate to other agents.

The roles are defined in `.codex/agents/`. If named profiles are unavailable in the current client, use the corresponding role instructions when delegating.
