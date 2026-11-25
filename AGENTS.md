# MiniDB project guide

MiniDB is an embedded Rust key-value engine being built around an LSM tree. The goal is durable, concurrent byte storage with storage internals implemented in this repository. General-purpose crates are allowed. Implement the core storage engine here, without delegating it to RocksDB, LevelDB, sled, redb, or another database engine.

## Current state

Milestone 1 is complete. The crate uses Rust 2024, requires Rust 1.88 or newer, and has no dependencies.

- `src/lib.rs` exports `MiniDB`, `Options`, `Error`, and `Result`.
- `src/db.rs` implements `open`, `open_with_options`, `put`, `get`, `delete`, `flush`, and `close`. A `RefCell` allows single-threaded access through `&self`.
- `src/memtable.rs` uses a `BTreeMap` ordered lexicographically by key bytes. It distinguishes a stored value, a tombstone, and a missing key. The public `get` returns owned bytes for a stored value and `None` for a tombstone or missing key.
- `src/options.rs` and `src/error.rs` define optional input-size limits and typed errors. Limits default to unlimited. All key operations reject empty keys. Empty values are valid, and a failed mutation leaves existing data intact.
- `tests/basic.rs` covers the public API. MemTable unit tests and the `open` doctest live alongside their implementations.

Every `open` currently creates an independent empty database and ignores the path. `close` consumes the handle and releases the data. `flush` returns `Error::PersistenceUnavailable` until persistent table storage is implemented.

## Milestones

1. **Complete.** In-memory byte API, MemTable, typed errors, options, and tests.
2. **Next, when explicitly requested.** Add an append-only binary WAL with framed, checksummed records and an exclusive directory lock. Synchronize the WAL before applying a mutation to the MemTable or acknowledging the write. Replay valid records in order, tolerate an incomplete final record, and report corruption. Test restart, truncated tails, corruption, and forced process termination.
3. Add versioned, indexed SSTables, MemTable flushing, safe WAL rotation, and a crash-consistent manifest. Publish metadata durably before removing superseded files.
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
cargo test
```

Milestone 1 passed formatting and strict Clippy on Rust 1.100. All 12 tests passed on Rust 1.100 and Rust 1.88. Report future test and benchmark results only after running them.

## Agent workflow

The coordinating agent follows this workflow for implementation work.

1. Explain the plan for the requested milestone or fix. Ask `minidb-implementer` to make the scoped changes and run relevant checks.
2. Once the implementer returns, ask `minidb-reviewer` to independently review the change. Supply the diff or changed paths, acceptance criteria, and check results. The reviewer suggests fixes without editing the implementation.
3. Stop when there are no actionable in-scope findings. Otherwise, send confirmed findings back to the implementer and ask the reviewer to check the resulting changes.

The coordinating agent tracks a maximum of **three runs per role per task**. The initial run counts, and so does every retry, resume, or follow-up assignment. Run the roles sequentially and count equivalent fallback agents against the same limit. If the limit leaves findings unresolved, report them to the user. The roles never delegate to other agents.

The roles are defined in `.codex/agents/`. If named profiles are unavailable in the current client, use the corresponding role instructions when delegating.
