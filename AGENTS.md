# MiniDB project guide

MiniDB is an embedded Rust key-value engine being built around an LSM tree. The goal is durable, concurrent byte storage with storage internals implemented in this repository. General-purpose crates are allowed. Implement the core storage engine here, without delegating it to RocksDB, LevelDB, sled, redb, or another database engine.

## Current state

Milestones 1–3 implement the byte API, durable WAL recovery, and an LSM read path with immutable tables. The crate uses Rust 2024 and requires Rust 1.88 or newer. It uses `crc32fast` for checksums and `fs2` for advisory file locking.

- `src/lib.rs` exports `MiniDB`, `Options`, `Error`, and `Result`.
- `src/db.rs` implements the public API and coordinates locking, reads, recovery, and synchronous flushes. A `RefCell` allows single-threaded access through `&self`.
- `src/memtable.rs` uses a `BTreeMap`, tracks approximate memory use, and stores values and explicit tombstones. Reads check active memory first and then immutable tables from newest to oldest. A tombstone ends the search.
- `src/wal.rs` creates, appends, and replays WAL version 1. A write synchronizes the WAL before updating the MemTable. Recovery validates record headers and payloads, repairs an incomplete tail, and reports corruption.
- `src/sstable.rs` writes sorted, checksummed blocks and loads a sparse index for point lookups. `src/manifest.rs` records the active WAL and live tables in generation order. See [storage formats and durability](docs/storage_format.md).
- `src/options.rs` and `src/error.rs` define optional per-handle input limits and typed errors. The WAL format bounds each field, and replay uses format limits. All key operations reject empty keys. Empty values are valid.
- Public API and recovery tests cover restart, damaged files, exclusive opens, cross-table precedence, and forced process termination. Internal fault injection exercises interrupted flush stages.

`open` requires Unix directory synchronization and holds the directory lock until the handle is dropped or the process exits. A failed WAL append or sync leaves the current MemTable intact and requires reopening for more writes. A flush stages the table and the next WAL, publishes a durable manifest, then retires the old WAL. The ordinary threshold is 4 MiB of approximate MemTable usage and callers can override it with `open_with_flush_threshold`. A threshold-crossing mutation can return `AutoFlushFailed` after its WAL sync and MemTable update. Table count and read amplification grow between compactions, which arrive in Milestone 4.

## Milestones

1. **Complete.** In-memory byte API, MemTable, typed errors, options, and tests.
2. **Complete.** Checksummed binary WAL, strict synchronization before mutation acknowledgment, ordered recovery, and an exclusive directory lock. Verified restart, incomplete tails, corruption, and forced process termination.
3. **Complete.** Indexed SSTables, manual and automatic MemTable flushing, WAL rotation, and a crash-consistent manifest. Milestone 2 databases migrate from the legacy `wal.log`.
4. **Next, when explicitly requested.** Add background compaction using a sorted-stream merge. Preserve newest-value precedence and keep tombstones until dropping them cannot expose older values.
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

Milestone 3 passed formatting and strict Clippy on Rust 1.100. All 35 reported tests passed on Rust 1.100 and Rust 1.88 on macOS, including process interruption at four flush publication boundaries and an ambiguous manifest rename error. Report future test and benchmark results only after running them.

## Agent workflow

The coordinating agent follows this workflow for implementation work.

1. Explain the plan for the requested milestone or fix. Ask `minidb-implementer` to make the scoped changes and run relevant checks.
2. Once the implementer returns, ask `minidb-reviewer` to independently review the change. Supply the diff or changed paths, acceptance criteria, and check results. The reviewer suggests fixes without editing the implementation.
3. Stop when there are no actionable in-scope findings. Otherwise, send confirmed findings back to the implementer and ask the reviewer to check the resulting changes.

The coordinating agent tracks a maximum of **three runs per role per task**. The initial run counts, and so does every retry, resume, or follow-up assignment. Run the roles sequentially and count equivalent fallback agents against the same limit. If the limit leaves findings unresolved, report them to the user. The roles never delegate to other agents.

The roles are defined in `.codex/agents/`. If named profiles are unavailable in the current client, use the corresponding role instructions when delegating.
