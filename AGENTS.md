# MiniDB project guide

MiniDB is an embedded Rust key-value engine being built around an LSM tree. The goal is durable, concurrent byte storage with storage internals implemented in this repository. General-purpose crates are allowed. Implement the core storage engine here, without delegating it to RocksDB, LevelDB, sled, redb, or another database engine.

## Current state

Milestones 1–5 implement the byte API, durable WAL recovery, an LSM read path with immutable tables, background compaction, and concurrent access in a single process. The crate uses Rust 2024 and requires Rust 1.88 or newer. It uses `crc32fast` for checksums and `fs2` for advisory file locking.

- `src/lib.rs` exports `MiniDB`, `Options`, `Error`, and `Result`.
- `src/db.rs` implements the public API and coordinates recovery and background flush and compaction workers. The handle is `Send + Sync`. Readers take a shared view lock and release it before table I/O. Mutations and flush requests go through one writer gate. Flush callers wait for the worker result, and both workers serialize manifest publication.
- `src/memtable.rs` uses a `BTreeMap`, tracks approximate memory use, and stores values and explicit tombstones. Reads check active memory first and then immutable tables from newest to oldest. A tombstone ends the search.
- `src/wal.rs` creates, appends, and replays WAL version 1. A write synchronizes the WAL before updating the MemTable. Recovery validates record headers and payloads, repairs an incomplete tail, and reports corruption.
- `src/sstable.rs` writes sorted, checksummed blocks and loads a sparse index for point lookups. `src/manifest.rs` records the active WAL, file ID high-water mark, and live tables in lookup precedence order. `src/compaction.rs` streams the four oldest tables and drops a winning tombstone once the merge covers every older table. See [storage formats and durability](docs/storage_format.md).
- `src/options.rs` and `src/error.rs` define optional per-handle input limits and typed errors. The WAL format bounds each field, and replay uses format limits. All key operations reject empty keys. Empty values are valid.
- Public API and recovery tests cover restart, damaged files, exclusive opens, cross-table precedence, and forced process termination. Internal tests exercise concurrent readers and writers, compare seeded operations with a reference map, and interrupt child processes at WAL, flush, and compaction boundaries.

`open` requires Unix directory synchronization and holds the directory lock until the handle is dropped or the process exits. A failed WAL append or sync leaves the current MemTable intact and requires reopening for more writes. A flush stages the table and the next WAL, publishes a durable manifest, then retires the old WAL. The ordinary threshold is 4 MiB of approximate MemTable usage and callers can override it with `open_with_flush_threshold`. A threshold-crossing mutation can return `AutoFlushFailed` after its WAL sync and MemTable update. A single worker starts a merge when four tables are live, keeps newer tables ahead of its output in read order, and reclaims inputs only after durable publication and release by readers. The worker stops on error and reports it through `compaction_error` and `close`. An uncertain manifest replacement requires reopen before further writes. Close and drop join both workers before unlocking. A successful point read reflects one published state, and durable mutations become visible when applied to the MemTable. Compaction has no rate limit and can repeatedly rewrite older data.

## Milestones

1. **Complete.** In-memory byte API, MemTable, typed errors, options, and tests.
2. **Complete.** Checksummed binary WAL, strict synchronization before mutation acknowledgment, ordered recovery, and an exclusive directory lock. Verified restart, incomplete tails, corruption, and forced process termination.
3. **Complete.** Indexed SSTables, manual and automatic MemTable flushing, WAL rotation, and a crash-consistent manifest. Milestone 2 databases migrate from the legacy `wal.log`.
4. **Complete.** Background sorted-stream compaction of the oldest SSTables, explicit lookup precedence in manifest version 2, and crash-safe input retirement. MiniDB still opens version 1 manifests.
5. **Complete.** Shareable handles, linearizable point operations, coordinated background flushing and compaction, seeded reference-model tests, and repeated process-kill recovery.
6. **Next, when explicitly requested.** Add Bloom filters and a bounded block cache. Measure throughput, latency percentiles, memory use, and relevant storage tradeoffs with reproducible benchmarks.

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
cargo +1.88.0 test --locked
```

Milestone 3 passed formatting and strict Clippy on Rust 1.100. All 35 reported tests passed on Rust 1.100 and Rust 1.88 on macOS, including process interruption at four flush publication boundaries and an ambiguous manifest rename error. Milestone 4 passed formatting and strict Clippy on Rust 1.100. All 42 reported tests passed on macOS on Rust 1.100 and 1.88. The new tests cover concurrent flush publication, in-flight reads, tombstone elimination, empty output, worker errors, shutdown, and process interruption at three compaction publication boundaries. Milestone 5 passed formatting and strict Clippy on Rust 1.100. All 51 reported tests passed on macOS on Rust 1.100 and 1.88. The new tests cover visibility at WAL and manifest boundaries, sustained reader and writer load, comparison with a seeded reference model, and 22 process kills across 11 WAL, flush, and compaction checkpoints. Report future test and benchmark results only after running them.

## Agent workflow

The coordinating agent follows this workflow for implementation work.

1. Explain the plan for the requested milestone or fix. Ask `minidb-implementer` to make the scoped changes and run relevant checks.
2. Once the implementer returns, ask `minidb-reviewer` to independently review the change. Supply the diff or changed paths, acceptance criteria, and check results. The reviewer suggests fixes without editing the implementation.
3. Stop when there are no actionable in-scope findings. Otherwise, send confirmed findings back to the implementer and ask the reviewer to check the resulting changes.

The coordinating agent tracks a maximum of **three runs per role per task**. The initial run counts, and so does every retry, resume, or follow-up assignment. Run the roles sequentially and count equivalent fallback agents against the same limit. If the limit leaves findings unresolved, report them to the user. The roles never delegate to other agents.

The roles are defined in `.codex/agents/`. If named profiles are unavailable in the current client, use the corresponding role instructions when delegating.
