# MiniDB performance

## Reproduce the comparison

```sh
tools/benchmark.sh > minidb-performance.tsv
python3 tools/summarize_benchmark.py minidb-performance.tsv
```

The script compiles the same `examples/perf.rs` in release mode against the current worktree and Milestone 5 commit `e96b5c0` from `git archive`. It uses the same Rust toolchain, lockfile dependencies, scratch filesystem, workload, and starting data for both variants. The default comparison uses three timing trials per variant and size, alternating which binary goes first. An isolated allocation-counting trial and a separate peak-RSS process run once per size and variant. `MINIDB_BASELINE`, `MINIDB_TRIALS`, `MINIDB_SMALL_ROWS`, `MINIDB_LARGE_ROWS`, `MINIDB_SMALL_OPS`, and `MINIDB_LARGE_OPS` override the preset. Set `TMPDIR` to put the data on the target filesystem. On Linux, RSS collection uses GNU `/usr/bin/time`. On macOS, it uses `/usr/bin/time -l`.

| Fixture | Records | User value bytes | Operations per PUT or mixed trial | GETs per pass |
| --- | ---: | ---: | ---: | ---: |
| Small | 1,024 | 524,288 | 128 | 1,024 |
| Large | 8,192 | 4,194,304 | 256 | 2,048 |

All keys are 8 bytes and all values are 512 bytes. Each workload issues calls from one client thread, while MiniDB keeps its usual flush and compaction workers. Each successful PUT synchronizes its WAL record. Automatic flush is disabled for these workloads and all flushes are explicit. Per-operation latency uses `Instant` with nearest-rank percentiles. Tables report each rate or percentile as the median of its three per-trial values. The harness includes its own per-call timer and result checks.

- Sequential and random PUT start with an empty database and time WAL-backed mutations. Random PUT permutes distinct keys in a power-of-two range. File loading, fixture generation, and explicit flushes happen outside those timings.
- Random GET uses a seeded trace of existing keys and replays that trace for the warm pass. Process-cold starts with an empty MiniDB cache and can warm during the pass. The OS can keep file pages resident. Odd-numbered absent keys are interleaved with existing keys to exercise the Bloom filters. The absent pass runs after the warm hit pass.
- Mixed work starts with a copy of the one-table fixture and runs a deterministic 80% GET, 20% PUT sequence over existing keys. It times from the first request with a new handle. It includes subsequent MemTable hits. The read and write latencies share one distribution.
- The overwrite case writes every key in a 128- or 256-key working set three times, flushing after each pass. It reports logical bytes written for three SSTables, all WAL records and four WAL headers, and all four manifests, divided by the supplied key and value bytes across all three passes. It stays below the four-table compaction trigger. OS filesystem metadata and device write amplification are outside that count.
- The compaction case grows from three tables to four, then waits for retirement. It merges 128 or 256 total records by default, with 32 or 64 records per input. The corresponding `MINIDB_SMALL_OPS` or `MINIDB_LARGE_OPS` value sets the total, rounded down to a multiple of four. It reports the time from the triggering flush through retirement and the remaining wait after the flush returns. Both durations use the same retirement timestamp, captured before output. The post-flush figure reports the wait visible to the caller and can omit compaction that overlaps the flush return. Polling adds a small observation delay. Input bytes are the exact combined data-block extents, including block checksums. Output bytes are the exact SSTable file length. A one-table twin measures the fourth input extent, which can disappear before the client sees it. These are logical SSTable I/O bytes. File headers and indexes outside the data-read extent, the triggering flush, manifest I/O, and OS and device traffic are outside that estimate.
- Restart measures open and immediate close on a directory with one SSTable and 128 or 256 unsummarized WAL records. Each trial times one cycle. The timer includes worker startup and shutdown, recovery validation, and directory synchronization.
- Memory runs open the fixture and issue four random GETs per record before closing. The reported peak RSS includes the executable, allocator, and worker threads. The library cache charges block buffers and record arrays by capacity with an allowance for cache metadata. Pinning by in-flight readers, allocator overhead, MemTable and compaction state, SSTable indexes and filters, and caller-owned values are outside this charge.

## Measured baseline and optimized results

Measured on Apple M1 Pro with 8 logical CPUs, 16 GiB RAM, macOS 14.6.1 (23G93), an internal APFS SSD, and Rust 1.100.0. Timed passes ran with the optional allocation counters disabled. The optimized build includes SSTable v2 filters, a 4 MiB cache, a single payload-and-checksum read per uncached block, and binary search in verified records.

### 1,024 records

| Workload | Baseline ops/s | Optimized ops/s | Baseline p50 / p95 / p99 µs | Optimized p50 / p95 / p99 µs |
| --- | ---: | ---: | ---: | ---: |
| Sequential PUT | 247 | 237 | 4,001.12 / 5,115.04 / 7,463.50 | 4,035.08 / 5,918.96 / 7,151.04 |
| Random PUT | 242 | 222 | 4,010.58 / 5,291.75 / 7,176.33 | 4,034.00 / 6,086.17 / 7,936.88 |
| Random GET, process-cold | 50,368 | 228,452 | 16.46 / 27.21 / 32.62 | 0.71 / 25.00 / 28.79 |
| Random GET, warm | 70,662 | 1,390,124 | 13.17 / 16.29 / 18.08 | 0.62 / 0.83 / 0.96 |
| Random GET, absent | 83,948 | 3,999,359 | 11.58 / 13.00 / 14.83 | 0.17 / 0.25 / 0.50 |
| Mixed 80% GET, 20% PUT | 1,095 | 996 | 80.54 / 4,630.96 / 4,882.25 | 117.75 / 4,761.71 / 6,110.29 |
| Restart cycle | 752 | 480 | 1,329.58 / 1,329.58 / 1,329.58 | 2,083.54 / 2,083.54 / 2,083.54 |

### 8,192 records

| Workload | Baseline ops/s | Optimized ops/s | Baseline p50 / p95 / p99 µs | Optimized p50 / p95 / p99 µs |
| --- | ---: | ---: | ---: | ---: |
| Sequential PUT | 238 | 233 | 4,019.25 / 6,084.83 / 7,250.46 | 4,054.33 / 6,061.04 / 8,515.54 |
| Random PUT | 241 | 239 | 4,012.29 / 5,389.04 / 7,051.46 | 4,021.71 / 5,908.96 / 7,551.42 |
| Random GET, process-cold | 43,514 | 57,721 | 12.12 / 117.25 / 153.29 | 1.67 / 111.38 / 143.12 |
| Random GET, warm | 84,882 | 1,244,512 | 11.71 / 11.92 / 12.08 | 0.71 / 1.12 / 1.96 |
| Random GET, absent | 85,573 | 4,991,567 | 11.54 / 11.83 / 14.04 | 0.17 / 0.21 / 0.25 |
| Mixed 80% GET, 20% PUT | 1,067 | 1,006 | 177.58 / 4,437.96 / 5,107.42 | 189.54 / 4,634.67 / 5,218.21 |
| Restart cycle | 409 | 893 | 2,445.21 / 2,445.21 / 2,445.21 | 1,119.67 / 1,119.67 / 1,119.67 |

Warm hit throughput rose 19.7× and 14.7× at the two sizes. The absent pass bypassed most block reads. Process-cold throughput also rose and its p99 stayed above warm p99. Sequential and random PUT rates ranged from 222 to 247 ops/s under synchronous WAL writes. Mixed throughput was 9% lower on the small fixture and 6% lower on the large fixture. Sync latency and first-touch read costs both contribute to the mixed result.

### Maintenance and memory

| Metric | Small preset baseline | Small preset optimized | Large preset baseline | Large preset optimized |
| --- | ---: | ---: | ---: | ---: |
| Overwrite amplification | 2.058× | 2.061× | 2.057× | 2.060× |
| Compaction trigger to retirement | 48.78 ms | 52.58 ms | 48.36 ms | 51.01 ms |
| Compaction wait after flush | 21.89 ms | 21.33 ms | 22.24 ms | 22.55 ms |
| Compaction data bytes read | 67,792 | 67,792 | 135,584 | 135,584 |
| Compaction SSTable bytes written | 68,204 | 68,412 | 136,348 | 136,754 |
| Read-process peak RSS | 1.56 MiB | 2.30 MiB | 2.05 MiB | 12.66 MiB |

Columns label the benchmark presets. The compaction figures come from a separate three-trial comparison with alternating build order. The focused run skips read fixtures. Its TSV `rows` field still labels the preset with its configured read-fixture size. To reproduce just the compaction measurements, run:

```sh
MINIDB_WORKLOAD=compaction tools/benchmark.sh > minidb-compaction.tsv
python3 tools/summarize_benchmark.py minidb-compaction.tsv
```

Each measured compaction output grew by about 0.3%. The optimized build took 2.65–3.80 ms longer from the triggering flush to retirement, and median post-flush waits stayed within 0.6 ms of the baseline. These short trials include directory synchronization and polling. The overwrite setup measured the cost of WAL and table publication without compaction. Sustained overwrite amplification can be higher when the same older file gets rewritten by successive compactions. RSS rose with caching and the larger fixture. The cache byte budget limits resident *charged entries*. Process RSS includes all allocator and engine state.

### Allocation profile

Values are successful allocator calls and requested bytes per operation, from one instrumented pass per size and variant. Reallocation counts the new request. Freed bytes and live heap size use different metrics. Background engine threads are included if they allocate during the sampled window.

| Workload | 1,024 baseline | 1,024 optimized | 8,192 baseline | 8,192 optimized |
| --- | ---: | ---: | ---: | ---: |
| Random GET, process-cold | 2.01 / 4,212.76 B | 1.74 / 1,185.04 B | 2.00 / 4,215.17 B | 3.06 / 2,643.94 B |
| Random GET, warm | 2.00 / 4,212.42 B | 1.16 / 557.90 B | 2.00 / 4,215.00 B | 1.16 / 559.77 B |
| Random GET, absent | 1.00 / 3,700.42 B | 0.00 / 0.00 B | 1.00 / 3,703.00 B | 0.00 / 0.41 B |
| Mixed 80% GET, 20% PUT | 2.09 / 3,456.50 B | 3.85 / 3,175.98 B | 2.06 / 3,483.44 B | 4.35 / 3,758.73 B |

On the large fixture a cold pass requested fewer bytes per operation with the cache, but it made more allocation calls to construct and index verified blocks. Warm hits mostly allocate the returned value. Bloom negatives bypass block opening, loading, parsing, and cache lookup.

### Supplemental I/O and memory profiles

The larger profile used a 65,536-record single SSTable containing 32 MiB of user values. Each release-mode process issued 262,144 seeded GETs. Across three alternating trials, median peak RSS was 2.39 MiB on the baseline and 28.66 MiB on the optimized build. The 4 MiB charge bounds cached buffers and associated metadata. RSS includes index entries, Bloom filters, active allocations, and allocator-retained pages. The large fixture exceeds the cache budget and exercises eviction. The live-allocation build recorded peak requested heap bytes of 608,580 for the baseline and 5,335,239 for the optimized build over four passes. The optimized build peaked at 5,334,960 requested heap bytes over 16 passes (1,048,576 lookups). Those runs cover live allocations tracked by the Rust global allocator. The 16-pass process had a peak RSS of 28.83 MiB, which also includes resident pages outside its live requested heap.

The macOS I/O profiler sampled single-table read processes separately from the timing trials. It counted the calls made on SSTable descriptors, including the index reads during open, and the bytes returned by libc `read`.

| Records / random GETs | Build | Opens | Reads | Seeks | Bytes returned |
| --- | --- | ---: | ---: | ---: | ---: |
| 1,024 / 4,096 | Baseline | 4,097 | 8,197 | 4,098 | 15,176,268 |
| 1,024 / 4,096 | Optimized | 148 | 152 | 149 | 546,876 |
| 65,536 / 262,144 | Baseline | 262,145 | 524,315 | 262,146 | 971,933,944 |
| 65,536 / 262,144 | Optimized | 234,257 | 234,296 | 234,258 | 868,658,765 |

The small fixture fits in cache. The large random working set permits fewer cache hits, while reading the checksum with the block still saves calls. These are userspace counts and include reads served by the OS page cache. They do not measure device traffic.

To reproduce one profile on macOS, use an unused directory for the fixture and the freshly built local benchmark executable. Repeat with the same harness copied to the baseline source tree. Use 1,024 rows for the small profile. The measured larger fixture was prepared by replaying a generated WAL with the same bytes and flushing it once to avoid setup syncs. The normal fixture command produces the same SSTable.

```sh
scratch=$(mktemp -d)
cargo build --release --locked --example perf
clang -std=c11 -O2 -Wall -Wextra -Werror -dynamiclib tools/sstable-io-macos.c \
    -o "$scratch/sstable-io.dylib"
target/release/examples/perf fixture "$scratch/db" 65536
/usr/bin/time -l target/release/examples/perf memory "$scratch/db" 65536
DYLD_INSERT_LIBRARIES="$scratch/sstable-io.dylib" \
    target/release/examples/perf memory "$scratch/db" 65536
MINIDB_PROFILE_LIVE=1 cargo build --release --locked --example perf
target/release/examples/perf memory "$scratch/db" 65536 4
/usr/bin/time -l target/release/examples/perf memory "$scratch/db" 65536 16
rm -rf "$scratch"
```

## Group commit experiment

```sh
scratch=$(mktemp -d)
rustc -O --edition=2024 tools/group_sync.rs -o "$scratch/group-sync"
"$scratch/group-sync" "$scratch"
rm -rf "$scratch"
```

The same machine ran three counterbalanced trials with 4,096 synthetic 161-byte WAL frames per trial. Each frame uses four writes. One thread calls `File::sync_all` for each group and acknowledges its frames together after sync. Latency spans each frame's first write through its group's sync.

| Frames per sync | Median frames/s | p50 µs | p95 µs | p99 µs |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 243 | 4,007.25 | 5,912.75 | 8,277.33 |
| 8 | 1,838 | 3,904.33 | 6,195.42 | 9,701.96 |
| 32 | 6,113 | 4,627.46 | 7,065.17 | 9,352.08 |

Grouping amortized sync overhead in this saturated synthetic workload. Immediate full groups omit queue formation and idle delay. The experiment omits MiniDB checksums, MemTable publication, and flush coordination. A production group-commit design would need bounded wait and queue size, sync-before-visibility for the whole group, ordered application, and recovery tests for interrupted or failed groups. MiniDB uses one sync per mutation.

## Limits

These are local microbenchmarks on one filesystem and hardware configuration. Reads exercise warm OS pages and at most one SSTable in the fixture. Workloads use one client thread. Timing is sensitive to filesystem and host load, and the 128- and 256-operation write trials make tail estimates coarse. Physical device I/O and power-loss behavior require storage-specific measurement.
