#!/usr/bin/env bash
set -euo pipefail
# Live allocation tracking is reserved for a separate profiling build.
unset MINIDB_PROFILE_LIVE

# Run from any directory. Keep scratch and toolchain identical for both builds.
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
baseline=${MINIDB_BASELINE:-e96b5c0}
trials=${MINIDB_TRIALS:-3}
workload=${MINIDB_WORKLOAD:-run}
case "$workload" in
    run|compaction) ;;
    *) printf "MINIDB_WORKLOAD must be run or compaction\n" >&2; exit 2 ;;
esac
small_rows=${MINIDB_SMALL_ROWS:-1024}
large_rows=${MINIDB_LARGE_ROWS:-8192}
small_ops=${MINIDB_SMALL_OPS:-128}
large_ops=${MINIDB_LARGE_OPS:-256}
scratch=$(mktemp -d "${TMPDIR:-/tmp}/minidb-perf.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

mkdir -p "$scratch/baseline/examples" "$scratch/runs"
git -C "$repo" archive "$baseline" | tar -x -C "$scratch/baseline"
cp "$repo/examples/perf.rs" "$scratch/baseline/examples/perf.rs"
for variant in baseline optimized; do
    source_root="$scratch/baseline"
    if [[ "$variant" == optimized ]]; then source_root=$repo; fi
    CARGO_TARGET_DIR="$scratch/target-$variant" cargo build --manifest-path "$source_root/Cargo.toml" --release --locked --example perf >&2
done

printf '# baseline=%s optimized_worktree_base=%s trials=%s workload=%s\n' "$baseline" "$(git -C "$repo" rev-parse --short HEAD)" "$trials" "$workload"
worktree=clean
if ! git -C "$repo" diff HEAD --quiet; then worktree=dirty; fi
printf '# optimized_worktree=%s\n' "$worktree"
printf '# %s\n' "$(uname -smr)" "$(rustc -V)"
printf '# sample columns: rows trial mode workload operations ops/s p50_us p95_us p99_us allocations requested_bytes\n'
printf '# storage columns: rows trial workload metric value\n'
printf '# rss columns: rows peak_rss_bytes\n'
for size in small large; do
    rows=$small_rows
    operations=$small_ops
    if [[ "$size" == large ]]; then
        rows=$large_rows
        operations=$large_ops
    fi
    if [[ "$workload" == run ]]; then
        for variant in baseline optimized; do
            bin="$scratch/target-$variant/release/examples/perf"
            "$bin" fixture "$scratch/$variant-$size" "$rows"
        done
    fi
    for (( trial=1; trial<=trials; trial++ )); do
        order=(baseline optimized)
        if (( trial % 2 == 0 )); then order=(optimized baseline); fi
        for variant in "${order[@]}"; do
            bin="$scratch/target-$variant/release/examples/perf"
            "$bin" "$workload" "$scratch/$variant-$size" "$rows" "$operations" "$trial" "$scratch/runs" | sed "s/^/$variant	/"
        done
    done
    if [[ "$workload" == compaction ]]; then continue; fi
    for variant in baseline optimized; do
        bin="$scratch/target-$variant/release/examples/perf"
        "$bin" alloc "$scratch/$variant-$size" "$rows" "$operations" 1 "$scratch/runs" | sed "s/^/$variant	/"
        output="$scratch/time.out"
        case "$(uname -s)" in
            Darwin)
                /usr/bin/time -l "$bin" memory "$scratch/$variant-$size" "$rows" 2>"$output"
                bytes=$(awk '/maximum resident set size/ {print $1}' "$output")
                ;;
            Linux)
                /usr/bin/time -f '%M' "$bin" memory "$scratch/$variant-$size" "$rows" 2>"$output"
                bytes=$(awk '{printf "%.0f", $1 * 1024}' "$output")
                ;;
            *) continue ;;
        esac
        printf '%s\trss\t%s\t%s\n' "$variant" "$rows" "$bytes"
    done
done
