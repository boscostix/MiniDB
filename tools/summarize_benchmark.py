"""Aggregate the tab-separated output from tools/benchmark.sh."""

import csv
import statistics
import sys
from collections import defaultdict

timings = defaultdict(list)
storage = defaultdict(list)
allocations = []
memory = []
with open(sys.argv[1], newline="", encoding="utf-8") as source:
    for row in csv.reader(
        (line for line in source if not line.startswith("#")), delimiter="\t"
    ):
        variant, kind, rows, *fields = row
        if kind == "sample":
            _, mode, workload, count, rate, p50, p95, p99, calls, requested = fields
            if mode == "timing":
                timings[rows, workload, variant].append(
                    tuple(map(float, (rate, p50, p95, p99)))
                )
            else:
                allocations.append(
                    (int(rows), workload, variant, int(calls) / int(count), int(requested) / int(count))
                )
        elif kind == "storage":
            _, workload, metric, value = fields
            storage[rows, workload, metric, variant].append(int(value))
        elif kind == "rss":
            memory.append((int(rows), variant, int(fields[0])))

print("Timing medians (ops/s followed by p50, p95, p99 in µs)")
for (rows, workload, variant), values in sorted(timings.items()):
    summary = " ".join(f"{statistics.median(column):.2f}" for column in zip(*values))
    print(rows, workload, variant, summary)
print("\nStorage medians")
for key, values in sorted(storage.items()):
    print(*key, statistics.median(values))
print("\nAllocator profile (calls/op and requested bytes/op)")
for rows, workload, variant, calls, requested in sorted(allocations):
    print(rows, workload, variant, f"{calls:.2f}", f"{requested:.2f}")
print("\nPeak RSS in bytes")
for record in sorted(memory):
    print(*record)
