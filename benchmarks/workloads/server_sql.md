# Server-level SQL workloads

Fixed datasets, release mode, and Criterion `iter_custom` batch timing. The
server runs in-process (`nodus_testkit`) with a real data directory and WAL;
clients drive it over real pgwire connections. Criterion reports batch latency,
throughput, and confidence intervals across 10 samples after warm-up.

## Workloads

1. **point_insert**: 50 single-row `INSERT` statements per iteration into an
   initially empty table, split across 1, 8, or 32 independent clients. The
   operation count is the total across all clients, not a per-client count.
2. **indexed_point_insert**: 50 single-row inserts per iteration across eight
   clients into a table with a non-unique secondary index on `note`.
3. **point_read**: 1,000 primary-key `SELECT` statements per iteration over a
   preloaded 10,000-row table.
4. **mixed_oltp**: 500 operations per iteration, 80% point reads and 20% unique
   point writes, distributed across eight independent client connections.
5. **bulk_insert**: 10,000 rows per iteration through `COPY FROM STDIN` in
   100-row batches.
6. **range_scan**: full ordered `SELECT` over the preloaded 10,000-row table.
7. **replicated_oltp**: 200 mixed operations per iteration on a three-node mTLS
   cluster, distributed across eight independent client connections spread over
   all members.

`NODUS_BENCH_SCALE=smoke` uses a 100-row dataset, proportionally fewer
operations, and skips the cluster variant. `NODUS_BENCH_OPS` overrides the
operation count for isolated diagnosis; do not use it for a baseline report.

By default every server uses the legacy write path. Setting
`NODUS_BENCH_WRITE_BATCH=1` finalizes `snapshot-v2`, `migration-v3`, and
`write-batching-v4`, in that order, on every disposable server before its table
is created or preloaded. For each target the benchmark sends the admin start,
node-upgraded, and finalize requests and fails on an HTTP or response error. The
single-node workloads report node 1; the three-node workload reports every member
and verifies the finalized level-4 feature gate before measuring. This opt-in
applies to all workloads, including `replicated_oltp`.

## Recording

Hardware (CPU, cores, RAM), OS, toolchain (`rustc --version`), filesystem of
the data directory, run count, and confidence intervals go into the report
header. Reports live in `benchmarks/reports/`; generated Criterion output stays
under `target/criterion`.

Each `iter_custom` invocation also appends a schema-version-1 JSON object to
`target/server-bench/<run-id>/measurements.jsonl`. Set
`NODUS_BENCH_ARTIFACT_DIR` to choose a different output directory. The record
contains the workload and configuration, Criterion iteration count, request and
optional row counts, elapsed time and throughput, request-latency summary,
process RSS, and before/after/delta Prometheus samples for every server node.
The benchmark fails if it cannot collect required metrics or write the artifact.

Latency is measured around each awaited pgwire request and pooled across the
workload's clients. p50, p95, and p99 use nearest-rank samples after sorting;
the mean uses all request samples. One bulk-insert sample is one COPY batch of
up to 100 rows, not one row. One range-scan sample is one complete ordered
SELECT. These request percentiles are distinct from Criterion's
whole-iteration estimates.

RSS is sampled every 100 ms on Linux and macOS and reported as start, end, and
observed high-water bytes. Because the server runs in the benchmark process,
RSS includes both server and benchmark clients; it is not allocator-only or
server-only memory. Unsupported platforms and collection failures produce null
RSS fields rather than zero.

## Budgets

The CI smoke job catches harness breakage and provides coarse output for
regression review; shared runners do not enforce stable performance budgets.
Allocator/queue-specific high-water marks, compaction timings, recovery
budgets, scheduled mixed-activity soaks, and the final 24-hour soak remain PR2
work. Production budgets require maintainer approval and repeated supported-
profile evidence, never a number invented from one laptop run.
