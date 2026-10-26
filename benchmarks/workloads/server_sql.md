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

## Budgets

The CI smoke job catches harness breakage and order-of-magnitude regressions;
it is not a stable microbenchmark on shared runners. The scheduled soak runs
the full matrix. Budgets derive from the recorded baseline, never invented.
