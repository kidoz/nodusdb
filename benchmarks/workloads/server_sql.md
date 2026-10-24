# Server-level SQL workloads

Fixed datasets, release mode, and Criterion `iter_custom` batch timing. The
server runs in-process (`nodus_testkit`) with a real data directory and WAL;
clients drive it over real pgwire connections. Criterion reports batch latency,
throughput, and confidence intervals across 10 samples after warm-up.

## Workloads

1. **point_insert**: 50 single-row `INSERT` statements per iteration into an
   initially empty table.
2. **point_read**: 1,000 primary-key `SELECT` statements per iteration over a
   preloaded 10,000-row table.
3. **mixed_oltp**: 500 operations per iteration, 80% point reads and 20% unique
   point writes, distributed across eight independent client connections.
4. **bulk_insert**: 10,000 rows per iteration through `COPY FROM STDIN` in
   100-row batches.
5. **range_scan**: full ordered `SELECT` over the preloaded 10,000-row table.
6. **replicated_oltp**: 200 mixed operations per iteration on a three-node mTLS
   cluster, distributed across eight independent client connections spread over
   all members.

`NODUS_BENCH_SCALE=smoke` uses a 100-row dataset, proportionally fewer
operations, and skips the cluster variant. `NODUS_BENCH_OPS` overrides the
operation count for isolated diagnosis; do not use it for a baseline report.

## Recording

Hardware (CPU, cores, RAM), OS, toolchain (`rustc --version`), filesystem of
the data directory, run count, and confidence intervals go into the report
header. Reports live in `benchmarks/reports/`; generated Criterion output stays
under `target/criterion`.

## Budgets

The CI smoke job catches harness breakage and order-of-magnitude regressions;
it is not a stable microbenchmark on shared runners. The scheduled soak runs
the full matrix. Budgets derive from the recorded baseline, never invented.
