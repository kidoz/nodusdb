# Server-level workload plan (Phase 7)

Fixed datasets, release mode, Criterion `iter_custom` batch timing. The server
runs in-process (nodus_testkit) with a real data directory and WAL; clients
drive it over the real pgwire protocol. Report throughput (ops/s) plus
per-batch latency; Criterion reports p50/p95/p99 across repetitions.

## Workloads

1. **point_insert**: single-row `INSERT` in a loop, fixed 10k-row target table.
2. **point_read**: primary-key `SELECT` over the preloaded 10k-row table.
3. **mixed_oltp**: 80% point reads / 20% point writes, 8 concurrent clients.
4. **bulk_insert**: multi-row `INSERT` batches, 10k+ rows per measurement.
5. **range_scan**: full ordered `SELECT` over the 10k-row table.
6. **replicated_oltp** (three-node mTLS cluster): mixed OLTP against the seed,
   with reads sampled from every member — the quorum-acknowledgement cost is
   the point.

## Recording

Hardware (CPU, cores, RAM), OS, toolchain (`rustc --version`), filesystem of
the data directory, run count and variance go into the report header. Reports
live in `benchmarks/reports/`; generated Criterion output stays under
`target/criterion`.

## Budgets

The CI smoke job runs each workload at 1/100 scale with a sanity floor (it
exists to catch harness breakage and order-of-magnitude regressions, not to
micro-detect drift); the scheduled soak runs the full matrix. Budgets derive
from the recorded baseline, never invented.
