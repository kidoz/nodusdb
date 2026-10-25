# Server SQL baseline — 2026-10-24

## Scope and verdict

Full release-mode run of the six workloads in
[Server-level SQL workloads](../workloads/server_sql.md). The command completed
successfully in 6,580.7 seconds (109.7 minutes):

```bash
cargo bench --locked -p nodus_integration_tests --bench server_bench
```

This is a performance baseline, not a production-readiness approval. Single-row
and bulk write throughput is unsuitable for write-heavy production workloads.
The experimental warning remains in force.

## Environment

- Machine: MacBook Pro (Mac16,5), Apple M4 Max, 16 cores, 48 GiB RAM
- OS: macOS 27.0.1 (26A434), arm64
- Toolchain: rustc/cargo 1.99.0 (2026-09-28 / 2026-08-27)
- Build: Cargo `bench` profile, optimized; Criterion 2-second warm-up,
  5-second target measurement time, 10 samples per workload
- Server: in-process `nodus_testkit`, real pgwire and persistent temporary data
  directories on local journaled APFS (`/dev/disk3s1`, Data volume)
- Configuration: `NodusConfig::default()` with test admin credentials; one node
  except for the three-node mTLS replicated workload
- Source: commit `45106f9ffabe8f9fe58dd912004fdeaf6d30a69c` plus the reviewed
  working-tree changes. Pre-report diff SHA-256:
  `e6eafd4a881c36bebbefbec4db9512f2db9bbd88feda4e8eb76a684a4609ad1b`
- Run count: one complete run on an otherwise quiet machine. Criterion output is
  retained under `target/criterion/server_*`; the console log is
  `target/tmp/server-baseline-2026-10-24.log`.

## Results

Batch latency is the Criterion mean with its 95% confidence interval. Mean
throughput is operations per second; latency and throughput confidence bounds
run in opposite directions.

| Workload | Operations per iteration | Mean batch latency (95% CI) | Mean throughput | Mean time per operation |
| --- | ---: | ---: | ---: | ---: |
| point_insert | 50 | 1.624 s [1.597, 1.656] | 30.786/s | 32.48 ms |
| point_read | 1,000 | 348.04 ms [337.11, 358.79] | 2,873.2/s | 0.348 ms |
| mixed_oltp | 500 | 3.291 s [3.202, 3.395] | 151.91/s | 6.583 ms |
| bulk_insert | 10,000 | 244.02 s [225.41, 263.60] | 40.979 rows/s | 24.40 ms/row |
| range_scan | 10,000 | 457.35 ms [400.50, 515.01] | 21,865 rows/s | 45.74 µs/row |
| replicated_oltp | 200 | 2.318 s [2.164, 2.508] | 86.281/s | 11.59 ms |

Actual measurement iterations were 10 for point insert, 20 for point read, 10
for mixed OLTP, 10 for bulk insert, 20 for range scan, and 10 for replicated
OLTP. Criterion identified three outliers in the replicated samples (one low
severe, one low mild, one high severe); its wider interval and single-run status
matter when comparing future results.

## Interpretation

- Point reads no longer scale with table size through the extended-query
  Describe probe or an unnecessary primary-key bitmap-costing scan.
- Single-row writes still cost about 32 ms each. COPY improves that only to
  about 24 ms/row. There is no batched/group commit path; row and index intents
  use separate Raft submissions, and durable commits retain their fsync
  boundaries.
- Eight-client mixed OLTP reaches about 152 operations/s on one node and about
  86 operations/s on the three-node mTLS cluster. These figures do not establish
  a high-load OLTP budget.
- The long preload and bulk times are measured behavior, not a hung harness.

## Limits and follow-up

This capture does not include per-operation p50/p95/p99 latency, memory growth,
Raft-log growth, compaction, recovery time, or a sustained soak. It is one run
on one laptop, not a cross-machine guarantee or a production capacity plan.
Scheduled load testing needs repeated same-host runs and explicit budgets from
those runs. Write batching/group commit is the highest-priority performance
investigation; it must preserve WAL sync, MVCC visibility, and quorum
acknowledgement.
