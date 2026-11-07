# Server Raft-log and storage growth measurements — 2026-11-07

## Scope and result

Three uninterrupted full runs of every [server SQL workload](../workloads/server_sql.md)
completed with write batching enabled. The capture contains 270 measured
Criterion samples and 336 schema-2 JSONL records including warm-ups. Legacy and
batched smoke captures contributed another 107 and 117 records. Every measured
Criterion sample matched its JSONL record, and all full and smoke records passed
schema, scale, finite-value, request-count, memory high-water, Raft-metric,
storage-node, and signed storage-delta validation.

COPY was the largest storage-growth case. Across the three runs it averaged
96.42 MiB net allocated growth per run, reached 120.49 MiB allocated storage per
node, and reached 56.88 MiB of retained serialized Raft payload per node. Net
allocated growth for COPY was 93.07, 90.72, and 105.48 MiB. The other workloads
remained below 5.78 MiB maximum observed retained Raft payload per node in this
capture.

Physical storage did not grow monotonically for every workload. Compaction and
other background maintenance produced negative net deltas for primary-key reads,
single-node mixed OLTP, ordered scans, and replicated mixed OLTP. The 32-client
insert workload ranged from a 0.73 MiB decrease to a 5.04 MiB increase. These
are short-run maintenance observations, not evidence of a leak or of bounded
long-term storage.

No successful Raft snapshot build was observed in any full-run record. The
capture therefore does not prove snapshot-driven Raft-log reclamation. General
production remains **NO-GO**.

## Measurement definitions

- `nodus_raft_log_serialized_bytes` is the exact serialized JSON payload size of
  currently retained Raft entries, summed across groups separately for each
  node. It excludes record framing, LSM/WAL overhead, snapshots, indexes, and
  filesystem allocation. The server exports it as a last-observed gauge from
  its two-second metrics loop.
- `logical_bytes` is the sum of regular-file lengths in the complete persistent
  data directory. It includes user storage, WAL, SSTables, manifests, snapshots,
  and other server files.
- `allocated_bytes` sums filesystem-reported allocated 512-byte blocks on this
  Unix host. Hard-linked files are counted once, symlinks are not followed, and
  directory metadata is excluded.
- Storage scans run immediately before and after each timed SQL section. Scan
  time is excluded from SQL elapsed time, but background maintenance can run
  during either scan. Scans do not capture physical-storage peaks between them.
- Net growth is the last post-iteration value minus the first pre-iteration
  value, including warm-ups and measured iterations. Replicated growth sums all
  three nodes; maxima in the tables are per node.
- Criterion batch times are normalized to one iteration. CV is sample standard
  deviation divided by the mean across 30 measured samples. Criterion's `change`
  output refers to earlier local baselines and is not used for claims here.

## Environment and method

- Machine: MacBook Pro (Mac16,5), Apple M4 Max, 16 cores, 48 GiB RAM.
- OS: macOS 27.2 (26B123), arm64; local journaled APFS.
- Toolchain: rustc 1.99.1 (58cf8f9c3 2026-10-13); cargo 1.99.1
  (5f94df478 2026-08-27).
- Build: Cargo `bench` profile, optimized and locked; Criterion 2-second warm-up,
  5-second target measurement time, and 10 samples per configuration and run.
- Server: in-process `nodus_testkit`, real pgwire and persistent temporary data
  directories. One node except for the three-node mTLS workload.
- Configuration: `NodusConfig::default()` with disposable test credentials and
  finalized levels 2–4 before table creation or preload. No other feature gate
  was enabled by the benchmark.
- Base source: `b1d39576e6efd1f6c86e8c8c722183e61a76dd17`, tree
  `77e322f5019ecb0379e10b0a0b32096500d3dd9f`, plus the preserved
  `implementation.patch` with SHA-256
  `9ca9afce6765ca5ed3dd6e67c1c8219eb8e6710d5e8046c004a69b9e4cffcbfc`.
  The benchmark used that working tree, not a clean commit.
- The pre-existing maintainer `.gitignore` modification was not a build input.
  Unrelated untracked files were left untouched.
- Capture: three sequential full runs completed in 582.051 seconds with no other
  build, test, implementation edit, or benchmark from this task running
  concurrently. Available
  filesystem space was 13 GiB near the start and 11 GiB after capture. This
  near-full local filesystem is not a supported-deployment recommendation.

Each full run used:

```bash
NODUS_BENCH_WRITE_BATCH=1 \
NODUS_BENCH_ARTIFACT_DIR=target/server-storage-growth-b1d3957/batched-run-N \
cargo bench --locked -p nodus_integration_tests --bench server_bench -- --noplot
```

`N` was 1, 2, or 3. Scale and operation-count overrides were unset. The legacy
and batched smoke runs used `NODUS_BENCH_SCALE=smoke`; the legacy smoke omitted
`NODUS_BENCH_WRITE_BATCH`.

## Throughput and timing observations

These are observations for the instrumented working tree, not a comparison
against the earlier source revision or an approved performance objective.

| Workload | Mean batch | CV | Throughput |
| --- | ---: | ---: | ---: |
| Single INSERT, 1 client | 1.281 s | 3.4% | 39.04 operations/s |
| Single INSERT, 8 clients | 559.96 ms | 7.2% | 89.29 operations/s |
| Single INSERT, 32 clients | 533.11 ms | 9.0% | 93.79 operations/s |
| Indexed INSERT, 8 clients | 577.62 ms | 7.0% | 86.56 operations/s |
| Primary-key read | 342.36 ms | 9.0% | 2,920.89 operations/s |
| Single-node mixed OLTP | 1.177 s | 5.7% | 424.87 operations/s |
| COPY bulk insert | 5.446 s | 7.3% | 1,836.08 rows/s |
| Ordered range scan | 479.22 ms | 35.3% | 20,867.13 rows/s |
| Three-node mixed OLTP | 1.199 s | 4.3% | 166.80 operations/s |

## Retained log and physical storage

| Workload | Max Raft payload / node | Max logical storage / node | Max allocated storage / node | Mean net logical / run | Mean net allocated / run | Net allocated runs 1/2/3 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single INSERT, 1 client | 0.557 MiB | 3.122 MiB | 3.145 MiB | 2.114 MiB | 2.118 MiB | 2.266 / 3.035 / 1.055 MiB |
| Single INSERT, 8 clients | 1.150 MiB | 5.454 MiB | 5.480 MiB | 2.696 MiB | 2.708 MiB | 1.566 / 3.434 / 3.125 MiB |
| Single INSERT, 32 clients | 1.784 MiB | 10.460 MiB | 10.520 MiB | 1.456 MiB | 1.461 MiB | 0.074 / 5.039 / -0.730 MiB |
| Indexed INSERT, 8 clients | 0.740 MiB | 4.730 MiB | 4.758 MiB | 2.696 MiB | 2.703 MiB | 1.953 / 3.352 / 2.805 MiB |
| Primary-key read | 4.816 MiB | 14.129 MiB | 14.156 MiB | -19.943 MiB | -19.952 MiB | -20.605 / -21.723 / -17.527 MiB |
| Single-node mixed OLTP | 5.771 MiB | 32.819 MiB | 32.852 MiB | -8.440 MiB | -8.432 MiB | -0.785 / -8.410 / -16.102 MiB |
| COPY bulk insert | 56.881 MiB | 120.011 MiB | 120.488 MiB | 96.272 MiB | 96.421 MiB | 93.066 / 90.715 / 105.480 MiB |
| Ordered range scan | 4.892 MiB | 7.141 MiB | 7.160 MiB | -10.756 MiB | -10.757 MiB | -13.719 / -15.730 / -2.820 MiB |
| Three-node mixed OLTP | 5.274 MiB | 32.250 MiB | 32.316 MiB | -29.183 MiB | -29.523 MiB | -48.719 / -18.566 / -21.285 MiB |

Retained Raft payload and whole-directory storage answer different questions.
Their values must not be divided to infer an encoding overhead or used as
substitutes for one another. Negative net storage deltas reflect the state at
the two endpoint scans and do not describe the path between them.

## Maintenance observations

Snapshot and compaction counts are sums of deltas over measured records. The
separate all-record check also found zero successful snapshot builds, including
warm-ups. Queue values are maxima of exported high-water gauges.

| Workload | Max retained Raft entries / node | Max commit queue | Max router queue | Snapshot builds | Compactions |
| --- | ---: | ---: | ---: | ---: | ---: |
| Single INSERT, 1 client | 1,312 | 1 | 1 | 0 | 12 |
| Single INSERT, 8 clients | 2,219 | 8 | 5 | 0 | 6 |
| Single INSERT, 32 clients | 3,182 | 26 | 5 | 0 | 6 |
| Indexed INSERT, 8 clients | 912 | 8 | 5 | 0 | 5 |
| Primary-key read | 314 | 1 | 1 | 0 | 5 |
| Single-node mixed OLTP | 1,746 | 8 | 3 | 0 | 12 |
| COPY bulk insert | 3,287 | 1 | 1 | 0 | 56 |
| Ordered range scan | 314 | 1 | 1 | 0 | 5 |
| Three-node mixed OLTP | 1,127 | 3 | 2 | 0 | 29 |

## Artifacts

Generated evidence is under `target/server-storage-growth-b1d3957/`:

- `batched-run-{1,2,3}/`: console logs, 112 JSONL records per run, and preserved
  Criterion raw samples and estimates;
- `smoke-legacy/` and `smoke-batched/`: 107 and 117 smoke records;
- `implementation.patch`: exact source changes used for the capture;
- `source.json`: source, toolchain, host, and configuration identity;
- `summary.json`: validated machine-readable statistics and per-run growth;
- `summarize.py` and `tables.md`: analysis helper and generated tables.

SHA-256 values:

- Run 1 JSONL: `8f742644c3410bb69f72e75fa547d347fff5a0694a9f74e6f0edf5bfa9590f50`
- Run 2 JSONL: `ca50ced542609422a984cdf47a2118f8707f35a08b5be4d81d8cb33d1225221f`
- Run 3 JSONL: `f7c4adbb249707069eedcfd13d5f76fec11a4c5f0b98d12a4e7931a1f7486150`
- Summary: `3f1adb0c7256d8bade76cfa76ed543b5322dd06393e4f20e02c37c2a82263c83`
- Analysis script: `e92795039440b81163042e7e7a5ae165b40c0ea64095d55dd9aaacb23d70dd4e`

Revalidate with:

```bash
python3 -B target/server-storage-growth-b1d3957/summarize.py
```

Expected output begins with `validated 270 measured samples across 3 full
batched runs and 336 total schema-2 records`.

## Limits and follow-up

- These are three short runs on one macOS host, not sustained-load, bounded-memory,
  bounded-storage, capacity, production-container, or cross-machine evidence.
- No snapshot build occurred. Sustained activity must trigger snapshots and show
  retained Raft entries and payload bytes falling after durable purge, together
  with physical-storage behavior and restart correctness.
- The two-second gauge can miss transient Raft-log peaks; endpoint directory
  scans can miss transient physical peaks. No maximum disk watermark is claimed.
- The capture did not include a current legacy full-run comparison. Its smoke
  legacy run validates harness behavior only and is not a performance baseline.
- Maintainer-approved latency, throughput, memory, log-growth, physical-storage,
  recovery, and availability budgets remain open. Scheduled sustained-load
  automation and final release-candidate qualification remain required.
