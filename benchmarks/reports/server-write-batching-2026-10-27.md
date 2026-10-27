# Server write-batching comparison — 2026-10-27

## Scope and verdict

Three complete legacy and three complete write-batching runs of every
[server SQL workload](../workloads/server_sql.md) demonstrate a material
write-throughput improvement on the measured host. Pooled p95 and p99 batch
latencies improved for every write-heavy workload, including the three-node
workload. This is not a production-readiness approval or a capacity guarantee.

The read-only range-scan comparison remains noisy. Six runs per mode show a
3.9% slower batched mean, a slightly lower batched p95, and one extreme batched
sample that determines its higher p99. The evidence does not show a systematic
range-scan regression, but it also does not establish a clean p99 bound.

## Environment and method

- Machine: MacBook Pro (Mac16,5), Apple M4 Max, 16 cores, 48 GiB RAM.
- OS: macOS 27.0.1 (26A434), arm64.
- Toolchain: rustc 1.99.0 (b940084d7 2026-09-28); cargo 1.99.0
  (5f94df478 2026-08-27).
- Build: Cargo `bench` profile, optimized; Criterion 2-second warm-up,
  5-second target measurement time, 10 samples per workload and run.
- Server: in-process `nodus_testkit`, real pgwire and persistent temporary data
  directories on local journaled APFS (`/dev/disk3s1`, Data volume).
- Configuration: `NodusConfig::default()` with disposable test credentials; one
  node except for the three-node mTLS workload. Batched runs finalize
  `snapshot-v2`, `migration-v3`, and `write-batching-v4` on every disposable
  node before creating or preloading tables.
- Source: `a5d6eeb757e4a028dff2efefa42e38c41213dd0f`, containing the measured
  write-path and benchmark harness. Later commits through
  `31106615673ab8e49f87530c550e471f299cac97` changed unrelated
  interoperability tooling and documentation, not these Rust paths.
- Commands: `cargo bench --locked -p nodus_integration_tests --bench
  server_bench -- --noplot` for legacy runs, with
  `NODUS_BENCH_WRITE_BATCH=1` for batched runs. No other build or test command
  was run concurrently by this task.

Each full workload contributes 30 samples per mode. Range scan contributes 60
samples per mode after three additional runs per mode. The second legacy run
was resumed after interruption: its first six workloads were retained, then
COPY, range scan, and replicated OLTP completed in separate invocations. Its
log and raw samples confirm all nine workloads completed.

Pooled throughput below is the operation count divided by the arithmetic mean
of per-iteration batch times; it is not an average of per-sample throughput.
The coefficient of variation (CV) is sample standard deviation divided by the
mean. p95 and p99 use the nearest-rank sample from the pooled batch times.
These are whole-iteration percentiles, **not per-operation latency
percentiles**.

## Results

| Workload | Legacy mean (CV) | Batched mean (CV) | Mean throughput | Ratio | Legacy p95 → batched p95 | Legacy p99 → batched p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single INSERT, 1 client | 1.958 s (25.8%) | 1.325 s (3.7%) | 25.5 → 37.7/s | 1.48× | 2.711 → 1.389 s | 4.060 → 1.444 s |
| Single INSERT, 8 clients | 1.703 s (8.9%) | 581.36 ms (6.1%) | 29.4 → 86.0/s | 2.93× | 1.901 → 0.643 s | 2.202 → 0.648 s |
| Single INSERT, 32 clients | 1.718 s (5.8%) | 568.42 ms (8.1%) | 29.1 → 88.0/s | 3.02× | 1.878 → 0.633 s | 2.037 → 0.637 s |
| Indexed INSERT, 8 clients | 2.088 s (4.6%) | 598.17 ms (5.9%) | 23.9 → 83.6/s | 3.49× | 2.256 → 0.648 s | 2.432 → 0.664 s |
| Primary-key read | 344.18 ms (7.0%) | 324.66 ms (7.7%) | 2,905.4 → 3,080.1/s | 1.06× | 0.383 → 0.362 s | 0.385 → 0.392 s |
| Single-node mixed OLTP | 3.513 s (15.9%) | 1.196 s (5.0%) | 142.3 → 418.1/s | 2.94× | 3.762 → 1.328 s | 6.344 → 1.356 s |
| COPY bulk insert | 210.451 s (5.7%) | 5.535 s (12.7%) | 47.5 → 1,806.6 rows/s | 38.02× | 239.354 → 7.724 s | 244.141 → 7.774 s |
| Ordered range scan | 493.63 ms (33.3%) | 513.03 ms (37.5%) | 20,258.1 → 19,492.1 rows/s | 0.96× | 0.748 → 0.739 s | 0.874 → 1.353 s |
| Three-node mixed OLTP | 2.171 s (16.6%) | 1.281 s (5.8%) | 92.1 → 156.1/s | 1.69× | 2.728 → 1.391 s | 3.292 → 1.415 s |

The operation counts are 50 for each point-insert iteration, 1,000 for point
read, 500 for single-node mixed OLTP, 10,000 for COPY and range scan, and 200
for replicated mixed OLTP.

## Per-run means and Criterion confidence intervals

Each entry shows the Criterion mean followed by its 95% confidence interval in
seconds. Entries are ordered by run. Range scan has six entries per mode; the
other workloads have three.

| Workload | Legacy mean [95% CI] | Batched mean [95% CI] |
| --- | --- | --- |
| Single INSERT, 1 client | 1.848 [1.815, 1.881]; 2.392 [2.033, 2.837]; 1.633 [1.610, 1.652] | 1.290 [1.268, 1.313]; 1.370 [1.355, 1.390]; 1.316 [1.292, 1.339] |
| Single INSERT, 8 clients | 1.870 [1.803, 1.956]; 1.633 [1.595, 1.673]; 1.607 [1.565, 1.653] | 0.586 [0.562, 0.610]; 0.601 [0.585, 0.617]; 0.557 [0.543, 0.570] |
| Single INSERT, 32 clients | 1.785 [1.730, 1.854]; 1.684 [1.660, 1.709]; 1.684 [1.625, 1.748] | 0.573 [0.547, 0.599]; 0.578 [0.547, 0.606]; 0.554 [0.527, 0.579] |
| Indexed INSERT, 8 clients | 2.100 [2.060, 2.140]; 2.128 [2.063, 2.211]; 2.035 [2.001, 2.067] | 0.600 [0.582, 0.617]; 0.609 [0.587, 0.630]; 0.586 [0.564, 0.609] |
| Primary-key read | 0.339 [0.328, 0.350]; 0.329 [0.319, 0.339]; 0.365 [0.351, 0.376] | 0.324 [0.314, 0.334]; 0.310 [0.298, 0.321]; 0.340 [0.324, 0.357] |
| Single-node mixed OLTP | 3.763 [3.426, 4.361]; 3.529 [3.471, 3.582]; 3.247 [3.198, 3.298] | 1.176 [1.157, 1.196]; 1.216 [1.192, 1.246]; 1.195 [1.149, 1.249] |
| COPY bulk insert | 217.844 [209.211, 227.558]; 208.206 [203.708, 212.564]; 205.303 [200.894, 209.933] | 5.438 [5.171, 5.757]; 5.290 [5.165, 5.409]; 5.877 [5.328, 6.546] |
| Ordered range scan | 0.443 [0.363, 0.526]; 0.523 [0.441, 0.604]; 0.627 [0.515, 0.731]; 0.420 [0.333, 0.519]; 0.513 [0.424, 0.601]; 0.436 [0.350, 0.531] | 0.720 [0.546, 0.914]; 0.486 [0.408, 0.571]; 0.476 [0.396, 0.558]; 0.453 [0.383, 0.524]; 0.474 [0.403, 0.544]; 0.469 [0.389, 0.548] |
| Three-node mixed OLTP | 2.492 [2.406, 2.580]; 1.974 [1.901, 2.040]; 2.047 [1.846, 2.354] | 1.280 [1.244, 1.317]; 1.312 [1.273, 1.348]; 1.251 [1.202, 1.303] |

## Storage microbenchmark

One Criterion storage run included 32 transactions per durable commit input.
Sequential commits took 134.427 ms [132.851, 136.149] versus 4.941 ms
[4.866, 5.018] for one atomic commit batch, a 27.2× latency improvement. The
atomic intent batch took 6.470 ms [6.259, 6.702] versus 5.967 ms [5.845,
6.089] for sequential intents. Intent batching alone does not reduce the number
of local WAL records; the larger server gains come from fewer Raft commands
and shared durable commit boundaries.

## Interpretation and limits

- COPY improves from 47.5 to 1,806.6 rows/s, and eight-client indexed INSERT
  improves from 23.9 to 83.6 writes/s on this host.
- Single-client INSERT gains less than concurrent INSERT because independent
  transactions cannot coalesce without concurrency. The bounded one-millisecond
  queue wait is visible in that trade-off.
- Three-node mixed OLTP improves while retaining quorum-plus-apply
  acknowledgement and peer mTLS. The benchmark verifies level-4 activation on
  every member before measuring.
- Range-scan samples vary widely in both modes. Its single 1.353-second batched
  sample is retained rather than discarded; per-operation tail measurement and
  controlled soak testing remain necessary.
- These runs do not measure memory growth, queue high-water marks, Raft-log
  reclamation, compaction, recovery time, sustained load, or a 24-hour soak.
  No production SLO is inferred from one laptop.

Raw logs, samples, and estimates are under
`target/write-batch-qualification/benchmarks/`. Generated output is not source
qualification by itself; final release qualification must use one reviewed
clean commit and the complete correctness and compatibility gates.
