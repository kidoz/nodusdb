# Repeated schema-2 server SQL baseline — 2026-10-29

## Scope and verdict

Three uninterrupted full runs of every [server SQL workload](../workloads/server_sql.md)
completed on one host with the legacy write path. The capture contains 270
measured Criterion samples and 318 schema-2 JSONL records including warm-ups.
This satisfies the repeated-baseline evidence requirement for this configuration;
it is not a production SLO, capacity guarantee, or general-production approval.

The largest variations are in single-client INSERT, range scan, and three-node
mixed OLTP. COPY averaged 215.564 seconds per 10,000-row iteration (46.39 rows/s)
with a 5.2% coefficient of variation. These measurements retain all outliers.

## Environment and method

- Machine: MacBook Pro (Mac16,5), Apple M4 Max, 16 cores, 48 GiB RAM.
- OS: macOS 27.0.1 (26A434), arm64.
- Toolchain: rustc 1.99.0 (b940084d7 2026-09-28); cargo 1.99.0
  (5f94df478 2026-08-27).
- Build: Cargo `bench` profile, optimized; Criterion 2-second warm-up,
  5-second target measurement time, and 10 samples per configuration and run.
- Server: in-process `nodus_testkit`, real pgwire and persistent temporary data
  directories on local journaled APFS (`/dev/disk3s1`, Data volume).
- Configuration: `NodusConfig::default()` with disposable test credentials and
  the legacy write path; one node except for the three-node mTLS workload.
- Source: `ba1da4087886346d1f6f1220989c7e1726583b67`, tree
  `0c50cae8b500b7ea3ae8a3a6aa2dc5934d006bfd`. The benchmark-source patch was
  empty (SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).
- Capture: 2026-10-28 20:36:07 UTC through 2026-10-29 01:03:30 UTC
  (16,043 seconds). No build, test, edit, or other benchmark ran concurrently
  from this task.

Each run used:

```bash
NODUS_BENCH_ARTIFACT_DIR=target/pr2-server-baseline-ba1da40/run-N \
cargo bench --locked -p nodus_integration_tests --bench server_bench -- --noplot
```

Scale, operation-count, and write-batching overrides were unset. Criterion used
two iterations per sample for point reads in all runs and for range scans in run
1; batch times were normalized to one iteration before pooling. Every Criterion
sample was matched to its JSONL record by configuration, iteration count, and
elapsed time. All records passed schema, scale, finite-value, latency-count, and
RSS/allocator high-water validation. Criterion's `change` output compared older,
incompatible local baselines and is intentionally not used here.

Pooled variance is sample variance across the 30 normalized batch times per
configuration. CV is sample standard deviation divided by the mean. Throughput
is operation count divided by mean batch time, not an average of per-sample
throughput. Whole-iteration p95/p99 use nearest-rank pooled batch times and are
distinct from request-latency percentiles below.

## Pooled measured results

| Workload | Mean batch | Variance (s²) | CV | Throughput | Batch p95 | Batch p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single INSERT, 1 client | 1.899 s | 4.8300e-01 | 36.6% | 26.33 operations/s | 3.770 s | 4.837 s |
| Single INSERT, 8 clients | 1.686 s | 1.3481e-02 | 6.9% | 29.65 operations/s | 2.006 s | 2.009 s |
| Single INSERT, 32 clients | 1.724 s | 1.7010e-01 | 23.9% | 29.00 operations/s | 2.109 s | 3.838 s |
| Indexed INSERT, 8 clients | 2.154 s | 2.3394e-01 | 22.5% | 23.22 operations/s | 2.283 s | 4.684 s |
| Primary-key read | 328.40 ms | 1.9705e-04 | 4.3% | 3045.11 operations/s | 350.82 ms | 351.49 ms |
| Single-node mixed OLTP | 3.387 s | 1.4400e-01 | 11.2% | 147.64 operations/s | 4.053 s | 5.174 s |
| COPY bulk insert | 215.564 s | 1.2519e+02 | 5.2% | 46.39 rows/s | 233.226 s | 237.193 s |
| Ordered range scan | 483.70 ms | 2.4192e-02 | 32.2% | 20673.86 rows/s | 702.10 ms | 754.54 ms |
| Three-node mixed OLTP | 2.521 s | 4.7723e-01 | 27.4% | 79.34 operations/s | 3.146 s | 5.815 s |

The operation counts per iteration are 50 for each point-insert configuration,
1,000 for point read, 500 for single-node mixed OLTP, 100 COPY batches and
10,000 rows for bulk insert, one scan and 10,000 rows for range scan, and 200
for replicated mixed OLTP.

## Per-run Criterion means and 95% confidence intervals

All values are seconds. Intervals are Criterion's per-run mean confidence
intervals; no additional pooled interval is inferred.

| Workload | Run 1 mean [95% CI] | Run 2 mean [95% CI] | Run 3 mean [95% CI] |
| --- | ---: | ---: | ---: |
| Single INSERT, 1 client | 1.65212 [1.61224, 1.69179] | 1.995 [1.67567, 2.44754] | 2.04919 [1.68872, 2.68909] |
| Single INSERT, 8 clients | 1.67096 [1.57357, 1.7901] | 1.64538 [1.62051, 1.66826] | 1.74218 [1.71624, 1.76425] |
| Single INSERT, 32 clients | 1.60177 [1.57095, 1.6324] | 1.6493 [1.62355, 1.68212] | 1.92209 [1.64521, 2.37748] |
| Indexed INSERT, 8 clients | 2.02988 [1.98244, 2.08135] | 2.35984 [2.06876, 2.89371] | 2.07146 [2.04628, 2.09923] |
| Primary-key read | 0.339562 [0.334479, 0.344676] | 0.323668 [0.318505, 0.329243] | 0.321956 [0.312428, 0.331509] |
| Single-node mixed OLTP | 3.27515 [3.23711, 3.31424] | 3.61282 [3.34841, 3.99731] | 3.27158 [3.18875, 3.36811] |
| COPY bulk insert | 203.775 [200.762, 207.034] | 216.852 [212.072, 220.947] | 226.064 [222.49, 229.746] |
| Ordered range scan | 0.490535 [0.404361, 0.576053] | 0.499464 [0.401099, 0.599565] | 0.461108 [0.36682, 0.55899] |
| Three-node mixed OLTP | 2.31342 [2.15954, 2.48992] | 2.44871 [2.3147, 2.62842] | 2.80013 [2.30923, 3.53335] |

## Request latency and resource observations

The mean request time is weighted by each record's request count. The p95
columns are the range of per-record p95 values, not a pooled percentile; the p99
column is the worst per-record p99. A bulk request is one COPY batch of up to
100 rows, and a range-scan request is one complete ordered SELECT.

Memory values are maxima of observed 100 ms high-water samples, including start
and end samples. RSS and allocator measurements include both the in-process
server and benchmark client; they are not server-only or native lifetime peaks.

| Workload | Mean request | Per-record p95 range | Worst p99 | Max RSS | Max allocator |
| --- | ---: | ---: | ---: | ---: | ---: |
| Single INSERT, 1 client | 37.97 ms | 34.08 ms–119.26 ms | 2.194 s | 0.038 GiB | 8.714 MiB |
| Single INSERT, 8 clients | 259.78 ms | 259.97 ms–453.99 ms | 454.13 ms | 0.071 GiB | 16.830 MiB |
| Single INSERT, 32 clients | 890.46 ms | 960.02 ms–2.550 s | 2.565 s | 0.082 GiB | 21.799 MiB |
| Indexed INSERT, 8 clients | 334.05 ms | 341.03 ms–2.050 s | 2.050 s | 0.083 GiB | 10.185 MiB |
| Primary-key read | 326.82 µs | 323.17 µs–405.38 µs | 478.71 µs | 0.206 GiB | 99.139 MiB |
| Single-node mixed OLTP | 53.17 ms | 248.55 ms–526.04 ms | 714.64 ms | 0.213 GiB | 83.086 MiB |
| COPY bulk insert | 2.156 s | 2.164 s–3.345 s | 4.823 s | 1.661 GiB | 566.458 MiB |
| Ordered range scan | 485.33 ms | 279.59 ms–754.46 ms | 754.46 ms | 0.578 GiB | 66.710 MiB |
| Three-node mixed OLTP | 97.19 ms | 404.09 ms–1.322 s | 2.148 s | 0.772 GiB | 120.862 MiB |

Across measured records, observed maxima were 28 for the Raft router queue
high-water and 4,194,350 bytes for the LSM memtable high-water. The largest
observed per-group Raft log contained 184,458 entries. The legacy path does not
use the bounded commit-batch queue, whose observed high-water remained zero.
These are short-workload observations, not bounded-memory or log-reclamation
proofs.

## Artifacts

Generated evidence is under `target/pr2-server-baseline-ba1da40/`:

- `run-1/`, `run-2/`, and `run-3/`: console log, 106-record
  `measurements.jsonl`, and preserved Criterion raw samples/estimates per run;
- `summary.json`: validated machine-readable pooled statistics;
- `summarize.py` and `tables.md`: analysis helper and generated tables.

JSONL SHA-256 values, in run order:

- `1452cac23bc92e52f16d1c775f1952d02f7cf9944b43dd76a70cd279a7617d54`
- `2515b6d3854abcd605ea2781be0ab4fd900117478185045b878b2e37789c25ab`
- `de655818eb4eef3c85c7042a1785b3b270cf739ace56fb36c8a7a14857d3f3a6`

## Limits and follow-up

These are three full runs on one macOS host, not cross-machine or GNU/Linux
allocator evidence. They cover the legacy write path, not a new write-batching
comparison. Short benchmark iterations do not prove bounded memory, queue, or
Raft-log behavior under sustained load. Maintainer-approved SLOs, scheduled
mixed-activity soaks, bounded-resource/reclamation evidence, and the final
24-hour release-candidate soak remain open. General production stays NO-GO.
