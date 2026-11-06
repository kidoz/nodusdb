# Server write-batching resource comparison — 2026-11-06

## Scope and verdict

Three uninterrupted full runs of every [server SQL workload](../workloads/server_sql.md)
completed with the legacy write path, followed by three equivalent runs with
write batching enabled. The capture contains 540 measured Criterion samples and
651 schema-2 JSONL records including warm-ups. Every Criterion sample was matched
to its JSONL record and passed schema, scale, finite-value, request-count,
row-count, memory high-water, and Criterion-mean validation.

Write batching materially improved every write-heavy workload in this capture.
COPY averaged 44.52 versus 1,818.66 rows/s (**40.85×**), eight-client indexed
INSERT 24.01 versus 81.64 writes/s (**3.40×**), and three-node mixed OLTP 84.51
versus 150.20 operations/s (**1.78×**). Primary-key reads averaged 4.0% lower
throughput with batching enabled, with higher variance; ordered scans differed
by 1.5% with high variance in both modes. These read results are noisy, not
evidence of a universal read improvement or regression.

These are short, same-host benchmark observations. They are not production
SLOs, capacity guarantees, bounded-memory proofs, sustained-soak results, or
general-production approval. General production remains **NO-GO**.

## Environment and method

- Machine: MacBook Pro (Mac16,5), Apple M4 Max, 16 cores, 48 GiB RAM.
- OS: macOS 27.2 (26B123), arm64.
- Toolchain: rustc 1.99.1 (58cf8f9c3 2026-10-13); cargo 1.99.1
  (5f94df478 2026-08-27).
- Build: Cargo `bench` profile, optimized and locked; Criterion 2-second warm-up,
  5-second target measurement time, and 10 samples per configuration and run.
- Server: in-process `nodus_testkit`, real pgwire and persistent temporary data
  directories on local journaled APFS (`/dev/disk3s1`, Data volume). The
  filesystem had 8.5 GiB available after capture; this near-full filesystem is
  part of the measured environment, not a supported-deployment recommendation.
- Configuration: `NodusConfig::default()` with disposable test credentials;
  one node except for the three-node mTLS workload. Batched runs finalize levels
  2–4 before table creation or preload. No other feature gate was enabled by the
  benchmark.
- Source: `bf4625a9c5129cdb753f3d00d73f9d35386e8f8d`, tree
  `5d9b7818303fad617063dbdfef619ad58814d451`. The only tracked modification
  was a pre-existing maintainer `.gitignore` edit, which was not a build input.
  Two unrelated untracked Markdown files were also left untouched.
- Capture: 2026-11-06 07:30:49 UTC through 12:07:44 UTC. The sequential command
  completed successfully in 16,617.4 seconds. No build, test, edit, or other
  benchmark ran concurrently from this task.
- Legacy runs took 3,968.806, 7,975.187, and 4,070.386 seconds respectively;
  batched runs took 201.906, 187.712, and 193.868 seconds. The second legacy run
  was substantially slower. All samples and outliers remain in the analysis.

Each full run used:

```bash
NODUS_BENCH_ARTIFACT_DIR=target/server-write-batching-resources-bf4625a/MODE-run-N \
cargo bench --locked -p nodus_integration_tests --bench server_bench -- --noplot
```

`MODE` was `legacy` or `batched`, and `N` was 1, 2, or 3. Batched runs additionally
set `NODUS_BENCH_WRITE_BATCH=1`. Scale and operation-count overrides were unset.
Criterion's `change` output compared earlier local baselines and is intentionally
not used for this comparison.

Batch times were normalized to one iteration before pooling. Pooled variance is
sample variance across the 30 normalized batch times per configuration and mode.
CV is sample standard deviation divided by the mean. Throughput is operation or
row count divided by mean batch time, not an average of per-sample throughput.
Whole-iteration p95/p99 use nearest-rank pooled batch times and are distinct
from the request-latency summaries below.

## Throughput comparison

| Workload | Legacy | Batched | Ratio |
| --- | ---: | ---: | ---: |
| Single INSERT, 1 client | 28.05 operations/s | 37.77 operations/s | 1.35× |
| Single INSERT, 8 clients | 30.37 operations/s | 84.67 operations/s | 2.79× |
| Single INSERT, 32 clients | 28.46 operations/s | 87.56 operations/s | 3.08× |
| Indexed INSERT, 8 clients | 24.01 operations/s | 81.64 operations/s | 3.40× |
| Primary-key read | 3,171.34 operations/s | 3,044.27 operations/s | 0.96× |
| Single-node mixed OLTP | 140.27 operations/s | 388.95 operations/s | 2.77× |
| COPY bulk insert | 44.52 rows/s | 1,818.66 rows/s | 40.85× |
| Ordered range scan | 20,044.79 rows/s | 20,340.13 rows/s | 1.01× |
| Three-node mixed OLTP | 84.51 operations/s | 150.20 operations/s | 1.78× |

The operation counts per iteration are 50 for each point-insert configuration,
1,000 for point read, 500 for single-node mixed OLTP, 100 COPY batches and
10,000 rows for bulk insert, one scan and 10,000 rows for range scan, and 200
for replicated mixed OLTP.

## Pooled batch results

| Workload | Legacy mean / CV / p95 / p99 | Batched mean / CV / p95 / p99 |
| --- | ---: | ---: |
| Single INSERT, 1 client | 1.783 s / 26.7% / 2.571 s / 4.069 s | 1.324 s / 7.8% / 1.520 s / 1.605 s |
| Single INSERT, 8 clients | 1.646 s / 4.7% / 1.797 s / 1.826 s | 590.53 ms / 9.4% / 691.46 ms / 766.68 ms |
| Single INSERT, 32 clients | 1.757 s / 25.3% / 1.919 s / 4.029 s | 571.04 ms / 21.2% / 756.58 ms / 1.115 s |
| Indexed INSERT, 8 clients | 2.083 s / 3.7% / 2.221 s / 2.298 s | 612.43 ms / 11.9% / 792.79 ms / 842.25 ms |
| Primary-key read | 315.32 ms / 4.7% / 337.18 ms / 341.76 ms | 328.49 ms / 13.5% / 444.12 ms / 470.27 ms |
| Single-node mixed OLTP | 3.565 s / 19.5% / 5.156 s / 6.503 s | 1.286 s / 10.4% / 1.553 s / 1.684 s |
| COPY bulk insert | 224.594 s / 3.6% / 233.324 s / 243.466 s | 5.499 s / 8.7% / 6.453 s / 6.465 s |
| Ordered range scan | 498.88 ms / 31.6% / 713.58 ms / 777.08 ms | 491.64 ms / 32.8% / 725.29 ms / 740.96 ms |
| Three-node mixed OLTP | 2.367 s / 10.1% / 2.744 s / 3.189 s | 1.332 s / 18.9% / 2.154 s / 2.179 s |

Sample variances in seconds squared, in the same workload order, were
`2.2605e-01`, `6.0712e-03`, `1.9826e-01`, `6.0940e-03`, `2.2333e-04`,
`4.8252e-01`, `6.4580e+01`, `2.4818e-02`, and `5.7116e-02` for legacy;
and `1.0550e-02`, `3.0725e-03`, `1.4639e-02`, `5.2996e-03`, `1.9770e-03`,
`1.7758e-02`, `2.2627e-01`, `2.5968e-02`, and `6.3492e-02` for batched.

## Per-run Criterion means and 95% confidence intervals

All values are seconds. Intervals are Criterion's per-run mean confidence
intervals; no additional pooled interval is inferred.

| Workload | Mode | Run 1 mean [95% CI] | Run 2 mean [95% CI] | Run 3 mean [95% CI] |
| --- | --- | ---: | ---: | ---: |
| Single INSERT, 1 client | Legacy | 1.63991 [1.59573, 1.70297] | 2.01652 [1.64826, 2.54451] | 1.69132 [1.66502, 1.71506] |
| Single INSERT, 1 client | Batched | 1.43101 [1.38488, 1.48404] | 1.24582 [1.20986, 1.28683] | 1.2945 [1.27108, 1.32233] |
| Single INSERT, 8 clients | Legacy | 1.60909 [1.58323, 1.63314] | 1.70374 [1.65097, 1.75309] | 1.62604 [1.58744, 1.66587] |
| Single INSERT, 8 clients | Batched | 0.626112 [0.600945, 0.652186] | 0.579975 [0.546857, 0.627058] | 0.565503 [0.550294, 0.581841] |
| Single INSERT, 32 clients | Legacy | 1.61022 [1.57832, 1.64239] | 1.88277 [1.58314, 2.38537] | 1.77827 [1.72728, 1.83191] |
| Single INSERT, 32 clients | Batched | 0.652203 [0.559609, 0.774123] | 0.53241 [0.515004, 0.548124] | 0.528511 [0.512621, 0.543162] |
| Indexed INSERT, 8 clients | Legacy | 2.12084 [2.06764, 2.17989] | 2.03721 [2.00651, 2.07532] | 2.08973 [2.05825, 2.12269] |
| Indexed INSERT, 8 clients | Batched | 0.670753 [0.620065, 0.726809] | 0.580488 [0.558783, 0.602593] | 0.586062 [0.563185, 0.610005] |
| Primary-key read | Legacy | 0.314362 [0.304778, 0.323788] | 0.313336 [0.305663, 0.320858] | 0.318275 [0.308393, 0.327909] |
| Primary-key read | Batched | 0.343602 [0.310641, 0.383697] | 0.339966 [0.325169, 0.356265] | 0.301888 [0.289033, 0.315627] |
| Single-node mixed OLTP | Legacy | 3.7275 [3.33963, 4.38211] | 3.52462 [3.17227, 3.98913] | 3.44141 [3.34089, 3.53428] |
| Single-node mixed OLTP | Batched | 1.30117 [1.22464, 1.38319] | 1.29053 [1.19898, 1.4015] | 1.26486 [1.21586, 1.31839] |
| COPY bulk insert | Legacy | 221.035 [215.534, 226.399] | 228.208 [225.643, 230.663] | 224.539 [219.875, 230.053] |
| COPY bulk insert | Batched | 5.96435 [5.67124, 6.22492] | 5.17215 [5.02211, 5.31431] | 5.35914 [5.20784, 5.49287] |
| Ordered range scan | Legacy | 0.516631 [0.412757, 0.624577] | 0.476997 [0.379696, 0.578005] | 0.503021 [0.425795, 0.580128] |
| Ordered range scan | Batched | 0.51407 [0.426612, 0.602633] | 0.492046 [0.392514, 0.594474] | 0.468801 [0.369128, 0.571947] |
| Three-node mixed OLTP | Legacy | 2.49013 [2.34124, 2.67813] | 2.246 [2.15342, 2.33584] | 2.36347 [2.24846, 2.48818] |
| Three-node mixed OLTP | Batched | 1.52894 [1.34503, 1.75536] | 1.18227 [1.14483, 1.21675] | 1.28356 [1.23335, 1.33474] |

## Request latency observations

The mean request time is weighted by each record's request count. The p50 and
p95 columns are ranges of per-record values, not pooled request percentiles;
raw request samples are not retained for an exact pooled percentile. The p99
column is the worst per-record p99. One bulk request is one COPY batch of up to
100 rows, and one range-scan request is one complete ordered SELECT.

| Workload | Mode | Mean request | Per-record p50 range | Per-record p95 range | Worst p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| Single INSERT, 1 client | Legacy | 35.65 ms | 29.06–48.12 ms | 34.92–149.12 ms | 1.166 s |
| Single INSERT, 1 client | Batched | 26.47 ms | 22.82–29.12 ms | 24.00–49.84 ms | 78.47 ms |
| Single INSERT, 8 clients | Legacy | 255.18 ms | 233.38–272.18 ms | 265.46–383.75 ms | 383.82 ms |
| Single INSERT, 8 clients | Batched | 90.14 ms | 80.14–105.16 ms | 83.90–151.15 ms | 151.19 ms |
| Single INSERT, 32 clients | Legacy | 897.17 ms | 825.96 ms–1.398 s | 938.18 ms–2.759 s | 2.805 s |
| Single INSERT, 32 clients | Batched | 296.34 ms | 277.11–848.19 ms | 278.53–848.44 ms | 934.91 ms |
| Indexed INSERT, 8 clients | Legacy | 322.68 ms | 303.86–347.08 ms | 323.84–510.79 ms | 511.13 ms |
| Indexed INSERT, 8 clients | Batched | 93.28 ms | 80.98–126.12 ms | 84.47–227.36 ms | 227.36 ms |
| Primary-key read | Legacy | 313.82 µs | 284.08–326.96 µs | 308.17–376.29 µs | 440.92 µs |
| Primary-key read | Batched | 326.81 µs | 260.21–340.46 µs | 307.58 µs–1.13 ms | 2.81 ms |
| Single-node mixed OLTP | Legacy | 56.02 ms | 410.42 µs–1.42 ms | 254.58–459.39 ms | 2.453 s |
| Single-node mixed OLTP | Batched | 20.02 ms | 695.67 µs–1.96 ms | 81.64–137.79 ms | 181.18 ms |
| COPY bulk insert | Legacy | 2.246 s | 1.941–2.280 s | 2.432–3.541 s | 6.379 s |
| COPY bulk insert | Batched | 54.97 ms | 45.08–62.68 ms | 64.80–93.00 ms | 118.87 ms |
| Ordered range scan | Legacy | 499.84 ms | 277.80–777.00 ms | 277.80–777.00 ms | 777.00 ms |
| Ordered range scan | Batched | 491.56 ms | 273.49–740.88 ms | 273.49–740.88 ms | 740.88 ms |
| Three-node mixed OLTP | Legacy | 89.93 ms | 515.33 µs–1.32 ms | 408.28–574.66 ms | 1.147 s |
| Three-node mixed OLTP | Batched | 50.24 ms | 479.00 µs–4.91 ms | 217.64–458.35 ms | 556.28 ms |

The write-heavy request tails improved in this capture. Primary-key reads had a
higher worst per-record p99 with batching, and the ordered-scan distributions
remained noisy. These observations do not establish an approved tail-latency
budget.

## Memory, queue, log, and maintenance observations

Memory values are maxima of observed 100 ms high-water samples, including start
and end samples. RSS and allocator measurements include both the in-process
server and benchmark client; they are not server-only or native lifetime peaks.
Queue and memtable high-water gauges are monotonic within each server process.
Snapshot and compaction values are sums of per-record deltas, so the much longer
legacy runs naturally provide more maintenance opportunities.

| Workload | Mode | Max RSS | Max allocator | Max commit queue | Max router queue | Max retained Raft entries |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Single INSERT, 1 client | Legacy | 0.038 GiB | 8.788 MiB | 0 | 1 | 1,906 |
| Single INSERT, 1 client | Batched | 0.041 GiB | 6.153 MiB | 1 | 1 | 1,293 |
| Single INSERT, 8 clients | Legacy | 0.068 GiB | 16.557 MiB | 0 | 8 | 3,835 |
| Single INSERT, 8 clients | Batched | 0.060 GiB | 12.893 MiB | 8 | 5 | 2,257 |
| Single INSERT, 32 clients | Legacy | 0.080 GiB | 21.682 MiB | 0 | 31 | 5,798 |
| Single INSERT, 32 clients | Batched | 0.080 GiB | 18.132 MiB | 28 | 9 | 3,208 |
| Indexed INSERT, 8 clients | Legacy | 0.079 GiB | 10.077 MiB | 0 | 8 | 2,495 |
| Indexed INSERT, 8 clients | Batched | 0.078 GiB | 6.518 MiB | 8 | 5 | 883 |
| Primary-key read | Legacy | 0.207 GiB | 93.638 MiB | 0 | 1 | 20,105 |
| Primary-key read | Batched | 0.138 GiB | 50.305 MiB | 1 | 1 | 314 |
| Single-node mixed OLTP | Legacy | 0.212 GiB | 82.946 MiB | 0 | 5 | 23,367 |
| Single-node mixed OLTP | Batched | 0.178 GiB | 50.091 MiB | 7 | 3 | 1,809 |
| COPY bulk insert | Legacy | 2.280 GiB | 620.770 MiB | 0 | 1 | 200,808 |
| COPY bulk insert | Batched | 0.757 GiB | 309.270 MiB | 1 | 1 | 3,244 |
| Ordered range scan | Legacy | 0.631 GiB | 64.883 MiB | 0 | 1 | 20,105 |
| Ordered range scan | Batched | 0.773 GiB | 30.393 MiB | 1 | 1 | 314 |
| Three-node mixed OLTP | Legacy | 0.597 GiB | 105.415 MiB | 0 | 3 | 21,406 |
| Three-node mixed OLTP | Batched | 0.518 GiB | 61.354 MiB | 3 | 2 | 1,101 |

For COPY, maximum approximate memtable high-water was 4.000 MiB legacy and
2.063 MiB batched. The legacy COPY records observed four successful snapshot
builds and 1,736 successful compactions; batched COPY observed zero snapshot
builds and 54 successful compactions. Legacy replicated OLTP observed one
snapshot build. These are activity observations, not normalized maintenance
rates or proof of reclamation under sustained load.

Retained Raft-log **entry counts** were captured. Retained-log **payload or
on-disk bytes were not instrumented at this source revision**, so this report
makes no byte-growth claim and does not infer bytes from row sizes.

## Artifacts

Generated evidence is under `target/server-write-batching-resources-bf4625a/`:

- `legacy-run-{1,2,3}/` and `batched-run-{1,2,3}/`: console logs, JSONL records,
  and preserved Criterion raw samples/estimates;
- `smoke-legacy/` and `smoke-batched/`: successful smoke-capture records;
- `summary.json`: validated machine-readable statistics and comparison ratios;
- `summarize.py` and `tables.md`: analysis helper and generated tables.

JSONL SHA-256 values:

- Legacy run 1: `3884d0b8492e3bc38ef13ce2826a90b5b5627dce596cc562f1e6195f1ecb7dfc`
- Legacy run 2: `661906d0439a4d29b834614669063dff3cfcc3c0657c7fef0a9bc16ab1cfafec`
- Legacy run 3: `332a1e1065f7f8a9f037f710a423758ced3441c9ab202779e67b070c658a5629`
- Batched run 1: `10ef30c91075f217827ab64356801a3a2626e7b322c4e50e413b4615952082ce`
- Batched run 2: `92b3e00ef1c3c27f7e7cb6fa273a12401d3754abef17be637c6ee6bdad5d9fbb`
- Batched run 3: `dda9b2426cb85c67283225288cacc9d2d30cab1425ed1192cfbd390762fbe3b8`

## Limits and follow-up

- No sustained-load or 24-hour soak was run. Short high-water samples cannot
  prove bounded memory, queues, or log reclamation.
- No new recovery-time, restore RPO/RTO, crash, or power-loss measurement was
  performed in this capture. Existing restore-drill instrumentation and older
  mixed-activity results do not become current-source evidence by association.
- Retained-log bytes require explicit instrumentation before byte-growth budgets
  can be evaluated.
- These are three full runs per mode on one macOS host, not GNU/Linux allocator,
  cross-machine, production-container, or supported-deployment evidence.
- Maintainer-approved latency, throughput, memory, log-growth, recovery, and
  availability budgets remain open. Scheduled full-load and sustained-soak
  automation also remains required before a production claim.
