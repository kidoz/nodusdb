# Resource budget proposal — 2026-11-08

## Status

**DRAFT — UNAPPROVED.** No latency, throughput, memory, storage, recovery, or
alerting budget is approved by this document. General production remains
**NO-GO**. The local measurements below are evidence for maintainer review, not
production SLOs, a supported deployment profile, or a release-candidate verdict.

## Evidence source and environment

The sustained mixed-activity drill and independent validator passed. Exact rows
and secondary-index checks succeeded on every node, both snapshot/purge cycles
reclaimed Raft entries and payload, retired namespaces were empty on every node,
and quiet-window router and commit queues were zero.

- Source: `d9d8344aa0a0d58ff6b005bf30666074ff802abb`, with unrelated existing
  working-tree files; this is not a clean-checkout qualification.
- Build: optimized and locked Cargo `release` test build.
- Host: MacBook Pro, Apple M4 Max, 16 cores, 48 GiB RAM, arm64.
- OS/filesystem: macOS 27.2 (26B123), local journaled APFS.
- Available space: about 27 GiB when the environment was recorded. This
  near-full local filesystem is not a production deployment recommendation.
- Deployment: three in-process nodes, persistent LSM, real pgwire, dedicated
  Raft mTLS listeners, filesystem backup repository, legacy write path.
- Workload: four writers and one reader targeting the seed; 6,000 initial
  inserts plus a requested 60-second sustained continuation.
- Full duration: 2,435.764 seconds. Sustained continuation: 61.227 seconds.

Command:

```bash
NODUS_SOAK_SUSTAIN_SECONDS=60 \
NODUS_SOAK_ARTIFACT_DIR=target/sustained-load-2026-11-08/run-4 \
cargo test --release --locked -p nodus_integration_tests \
  --test mixed_activity_soak -- --ignored --nocapture
```

## Measured observations

### Workload and latency

| Measurement | Observed value |
| --- | ---: |
| Sustained acknowledged inserts | 156 |
| Sustained elapsed time | 61.227 seconds |
| Sustained acknowledged throughput | 2.548 inserts/s |
| Total acknowledged rows validated on every node | 6,156 |
| Write attempts / retries / recovered uncertain outcomes | 6,174 / 36 / 18 |
| Successful reads / read retries | 269,021 / 85 |
| Write p50 / p95 / p99 / max | 1.195 / 1.801 / 3.096 / 5.194 seconds |
| Read p50 / p95 / p99 / max | 0.801 / 1.752 / 16.173 / 2,958.327 ms |

The sustained window yielded 17 usable storage sample sets per node rather than
60. Slow directory scans are part of the measurement limitation. Throughput and
latency are whole-drill observations unless explicitly identified as sustained;
they are not steady-state capacity claims.

### Process and storage resources

- RSS high water: 1,102,184,448 bytes; ending RSS: 1,086,210,048 bytes.
- Allocator high water: 1,081,993,744 bytes; ending allocator use:
  18,070,656 bytes.
- Sustained allocated-storage changes, immediate before/after:
  - Node 1: 392,019,968 to 399,933,440 bytes, +7,913,472.
  - Node 2: 389,308,416 to 396,976,128 bytes, +7,667,712.
  - Node 3: 319,397,888 to 328,192,000 bytes, +8,794,112.
- Retired-source allocated-byte reclamation:
  - Node 1: 3,448,832 bytes.
  - Node 2: 13,549,568 bytes.
  - Node 3: 36,352,000 bytes.
- Transport logs: 1,132 connection-refused errors and 17 unknown-group 404s;
  every case used the unavailability backoff. There were zero JSON decode errors.

Storage growth during the sustained window is recorded, not judged bounded or
unbounded. One short window cannot establish a long-term storage slope.

## Correctness checks completed

- Exact `(id, value)` multiset on every node, with no missing, unexpected,
  duplicate, or value-mismatched rows.
- One expected secondary-index row and complete index-entry counts on every node.
- Applied-index convergence before comparisons.
- Snapshot-required follower restart and catch-up.
- Two real snapshot/purge cycles with entry and serialized-byte reclamation.
- Roster-wide retired-group removal and direct empty-namespace checks.
- No allocated-storage increase after the teardown baseline, with a measured
  reduction on at least one node.
- Zero quiet-window router and commit queue depth.
- Independent validation of 997 events, 987 storage sample sets, and 17 sustained
  samples per node.

## Decisions required from maintainers

The following values remain **undecided**. No default or recommendation is
implied by the observations above.

| Decision | Required approval |
| --- | --- |
| Supported production deployment profile | Hardware, OS, filesystem, node count, network, repository, and enabled write protocol |
| Throughput floors | Sustained single-node and replicated read/write workloads |
| Latency ceilings | p50, p95, p99, and maximum tolerated outliers for each supported operation |
| Memory ceilings | RSS and allocator high-water and post-activity stabilization |
| Queue limits | Router, commit, replication, migration, backup, and retry queues during activity and recovery |
| Storage limits | Total allocated bytes, log/WAL growth, snapshot checkpoint size, and permitted growth per hour |
| Recovery objectives | RPO for each failure model and RTO for full, incremental, PITR, node, and distributed recovery |
| Operational alerting | Error-rate, retry-rate, unavailable-peer duration, snapshot deferral, and storage-pressure thresholds |
| Long-run acceptance | Statistical trend and stabilization criteria for the sustained and 24-hour runs |

A separate filesystem restore drill previously measured restore-to-ready times
of 324, 644, and 1,456 ms and restart-inclusive times of 543, 820, and 1,668 ms.
Those values came from a different source revision and a small clean-shutdown
fixture. They do not approve an RTO and do not establish crash, S3, or
distributed recovery behavior. See `restore-recovery-2026-11-07.md`.

## Proposed acceptance method after thresholds are approved

1. Freeze the exact production deployment profile and numeric thresholds.
2. Run at least three consecutive full measurements on that profile, retaining
   failures and reporting means, variance, and tail observations.
3. Require exact data and index correctness, zero unintended acknowledged-write
   loss, successful recovery, and drained post-activity queues on every run.
4. Evaluate the approved latency, throughput, memory, storage, and recovery
   thresholds on a clean reviewed commit in clean checkout and hosted CI.
5. Run the sustained workload and final 24-hour soak without concurrent test or
   benchmark load. Evaluate trends against the approved criteria rather than
   treating completion alone as a pass.

## Artifacts

Evidence is under `target/sustained-load-2026-11-08/`. SHA-256 values:

- Events: `977634835545f514bb1ad5e904e3158153ab659638740f22b874ec9e8639d2e6`
- Drill summary: `3d420af40ddb9d5dcc853243c355f1383d0c0b7977be9658cc0fd8e0300c8219`
- Original independent summary: `f1471e1ce14cf99cdff2746968bba06e5656f7523cde071baffc44b29a1f9334`
- Duration-independent validation report: `0a029f21487fc414eea9a8812f9b7dccd9bc39129a38fc3fdd58f58f296acf1a`
- Console log: `10668c48acdd2c47291ed83b7eaeb54e414b39b72458cf3350e9fd50d3d17ceb`
- Implementation patch: `cbdd73e858aab699862664942fa1ae0b471b9ffaa992b8d262fa02f07906def5`
- Original validator: `6dd53d4d4b76a0c26ff739e92713e4b359eb2ed93c172b921012d272dba46a78`

Revalidate with the repository tool:

```bash
python3 -B tools/testing/soak_validate.py \
  target/sustained-load-2026-11-08/run-4 \
  --console-log target/sustained-load-2026-11-08/run-4-console.log \
  --expected-sustained-seconds 60 \
  --output target/sustained-load-2026-11-08/run-4-validation-repeated.json
```

Expected output begins with `PASSED: 997 events, 17 sustained samples, expected
rows=6156, correctness failures=0`. The original one-off validator and its hash
remain listed for historical provenance; the repository tool adds streaming
resource trends and does not change the unapproved budget status.

## Remaining limits

This was a short local run with a coordinated migration write pause. It did not
prove uninterrupted online writes, a shared cross-shard linearizable snapshot,
crash or power-loss recovery, S3 or distributed recovery, bounded long-term
resources, clean-checkout or hosted-CI qualification, or production readiness.
The final 24-hour run has not been performed.
