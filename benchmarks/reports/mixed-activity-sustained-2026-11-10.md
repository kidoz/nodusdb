# Mixed-activity sustained-load drill — 2026-11-10

## Scope and result

The [mixed-activity drill](../workloads/mixed_activity_soak.md) with a one-hour
sustained-load window (`NODUS_SOAK_SUSTAIN_SECONDS=3600`) **passed** on clean
commit `9ffd8d6` in 22,257 seconds (6 hours 11 minutes). All activities
succeeded: backup verification, automatic compaction, two snapshot/purge
cycles, snapshot-required follower catch-up, split/rebalance with roster-wide
source teardown and physical reclamation, 3,601 seconds of sustained load with
9,468 acknowledged inserts, and exact recovery validation of 15,468 rows on
every node with zero missing, duplicate, unexpected, or mismatched rows.

Independent validation with `tools/testing/soak_validate.py` **passed**:
12,551 events, 1,017 sustained samples, zero correctness failures, exact
dynamic row totals, drained quiet-window queues, and zero JSON decode errors.

The run also produced two operational findings, recorded below: multi-hour
meta-group quorum instability after the sustained window, and sustained
memory/storage growth trends. All budget, bounded-resource, and stabilization
verdicts remain null because maintainer-approved thresholds do not exist.
General production remains **NO-GO**.

A first attempt (r1) failed after 1 hour 59 minutes because the host entered
macOS idle sleep during the final phase; it is preserved as development
evidence and analyzed below. The passing run (r2) executed under
`caffeinate -isu`.

## Environment and method

- Machine: MacBook Pro (Mac16,5), Apple M4 Max, 16 cores, 48 GiB RAM.
- OS: macOS 27.2, arm64; local journaled APFS.
- Toolchain: rustc 1.99.1 (58cf8f9c3 2026-10-13); cargo 1.99.1
  (5f94df478 2026-08-27).
- Build: Cargo `release` profile, optimized and locked.
- Source: clean commit `9ffd8d6bca7ce1e1de4d266069907f549a866419`. The
  artifacts record `source.dirty=true` solely because of the pre-existing
  maintainer `.gitignore` modification, which is not a build input; the
  untracked maintainer planning documents were left untouched.
- Server: three in-process `nodus_testkit` nodes, real pgwire, Raft over
  dedicated mTLS peer listeners, persistent LSM directories, filesystem backup
  repository, legacy writes, and production snapshot/compaction/timer settings.
- The passing run executed under `caffeinate -isu` to prevent host idle sleep.
  No other build, test, or benchmark ran concurrently.
- Power log review confirms no host sleep during the r2 execution window; the
  first post-run idle sleep began seven minutes after the test finished.

Passing run (r2):

```bash
caffeinate -isu env \
NODUS_SOAK_SUSTAIN_SECONDS=3600 \
NODUS_SOAK_ARTIFACT_DIR=target/mixed-activity-sustained-2026-11-10-r2 \
cargo test --release --locked -p nodus_integration_tests \
  --test mixed_activity_soak -- --ignored --nocapture \
  > target/mixed-activity-sustained-2026-11-10-r2-console.log 2>&1
```

Independent validation:

```bash
python3 -B tools/testing/soak_validate.py \
  target/mixed-activity-sustained-2026-11-10-r2 \
  --expected-sustained-seconds 3600 \
  --console-log target/mixed-activity-sustained-2026-11-10-r2-console.log
```

Expected output begins with `PASSED: 12551 events, 1017 sustained samples,
expected rows=15468, correctness failures=0`.

## Activity timeline

Elapsed times are from drill start; the sustained window ran from elapsed
2,219 s to 5,819 s.

| Activity | Elapsed | Result |
| --- | ---: | --- |
| Full backup created and verified | 32 s | success |
| Automatic LSM compaction (10 compactions) | 32 s | success |
| First snapshot and log reclamation | 496 s | success |
| Graceful follower (node 3) stop | 505 s | success |
| Second snapshot/purge while follower down | 838 s | success |
| Snapshot-required follower restart | 840 s | success |
| Snapshot catch-up verified (applied 10,063 on every node) | 870 s | success |
| Split/rebalance with read load | 1,203 s | success |
| Sustained load (requested 3,600 s) | 5,819 s | success, actual 3,601.3 s |
| Exact recovery validation | 21,828 s | success |

## Snapshot and log reclamation

| Observation | Peak entries | Retained entries | Entries reclaimed | Peak payload | Retained payload | Payload reclaimed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| First snapshot | 4,999 | 1,019 | 3,980 | 2.92 MiB | 0.60 MiB | 2.32 MiB |
| Second snapshot, follower down | 6,000 | 1,024 | 4,976 | 3.53 MiB | 0.60 MiB | 2.93 MiB |

The follower missed 4,987 log entries while the leader retained 1,024, so
append-only catch-up was impossible and snapshot install was required. After
restart the data group reached applied index 10,063 on every node.

Split/rebalance recorded `source_decommissioned_on_every_node=true`,
`physical_reclamation_verified=true`, `source_namespaces_empty_on_every_node=
true`, and `allocated_storage_not_increased_on_every_node=true` with an
observed allocated reduction. Archived WAL was deliberately retained for
backup policy.

## Sustained workload

| Measurement | Result |
| --- | ---: |
| Requested / actual sustained duration | 3,600 / 3,601.3 s |
| Sustained acknowledged inserts | 9,468 (2.63/s) |
| Total acknowledged inserts | 15,468 |
| Write attempts / retries | 15,586 / 236 (1.51%) |
| Uncertain writes recovered by exact-value confirmation | 118 |
| Successful reads / read retries | 671,557 / 104 (0.015%) |
| Write mean / p50 / p95 / p99 / max | 1.399 / 1.362 / 2.048 / 3.376 / 6.329 s |
| Read mean / p50 / p95 / p99 / max | 1.94 / 1.09 / 2.12 / 19.95 / 4,139 ms |

Captured write errors were the expected transient SQLSTATE 40001 shard-routing
conflicts and the corresponding duplicate-key retries after uncertain
outcomes; every uncertain write was resolved by exact-value confirmation.
Throughput and latency are observations against no approved budget.

## Resource trends during the sustained window

Least-squares trends over the 1,017 sustained samples. Sustained user data was
about 2.5 MiB (9,468 inserts of roughly 280 bytes); measured growth is
dominated by Raft log, WAL, and LSM overhead between maintenance cycles.

| Metric | Start | End | Max | Trend |
| --- | ---: | ---: | ---: | ---: |
| Process RSS | 885 MiB | 2,347 MiB | 2,380 MiB | +1.19 GiB/h |
| Allocator bytes | 735 MiB | 1,456 MiB | 1,867 MiB | +0.99 GiB/h |
| Node 1 allocated storage | 331 MiB | 809 MiB | 809 MiB | +481 MiB/h |
| Node 2 allocated storage | 334 MiB | 803 MiB | 803 MiB | +465 MiB/h |
| Node 3 allocated storage | 297 MiB | 782 MiB | 782 MiB | +472 MiB/h |
| Router/commit queue depth (all nodes) | — | 0 | 1 | flat |

Whole-run process RSS reached a high water of 7.56 GiB and ended at 3.71 GiB;
the allocator high water was 3.41 GiB. The process contains three servers plus
clients. These are trends over one hour on one host, not evidence of a leak or
of bounded long-term memory; stabilization verdicts remain null.

## Operational findings

### Meta-group quorum instability after sustained load

The migration coordinator logged 589
`migration journal discovery failed: not enough for a quorum` warnings between
01:03 and 06:50 UTC; from 02:05 onward they were persistent, with only one
meta voter answering (`got: {1}`). The post-sustained phase (writer
drain, secondary-index creation, applied-index convergence, exact-row
validation) consequently stretched from minutes in prior short drills to
4 hours 27 minutes. The data groups kept quorum throughout: sustained writes
were acknowledged at 2.63/s without data-group leadership loss, and final
validation passed once the meta group stabilized. The console recorded 69,088
Raft RPC 200 ms timeouts (`other_raft_rpc_errors`), against 143–654 in the
earlier roughly 35-minute drills. Zero responses were misclassified and there
were zero JSON decode errors; retry pacing behaved as designed. The cause
(event-loop starvation in the in-process three-node harness versus a
meta-group liveness defect) is not established by this drill and is a
follow-up investigation. This is an availability-relevant signal with no
approved budget to judge it against.

### Failed first attempt: host idle sleep

Run r1 failed after 1 hour 59 minutes with `raft write to 'shard-meta' did not
reach a leader` surfaced by the final workload result. The `pmset` log shows
the host entered idle sleep at 03:18 local and cycled maintenance sleeps; a
377-second maintenance sleep matches the drill's console silence
(00:28:22–00:34:38 UTC) exactly. Frozen Raft timers across the sleep/wake
boundaries caused cluster-wide heartbeat and vote timeouts and meta-group
quorum loss. All scheduled activities, including the complete sustained
window (3,601 s, 9,361 inserts), had succeeded before the failure. The r1
artifacts are preserved as development evidence, not a passing result. Long
evidence runs on this host class must run under `caffeinate` or an equivalent
sleep prevention.

## Artifacts

Passing run (r2) evidence is under `target/mixed-activity-sustained-2026-11-10-r2/`:

- `events.jsonl`: 12,551 schema-1 events including 1,017 sustained samples;
- `summary.json`: passed drill summary with activity timings and exact-row
  validation;
- `validation.json`: independent validator report with trends and hashes.

SHA-256 values (r2):

- Events: `7e47139081a183d453af6cca6d0b99a79b4767bc1f64347d9bdbbeeb27988283`
- Drill summary: `8116e66c3dc17a56fa7b24cef55439c3e1b2d2af9acaa39e1f76fcac111548b0`
- Console log: `6e1d9347d7ef1fdab1c429f0aaaea6182b37c9da0f8d5e96be47bb98eb4889f5`

Failed first attempt (r1) evidence is under `target/mixed-activity-sustained-2026-11-10/`:

- Events: `ab238fd01fea42fb1a1522a5036d1f46286b332017aa72b27477a7614bb85bed`
- Drill summary (`status=failed`): `610b8bffb6df4f9eb3d112d6bd5d7dcfbd6347efb9ddae0d5ef29ffd9ffc3947`
- Console log: `0b77a58612b529b8076959ea9dc5ddfc359834a4e56ad7a98a370a6885416ef0`

## Limits

- One one-hour sustained window on one macOS host is not the 24-hour
  release-candidate soak, repeated supported-profile qualification, or an
  approved resource budget. All budget and stabilization verdicts remain null.
- Restart was graceful and in-process. This does not prove SIGKILL, machine
  crash, power-loss, or lying-device recovery.
- Filesystem backup creation and verification were exercised, not restoration
  or S3 behavior.
- Migration paused writes while reads continued. Uninterrupted online writes
  and migration progress under continuous write pressure were not proven.
- Exact validation used per-group convergence and quiescent MVCC reads, not a
  shared cross-shard linearizable snapshot.
- The multi-hour meta-group quorum instability occurred in an in-process
  harness on a loaded host; whether it reproduces in a supported multi-process
  deployment is unknown.
- One-second storage scans can miss shorter-lived physical peaks; the
  two-second Raft payload gauge can miss transient log peaks.
