# Mixed-activity smoke drill

This opt-in drill combines a bounded SQL workload with real maintenance on a
three-node in-process cluster. It uses persistent LSM directories, real pgwire,
Raft over dedicated mTLS peer listeners, a filesystem backup repository, legacy
writes, and the production snapshot, compaction, and Raft timer settings
(200 ms heartbeat, 600–1200 ms election timeouts, 256 KiB snapshot chunks, and
a 30-second snapshot RPC timeout). It is a short correctness and observability
drill, not the final 24-hour release-candidate soak or a production capacity
test. The [2026-11-07 reclamation and physical-storage capture](../reports/raft-log-reclamation-2026-11-07.md)
records measured results and operational limitations.

## Run

Use an optimized test build and a fresh artifact directory:

```bash
NODUS_SOAK_ARTIFACT_DIR=target/mixed-activity-smoke \
cargo test --release --locked -p nodus_integration_tests \
  --test mixed_activity_soak -- --ignored --nocapture
```

The test is ignored by ordinary workspace runs. Do not run other builds, tests,
or benchmarks concurrently when collecting performance evidence. The artifact
directory must not already contain `events.jsonl`; existing evidence is never
overwritten. Without the environment variable, a unique directory is created
below `target/mixed-activity-soak/`.

## Workload and schedule

Four writers issue unique single-row inserts to the seed and one reader performs
primary-key reads. The workload stops after 6,000 acknowledged inserts. Each
value contains an ID prefix plus 256 bytes. Each pgwire attempt has a 30-second
timeout. SQL failures and timeouts are recorded and retried with a per-operation
120-second deadline. Successfully acknowledged inserts enter the expected-row
oracle. A duplicate-key retry after an uncertain outcome is included only after
an independent read confirms the complete expected value; a different value
fails the drill. Failure summaries include current workload counters and the 20
most recent read/write errors. Waiting for an acknowledgement target is bounded
by 1,800 seconds overall and 180 seconds without acknowledged progress.

Maintenance proceeds in this order while the workload continues:

1. Finalize `snapshot-v2` and `migration-v3`, create the table, and initialize
   its data shard. Before starting the workload, require three voters on the
   primary and a hosted replica on every node. The secondary index is created
   before final validation to avoid additional secondary-index writes during
   the sustained load; primary-key constraints retain their normal write path.
2. Create a full backup and verify it through the admin API.
3. Require a real automatic LSM compaction. Production defaults flush at 4 MiB
   and compact when four SSTables accumulate.
4. Require more than 1,500 retained entries and growth in both retained-entry
   count and serialized log payload bytes, followed by a new successful snapshot
   and observed drops from both pre-snapshot peaks. The production policy builds
   after 5,000 new logs and keeps 1,000 at purge; later samples may already
   include new writes.
5. Record the follower's applied index and gracefully stop node 3. While it is
   down, require another snapshot/purge cycle with entry and serialized-byte
   reclamation. The leader's applied index must advance by more than the entries
   retained at that observation, so the follower cannot catch up from the
   complete missed prefix and must receive a snapshot.
6. Restart node 3 on its original ports and persistent directory, then require
   readiness and three live nodes within bounded 300-second waits. After pausing
   writes, also require the same data-group applied index on every replica
   before migration.
7. With reads continuing, drain Raft queues and pending intents, allowing 360
   seconds so the 300-second abandoned-intent TTL and a janitor sweep can elapse,
   then split and rebalance across all three nodes. Accept the documented
   journaled-for-recovery response and wait for the two-shard map. Require the
   retired source group to be removed on every node before resuming writes; any
   other split error fails. Copy checkpoints contain 256 rows, up to 16 rows are
   copied concurrently, and each bounded step has 120 seconds. Backup and
   migration use their existing mutual-exclusion barrier and are not forced to
   run simultaneously.
8. Drain writers and readers, require Raft queue depths to return to zero, and
   require every post-split data group to converge to the same applied index on
   every node. Only then create the secondary index, so its backfill cannot scan
   incomplete local rows. Require meta-group applied-index convergence after
   index creation before comparing follower index entries. Compare the complete
   acknowledged `(id, value)` multiset on every node using quiescent MVCC reads;
   this is not a shared cross-shard linearizable snapshot. Check for missing,
   duplicate, unexpected,
   and value-mismatched rows. A bounded per-node retry waits for the secondary-
   index lookup to return exactly one expected row.
9. Continue sampling during a 10-second post-activity observation window.

A `passed` summary covers only these executed activity and correctness checks.
It does not assert long-term memory stabilization, bounded physical storage, an
approved performance SLO, or general production readiness.

## Artifacts

`events.jsonl` contains schema-version-1 `activity` and `sample` events. Activity
events include success and operation-specific details. Samples are collected
once per second and contain elapsed and wall-clock time, phase, process RSS and
allocator bytes, and per-node Prometheus and physical-storage samples. A stopped
node has null `metrics` and an error string. Unsupported process-resource
collection also produces null, not zero.

Each node's `storage.usage.logical_bytes` sums regular-file lengths in its whole
persistent data directory. On Unix, `storage.usage.allocated_bytes` sums
filesystem-reported allocated 512-byte blocks, counts hard-linked files once,
and does not follow symlinks; directories themselves are not counted. Other
platforms report allocated bytes as null. `storage.volume_available_bytes` is
the containing filesystem's available space for unprivileged users, derived
from `f_bavail` and `f_frsize` on Unix. A directory scan failure leaves `usage`
null and records `storage.error`; free-space collection failures leave
`volume_available_bytes` null. These measurements remain distinct from the
Raft payload gauge and from one another.

`summary.json` contains schema version, run identity, source/build metadata,
workload configuration and results, activity timings, exact-row validation,
100-millisecond process RSS/allocator summaries, storage-sampling semantics,
and resource assessment. The `bounded_memory`, `bounded_queues`, and
`production_budgets_met` verdicts remain null.
`raft_log_reclamation_observed` records only the observed purge activity.

## Limits

- Backup creation and verification are exercised; this drill does not restore
  that backup. Full/incremental/PITR restore evidence belongs to the restore
  drills.
- Restart is graceful and in-process. It does not prove SIGKILL, machine crash,
  power-loss, or lying-device recovery.
- Filesystem backup does not establish S3 behavior.
- The workload targets one seed to keep the follower-stop window controlled. It
  is not a linearizability proof or a multi-leader client availability test.
- Migration uses a coordinated write pause with continued reads. It does not
  prove uninterrupted online writes or migration progress under continuous
  write pressure.
- One-second directory scans can miss shorter-lived physical peaks. The short
  run and high-water samples cannot prove bounded memory, queues, or physical
  storage. Sustained stabilization, approved budgets, scheduled execution, and
  the final 24-hour soak remain separate release-qualification work.
