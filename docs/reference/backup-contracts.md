# Backup contracts

Rules a backup implementation may not break, the repository layout they assume,
and the scenarios that prove them. These are stronger than implementation
details: they must survive refactors.

**Status.** The contracts are binding on the design. Current behaviour does not
yet satisfy all of them — the gaps are named in
[backup architecture](../explanation/backup-architecture.md), which is also
where the reasoning behind each rule lives.

Storage snapshot installation now creates an atomic local recovery generation
and WAL floor. Source-bound backup operations enforce matching generations for
incremental ancestry and PITR extension; a new full backup is required after a
checkpoint. Offline enforcement starts when the source publishes the boundary
to the repository. See the [implemented boundary and its limits](../explanation/mvcc-snapshots.md#backup-recovery-boundary).
The optional checksummed generation object and mutable repository boundary do
not implement the collection layout or distributed retention design below.

Logical backup and PITR replay retain the destination cluster's upgrade policy:
source upgrade-authority records are excluded from export and skipped during
replay. They transfer through meta Raft snapshots instead. See the
[upgrade authority contract](../explanation/upgrade-authority.md).

The same is true of every node-local or consensus-local namespace: export drops
them and replay skips them (base dump and PITR WAL alike), so a restore can
never import another node's local control state. The excluded set is defined by
`nodus_backup::is_backup_excluded_key` and covers, also inside `shard-{id}\0`
data-group namespaces:

- `\x00raft\x00…` — Raft votes, applied pointers, log entries, snapshot
  metadata. Restoring a rolled-back vote on rejoin would break election safety.
- `\x00recovery\x00generation` — the local storage recovery generation.
- `\x01upgrade/…` — upgrade authority and admission ledger.
- `\x01migration/…` — shard-migration fences and coordinator journals.

Everything else round-trips deliberately: user and catalog data, `meta:*`
shard maps and placements, the `\x00catalog\x00state` blob, `\x00txn2pc\x00…`
two-phase-commit decision records (needed to resolve restored prepared
intents), and the `\x00hlc\x00watermark` clock reservation.

## Repository layout

Append-only object paths, so a repository can be made immutable:

```text
clusters/{cluster_id}/
  collections/{collection}/
    backups/{backup_id}/
      manifest.pending.json
      manifest.json
      data/
        catalog.snapshot
        audit.snapshot
        shard-{shard_id}/part-{n}.ndb
      verify/
        restore-test-{run_id}.json
    wal/{timeline_id}/
      {segment_id}.log
      {segment_id}.index.json
    indexes/
      backups.json
      wal.json
      retention.json
```

`manifest.json` is the commit point. A backup carrying only
`manifest.pending.json` is not restorable.

## Repository backends

The server selects the repository from `[backup] repository_uri` at startup:

- `file:///path` or a bare path — local filesystem (`FsBackupRepository`).
  Writes are durable and atomic: temp file, fsync, rename, directory fsync.
- `s3://bucket[/prefix]` — S3-compatible object store (`S3BackupRepository`
  over the `object_store` crate). Credentials, region, and endpoint come from
  the standard AWS-compatible environment variables (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`),
  never from the config file. Existence checks distinguish genuine absence
  (`404 NotFound`) from transient errors, so a network outage fails the
  protection paths closed instead of flipping manifests to unreadable.

Any other URI scheme fails startup with an error naming the scheme.

CI coverage includes an in-memory object-store round trip and an explicitly
environment-gated live test. The infrastructure job starts a pinned MinIO source
release, creates an isolated bucket per run, and exercises backup, verification,
semantic restore, deletion, checksum failure, and unavailable-endpoint behavior.
It also runs full, incremental, and PITR restore drills against MinIO and exports
a real span to an OpenTelemetry Collector. Without `NODUS_S3_ENDPOINT` or
`NODUS_OTLP_ENDPOINT`, the corresponding tests print an explicit skip; the live
job always sets them, so its pass cannot be a vacuous skip.

## Core concepts

| Term | Definition |
| --- | --- |
| Backup collection | A durable namespace holding a chain of full backups, incrementals, WAL archives, verification results, and a retention policy. |
| Backup manifest | Immutable metadata for one backup: cluster id, timeline id, type, parent id, snapshot timestamp, WAL coverage, format versions, files, checksums, encryption metadata, status. |
| Timeline | A restore lineage. Restoring creates a new timeline so WAL from old and new histories cannot be confused. |
| Protected timestamp | An MVCC timestamp below which versions needed by a running backup cannot be garbage-collected. |
| WAL archive index | A manifest of segment names, commit timestamp ranges, byte ranges, checksums, encryption key id, and upload state. |
| Restore plan | A deterministic selection of full backup, incrementals, and WAL segments needed to reach a target timestamp. |

## Contracts

1. `manifest.json` is the only durable signal that a backup is complete.
2. `manifest.pending.json` and staged data objects are never listed as
   restorable.
3. A completed manifest references only objects that exist and whose checksums
   have been verified.
4. Manifest, WAL archive index, restore plan, and persistent object formats are
   versioned and forward-readable by newer binaries.
5. Every backup belongs to exactly one cluster id, collection, and timeline.
6. A restore into a new timeline never replays WAL from a different timeline
   unless an explicit migration tool has validated the lineage.
7. A protected timestamp is registered before MVCC data is exported and stays
   active until the backup completes, or fails and is cleaned up.
8. MVCC garbage collection and WAL cleanup consult retained restore points
   before deleting versions or segments. Both fail closed: an unreadable
   backup repository pins the GC watermark at 0 (unreadable manifests) or
   skips the GC tick entirely (repository error), never "no protection".
9. An incremental backup fails if the parent chain is incomplete, the parent is
   not complete, or required MVCC history is gone.
10. Restore defaults to a fresh data directory or a new cluster timeline.
    In-place restore over live data is out of scope, and the server enforces
    it: `POST /api/v1/backups/{id}/restore` fails with `409 Conflict`
    (`restore_destination_not_empty`) when the destination engine holds
    anything beyond bootstrap-internal state (Raft/consensus records, the HLC
    watermark, the recovery generation, upgrade records, the bootstrap
    catalog blob). On a cluster (more than one meta-group member) the same
    refusal is `409 Conflict` (`restore_clustered_not_empty`) when any data
    exists; a **fresh** cluster accepts the restore, and the replay reaches
    every member through each group's Raft log (the single-node fences stay
    node-local, so a fresh cluster restore must complete before the cluster
    opens for traffic). The one exception is retrying a restore that failed
    mid-mutation: the engine stays fenced and the idempotent retry is allowed
    to complete.

11. Backup, restore, delete, retention, and key-access operations pass
    authorization and emit audit events.
12. Archived WAL objects and indexes are append-only. Identical publication
    retries are idempotent; a different segment or index at an existing key is
    rejected without replacing the original. A restored node must use an isolated
    repository prefix before resuming normal archiving until automatic timeline
    allocation is implemented. The current check is not an atomic cross-writer
    conditional put; concurrent repository writers require the distributed
    coordination milestone, not independent node archivers pointed at one prefix.
13. On clusters that finalized the migration level, every backup create is
    journaled on the meta group before any export: one exporting job at a
    time, migrations refused while a job exports and vice versa, the job
    marked `Failed` by the meta leader if the exporter's heartbeat dies, and
    an incremental job's parent snapshot pinned against MVCC GC for the job's
    lifetime. Journaled backups write object format V3: `meta.json` plus one
    `shard-{id}.json` object per shard, with `shard_coverage` in the manifest
    (shard, group, range, placement, object, row count). Restore validates the
    coverage against the fetched objects before mutating anything. Legacy
    V2/V1-format backups restore unchanged.

Level 1 sources write manifest V2 / object format V1 without role memberships.
Level 2 sources write manifest V3 / object format V2 with principals, grants,
and role memberships. Level 3 sources write manifest V3 / object format V3
with per-shard coverage. New binaries read all three pairs; older binaries
reject what they cannot decode. V2 backups remain useful for their recorded
data but cannot prove complete role-membership recovery.

## Restore plan contents

A plan is produced before any target directory is touched, and the executor
treats it as immutable input:

- selected full backup id;
- ordered incremental backup ids;
- target timestamp and target timeline;
- WAL segment ids, byte ranges, and the replay stop condition;
- expected source cluster id and source timeline id;
- target directory and target cluster/timeline policy;
- catalog, shard, index, and security metadata restore actions;
- checksum and format-version checks to run before installing data;
- validation actions to run before marking the restored cluster ready.

If repository state changes between planning and execution, the executor
replans or fails with an actionable error rather than proceeding.

## Retention rules

Retention preserves every object required to restore each retained point:

- the full backup;
- every incremental in the selected chain;
- every WAL segment from the base backup's start through the point-in-time
  window;
- manifests and verification reports, for as long as audit requires.

Garbage collection is backup-aware: MVCC GC and WAL cleanup cannot run purely on
local storage pressure.

## Verification levels

1. **Object verification** — the manifest exists, all files exist, checksums
   match.
2. **Dry restore** — restore into a scratch directory and open the engine.
3. **Semantic restore** — run SQL and catalog checks against the restored
   database.

A backup strategy that has never been restored and queried is unproven.

## Scenario matrix

| Scenario | Expected result | Coverage |
| --- | --- | --- |
| Full backup and restore round trip | Restored cluster contains catalog, audit, and KV data from the snapshot. | `nodus_backup` tests, admin API integration, and persistent filesystem/S3 restore drills. |
| Failed backup after partial upload | No completed manifest published; listings exclude it. | Orchestrator failure-path tests. |
| Missing data object | Verification fails before restore. | Repository corruption test and live MinIO gate. |
| Checksum mismatch | Verification fails; restore refuses to proceed. | Repository corruption test and live MinIO gate. |
| Manifest corruption | Load fails with an actionable error; backup is not restorable. | Manifest compatibility test. |
| V2 manifest upgrade | New binary reads V2/V1; V3/V2 is written only at compatibility level 2 or later. | Manifest version tests and security-complete restore drills. |
| Metadata-only incremental | Rejected once real incrementals are required. | Restore planner test. |
| Full plus incremental restore | Parent chain resolved in order; final state includes changed versions and tombstones. | Persistent filesystem and live MinIO restore drills with restart checks. |
| Required MVCC history collected | Incremental fails and directs the operator to a new full backup. | Protected timestamp test. |
| Point-in-time restore | WAL replay stops at the target; incomplete transactions stay invisible. | WAL replay tests and persistent filesystem/S3 PITR drills. |
| Security metadata after restore | Principals, grants, memberships, and SCRAM login are preserved in V3 restores. | Catalog unit test and restore-drill transitive permission checks. |
| Restore into a non-empty directory | Restore is refused with `409 Conflict` before any mutation. | Admin API integration test. |
| Secondary indexes after restore | Index metadata survives restore and restart. | Restore-drill `pg_indexes` checks. |
| WAL archive collision | Different bytes or metadata never replace an archived object; identical retries are idempotent. | WAL publication unit test and repeated destination drills. |
| Unavailable object repository | Transport errors propagate and protection paths fail closed. | Live MinIO unavailable-endpoint test. |
| Delete with a dependent incremental | Retention refuses to remove required parent data. | Retention planner test. |
| Missing, stale, or failed restore drill | Alert fires from the drill's Prometheus textfile metrics. | Docker alert rules and Helm `PrometheusRule` rendering. |
| Stale WAL archive | Lag metric exceeds the configured recovery point objective and alerts. | Not yet implemented. |
| Shard movement during backup | Split, move, or merge is refused while a backup job exports (and vice versa) on migration-level clusters; the journaled barrier is enforced at meta-group apply in both directions. | Covered by `migration_tests::discovery::backup_and_migration_jobs_exclude_each_other` and `migration_tests::cluster_v3::backup_job_abandoned_by_a_dead_exporter_is_swept_after_failover`; the three-node mTLS coverage/restore proof is `tests/integration/tests/cluster_backup.rs`. |

## Acceptance criteria

- A completed backup can always be restored in tests.
- Failed or partial backups are never listed as restorable.
- Full plus incremental plus WAL restore reaches the requested timestamp.
- WAL archive lag is observable and bounded by the configured recovery point
  objective.
- MVCC garbage collection cannot remove versions needed by a pending or
  retained backup.
- Repository corruption is detected by verification.
- Restore drills record measured recovery time and data loss.
- Backup, restore, delete, and retention changes are authorized and audited.
