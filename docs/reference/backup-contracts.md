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
   before deleting versions or segments.
9. An incremental backup fails if the parent chain is incomplete, the parent is
   not complete, or required MVCC history is gone.
10. Restore defaults to a fresh data directory or a new cluster timeline.
    In-place restore over live data is out of scope.
11. Backup, restore, delete, retention, and key-access operations pass
    authorization and emit audit events.

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
| Full backup and restore round trip | Restored cluster contains catalog, audit, and KV data from the snapshot. | `nodus_backup` tests plus an admin API integration test. |
| Failed backup after partial upload | No completed manifest published; listings exclude it. | Orchestrator failure-path tests. |
| Missing data object | Verification fails before restore. | Repository corruption test. |
| Checksum mismatch | Verification fails; restore refuses to proceed. | Repository corruption test. |
| Manifest corruption | Load fails with an actionable error; backup is not restorable. | Manifest compatibility test. |
| Metadata-only incremental | Rejected once real incrementals are required. | Restore planner test. |
| Full plus incremental restore | Parent chain resolved in order; final state includes changed versions and tombstones. | Incremental restore integration test. |
| Required MVCC history collected | Incremental fails and directs the operator to a new full backup. | Protected timestamp test. |
| Point-in-time restore | WAL replay stops at the target; incomplete transactions stay invisible. | WAL replay integration test. |
| Restore into a non-empty directory | Restore fails absent an explicit unsafe override. | Restore executor test. |
| Secondary indexes after restore | Indexes validated or rebuilt before read-write promotion. | Index validation test. |
| Delete with a dependent incremental | Retention refuses to remove required parent data. | Retention planner test. |
| Stale WAL archive | Lag metric exceeds the configured recovery point objective and alerts. | Monitoring test and alert rule check. |
| Shard movement during backup | Split, move, or merge waits behind the backup barrier or is represented in snapshot metadata. | Distributed coordination fault test. |

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
