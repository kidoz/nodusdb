# Backup architecture

**Status: target architecture.** This page surveys how mature databases do
continuous backup and argues for the shape NodusDB should converge on. Parts of
it are implemented; most of it is not. The rules it produces are collected in
[backup contracts](../reference/backup-contracts.md), and the operations that
work today are in [back up and restore](../how-to/back-up-and-restore.md).

Last reviewed: 2026-06-25.

## The argument

A backup system worth trusting has five properties, and they reinforce each
other:

1. Periodic full backups establish a restorable base snapshot.
2. Incremental backups capture changed MVCC versions between protected
   timestamps.
3. Archived WAL segments bridge the gaps, giving point-in-time recovery between
   backup points.
4. Everything is written to immutable, encrypted, off-node storage.
5. A backup is not considered healthy until an automated restore has proved it
   against stated recovery objectives.

This is the common shape across mature systems, arrived at independently:
PostgreSQL combines base backups with archived WAL for point-in-time recovery;
CockroachDB combines full and incremental backups with protected revision
history; FoundationDB combines inconsistent snapshots with mutation logs and
reconstructs a consistent point in time during restore.

The fifth property is the one most often skipped and the one that matters most.
A backup that has never been restored is a hypothesis, not a safety net.

## Source basis

- PostgreSQL continuous archiving — archived WAL must cover at least as far back
  as the start of the base backup, and replay can stop at a chosen point in
  time:
  <https://www.postgresql.org/docs/current/continuous-archiving.html>
- CockroachDB full and incremental backups — incrementals depend on a full
  backup, and revision history must be protected from garbage collection:
  <https://www.cockroachlabs.com/docs/stable/take-full-and-incremental-backups>
- CockroachDB backup overview — scheduled backups, point-in-time recovery,
  encryption, locality-aware backups, and job control as first-class
  capabilities:
  <https://www.cockroachlabs.com/docs/stable/backup-and-restore-overview>
- FoundationDB backups — a distributed backup can copy inconsistent snapshots
  plus mutation logs, then combine them at restore into a consistent
  point-in-time snapshot:
  <https://apple.github.io/foundationdb/backups.html>
- AWS Well-Architected reliability guidance — identify data sources, back them
  up against recovery objectives, automate, encrypt, and test recovery
  regularly:
  <https://docs.aws.amazon.com/wellarchitected/latest/reliability-pillar/back-up-data.html>
- AWS S3 Object Lock — compliance-mode objects cannot be overwritten or deleted,
  even by the account root user, until retention expires:
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html>
- NIST SP 800-209 — backup and recovery design should cover recovery
  objectives, retention, backup health validation, point-in-time copies,
  replication, immutability, and continuous data protection:
  <https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-209.pdf>

## Where NodusDB stands

The current implementation is a useful MVP and should be read as one:

- `nodus_backup` has filesystem and S3-compatible repository backends.
- The orchestrator writes a versioned JSON manifest with SHA-256 checksums.
- The admin API exposes create, list, verify, delete, and restore.
- A full backup captures a catalog snapshot, audit events, and a full key-value
  dump.
- A background WAL archiver flushes the local LSM and archives closed segments.
- Restore can replay archived WAL records up to a target timestamp.
- Incremental backups are metadata-only: they verify the parent and write an
  empty manifest.

The gaps that matter: no real incremental data export, no protected backup
timestamp, weak WAL archive indexing, no repository immutability contract, no
schedule or job model, no restore drills, and no distributed coordination.

| Capability | Today | Target |
| --- | --- | --- |
| Repository backends | Filesystem and S3-compatible. | Repositories expose capability metadata for immutability, conditional put, range reads, multipart upload, and encryption. |
| Manifest | Versioned JSON with object checksums. | Manifest V2 as the immutable commit record: timeline, WAL coverage, layout version, format versions, compression, encryption metadata, restore validation status. |
| Full backup | Catalog, audit events, full KV dump. | Catalog, shard metadata, and shard data at a protected snapshot timestamp, with WAL coverage sufficient for point-in-time recovery. |
| Incremental backup | Metadata-only. | Exports changed MVCC versions and tombstones since the parent, fails when required history was collected, bounds chain length. |
| WAL archiving | Background task archives closed local segments. | Segments indexed by timeline, timestamp range, checksum, byte range, encryption key id, and upload state. |
| Restore | Loads backup objects, replays WAL to a timestamp. | Explicit idempotent planning, validated inputs, fresh directory or new timeline only, consistency checks before readiness. |
| Job control | Admin and CLI operations, no durable model. | Durable job records with inspect, retry, cancel, pause, resume, audit, and policy enforcement. |
| Retention | Manual delete. | Graph-aware retention that never removes anything a retained restore point needs. |
| Distributed backup | Not coordinated across shard groups. | Raft-coordinated cluster snapshot timestamp, parallel per-shard export, migration operations serialized behind backup barriers. |
| Restore drills | None. | Periodic scratch restores that persist reports and measure compliance. |

## Recovery objectives come first

Each deployment declares four numbers before any of this can be tuned:

- **Recovery point objective** — maximum tolerated data loss. For production
  OLTP this means minutes or less, which in turn requires continuous WAL
  archiving.
- **Recovery time objective** — maximum tolerated time to restore service. This
  drives backup shard sizing, parallel restore, local cache strategy, and
  whether a warm standby is needed at all.
- **Retention** — short hot retention for fast restores, longer immutable
  archive retention for compliance and ransomware recovery.
- **Restore scope** — cluster-level first; database and table-level later.

Backup cadence chosen without these is guesswork. A daily full backup cannot
serve a five-minute recovery point objective, no matter how reliable it is.

## Why the flows look the way they do

**Full backup.** Create a job record, choose a global snapshot timestamp through
the metadata Raft path, register a protected timestamp so garbage collection
cannot remove what is about to be exported, record the first required WAL
segment, export catalog and shard data, upload to a staging prefix, checksum,
verify every object, and only then write the final manifest. The ordering is the
design: the manifest is written last because it is the commit point, and every
failure before it leaves a job marked failed and nothing advertised as
restorable.

**Incremental backup.** Resolve the parent and its snapshot, choose a new
timestamp, and check that MVCC history between the two is still protected — if
it is not, fail loudly and require a new full backup rather than produce a chain
with a hole in it. Chains stay bounded, either by policy (weekly fulls with
frequent incrementals) or by a maximum length, because long chains multiply both
restore time and the chance that one link is bad.

**WAL archiving.** Segments are immutable once closed and uploaded before local
deletion or reuse, recording checksum, segment id, timeline, first and last
commit timestamp, and encryption key id. Archive lag is exposed as a metric
because it *is* the effective recovery point objective. Retention cleanup may
never delete a segment a retained chain still needs. Archived WAL is part of the
backup contract, not a side effect of LSM flush.

**Restore.** Resolve the target, build a plan, verify manifests and checksums
and format versions and key access, restore into a fresh directory or new
timeline, install catalog and shard metadata, restore shard data in parallel,
replay WAL to the target while aborting incomplete transactions, rebuild derived
state such as secondary indexes, run consistency checks, and only then mark the
cluster ready. Restores must be idempotent: a failed restore either resumes from
validated checkpoints or starts cleanly in a new directory.

## Distributed backup

Single-node backup can lean on the local snapshot. A distributed backup needs a
coordinator, and the coordination is the hard part:

- The metadata Raft group owns backup job state.
- Each shard group exports its own range at the chosen timestamp.
- Nodes upload to locality-aware destinations when configured.
- The coordinator finalizes the manifest only after every shard reports success.
- Placement metadata and data snapshots must refer to the same timestamp.
- Shard split, merge, and move either appear in the snapshot metadata or wait
  behind the backup barrier.

The first distributed milestone supports cluster-level restore only. Database
and table-level restore needs dependency ordering, schema filtering, and index
validation to mature first.

## Security and ransomware resilience

A backup repository is the thing an attacker wants to delete before they
encrypt. Production repositories should use TLS in transit; encryption at rest
with customer-controlled key ids recorded in manifests; separate write and
delete privileges; immutable retention for completed backups and WAL objects;
cross-account or cross-region copies; audit events for every create, verify,
restore, delete, retention change, and key-access failure; and workload identity
instead of long-lived secrets in configuration files.

For S3-compatible backends this means Object Lock where available. Governance
mode is useful for testing; compliance mode is the control that actually holds.

## Observability

Expose the last successful full and incremental backup timestamps, WAL archive
lag by time and bytes, backup job duration and bytes and failures, restore
duration and replay time and validation failures, retained bytes by collection
and tier, and the oldest protected timestamp with the bytes its existence is
blocking.

Alert when archive lag exceeds the recovery point objective, when no completed
backup exists inside the required window, when a restore test fails, or when
repository verification finds checksum drift.

## Roadmap

Each phase has an exit gate; the gate is the point of the phase.

**B1 — Manifest V2.** Add timeline id, snapshot timestamp, WAL start and end,
layout and object format versions, encryption key id, compression, and
validation status. Make `manifest.json` append-only with
`manifest.pending.json` for in-flight work, and add repository capability
metadata. *Gate:* an old manifest still loads, a V2 manifest round-trips, and a
partially written V2 backup is not listed as restorable.

**B2 — Real incremental backups.** Add protected timestamp records, export
changed versions and tombstones since the parent, and fail when history has been
collected. *Gate:* full plus incremental restore passes for inserts, updates,
deletes, and catalog changes.

**B3 — WAL archive contract.** Add archive index objects with per-segment commit
timestamp ranges, prevent deletion while any retained restore point needs a
segment, and add lag metrics. *Gate:* restore planning can select the exact WAL
range for a target timestamp and detect missing segments before execution.

**B4 — Restore engine.** Replace ad hoc replay with a planner and executor,
restore only into a fresh directory or new timeline, and validate catalog, MVCC
visibility, indexes, and checksums before readiness. *Gate:* restore is
idempotent, refuses unsafe targets, and records a validation report.

**B5 — Scheduler and job control.** Scheduled backups, pause, resume, cancel,
retry, inspect, durable job state, CLI and admin controls. *Gate:* jobs survive
restart, cancellation is explicit, every transition is audited.

**B6 — Distributed backup.** Raft-coordinated snapshot timestamps, parallel
per-shard export, locality-aware destinations, and shard movement serialized
across backup barriers. *Gate:* a cluster backup covers all shard ranges exactly
once, and restore verifies placement metadata against restored data.

**B7 — Restore drills and policy.** Automated scratch restores, persisted
verification reports, and a policy that denies production-ready status to
backups that are stale, unverifiable, or outside objectives. *Gate:* drills
measure recovery time and effective data loss.
