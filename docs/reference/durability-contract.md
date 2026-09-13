# Durability contract

What NodusDB guarantees about data surviving a crash, what it does not, and
which test proves each case. For the mechanisms behind these guarantees, see
[how durability works](../explanation/durability.md).

## Guarantees

These hold when a persistent data directory is configured (`[storage] data_dir`):

| Guarantee | Statement |
| --- | --- |
| Commit durability | A transaction whose `COMMIT` has been acknowledged to the client survives process crash and restart. Its writes reach the write-ahead log, and the log is `fsync`ed, before the acknowledgement is sent. |
| Abort invisibility | An aborted or never-committed transaction is never visible, before or after a crash. Its intents are invisible to readers and discarded on abort. |
| No torn reads after crash | Recovery never mistakes a torn or partial file for a complete one. A crash part-way through a flush, compaction, WAL append, or manifest write leaves the store recoverable to its last consistent state, with no acknowledged write lost. |
| Version retention | MVCC versions are reclaimed only at or below the garbage-collection watermark — the oldest timestamp any in-flight reader could still observe — so a live read never loses a version it is entitled to see. |

## Default: no durability

With no `[storage] data_dir`, NodusDB runs fully in memory and **nothing
survives restart**. This is an explicit opt-out intended for tests and ephemeral
use. Production deployments must set `data_dir`.

## Validation matrix

Each row is covered by a named test. Crash residue is reproduced directly —
truncating and corrupting files, leaving orphan SSTables and manifests — rather
than through a runtime fault injector, so the recovery code sees exactly the
on-disk state a crash would leave.

| Scenario (crash residue) | Validated by |
| --- | --- |
| Torn or partial trailing WAL record | `nodus_storage_lsm`: `torn_wal_tail_does_not_prevent_recovery` |
| Partial SSTable (`*.sst.tmp`) from a crash mid-build | `nodus_storage_lsm`: `partial_sstable_tmp_is_ignored_on_recovery` |
| Orphan SSTable absent from the manifest (crash before swap) | `nodus_storage_lsm`: `orphan_sstable_not_in_manifest_is_ignored` |
| Corrupt or torn manifest | `nodus_storage_lsm`: `corrupt_manifest_recovers_via_directory_scan` |
| Flush mid-transaction strands an intent | `nodus_storage_lsm`: `flush_retains_uncommitted_intents` |
| Archive cleanup while a commit spans WAL segments | `nodus_storage_lsm`: `archived_wal_reclamation_preserves_cross_segment_commits` |
| Cleanup after an unpublished or corrupt manifest | `nodus_storage_lsm`: `wal_reclamation_uses_published_manifest_and_fails_closed` |
| Compaction merges accumulated SSTables | `nodus_storage_lsm`: `compaction_merges_accumulated_sstables` |
| Recovery across flush and compaction (manifest-driven) | `nodus_storage_lsm`: `manifest_recovery_after_flush_and_compaction` |
| Unreadable or missing SSTable during a read | `nodus_storage_lsm`: `unavailable_sstable_fails_reads_instead_of_hiding_committed_rows` |
| Committed row and catalog survive a full restart | `tests/fault`: `committed_data_survives_a_restart` |
| Rolled-back writes never visible, before or after restart | `tests/fault`: `rolled_back_writes_are_never_visible` |
| Point-in-time restore replays archived WAL to a target time | `tests/integration`: `admin_backup_pitr_restore` |

## Local WAL archive cleanup

The server may remove an archived local WAL segment only after backup retention
permits cleanup **and** the LSM's published manifest places it below the local
replay floor. The storage engine checks and removes it while serialized with
checkpoint publication. Archiving alone does not make a segment dispensable:
an intent retained during a flush can still require an earlier segment on restart.
Invalid or unavailable checkpoint metadata prevents cleanup. This uses the
existing manifest format and does not require upgrade finalization.

## Atomic Raft snapshot installation

With persistent LSM storage, a validated snapshot replaces its group's rows and
applied pointer in one manifest publication. Other groups' state and pending
transactions remain intact. An interrupted file-publication step leaves complete
KV state that can rebuild the snapshot; it never intentionally serves a new
snapshot descriptor with the previous file. Uncertain manifest I/O fails closed
until reopen. These guarantees assume the authoritative manifest remains readable.

| Scenario | Evidence |
| --- | --- |
| Abrupt exit on either side of the manifest swap | `nodus_storage_lsm`: `abrupt_exit_at_manifest_boundary_recovers_old_or_complete_checkpoint` |
| Shared-engine history, intents and node-local records survive, including encrypted WAL reopen | `nodus_storage_lsm`: `checkpoint_preserves_other_group_history_intents_and_local_records` |
| Exit after checkpoint but before snapshot file publication | `nodus_server`: `abrupt_install_restart_never_serves_mixed_snapshot_and_applied_state` |
| Catalog/role membership and another shard survive meta install/reopen | `nodus_server`: `meta_snapshot_preserves_local_shards_and_recovers_catalog_authorization` |
| Purged-log learner receives a real HTTP snapshot and reopens | `nodus_server`: `lagging_learner_receives_snapshot_over_tcp_and_reopens_on_lsm` |
| V2 history and pending intent survive TCP transfer and a subsequent replicated commit | `nodus_server`: `mvcc_learner_preserves_history_and_resolves_snapshotted_intent_over_tcp` |
| V2 abrupt checkpoint interruption retains history, intent and recovery generation | `nodus_server`: `mvcc_install_crash_recovers_history_intents_and_backup_generation` |
| Restart enforces backup boundary; fresh full restores rows/indexes and excludes older WAL | `nodus_server`: `durable_checkpoint_backup_boundary_survives_restart_and_new_full_restores_rows` |
| Local clock remains above installed timestamps after restart | `nodus_server`: `snapshot_reserves_clock_above_incoming_versions_across_restart` |

NSNP v2 preserves retained user history, tombstones and pending intent identities.
The production writer enables v2 only after [verified durable finalization](../explanation/upgrade-authority.md).
Before finalization it uses v1, which refuses those states and can stop the group
because OpenRaft treats build errors as fatal. Installation materializes a bounded checkpoint and pauses the
shared engine. The same checkpoint publishes a local recovery generation and WAL
floor. Source-bound backup operations reject cross-generation incremental ancestry
and PITR extension until a new full backup; offline enforcement requires the
repository to have observed the boundary. Live-reader retention, coordinated
repository publication and superseded-file cleanup remain open. See the
[MVCC snapshot and backup contract](../explanation/mvcc-snapshots.md). See the [snapshot limits and compatibility
matrix](../explanation/shard-migration-protocol.md#snapshot-limits).

## Dormant migration recovery

The disabled migration protocol has separate Raft/LSM evidence:
`coordinator_resumes_after_abrupt_process_exit_before_ack` exits a subprocess
without shutdown after acquire acknowledgement, then reopens the same LSM
storage and resumes the coordinator from its journal. The real-TCP test
`coordinator_journal_resumes_on_new_tcp_leader_without_acquire_ack` exercises
leader loss at that boundary. These tests do not enable online migration or
prove power-loss durability. See the [protocol and compatibility limits](../explanation/shard-migration-protocol.md).

## Limitations

| Limitation | Consequence |
| --- | --- |
| In-memory by default | Durability requires `[storage] data_dir`. |
| On-disk formats are not backward compatible across the hardening changes (WAL CRC frame, SSTable v2, catalog-in-KV) | Upgrading across those changes requires a clean restart. There is no cross-version on-disk compatibility guarantee yet. |
| Catalog persistence writes a full-state blob per DDL | Catalog writes are not incremental per entry. |
| WAL retention has two boundaries | Local cleanup waits for both backup retention and the durable local replay floor; retained intents or restore points can delay reclamation. |
| General runtime fault injection is not wired into the storage engines | Snapshot tests have targeted test-only subprocess exit hooks; these and existing disk-residue tests do not establish power-loss durability. |
| Single-node scope | This contract covers local durability. Replication and cross-node durability are properties of the Raft layer, not of this contract. |
