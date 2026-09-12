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
| Compaction merges accumulated SSTables | `nodus_storage_lsm`: `compaction_merges_accumulated_sstables` |
| Recovery across flush and compaction (manifest-driven) | `nodus_storage_lsm`: `manifest_recovery_after_flush_and_compaction` |
| Unreadable or missing SSTable during a read | `nodus_storage_lsm`: `unavailable_sstable_fails_reads_instead_of_hiding_committed_rows` |
| Committed row and catalog survive a full restart | `tests/fault`: `committed_data_survives_a_restart` |
| Rolled-back writes never visible, before or after restart | `tests/fault`: `rolled_back_writes_are_never_visible` |
| Point-in-time restore replays archived WAL to a target time | `tests/integration`: `admin_backup_pitr_restore` |

## Limitations

| Limitation | Consequence |
| --- | --- |
| In-memory by default | Durability requires `[storage] data_dir`. |
| On-disk formats are not backward compatible across the hardening changes (WAL CRC frame, SSTable v2, catalog-in-KV) | Upgrading across those changes requires a clean restart. There is no cross-version on-disk compatibility guarantee yet. |
| Catalog persistence writes a full-state blob per DDL | Catalog writes are not incremental per entry. |
| WAL segment retention is not automatic | Superseded segments are retained for point-in-time recovery; reclaiming them is manual. |
| Runtime fault injection is not wired into the storage engines | `nodus_testkit::FaultInjector` exists, but the low-level storage crates cannot depend on it; the matrix above simulates crash residue on disk instead. |
| Single-node scope | This contract covers local durability. Replication and cross-node durability are properties of the Raft layer, not of this contract. |
