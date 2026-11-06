# Durability contract

What NodusDB guarantees about data surviving a crash, what it does not, and
which test proves each case. For the mechanisms behind these guarantees, see
[how durability works](../explanation/durability.md).

## Guarantees

These hold when a persistent data directory is configured (`[storage] data_dir`):

| Guarantee | Statement |
| --- | --- |
| Commit durability | A transaction whose `COMMIT` has been acknowledged to the client survives process crash and restart. Its writes reach the write-ahead log, and the log is `fsync`ed, before the acknowledgement is sent. |
| Batch commit recovery | A persistent `CommitTxnBatchV1` is one atomic WAL record followed by one sync. Recovery either replays every transaction in that record or none of them; live publication begins only after the sync succeeds. |
| Abort invisibility | An aborted or never-committed transaction is never visible, before or after a crash. Its intents are invisible to readers and discarded on abort. Batched put/delete intents remain invisible until their transaction commits. |
| Uncertain ownership is protected | The intent janitor does not infer death from age, local-session absence, or an absent visible 2PC decision. It retains uncertain intents instead of automatically aborting a possibly live remote transaction. |
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
on-disk state a crash would leave. Power loss is simulated with a
sync-tracking WAL wrapper (`nodus_storage_lsm::powercut_tests`): the wrapper
delegates to the real `FileWalEngine` and records how many bytes each `fdatasync`
made durable, then the segment is cut back to a state a power failure could
legitimately leave — synced prefix intact, the unsynced tail kept up to an
arbitrary byte boundary, optionally with a torn final write — and recovery runs
as the real engine reopen. Unlike SIGKILL (whose surviving OS still flushes the
dead process's dirty pages), this distinguishes "written" from "fsynced", which
is what the commit-acknowledgement ordering rests on.

| Scenario (crash residue) | Validated by |
| --- | --- |
| Torn or partial trailing WAL record | `nodus_storage_lsm`: `torn_wal_tail_does_not_prevent_recovery` |
| Torn WAL tail truncates at recovery, and commits acknowledged after that recovery survive the next power cut | `nodus_storage_wal`: `recover_truncates_a_torn_tail_so_later_appends_stay_replayable`; `nodus_storage_lsm`: `powercut_tests::torn_wal_tail_recovers_and_later_commits_survive_the_next_power_cut` |
| Power cut after acknowledged commits, cut at every byte of the unsynced WAL tail (clean and torn variants) | `nodus_storage_lsm`: `powercut_tests::acknowledged_commits_survive_power_cut_at_every_byte_of_the_unsynced_tail` |
| Power cut with a commit whose fsync never completed is all-or-nothing, never partially applied | `nodus_storage_lsm`: `powercut_tests::unacknowledged_commit_is_never_partially_applied_after_power_cut` |
| Power cut with an unsynced commit batch never partially applies the batch | `nodus_storage_lsm`: `powercut_tests::unacknowledged_commit_batch_is_never_partially_applied_after_power_cut` |
| One commit batch syncs once and recovers every transaction, including after intent flushes | `nodus_storage_lsm`: `powercut_tests::commit_batch_syncs_once_and_recovers_every_transaction`, `commit_batch_recovers_all_transactions_after_intent_flushes` |
| Reserved keys, write conflicts, and invalid sizes reject a write batch without partial intents | `nodus_raftstore`: `migration::tests::write_batch_rejects_any_reserved_key_before_writing`, `migration::tests::write_batch_conflict_is_rejected_without_partial_intents`, `migration::tests::invalid_batch_sizes_are_rejected_at_apply` |
| Batched commits publish all transactions and replay idempotently | `nodus_raftstore`: `migration::tests::commit_batch_publishes_all_transactions_and_replays_idempotently` |
| An old remote transaction still commits all acknowledged writes; a remote sweep after prepare but before the commit decision cannot tear a successful commit | `nodus_server`: `raft_kv::janitor_tests::old_remote_transaction_survives_sweep_and_commits_every_write`, `remote_sweep_between_prepare_and_decision_cannot_tear_a_commit` (two persistent in-process nodes, real TCP Raft forwarding, parameterized age, bounded pause at the production meta-decision write) |
| Unknown-owned intents remain uncommitted and block conflicting writes and fencing until authoritative resolution | `nodus_server`: `raft_kv::tests::janitor_retains_uncertain_intents_and_conflicting_writes_remain_blocked`, `janitor_skips_live_sessions_and_young_orphans`, `migration_tests::coordinator::uncertain_intent_keeps_fencing_blocked_until_owner_resolution` |
| Decided 2PC commits remain recoverable and uncertain-backlog metrics are exported | `nodus_server`: `raft_kv::tests::janitor_never_aborts_decided_2pc_participants`; `nodus_monitoring`: `registered_metrics_appear_in_output` |
| Unknown valid-frame WAL and Raft log records fail recovery rather than being skipped | `nodus_storage_wal`: `recover_rejects_a_newer_record_version`, `recover_rejects_undecodable_supported_version_payloads`; `nodus_raftstore`: `store_construction_fails_closed_on_unknown_log_envelope`, `store_construction_fails_closed_on_malformed_log_entry_json` |
| Incorrect or missing WAL encryption keys, short encrypted payloads, and undecodable legacy records reject recovery without changing the segment | `nodus_storage_wal`: `incorrect_encryption_configuration_preserves_wal_bytes`, `authentication_failure_after_valid_prefix_preserves_entire_wal`, `complete_frames_with_short_encrypted_payloads_are_not_truncated`, `undecodable_legacy_frames_are_not_skipped_or_truncated` |
| Wrong/missing-key LSM startup and predecessor authentication failure preserve intact files; retries recover acknowledged commits across rotation and exclude aborts | `nodus_storage_lsm`, integration test `wal_recovery`: `failed_encrypted_startup_preserves_files_and_correct_key_recovers_commits`, `authentication_failure_in_predecessor_segment_rejects_startup_without_truncation` |
| Missing active/middle/root WALs, invalid links or headers, and invalid recovery bounds reject startup without changing files; restoring missing WALs recovers committed rows | `nodus_storage_lsm`, integration test `wal_chain`: `missing_active_middle_or_root_wal_rejects_startup_and_restores_commits`, `malformed_segment_headers_and_links_reject_without_modifying_files`, `invalid_manifest_wal_ids_and_floor_fail_without_creating_files`, `dangling_active_wal_symlink_cannot_create_its_target` (Unix) |
| Chain validation precedes all tail repair; reclaimed predecessors below the replay floor and unpublished WALs remain unnecessary | `nodus_storage_lsm`, integration test `wal_chain`: `missing_predecessor_does_not_repair_active_tail_until_chain_is_valid`, `replay_floor_allows_reclaimed_predecessors_and_ignores_orphan_wals`; `wal_recovery`: `authentication_failure_in_predecessor_segment_rejects_startup_without_truncation` |
| WAL scanning is read-only, deferred repair refuses a changed length, and existing-file opens do not create missing WALs | `nodus_storage_wal`: `scan_defers_tail_repair_until_explicitly_requested`, `deferred_tail_repair_refuses_a_changed_file_length`, `open_existing_never_creates_a_missing_wal_and_preserves_append_support` |
| Restart reserves orphan file ids without adopting their contents; new WAL creation cannot append to existing segments and file ids cannot wrap | `nodus_storage_lsm`, integration test `wal_chain`: `restart_reserves_orphan_ids_without_reading_or_reusing_their_files`; `nodus_storage_lsm`: `file_id_exhaustion_never_wraps_into_existing_ids`; `nodus_storage_wal`: `exclusive_creation_never_appends_to_an_existing_segment` |
| Incomplete encrypted tails are repaired at every byte boundary, and later appends survive reopen | `nodus_storage_wal`: `encrypted_incomplete_tail_is_repaired_before_later_appends` |
| WAL segment power cut keeps exactly the fsynced prefix; garbled or flipped tails are rejected | `nodus_storage_wal`: `power_cut_preserves_exactly_the_synced_prefix`, `power_cut_with_garbled_tail_recovers_the_synced_prefix` |
| Power cut on either side of the flush's manifest rename (old or new manifest, orphan files present or lost, never a hybrid) | `nodus_storage_lsm`: `powercut_tests::power_cut_around_flush_publishes_old_or_new_manifest` |
| SSTable creation, WAL rotation, or manifest publication fails; acknowledged versions and tombstones remain readable through orphan cleanup, retry, WAL reclamation, and restart | `nodus_storage_lsm`: `flush_tests::failed_sstable_flush_preserves_rows_through_retry_and_restart`, `failed_wal_rotation_preserves_rows_through_retry_and_restart`, `failed_manifest_flush_preserves_rows_through_retry_and_restart` |
| Power cut on either side of the checkpoint's manifest rename (old or new snapshot in full) | `nodus_storage_lsm`: `powercut_tests::power_cut_around_checkpoint_publishes_old_or_new_snapshot` |
| Partial SSTable (`*.sst.tmp`) from a crash mid-build | `nodus_storage_lsm`: `partial_sstable_tmp_is_ignored_on_recovery` |
| Orphan SSTable absent from the manifest (crash before swap) | `nodus_storage_lsm`: `orphan_sstable_not_in_manifest_is_ignored` |
| Corrupt or torn existing manifest rejects startup without replacing it | `nodus_storage_lsm`: `corrupt_manifest_rejects_startup_without_replacing_manifest` |
| Manifest decoding/read failures preserve every file, reject orphan adoption, and recover WAL-only commits after repair | `nodus_storage_lsm`, integration test `manifest_recovery`: `corrupt_manifest_rejects_orphans_and_preserves_wal_only_commits`, `unreadable_manifest_rejects_startup_before_creating_files`, `dangling_manifest_symlink_is_not_a_missing_legacy_manifest` (Unix) |
| Fresh stores initialize, pre-manifest layouts migrate explicitly, and older manifests without a replay floor remain readable | `nodus_storage_lsm`, integration test `manifest_recovery`: `absent_manifest_initializes_empty_and_new_directories`, `pre_manifest_layout_recovers_and_publishes_authoritative_manifest`, `legacy_manifest_without_replay_floor_recovers_cross_segment_commits` |
| Missing manifest after writes, flush, compaction, or snapshot install preserves files and rejects startup; restoring the correct manifest recovers rows and snapshot metadata | `nodus_storage_lsm`, integration test `manifest_recovery`: `missing_manifest_after_writes_flush_compaction_or_snapshot_is_not_fresh`, `missing_manifest_with_only_partial_storage_files_is_not_fresh` |
| Explicit legacy migration validates files without repair, rejects modern WALs and ambiguous inventories, and preserves data on validation failures | `nodus_storage_lsm`, integration test `legacy_migration`: `explicit_migration_validates_plaintext_and_encrypted_legacy_stores`, `migration_validation_failures_preserve_every_file`, `wrong_or_missing_migration_key_preserves_encrypted_wal`, `migration_refuses_symlinked_data_files` (Unix) |
| Legacy manifest publication cannot overwrite a concurrent manifest; abrupt exit leaves no manifest or a complete one | `nodus_storage_lsm`: `legacy::tests::publication_does_not_overwrite_a_manifest_created_after_validation`, `abrupt_exit_around_publication_leaves_missing_or_complete_manifest` (process exit, not power loss) |
| Flush mid-transaction strands an intent | `nodus_storage_lsm`: `flush_retains_uncommitted_intents` |
| Archive cleanup while a commit spans WAL segments | `nodus_storage_lsm`: `archived_wal_reclamation_preserves_cross_segment_commits` |
| Cleanup after an unpublished or corrupt manifest | `nodus_storage_lsm`: `wal_reclamation_uses_published_manifest_and_fails_closed` |
| Compaction merges accumulated SSTables | `nodus_storage_lsm`: `compaction_merges_accumulated_sstables` |
| Recovery across flush and compaction (manifest-driven) | `nodus_storage_lsm`: `manifest_recovery_after_flush_and_compaction` |
| Unreadable or missing SSTable during a read | `nodus_storage_lsm`: `unavailable_sstable_fails_reads_instead_of_hiding_committed_rows` |
| Committed row and catalog survive a full restart | `tests/fault`: `committed_data_survives_a_restart` |
| Rolled-back writes never visible, before or after restart | `tests/fault`: `rolled_back_writes_are_never_visible` |
| Point-in-time restore replays archived WAL to a target time | `tests/integration`: `admin_backup_pitr_restore` and the persistent filesystem/S3 `restore_drill` scenarios |
| PITR applies each commit in a batch according to its own target-timestamp boundary | `nodus_backup`: `pitr_commit_batch_applies_each_transaction_at_the_target_boundary` |
| Late COPY failure rolls back an autocommit import, while explicit transaction ownership is preserved | `tests/integration`: `copy_in_late_constraint_failure_rolls_back_autocommit`, `copy_in_preserves_explicit_transaction_ownership` |
| Write batching handles indexed COPY and concurrent single-participant commits | `tests/integration`: `write_batching_handles_indexed_copy_and_concurrent_commits` |

A graceful server shutdown runs a final storage flush and closed-segment archive
pass before its archiver handle completes. This prevents acknowledged writes in
the active segment from remaining local-only during an orderly recovery drill.
It is not a distributed write-drain fence. An abrupt exit cannot perform that
pass; PITR coverage after a crash is bounded by the last successfully archived
segment.

## Batched writes and COPY

After `write-batching-v4` finalization, `EpochWriteBatchV1` carries one
transaction's put/delete intents for one Raft group and epoch. Each apply is
bounded to 256 operations and 1 MiB of key/value payload. These are still
intents, not committed versions. `EpochCommitBatchV1` instead carries independent
single-participant commits for one group and epoch. The router uses a maximum of
32 commits, a one-millisecond batching wait, and a 1,024-request queue; a full
queue uses the legacy single-commit command. Cross-shard 2PC retains its existing
protocol and is not grouped.

On persistent LSM, the resulting `CommitTxnBatchV1` is appended as one WAL record
and synced once before any of its transactions are published. The transactions
keep their own commit timestamps. PITR compares each timestamp independently
with the restore target rather than applying or skipping the whole record as a
unit. Unknown record versions and undecodable supported-version payloads in
complete, valid WAL frames fail recovery; unknown or malformed persisted Raft
log entries likewise fail store construction. This is distinct from a torn or
partial trailing frame, which recovery truncates at the last valid boundary.

WAL authentication failures and undecodable legacy records in complete,
CRC-valid frames also fail recovery without truncating or skipping bytes. This
includes opening an encrypted WAL with the wrong key or without a key, and
opening a plaintext WAL with encryption enabled. Errors identify the segment
path and frame byte offset. Verify the configured key and file integrity before
retrying; do not delete or truncate the log to bypass the error. After a failed
key check, reopening with the original key can replay the intact records.

This hardening changes no WAL bytes or record versions: supported versioned and
legacy records retain their readers, including the previous AES-GCM codec.
Recovery must run before concurrent appenders are admitted. Incomplete trailing
headers or payloads still truncate to the last valid record, and the existing
CRC-mismatch tail-repair policy remains unchanged. A CRC mismatch alone cannot
distinguish a torn unsynced append from damage to previously synced bytes; this
change does not add a durable synced-length watermark.

`COPY FROM STDIN` accumulates all wire chunks and executes their rows in one
executor batch. Outside an explicit transaction, that batch is one autocommit
transaction: a late decoding, planning, or constraint failure rolls back rows
from every chunk. Inside an existing user transaction, COPY joins that
transaction and leaves commit or rollback ownership with the user.

## Uncertain transaction intents

An intent's age and a node-local transaction map cannot establish that its
coordinator is dead or no longer permitted to commit. A remote transaction may
remain live beyond the configured threshold, and a successful prepare can
precede publication or local observation of its durable commit decision.

The janitor therefore performs inspection only. It classifies pending
transaction/group sets led by this node as locally live, visibly coordinated,
young, or old with uncertain ownership. It submits no abort commands. An
unreadable upgrade authority or undecodable 2PC record fails the sweep closed;
the existing rolling-upgrade pause remains. Explicit owner-driven rollback and
durable decided-commit recovery retain their replicated resolution paths.
Historical recorded-ID cleanup abort commands now fail closed, including on
replay: a physical transaction ID does not establish an authoritative abort.
Explicit legacy owner rollback continues to use the logical transaction ID.
All nodes that may lead a group must run this behavior: a patched coordinator
alone cannot protect its intents from an older remote janitor.

`txn.abandoned_intent_ttl_secs` keeps its existing name, default of 300 seconds,
positive-value validation, and environment override. It now sets the age at
which an unknown-owned intent is reported as uncertain, and continues to set
the inspection cadence (half the threshold, clamped to 1–30 seconds). Expiry
does not authorize abort. Inspection emits no transaction protocol records.
The [ownership and participant protocol](../explanation/transaction-authority.md)
activates only after `transaction-authority-v5` finalization with authenticated
readers on every voter and learner. Its bounded meta-leader worker replays only
terminal decisions and persists participant acknowledgements. Active, Preparing
and unknown legacy transactions remain protected from automatic abort. Levels
1–4 keep the legacy protocol. Logical backup/PITR excludes source transaction
fences and Raft-indexed recovery progress while retaining committed user data.

The latest successful sweep updates these Prometheus samples:

| Sample | Meaning |
| --- | --- |
| `nodus_pending_intents` | Observed pending transaction/group sets on groups led here; this counts sets, not individual keys. |
| `nodus_janitor_uncertain_transactions` | Old sets retained because no authoritative owner/decision resolution is available. |
| `nodus_janitor_aborts_total_total` | Existing historical abort counter, retained under its published sample name; inspection-only sweeps do not increase it. |

Sweeps log a summary when old uncertain sets remain. Metrics are observations,
not death proofs, and locally visible decisions can lag a remote coordinator.
True orphan cleanup is deferred: uncertain intents may indefinitely block
writers, migration fencing, MVCC retention progress, or legacy snapshot builds
and log reclamation. Fencing must remain blocked until resolution is known;
shortening the threshold is not a safe cleanup mechanism. Automatic cleanup
requires a durable cluster-wide ownership/abort-decision authority that fences
late commits. The tracked ownership protocol is available at level 5, but automatic orphan
abort and bounded decision reclamation remain follow-up work. The two-node tests prove the targeted safety regressions, not general
distributed isolation, process-crash recovery of undecided transactions, or
power-loss behavior.

## Required WAL chain

For an existing manifest, the active WAL must already exist. Startup does not
create an empty replacement if it is missing, including when a dangling symlink
names a missing target. Missing required predecessors fail recovery too. Errors
identify the failing WAL or manifest and preserve all files during validation.
Restore the matching missing segment from a verified source or restore a
consistent backup; do not manufacture an empty log to bypass the error.

Startup scans the complete required chain read-only before opening the active
WAL for append or repairing any tail. Active ids are positive, file-id arithmetic
must not overflow, and the replay floor cannot exceed the active id. A segment
header, when present, must be its first and only header. Predecessor ids must be
positive and strictly decrease; cycles, forward/self links, premature roots and
links that skip a positive required replay floor are errors.

A positive `min_replay_wal` names the oldest required segment, inclusive. Each
required segment must have its header, including the floor segment, and the
chain must reach that exact id. Its predecessor may be below the floor and need
not exist, so legitimate archived-WAL reclamation remains supported. With floor
zero (including older manifests without the field), recovery follows declared
links to a root. Headerless roots and explicit headers with no predecessor remain
supported. Unpublished WALs outside this chain are not read or replayed. Their
numeric filenames, and numeric final/partial SSTable filenames, reserve file ids
so restart cannot reuse an orphan output. New WALs are created exclusively;
allocation fails instead of wrapping ids or appending a second segment header
to an existing file. Strict validation can reject malformed WALs that older
binaries accepted; restore a verified copy/backup rather than editing headers
or using legacy migration to bypass that error.

Only after every required segment and link passes validation may recovery
truncate incomplete or checksum-failing tails and replay records from oldest to
newest. A later missing/undecodable/unauthentic segment therefore cannot cause
an earlier tail to be changed. The scan's repair plan refuses changed file
lengths and never creates files. Tail-repair I/O errors can still leave an
uncertain repair outcome; repairs across multiple files are not transactional.
Exclude all concurrent writers and filesystem changes throughout startup:
the plan is not an inter-process lock or a check against same-length changes.

This changes no WAL records, manifest fields, or format versions. The existing
CRC tail policy remains unchanged. Without durable synced-length or format-mode
metadata, arbitrary deletion at valid record boundaries (including loss of a
header in an otherwise valid historical-looking root) cannot always be detected.

## Manifest recovery

An existing `MANIFEST` is authoritative. Startup must read and decode it before
opening or replaying any WAL. A read or decoding failure rejects startup with
the manifest path and an error cause, preserving the manifest, SSTables, and WAL
files. This includes an empty/truncated manifest, invalid JSON, missing required
fields, and a dangling manifest symlink. Recovery does not adopt an orphan
SSTable, guess a different active WAL, or overwrite the failed manifest.

Repair access to the manifest or restore a known-good manifest matching these
data files; otherwise restore a consistent backup. Do not delete `MANIFEST` to
bypass the error. With the correct manifest restored, recovery uses its file set
and replays the required WAL lineage, including commits not yet flushed into an
SSTable. This change introduces no new manifest fields or durable format.

An absent manifest permits initialization only when no storage artifacts exist.
Numeric WAL files, final or partial SSTables, and manifest publication residue
reject startup without creating a replacement manifest or repairing a WAL.
Unrelated files and subdirectories do not count as LSM storage artifacts.
Even an empty existing `1.log` requires explicit handling: it could be residue
from interrupted initialization. Older manifests lacking `min_replay_wal` still
default to replay floor zero. The on-disk format is unchanged.

Automatic pre-manifest discovery is removed. For a verified historical layout,
use the [offline legacy migration procedure](../how-to/migrate-legacy-storage.md).
The operator explicitly names every final SSTable and the active WAL; all final
SSTables must be listed, the active WAL must be `max_sst_id + 1` (or `1.log`
without SSTables), and it must already exist. Every numeric WAL is inspected
read-only. Incomplete/checksum-failing frames, authentication/decoding failures,
modern segment headers or commit batches, symlinked data files, and manifest
publication residue reject migration before any writes. SSTable iteration and
indexed lookup must validate. Partial `*.sst.tmp` files remain untouched and
are excluded from the manifest.

Only after validation does migration write and sync `LEGACY_MANIFEST.tmp`,
publish it through a no-overwrite hard link to `MANIFEST`, sync the directory,
then remove the staging link and sync again. Validation failures preserve every
file. Publication I/O failures or process exit can leave a staging file or a
complete manifest; no existing manifest is replaced. This requires a filesystem
supporting hard links and directory sync. Stop the node and exclude all writers:
the offline command does not provide inter-process locking. Filesystem access
controls authorize this local operation; no HTTP admin token is used.

This procedure does not reconstruct a lost modern manifest or prove that a
caller-supplied historical inventory contains all committed data. Modern WAL
markers are rejected, but an unrotated modern WAL can be indistinguishable from
a historical one. Use deployment provenance to establish a genuine legacy
store, and restore a consistent manifest/backup for modern manifest loss.

## Local WAL archive cleanup

The server may remove an archived local WAL segment only after backup retention
permits cleanup **and** the LSM's published manifest places it below the local
replay floor. The storage engine checks and removes it while serialized with
checkpoint publication. Archiving alone does not make a segment dispensable:
an intent retained during a flush can still require an earlier segment on restart.
Invalid or unavailable checkpoint metadata prevents cleanup. This uses the
existing manifest format and does not require upgrade finalization.

## Raft log purge

Prefix purge writes at most 256 log tombstones and the corresponding applied-state
record (including `last_purged`) in one KV transaction per batch. The WAL commit
is synced before that batch updates the cached log and watermark. A crash between
batches therefore leaves a shorter, consistent purged prefix; surviving applied
entries, membership, vote and the unpurged tail remain recoverable. The existing
v1 record encoding is unchanged, so this needs no upgrade finalization.

Deletion, watermark-write and commit errors reach Raft as storage errors. An
uncertain commit outcome requires recovery; it is never reported as successful.
The blocking task owns the log and state-machine locks through publication and
cache updates, even if its async caller is cancelled. Other Tokio tasks can run,
but this group's apply/log operations still wait for the purge to finish. The
`Raft log prefix purged` trace records group, target, entry/batch counts and elapsed
milliseconds; failures include the target and storage error.

`nodus_raftstore::purge::tests` covers persistent reopen, partial progress after
injected errors, ambiguous commit results, cancellation on a single-worker runtime,
and subprocess exit before/after the second batch's commit without running
destructors. It inspects persisted records before constructor reconciliation.
These subprocess tests demonstrate process-exit recovery, not power-loss behavior.

Before a group restarts Raft, construction validates its committed log, vote,
applied state and snapshot descriptor, then discards unfinished local metadata
transactions confined to `\0raft\0`. A transaction containing both metadata and
user keys fails recovery without being discarded. This prevents an interrupted
metadata write from locking the next applied-state update. It requires exclusive
ownership of the group's storage with old Raft workers stopped, and supported,
successful pending-intent inspection. It never infers death from age or aborts
user, transaction-authority or participant-control intents. Persistent tests in
`nodus_raftstore::control_recovery::tests` cover raw and namespaced stores, mixed-key
rejection, and preservation of user intents; the tracked-transaction process-exit
tests exercise this during live Raft replication.

Constructor reconciliation (`reconcile_torn_recovery`) handles a crash that
durably keeps the applied-state pointer but loses log entries it refers to: the
applied prefix is reported as purged and stale entries are dropped so openraft
never reads a compacted index. If that reconciled watermark is newer than the
durable snapshot descriptor, recovery drops the descriptor and its file before
openraft starts — openraft would otherwise reject
`purge_upto > snapshot_last_log_id` with a startup panic, and with no
descriptor it rebuilds the snapshot from the durable state machine through the
normal gated builder. `nodus_raftstore::tests` covers the descriptor drop, the
preservation of a snapshot that still covers the watermark, and a full
`Raft::new` startup over the repaired state.

Run the local persistent comparison with:

```sh
cargo test -p nodus_raftstore purge::tests::benchmark_purge_commits --locked -- --ignored --nocapture
```

Two local macOS debug-build runs on September 14, 2026 purged 4,000 entries in
128–137 ms with batching (16 commits), versus 16.30–16.36 seconds with per-entry
commits. Setup is excluded; both paths verify the recovered prefix and tail.
These are local comparisons, not a release-build or cluster latency guarantee.

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
| No general cross-version on-disk compatibility guarantee | Upgrading across hardening changes (WAL CRC frame, SSTable v2, catalog-in-KV) requires a clean restart except for explicitly verified maintenance/candidate pairs. The historical mixed-binary evidence in [the upgrade gate](../how-to/test-mixed-binary-upgrades.md), including the October 25, 2026 run at commit `c3f31f942b32afe22143d52e3e796728ba738df3`, qualifies only its recorded sources. That workload did not activate write batching and is not current-candidate evidence for `CommitTxnBatchV1` recovery or executable downgrade. |
| Catalog persistence writes a full-state blob per DDL | Catalog writes are not incremental per entry. |
| WAL retention has two boundaries | Local cleanup waits for both backup retention and the durable local replay floor; retained intents or restore points can delay reclamation. |
| General runtime error injection is not wired into the storage engines | Power loss is simulated by cutting files back to their fsynced prefixes (`powercut_tests`, above), which proves the fsync discipline under a device that honors fsync. It does not prove real device behavior: volatile write caches, drives or filesystems that lie about fsync, and fsync error handling on subsequent writes are out of scope for in-process tests. The `FaultPoint`/`FaultAction` injector in `nodus_testkit` remains a registry without production call sites. |
| Single-node scope | This contract covers local durability. Replication and cross-node durability are properties of the Raft layer, not of this contract. |
