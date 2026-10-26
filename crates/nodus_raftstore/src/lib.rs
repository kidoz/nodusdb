// `openraft` fixes `StorageError<u64>` (224 bytes) as the error type of every
// storage callback, and the snapshot helpers below feed those callbacks
// directly, so boxing it here would only move the cost to each trait boundary.
#![allow(clippy::result_large_err)]

pub mod upgrade;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::SeekFrom;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::RwLock;

use bytes::Bytes;
use nodus_storage_api::{KeyRange, KvEngine, KvError, KvResult, TxnId};
use openraft::AnyError;

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

pub mod backup;
pub mod migration;
pub mod network;
mod purge;
pub mod server;
mod snapshots;
pub use snapshots::v2::SnapshotCompatibility;

/// Shared, cheap-to-clone tracker for a group's snapshot-build deferrals.
/// `build_snapshot` records every deferral here; the owning
/// `MultiRaftManager` keeps one handle per group so the node's metrics loop
/// can expose per-group deferral counts and how long the current unbroken
/// deferral streak has lasted (pending intents or retained MVCC history block
/// legacy snapshot builds, and while deferred the Raft log cannot be purged).
#[derive(Clone, Default)]
pub struct SnapshotDeferral {
    inner: Arc<std::sync::Mutex<Option<DeferralState>>>,
}

#[derive(Debug)]
struct DeferralState {
    /// When the current unbroken deferral streak began.
    since: std::time::Instant,
    /// When the most recent deferral happened (drives the retry throttle).
    last: std::time::Instant,
    /// Deferrals in the current streak.
    count: u64,
    reason: &'static str,
}

/// A point-in-time view of a group's ongoing deferral streak.
#[derive(Debug, Clone)]
pub struct DeferralSnapshot {
    pub since: std::time::Instant,
    pub count: u64,
    pub reason: &'static str,
}

impl SnapshotDeferral {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<DeferralState>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn record(&self, reason: &'static str) {
        let now = std::time::Instant::now();
        let mut guard = self.lock();
        match &mut *guard {
            Some(state) => {
                state.last = now;
                state.count += 1;
                state.reason = reason;
            }
            None => {
                *guard = Some(DeferralState {
                    since: now,
                    last: now,
                    count: 1,
                    reason,
                });
            }
        }
    }

    pub(crate) fn clear(&self) {
        *self.lock() = None;
    }

    /// Whether a build was deferred within `within`, so the next build reports
    /// no progress without rescanning the whole state machine.
    pub(crate) fn recently_deferred(&self, within: std::time::Duration) -> bool {
        self.lock()
            .as_ref()
            .is_some_and(|state| state.last.elapsed() < within)
    }

    /// The current deferral streak, if any.
    pub fn current(&self) -> Option<DeferralSnapshot> {
        self.lock().as_ref().map(|state| DeferralSnapshot {
            since: state.since,
            count: state.count,
            reason: state.reason,
        })
    }
}

openraft::declare_raft_types!(
    /// Declare the type configuration for `openraft`.
    #[derive(serde::Serialize, serde::Deserialize, Hash)]
    pub NodusTypeConfig:
        D = ShardCommand,
        R = ShardResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<NodusTypeConfig>,
        SnapshotData = tokio::fs::File,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Magic + format version prefixing a streamed snapshot file, so a reader can
/// reject an unrecognized format rather than misparsing it.
const SNAPSHOT_MAGIC: &[u8; 4] = b"NSNP";
const SNAPSHOT_FORMAT_VERSION: u16 = 1;

fn snapshot_io_err(context: &str, err: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::write_snapshot(None, AnyError::error(format!("{context}: {err}"))).into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ShardCommand {
    PutIntent {
        txn_id: String,
        key: Vec<u8>,
        value: Vec<u8>,
        /// Data shard this write was routed to (`None` = the meta group). Recorded
        /// for observability and forward use (cross-shard commit, data movement);
        /// the applying group already owns the correct (namespaced) engine.
        #[serde(default)]
        shard_id: Option<String>,
    },
    CommitTxn {
        txn_id: String,
        commit_ts: u64,
        #[serde(default)]
        shard_id: Option<String>,
    },
    AbortTxn {
        txn_id: String,
        #[serde(default)]
        shard_id: Option<String>,
        /// `true` when `txn_id` is the id as RECORDED in this group's store
        /// (the intent janitor's orphan aborts — the only id recoverable from
        /// a raw store scan after a restart), bypassing the namespace id
        /// mapping a session's abort applies. Defaults to `false`, so older
        /// binaries parse the command unchanged (they apply it as a session
        /// abort, a benign no-op for recorded ids, and the janitor refrains
        /// from submitting these while a rolling upgrade is in flight).
        #[serde(default)]
        recorded: bool,
    },
    /// Two-phase-commit prepare vote for a cross-shard transaction. The intents
    /// were already durably replicated by their `PutIntent`s; applying this
    /// confirms the participant's leader is current and ready to commit.
    PrepareTxn {
        txn_id: String,
        #[serde(default)]
        shard_id: Option<String>,
    },
    IndexPutIntent {
        txn_id: String,
        index_id: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    IndexDeleteIntent {
        txn_id: String,
        index_id: String,
        key: Vec<u8>,
    },
    DeleteIntent {
        txn_id: String,
        key: Vec<u8>,
        #[serde(default)]
        shard_id: Option<String>,
    },
    SplitShard {
        split_key: Vec<u8>,
    },
    InstallSnapshot {
        snapshot_id: String,
    },
    // Cluster-metadata replication (applied on the meta group so every node's
    // local `MetaStore` agrees on routing and shard placement).
    UpdateShardMap(nodus_meta::ShardMap),
    UpdateShardPlacements(std::collections::HashMap<nodus_catalog::ShardId, String>),
    // Catalog Replication Commands
    CreateDatabase(nodus_catalog::CreateDatabaseRequest),
    CreateSchema(nodus_catalog::CreateSchemaRequest),
    CreateTable(nodus_catalog::CreateTableRequest),
    DropTable(nodus_catalog::TableId),
    DropSchema(nodus_catalog::SchemaId),
    SetSchemaOwner {
        id: nodus_catalog::SchemaId,
        owner_role_id: Option<nodus_catalog::RoleId>,
    },
    GrantPrivileges(nodus_catalog::GrantPrivilegesRequest),
    RevokePrivileges(nodus_catalog::RevokePrivilegesRequest),
    UpdateTableDescriptor(nodus_catalog::TableDescriptorChange),
    CreateRole(nodus_catalog::CreateRoleRequest),
    GrantPrivilege(nodus_catalog::GrantPrivilegeRequest),
    RevokePrivilege(nodus_catalog::RevokePrivilegeRequest),
    AddRoleMember(nodus_catalog::AddRoleMemberRequest),
    RemoveRoleMember(nodus_catalog::RemoveRoleMemberRequest),
    UpdatePrincipal(nodus_catalog::UpdatePrincipalRequest),
    DropPrincipal(nodus_catalog::PrincipalId),
    UpdateIndexState {
        table_id: nodus_catalog::TableId,
        index_id: nodus_catalog::IndexId,
        state: nodus_catalog::IndexState,
    },
    /// Replicated catalog restore: replaces catalog state from a snapshot. Routed
    /// through Raft so a cluster restore reaches every node's catalog rather than
    /// only the node that served the request (which would split-brain the
    /// replicated catalog).
    ImportCatalogSnapshot(nodus_catalog::CatalogSnapshot),
    // Upgrade Replication Commands
    UpgradeStart {
        target_version: String,
    },
    UpgradeNodeUpgraded {
        node_id: String,
    },
    UpgradeFinalize,
    UpgradeRollback,
    /// Dormant R3 protocol; client ingress rejects activation until every member
    /// can enforce the versioned format. Appended to preserve legacy variants.
    MigrationV1(migration::MigrationCommandV1),
    EpochWriteV1 {
        epoch: u64,
        mutation: migration::EpochMutationV1,
    },
    MigrationV2(migration::coordinator::CommandV2),
    EpochPrepareV2 {
        epoch: u64,
        txn_id: uuid::Uuid,
        expect_intents: bool,
    },
    EpochRepairV2 {
        epoch: u64,
        txn_id: uuid::Uuid,
        key: Vec<u8>,
        replacement: migration::writes::ReplacementV2,
    },
    UpgradeControlV1(upgrade::CommandV1),
    UpgradeAdmissionV1(upgrade::admission::CommandV1),
    /// Dormant V3 copy/cutover journal; same activation fences as V1/V2.
    MigrationV3(migration::coordinator::v3::CommandV3),
    /// Cluster backup job journal on the meta group; same activation fence as
    /// the migration protocol (the level-3 lineage contains both decodes).
    BackupJobV1(backup::CommandV1),
    /// Conditional routing publication: applies only if the current durable
    /// shard-map content equals `expected` and the V3 journal for `table` is at
    /// the `phase` step owned by `operation`. Old binaries fail closed on
    /// decode; level-3 gating keeps them out of a cluster that emits these.
    UpdateShardMapConditional {
        table: nodus_catalog::TableId,
        operation: uuid::Uuid,
        phase: migration::coordinator::v3::PhaseV3,
        expected: Option<nodus_meta::ShardMap>,
        map: nodus_meta::ShardMap,
    },
    /// Conditional placements publication; same discipline as
    /// [`ShardCommand::UpdateShardMapConditional`].
    UpdateShardPlacementsConditional {
        table: nodus_catalog::TableId,
        operation: uuid::Uuid,
        phase: migration::coordinator::v3::PhaseV3,
        expected: std::collections::HashMap<nodus_catalog::ShardId, String>,
        placements: std::collections::HashMap<nodus_catalog::ShardId, String>,
    },
    EpochWriteBatchV1 {
        epoch: u64,
        txn_id: uuid::Uuid,
        operations: Vec<migration::EpochIntentOperationV1>,
    },
    EpochCommitBatchV1 {
        epoch: u64,
        commits: Vec<migration::EpochCommitV1>,
    },
}

impl ShardCommand {
    /// Fixed, low-cardinality command kinds for metrics and tracing.
    pub fn metrics_kind(&self) -> &'static str {
        match self {
            Self::EpochWriteBatchV1 { .. } => "intent_batch",
            Self::EpochCommitBatchV1 { .. } => "commit_batch",
            Self::EpochWriteV1 { mutation, .. } => match mutation {
                migration::EpochMutationV1::Put { .. } => "put_intent",
                migration::EpochMutationV1::Delete { .. } => "delete_intent",
                migration::EpochMutationV1::Prepare { .. } => "prepare",
                migration::EpochMutationV1::Commit { .. } => "commit",
            },
            Self::PutIntent { .. } | Self::IndexPutIntent { .. } => "put_intent",
            Self::DeleteIntent { .. } | Self::IndexDeleteIntent { .. } => "delete_intent",
            Self::CommitTxn { .. } => "commit",
            Self::AbortTxn { .. } => "abort",
            Self::PrepareTxn { .. } | Self::EpochPrepareV2 { .. } => "prepare",
            Self::EpochRepairV2 { .. } => "repair",
            _ => "other",
        }
    }

    pub fn is_upgrade_command(&self) -> bool {
        matches!(
            self,
            Self::UpgradeControlV1(_)
                | Self::UpgradeAdmissionV1(_)
                | Self::UpgradeStart { .. }
                | Self::UpgradeNodeUpgraded { .. }
                | Self::UpgradeFinalize
                | Self::UpgradeRollback
        )
    }

    pub fn requires_migration_protocol(&self) -> bool {
        matches!(
            self,
            Self::MigrationV1(_)
                | Self::EpochWriteV1 { .. }
                | Self::EpochWriteBatchV1 { .. }
                | Self::EpochCommitBatchV1 { .. }
                | Self::MigrationV2(_)
                | Self::EpochRepairV2 { .. }
                | Self::EpochPrepareV2 { .. }
                | Self::MigrationV3(_)
                | Self::BackupJobV1(_)
                | Self::UpdateShardMapConditional { .. }
                | Self::UpdateShardPlacementsConditional { .. }
        )
    }

    pub fn requires_write_batch_protocol(&self) -> bool {
        matches!(
            self,
            Self::EpochWriteBatchV1 { .. } | Self::EpochCommitBatchV1 { .. }
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardResponse {
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The leader's read log index, returned by the `/raft/{group}/read_index`
/// endpoint. A follower serving a linearizable read asks the leader for this
/// index (the leader confirms its leadership via a quorum heartbeat first), then
/// waits for its own state machine to apply at least this far before reading.
/// `None` means the leader's log is empty, so there is nothing to wait for.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadIndexResponse {
    pub index: Option<u64>,
}

/// Classifies a state-machine KV apply result so a real failure is never
/// silently dropped. A committed Raft entry whose KV effect fails to apply is a
/// durability hazard: the entry sits below the applied watermark yet isn't
/// reflected in the store.
///
/// `IntentNotFound` is the *benign, idempotent* outcome of re-applying a commit
/// or abort whose intents were already consumed (post-crash log replay above the
/// last durably-persisted applied index, or a re-driven 2PC commit); it is
/// logged at debug and tolerated.
///
/// `WriteConflict` means another transaction holds the key's intent, so the write
/// was **not** applied. It is returned as a rejection reason for the entry's
/// response: acknowledging it would silently lose the write, and every later
/// writer of the key could be lost the same way. The store is unchanged either
/// way, so replay stays deterministic, and a replayed entry's response is
/// discarded.
///
/// Any other (storage/I/O) failure is **fatal**: it is returned as a
/// `StorageError` so openraft halts apply rather than advancing the applied
/// watermark past an entry whose effect was lost.
// `StorageError` is openraft's own (large) error that the whole `RaftStorage`
// trait returns; this helper feeds straight into those methods via `?`, so
// boxing it here would just force an unbox at every call site.
#[allow(clippy::result_large_err)]
fn apply_kv_result(
    command: &str,
    txn_id: &str,
    result: KvResult<()>,
) -> Result<Option<String>, StorageError<u64>> {
    match result {
        Ok(()) => Ok(None),
        Err(e @ KvError::IntentNotFound(_)) => {
            tracing::debug!(
                "Raft apply {command} (txn {txn_id}): benign idempotent outcome (likely replay): {e}"
            );
            Ok(None)
        }
        Err(e @ KvError::WriteConflict(_)) => {
            tracing::debug!("Raft apply {command} (txn {txn_id}) rejected: {e}");
            Ok(Some("write conflict; retry transaction".into()))
        }
        Err(e) => {
            tracing::error!("Raft apply {command} (txn {txn_id}) KV op failed fatally: {e}");
            Err(StorageIOError::write_state_machine(AnyError::error(format!(
                "apply {command} (txn {txn_id}): {e}"
            )))
            .into())
        }
    }
}

/// Logs a catalog-apply failure. State-machine apply must be idempotent: log
/// replay on restart and the non-idempotent bootstrap both re-issue create
/// commands. A replay is expected only when the catalog already has the
/// descriptor ID carried by the Raft command; duplicate names with different
/// IDs remain real apply failures.
fn log_create_apply_error(command: &str, err: &anyhow::Error, already_applied: bool) {
    if already_applied {
        tracing::debug!("{} already applied: {}", command, err);
    } else {
        tracing::error!("{} apply failed: {}", command, err);
    }
}

fn database_create_already_applied(
    reader: Option<&Arc<dyn nodus_catalog::CatalogReader>>,
    req: &nodus_catalog::CreateDatabaseRequest,
) -> bool {
    reader
        .and_then(|reader| reader.get_database_by_id(req.id).ok())
        .is_some_and(|database| database.name == req.name)
}

fn schema_create_already_applied(
    reader: Option<&Arc<dyn nodus_catalog::CatalogReader>>,
    req: &nodus_catalog::CreateSchemaRequest,
) -> bool {
    reader
        .and_then(|reader| reader.get_schema_by_id(req.id).ok())
        .is_some_and(|schema| schema.database_id == req.database_id && schema.name == req.name)
}

fn table_create_already_applied(
    reader: Option<&Arc<dyn nodus_catalog::CatalogReader>>,
    req: &nodus_catalog::CreateTableRequest,
) -> bool {
    reader
        .and_then(|reader| reader.get_table_by_id(req.id).ok())
        .is_some_and(|table| {
            table.database_id == req.database_id
                && table.schema_id == req.schema_id
                && table.name == req.name
        })
}

// Reserved keys under which a group's Raft consensus state is persisted into its
// `KvEngine`. The leading `\0` sorts them before any user/catalog/2PC key, and
// Snapshots exclude node-local Raft records while retaining replicated metadata.
const RAFT_VOTE_KEY: &[u8] = b"\x00raft\x00vote";
const RAFT_APPLIED_KEY: &[u8] = b"\x00raft\x00applied";
const RAFT_LOG_PREFIX: &[u8] = b"\x00raft\x00log\x00";
const RAFT_LOG_END: &[u8] = b"\x00raft\x00log\x01";
/// KV key holding the *metadata* of the current snapshot. The snapshot bytes
/// live off-heap in `{snapshot_dir}/current.snap`; only this small descriptor is
/// kept in the engine, so a restarted node can resume from the snapshot (and the
/// log purged below it) instead of replaying the whole log.
const RAFT_SNAPSHOT_META_KEY: &[u8] = b"\x00raft\x00snapshot_meta";

/// File name of the durable current snapshot within a group's snapshot dir.
const CURRENT_SNAPSHOT_FILE: &str = "current.snap";

/// Scratch file openraft streams an incoming snapshot's chunks into before
/// install. Only one snapshot is received at a time per group, so a fixed name
/// is safe; it is reused (truncated) on each receive.
const RECV_SNAPSHOT_FILE: &str = "recv.tmp";

type NodusSnapshotMeta = SnapshotMeta<u64, openraft::BasicNode>;

fn log_key(index: u64) -> Vec<u8> {
    let mut key = RAFT_LOG_PREFIX.to_vec();
    key.extend_from_slice(&index.to_be_bytes()); // big-endian: keys sort by index
    key
}

/// The applied-state pointer persisted alongside the log: how far the state
/// machine has applied, the membership at that point, and the purge watermark.
#[derive(Serialize, Deserialize, Default, Clone)]
struct AppliedState {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, openraft::BasicNode>,
    last_purged: Option<LogId<u64>>,
}

/// Repairs a torn recovery so openraft sees a self-consistent log state.
///
/// Log entries, `last_applied`, and `last_purged` are persisted as independent
/// KV records, so a crash can durably retain an inconsistent subset — most
/// notably the applied-state pointer surviving while the log entries it refers
/// to do not. openraft's startup (`Raft::new` → `get_initial_state`) requires
/// `last_applied <= last_log_id` with every index in `(last_purged, last_log_id]`
/// present in the log; a torn state otherwise makes it read a missing entry and
/// abort with *"try to get log at index N but got None"*.
///
/// The state machine's data is durable independently of the Raft log, so every
/// entry at or below `last_applied` is already captured there. When the log is
/// missing part of that applied prefix, report the prefix as purged (raise
/// `last_purged` to `last_applied`) and drop any stale cached entries at or
/// below it — yielding a consistent view that serves applied state from the
/// snapshot/state machine. A healthy recovery (the applied prefix fully present
/// in the log) is left untouched, preserving the log for follower replication.
///
/// Returns `true` when it changed the recovered state.
fn reconcile_torn_recovery(
    log: &mut BTreeMap<u64, Entry<NodusTypeConfig>>,
    applied: &mut AppliedState,
) -> bool {
    let Some(applied_id) = applied.last_applied else {
        return false;
    };
    let purged_idx = applied.last_purged.map(|l| l.index).unwrap_or(0);
    let expected = applied_id.index.saturating_sub(purged_idx);
    if expected == 0 {
        return false; // No retained applied prefix to validate.
    }
    let present = log.range(purged_idx + 1..=applied_id.index).count() as u64;
    if present == expected && log.get(&applied_id.index).map(|e| e.log_id) == Some(applied_id) {
        return false; // Applied prefix intact — healthy recovery.
    }
    applied.last_purged = Some(applied_id);
    log.retain(|index, _| *index > applied_id.index);
    true
}

/// Persists Raft vote, log entries, and applied-state into a `KvEngine` so a
/// node's consensus state survives restart. Commit timestamps are wall-clock
/// monotonic (`max(now, last + 1)`) so the newest write always wins even after a
/// restart resets the in-memory counter — the same scheme the catalog store uses.
struct RaftMetaStore {
    kv: Arc<dyn KvEngine>,
    last_ts: AtomicU64,
}

impl RaftMetaStore {
    fn new(kv: Arc<dyn KvEngine>) -> Self {
        Self {
            kv,
            last_ts: AtomicU64::new(0),
        }
    }

    fn next_ts(&self) -> u64 {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        loop {
            let last = self.last_ts.load(Ordering::SeqCst);
            let next = wall.max(last + 1);
            if self
                .last_ts
                .compare_exchange(last, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return next;
            }
        }
    }

    fn put_checked(&self, key: &[u8], value: Vec<u8>) -> anyhow::Result<()> {
        let txn = TxnId::new();
        let result = (|| {
            self.kv
                .write_intent(txn, Bytes::copy_from_slice(key), Bytes::from(value))?;
            self.kv.commit(txn, self.next_ts())?;
            Ok(())
        })();
        if result.is_err() {
            let _ = self.kv.abort(txn);
        }
        result
    }

    fn delete_checked(&self, key: &[u8]) -> anyhow::Result<()> {
        let txn = TxnId::new();
        let result = (|| {
            self.kv.delete_intent(txn, Bytes::copy_from_slice(key))?;
            self.kv.commit(txn, self.next_ts())?;
            Ok(())
        })();
        if result.is_err() {
            let _ = self.kv.abort(txn);
        }
        result
    }

    /// Atomically persists several records in one transaction (one durable
    /// commit), so a multi-record write is all-or-nothing across a crash and
    /// costs a single fsync instead of one per record. Used for a batch of
    /// appended log entries: a partial batch can never survive recovery.
    /// Failures are reported — never swallowed — so a caller cannot ack a
    /// write that was not durably stored.
    fn put_batch(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> anyhow::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let txn = TxnId::new();
        let result = (|| {
            for (key, value) in items {
                self.kv
                    .write_intent(txn, Bytes::from(key), Bytes::from(value))?;
            }
            self.kv.commit(txn, self.next_ts())?;
            Ok(())
        })();
        if result.is_err() {
            // A failed commit can have an uncertain durable outcome. Report it
            // to Raft even if abort succeeds; recovery is authoritative.
            let _ = self.kv.abort(txn);
        }
        result
    }

    /// Persists a batch of log entries atomically (see [`Self::put_batch`]).
    fn append_entries(&self, entries: &[Entry<NodusTypeConfig>]) -> anyhow::Result<()> {
        let items: Vec<(Vec<u8>, Vec<u8>)> = entries
            .iter()
            .map(|entry| {
                serde_json::to_vec(entry)
                    .map(|bytes| (log_key(entry.log_id.index), encode_raft(bytes)))
                    .map_err(anyhow::Error::from)
            })
            .collect::<anyhow::Result<_>>()?;
        self.put_batch(items)
    }

    fn save_vote(&self, vote: &Vote<u64>) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(vote)?;
        self.put_checked(RAFT_VOTE_KEY, encode_raft(bytes))
    }

    /// Reads the persisted vote. Distinguishes a genuine first boot (key
    /// absent) from a storage read or decode failure: the error is propagated so
    /// the caller fails closed. Dropping a persisted vote would let a restarted
    /// node vote again in the same term — a double-vote election-safety
    /// violation.
    fn load_vote(&self) -> anyhow::Result<Option<Vote<u64>>> {
        self.kv
            .get(RAFT_VOTE_KEY, u64::MAX)?
            .map(|bytes| decode_raft_json(&bytes, "vote"))
            .transpose()
    }

    fn delete_entry(&self, index: u64) -> anyhow::Result<()> {
        self.delete_checked(&log_key(index))
    }

    /// Reads the persisted log. A scan failure or an unreadable record is
    /// propagated (fail closed): silently dropping the log tail would invite
    /// re-application of committed entries or loss of unreplicated ones.
    fn load_log(&self) -> anyhow::Result<BTreeMap<u64, Entry<NodusTypeConfig>>> {
        let mut out = BTreeMap::new();
        let range = KeyRange {
            start: Bytes::from_static(RAFT_LOG_PREFIX),
            end: Bytes::from_static(RAFT_LOG_END),
        };
        for pair in self.kv.scan(range, u64::MAX)? {
            let pair = pair?;
            let payload = decode_raft(&pair.value).ok_or_else(|| {
                anyhow::anyhow!(
                    "unsupported persisted Raft log envelope at key {:?}",
                    pair.key
                )
            })?;
            let entry = serde_json::from_slice::<Entry<NodusTypeConfig>>(payload).map_err(|e| {
                anyhow::anyhow!("decode persisted Raft log entry at key {:?}: {e}", pair.key)
            })?;
            out.insert(entry.log_id.index, entry);
        }
        Ok(out)
    }

    fn save_applied(&self, state: &AppliedState) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(state)?;
        self.put_checked(RAFT_APPLIED_KEY, encode_raft(bytes))
    }

    /// Reads the persisted applied-state (applied pointer, membership, purge
    /// watermark). Absence means a first boot (default state); storage read and
    /// decode failures propagate so recovery cannot silently restart from zero
    /// and re-apply or lose committed state.
    fn load_applied(&self) -> anyhow::Result<AppliedState> {
        self.kv
            .get(RAFT_APPLIED_KEY, u64::MAX)?
            .map(|bytes| decode_raft_json(&bytes, "applied state"))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    /// Reads the persisted snapshot metadata. Absence means no snapshot was
    /// ever built; storage read and decode failures propagate so a restarted
    /// node does not wrongly conclude it has no snapshot to resume from.
    fn load_snapshot_meta(&self) -> anyhow::Result<Option<NodusSnapshotMeta>> {
        self.kv
            .get(RAFT_SNAPSHOT_META_KEY, u64::MAX)?
            .map(|bytes| decode_raft_json(&bytes, "snapshot metadata"))
            .transpose()
    }
}

/// On-disk format version of persisted Raft records (vote, log entries, applied
/// state) and the snapshot payload.
const RAFT_RECORD_VERSION: u16 = 1;

/// Wraps a serialized Raft record in the versioned envelope for storage.
fn encode_raft(payload: Vec<u8>) -> Vec<u8> {
    nodus_common::versioned::encode(RAFT_RECORD_VERSION, &payload)
}

/// Returns the payload of a persisted Raft record for a supported version, or
/// legacy (pre-envelope) bytes. Returns `None` for an unrecognized future
/// version so each caller can apply its own fail-closed recovery policy.
fn decode_raft(bytes: &[u8]) -> Option<&[u8]> {
    use nodus_common::versioned::{Envelope, decode};
    match decode(bytes) {
        Envelope::Versioned { version, payload } if version == RAFT_RECORD_VERSION => Some(payload),
        Envelope::Versioned { .. } => None,
        Envelope::Legacy(legacy) => Some(legacy),
    }
}

fn decode_raft_json<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    record: &str,
) -> anyhow::Result<T> {
    let payload = decode_raft(bytes)
        .ok_or_else(|| anyhow::anyhow!("unsupported persisted Raft {record} envelope"))?;
    serde_json::from_slice(payload)
        .map_err(|e| anyhow::anyhow!("decode persisted Raft {record}: {e}"))
}

pub struct StateMachine {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, openraft::BasicNode>,
    /// Log purge watermark; persisted so `get_log_state` reports it after a
    /// restart even once the purged entries are gone.
    pub last_purged: Option<LogId<u64>>,
    pub kv: Option<Arc<dyn nodus_storage_api::KvEngine>>,
    pub catalog_writer: Option<Arc<dyn nodus_catalog::CatalogWriter>>,
    pub catalog_reader: Option<Arc<dyn nodus_catalog::CatalogReader>>,
    /// Local cluster-metadata store the meta group applies shard-map/placement
    /// commands to, so every node converges on the same routing state.
    pub meta_store: Option<Arc<dyn nodus_meta::MetaStore>>,
}

#[derive(Clone)]
pub struct NodusRaftStore {
    snapshot_group: String,
    snapshot_compatibility: Option<Arc<dyn SnapshotCompatibility>>,
    /// Snapshot-build deferral streak, so the next build within
    /// [`SNAPSHOT_RETRY_INTERVAL`] reports no progress without rescanning, and
    /// the node's metrics loop can watch per-group deferrals.
    deferral: SnapshotDeferral,
    pub log: Arc<RwLock<BTreeMap<u64, Entry<NodusTypeConfig>>>>,
    pub vote: Arc<RwLock<Option<Vote<u64>>>>,
    pub state_machine: Arc<RwLock<StateMachine>>,
    /// Durable backing for log/vote/applied-state. `None` for the in-memory
    /// store ([`Self::new`], used in unit tests); `Some` whenever a `KvEngine`
    /// is provided, so consensus state survives restart.
    meta: Option<Arc<RaftMetaStore>>,
    /// Directory holding this group's snapshot files (the durable
    /// `current.snap` plus transient build/receive temp files). Snapshots are
    /// streamed to/from these files so the whole state is never held in memory.
    snapshot_dir: PathBuf,
    /// Metadata of the current on-disk snapshot (the bytes live in
    /// `{snapshot_dir}/current.snap`), served by `get_current_snapshot`. Persisted
    /// via `meta` and recovered on restart so a node resumes from the snapshot.
    current_snapshot_meta: Arc<RwLock<Option<NodusSnapshotMeta>>>,
}

impl Default for NodusRaftStore {
    fn default() -> Self {
        Self::new()
    }
}

/// A unique temp directory for an ephemeral/in-memory store's snapshots, so
/// tests and inert groups have a place to stream snapshot files without a
/// configured data dir.
fn temp_snapshot_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("nodus-raft-snap-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

impl NodusRaftStore {
    /// This group's snapshot-deferral tracker (shared handle). The manager
    /// captures one per group at creation for the node's metrics loop.
    pub fn deferral(&self) -> SnapshotDeferral {
        self.deferral.clone()
    }

    /// In-memory store (no durability) — for tests and ephemeral groups.
    pub fn new() -> Self {
        Self {
            log: Arc::new(RwLock::new(BTreeMap::new())),
            vote: Arc::new(RwLock::new(None)),
            state_machine: Arc::new(RwLock::new(StateMachine {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                last_purged: None,
                kv: None,
                catalog_writer: None,
                catalog_reader: None,
                meta_store: None,
            })),
            meta: None,
            snapshot_group: "unscoped".into(),
            snapshot_compatibility: None,
            deferral: SnapshotDeferral::default(),
            snapshot_dir: temp_snapshot_dir(),
            current_snapshot_meta: Arc::new(RwLock::new(None)),
        }
    }

    /// Builds a durable store over `kv`, **recovering** any previously persisted
    /// vote, log, and applied-state so a restarted node resumes its term,
    /// membership, and committed log.
    ///
    /// Fails closed: a durable read error during recovery aborts construction
    /// rather than starting the group with partial amnesia (a dropped vote
    /// invites a double-vote in the same term; a dropped applied pointer or log
    /// tail invites re-application or data loss). Genuine absence of the keys
    /// — a first boot — recovers as empty state.
    fn with(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        catalog_writer: Option<Arc<dyn nodus_catalog::CatalogWriter>>,
        catalog_reader: Option<Arc<dyn nodus_catalog::CatalogReader>>,
        meta_store: Option<Arc<dyn nodus_meta::MetaStore>>,
        snapshot_dir: PathBuf,
    ) -> anyhow::Result<Self> {
        let _ = std::fs::create_dir_all(&snapshot_dir);
        let meta = Arc::new(RaftMetaStore::new(kv.clone()));
        let mut log = meta.load_log()?;
        let vote = meta.load_vote()?;
        let mut applied = meta.load_applied()?;
        if reconcile_torn_recovery(&mut log, &mut applied) {
            // Persist the repaired watermark so the consistent view is stable
            // across a subsequent restart, even if no apply/purge runs first.
            // A failure here is not fatal to opening the store: the reconcile
            // is deterministic and re-runs on the next open, and the next
            // successful apply or purge persists the watermark — but the failed
            // durable write must not pass silently.
            if let Err(e) = meta.save_applied(&applied) {
                tracing::error!(error = %e, "failed to persist reconciled applied state");
            }
        }
        // Recover the snapshot metadata so a restarted node can resume from the
        // on-disk snapshot (and the purged log below it) rather than replaying
        // the whole log. Only honor it if the snapshot file is actually present.
        let snapshot_meta = meta
            .load_snapshot_meta()?
            .filter(|_| snapshot_dir.join(CURRENT_SNAPSHOT_FILE).exists());
        // A torn recovery can leave the purge watermark above the durable
        // snapshot: `reconcile_torn_recovery` raises the watermark to the
        // applied pointer when the applied log prefix is lost, and that pointer
        // may be newer than the last snapshot. openraft's startup rejects
        // `purge_upto > snapshot_last_log_id` with a panic, so drop the stale
        // descriptor instead: with no snapshot, openraft validates the
        // watermark against the applied pointer and rebuilds the snapshot from
        // the durable state machine through the normal gated builder before
        // serving. The descriptor is invalidated before the file, matching the
        // publication-gap ordering, so a crash here never leaves a mismatched
        // pair.
        let snapshot_meta = match snapshot_meta {
            Some(m) if applied.last_purged > m.last_log_id => {
                tracing::warn!(
                    purged = ?applied.last_purged,
                    snapshot = ?m.last_log_id,
                    "purge watermark outlived the durable snapshot; dropping the stale descriptor for rebuild"
                );
                snapshots::remove_snapshot_publication(Some(meta.as_ref()), &snapshot_dir)?;
                None
            }
            other => other,
        };
        Ok(Self {
            log: Arc::new(RwLock::new(log)),
            vote: Arc::new(RwLock::new(vote)),
            state_machine: Arc::new(RwLock::new(StateMachine {
                last_applied_log: applied.last_applied,
                last_membership: applied.last_membership,
                last_purged: applied.last_purged,
                kv: Some(kv),
                catalog_writer,
                catalog_reader,
                meta_store,
            })),
            meta: Some(meta),
            snapshot_group: "unscoped".into(),
            snapshot_compatibility: None,
            deferral: SnapshotDeferral::default(),
            snapshot_dir,
            current_snapshot_meta: Arc::new(RwLock::new(snapshot_meta)),
        })
    }

    pub fn with_kv_and_catalog(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        catalog_writer: Arc<dyn nodus_catalog::CatalogWriter>,
        catalog_reader: Arc<dyn nodus_catalog::CatalogReader>,
    ) -> anyhow::Result<Self> {
        Self::with(
            kv,
            Some(catalog_writer),
            Some(catalog_reader),
            None,
            temp_snapshot_dir(),
        )
    }

    /// Builds a store for a data shard with an ephemeral snapshot dir (tests /
    /// inert groups). Use [`Self::with_kv_at`] to back snapshots with a durable
    /// directory. Fails closed on a durable read error during recovery.
    pub fn with_kv(kv: Arc<dyn nodus_storage_api::KvEngine>) -> anyhow::Result<Self> {
        Self::with(kv, None, None, None, temp_snapshot_dir())
    }

    /// Builds a data-shard store whose snapshots are streamed to/from durable
    /// files under `snapshot_dir`. Catalog/RBAC and upgrade commands are no-ops
    /// on a data group — those are owned by the meta group. Fails closed on a
    /// durable read error during recovery.
    pub fn with_kv_at(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        snapshot_dir: PathBuf,
    ) -> anyhow::Result<Self> {
        Self::with(kv, None, None, None, snapshot_dir)
    }

    /// Builds the meta-group store with full catalog/RBAC/upgrade components plus
    /// the local cluster-metadata store that shard-map/placement commands apply
    /// to. Snapshots stream to/from durable files under `snapshot_dir`. Fails
    /// closed on a durable read error during recovery.
    pub fn with_components(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        catalog_writer: Arc<dyn nodus_catalog::CatalogWriter>,
        catalog_reader: Arc<dyn nodus_catalog::CatalogReader>,
        meta_store: Arc<dyn nodus_meta::MetaStore>,
        snapshot_dir: PathBuf,
    ) -> anyhow::Result<Self> {
        Self::with(
            kv,
            Some(catalog_writer),
            Some(catalog_reader),
            Some(meta_store),
            snapshot_dir,
        )
    }

    /// Bind snapshots to the physical Raft group. Configure before starting Raft.
    pub fn with_snapshot_group(mut self, group: impl Into<String>) -> Self {
        self.snapshot_group = group.into();
        self
    }

    /// Attach an authoritative compatibility provider. No runtime server route
    /// currently supplies one; merely understanding v2 never enables its writer.
    pub fn with_snapshot_compatibility(mut self, provider: Arc<dyn SnapshotCompatibility>) -> Self {
        self.snapshot_compatibility = Some(provider);
        self
    }

    /// Persists the current applied-state pointer (durable when backed by a
    /// `KvEngine`; a no-op for the in-memory store). A durable-write failure is
    /// reported so the apply is not acknowledged against state a crash would lose.
    fn persist_applied(&self, sm: &StateMachine) -> Result<(), StorageError<u64>> {
        if let Some(meta) = &self.meta {
            meta.save_applied(&AppliedState {
                last_applied: sm.last_applied_log,
                last_membership: sm.last_membership.clone(),
                last_purged: sm.last_purged,
            })
            .map_err(|e| {
                StorageIOError::write_state_machine(AnyError::error(format!(
                    "persist applied state: {e:#}"
                )))
            })?;
        }
        Ok(())
    }
}

impl RaftLogReader<NodusTypeConfig> for NodusRaftStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<NodusTypeConfig>>, StorageError<u64>> {
        let log = self.log.read().await;
        let mut entries = vec![];
        for (_, entry) in log.range(range.clone()) {
            entries.push(entry.clone());
        }
        Ok(entries)
    }
}

// --- Streamed snapshot record format ---------------------------------------
// [magic "NSNP"][u16 version][u8 has_catalog][u64 len + catalog bytes?]
// then repeated KV records to EOF: [u64 key_len][key][u64 val_len][val][u64 ver]
// Records are written in key order. The wire is streamed; validation and the
// atomic storage checkpoint currently materialize bounded state.

// Preserve OpenRaft's required storage-error type at the snapshot boundary.
#[allow(clippy::result_large_err)]
async fn write_snapshot_header<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    catalog: Option<&[u8]>,
) -> Result<(), StorageError<u64>> {
    w.write_all(SNAPSHOT_MAGIC)
        .await
        .map_err(|e| snapshot_io_err("write magic", e))?;
    w.write_u16(SNAPSHOT_FORMAT_VERSION)
        .await
        .map_err(|e| snapshot_io_err("write version", e))?;
    match catalog {
        Some(bytes) => {
            w.write_u8(1)
                .await
                .map_err(|e| snapshot_io_err("catalog flag", e))?;
            w.write_u64(bytes.len() as u64)
                .await
                .map_err(|e| snapshot_io_err("catalog len", e))?;
            w.write_all(bytes)
                .await
                .map_err(|e| snapshot_io_err("catalog body", e))?;
        }
        None => w
            .write_u8(0)
            .await
            .map_err(|e| snapshot_io_err("catalog flag", e))?,
    }
    Ok(())
}

// Preserve OpenRaft's required storage-error type at the snapshot boundary.
#[allow(clippy::result_large_err)]
async fn write_kv_record<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    key: &[u8],
    value: &[u8],
    version: u64,
) -> Result<(), StorageError<u64>> {
    w.write_u64(key.len() as u64)
        .await
        .map_err(|e| snapshot_io_err("key len", e))?;
    w.write_all(key)
        .await
        .map_err(|e| snapshot_io_err("key", e))?;
    w.write_u64(value.len() as u64)
        .await
        .map_err(|e| snapshot_io_err("val len", e))?;
    w.write_all(value)
        .await
        .map_err(|e| snapshot_io_err("val", e))?;
    w.write_u64(version)
        .await
        .map_err(|e| snapshot_io_err("version", e))?;
    Ok(())
}

// Preserve OpenRaft's required storage-error type at the snapshot boundary.
#[allow(clippy::result_large_err)]
async fn read_snapshot_catalog<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
) -> Result<Option<Vec<u8>>, StorageError<u64>> {
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)
        .await
        .map_err(|e| snapshot_io_err("read magic", e))?;
    if &magic != SNAPSHOT_MAGIC {
        return Err(snapshot_io_err("bad snapshot magic", "unrecognized format"));
    }
    let version = r
        .read_u16()
        .await
        .map_err(|e| snapshot_io_err("read version", e))?;
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(snapshot_io_err(
            "unsupported snapshot format version",
            version,
        ));
    }
    let has_catalog = r
        .read_u8()
        .await
        .map_err(|e| snapshot_io_err("catalog flag", e))?;
    if has_catalog == 1 {
        let len = r
            .read_u64()
            .await
            .map_err(|e| snapshot_io_err("catalog len", e))? as usize;
        if len > nodus_storage_api::snapshot::SNAPSHOT_MEMORY_LIMIT {
            return Err(snapshot_io_err(
                "catalog length",
                "snapshot record too large",
            ));
        }
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf)
            .await
            .map_err(|e| snapshot_io_err("catalog body", e))?;
        Ok(Some(buf))
    } else if has_catalog == 0 {
        Ok(None)
    } else {
        Err(snapshot_io_err("catalog flag", "must be zero or one"))
    }
}

/// Reads the next KV record, or `None` at a clean end of stream.
// Preserve OpenRaft's required storage-error type at the snapshot boundary.
#[allow(clippy::result_large_err)]
async fn read_kv_record<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
) -> Result<Option<(Vec<u8>, Vec<u8>, u64)>, StorageError<u64>> {
    let mut len_bytes = [0u8; 8];
    let n = r
        .read(&mut len_bytes[..1])
        .await
        .map_err(|e| snapshot_io_err("read key length", e))?;
    if n == 0 {
        return Ok(None);
    }
    r.read_exact(&mut len_bytes[1..])
        .await
        .map_err(|e| snapshot_io_err("truncated key length", e))?;
    let key_len = u64::from_be_bytes(len_bytes) as usize;
    if key_len > nodus_storage_api::snapshot::SNAPSHOT_MEMORY_LIMIT {
        return Err(snapshot_io_err("key length", "snapshot record too large"));
    }
    let mut key = vec![0u8; key_len];
    r.read_exact(&mut key)
        .await
        .map_err(|e| snapshot_io_err("read key", e))?;
    let val_len = r
        .read_u64()
        .await
        .map_err(|e| snapshot_io_err("read val len", e))? as usize;
    if val_len > nodus_storage_api::snapshot::SNAPSHOT_MEMORY_LIMIT {
        return Err(snapshot_io_err("value length", "snapshot record too large"));
    }
    let mut value = vec![0u8; val_len];
    r.read_exact(&mut value)
        .await
        .map_err(|e| snapshot_io_err("read val", e))?;
    let version = r
        .read_u64()
        .await
        .map_err(|e| snapshot_io_err("read version", e))?;
    Ok(Some((key, value, version)))
}

impl NodusRaftStore {
    /// Opens the current snapshot file seeked to the start, if it exists.
    // Called directly by the OpenRaft snapshot trait implementation.
    #[allow(clippy::result_large_err)]
    async fn open_current_snapshot(&self) -> Result<Option<tokio::fs::File>, StorageError<u64>> {
        let path = self.snapshot_dir.join(CURRENT_SNAPSHOT_FILE);
        match tokio::fs::File::open(&path).await {
            Ok(mut file) => {
                file.seek(SeekFrom::Start(0))
                    .await
                    .map_err(|e| snapshot_io_err("seek snapshot", e))?;
                Ok(Some(file))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(snapshot_io_err("open snapshot", e)),
        }
    }
}

/// How long a deferred snapshot waits before a build scans the state
/// machine again. Raft asks for one after every applied entry while its log
/// is past the snapshot policy, and each attempt reads every row.
const SNAPSHOT_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

impl RaftSnapshotBuilder<NodusTypeConfig> for NodusRaftStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<NodusTypeConfig>, StorageError<u64>> {
        if self.deferral.recently_deferred(SNAPSHOT_RETRY_INTERVAL) {
            return self.unchanged_snapshot().await;
        }
        match self.build_atomic_snapshot().await {
            Ok(snapshot) => {
                self.deferral.clear();
                Ok(snapshot)
            }
            Err(e) if e.downcast_ref::<snapshots::SnapshotDeferred>().is_some() => {
                tracing::debug!(reason = %e, "snapshot deferred");
                let reason = e
                    .downcast_ref::<snapshots::SnapshotDeferred>()
                    .map(|d| d.0)
                    .unwrap_or("snapshot deferred");
                self.deferral.record(reason);
                self.unchanged_snapshot().await
            }
            Err(e) => Err(snapshot_io_err("build snapshot", e)),
        }
    }
}

impl NodusRaftStore {
    /// The current snapshot again (or, before the first, an empty one at no
    /// log position): Raft ignores a snapshot no newer than its own, so the
    /// log stays until a later build succeeds.
    async fn unchanged_snapshot(&mut self) -> Result<Snapshot<NodusTypeConfig>, StorageError<u64>> {
        if let Some(current) = self.get_current_snapshot().await? {
            return Ok(current);
        }
        let path = self
            .snapshot_dir
            .join(format!("deferred-{}.tmp", uuid::Uuid::new_v4()));
        let file = tokio::fs::File::create(&path)
            .await
            .map_err(|e| snapshot_io_err("create deferred snapshot", e))?;
        let _ = std::fs::remove_file(&path);
        let last_membership = self.state_machine.read().await.last_membership.clone();
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership,
                snapshot_id: "deferred".to_string(),
            },
            snapshot: Box::new(file),
        })
    }
}

impl RaftStorage<NodusTypeConfig> for NodusRaftStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        // Persist before caching: a vote acknowledged to openraft but lost to a
        // crash could be double-cast after a restart.
        if let Some(meta) = &self.meta {
            meta.save_vote(vote).map_err(|e| {
                StorageIOError::write_vote(AnyError::error(format!("persist vote: {e:#}")))
            })?;
        }
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(*self.vote.read().await)
    }

    async fn get_log_state(&mut self) -> Result<LogState<NodusTypeConfig>, StorageError<u64>> {
        let last = self.log.read().await.values().last().map(|e| e.log_id);
        let purged = self.state_machine.read().await.last_purged;
        Ok(LogState {
            last_purged_log_id: purged,
            // After a restart that purged everything, the last log id is the
            // purge watermark.
            last_log_id: last.or(purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<NodusTypeConfig>> + OptionalSend,
    {
        let started = std::time::Instant::now();
        let mut log = self.log.write().await;
        let batch: Vec<Entry<NodusTypeConfig>> = entries.into_iter().collect();
        let entries = batch.len();
        // Persist the whole append atomically before caching and surface any
        // failure to openraft, so a crash never loses an acked entry and a
        // multi-entry append is never half durable — a leader must not ack a
        // write it hasn't durably stored.
        if let Some(meta) = &self.meta {
            meta.append_entries(&batch).map_err(|e| {
                StorageIOError::write_logs(AnyError::error(format!("persist log append: {e:#}")))
            })?;
        }
        for entry in batch {
            log.insert(entry.log_id.index, entry);
        }
        tracing::debug!(
            entries,
            durable = self.meta.is_some(),
            elapsed = ?started.elapsed(),
            "Raft log append persisted and cached"
        );
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            // Drop the durable record before the cache entry: on failure the
            // entry stays cached and openraft sees the error, so a conflict
            // truncation is never acknowledged while the conflicting entry
            // would survive a restart.
            if let Some(meta) = &self.meta {
                meta.delete_entry(key).map_err(|e| {
                    StorageIOError::write_logs(AnyError::error(format!(
                        "delete conflict log entry {key}: {e:#}"
                    )))
                })?;
            }
            log.remove(&key);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.purge_log_prefix(log_id).await
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        let sm = self.state_machine.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<NodusTypeConfig>],
    ) -> Result<Vec<ShardResponse>, StorageError<u64>> {
        let started = std::time::Instant::now();
        let entry_count = entries.len();
        let mut sm = self.state_machine.write().await;
        let mut res = Vec::with_capacity(entries.len());
        for entry in entries {
            match &entry.payload {
                EntryPayload::Normal(cmd) => {
                    tracing::debug!(command = cmd.metrics_kind(), "Raft applying command");
                    if cmd.is_upgrade_command() {
                        let response = if let ShardCommand::UpgradeControlV1(command) = cmd {
                            let kv = sm.kv.as_ref().ok_or_else(|| {
                                StorageIOError::write_state_machine(AnyError::error(
                                    "upgrade requires storage",
                                ))
                            })?;
                            upgrade::apply(kv.as_ref(), &sm, command, entry.log_id.index).map_err(
                                |e| {
                                    StorageIOError::write_state_machine(AnyError::error(format!(
                                        "upgrade apply: {e}"
                                    )))
                                },
                            )?
                        } else if let ShardCommand::UpgradeAdmissionV1(command) = cmd {
                            let kv = sm.kv.as_ref().ok_or_else(|| {
                                StorageIOError::write_state_machine(AnyError::error(
                                    "admission requires storage",
                                ))
                            })?;
                            upgrade::admission::apply(kv.as_ref(), &sm, command, entry.log_id.index)
                                .map_err(|e| {
                                    StorageIOError::write_state_machine(AnyError::error(format!(
                                        "admission apply: {e}"
                                    )))
                                })?
                        } else {
                            ShardResponse { success: false, error: Some("legacy upgrade reports are not capability evidence; use the verified upgrade service".into()) }
                        };
                        sm.last_applied_log = Some(entry.log_id);
                        res.push(response);
                        continue;
                    }
                    let reserved_upgrade_write = match cmd {
                        ShardCommand::PutIntent { key, .. }
                        | ShardCommand::DeleteIntent { key, .. }
                        | ShardCommand::IndexPutIntent { key, .. }
                        | ShardCommand::IndexDeleteIntent { key, .. } => {
                            key.starts_with(b"\x01upgrade/")
                        }
                        _ => false,
                    };
                    if reserved_upgrade_write {
                        sm.last_applied_log = Some(entry.log_id);
                        res.push(ShardResponse {
                            success: false,
                            error: Some("reserved upgrade authority key".into()),
                        });
                        continue;
                    }
                    if let Some(kv) = &sm.kv {
                        if let Some(response) = migration::apply(
                            kv.as_ref(),
                            cmd,
                            entry.log_id.index,
                            sm.meta_store.as_ref(),
                        )
                        .map_err(|e| {
                            StorageIOError::write_state_machine(AnyError::error(format!(
                                "migration apply: {e}"
                            )))
                        })? {
                            sm.last_applied_log = Some(entry.log_id);
                            res.push(response);
                            continue;
                        }
                    } else if cmd.requires_migration_protocol() {
                        return Err(StorageIOError::write_state_machine(AnyError::error(
                            "migration requires a storage engine",
                        ))
                        .into());
                    }
                    // Entry outcome: a `PrepareTxn` NO vote or a rejected intent write clears it.
                    let mut success = true;
                    let mut error = None;
                    if let Some(kv) = &sm.kv {
                        use bytes::Bytes;
                        use nodus_storage_api::TxnId;
                        use std::str::FromStr;

                        match cmd {
                            ShardCommand::PutIntent {
                                txn_id, key, value, ..
                            } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    error = apply_kv_result(
                                        "PutIntent",
                                        txn_id,
                                        kv.write_intent(
                                            TxnId(tid),
                                            Bytes::from(key.clone()),
                                            Bytes::from(value.clone()),
                                        ),
                                    )?;
                                }
                            }
                            ShardCommand::DeleteIntent { txn_id, key, .. } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    error = apply_kv_result(
                                        "DeleteIntent",
                                        txn_id,
                                        kv.delete_intent(TxnId(tid), Bytes::from(key.clone())),
                                    )?;
                                }
                            }
                            ShardCommand::CommitTxn {
                                txn_id, commit_ts, ..
                            } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    error = apply_kv_result(
                                        "CommitTxn",
                                        txn_id,
                                        kv.commit(TxnId(tid), *commit_ts),
                                    )?;
                                }
                            }
                            ShardCommand::AbortTxn {
                                txn_id, recorded, ..
                            } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    // Session aborts name the session's txn id,
                                    // which a namespaced group engine maps to
                                    // the recorded id. Janitor aborts
                                    // (`recorded`) name the id as stored, so
                                    // they bypass the mapping.
                                    let result = if *recorded {
                                        kv.abort_physical(TxnId(tid))
                                    } else {
                                        kv.abort(TxnId(tid))
                                    };
                                    error = apply_kv_result("AbortTxn", txn_id, result)?;
                                }
                            }
                            ShardCommand::PrepareTxn { txn_id, .. } => {
                                // Real 2PC vote: this participant can commit only if
                                // it still holds the transaction's intents. A
                                // vanished intent (lost, aborted, or never
                                // replicated here) is a NO vote, so the coordinator
                                // aborts instead of driving a torn commit. Prepare
                                // changes no state, so re-applying a retried prepare
                                // re-checks and yields the same vote.
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    if kv.pending_intent_keys(TxnId(tid)).is_empty() {
                                        success = false;
                                        tracing::warn!(
                                            "Raft prepare voted NO for txn {txn_id}: no live intents on this participant"
                                        );
                                    } else {
                                        tracing::debug!("Raft prepared txn {txn_id} for commit");
                                    }
                                }
                            }
                            ShardCommand::IndexPutIntent {
                                txn_id, key, value, ..
                            } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    error = apply_kv_result(
                                        "IndexPutIntent",
                                        txn_id,
                                        kv.write_intent(
                                            TxnId(tid),
                                            Bytes::from(key.clone()),
                                            Bytes::from(value.clone()),
                                        ),
                                    )?;
                                }
                            }
                            ShardCommand::IndexDeleteIntent { txn_id, key, .. } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    error = apply_kv_result(
                                        "IndexDeleteIntent",
                                        txn_id,
                                        kv.delete_intent(TxnId(tid), Bytes::from(key.clone())),
                                    )?;
                                }
                            }
                            _ => {}
                        }
                    }
                    if let Some(catalog) = &sm.catalog_writer {
                        let catalog_reader = sm.catalog_reader.as_ref();
                        match cmd {
                            ShardCommand::CreateDatabase(req) => {
                                if let Err(e) = catalog.create_database(req.clone()) {
                                    log_create_apply_error(
                                        "CreateDatabase",
                                        &e,
                                        database_create_already_applied(catalog_reader, req),
                                    );
                                }
                            }
                            ShardCommand::CreateSchema(req) => {
                                if let Err(e) = catalog.create_schema(req.clone()) {
                                    log_create_apply_error(
                                        "CreateSchema",
                                        &e,
                                        schema_create_already_applied(catalog_reader, req),
                                    );
                                }
                            }
                            ShardCommand::CreateTable(req) => {
                                if let Err(e) = catalog.create_table(req.clone()) {
                                    log_create_apply_error(
                                        "CreateTable",
                                        &e,
                                        table_create_already_applied(catalog_reader, req),
                                    );
                                }
                            }
                            ShardCommand::DropTable(id) => {
                                if let Err(e) = catalog.drop_table(*id) {
                                    tracing::debug!("DropTable error: {}", e);
                                }
                            }
                            ShardCommand::DropSchema(id) => {
                                if let Err(e) = catalog.drop_schema(*id) {
                                    tracing::debug!("DropSchema error: {}", e);
                                }
                            }
                            ShardCommand::SetSchemaOwner { id, owner_role_id } => {
                                if let Err(e) = catalog.set_schema_owner(*id, *owner_role_id) {
                                    tracing::debug!("SetSchemaOwner error: {}", e);
                                }
                            }
                            ShardCommand::GrantPrivileges(req) => {
                                let _ = catalog.grant_privileges(req.clone());
                            }
                            ShardCommand::RevokePrivileges(req) => {
                                let _ = catalog.revoke_privileges(req.clone());
                            }
                            ShardCommand::UpdateTableDescriptor(req) => {
                                let _ = catalog.update_table_descriptor(req.clone());
                            }
                            ShardCommand::CreateRole(req) => {
                                if let Err(e) = catalog.create_role(req.clone()) {
                                    tracing::debug!("CreateRole error: {}", e);
                                }
                            }
                            ShardCommand::GrantPrivilege(req) => {
                                let _ = catalog.grant_privilege(req.clone());
                            }
                            ShardCommand::RevokePrivilege(req) => {
                                let _ = catalog.revoke_privilege(req.clone());
                            }
                            ShardCommand::AddRoleMember(req) => {
                                let _ = catalog.add_role_member(req.clone());
                            }
                            ShardCommand::RemoveRoleMember(req) => {
                                let _ = catalog.remove_role_member(req.clone());
                            }
                            ShardCommand::UpdatePrincipal(req) => {
                                let _ = catalog.update_principal(req.clone());
                            }
                            ShardCommand::DropPrincipal(id) => {
                                let _ = catalog.drop_principal(*id);
                            }
                            ShardCommand::UpdateIndexState {
                                table_id,
                                index_id,
                                state,
                            } => {
                                let _ =
                                    catalog.update_index_state(*table_id, *index_id, state.clone());
                            }
                            ShardCommand::ImportCatalogSnapshot(snapshot) => {
                                if let Err(e) = catalog.import_snapshot(snapshot.clone()) {
                                    tracing::error!("ImportCatalogSnapshot apply failed: {e}");
                                }
                            }
                            _ => {}
                        }
                    }
                    if let Some(meta_store) = &sm.meta_store {
                        match cmd {
                            ShardCommand::UpdateShardMap(map) => {
                                if let Err(e) = meta_store.update_shard_map(map.clone()) {
                                    tracing::error!("UpdateShardMap apply failed: {e}");
                                }
                            }
                            ShardCommand::UpdateShardPlacements(placements) => {
                                if let Err(e) = meta_store.update_shard_placements(placements) {
                                    tracing::error!("UpdateShardPlacements apply failed: {e}");
                                }
                            }
                            _ => {}
                        }
                    }
                    res.push(ShardResponse {
                        success: success && error.is_none(),
                        error,
                    });
                }
                EntryPayload::Membership(mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(ShardResponse {
                        success: true,
                        error: None,
                    });
                }
                EntryPayload::Blank => res.push(ShardResponse {
                    success: true,
                    error: None,
                }),
            }
            sm.last_applied_log = Some(entry.log_id);
        }
        // Persist how far we've applied (and the membership at that point) so a
        // restart resumes from here instead of re-applying the whole log. A
        // durable-write failure aborts the apply: openraft must not treat these
        // entries as applied when a crash would forget them.
        self.persist_applied(&sm)?;
        tracing::debug!(
            entries = entry_count,
            elapsed = ?started.elapsed(),
            "Raft entries applied and applied-state persisted"
        );
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<u64>> {
        // openraft streams the incoming snapshot's chunks into this file, so the
        // wire transfer is streamed. Installation validates bounded state. Open it
        // read+write: openraft writes the chunks, then `install_snapshot` reads
        // them back.
        let path = self.snapshot_dir.join(RECV_SNAPSHOT_FILE);
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|e| snapshot_io_err("create receive file", e))?;
        Ok(Box::new(file))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<u64>> {
        self.install_atomic_snapshot(meta, *snapshot)
            .await
            .map_err(|e| snapshot_io_err("install snapshot", e))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<NodusTypeConfig>>, StorageError<u64>> {
        let current = self.current_snapshot_meta.read().await;
        let meta = match current.clone() {
            Some(meta) => meta,
            None => return Ok(None),
        };
        match self.open_current_snapshot().await? {
            Some(file) => Ok(Some(Snapshot {
                meta,
                snapshot: Box::new(file),
            })),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_catalog::{CatalogWriter, MemoryCatalog};

    #[test]
    fn raft_records_round_trip_and_tolerate_legacy_and_future_versions() {
        // Versioned round-trip.
        let encoded = encode_raft(b"payload".to_vec());
        assert_eq!(decode_raft(&encoded), Some(&b"payload"[..]));
        // Legacy (pre-envelope) bytes pass through unchanged.
        assert_eq!(decode_raft(b"legacy-json"), Some(&b"legacy-json"[..]));
        // A record from a newer format decodes as absent, not misparsed.
        let future = nodus_common::versioned::encode(RAFT_RECORD_VERSION + 1, b"{}");
        assert_eq!(decode_raft(&future), None);
    }

    fn reopen_with_raw_record(key: &[u8], record: Vec<u8>) -> anyhow::Error {
        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let txn = TxnId::new();
        kv.write_intent(txn, Bytes::copy_from_slice(key), Bytes::from(record))
            .unwrap();
        kv.commit(txn, 1).unwrap();
        match NodusRaftStore::with_kv(kv) {
            Ok(_) => panic!("construction must reject an unreadable Raft record"),
            Err(e) => e,
        }
    }

    fn reopen_with_raw_log_record(record: Vec<u8>) -> anyhow::Error {
        reopen_with_raw_record(&log_key(1), record)
    }

    fn pointer_records() -> [(&'static [u8], &'static str); 3] {
        [
            (RAFT_VOTE_KEY, "vote"),
            (RAFT_APPLIED_KEY, "applied state"),
            (RAFT_SNAPSHOT_META_KEY, "snapshot metadata"),
        ]
    }

    #[test]
    fn store_construction_fails_closed_on_unknown_log_envelope() {
        let future = nodus_common::versioned::encode(RAFT_RECORD_VERSION + 1, b"{}");
        let err = reopen_with_raw_log_record(future);
        assert!(
            err.to_string()
                .contains("unsupported persisted Raft log envelope"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn store_construction_fails_closed_on_malformed_log_entry_json() {
        let err = reopen_with_raw_log_record(encode_raft(b"{not-json".to_vec()));
        assert!(
            err.to_string().contains("decode persisted Raft log entry"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn store_construction_fails_closed_on_unknown_pointer_envelopes() {
        for (key, record) in pointer_records() {
            let future = nodus_common::versioned::encode(RAFT_RECORD_VERSION + 1, b"{}");
            let err = reopen_with_raw_record(key, future);
            assert!(
                err.to_string()
                    .contains(&format!("unsupported persisted Raft {record} envelope")),
                "unexpected {record} error: {err}"
            );
        }
    }

    #[test]
    fn store_construction_fails_closed_on_malformed_pointer_payloads() {
        for (key, record) in pointer_records() {
            let err = reopen_with_raw_record(key, encode_raft(b"{not-json".to_vec()));
            assert!(
                err.to_string()
                    .contains(&format!("decode persisted Raft {record}")),
                "unexpected {record} error: {err}"
            );
        }
    }

    fn blank_entry(term: u64, index: u64) -> Entry<NodusTypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Blank,
        }
    }

    /// Vote, log, and applied-state written through one store are recovered by a
    /// fresh store over the same `KvEngine` — i.e. a node's consensus state
    /// survives a restart.
    #[tokio::test]
    async fn raft_consensus_state_survives_reopening_the_store() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());

        {
            let mut store = NodusRaftStore::with_kv(kv.clone()).unwrap();
            store.save_vote(&Vote::new(3, 1)).await.unwrap();
            store
                .append_to_log(vec![
                    blank_entry(1, 1),
                    blank_entry(1, 2),
                    blank_entry(3, 3),
                ])
                .await
                .unwrap();
            // Apply up to index 2; index 3 remains an un-applied log tail.
            store
                .apply_to_state_machine(&[blank_entry(1, 1), blank_entry(1, 2)])
                .await
                .unwrap();
        }

        // Reopen over the same KV — recovery path runs in the constructor.
        let mut store = NodusRaftStore::with_kv(kv.clone()).unwrap();

        assert_eq!(store.read_vote().await.unwrap(), Some(Vote::new(3, 1)));

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.map(|l| l.index), Some(3), "log recovered");

        let entries = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[2].log_id.index, 3);

        let (applied, _membership) = store.last_applied_state().await.unwrap();
        assert_eq!(
            applied.map(|l| l.index),
            Some(2),
            "applied pointer recovered"
        );
    }

    /// Test-only engine that injects durable-write failures, proving the store
    /// reports them to openraft instead of acking writes a crash would lose.
    /// Also injects read failures, proving recovery fails closed instead of
    /// mistaking a read error for absent state.
    #[derive(Default)]
    struct FailingKv {
        inner: nodus_storage_mem::MemKvEngine,
        fail_writes: std::sync::atomic::AtomicBool,
        fail_commits: std::sync::atomic::AtomicBool,
        fail_gets: std::sync::atomic::AtomicBool,
        fail_scans: std::sync::atomic::AtomicBool,
    }

    impl FailingKv {
        fn injected() -> KvError {
            KvError::Other(anyhow::anyhow!("injected durable failure"))
        }
    }

    impl KvEngine for FailingKv {
        fn get(&self, key: &[u8], ts: u64) -> anyhow::Result<Option<Bytes>> {
            if self.fail_gets.load(Ordering::SeqCst) {
                return Err(Self::injected().into());
            }
            self.inner.get(key, ts)
        }
        fn scan(
            &self,
            range: KeyRange,
            ts: u64,
        ) -> anyhow::Result<
            Box<dyn Iterator<Item = anyhow::Result<nodus_storage_api::KvPair>> + Send>,
        > {
            if self.fail_scans.load(Ordering::SeqCst) {
                return Err(Self::injected().into());
            }
            self.inner.scan(range, ts)
        }
        fn write_intent(&self, txn: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
            if self.fail_writes.load(Ordering::SeqCst) {
                return Err(Self::injected());
            }
            self.inner.write_intent(txn, key, value)
        }
        fn delete_intent(&self, txn: TxnId, key: Bytes) -> KvResult<()> {
            if self.fail_writes.load(Ordering::SeqCst) {
                return Err(Self::injected());
            }
            self.inner.delete_intent(txn, key)
        }
        fn replace_intent(
            &self,
            txn: TxnId,
            key: Bytes,
            replacement: nodus_storage_api::IntentReplacement,
        ) -> KvResult<()> {
            self.inner.replace_intent(txn, key, replacement)
        }
        fn commit(&self, txn: TxnId, ts: u64) -> KvResult<()> {
            if self.fail_commits.load(Ordering::SeqCst) {
                return Err(Self::injected());
            }
            self.inner.commit(txn, ts)
        }
        fn abort(&self, txn: TxnId) -> KvResult<()> {
            self.inner.abort(txn)
        }
    }

    fn failing_store() -> (NodusRaftStore, Arc<FailingKv>) {
        let engine = Arc::new(FailingKv::default());
        let store = NodusRaftStore::with_kv(engine.clone()).unwrap();
        (store, engine)
    }

    /// Recovery fails closed on a durable log-scan error: the store does not
    /// open over a log tail it could not read, so committed entries are neither
    /// re-applied nor silently dropped.
    #[tokio::test]
    async fn store_construction_fails_closed_on_a_log_scan_error() {
        let (mut store, engine) = failing_store();
        store
            .append_to_log(vec![blank_entry(1, 1), blank_entry(1, 2)])
            .await
            .unwrap();
        drop(store);

        engine.fail_scans.store(true, Ordering::SeqCst);
        let dyn_engine: Arc<dyn KvEngine> = engine.clone();
        let err = match NodusRaftStore::with_kv(dyn_engine) {
            Ok(_) => panic!("construction must fail closed on an injected scan error"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("injected durable failure"),
            "error must carry the storage cause: {err}"
        );

        // Once the transient error clears, recovery succeeds and the log is intact.
        engine.fail_scans.store(false, Ordering::SeqCst);
        let dyn_engine: Arc<dyn KvEngine> = engine;
        let mut store = NodusRaftStore::with_kv(dyn_engine).unwrap();
        assert_eq!(store.try_get_log_entries(1..=2).await.unwrap().len(), 2);
    }

    /// Recovery fails closed on a durable point-read error: a persisted vote,
    /// applied pointer, or snapshot meta that cannot be read aborts store
    /// construction instead of being mistaken for a first boot — a dropped vote
    /// would let the restarted node vote again in the same term.
    #[tokio::test]
    async fn store_construction_fails_closed_on_a_meta_read_error() {
        let (mut store, engine) = failing_store();
        store.save_vote(&Vote::new(3, 1)).await.unwrap();
        store.append_to_log(vec![blank_entry(1, 1)]).await.unwrap();
        store
            .apply_to_state_machine(&[blank_entry(1, 1)])
            .await
            .unwrap();
        drop(store);

        engine.fail_gets.store(true, Ordering::SeqCst);
        let dyn_engine: Arc<dyn KvEngine> = engine.clone();
        let err = match NodusRaftStore::with_kv(dyn_engine) {
            Ok(_) => panic!("construction must fail closed on an injected read error"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("injected durable failure"),
            "error must carry the storage cause: {err}"
        );

        // Once the transient error clears, the persisted vote and applied
        // pointer are recovered — they were never lost, only unreadable.
        engine.fail_gets.store(false, Ordering::SeqCst);
        let dyn_engine: Arc<dyn KvEngine> = engine;
        let mut store = NodusRaftStore::with_kv(dyn_engine).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(Vote::new(3, 1)));
        let (applied, _) = store.last_applied_state().await.unwrap();
        assert_eq!(applied.map(|l| l.index), Some(1));
    }

    /// Genuine absence (a first boot) is not an error: construction succeeds
    /// with empty consensus state.
    #[test]
    fn store_construction_succeeds_on_genuine_absence() {
        let engine: Arc<dyn KvEngine> = Arc::new(FailingKv::default());
        let store = NodusRaftStore::with_kv(engine).unwrap();
        assert!(store.log.blocking_read().is_empty());
        assert!(store.vote.blocking_read().is_none());
        let state_machine = store.state_machine.blocking_read();
        assert!(state_machine.last_applied_log.is_none());
        assert!(state_machine.last_purged.is_none());
        drop(state_machine);
        assert!(store.current_snapshot_meta.blocking_read().is_none());
    }

    /// A failed durable append is returned to openraft and the entry never
    /// enters the in-memory log, so no write is acked that a crash would lose.
    #[tokio::test]
    async fn append_to_log_reports_durable_failure_and_caches_nothing() {
        let (mut store, engine) = failing_store();
        engine.fail_commits.store(true, Ordering::SeqCst);
        let err = store
            .append_to_log(vec![blank_entry(1, 1)])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("injected durable failure"),
            "error must carry the storage cause: {err}"
        );
        assert!(
            store.log.read().await.is_empty(),
            "a failed append must not enter the cache"
        );
        // A failed intent write (before commit) propagates the same way.
        engine.fail_commits.store(false, Ordering::SeqCst);
        engine.fail_writes.store(true, Ordering::SeqCst);
        assert!(
            store.append_to_log(vec![blank_entry(1, 1)]).await.is_err(),
            "write-intent failure must also be reported"
        );
        assert!(store.log.read().await.is_empty());
    }

    /// A failed durable vote write is returned to openraft and the cached vote
    /// keeps its previous value, so a vote cannot be lost-then-double-cast.
    #[tokio::test]
    async fn save_vote_reports_durable_failure_and_keeps_prior_vote() {
        let (mut store, engine) = failing_store();
        store.save_vote(&Vote::new(1, 1)).await.unwrap();
        engine.fail_commits.store(true, Ordering::SeqCst);
        let err = store.save_vote(&Vote::new(2, 1)).await.unwrap_err();
        assert!(
            err.to_string().contains("injected durable failure"),
            "error must carry the storage cause: {err}"
        );
        assert_eq!(
            store.read_vote().await.unwrap(),
            Some(Vote::new(1, 1)),
            "a failed vote write must not update the cache"
        );
    }

    /// A failed applied-state persist fails the apply, so openraft does not
    /// treat entries as applied when a restart would forget them.
    #[tokio::test]
    async fn apply_to_state_machine_reports_applied_persist_failure() {
        let (mut store, engine) = failing_store();
        store.append_to_log(vec![blank_entry(1, 1)]).await.unwrap();
        engine.fail_commits.store(true, Ordering::SeqCst);
        let err = store
            .apply_to_state_machine(&[blank_entry(1, 1)])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("injected durable failure"),
            "error must carry the storage cause: {err}"
        );
    }

    /// A failed durable conflict-truncation is returned to openraft and the
    /// conflicting entries stay in the log, so truncation is never acked while
    /// the entries would survive a restart.
    #[tokio::test]
    async fn delete_conflict_logs_reports_durable_failure_and_keeps_entries() {
        let (mut store, engine) = failing_store();
        store
            .append_to_log(vec![
                blank_entry(1, 1),
                blank_entry(1, 2),
                blank_entry(1, 3),
            ])
            .await
            .unwrap();
        engine.fail_commits.store(true, Ordering::SeqCst);
        let err = store
            .delete_conflict_logs_since(LogId::new(openraft::CommittedLeaderId::new(1, 1), 2))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("injected durable failure"),
            "error must carry the storage cause: {err}"
        );
        let entries = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(
            entries.len(),
            3,
            "a failed truncation must not drop cached entries"
        );
    }

    #[test]
    fn apply_kv_result_tolerates_replay_but_fails_on_real_errors() {
        let tid = TxnId::new();
        // Success and the benign idempotent-replay outcome are tolerated.
        assert_eq!(apply_kv_result("CommitTxn", "t", Ok(())).unwrap(), None);
        assert_eq!(
            apply_kv_result("CommitTxn", "t", Err(KvError::IntentNotFound(tid))).unwrap(),
            None
        );
        // A write blocked by another transaction's intent was not applied, so it
        // is reported to the proposer instead of acknowledged.
        assert!(
            apply_kv_result("PutIntent", "t", Err(KvError::WriteConflict(tid)))
                .unwrap()
                .is_some()
        );
        // A real storage/I/O failure is fatal so openraft halts apply.
        assert!(
            apply_kv_result(
                "PutIntent",
                "t",
                Err(KvError::Other(anyhow::anyhow!("disk full")))
            )
            .is_err()
        );
    }

    fn applied_at(index: u64) -> AppliedState {
        AppliedState {
            last_applied: Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), index)),
            last_membership: StoredMembership::default(),
            last_purged: None,
        }
    }

    #[test]
    fn reconcile_leaves_a_healthy_recovery_untouched() {
        // The applied prefix (indices 1..=2) is fully present, plus an unapplied
        // tail at 3 — a normal restart, which must not be disturbed.
        let mut log: BTreeMap<u64, Entry<NodusTypeConfig>> = [1, 2, 3]
            .into_iter()
            .map(|i| (i, blank_entry(1, i)))
            .collect();
        let mut applied = applied_at(2);
        assert!(!reconcile_torn_recovery(&mut log, &mut applied));
        assert_eq!(log.len(), 3, "log preserved for follower replication");
        assert_eq!(applied.last_purged, None);
    }

    #[test]
    fn reconcile_repairs_an_applied_pointer_beyond_a_lost_log() {
        // Applied through index 3, but every log entry was lost on the crash.
        let mut log: BTreeMap<u64, Entry<NodusTypeConfig>> = BTreeMap::new();
        let mut applied = applied_at(3);
        let applied_id = applied.last_applied;
        assert!(reconcile_torn_recovery(&mut log, &mut applied));
        // Reported as purged through the applied watermark, so openraft serves
        // it from the snapshot/state machine instead of reading a missing entry.
        assert_eq!(applied.last_purged, applied_id);
        assert!(log.is_empty());
    }

    #[test]
    fn reconcile_drops_a_partial_applied_prefix() {
        // Applied through 3, but only 1,2 survived (index 3's append was lost).
        let mut log: BTreeMap<u64, Entry<NodusTypeConfig>> =
            [1, 2].into_iter().map(|i| (i, blank_entry(1, i))).collect();
        let mut applied = applied_at(3);
        assert!(reconcile_torn_recovery(&mut log, &mut applied));
        assert_eq!(applied.last_purged, applied.last_applied);
        assert!(
            log.is_empty(),
            "stale applied entries dropped below the purge watermark"
        );
    }

    /// A torn crash that durably keeps the applied-state pointer but loses the
    /// log entries it refers to must recover into an openraft-consistent state,
    /// not one that reads a compacted index and aborts `Raft::new`.
    #[tokio::test]
    async fn recovery_repairs_an_applied_pointer_that_outlives_the_log() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        {
            let mut store = NodusRaftStore::with_kv(kv.clone()).unwrap();
            store
                .append_to_log(vec![blank_entry(1, 1), blank_entry(1, 2)])
                .await
                .unwrap();
            store
                .apply_to_state_machine(&[blank_entry(1, 1), blank_entry(1, 2)])
                .await
                .unwrap();
        }
        // Simulate the torn durability: the applied-state record survived, but
        // the log entries (separate KV records) did not.
        let meta = RaftMetaStore::new(kv.clone());
        meta.delete_entry(1).unwrap();
        meta.delete_entry(2).unwrap();

        // Reopen — recovery must reconcile rather than expose a torn view.
        let mut store = NodusRaftStore::with_kv(kv.clone()).unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(
            state.last_log_id.map(|l| l.index),
            Some(2),
            "last_log_id still covers the applied pointer"
        );
        assert_eq!(
            state.last_purged_log_id.map(|l| l.index),
            Some(2),
            "the lost applied prefix is reported as purged"
        );
        // `(last_purged, last_log_id]` is empty, so no phantom log reads remain.
        assert!(
            store.try_get_log_entries(1..=2).await.unwrap().is_empty(),
            "no stale entries below the purge watermark"
        );
    }

    /// When the reconciled purge watermark moves past the durable snapshot,
    /// recovery must drop the stale snapshot descriptor: openraft rejects
    /// `purge_upto > snapshot_last_log_id` with a startup panic, while a
    /// missing descriptor makes it rebuild the snapshot from the durable state
    /// machine through the normal builder.
    #[tokio::test]
    async fn recovery_drops_a_snapshot_older_than_the_reconciled_watermark() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let snapshot_dir = temp_snapshot_dir();
        {
            let mut store = NodusRaftStore::with_kv_at(kv.clone(), snapshot_dir.clone()).unwrap();
            store
                .append_to_log(vec![
                    blank_entry(1, 1),
                    blank_entry(1, 2),
                    blank_entry(1, 3),
                ])
                .await
                .unwrap();
            store
                .apply_to_state_machine(&[blank_entry(1, 1), blank_entry(1, 2)])
                .await
                .unwrap();
            // A durable snapshot at index 2, then one more applied entry.
            let snapshot = store.build_snapshot().await.unwrap();
            assert_eq!(snapshot.meta.last_log_id.map(|l| l.index), Some(2));
            store
                .apply_to_state_machine(&[blank_entry(1, 3)])
                .await
                .unwrap();
        }
        // Simulate the torn durability: the applied pointer (@3) and the
        // snapshot descriptor (@2) survived, but the log entries did not.
        let meta = RaftMetaStore::new(kv.clone());
        for index in 1..=3 {
            meta.delete_entry(index).unwrap();
        }

        // Reopen: reconcile purges to the applied pointer (@3), which is newer
        // than the snapshot (@2), so the descriptor must be dropped.
        let mut store = NodusRaftStore::with_kv_at(kv.clone(), snapshot_dir.clone()).unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(3));
        assert!(
            store.get_current_snapshot().await.unwrap().is_none(),
            "stale snapshot descriptor dropped so openraft rebuilds it"
        );
        assert!(
            !snapshot_dir.join(CURRENT_SNAPSHOT_FILE).exists(),
            "the stale snapshot file is removed with its descriptor"
        );
        assert!(
            RaftMetaStore::new(kv.clone())
                .load_snapshot_meta()
                .unwrap()
                .is_none(),
            "the persisted descriptor is removed, not just the cache"
        );

        // The exact pre-fix failure mode: openraft startup must accept the
        // repaired state — and rebuild the snapshot from the durable state
        // machine — instead of panicking on `purge_upto > snapshot_last_log_id`.
        let config = std::sync::Arc::new(
            openraft::Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let (log, sm) = openraft::storage::Adaptor::new(store);
        let raft = crate::server::NodusRaft::new(
            1,
            config,
            crate::network::NodusNetworkFactory::new("unscoped".into(), Default::default()),
            log,
            sm,
        )
        .await
        .unwrap();
        raft.shutdown().await.unwrap();
    }

    /// A snapshot at or beyond the reconciled purge watermark still satisfies
    /// openraft's invariant, so recovery must keep serving it.
    #[tokio::test]
    async fn recovery_keeps_a_snapshot_covering_the_reconciled_watermark() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let snapshot_dir = temp_snapshot_dir();
        let snapshot_id;
        {
            let mut store = NodusRaftStore::with_kv_at(kv.clone(), snapshot_dir.clone()).unwrap();
            store
                .append_to_log(vec![
                    blank_entry(1, 1),
                    blank_entry(1, 2),
                    blank_entry(1, 3),
                ])
                .await
                .unwrap();
            store
                .apply_to_state_machine(&[blank_entry(1, 1), blank_entry(1, 2), blank_entry(1, 3)])
                .await
                .unwrap();
            let snapshot = store.build_snapshot().await.unwrap();
            snapshot_id = snapshot.meta.snapshot_id.clone();
        }
        let meta = RaftMetaStore::new(kv.clone());
        for index in 1..=3 {
            meta.delete_entry(index).unwrap();
        }

        let mut store = NodusRaftStore::with_kv_at(kv.clone(), snapshot_dir.clone()).unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(3));
        let snapshot = store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot covering the purge watermark survives recovery");
        assert_eq!(snapshot.meta.snapshot_id, snapshot_id);
        assert_eq!(snapshot.meta.last_log_id.map(|l| l.index), Some(3));
    }

    /// A built snapshot is served by `get_current_snapshot` and is durable: a
    /// store reopened over the same engine recovers it, so a restarted node can
    /// resume from the snapshot instead of replaying the whole log.
    #[tokio::test]
    async fn snapshot_is_served_and_survives_reopening() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        // Both lifetimes share a snapshot dir so the on-disk snapshot is found
        // after reopening (the engine holds only the metadata).
        let snapshot_dir = temp_snapshot_dir();
        let snapshot_id;
        {
            let mut store = NodusRaftStore::with_kv_at(kv.clone(), snapshot_dir.clone()).unwrap();
            store.append_to_log(vec![blank_entry(1, 1)]).await.unwrap();
            store
                .apply_to_state_machine(&[blank_entry(1, 1)])
                .await
                .unwrap();

            let snapshot = store.build_snapshot().await.unwrap();
            snapshot_id = snapshot.meta.snapshot_id.clone();
            assert_eq!(snapshot.meta.last_log_id.map(|l| l.index), Some(1));

            // Immediately served from the cache.
            let current = store
                .get_current_snapshot()
                .await
                .unwrap()
                .expect("snapshot is available right after building it");
            assert_eq!(current.meta.snapshot_id, snapshot_id);
        }

        // Reopen over the same engine and snapshot dir: the snapshot is recovered.
        let mut store = NodusRaftStore::with_kv_at(kv.clone(), snapshot_dir.clone()).unwrap();
        let recovered = store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot survives reopening the store");
        assert_eq!(recovered.meta.snapshot_id, snapshot_id);
        assert_eq!(recovered.meta.last_log_id.map(|l| l.index), Some(1));
    }

    /// Installing a snapshot replaces the state machine: a local key absent from
    /// the snapshot (an orphan) is dropped rather than left behind.
    #[tokio::test]
    async fn install_snapshot_drops_orphan_keys() {
        use bytes::Bytes;
        use nodus_storage_api::TxnId;

        // Source state machine carries one user key, captured in a snapshot.
        let src_kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let mut src = NodusRaftStore::with_kv(src_kv.clone()).unwrap();
        let tid = TxnId::new();
        src_kv
            .write_intent(
                tid,
                Bytes::from_static(b"\x01keep"),
                Bytes::from_static(b"v"),
            )
            .unwrap();
        src_kv.commit(tid, 10).unwrap();
        let snapshot = src.build_snapshot().await.unwrap();

        // Target has an orphan key not present in the snapshot.
        let dst_kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let mut dst = NodusRaftStore::with_kv(dst_kv.clone()).unwrap();
        let t2 = TxnId::new();
        dst_kv
            .write_intent(
                t2,
                Bytes::from_static(b"\x01orphan"),
                Bytes::from_static(b"x"),
            )
            .unwrap();
        dst_kv.commit(t2, 5).unwrap();

        dst.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        assert!(
            dst_kv.get(b"\x01orphan", u64::MAX).unwrap().is_none(),
            "orphan key must be dropped on install"
        );
        assert_eq!(
            dst_kv.get(b"\x01keep", u64::MAX).unwrap().as_deref(),
            Some(&b"v"[..]),
            "snapshot key must be installed"
        );
    }

    /// The full receive path: openraft opens a receive file via
    /// `begin_receiving_snapshot`, streams the snapshot bytes into it, then hands
    /// it to `install_snapshot`. The bytes never live in memory as one blob.
    #[tokio::test]
    async fn snapshot_installs_through_begin_receiving() {
        use bytes::Bytes;
        use nodus_storage_api::TxnId;

        // Build a snapshot on the source.
        let src_kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let mut src = NodusRaftStore::with_kv(src_kv.clone()).unwrap();
        let tid = TxnId::new();
        src_kv
            .write_intent(tid, Bytes::from_static(b"\x01k"), Bytes::from_static(b"v"))
            .unwrap();
        src_kv.commit(tid, 7).unwrap();
        let snapshot = src.build_snapshot().await.unwrap();

        // Receiver: get a fresh receive file, stream the snapshot bytes into it
        // (as openraft would, chunk by chunk), then install.
        let dst_kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let mut dst = NodusRaftStore::with_kv(dst_kv.clone()).unwrap();
        let mut recv = dst.begin_receiving_snapshot().await.unwrap();
        let mut src_file = *snapshot.snapshot;
        src_file.seek(SeekFrom::Start(0)).await.unwrap();
        tokio::io::copy(&mut src_file, &mut *recv).await.unwrap();
        recv.flush().await.unwrap();

        dst.install_snapshot(&snapshot.meta, recv).await.unwrap();

        assert_eq!(
            dst_kv.get(b"\x01k", u64::MAX).unwrap().as_deref(),
            Some(&b"v"[..])
        );
        // The installed snapshot is now durable and servable.
        assert!(dst.get_current_snapshot().await.unwrap().is_some());
    }

    /// A conflicting-log truncation is durable: removed tail entries do not
    /// reappear after reopening.
    #[tokio::test]
    async fn truncated_log_entries_stay_gone_after_reopen() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        {
            let mut store = NodusRaftStore::with_kv(kv.clone()).unwrap();
            store
                .append_to_log(vec![
                    blank_entry(1, 1),
                    blank_entry(1, 2),
                    blank_entry(1, 3),
                ])
                .await
                .unwrap();
            store
                .delete_conflict_logs_since(LogId::new(openraft::CommittedLeaderId::new(1, 1), 2))
                .await
                .unwrap();
        }
        let mut store = NodusRaftStore::with_kv(kv.clone()).unwrap();
        let entries = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(entries.len(), 1, "only the un-truncated entry survives");
        assert_eq!(entries[0].log_id.index, 1);
    }

    fn catalog_reader(catalog: &Arc<MemoryCatalog>) -> Arc<dyn nodus_catalog::CatalogReader> {
        catalog.clone()
    }

    #[test]
    fn create_database_replay_is_detected_by_descriptor_id() {
        let catalog = Arc::new(MemoryCatalog::new());
        let reader = catalog_reader(&catalog);
        let req = nodus_catalog::CreateDatabaseRequest {
            id: nodus_catalog::DatabaseId::new(),
            name: "default".into(),
            owner_role_id: None,
        };

        catalog.create_database(req.clone()).unwrap();

        assert!(database_create_already_applied(Some(&reader), &req));

        let conflicting_replay = nodus_catalog::CreateDatabaseRequest {
            id: nodus_catalog::DatabaseId::new(),
            name: req.name.clone(),
            owner_role_id: None,
        };
        assert!(!database_create_already_applied(
            Some(&reader),
            &conflicting_replay
        ));
    }

    #[test]
    fn create_schema_replay_is_detected_by_descriptor_id() {
        let catalog = Arc::new(MemoryCatalog::new());
        let reader = catalog_reader(&catalog);
        let database_id = nodus_catalog::DatabaseId::new();
        let req = nodus_catalog::CreateSchemaRequest {
            id: nodus_catalog::SchemaId::new(),
            database_id,
            name: "public".into(),
            owner_role_id: None,
            managed_access: false,
        };

        catalog.create_schema(req.clone()).unwrap();

        assert!(schema_create_already_applied(Some(&reader), &req));

        let conflicting_replay = nodus_catalog::CreateSchemaRequest {
            id: nodus_catalog::SchemaId::new(),
            database_id,
            name: req.name.clone(),
            owner_role_id: None,
            managed_access: false,
        };
        assert!(!schema_create_already_applied(
            Some(&reader),
            &conflicting_replay
        ));
    }

    #[test]
    fn create_table_replay_is_detected_by_descriptor_id() {
        let catalog = Arc::new(MemoryCatalog::new());
        let reader = catalog_reader(&catalog);
        let database_id = nodus_catalog::DatabaseId::new();
        let schema_id = nodus_catalog::SchemaId::new();
        let req = nodus_catalog::CreateTableRequest {
            id: nodus_catalog::TableId::new(),
            database_id,
            schema_id,
            name: "users".into(),
            columns: Vec::new(),
            constraints: Vec::new(),
            view_query: None,
            materialized_query: None,
            parents: Vec::new(),
            partition_by: None,
            partition_bound: None,
        };

        catalog.create_table(req.clone()).unwrap();

        assert!(table_create_already_applied(Some(&reader), &req));

        let conflicting_replay = nodus_catalog::CreateTableRequest {
            id: nodus_catalog::TableId::new(),
            database_id,
            schema_id,
            name: req.name.clone(),
            columns: Vec::new(),
            constraints: Vec::new(),
            view_query: None,
            materialized_query: None,
            parents: Vec::new(),
            partition_by: None,
            partition_bound: None,
        };
        assert!(!table_create_already_applied(
            Some(&reader),
            &conflicting_replay
        ));
    }
}
