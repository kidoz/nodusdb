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

pub mod network;
pub mod server;

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
    GrantPrivileges(nodus_catalog::GrantPrivilegesRequest),
    RevokePrivileges(nodus_catalog::RevokePrivilegesRequest),
    UpdateTableDescriptor(nodus_catalog::TableDescriptorChange),
    CreateRole(nodus_catalog::CreateRoleRequest),
    GrantPrivilege(nodus_catalog::GrantPrivilegeRequest),
    RevokePrivilege(nodus_catalog::RevokePrivilegeRequest),
    AddRoleMember(nodus_catalog::AddRoleMemberRequest),
    UpdateIndexState {
        table_id: nodus_catalog::TableId,
        index_id: nodus_catalog::IndexId,
        state: nodus_catalog::IndexState,
    },
    // Upgrade Replication Commands
    UpgradeStart {
        target_version: String,
    },
    UpgradeNodeUpgraded {
        node_id: String,
    },
    UpgradeFinalize,
    UpgradeRollback,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardResponse {
    pub success: bool,
}

/// Classifies a state-machine KV apply result so a real failure is never
/// silently dropped. A committed Raft entry whose KV effect fails to apply is a
/// durability hazard: the entry sits below the applied watermark yet isn't
/// reflected in the store.
///
/// `IntentNotFound`/`WriteConflict` are the *benign, idempotent* outcomes that
/// legitimately occur when post-crash log replay re-applies an entry above the
/// last durably-persisted applied index (the intent was already consumed) — they
/// are logged at debug and tolerated. Any other (storage/I/O) failure is
/// **fatal**: it is returned as a `StorageError` so openraft halts apply rather
/// than advancing the applied watermark past an entry whose effect was lost.
// `StorageError` is openraft's own (large) error that the whole `RaftStorage`
// trait returns; this helper feeds straight into those methods via `?`, so
// boxing it here would just force an unbox at every call site.
#[allow(clippy::result_large_err)]
fn apply_kv_result(
    command: &str,
    txn_id: &str,
    result: KvResult<()>,
) -> Result<(), StorageError<u64>> {
    match result {
        Ok(()) => Ok(()),
        Err(e @ (KvError::IntentNotFound(_) | KvError::WriteConflict(_))) => {
            tracing::debug!(
                "Raft apply {command} (txn {txn_id}): benign idempotent outcome (likely replay): {e}"
            );
            Ok(())
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
// `build_snapshot` excludes the whole `\0`-prefixed range from the data snapshot.
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
    let present = log.range(purged_idx + 1..=applied_id.index).count() as u64;
    if present == expected {
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

    fn put(&self, key: &[u8], value: Vec<u8>) {
        let txn = TxnId::new();
        if self
            .kv
            .write_intent(txn, Bytes::copy_from_slice(key), Bytes::from(value))
            .is_ok()
        {
            let _ = self.kv.commit(txn, self.next_ts());
        }
    }

    /// Atomically persists several records in one transaction (one durable
    /// commit), so a multi-record write is all-or-nothing across a crash and
    /// costs a single fsync instead of one per record. Used for a batch of
    /// appended log entries: a partial batch can never survive recovery.
    fn put_batch(&self, items: Vec<(Vec<u8>, Vec<u8>)>) {
        if items.is_empty() {
            return;
        }
        let txn = TxnId::new();
        for (key, value) in items {
            if self
                .kv
                .write_intent(txn, Bytes::from(key), Bytes::from(value))
                .is_err()
            {
                let _ = self.kv.abort(txn);
                return;
            }
        }
        let _ = self.kv.commit(txn, self.next_ts());
    }

    /// Persists a batch of log entries atomically (see [`Self::put_batch`]).
    fn append_entries(&self, entries: &[Entry<NodusTypeConfig>]) {
        let items: Vec<(Vec<u8>, Vec<u8>)> = entries
            .iter()
            .filter_map(|entry| {
                serde_json::to_vec(entry)
                    .ok()
                    .map(|bytes| (log_key(entry.log_id.index), encode_raft(bytes)))
            })
            .collect();
        self.put_batch(items);
    }

    fn delete(&self, key: &[u8]) {
        let txn = TxnId::new();
        if self
            .kv
            .delete_intent(txn, Bytes::copy_from_slice(key))
            .is_ok()
        {
            let _ = self.kv.commit(txn, self.next_ts());
        }
    }

    fn save_vote(&self, vote: &Vote<u64>) {
        if let Ok(bytes) = serde_json::to_vec(vote) {
            self.put(RAFT_VOTE_KEY, encode_raft(bytes));
        }
    }

    fn load_vote(&self) -> Option<Vote<u64>> {
        self.kv
            .get(RAFT_VOTE_KEY, u64::MAX)
            .ok()
            .flatten()
            .and_then(|b| decode_raft(&b).and_then(|p| serde_json::from_slice(p).ok()))
    }

    fn delete_entry(&self, index: u64) {
        self.delete(&log_key(index));
    }

    fn load_log(&self) -> BTreeMap<u64, Entry<NodusTypeConfig>> {
        let mut out = BTreeMap::new();
        let range = KeyRange {
            start: Bytes::from_static(RAFT_LOG_PREFIX),
            end: Bytes::from_static(RAFT_LOG_END),
        };
        if let Ok(iter) = self.kv.scan(range, u64::MAX) {
            for pair in iter.flatten() {
                if let Some(entry) = decode_raft(&pair.value)
                    .and_then(|p| serde_json::from_slice::<Entry<NodusTypeConfig>>(p).ok())
                {
                    out.insert(entry.log_id.index, entry);
                }
            }
        }
        out
    }

    fn save_applied(&self, state: &AppliedState) {
        if let Ok(bytes) = serde_json::to_vec(state) {
            self.put(RAFT_APPLIED_KEY, encode_raft(bytes));
        }
    }

    fn load_applied(&self) -> AppliedState {
        self.kv
            .get(RAFT_APPLIED_KEY, u64::MAX)
            .ok()
            .flatten()
            .and_then(|b| decode_raft(&b).and_then(|p| serde_json::from_slice(p).ok()))
            .unwrap_or_default()
    }

    fn save_snapshot_meta(&self, meta: &NodusSnapshotMeta) {
        if let Ok(bytes) = serde_json::to_vec(meta) {
            self.put(RAFT_SNAPSHOT_META_KEY, encode_raft(bytes));
        }
    }

    fn load_snapshot_meta(&self) -> Option<NodusSnapshotMeta> {
        self.kv
            .get(RAFT_SNAPSHOT_META_KEY, u64::MAX)
            .ok()
            .flatten()
            .and_then(|b| decode_raft(&b).and_then(|p| serde_json::from_slice(p).ok()))
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
/// version, so a load path treats that state as absent — the node re-syncs it
/// from the leader — rather than misparsing a format it does not understand.
fn decode_raft(bytes: &[u8]) -> Option<&[u8]> {
    use nodus_common::versioned::{Envelope, decode};
    match decode(bytes) {
        Envelope::Versioned { version, payload } if version == RAFT_RECORD_VERSION => Some(payload),
        Envelope::Versioned { .. } => None,
        Envelope::Legacy(legacy) => Some(legacy),
    }
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
    pub upgrade: Option<Arc<dyn nodus_upgrade::UpgradeCoordinator>>,
    /// Local cluster-metadata store the meta group applies shard-map/placement
    /// commands to, so every node converges on the same routing state.
    pub meta_store: Option<Arc<dyn nodus_meta::MetaStore>>,
}

#[derive(Clone)]
pub struct NodusRaftStore {
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
                upgrade: None,
                meta_store: None,
            })),
            meta: None,
            snapshot_dir: temp_snapshot_dir(),
            current_snapshot_meta: Arc::new(RwLock::new(None)),
        }
    }

    /// Builds a durable store over `kv`, **recovering** any previously persisted
    /// vote, log, and applied-state so a restarted node resumes its term,
    /// membership, and committed log.
    fn with(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        catalog_writer: Option<Arc<dyn nodus_catalog::CatalogWriter>>,
        catalog_reader: Option<Arc<dyn nodus_catalog::CatalogReader>>,
        upgrade: Option<Arc<dyn nodus_upgrade::UpgradeCoordinator>>,
        meta_store: Option<Arc<dyn nodus_meta::MetaStore>>,
        snapshot_dir: PathBuf,
    ) -> Self {
        let _ = std::fs::create_dir_all(&snapshot_dir);
        let meta = Arc::new(RaftMetaStore::new(kv.clone()));
        let mut log = meta.load_log();
        let vote = meta.load_vote();
        let mut applied = meta.load_applied();
        if reconcile_torn_recovery(&mut log, &mut applied) {
            // Persist the repaired watermark so the consistent view is stable
            // across a subsequent restart, even if no apply/purge runs first.
            meta.save_applied(&applied);
        }
        // Recover the snapshot metadata so a restarted node can resume from the
        // on-disk snapshot (and the purged log below it) rather than replaying
        // the whole log. Only honor it if the snapshot file is actually present.
        let snapshot_meta = meta
            .load_snapshot_meta()
            .filter(|_| snapshot_dir.join(CURRENT_SNAPSHOT_FILE).exists());
        Self {
            log: Arc::new(RwLock::new(log)),
            vote: Arc::new(RwLock::new(vote)),
            state_machine: Arc::new(RwLock::new(StateMachine {
                last_applied_log: applied.last_applied,
                last_membership: applied.last_membership,
                last_purged: applied.last_purged,
                kv: Some(kv),
                catalog_writer,
                catalog_reader,
                upgrade,
                meta_store,
            })),
            meta: Some(meta),
            snapshot_dir,
            current_snapshot_meta: Arc::new(RwLock::new(snapshot_meta)),
        }
    }

    pub fn with_kv_and_catalog(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        catalog_writer: Arc<dyn nodus_catalog::CatalogWriter>,
        catalog_reader: Arc<dyn nodus_catalog::CatalogReader>,
    ) -> Self {
        Self::with(
            kv,
            Some(catalog_writer),
            Some(catalog_reader),
            None,
            None,
            temp_snapshot_dir(),
        )
    }

    /// Builds a store for a data shard with an ephemeral snapshot dir (tests /
    /// inert groups). Use [`Self::with_kv_at`] to back snapshots with a durable
    /// directory.
    pub fn with_kv(kv: Arc<dyn nodus_storage_api::KvEngine>) -> Self {
        Self::with(kv, None, None, None, None, temp_snapshot_dir())
    }

    /// Builds a data-shard store whose snapshots are streamed to/from durable
    /// files under `snapshot_dir`. Catalog/RBAC and upgrade commands are no-ops
    /// on a data group — those are owned by the meta group.
    pub fn with_kv_at(kv: Arc<dyn nodus_storage_api::KvEngine>, snapshot_dir: PathBuf) -> Self {
        Self::with(kv, None, None, None, None, snapshot_dir)
    }

    /// Builds the meta-group store with full catalog/RBAC/upgrade components plus
    /// the local cluster-metadata store that shard-map/placement commands apply
    /// to. Snapshots stream to/from durable files under `snapshot_dir`.
    pub fn with_components(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        catalog_writer: Arc<dyn nodus_catalog::CatalogWriter>,
        catalog_reader: Arc<dyn nodus_catalog::CatalogReader>,
        upgrade: Arc<dyn nodus_upgrade::UpgradeCoordinator>,
        meta_store: Arc<dyn nodus_meta::MetaStore>,
        snapshot_dir: PathBuf,
    ) -> Self {
        Self::with(
            kv,
            Some(catalog_writer),
            Some(catalog_reader),
            Some(upgrade),
            Some(meta_store),
            snapshot_dir,
        )
    }

    /// Persists the current applied-state pointer (durable when backed by a
    /// `KvEngine`; a no-op for the in-memory store).
    fn persist_applied(&self, sm: &StateMachine) {
        if let Some(meta) = &self.meta {
            meta.save_applied(&AppliedState {
                last_applied: sm.last_applied_log,
                last_membership: sm.last_membership.clone(),
                last_purged: sm.last_purged,
            });
        }
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

/// The user-data key range a snapshot covers: everything from `0x01` up, i.e.
/// excluding the `\0`-prefixed reserved keys (Raft log/vote/applied, catalog
/// state, 2PC records), which are restored by their own mechanisms.
fn snapshot_user_range() -> nodus_storage_api::KeyRange {
    nodus_storage_api::KeyRange {
        start: Bytes::from_static(&[1]),
        end: Bytes::from(vec![255u8; 1024]),
    }
}

// --- Streamed snapshot record format ---------------------------------------
// [magic "NSNP"][u16 version][u8 has_catalog][u64 len + catalog bytes?]
// then repeated KV records to EOF: [u64 key_len][key][u64 val_len][val][u64 ver]
// Records are written in key order (the scan is sorted), and never materialize
// the whole snapshot in memory on either end.

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
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf)
            .await
            .map_err(|e| snapshot_io_err("catalog body", e))?;
        Ok(Some(buf))
    } else {
        Ok(None)
    }
}

/// Reads the next KV record, or `None` at a clean end of stream.
async fn read_kv_record<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
) -> Result<Option<(Vec<u8>, Vec<u8>, u64)>, StorageError<u64>> {
    let key_len = match r.read_u64().await {
        Ok(n) => n as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(snapshot_io_err("read key len", e)),
    };
    let mut key = vec![0u8; key_len];
    r.read_exact(&mut key)
        .await
        .map_err(|e| snapshot_io_err("read key", e))?;
    let val_len = r
        .read_u64()
        .await
        .map_err(|e| snapshot_io_err("read val len", e))? as usize;
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

impl RaftSnapshotBuilder<NodusTypeConfig> for NodusRaftStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<NodusTypeConfig>, StorageError<u64>> {
        // Capture the consistent header under the lock, then stream the KV data
        // without holding it (the scan returns a materialized, point-in-time
        // iterator, so applies are not blocked for the whole build).
        let (last_log_id, last_membership, catalog, kv) = {
            let sm = self.state_machine.read().await;
            (
                sm.last_applied_log,
                sm.last_membership.clone(),
                sm.catalog_reader.as_ref().map(|c| c.export_snapshot()),
                sm.kv.clone(),
            )
        };
        let catalog_bytes = match catalog {
            Some(c) => {
                Some(serde_json::to_vec(&c).map_err(|e| snapshot_io_err("serialize catalog", e))?)
            }
            None => None,
        };

        let tmp = self
            .snapshot_dir
            .join(format!("build-{}.tmp", uuid::Uuid::new_v4()));
        {
            let file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| snapshot_io_err("create build tmp", e))?;
            let mut w = BufWriter::new(file);
            write_snapshot_header(&mut w, catalog_bytes.as_deref()).await?;
            if let Some(kv) = kv
                && let Ok(iter) = kv.scan(snapshot_user_range(), u64::MAX)
            {
                for pair in iter.flatten() {
                    write_kv_record(&mut w, &pair.key, &pair.value, pair.version).await?;
                }
            }
            w.flush()
                .await
                .map_err(|e| snapshot_io_err("flush build", e))?;
            w.into_inner()
                .sync_all()
                .await
                .map_err(|e| snapshot_io_err("fsync build", e))?;
        }

        // Atomically install as the current snapshot and persist its metadata.
        let current = self.snapshot_dir.join(CURRENT_SNAPSHOT_FILE);
        tokio::fs::rename(&tmp, &current)
            .await
            .map_err(|e| snapshot_io_err("publish snapshot", e))?;
        let meta = SnapshotMeta {
            last_log_id,
            last_membership,
            snapshot_id: format!("snapshot-{}", uuid::Uuid::new_v4()),
        };
        if let Some(meta_store) = &self.meta {
            meta_store.save_snapshot_meta(&meta);
        }
        *self.current_snapshot_meta.write().await = Some(meta.clone());

        let file = self
            .open_current_snapshot()
            .await?
            .ok_or_else(|| snapshot_io_err("reopen snapshot", "missing after publish"))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(file),
        })
    }
}

impl RaftStorage<NodusTypeConfig> for NodusRaftStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        *self.vote.write().await = Some(*vote);
        if let Some(meta) = &self.meta {
            meta.save_vote(vote);
        }
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
        let mut log = self.log.write().await;
        let batch: Vec<Entry<NodusTypeConfig>> = entries.into_iter().collect();
        // Persist the whole append atomically before caching, so a crash never
        // leaves an acked-but-lost entry and a multi-entry append is never half
        // durable — a leader must not ack a write it hasn't durably stored.
        if let Some(meta) = &self.meta {
            meta.append_entries(&batch);
        }
        for entry in batch {
            log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            log.remove(&key);
            if let Some(meta) = &self.meta {
                meta.delete_entry(key);
            }
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        {
            let mut log = self.log.write().await;
            let keys: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
            for key in keys {
                log.remove(&key);
                if let Some(meta) = &self.meta {
                    meta.delete_entry(key);
                }
            }
        }
        let mut sm = self.state_machine.write().await;
        sm.last_purged = Some(log_id);
        self.persist_applied(&sm);
        Ok(())
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
        let mut sm = self.state_machine.write().await;
        let mut res = Vec::with_capacity(entries.len());
        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Normal(cmd) => {
                    tracing::info!("Raft applying command: {:?}", cmd);
                    if let Some(kv) = &sm.kv {
                        use bytes::Bytes;
                        use nodus_storage_api::TxnId;
                        use std::str::FromStr;

                        match cmd {
                            ShardCommand::PutIntent {
                                txn_id, key, value, ..
                            } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    apply_kv_result(
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
                                    apply_kv_result(
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
                                    apply_kv_result(
                                        "CommitTxn",
                                        txn_id,
                                        kv.commit(TxnId(tid), *commit_ts),
                                    )?;
                                }
                            }
                            ShardCommand::AbortTxn { txn_id, .. } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    apply_kv_result("AbortTxn", txn_id, kv.abort(TxnId(tid)))?;
                                }
                            }
                            ShardCommand::PrepareTxn { txn_id, .. } => {
                                // Durably acknowledged by virtue of being applied;
                                // the actual intents are already present. No state
                                // change — the decision is recorded by the coordinator.
                                tracing::debug!("Raft prepared txn {txn_id} for commit");
                            }
                            ShardCommand::IndexPutIntent {
                                txn_id, key, value, ..
                            } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    apply_kv_result(
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
                                    apply_kv_result(
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
                            ShardCommand::UpdateIndexState {
                                table_id,
                                index_id,
                                state,
                            } => {
                                let _ =
                                    catalog.update_index_state(*table_id, *index_id, state.clone());
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
                    if let Some(upgrade) = &sm.upgrade {
                        match cmd {
                            ShardCommand::UpgradeStart { target_version } => {
                                if let Err(e) = upgrade.start_upgrade(target_version.clone()) {
                                    tracing::error!("UpgradeStart error: {}", e);
                                }
                            }
                            ShardCommand::UpgradeNodeUpgraded { node_id } => {
                                if let Err(e) = upgrade.report_node_upgraded(node_id) {
                                    tracing::error!("UpgradeNodeUpgraded error: {}", e);
                                }
                            }
                            ShardCommand::UpgradeFinalize => {
                                if let Err(e) = upgrade.finalize_upgrade() {
                                    tracing::error!("UpgradeFinalize error: {}", e);
                                }
                            }
                            ShardCommand::UpgradeRollback => {
                                if let Err(e) = upgrade.rollback() {
                                    tracing::error!("UpgradeRollback error: {}", e);
                                }
                            }
                            _ => {}
                        }
                    }
                    res.push(ShardResponse { success: true });
                }
                EntryPayload::Membership(mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(ShardResponse { success: true });
                }
                EntryPayload::Blank => res.push(ShardResponse { success: true }),
            }
        }
        // Persist how far we've applied (and the membership at that point) so a
        // restart resumes from here instead of re-applying the whole log.
        self.persist_applied(&sm);
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<u64>> {
        // openraft streams the incoming snapshot's chunks into this file, so the
        // whole snapshot never lives in memory on the receiving side. Open it
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
        use bytes::Bytes;
        use nodus_storage_api::TxnId;

        // Stream-read the received snapshot file, applying records one at a time
        // so the whole state is never materialized in memory.
        let mut file = *snapshot;
        file.seek(SeekFrom::Start(0))
            .await
            .map_err(|e| snapshot_io_err("seek receive file", e))?;
        let mut reader = BufReader::new(file);
        let catalog_bytes = read_snapshot_catalog(&mut reader).await?;

        let mut sm = self.state_machine.write().await;
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();
        self.persist_applied(&sm);

        if let (Some(bytes), Some(cat)) = (&catalog_bytes, &sm.catalog_writer)
            && let Ok(snap) = serde_json::from_slice::<nodus_catalog::CatalogSnapshot>(bytes)
        {
            let _ = cat.import_snapshot(snap);
        }

        if let Some(kv) = sm.kv.clone() {
            // Apply the snapshot's keys as they stream in, tracking the key set so
            // orphans (local keys the snapshot doesn't contain) can be dropped —
            // an install *replaces* state, it does not merge into a superset.
            let mut snapshot_keys: std::collections::HashSet<Vec<u8>> =
                std::collections::HashSet::new();
            let mut max_version = 0u64;
            while let Some((k, v, version)) = read_kv_record(&mut reader).await? {
                max_version = max_version.max(version);
                let tid = TxnId::new();
                // A fresh txn per key, so any error is a genuine restore failure
                // (not a benign replay duplicate) — surface it.
                if let Err(e) = kv
                    .write_intent(tid, Bytes::from(k.clone()), Bytes::from(v))
                    .and_then(|_| kv.commit(tid, version))
                {
                    tracing::error!("snapshot install KV write failed at version {version}: {e}");
                }
                snapshot_keys.insert(k);
            }

            let mut orphans = Vec::new();
            if let Ok(iter) = kv.scan(snapshot_user_range(), u64::MAX) {
                for pair in iter.flatten() {
                    max_version = max_version.max(pair.version);
                    if !snapshot_keys.contains(pair.key.as_ref()) {
                        orphans.push(pair.key.to_vec());
                    }
                }
            }
            // Tombstone orphans above every retained version so they read absent.
            let clear_version = max_version + 1;
            for key in orphans {
                let tid = TxnId::new();
                if let Err(e) = kv
                    .delete_intent(tid, Bytes::from(key))
                    .and_then(|_| kv.commit(tid, clear_version))
                {
                    tracing::error!("snapshot install orphan delete failed: {e}");
                }
            }
        }
        drop(sm);

        // Publish the received snapshot as the durable current one by streaming
        // it to a temp file and atomically renaming — independent of where the
        // received file lives — then persist its metadata so the node can serve
        // it and resume from it after restart.
        let mut received = reader.into_inner();
        received
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|e| snapshot_io_err("rewind received snapshot", e))?;
        let tmp = self
            .snapshot_dir
            .join(format!("install-{}.tmp", uuid::Uuid::new_v4()));
        {
            let mut out = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| snapshot_io_err("create install tmp", e))?;
            tokio::io::copy(&mut received, &mut out)
                .await
                .map_err(|e| snapshot_io_err("copy received snapshot", e))?;
            out.sync_all()
                .await
                .map_err(|e| snapshot_io_err("fsync install tmp", e))?;
        }
        let current = self.snapshot_dir.join(CURRENT_SNAPSHOT_FILE);
        tokio::fs::rename(&tmp, &current)
            .await
            .map_err(|e| snapshot_io_err("publish received snapshot", e))?;
        if let Some(meta_store) = &self.meta {
            meta_store.save_snapshot_meta(meta);
        }
        *self.current_snapshot_meta.write().await = Some(meta.clone());

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<NodusTypeConfig>>, StorageError<u64>> {
        let meta = match self.current_snapshot_meta.read().await.clone() {
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
            let mut store = NodusRaftStore::with_kv(kv.clone());
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
        let mut store = NodusRaftStore::with_kv(kv.clone());

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

    #[test]
    fn apply_kv_result_tolerates_replay_but_fails_on_real_errors() {
        let tid = TxnId::new();
        // Success and the benign idempotent-replay outcomes are tolerated.
        assert!(apply_kv_result("CommitTxn", "t", Ok(())).is_ok());
        assert!(apply_kv_result("CommitTxn", "t", Err(KvError::IntentNotFound(tid))).is_ok());
        assert!(apply_kv_result("PutIntent", "t", Err(KvError::WriteConflict(tid))).is_ok());
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
            let mut store = NodusRaftStore::with_kv(kv.clone());
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
        meta.delete_entry(1);
        meta.delete_entry(2);

        // Reopen — recovery must reconcile rather than expose a torn view.
        let mut store = NodusRaftStore::with_kv(kv.clone());
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
            let mut store = NodusRaftStore::with_kv_at(kv.clone(), snapshot_dir.clone());
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
        let mut store = NodusRaftStore::with_kv_at(kv.clone(), snapshot_dir.clone());
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
        let mut src = NodusRaftStore::with_kv(src_kv.clone());
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
        let mut dst = NodusRaftStore::with_kv(dst_kv.clone());
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
        let mut src = NodusRaftStore::with_kv(src_kv.clone());
        let tid = TxnId::new();
        src_kv
            .write_intent(tid, Bytes::from_static(b"\x01k"), Bytes::from_static(b"v"))
            .unwrap();
        src_kv.commit(tid, 7).unwrap();
        let snapshot = src.build_snapshot().await.unwrap();

        // Receiver: get a fresh receive file, stream the snapshot bytes into it
        // (as openraft would, chunk by chunk), then install.
        let dst_kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let mut dst = NodusRaftStore::with_kv(dst_kv.clone());
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
            let mut store = NodusRaftStore::with_kv(kv.clone());
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
        let mut store = NodusRaftStore::with_kv(kv.clone());
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
        };
        assert!(!table_create_already_applied(
            Some(&reader),
            &conflicting_replay
        ));
    }
}
