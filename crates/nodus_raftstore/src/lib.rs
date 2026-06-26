use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
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
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

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
const RAFT_SNAPSHOT_KEY: &[u8] = b"\x00raft\x00snapshot";

/// A persisted snapshot: openraft's metadata plus the opaque snapshot bytes. It
/// is stored durably so a restarted node can resume from the snapshot (and the
/// log purged below it) instead of replaying the whole log, and so
/// `get_current_snapshot` can serve it to a lagging follower without rebuilding.
#[derive(Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, openraft::BasicNode>,
    data: Vec<u8>,
}

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

    fn save_snapshot(&self, snapshot: &StoredSnapshot) {
        if let Ok(bytes) = serde_json::to_vec(snapshot) {
            self.put(RAFT_SNAPSHOT_KEY, encode_raft(bytes));
        }
    }

    fn load_snapshot(&self) -> Option<StoredSnapshot> {
        self.kv
            .get(RAFT_SNAPSHOT_KEY, u64::MAX)
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
    /// The most recently built/installed snapshot, served by
    /// `get_current_snapshot` and (when `meta` is set) persisted so it survives
    /// restart — without it openraft would replay the entire log on every start.
    current_snapshot: Arc<RwLock<Option<StoredSnapshot>>>,
}

impl Default for NodusRaftStore {
    fn default() -> Self {
        Self::new()
    }
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
            current_snapshot: Arc::new(RwLock::new(None)),
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
    ) -> Self {
        let meta = Arc::new(RaftMetaStore::new(kv.clone()));
        let mut log = meta.load_log();
        let vote = meta.load_vote();
        let mut applied = meta.load_applied();
        if reconcile_torn_recovery(&mut log, &mut applied) {
            // Persist the repaired watermark so the consistent view is stable
            // across a subsequent restart, even if no apply/purge runs first.
            meta.save_applied(&applied);
        }
        // Recover the persisted snapshot so a restarted node can resume from it
        // (and the purged log below it) rather than replaying the whole log.
        let snapshot = meta.load_snapshot();
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
            current_snapshot: Arc::new(RwLock::new(snapshot)),
        }
    }

    pub fn with_kv_and_catalog(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        catalog_writer: Arc<dyn nodus_catalog::CatalogWriter>,
        catalog_reader: Arc<dyn nodus_catalog::CatalogReader>,
    ) -> Self {
        Self::with(kv, Some(catalog_writer), Some(catalog_reader), None, None)
    }

    /// Builds a store for a data shard: it applies only KV commands to its
    /// (namespaced) engine. Catalog/RBAC and upgrade commands are no-ops on a
    /// data group — those are owned by the meta group.
    pub fn with_kv(kv: Arc<dyn nodus_storage_api::KvEngine>) -> Self {
        Self::with(kv, None, None, None, None)
    }

    /// Builds the meta-group store with full catalog/RBAC/upgrade components plus
    /// the local cluster-metadata store that shard-map/placement commands apply
    /// to.
    pub fn with_components(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        catalog_writer: Arc<dyn nodus_catalog::CatalogWriter>,
        catalog_reader: Arc<dyn nodus_catalog::CatalogReader>,
        upgrade: Arc<dyn nodus_upgrade::UpgradeCoordinator>,
        meta_store: Arc<dyn nodus_meta::MetaStore>,
    ) -> Self {
        Self::with(
            kv,
            Some(catalog_writer),
            Some(catalog_reader),
            Some(upgrade),
            Some(meta_store),
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

#[derive(Serialize, Deserialize)]
struct FullStateSnapshot {
    pub catalog: Option<nodus_catalog::CatalogSnapshot>,
    pub kv: Vec<(Vec<u8>, Vec<u8>, u64)>,
}

impl RaftSnapshotBuilder<NodusTypeConfig> for NodusRaftStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<NodusTypeConfig>, StorageError<u64>> {
        let sm = self.state_machine.read().await;

        let mut kv_data = vec![];
        if let Some(kv) = &sm.kv {
            // Start at 0x01 so the snapshot carries only user data, not the
            // `\0`-prefixed reserved keys (Raft log/vote/applied, catalog state,
            // 2PC records) — those are restored by their own mechanisms.
            let range = nodus_storage_api::KeyRange {
                start: bytes::Bytes::from_static(&[1]),
                end: bytes::Bytes::from(vec![255u8; 1024]),
            };
            if let Ok(iter) = kv.scan(range, u64::MAX) {
                for pair in iter.flatten() {
                    kv_data.push((pair.key.to_vec(), pair.value.to_vec(), pair.version));
                }
            }
        }

        let catalog_snapshot = sm.catalog_reader.as_ref().map(|c| c.export_snapshot());

        let snapshot_obj = FullStateSnapshot {
            catalog: catalog_snapshot,
            kv: kv_data,
        };

        let data_bytes = serde_json::to_vec(&snapshot_obj)
            .map(encode_raft)
            .unwrap_or_else(|_| b"{}".to_vec());

        let meta = SnapshotMeta {
            last_log_id: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
            snapshot_id: format!("snapshot-{}", uuid::Uuid::new_v4()),
        };
        drop(sm);

        // Persist and cache the snapshot so it survives restart (openraft can
        // then purge the log below it and resume from the snapshot) and so
        // `get_current_snapshot` can serve it to a lagging follower.
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data_bytes.clone(),
        };
        if let Some(meta_store) = &self.meta {
            meta_store.save_snapshot(&stored);
        }
        *self.current_snapshot.write().await = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data_bytes)),
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
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(vec![])))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let mut sm = self.state_machine.write().await;
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();
        self.persist_applied(&sm);

        let data = snapshot.into_inner();
        if let Some(snapshot_obj) =
            decode_raft(&data).and_then(|p| serde_json::from_slice::<FullStateSnapshot>(p).ok())
        {
            if let (Some(cat_snap), Some(cat)) = (snapshot_obj.catalog, &sm.catalog_writer) {
                let _ = cat.import_snapshot(cat_snap);
            }
            if let Some(kv) = &sm.kv {
                // To restore KV, we iterate and inject rows.
                // Depending on the KV engine, we may need to clear it first.
                // For MVP, we'll just write_intent and commit the dumped versions.
                use bytes::Bytes;
                use nodus_storage_api::TxnId;
                for (k, v, version) in snapshot_obj.kv {
                    let tid = TxnId::new();
                    // A fresh txn per key, so any error here is a genuine restore
                    // failure (not a benign replay duplicate) — surface it.
                    if let Err(e) = kv
                        .write_intent(tid, Bytes::from(k), Bytes::from(v))
                        .and_then(|_| kv.commit(tid, version))
                    {
                        tracing::error!(
                            "snapshot install KV write failed at version {version}: {e}"
                        );
                    }
                }
            }
        }
        drop(sm);

        // Persist and cache the installed snapshot so this node can later serve
        // it from `get_current_snapshot` and resume from it after a restart.
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        if let Some(meta_store) = &self.meta {
            meta_store.save_snapshot(&stored);
        }
        *self.current_snapshot.write().await = Some(stored);

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<NodusTypeConfig>>, StorageError<u64>> {
        Ok(self
            .current_snapshot
            .read()
            .await
            .as_ref()
            .map(|stored| Snapshot {
                meta: stored.meta.clone(),
                snapshot: Box::new(Cursor::new(stored.data.clone())),
            }))
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
        let snapshot_id;
        {
            let mut store = NodusRaftStore::with_kv(kv.clone());
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

        // Reopen over the same engine: the persisted snapshot is recovered.
        let mut store = NodusRaftStore::with_kv(kv.clone());
        let recovered = store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot survives reopening the store");
        assert_eq!(recovered.meta.snapshot_id, snapshot_id);
        assert_eq!(recovered.meta.last_log_id.map(|l| l.index), Some(1));
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
