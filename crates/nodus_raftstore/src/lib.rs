use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError, StoredMembership, Vote,
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
    },
    CommitTxn {
        txn_id: String,
        commit_ts: u64,
    },
    AbortTxn {
        txn_id: String,
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
    },
    SplitShard {
        split_key: Vec<u8>,
    },
    InstallSnapshot {
        snapshot_id: String,
    },
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
    UpgradeStart { target_version: String },
    UpgradeNodeUpgraded { node_id: String },
    UpgradeFinalize,
    UpgradeRollback,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardResponse {
    pub success: bool,
}

pub struct StateMachine {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, openraft::BasicNode>,
    pub kv: Option<Arc<dyn nodus_storage_api::KvEngine>>,
    pub catalog_writer: Option<Arc<dyn nodus_catalog::CatalogWriter>>,
    pub catalog_reader: Option<Arc<dyn nodus_catalog::CatalogReader>>,
    pub upgrade: Option<Arc<dyn nodus_upgrade::UpgradeCoordinator>>,
}

#[derive(Clone)]
pub struct NodusRaftStore {
    pub log: Arc<RwLock<BTreeMap<u64, Entry<NodusTypeConfig>>>>,
    pub vote: Arc<RwLock<Option<Vote<u64>>>>,
    pub state_machine: Arc<RwLock<StateMachine>>,
}

impl Default for NodusRaftStore {
    fn default() -> Self {
        Self::new()
    }
}

impl NodusRaftStore {
    pub fn new() -> Self {
        Self {
            log: Arc::new(RwLock::new(BTreeMap::new())),
            vote: Arc::new(RwLock::new(None)),
            state_machine: Arc::new(RwLock::new(StateMachine {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                kv: None,
                catalog_writer: None,
                catalog_reader: None,
                upgrade: None,
            })),
        }
    }

    pub fn with_kv_and_catalog(kv: Arc<dyn nodus_storage_api::KvEngine>, catalog_writer: Arc<dyn nodus_catalog::CatalogWriter>, catalog_reader: Arc<dyn nodus_catalog::CatalogReader>) -> Self {
        Self {
            log: Arc::new(RwLock::new(BTreeMap::new())),
            vote: Arc::new(RwLock::new(None)),
            state_machine: Arc::new(RwLock::new(StateMachine {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                kv: Some(kv),
                catalog_writer: Some(catalog_writer),
                catalog_reader: Some(catalog_reader),
                upgrade: None,
            })),
        }
    }

    pub fn with_components(
        kv: Arc<dyn nodus_storage_api::KvEngine>,
        catalog_writer: Arc<dyn nodus_catalog::CatalogWriter>,
        catalog_reader: Arc<dyn nodus_catalog::CatalogReader>,
        upgrade: Arc<dyn nodus_upgrade::UpgradeCoordinator>,
    ) -> Self {
        Self {
            log: Arc::new(RwLock::new(BTreeMap::new())),
            vote: Arc::new(RwLock::new(None)),
            state_machine: Arc::new(RwLock::new(StateMachine {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                kv: Some(kv),
                catalog_writer: Some(catalog_writer),
                catalog_reader: Some(catalog_reader),
                upgrade: Some(upgrade),
            })),
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
            let range = nodus_storage_api::KeyRange {
                start: bytes::Bytes::new(),
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

        let data_bytes = serde_json::to_vec(&snapshot_obj).unwrap_or_else(|_| b"{}".to_vec());

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: sm.last_applied_log,
                last_membership: sm.last_membership.clone(),
                snapshot_id: format!("snapshot-{}", uuid::Uuid::new_v4()),
            },
            snapshot: Box::new(Cursor::new(data_bytes)),
        })
    }
}

impl RaftStorage<NodusTypeConfig> for NodusRaftStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(*self.vote.read().await)
    }

    async fn get_log_state(&mut self) -> Result<LogState<NodusTypeConfig>, StorageError<u64>> {
        let log = self.log.read().await;
        let last = log.values().last().map(|e| e.log_id);
        Ok(LogState {
            last_purged_log_id: None,
            last_log_id: last,
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
        for entry in entries {
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
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            log.remove(&key);
        }
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
                        use nodus_storage_api::TxnId;
                        use bytes::Bytes;
                        use std::str::FromStr;

                        match cmd {
                            ShardCommand::PutIntent { txn_id, key, value } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    let _ = kv.write_intent(TxnId(tid), Bytes::from(key.clone()), Bytes::from(value.clone()));
                                }
                            }
                            ShardCommand::DeleteIntent { txn_id, key } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    let _ = kv.delete_intent(TxnId(tid), Bytes::from(key.clone()));
                                }
                            }
                            ShardCommand::CommitTxn { txn_id, commit_ts } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    let _ = kv.commit(TxnId(tid), *commit_ts);
                                }
                            }
                            ShardCommand::AbortTxn { txn_id } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    let _ = kv.abort(TxnId(tid));
                                }
                            }
                            ShardCommand::IndexPutIntent { txn_id, key, value, .. } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    let _ = kv.write_intent(TxnId(tid), Bytes::from(key.clone()), Bytes::from(value.clone()));
                                }
                            }
                            ShardCommand::IndexDeleteIntent { txn_id, key, .. } => {
                                if let Ok(tid) = uuid::Uuid::from_str(txn_id) {
                                    let _ = kv.delete_intent(TxnId(tid), Bytes::from(key.clone()));
                                }
                            }
                            _ => {}
                        }
                    }
                    if let Some(catalog) = &sm.catalog_writer {
                        match cmd {
                            ShardCommand::CreateDatabase(req) => { 
                                if let Err(e) = catalog.create_database(req.clone()) {
                                    tracing::debug!("CreateDatabase error: {}", e);
                                }
                            }
                            ShardCommand::CreateSchema(req) => { 
                                if let Err(e) = catalog.create_schema(req.clone()) {
                                    tracing::debug!("CreateSchema error: {}", e);
                                }
                            }
                            ShardCommand::CreateTable(req) => { 
                                if let Err(e) = catalog.create_table(req.clone()) {
                                    tracing::debug!("CreateTable error: {}", e);
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
                            ShardCommand::GrantPrivileges(req) => { let _ = catalog.grant_privileges(req.clone()); }
                            ShardCommand::RevokePrivileges(req) => { let _ = catalog.revoke_privileges(req.clone()); }
                            ShardCommand::UpdateTableDescriptor(req) => { let _ = catalog.update_table_descriptor(req.clone()); }
                            ShardCommand::CreateRole(req) => { 
                                if let Err(e) = catalog.create_role(req.clone()) {
                                    tracing::debug!("CreateRole error: {}", e);
                                }
                            }
                            ShardCommand::GrantPrivilege(req) => { let _ = catalog.grant_privilege(req.clone()); }
                            ShardCommand::RevokePrivilege(req) => { let _ = catalog.revoke_privilege(req.clone()); }
                            ShardCommand::AddRoleMember(req) => { let _ = catalog.add_role_member(req.clone()); }
                            ShardCommand::UpdateIndexState { table_id, index_id, state } => { let _ = catalog.update_index_state(*table_id, *index_id, state.clone()); }
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

        let data = snapshot.into_inner();
        if let Ok(snapshot_obj) = serde_json::from_slice::<FullStateSnapshot>(&data) {
            if let (Some(cat_snap), Some(cat)) = (snapshot_obj.catalog, &sm.catalog_writer) {
                let _ = cat.import_snapshot(cat_snap);
            }
            if let Some(kv) = &sm.kv {
                // To restore KV, we iterate and inject rows.
                // Depending on the KV engine, we may need to clear it first.
                // For MVP, we'll just write_intent and commit the dumped versions.
                use nodus_storage_api::TxnId;
                use bytes::Bytes;
                for (k, v, version) in snapshot_obj.kv {
                    let tid = TxnId::new();
                    let _ = kv.write_intent(tid, Bytes::from(k), Bytes::from(v));
                    let _ = kv.commit(tid, version);
                }
            }
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<NodusTypeConfig>>, StorageError<u64>> {
        Ok(None)
    }
}
