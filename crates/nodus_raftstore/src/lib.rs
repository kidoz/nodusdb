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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
    SplitShard {
        split_key: Vec<u8>,
    },
    InstallSnapshot {
        snapshot_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardResponse {
    pub success: bool,
}

pub struct StateMachine {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, openraft::BasicNode>,
    pub data: BTreeMap<String, String>,
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
                data: BTreeMap::new(),
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

impl RaftSnapshotBuilder<NodusTypeConfig> for NodusRaftStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<NodusTypeConfig>, StorageError<u64>> {
        let sm = self.state_machine.read().await;

        let data_bytes = serde_json::to_vec(&sm.data).unwrap_or_default();

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: sm.last_applied_log,
                last_membership: sm.last_membership.clone(),
                snapshot_id: "snapshot".to_string(),
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
                    if let ShardCommand::PutIntent { key, value, .. } = cmd
                        && let (Ok(k), Ok(v)) = (
                            String::from_utf8(key.clone()),
                            String::from_utf8(value.clone()),
                        )
                    {
                        sm.data.insert(k, v);
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

        let bytes = snapshot.into_inner();
        if let Ok(data) = serde_json::from_slice(&bytes) {
            sm.data = data;
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<NodusTypeConfig>>, StorageError<u64>> {
        Ok(None)
    }
}
