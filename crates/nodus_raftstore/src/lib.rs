use serde::{Deserialize, Serialize};
use std::io::Cursor;

openraft::declare_raft_types!(
    /// Declare the type configuration for `openraft`.
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

// In MVP, we would implement openraft::RaftStorage / RaftLogReader / RaftStateMachine traits here,
// using MemKvEngine or MemTxnManager internally.
