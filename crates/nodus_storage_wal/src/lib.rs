use nodus_storage_api::{Timestamp, TxnId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecordV1 {
    BeginTxn {
        txn_id: TxnId,
    },
    WriteIntent {
        txn_id: TxnId,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    CommitTxn {
        txn_id: TxnId,
        commit_ts: Timestamp,
    },
    AbortTxn {
        txn_id: TxnId,
    },
    IndexPutIntent {
        txn_id: TxnId,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    IndexDeleteIntent {
        txn_id: TxnId,
        key: Vec<u8>,
    },
    Checkpoint {
        ts: Timestamp,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    V1(WalRecordV1),
}

pub trait WalEngine: Send + Sync {
    fn append(&self, record: WalRecord) -> anyhow::Result<u64>; // returns LSN
    fn sync(&self) -> anyhow::Result<()>;
}
