use nodus_storage_api::{Timestamp, TxnId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MvccValue {
    pub value: Option<Vec<u8>>, // None = Tombstone
    pub version: Timestamp,
    pub txn_id: Option<TxnId>,
    pub is_intent: bool,
}

impl MvccValue {
    pub fn is_visible(&self, read_ts: Timestamp) -> bool {
        !self.is_intent && self.version <= read_ts && self.value.is_some()
    }
}
