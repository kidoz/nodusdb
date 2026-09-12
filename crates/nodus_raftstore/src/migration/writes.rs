//! Replicated savepoint repairs use the transaction's original participant epoch.
use super::*;
use nodus_storage_api::IntentReplacement;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplacementV2 {
    Put(Vec<u8>),
    Delete,
    Clear,
}

impl From<IntentReplacement> for ReplacementV2 {
    fn from(value: IntentReplacement) -> Self {
        match value {
            IntentReplacement::Put(value) => Self::Put(value.to_vec()),
            IntentReplacement::Delete => Self::Delete,
            IntentReplacement::Clear => Self::Clear,
        }
    }
}

pub(super) fn apply_repair(
    kv: &dyn KvEngine,
    epoch: u64,
    txn: Uuid,
    key: &[u8],
    replacement: &ReplacementV2,
) -> Result<ShardResponse> {
    let fence = read_fence(kv)?;
    if fence.as_ref().map_or(0, |f| f.epoch) != epoch || fence.is_some_and(|f| f.closed) {
        return Ok(reject("stale savepoint epoch; retry transaction"));
    }
    if is_control_key(key) {
        return Ok(reject("reserved migration key"));
    }
    let replacement = match replacement {
        ReplacementV2::Put(value) => IntentReplacement::Put(Bytes::copy_from_slice(value)),
        ReplacementV2::Delete => IntentReplacement::Delete,
        ReplacementV2::Clear => IntentReplacement::Clear,
    };
    match kv.replace_intent(TxnId(txn), Bytes::copy_from_slice(key), replacement) {
        Ok(()) => Ok(accepted()),
        Err(nodus_storage_api::KvError::WriteConflict(_)) => Ok(reject("savepoint write conflict")),
        Err(e) => Err(e.into()),
    }
}
