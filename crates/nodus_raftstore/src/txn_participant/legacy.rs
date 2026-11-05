use super::*;
use crate::{ShardCommand, migration::EpochMutationV1};

/// Legacy savepoint repairs run outside Raft but under the same apply lock.
/// They must not mutate protocol metadata or tracked transactions.
pub fn validate_legacy_repair(kv: &dyn KvEngine, txn: TxnId, key: &[u8]) -> Result<()> {
    ensure!(
        !migration::is_control_key(key) && !key.starts_with(b"\x01upgrade/"),
        "reserved control key"
    );
    ensure!(
        read(kv, txn.0)?.is_none(),
        "tracked transaction requires participant protocol"
    );
    Ok(())
}

pub(crate) fn reject_legacy(
    kv: &dyn KvEngine,
    cmd: &ShardCommand,
    tracking: bool,
) -> Result<Option<ShardResponse>> {
    if !tracking && !matches!(cmd, ShardCommand::AbortTxn { recorded: true, .. }) {
        return Ok(None);
    }
    let ids: Vec<Uuid> = match cmd {
        ShardCommand::AbortTxn { recorded: true, .. } => {
            // Physical IDs cannot be reversed to session IDs. On the raw meta
            // engine they can even name another group's intents. No current
            // writer emits these historical age-only cleanup commands; reject
            // them everywhere, including replay, rather than bypass ownership.
            // Explicit legacy owner rollbacks use recorded=false and still work.
            return Ok(Some(reject(
                "recorded-ID abort has no authoritative decision",
            )));
        }
        ShardCommand::PutIntent { txn_id, .. }
        | ShardCommand::DeleteIntent { txn_id, .. }
        | ShardCommand::IndexPutIntent { txn_id, .. }
        | ShardCommand::IndexDeleteIntent { txn_id, .. }
        | ShardCommand::CommitTxn { txn_id, .. }
        | ShardCommand::AbortTxn { txn_id, .. }
        | ShardCommand::PrepareTxn { txn_id, .. } => {
            Uuid::parse_str(txn_id).ok().into_iter().collect()
        }
        ShardCommand::EpochWriteV1 { mutation, .. } => vec![match mutation {
            EpochMutationV1::Put { txn_id, .. }
            | EpochMutationV1::Delete { txn_id, .. }
            | EpochMutationV1::Commit { txn_id, .. }
            | EpochMutationV1::Prepare { txn_id } => *txn_id,
        }],
        ShardCommand::EpochPrepareV2 { txn_id, .. }
        | ShardCommand::EpochRepairV2 { txn_id, .. }
        | ShardCommand::EpochWriteBatchV1 { txn_id, .. } => vec![*txn_id],
        ShardCommand::EpochCommitBatchV1 { commits, .. } => {
            commits.iter().map(|c| c.txn_id).collect()
        }
        _ => Vec::new(),
    };
    for id in ids {
        if read(kv, id)?.is_some() {
            return Ok(Some(reject(
                "tracked transaction requires participant protocol",
            )));
        }
    }
    Ok(None)
}
