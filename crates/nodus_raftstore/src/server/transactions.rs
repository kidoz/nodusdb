//! Authenticated protocol ingress. Records supplied by a peer are compared to
//! the locally applied authority only after a successful meta ReadIndex.
use super::*;
use crate::{
    txn_authority as authority, txn_participant as participant, txn_resolution as journal, upgrade,
};
use anyhow::{Result, ensure};

pub(super) async fn verify(state: &RaftState, group: &str, cmd: &ShardCommand) -> Result<bool> {
    match cmd {
        ShardCommand::TxnAuthorityV1(_)
        | ShardCommand::TxnResolutionV1(journal::CommandV1::Advance { .. }) => {
            return Ok(group == "shard-meta");
        }
        _ => {}
    }
    let meta = get_raft(state, "shard-meta")
        .await
        .ok_or_else(|| anyhow::anyhow!("meta group unavailable for decision verification"))?;
    let transport = state
        .peer_transport
        .get()
        .ok_or_else(|| anyhow::anyhow!("peer transport unavailable"))?;
    crate::read_barrier::ensure_local(meta, "shard-meta", transport).await?;
    let kv = state
        .capability_kv
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("authority storage unavailable"))?;
    let group = group.to_owned();
    let cmd = cmd.clone();
    tokio::task::spawn_blocking(move || matches_authority(kv.as_ref(), &group, &cmd)).await?
}

fn matches_authority(
    kv: &dyn nodus_storage_api::KvEngine,
    group: &str,
    cmd: &ShardCommand,
) -> Result<bool> {
    match cmd {
        ShardCommand::TxnParticipantV1(cmd) => {
            let Some(record) = authority::read(kv, cmd.txn_id)? else {
                return Ok(false);
            };
            if record.token.owner != cmd.owner
                || record.token.generation != cmd.generation
                || record.participants.get(group) != Some(&cmd.epoch)
            {
                return Ok(false);
            }
            Ok(match &cmd.operation {
                participant::OperationV1::Write { .. }
                | participant::OperationV1::Repair { .. } => {
                    record.phase == authority::PhaseV1::Active
                }
                participant::OperationV1::Prepare { .. } => {
                    record.phase == authority::PhaseV1::Preparing
                }
                participant::OperationV1::Resolve { decision } => {
                    **decision == record
                        && matches!(
                            record.phase,
                            authority::PhaseV1::Committed { .. } | authority::PhaseV1::Aborted
                        )
                }
            })
        }
        ShardCommand::TxnResolutionV1(journal::CommandV1::Acknowledge {
            txn_id,
            decision,
            group: participant,
        }) => {
            let Some(record) = authority::read(kv, *txn_id)? else {
                return Ok(false);
            };
            Ok(group == "shard-meta"
                && record.token == *decision
                && record.participants.contains_key(participant)
                && matches!(
                    record.phase,
                    authority::PhaseV1::Committed { .. } | authority::PhaseV1::Aborted
                ))
        }
        _ => Ok(false),
    }
}

/// A stale binary reusing a member ID cannot win votes or append legacy traffic
/// after the reader boundary. Probes bind the declared sender to the approved
/// roster address and a fresh authenticated challenge; capability RPCs do not
/// recursively perform this check.
pub(super) async fn verify_consensus_peer(
    state: &RaftState,
    mode: Option<axum::Extension<RaftAuthMode>>,
    peer: Option<u64>,
) -> Result<()> {
    let Some(kv) = state.capability_kv.get() else {
        return Ok(());
    };
    let authority = upgrade::read(kv.as_ref())?;
    if !upgrade::requires_txn_reader(&authority) {
        return Ok(());
    }
    ensure!(
        matches!(
            mode,
            Some(axum::Extension(RaftAuthMode::TransportAuthenticated))
        ),
        "transaction consensus requires peer mTLS"
    );
    let id = peer.ok_or_else(|| anyhow::anyhow!("consensus sender identity missing"))?;
    let roster =
        upgrade::admission::approved(&authority, upgrade::admission::read(kv.as_ref())?.as_ref())?;
    let addr = roster
        .get(&id)
        .ok_or_else(|| anyhow::anyhow!("consensus sender is not approved"))?;
    let transport = state
        .peer_transport
        .get()
        .ok_or_else(|| anyhow::anyhow!("peer transport unavailable"))?;
    let report = transport
        .probe(id, addr, uuid::Uuid::new_v4(), false)
        .await?;
    upgrade::require_txn_reader(&report)
}
