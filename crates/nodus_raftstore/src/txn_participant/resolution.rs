use super::*;
use crate::txn_authority::PhaseV1 as DecisionPhase;

pub(super) fn apply(
    kv: &dyn KvEngine,
    group: &str,
    cmd: &CommandV1,
    decision: &DecisionV1,
    old: Option<&RecordV1>,
    index: u64,
) -> Result<ShardResponse> {
    // A malformed message is rejected. Corrupt existing storage was already
    // rejected as a fatal error by the caller's durable read.
    let next = match validate_decision(group, cmd, decision, old) {
        Ok(record) => record,
        Err(error) => return Ok(reject(error)),
    };
    match next.phase {
        PhaseV1::Committed { commit_ts } => {
            if old == Some(&next) {
                return Ok(accepted());
            }
            let fence = migration::read_fence(kv)?;
            if fence.as_ref().map_or(0, |f| f.epoch) != cmd.epoch || fence.is_some_and(|f| f.closed)
            {
                return Ok(reject("stale epoch or fenced participant; commit refused"));
            }
            let Some(RecordV1 {
                phase: PhaseV1::Prepared { keys_hash },
                ..
            }) = old
            else {
                return Ok(reject("commit requires a prepared participant"));
            };
            if pending_hash(kv, cmd.txn_id) != *keys_hash {
                return Ok(reject("prepared participant lost intents; commit refused"));
            }
            // Previous participant metadata was committed at Raft indexes.
            // HLC timestamps must sort above it so the terminal marker wins.
            if commit_ts <= index {
                return Ok(reject(
                    "commit timestamp must exceed participant Raft index",
                ));
            }
            // Atomic publication: there is no durable committed marker without
            // the user writes, and no user commit without its terminal fence.
            kv.write_intent(
                TxnId(cmd.txn_id),
                Bytes::from(key(cmd.txn_id)),
                encode(&next)?,
            )?;
            kv.commit(TxnId(cmd.txn_id), commit_ts)?;
        }
        PhaseV1::Aborted => {
            if old.is_none() && !kv.pending_intent_keys(TxnId(cmd.txn_id)).is_empty() {
                return Ok(reject("cannot abort untracked participant intents"));
            }
            if old != Some(&next) {
                save(kv, &next, index)?;
            }
            // Always repeat abort: a crash can leave the fence durable before
            // its invisible user intents have been removed.
            kv.abort(TxnId(cmd.txn_id))?;
        }
        _ => unreachable!("validated terminal decision"),
    }
    Ok(accepted())
}

fn validate_decision(
    group: &str,
    cmd: &CommandV1,
    decision: &DecisionV1,
    old: Option<&RecordV1>,
) -> Result<RecordV1> {
    decision.validate()?;
    ensure!(
        decision.txn_id == cmd.txn_id
            && decision.token.owner == cmd.owner
            && decision.token.generation == cmd.generation,
        "decision and participant identities differ"
    );
    ensure!(
        decision.participants.get(group) == Some(&cmd.epoch),
        "decision does not enroll this participant epoch"
    );
    let phase = match decision.phase {
        DecisionPhase::Committed { commit_ts } => PhaseV1::Committed { commit_ts },
        DecisionPhase::Aborted => PhaseV1::Aborted,
        _ => anyhow::bail!("resolution requires a terminal authority decision"),
    };
    let next = RecordV1 {
        txn_id: cmd.txn_id,
        owner: cmd.owner.clone(),
        generation: cmd.generation,
        epoch: cmd.epoch,
        phase,
    };
    validate(&next)?;
    if let Some(old) = old {
        ensure!(
            old.epoch == next.epoch,
            "participant epoch differs from decision"
        );
        if matches!(old.phase, PhaseV1::Committed { .. } | PhaseV1::Aborted) {
            ensure!(old == &next, "participant terminal decision is immutable");
        } else if next.generation == old.generation {
            ensure!(
                next.owner == old.owner,
                "participant owner changed without fencing"
            );
        } else {
            ensure!(
                next.phase == PhaseV1::Aborted
                    && old.generation.checked_add(1) == Some(next.generation)
                    && old.owner != next.owner,
                "stale or invalid participant generation"
            );
        }
    }
    Ok(next)
}

/// Snapshot validation also protects records from omissions through the shared
/// control-record checks in snapshots.rs. Terminal decisions never regress.
pub(crate) fn validate_snapshot(kv: &dyn KvEngine, record_key: &[u8], bytes: &[u8]) -> Result<()> {
    let incoming = decode(bytes)?;
    ensure!(
        key(incoming.txn_id) == record_key,
        "participant key/ID mismatch"
    );
    if let Some(current) = read(kv, incoming.txn_id)? {
        ensure!(
            incoming.epoch == current.epoch,
            "snapshot changes participant epoch"
        );
        if matches!(current.phase, PhaseV1::Committed { .. } | PhaseV1::Aborted) {
            ensure!(
                incoming == current,
                "snapshot changes a participant decision"
            );
        } else if incoming.generation == current.generation {
            ensure!(
                incoming.owner == current.owner,
                "snapshot changes participant owner"
            );
            if let PhaseV1::Prepared { keys_hash } = current.phase {
                ensure!(
                    incoming.phase != PhaseV1::Active,
                    "snapshot unprepares a participant"
                );
                if let PhaseV1::Prepared {
                    keys_hash: incoming_hash,
                } = incoming.phase
                {
                    ensure!(
                        keys_hash == incoming_hash,
                        "snapshot changes prepared write set"
                    );
                }
            }
        } else {
            ensure!(
                current.generation.checked_add(1) == Some(incoming.generation)
                    && incoming.phase == PhaseV1::Aborted
                    && incoming.owner != current.owner,
                "snapshot contains invalid participant fencing"
            );
        }
    }
    Ok(())
}
