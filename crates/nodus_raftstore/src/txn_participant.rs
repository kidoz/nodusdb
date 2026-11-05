//! Dormant participant protocol. Trusted resolution is constructed only after a
//! linearizable meta-authority read. Public ingress remains closed. Terminal
//! fences are retained; a committed fence and user data share one KV commit.

mod legacy;
mod resolution;

use anyhow::{Result, ensure};
use bytes::Bytes;
use nodus_storage_api::{KvEngine, TxnId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    ShardResponse, migration,
    txn_authority::{OwnerV1, RecordV1 as DecisionV1},
};

pub(crate) use legacy::reject_legacy;
pub use legacy::validate_legacy_repair;
pub(crate) use resolution::validate_snapshot;

/// Reserved participant control records, included in group snapshots.
pub const PREFIX: &[u8] = b"\x01txn-participant/";
const V1_PREFIX: &[u8] = b"\x01txn-participant/v1/";
const MAX_RECORD_BYTES: usize = 4096;

/// Preparation freezes further mutation; terminal states have no age-based expiry.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PhaseV1 {
    Active,
    Prepared { keys_hash: [u8; 32] },
    Committed { commit_ts: u64 },
    Aborted,
}

/// Group-local identity and fence for one tracked transaction.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordV1 {
    pub txn_id: Uuid,
    pub owner: OwnerV1,
    pub generation: u64,
    pub epoch: u64,
    pub phase: PhaseV1,
}

/// Versioned participant operations; write batches retain existing size bounds.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OperationV1 {
    Write {
        operations: Vec<migration::EpochIntentOperationV1>,
    },
    Repair {
        key: Vec<u8>,
        replacement: migration::writes::ReplacementV2,
    },
    Prepare {
        keys_hash: [u8; 32],
    },
    /// This is a trusted resolver message, not a caller-provided certificate.
    Resolve {
        decision: Box<DecisionV1>,
    },
}

/// Mutation, preparation and resolution all bind the same participant epoch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandV1 {
    pub txn_id: Uuid,
    pub owner: OwnerV1,
    pub generation: u64,
    pub epoch: u64,
    pub operation: OperationV1,
}

fn key(txn: Uuid) -> Vec<u8> {
    [V1_PREFIX, txn.to_string().as_bytes()].concat()
}

fn validate(record: &RecordV1) -> Result<()> {
    ensure!(
        !record.txn_id.is_nil() && !record.owner.incarnation.is_nil(),
        "nil participant identity"
    );
    ensure!(record.generation > 0, "zero participant generation");
    ensure!(
        record.generation == 1 || record.phase == PhaseV1::Aborted,
        "invalid participant generation"
    );
    if let PhaseV1::Committed { commit_ts } = record.phase {
        ensure!(commit_ts > 0, "zero participant commit timestamp");
    }
    Ok(())
}

/// Decode only a valid version-1 envelope; malformed/future records fail closed.
pub fn decode(bytes: &[u8]) -> Result<RecordV1> {
    ensure!(
        bytes.len() <= MAX_RECORD_BYTES,
        "participant record too large"
    );
    let record = match nodus_common::versioned::decode(bytes) {
        nodus_common::versioned::Envelope::Versioned {
            version: 1,
            payload,
        } => serde_json::from_slice(payload)?,
        _ => anyhow::bail!("unsupported participant record envelope"),
    };
    validate(&record)?;
    Ok(record)
}

/// Inspect local participant state. This is not a meta decision certificate.
pub fn read(kv: &dyn KvEngine, txn: Uuid) -> Result<Option<RecordV1>> {
    let record = kv
        .get(&key(txn), u64::MAX)?
        .map(|v| decode(&v))
        .transpose()?;
    if let Some(record) = &record {
        ensure!(record.txn_id == txn, "participant key/ID mismatch");
    }
    Ok(record)
}

/// A prepared transaction whose savepoint cleared all keys still owns this
/// epoch. Migration must not advance it until an authoritative resolution.
pub(crate) fn has_unresolved(kv: &dyn KvEngine) -> Result<bool> {
    for row in kv.scan(
        nodus_storage_api::KeyRange {
            start: Bytes::from_static(PREFIX),
            end: Bytes::from_static(b"\x01txn-participant0"),
        },
        u64::MAX,
    )? {
        let row = row?;
        let record = decode(&row.value)?;
        ensure!(
            key(record.txn_id) == row.key.as_ref(),
            "participant key/ID mismatch"
        );
        if matches!(record.phase, PhaseV1::Active | PhaseV1::Prepared { .. }) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Hash a canonical, length-delimited set of acknowledged user-intent keys.
/// Values remain protected by their Raft acknowledgements; prepare freezes all
/// further writes and repairs, including writes to an already present key.
pub fn digest_keys(keys: &[Vec<u8>]) -> [u8; 32] {
    let mut keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    keys.sort_unstable();
    keys.dedup();
    let mut hash = Sha256::new();
    hash.update(b"nodus-participant-keys-v1");
    for key in keys {
        hash.update((key.len() as u64).to_be_bytes());
        hash.update(key);
    }
    hash.finalize().into()
}

fn pending_hash(kv: &dyn KvEngine, txn: Uuid) -> [u8; 32] {
    let marker = key(txn);
    let keys = kv
        .pending_intent_keys(TxnId(txn))
        .into_iter()
        .filter(|k| k.as_ref() != marker)
        .map(|k| k.to_vec())
        .collect::<Vec<_>>();
    digest_keys(&keys)
}

fn encode(record: &RecordV1) -> Result<Bytes> {
    validate(record)?;
    Ok(Bytes::from(nodus_common::versioned::encode(
        1,
        &serde_json::to_vec(record)?,
    )))
}

fn save(kv: &dyn KvEngine, record: &RecordV1, index: u64) -> Result<()> {
    let record_key = key(record.txn_id);
    let txn = TxnId(Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        &[record_key.as_slice(), &index.to_be_bytes()].concat(),
    ));
    kv.abort(txn)?;
    kv.write_intent(txn, Bytes::from(record_key), encode(record)?)?;
    kv.commit(txn, index)?;
    Ok(())
}

fn reject(error: impl ToString) -> ShardResponse {
    ShardResponse {
        success: false,
        error: Some(error.to_string()),
    }
}

fn accepted() -> ShardResponse {
    ShardResponse {
        success: true,
        error: None,
    }
}

/// Called only under the participant's exclusive Raft state-machine lock.
pub(crate) fn apply(
    kv: &dyn KvEngine,
    group: &str,
    cmd: &CommandV1,
    index: u64,
) -> Result<ShardResponse> {
    // Durable read/validation errors must stop apply, never turn into absence.
    let old = read(kv, cmd.txn_id)?;
    if let OperationV1::Resolve { decision } = &cmd.operation {
        return resolution::apply(kv, group, cmd, decision, old.as_ref(), index);
    }
    let identity = RecordV1 {
        txn_id: cmd.txn_id,
        owner: cmd.owner.clone(),
        generation: cmd.generation,
        epoch: cmd.epoch,
        phase: PhaseV1::Active,
    };
    if let Err(error) = validate(&identity) {
        return Ok(reject(error));
    }
    let fence = migration::read_fence(kv)?;
    if fence.as_ref().map_or(0, |f| f.epoch) != cmd.epoch || fence.is_some_and(|f| f.closed) {
        return Ok(reject(
            "stale epoch or fenced participant; retry transaction",
        ));
    }
    if let Some(old) = &old {
        if old.owner != cmd.owner || old.generation != cmd.generation || old.epoch != cmd.epoch {
            return Ok(reject("stale participant owner, generation or epoch"));
        }
    } else if !matches!(cmd.operation, OperationV1::Write { .. })
        || !kv.pending_intent_keys(TxnId(cmd.txn_id)).is_empty()
    {
        return Ok(reject("participant is absent or has untracked intents"));
    }
    let phase = old.as_ref().map_or(&PhaseV1::Active, |r| &r.phase);
    match &cmd.operation {
        OperationV1::Write { operations } => {
            if *phase != PhaseV1::Active {
                return Ok(reject("participant no longer accepts writes"));
            }
            if let Some(error) = migration::invalid_write_batch(operations) {
                return Ok(reject(error));
            }
            if operations.iter().any(|op| {
                migration::is_control_key(match op {
                    migration::EpochIntentOperationV1::Put { key, .. }
                    | migration::EpochIntentOperationV1::Delete { key } => key,
                }) || match op {
                    migration::EpochIntentOperationV1::Put { key, .. }
                    | migration::EpochIntentOperationV1::Delete { key } => {
                        key.starts_with(b"\x01upgrade/")
                    }
                }
            }) {
                return Ok(reject("reserved participant control key"));
            }
            if old.is_none() {
                save(kv, &identity, index)?;
            }
            let ops = operations
                .iter()
                .map(|op| match op {
                    migration::EpochIntentOperationV1::Put { key, value } => {
                        nodus_storage_api::IntentOperation::Put {
                            key: Bytes::copy_from_slice(key),
                            value: Bytes::copy_from_slice(value),
                        }
                    }
                    migration::EpochIntentOperationV1::Delete { key } => {
                        nodus_storage_api::IntentOperation::Delete {
                            key: Bytes::copy_from_slice(key),
                        }
                    }
                })
                .collect();
            match kv.write_intent_batch(TxnId(cmd.txn_id), ops) {
                Ok(()) => Ok(accepted()),
                Err(nodus_storage_api::KvError::WriteConflict(_)) => {
                    Ok(reject("write conflict; retry transaction"))
                }
                Err(error) => Err(error.into()),
            }
        }
        OperationV1::Repair { key, replacement } => {
            if *phase != PhaseV1::Active {
                return Ok(reject("participant no longer accepts repairs"));
            }
            migration::writes::apply_repair(kv, cmd.epoch, cmd.txn_id, key, replacement)
        }
        OperationV1::Prepare { keys_hash } => {
            if !matches!(phase, PhaseV1::Active | PhaseV1::Prepared { .. })
                || pending_hash(kv, cmd.txn_id) != *keys_hash
            {
                return Ok(reject(
                    "participant write set changed or transaction decided",
                ));
            }
            if let PhaseV1::Prepared {
                keys_hash: old_hash,
            } = phase
            {
                return Ok(if old_hash == keys_hash {
                    accepted()
                } else {
                    reject("conflicting prepare")
                });
            }
            let mut record = identity;
            record.phase = PhaseV1::Prepared {
                keys_hash: *keys_hash,
            };
            save(kv, &record, index)?;
            Ok(accepted())
        }
        OperationV1::Resolve { .. } => unreachable!("resolution handled above"),
    }
}

#[cfg(test)]
mod tests;
