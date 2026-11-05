//! Dormant transaction decision authority. No production writer emits these
//! commands and forwarded ingress rejects them until cluster-wide activation
//! and participant fencing are implemented. Apply must hold the meta Raft lock.

use anyhow::{Result, ensure};
use bytes::Bytes;
use nodus_storage_api::{KvEngine, TxnId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::ShardResponse;

/// Included in the snapshot user range; never reclaim terminal records by age.
pub const PREFIX: &[u8] = b"\x01txn-authority/";
const V1_PREFIX: &[u8] = b"\x01txn-authority/v1/";
const MAX_PARTICIPANTS: usize = 256;
const MAX_GROUP_BYTES: usize = 512;
const MAX_RECORD_BYTES: usize = 256 * 1024;

/// A node ID alone is insufficient to identify a coordinator after restart.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnerV1 {
    pub node_id: u64,
    pub incarnation: Uuid,
}

impl OwnerV1 {
    fn validate(&self) -> Result<()> {
        ensure!(!self.incarnation.is_nil(), "nil coordinator incarnation");
        Ok(())
    }
}

/// A conditional mutation must match all three fields, not just its owner.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenV1 {
    pub owner: OwnerV1,
    pub generation: u64,
    pub revision: u64,
}

/// Only a prepared, frozen participant set may acquire a commit decision.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PhaseV1 {
    Active,
    Preparing,
    Committed { commit_ts: u64 },
    Aborted,
}

/// Retained terminal decisions prevent an old transaction ID being resurrected.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordV1 {
    pub txn_id: Uuid,
    pub token: TokenV1,
    pub participants: BTreeMap<String, u64>,
    pub phase: PhaseV1,
}

impl RecordV1 {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(!self.txn_id.is_nil(), "nil transaction ID");
        self.token.owner.validate()?;
        ensure!(
            self.token.generation > 0 && self.token.revision >= self.token.generation,
            "invalid transaction generation or revision"
        );
        ensure!(
            self.token.generation == 1 || self.phase == PhaseV1::Aborted,
            "a fenced transaction must be aborted"
        );
        ensure!(
            self.participants.len() <= MAX_PARTICIPANTS,
            "too many participants"
        );
        for group in self.participants.keys() {
            validate_group(group)?;
        }
        if matches!(self.phase, PhaseV1::Preparing | PhaseV1::Committed { .. }) {
            ensure!(!self.participants.is_empty(), "no transaction participants");
        }
        if let PhaseV1::Committed { commit_ts } = self.phase {
            ensure!(commit_ts > 0, "zero commit timestamp");
        }
        Ok(())
    }
}

/// No timeout or local-session absence is a transition precondition. A future
/// trusted recovery service may choose abort only for fully tracked transactions.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OperationV1 {
    Begin { owner: OwnerV1 },
    Enroll { group: String, epoch: u64 },
    Seal,
    Commit { commit_ts: u64 },
    Abort,
    FenceAbort { recovery: OwnerV1 },
}

/// Versioned Raft command; `expected` is absent only for initial creation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandV1 {
    pub txn_id: Uuid,
    pub expected: Option<TokenV1>,
    pub operation: OperationV1,
}

fn validate_group(group: &str) -> Result<()> {
    ensure!(
        !group.is_empty() && group.len() <= MAX_GROUP_BYTES && !group.contains('\0'),
        "invalid participant group"
    );
    Ok(())
}

fn key(txn: Uuid) -> Vec<u8> {
    [V1_PREFIX, txn.to_string().as_bytes()].concat()
}

/// Reject unknown envelopes, malformed records and legacy unversioned payloads.
pub fn decode(bytes: &[u8]) -> Result<RecordV1> {
    ensure!(
        bytes.len() <= MAX_RECORD_BYTES,
        "transaction authority record too large"
    );
    let record: RecordV1 = match nodus_common::versioned::decode(bytes) {
        nodus_common::versioned::Envelope::Versioned {
            version: 1,
            payload,
        } => serde_json::from_slice(payload)?,
        nodus_common::versioned::Envelope::Versioned { version, .. } => {
            anyhow::bail!("unsupported transaction authority version {version}")
        }
        nodus_common::versioned::Envelope::Legacy(_) => {
            anyhow::bail!("transaction authority requires a versioned envelope")
        }
    };
    record.validate()?;
    Ok(record)
}

/// A local read is useful for inspection; it is not a linearizable certificate.
pub fn read(kv: &dyn KvEngine, txn: Uuid) -> Result<Option<RecordV1>> {
    let record = kv
        .get(&key(txn), u64::MAX)?
        .map(|b| decode(&b))
        .transpose()?;
    if let Some(record) = &record {
        ensure!(
            record.txn_id == txn,
            "transaction authority key/ID mismatch"
        );
    }
    Ok(record)
}

/// Snapshot replacement cannot erase a decision or restore an older owner.
pub(crate) fn validate_snapshot_record(
    kv: &dyn KvEngine,
    record_key: &[u8],
    value: &[u8],
) -> Result<()> {
    let incoming = decode(value)?;
    ensure!(
        key(incoming.txn_id) == record_key,
        "transaction authority key/ID mismatch"
    );
    if let Some(current) = read(kv, incoming.txn_id)? {
        ensure!(
            incoming.token.revision >= current.token.revision,
            "snapshot would roll back transaction authority"
        );
        if incoming.token.revision == current.token.revision {
            ensure!(
                incoming == current,
                "conflicting transaction authority revision"
            );
        }
        ensure!(
            incoming.token.generation >= current.token.generation,
            "snapshot would roll back transaction generation"
        );
        if incoming.token.generation == current.token.generation {
            ensure!(
                incoming.token.owner == current.token.owner,
                "snapshot changes owner without fencing"
            );
        } else {
            ensure!(
                current.token.generation.checked_add(1) == Some(incoming.token.generation)
                    && incoming.phase == PhaseV1::Aborted
                    && incoming.token.owner != current.token.owner,
                "snapshot contains an invalid transaction fence"
            );
        }
        ensure!(
            current
                .participants
                .iter()
                .all(|(group, epoch)| incoming.participants.get(group) == Some(epoch)),
            "snapshot removes or changes a participant"
        );
        if current.phase == PhaseV1::Preparing {
            ensure!(
                incoming.phase != PhaseV1::Active && incoming.participants == current.participants,
                "snapshot changes a sealed participant set"
            );
        }
        if matches!(current.phase, PhaseV1::Committed { .. } | PhaseV1::Aborted) {
            ensure!(
                incoming == current,
                "snapshot would change a transaction decision"
            );
        }
    }
    Ok(())
}

fn transition(old: Option<&RecordV1>, cmd: &CommandV1) -> Result<RecordV1> {
    ensure!(!cmd.txn_id.is_nil(), "nil transaction ID");
    if let OperationV1::Begin { owner } = &cmd.operation {
        owner.validate()?;
        ensure!(cmd.expected.is_none(), "begin must not carry a token");
        ensure!(old.is_none(), "transaction ID already registered");
        return Ok(RecordV1 {
            txn_id: cmd.txn_id,
            token: TokenV1 {
                owner: owner.clone(),
                generation: 1,
                revision: 1,
            },
            participants: BTreeMap::new(),
            phase: PhaseV1::Active,
        });
    }
    let old = old.ok_or_else(|| anyhow::anyhow!("transaction authority is absent"))?;
    let token = cmd
        .expected
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing transaction token"))?;
    ensure!(
        old.token.owner == token.owner && old.token.generation == token.generation,
        "stale transaction owner or generation"
    );
    // A retry after a lost decision response is safe only for the same exact
    // terminal outcome. Revision can be stale, but cannot come from the future.
    let same_terminal = match (&old.phase, &cmd.operation) {
        (PhaseV1::Committed { commit_ts: a }, OperationV1::Commit { commit_ts: b }) => a == b,
        (PhaseV1::Aborted, OperationV1::Abort) => true,
        _ => false,
    };
    if same_terminal && token.revision > 0 && token.revision <= old.token.revision {
        return Ok(old.clone());
    }
    ensure!(old.token == *token, "stale transaction revision");
    ensure!(
        matches!(old.phase, PhaseV1::Active | PhaseV1::Preparing),
        "transaction already decided"
    );
    let mut next = old.clone();
    next.token.revision = old
        .token
        .revision
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("transaction revision exhausted"))?;
    match &cmd.operation {
        OperationV1::Begin { .. } => unreachable!("begin handled above"),
        OperationV1::Enroll { group, epoch } => {
            ensure!(old.phase == PhaseV1::Active, "participant set is sealed");
            validate_group(group)?;
            if let Some(existing) = old.participants.get(group) {
                ensure!(existing == epoch, "participant epoch changed");
            }
            next.participants.insert(group.clone(), *epoch);
        }
        OperationV1::Seal => {
            ensure!(
                old.phase == PhaseV1::Active,
                "participant set already sealed"
            );
            ensure!(!old.participants.is_empty(), "no transaction participants");
            next.phase = PhaseV1::Preparing;
        }
        OperationV1::Commit { commit_ts } => {
            ensure!(
                old.phase == PhaseV1::Preparing,
                "transaction is not preparing"
            );
            ensure!(*commit_ts > 0, "zero commit timestamp");
            next.phase = PhaseV1::Committed {
                commit_ts: *commit_ts,
            };
        }
        OperationV1::Abort => next.phase = PhaseV1::Aborted,
        OperationV1::FenceAbort { recovery } => {
            recovery.validate()?;
            ensure!(recovery != &old.token.owner, "recovery must change owner");
            next.token.generation = old
                .token
                .generation
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("transaction generation exhausted"))?;
            next.token.owner = recovery.clone();
            next.phase = PhaseV1::Aborted;
        }
    }
    next.validate()?;
    Ok(next)
}

/// Apply only under the meta group's exclusive Raft apply lock. Decode and
/// storage failures are fatal; conditional conflicts are ordinary rejections.
pub(crate) fn apply(kv: &dyn KvEngine, cmd: &CommandV1, index: u64) -> Result<ShardResponse> {
    let old = read(kv, cmd.txn_id)?;
    let next = match transition(old.as_ref(), cmd) {
        Ok(next) => next,
        Err(error) => {
            return Ok(ShardResponse {
                success: false,
                error: Some(error.to_string()),
            });
        }
    };
    if old.as_ref() != Some(&next) {
        let payload = serde_json::to_vec(&next)?;
        let bytes = nodus_common::versioned::encode(1, &payload);
        ensure!(
            bytes.len() <= MAX_RECORD_BYTES,
            "transaction authority record too large"
        );
        let record_key = key(cmd.txn_id);
        let write_id = TxnId(Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            &[record_key.as_slice(), &index.to_be_bytes()].concat(),
        ));
        kv.abort(write_id)?;
        kv.write_intent(write_id, Bytes::from(record_key), Bytes::from(bytes))?;
        kv.commit(write_id, index)?;
    }
    Ok(ShardResponse {
        success: true,
        error: None,
    })
}

#[cfg(test)]
mod tests;
