//! R3 protocol foundation. All transitions run under the owning group's Raft
//! apply lock. Network activation is deliberately disabled until cluster-wide
//! format negotiation and the migration coordinator are implemented.

use anyhow::{Result, bail};
use bytes::Bytes;
use nodus_catalog::TableId;
use nodus_storage_api::{KvEngine, TxnId};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{ShardCommand, ShardResponse};

// Included in the existing snapshot user range, unlike the \0raft prefix.
const PREFIX: &[u8] = b"\x01migration/v1/";
const FENCE_KEY: &[u8] = b"\x01migration/v1/fence";
const RECORD_VERSION: u16 = 1;

/// Only pre-copy phases are available. CancelRequested is not an assertion that
/// participant fences have been released. No command can publish a new map.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    Planned,
    Fencing,
    CancelRequested,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub operation_id: Uuid,
    pub table_id: TableId,
    pub source_epochs: std::collections::BTreeMap<String, u64>,
    pub sources: Vec<String>,
    pub destinations: Vec<String>,
}

/// One active journal per table. A future coordinator must verify participant
/// acknowledgements before advancing beyond these pre-copy phases.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub plan: MigrationPlan,
    pub phase: MigrationPhase,
}

/// Epochs are group-local and never decrease, including when a migration is
/// cancelled. The latest operation ID makes lost-response retries idempotent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantFence {
    pub operation_id: Uuid,
    pub epoch: u64,
    pub closed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MigrationCommandV1 {
    Plan(MigrationPlan),
    StartFencing {
        table_id: TableId,
        operation_id: Uuid,
    },
    RequestCancel {
        table_id: TableId,
        operation_id: Uuid,
    },
    /// Refuses while *any* intents remain on this participant, including those
    /// of prepared transactions. This slice never invalidates live intents.
    Acquire {
        operation_id: Uuid,
        expected_epoch: u64,
    },
    /// Cancellation only: reopen the same owner at its advanced epoch. This is
    /// not a cutover or permission to remove the source group.
    Release {
        operation_id: Uuid,
        epoch: u64,
    },
}

/// Versioned mutations for an epoch-aware writer. Existing producers continue
/// using legacy commands until activation is proven safe for every replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EpochMutationV1 {
    Put {
        txn_id: Uuid,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        txn_id: Uuid,
        key: Vec<u8>,
    },
    Prepare {
        txn_id: Uuid,
    },
    Commit {
        txn_id: Uuid,
        commit_ts: u64,
    },
}

fn read<T: DeserializeOwned>(kv: &dyn KvEngine, key: &[u8]) -> Result<Option<T>> {
    let Some(bytes) = kv.get(key, u64::MAX)? else {
        return Ok(None);
    };
    decode_record(&bytes).map(Some)
}

fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    match nodus_common::versioned::decode(bytes) {
        nodus_common::versioned::Envelope::Versioned {
            version: RECORD_VERSION,
            payload,
        } => Ok(serde_json::from_slice(payload)?),
        _ => bail!("unsupported or corrupt migration record envelope"),
    }
}

pub(crate) fn is_control_key(key: &[u8]) -> bool {
    key.starts_with(PREFIX)
}

/// Snapshots cannot delete or roll back durable migration state.
pub(crate) fn validate_snapshot_record(
    kv: &dyn KvEngine,
    key: &[u8],
    value: &[u8],
    version: u64,
) -> Result<()> {
    if !is_control_key(key) {
        return Ok(());
    }
    if key == FENCE_KEY {
        let incoming: ParticipantFence = decode_record(value)?;
        if let Some(current) = read_fence(kv)? {
            anyhow::ensure!(
                incoming.epoch >= current.epoch,
                "snapshot would roll back a participant epoch"
            );
            anyhow::ensure!(
                incoming.epoch != current.epoch || incoming.operation_id == current.operation_id,
                "snapshot fence owner conflicts at the same epoch"
            );
        }
    } else {
        let _: MigrationRecord = decode_record(value)?;
    }
    let mut end = key.to_vec();
    end.push(0);
    if let Some(current) = kv
        .scan(
            nodus_storage_api::KeyRange {
                start: Bytes::copy_from_slice(key),
                end: Bytes::from(end),
            },
            u64::MAX,
        )?
        .next()
    {
        anyhow::ensure!(
            current?.version <= version,
            "snapshot would roll back migration state"
        );
    }
    Ok(())
}

fn write(kv: &dyn KvEngine, key: &[u8], record: &impl Serialize, index: u64) -> Result<()> {
    let value = Bytes::from(nodus_common::versioned::encode(
        RECORD_VERSION,
        &serde_json::to_vec(record)?,
    ));
    // Raft replay above the applied watermark uses the same intent identity.
    let mut identity = key.to_vec();
    identity.extend_from_slice(&index.to_be_bytes());
    let txn = TxnId(Uuid::new_v5(&Uuid::NAMESPACE_OID, &identity));
    kv.write_intent(txn, Bytes::copy_from_slice(key), value)?;
    kv.commit(txn, index)?;
    Ok(())
}

fn journal_key(table_id: TableId) -> Vec<u8> {
    [PREFIX, format!("table/{table_id}").as_bytes()].concat()
}

pub fn read_record(kv: &dyn KvEngine, table_id: TableId) -> Result<Option<MigrationRecord>> {
    read(kv, &journal_key(table_id))
}

pub fn read_fence(kv: &dyn KvEngine) -> Result<Option<ParticipantFence>> {
    read(kv, FENCE_KEY)
}

fn reject(reason: &str) -> ShardResponse {
    ShardResponse {
        success: false,
        error: Some(format!("shard routing changed: {reason}")),
    }
}

fn accepted() -> ShardResponse {
    ShardResponse {
        success: true,
        error: None,
    }
}

fn apply_control(
    kv: &dyn KvEngine,
    cmd: &MigrationCommandV1,
    index: u64,
    is_meta: bool,
) -> Result<ShardResponse> {
    // A crash after the journal's intent WAL write but before its commit may
    // leave this entry's own control intent. Remove it before quiescence checks;
    // user intents are never removed here.
    let key = match cmd {
        MigrationCommandV1::Plan(plan) => journal_key(plan.table_id),
        MigrationCommandV1::StartFencing { table_id, .. }
        | MigrationCommandV1::RequestCancel { table_id, .. } => journal_key(*table_id),
        _ => FENCE_KEY.to_vec(),
    };
    let mut identity = key;
    identity.extend_from_slice(&index.to_be_bytes());
    let txn = TxnId(Uuid::new_v5(&Uuid::NAMESPACE_OID, &identity));
    if !kv.pending_intent_keys(txn).is_empty() {
        kv.abort(txn)?;
    }
    match cmd {
        MigrationCommandV1::Plan(plan) => {
            if !is_meta {
                return Ok(reject("migration journal belongs to the meta group"));
            }
            let valid_groups = |groups: &[String]| {
                !groups.is_empty()
                    && groups.len() <= 128
                    && groups.iter().all(|s| !s.is_empty() && s.len() <= 128)
                    && groups
                        .iter()
                        .collect::<std::collections::HashSet<_>>()
                        .len()
                        == groups.len()
            };
            if !valid_groups(&plan.sources)
                || !valid_groups(&plan.destinations)
                || plan.sources.iter().any(|s| plan.destinations.contains(s))
                || plan.source_epochs.len() != plan.sources.len()
                || plan
                    .sources
                    .iter()
                    .any(|source| !plan.source_epochs.contains_key(source))
                || plan.source_epochs.values().any(|epoch| *epoch == u64::MAX)
            {
                return Ok(reject("invalid migration plan"));
            }
            if let Some(current) = read_record(kv, plan.table_id)? {
                return Ok(if current.plan == *plan {
                    accepted()
                } else {
                    reject("another migration owns this table")
                });
            }
            write(
                kv,
                &journal_key(plan.table_id),
                &MigrationRecord {
                    plan: plan.clone(),
                    phase: MigrationPhase::Planned,
                },
                index,
            )?;
        }
        MigrationCommandV1::StartFencing {
            table_id,
            operation_id,
        }
        | MigrationCommandV1::RequestCancel {
            table_id,
            operation_id,
        } => {
            if !is_meta {
                return Ok(reject("migration journal belongs to the meta group"));
            }
            let Some(mut record) = read_record(kv, *table_id)? else {
                return Ok(reject("migration is not planned"));
            };
            if record.plan.operation_id != *operation_id {
                return Ok(reject("migration operation ID mismatch"));
            }
            let next = if matches!(cmd, MigrationCommandV1::StartFencing { .. }) {
                MigrationPhase::Fencing
            } else {
                MigrationPhase::CancelRequested
            };
            if record.phase == next {
                return Ok(accepted());
            }
            if record.phase == MigrationPhase::CancelRequested {
                return Ok(reject("cancelled migration cannot resume fencing"));
            }
            record.phase = next;
            write(kv, &journal_key(*table_id), &record, index)?;
        }
        MigrationCommandV1::Acquire {
            operation_id,
            expected_epoch,
        } => {
            let Some(next_epoch) = expected_epoch.checked_add(1) else {
                return Ok(reject("routing epoch exhausted"));
            };
            let current = read_fence(kv)?;
            if let Some(fence) = &current {
                if fence.operation_id == *operation_id && fence.epoch == next_epoch {
                    return Ok(if fence.closed {
                        accepted()
                    } else {
                        reject("operation already released this participant")
                    });
                }
                if fence.closed || fence.operation_id == *operation_id {
                    return Ok(reject("participant already belongs to a migration"));
                }
            }
            if current.as_ref().map_or(0, |f| f.epoch) != *expected_epoch {
                return Ok(reject("stale participant epoch"));
            }
            if kv.has_pending_intents(b"")? {
                return Ok(reject(
                    "participant has pending intents; drain transactions before fencing",
                ));
            }
            write(
                kv,
                FENCE_KEY,
                &ParticipantFence {
                    operation_id: *operation_id,
                    epoch: next_epoch,
                    closed: true,
                },
                index,
            )?;
        }
        MigrationCommandV1::Release {
            operation_id,
            epoch,
        } => {
            let Some(mut fence) = read_fence(kv)? else {
                return Ok(reject("participant has no fence"));
            };
            if fence.operation_id != *operation_id || fence.epoch != *epoch {
                return Ok(reject("stale fence release"));
            }
            if !fence.closed {
                return Ok(accepted());
            }
            fence.closed = false;
            write(kv, FENCE_KEY, &fence, index)?;
        }
    }
    tracing::info!(?cmd, "migration transition applied");
    Ok(accepted())
}

/// Handles protocol commands and rejects legacy mutations after a fence epoch
/// exists. Returns None only when the ordinary apply path may handle a command.
pub(crate) fn apply(
    kv: &dyn KvEngine,
    cmd: &ShardCommand,
    index: u64,
    is_meta: bool,
) -> Result<Option<ShardResponse>> {
    match cmd {
        ShardCommand::MigrationV1(command) => {
            return apply_control(kv, command, index, is_meta).map(Some);
        }
        ShardCommand::EpochWriteV1 { epoch, mutation } => {
            let fence = read_fence(kv)?;
            if fence.as_ref().map_or(0, |f| f.epoch) != *epoch
                || fence.as_ref().is_some_and(|f| f.closed)
            {
                return Ok(Some(reject(
                    "stale epoch or fenced participant; retry transaction",
                )));
            }
            let result = match mutation {
                EpochMutationV1::Put { txn_id, key, value } => {
                    if key.starts_with(PREFIX) {
                        return Ok(Some(reject("reserved migration key")));
                    }
                    kv.write_intent(
                        TxnId(*txn_id),
                        Bytes::copy_from_slice(key),
                        Bytes::copy_from_slice(value),
                    )
                }
                EpochMutationV1::Delete { txn_id, key } => {
                    if key.starts_with(PREFIX) {
                        return Ok(Some(reject("reserved migration key")));
                    }
                    kv.delete_intent(TxnId(*txn_id), Bytes::copy_from_slice(key))
                }
                EpochMutationV1::Prepare { txn_id } => {
                    if kv.pending_intent_keys(TxnId(*txn_id)).is_empty() {
                        return Ok(Some(reject("no live participant intents")));
                    }
                    Ok(())
                }
                EpochMutationV1::Commit { txn_id, commit_ts } => {
                    match kv.commit(TxnId(*txn_id), *commit_ts) {
                        Err(nodus_storage_api::KvError::IntentNotFound(_)) => Ok(()), // idempotent replay
                        result => result,
                    }
                }
            };
            match result {
                Ok(()) => {}
                Err(nodus_storage_api::KvError::WriteConflict(_)) => {
                    return Ok(Some(reject("write conflict; retry transaction")));
                }
                Err(e) => return Err(e.into()),
            }
            return Ok(Some(accepted()));
        }
        ShardCommand::PutIntent { key, .. }
        | ShardCommand::DeleteIntent { key, .. }
        | ShardCommand::IndexPutIntent { key, .. }
        | ShardCommand::IndexDeleteIntent { key, .. } => {
            if key.starts_with(PREFIX) {
                return Ok(Some(reject("reserved migration key")));
            }
        }
        ShardCommand::PrepareTxn { .. } | ShardCommand::CommitTxn { .. } => {}
        _ => return Ok(None), // Abort remains available to drain old transactions.
    }
    Ok(read_fence(kv)?.map(|_| reject("epoch is required on this participant; retry transaction")))
}
