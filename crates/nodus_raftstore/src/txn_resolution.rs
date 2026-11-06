//! Durable terminal-decision work queue. Creation is atomic with the decision;
//! acknowledgements follow participant Raft success. Completed progress remains
//! as a snapshot/retry proof, while its pending index is removed.

mod snapshots;
pub(crate) use snapshots::{can_omit, validate_snapshot};

use crate::{
    ShardResponse,
    txn_authority::{self as authority, PhaseV1, RecordV1, TokenV1},
};
use anyhow::{Result, ensure};
use bytes::Bytes;
use nodus_storage_api::{KeyRange, KvEngine, TxnId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

pub const PREFIX: &[u8] = b"\x01txn-resolution/";
pub const PENDING_PREFIX: &[u8] = b"\x01txn-resolution/v1/pending/";
const PROGRESS_PREFIX: &[u8] = b"\x01txn-resolution/v1/progress/";
const HIGH_WATER: &[u8] = b"\x01txn-resolution/v1/high-water";
const CURSOR_KEY: &[u8] = b"\x01txn-resolution/v1/cursor";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProgressV1 {
    pub txn_id: Uuid,
    pub index: u64,
    pub decision: TokenV1,
    pub acknowledged: BTreeSet<String>,
    pub complete: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CursorV1 {
    pub revision: u64,
    pub after: u64,
    /// Fixed end of this scan cycle, so new traffic cannot prevent wrapping.
    pub through: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CommandV1 {
    Acknowledge {
        txn_id: Uuid,
        decision: TokenV1,
        group: String,
    },
    Advance {
        expected_revision: u64,
        after: u64,
        through: u64,
    },
}

fn progress_key(txn: Uuid) -> Vec<u8> {
    [PROGRESS_PREFIX, txn.as_bytes()].concat()
}
fn pending_key(index: u64, txn: Uuid) -> Vec<u8> {
    [
        PENDING_PREFIX,
        index.to_be_bytes().as_slice(),
        txn.as_bytes(),
    ]
    .concat()
}
pub fn parse_pending_key(key: &[u8]) -> Result<(u64, Uuid)> {
    let suffix = key
        .strip_prefix(PENDING_PREFIX)
        .ok_or_else(|| anyhow::anyhow!("invalid pending resolution key"))?;
    ensure!(suffix.len() == 24, "invalid pending resolution key length");
    let index = u64::from_be_bytes(suffix[..8].try_into()?);
    ensure!(index > 0, "zero resolution index");
    Ok((index, Uuid::from_slice(&suffix[8..])?))
}

fn encode(value: &impl Serialize) -> Result<Bytes> {
    Ok(Bytes::from(nodus_common::versioned::encode(
        1,
        &serde_json::to_vec(value)?,
    )))
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ensure!(bytes.len() <= 256 * 1024, "resolution record too large");
    match nodus_common::versioned::decode(bytes) {
        nodus_common::versioned::Envelope::Versioned {
            version: 1,
            payload,
        } => Ok(serde_json::from_slice(payload)?),
        _ => anyhow::bail!("unsupported resolution record envelope"),
    }
}
pub fn high_water(kv: &dyn KvEngine) -> Result<u64> {
    kv.get(HIGH_WATER, u64::MAX)?
        .map(|v| decode(&v))
        .transpose()
        .map(|v| v.unwrap_or(0))
}
pub fn cursor(kv: &dyn KvEngine) -> Result<CursorV1> {
    let c: CursorV1 = kv
        .get(CURSOR_KEY, u64::MAX)?
        .map(|v| decode(&v))
        .transpose()?
        .unwrap_or_default();
    ensure!(
        c.after <= c.through && c.through <= high_water(kv)?,
        "invalid resolution cursor"
    );
    Ok(c)
}
pub fn progress(kv: &dyn KvEngine, txn: Uuid) -> Result<Option<ProgressV1>> {
    let p: Option<ProgressV1> = kv
        .get(&progress_key(txn), u64::MAX)?
        .map(|v| decode(&v))
        .transpose()?;
    if let Some(p) = &p {
        ensure!(
            p.txn_id == txn && p.index > 0 && (!p.complete || p.acknowledged.is_empty()),
            "invalid resolution progress"
        );
        ensure!(
            p.index <= high_water(kv)?,
            "resolution progress exceeds high water"
        );
        let record = authority::read(kv, txn)?
            .ok_or_else(|| anyhow::anyhow!("resolution authority missing"))?;
        validate_progress(p, &record)?;
    }
    Ok(p)
}
fn validate_progress(p: &ProgressV1, record: &RecordV1) -> Result<()> {
    ensure!(
        p.txn_id == record.txn_id
            && p.decision == record.token
            && matches!(record.phase, PhaseV1::Committed { .. } | PhaseV1::Aborted),
        "resolution decision mismatch"
    );
    ensure!(
        p.index > 0
            && p.acknowledged
                .iter()
                .all(|g| record.participants.contains_key(g))
            && (p.complete || p.acknowledged.len() < record.participants.len())
            && (!p.complete || p.acknowledged.is_empty()),
        "invalid participant acknowledgements"
    );
    Ok(())
}

/// Add a pending row in the authority's own transaction, before its commit.
pub(crate) fn enqueue(kv: &dyn KvEngine, record: &RecordV1, txn: TxnId, index: u64) -> Result<()> {
    if record.participants.is_empty() {
        return Ok(());
    }
    ensure!(
        index > high_water(kv)?,
        "resolution high water would regress"
    );
    ensure!(
        kv.get(&progress_key(record.txn_id), u64::MAX)?.is_none(),
        "resolution identity reused"
    );
    let p = ProgressV1 {
        txn_id: record.txn_id,
        index,
        decision: record.token.clone(),
        acknowledged: BTreeSet::new(),
        complete: false,
    };
    for (key, value) in [
        (progress_key(record.txn_id), encode(&p)?),
        (pending_key(index, record.txn_id), encode(&record.txn_id)?),
        (HIGH_WATER.to_vec(), encode(&index)?),
    ] {
        kv.write_intent(txn, Bytes::from(key), value)?;
    }
    Ok(())
}

pub fn pending_page(
    kv: &dyn KvEngine,
    after: u64,
    through: u64,
    limit: usize,
) -> Result<Vec<(u64, Uuid)>> {
    if after >= through || limit == 0 {
        return Ok(Vec::new());
    }
    let start = [
        PENDING_PREFIX,
        after
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("resolution cursor exhausted"))?
            .to_be_bytes()
            .as_slice(),
    ]
    .concat();
    let end = if let Some(end) = through.checked_add(1) {
        [PENDING_PREFIX, end.to_be_bytes().as_slice()].concat()
    } else {
        b"\x01txn-resolution/v1/pending0".to_vec()
    };
    let mut page = Vec::new();
    for row in kv.scan_limited(
        KeyRange {
            start: Bytes::from(start),
            end: Bytes::from(end),
        },
        u64::MAX,
        limit,
    )? {
        let row = row?;
        let (index, txn) = parse_pending_key(&row.key)?;
        ensure!(
            decode::<Uuid>(&row.value)? == txn,
            "pending resolution identity mismatch"
        );
        let p = progress(kv, txn)?
            .ok_or_else(|| anyhow::anyhow!("pending resolution progress missing"))?;
        ensure!(p.index == index, "pending resolution progress inconsistent");
        // Another resolver can complete this item after the scan snapshot.
        if !p.complete {
            page.push((index, txn));
        }
    }
    Ok(page)
}

pub(crate) fn apply(kv: &dyn KvEngine, cmd: &CommandV1, index: u64) -> Result<ShardResponse> {
    let accepted = || ShardResponse {
        success: true,
        error: None,
    };
    let reject = |error: &str| ShardResponse {
        success: false,
        error: Some(error.into()),
    };
    let txn = TxnId(Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        &[PREFIX, &index.to_be_bytes()].concat(),
    ));
    // A replay may find this command's unpublished local intents.
    kv.abort(txn)?;
    match cmd {
        CommandV1::Acknowledge {
            txn_id,
            decision,
            group,
        } => {
            let Some(record) = authority::read(kv, *txn_id)? else {
                return Ok(reject("resolution authority missing"));
            };
            if record.token != *decision
                || !record.participants.contains_key(group)
                || !matches!(record.phase, PhaseV1::Committed { .. } | PhaseV1::Aborted)
            {
                return Ok(reject(
                    "resolution acknowledgement has no matching terminal decision",
                ));
            }
            let Some(mut p) = progress(kv, *txn_id)? else {
                return Ok(accepted());
            };
            if p.complete || p.acknowledged.contains(group) {
                return Ok(accepted());
            }
            p.acknowledged.insert(group.clone());
            if p.acknowledged.len() == record.participants.len() {
                p.complete = true;
                p.acknowledged.clear();
                kv.delete_intent(txn, Bytes::from(pending_key(p.index, *txn_id)))?;
            }
            kv.write_intent(txn, Bytes::from(progress_key(*txn_id)), encode(&p)?)?;
        }
        CommandV1::Advance {
            expected_revision,
            after,
            through,
        } => {
            let old = cursor(kv)?;
            if old.revision >= index {
                return Ok(accepted());
            }
            if old.revision != *expected_revision {
                return Ok(reject("stale resolution cursor"));
            }
            if after > through || *through > high_water(kv)? {
                return Ok(reject("invalid resolution cursor bound"));
            }
            kv.write_intent(
                txn,
                Bytes::from_static(CURSOR_KEY),
                encode(&CursorV1 {
                    revision: index,
                    after: *after,
                    through: *through,
                })?,
            )?;
        }
    }
    kv.commit(txn, index)?;
    Ok(accepted())
}

#[cfg(test)]
mod tests;
