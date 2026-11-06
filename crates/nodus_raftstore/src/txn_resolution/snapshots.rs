use super::*;
use nodus_storage_api::SnapshotRow;
use std::collections::BTreeMap;

fn value<'a>(rows: &'a BTreeMap<Bytes, SnapshotRow>, key: &[u8]) -> Option<&'a [u8]> {
    rows.get(key)?
        .versions
        .iter()
        .max_by_key(|v| v.version)?
        .value
        .as_deref()
}

pub(crate) fn can_omit(key: &[u8], rows: &BTreeMap<Bytes, SnapshotRow>) -> Result<bool> {
    if !key.starts_with(PENDING_PREFIX) {
        return Ok(false);
    }
    let (index, txn) = parse_pending_key(key)?;
    let Some(bytes) = value(rows, &progress_key(txn)) else {
        return Ok(false);
    };
    let p: ProgressV1 = decode(bytes)?;
    Ok(p.txn_id == txn && p.index == index && p.complete)
}

pub(crate) fn validate_snapshot(
    kv: &dyn KvEngine,
    rows: &BTreeMap<Bytes, SnapshotRow>,
    applied: u64,
) -> Result<()> {
    let next_high: u64 = value(rows, HIGH_WATER)
        .map(decode)
        .transpose()?
        .unwrap_or(0);
    ensure!(
        next_high >= high_water(kv)? && next_high <= applied,
        "snapshot rolls back resolution high water"
    );
    if let Some(bytes) = value(rows, CURSOR_KEY) {
        let next: CursorV1 = decode(bytes)?;
        let old = cursor(kv)?;
        ensure!(
            next.revision >= old.revision
                && next.revision <= applied
                && next.after <= next.through
                && next.through <= next_high,
            "invalid resolution snapshot cursor"
        );
        if next.revision == old.revision {
            ensure!(next == old, "conflicting resolution cursor revision");
        }
    }
    for row in rows.values().filter(|r| r.key.starts_with(PREFIX)) {
        ensure!(
            !row.versions.iter().any(|v| v.is_intent),
            "unfinished resolution snapshot state"
        );
        let bytes = value(rows, &row.key);
        if row.key.starts_with(PENDING_PREFIX) {
            let (index, txn) = parse_pending_key(&row.key)?;
            let p: ProgressV1 = decode(
                value(rows, &progress_key(txn))
                    .ok_or_else(|| anyhow::anyhow!("pending snapshot progress missing"))?,
            )?;
            ensure!(
                p.txn_id == txn && p.index == index && index <= next_high,
                "pending snapshot identity mismatch"
            );
            if let Some(bytes) = bytes {
                ensure!(
                    !p.complete && decode::<Uuid>(bytes)? == txn,
                    "completed resolution remains pending"
                );
            } else {
                ensure!(p.complete, "pending work deleted without completion proof");
            }
        } else if row.key.starts_with(PROGRESS_PREFIX) {
            let p: ProgressV1 =
                decode(bytes.ok_or_else(|| anyhow::anyhow!("deleted resolution progress"))?)?;
            ensure!(
                progress_key(p.txn_id) == row.key.as_ref() && p.index <= next_high,
                "invalid snapshot progress identity"
            );
            let canonical = value(rows, &authority::key(p.txn_id))
                .ok_or_else(|| anyhow::anyhow!("snapshot resolution authority missing"))?;
            let record = authority::decode(canonical)?;
            validate_progress(&p, &record)?;
            if let Some(old) = progress(kv, p.txn_id)? {
                ensure!(
                    old.index == p.index && old.decision == p.decision,
                    "snapshot changes resolution identity"
                );
                if old.complete {
                    ensure!(p == old, "snapshot rolls back completed resolution");
                } else {
                    ensure!(
                        p.complete || p.acknowledged.is_superset(&old.acknowledged),
                        "snapshot rolls back participant acknowledgement"
                    );
                }
            }
            if !p.complete {
                let pending = value(rows, &pending_key(p.index, p.txn_id))
                    .ok_or_else(|| anyhow::anyhow!("snapshot lost pending resolution work"))?;
                ensure!(
                    decode::<Uuid>(pending)? == p.txn_id,
                    "pending snapshot transaction mismatch"
                );
            }
        } else if row.key.as_ref() == HIGH_WATER {
            ensure!(bytes.is_some(), "deleted resolution high water");
        } else if row.key.as_ref() == CURSOR_KEY {
            ensure!(bytes.is_some(), "deleted resolution cursor");
        } else {
            anyhow::bail!("unknown resolution snapshot record");
        }
    }
    Ok(())
}
