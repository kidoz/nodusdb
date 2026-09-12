//! Bounded NSNP v2 codec: complete MVCC chains, identity, metadata and checksum.
use super::*;
use nodus_storage_api::SnapshotValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Cursor, Read};

/// Supplied by a trusted, durable cluster-version/capability authority. The
/// current server does not install a provider; production writers stay on v1.
/// Reports must cover every current voter and learner, including joint membership.
pub trait SnapshotCompatibility: Send + Sync {
    fn finalized_cluster_version(&self) -> anyhow::Result<u64>;
    fn member_snapshot_versions(&self) -> anyhow::Result<BTreeMap<u64, u16>>;
}

#[derive(Serialize, Deserialize)]
struct Header {
    group: String,
    meta: NodusSnapshotMeta,
    catalog: bool,
}

pub(super) fn permitted(store: &NodusRaftStore, sm: &StateMachine) -> anyhow::Result<bool> {
    let Some(provider) = &store.snapshot_compatibility else {
        return Ok(false);
    };
    if provider.finalized_cluster_version()? < 2 {
        return Ok(false);
    }
    let formats = provider.member_snapshot_versions()?;
    let members: Vec<_> = sm
        .last_membership
        .membership()
        .nodes()
        .map(|(id, _)| *id)
        .collect();
    Ok(!members.is_empty()
        && members
            .iter()
            .all(|id| formats.get(id).is_some_and(|v| *v >= 2)))
}

pub(super) fn canonicalize(rows: &mut [SnapshotRow]) -> anyhow::Result<()> {
    for row in rows {
        row.versions.sort_by_key(|v| std::cmp::Reverse(v.version));
        row.versions.dedup();
        validate_chain(row)?;
    }
    Ok(())
}

fn validate_chain(row: &SnapshotRow) -> anyhow::Result<()> {
    anyhow::ensure!(!row.versions.is_empty(), "empty snapshot version chain");
    let mut previous = None;
    let mut intents = 0;
    for value in &row.versions {
        anyhow::ensure!(
            previous.is_none_or(|p| p > value.version),
            "duplicate or unordered MVCC timestamp"
        );
        previous = Some(value.version);
        if value.is_intent {
            intents += 1;
            anyhow::ensure!(
                value.txn_id.is_some() && value.version == u64::MAX,
                "invalid snapshot intent identity/timestamp"
            );
        } else {
            anyhow::ensure!(
                value.version != u64::MAX,
                "committed snapshot timestamp is reserved"
            );
        }
    }
    anyhow::ensure!(intents <= 1, "conflicting snapshot intents");
    Ok(())
}

fn bytes(out: &mut Vec<u8>, bytes: &[u8]) -> anyhow::Result<()> {
    anyhow::ensure!(
        out.len().saturating_add(bytes.len()).saturating_add(8 + 32) <= SNAPSHOT_MEMORY_LIMIT,
        "snapshot exceeds 128 MiB wire limit"
    );
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

pub(super) fn encode(
    group: &str,
    meta: &NodusSnapshotMeta,
    catalog: bool,
    rows: &[SnapshotRow],
) -> anyhow::Result<Vec<u8>> {
    let mut out = b"NSNP\0\x02".to_vec();
    bytes(
        &mut out,
        &serde_json::to_vec(&Header {
            group: group.into(),
            meta: meta.clone(),
            catalog,
        })?,
    )?;
    out.extend_from_slice(&(rows.len() as u64).to_be_bytes());
    for row in rows {
        validate_chain(row)?;
        bytes(&mut out, &row.key)?;
        out.extend_from_slice(&(row.versions.len() as u64).to_be_bytes());
        for v in &row.versions {
            out.extend_from_slice(&v.version.to_be_bytes());
            out.push(
                u8::from(v.value.is_some())
                    | (u8::from(v.txn_id.is_some()) << 1)
                    | (u8::from(v.is_intent) << 2),
            );
            if let Some(txn) = v.txn_id {
                out.extend_from_slice(txn.0.as_bytes());
            }
            if let Some(value) = &v.value {
                bytes(&mut out, value)?;
            }
        }
    }
    anyhow::ensure!(
        out.len().saturating_add(32) <= SNAPSHOT_MEMORY_LIMIT,
        "snapshot exceeds 128 MiB wire limit"
    );
    let digest = Sha256::digest(&out);
    out.extend_from_slice(&digest);
    Ok(out)
}

fn integer(input: &mut Cursor<&[u8]>) -> anyhow::Result<u64> {
    let mut bytes = [0; 8];
    Read::read_exact(input, &mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}
fn blob(input: &mut Cursor<&[u8]>) -> anyhow::Result<Vec<u8>> {
    let size = usize::try_from(integer(input)?)?;
    anyhow::ensure!(
        size <= input
            .get_ref()
            .len()
            .saturating_sub(input.position() as usize),
        "truncated snapshot field"
    );
    let mut bytes = vec![0; size];
    Read::read_exact(input, &mut bytes)?;
    Ok(bytes)
}

pub(super) fn decode(
    data: &[u8],
    group: &str,
    meta: &NodusSnapshotMeta,
) -> anyhow::Result<(bool, Vec<SnapshotRow>)> {
    anyhow::ensure!(
        data.len() >= 6 + 8 + 8 + 32 && data.len() <= SNAPSHOT_MEMORY_LIMIT,
        "invalid snapshot size"
    );
    let (payload, digest) = data.split_at(data.len() - 32);
    anyhow::ensure!(
        Sha256::digest(payload).as_slice() == digest,
        "snapshot checksum mismatch"
    );
    anyhow::ensure!(
        &payload[..6] == b"NSNP\0\x02",
        "unsupported snapshot format"
    );
    let mut input = Cursor::new(&payload[6..]);
    let header: Header = serde_json::from_slice(&blob(&mut input)?)?;
    anyhow::ensure!(header.group == group, "snapshot group identity mismatch");
    anyhow::ensure!(
        &header.meta == meta,
        "snapshot metadata does not match payload"
    );
    let count = integer(&mut input)?;
    anyhow::ensure!(
        count <= (payload.len() / 16) as u64,
        "invalid snapshot row count"
    );
    let mut rows = Vec::new();
    let mut size = 0usize;
    for _ in 0..count {
        let key = Bytes::from(blob(&mut input)?);
        let count = integer(&mut input)?;
        anyhow::ensure!(
            count > 0 && count <= (payload.len() / 9) as u64,
            "invalid snapshot version count"
        );
        let mut versions = Vec::new();
        size = size.saturating_add(key.len());
        for _ in 0..count {
            let version = integer(&mut input)?;
            let mut flags = [0];
            Read::read_exact(&mut input, &mut flags)?;
            anyhow::ensure!(flags[0] & !7 == 0, "invalid snapshot version flags");
            let txn_id = if flags[0] & 2 != 0 {
                let mut bytes = [0; 16];
                Read::read_exact(&mut input, &mut bytes)?;
                Some(TxnId(uuid::Uuid::from_bytes(bytes)))
            } else {
                None
            };
            let value = if flags[0] & 1 != 0 {
                Some(blob(&mut input)?)
            } else {
                None
            };
            size = size.saturating_add(64 + value.as_ref().map_or(0, Vec::len));
            anyhow::ensure!(
                size <= SNAPSHOT_MEMORY_LIMIT,
                "snapshot exceeds checkpoint memory limit"
            );
            versions.push(SnapshotValue {
                value,
                version,
                txn_id,
                is_intent: flags[0] & 4 != 0,
            });
        }
        let row = SnapshotRow { key, versions };
        validate_chain(&row)?;
        rows.push(row);
    }
    anyhow::ensure!(
        input.position() as usize == input.get_ref().len(),
        "trailing snapshot records"
    );
    Ok((header.catalog, rows))
}
