//! Replicated snapshot compatibility authority. Only the authenticated leader
//! service may submit these commands; generic client-write ingress rejects them.
use crate::{NodusSnapshotMeta, ShardResponse, SnapshotCompatibility, StateMachine};
use anyhow::{Result, ensure};
use bytes::Bytes;
use nodus_storage_api::{KvEngine, TxnId};
use openraft::{BasicNode, StoredMembership};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

pub mod admission;

pub const KEY: &[u8] = b"\x01upgrade/v1/state";
pub const MAX_MEMBERS: usize = 256;
pub const TARGET: &str = "snapshot-v2";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeV1 {
    pub challenge: Uuid,
    pub preflight: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityV1 {
    pub version: u16,
    pub node_id: u64,
    pub challenge: Uuid,
    pub binary_version: String,
    pub authority_version: u16,
    pub snapshot_version: u16,
    pub ready_for_finalize: bool,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub admission_version: u16,
}
fn is_zero(value: &u16) -> bool {
    *value == 0
}

impl CapabilityV1 {
    pub fn local(node_id: u64, challenge: Uuid) -> Self {
        Self {
            version: 1,
            node_id,
            challenge,
            binary_version: env!("CARGO_PKG_VERSION").into(),
            authority_version: 1,
            snapshot_version: 2,
            ready_for_finalize: false,
            admission_version: 1,
        }
    }
    pub fn validate(&self, node: u64, challenge: Uuid) -> Result<()> {
        ensure!(
            self.version == 1 && self.node_id == node && self.challenge == challenge,
            "capability identity or challenge mismatch"
        );
        ensure!(
            self.authority_version == 1 && self.snapshot_version >= 2,
            "node does not support upgrade authority and snapshot v2"
        );
        ensure!(
            !self.binary_version.is_empty() && self.binary_version.len() <= 128,
            "invalid binary version report"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Phase {
    Idle,
    RollingNodes,
    ReadyToFinalize,
    Finalized,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordV1 {
    pub version: u16,
    pub revision: u64,
    pub phase: Phase,
    pub cluster_version: u64,
    pub session: Uuid,
    pub membership: StoredMembership<u64, BasicNode>,
    pub reports: BTreeMap<u64, CapabilityV1>,
}
impl Default for RecordV1 {
    fn default() -> Self {
        Self {
            version: 1,
            revision: 0,
            phase: Phase::Idle,
            cluster_version: 1,
            session: Uuid::nil(),
            membership: StoredMembership::default(),
            reports: BTreeMap::new(),
        }
    }
}
impl RecordV1 {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1 && (1..=2).contains(&self.cluster_version),
            "unsupported upgrade authority version"
        );
        let members = members(&self.membership)?;
        ensure!(
            self.reports.len() <= members.len(),
            "unknown upgrade reports"
        );
        for (id, report) in &self.reports {
            ensure!(members.contains_key(id), "unknown upgrade member");
            report.validate(*id, self.session)?;
        }
        if self.phase == Phase::Idle {
            ensure!(
                members.is_empty() && self.reports.is_empty() && self.session.is_nil(),
                "invalid idle authority"
            );
        } else {
            ensure!(
                !members.is_empty() && !self.session.is_nil(),
                "active authority has no membership/session"
            );
        }
        if self.phase == Phase::Finalized {
            ensure!(
                self.reports.values().all(|r| r.ready_for_finalize),
                "finalized authority lacks preflight evidence"
            );
        }
        if matches!(self.phase, Phase::ReadyToFinalize | Phase::Finalized) {
            ensure!(
                !members.is_empty() && self.reports.len() == members.len(),
                "incomplete upgrade reports"
            );
        }
        ensure!(
            (self.phase == Phase::Finalized) == (self.cluster_version == 2),
            "inconsistent finalized version"
        );
        Ok(())
    }
}

pub fn members(membership: &StoredMembership<u64, BasicNode>) -> Result<BTreeMap<u64, String>> {
    let result: BTreeMap<_, _> = membership
        .membership()
        .nodes()
        .map(|(id, node)| (*id, node.addr.clone()))
        .collect();
    ensure!(
        result.len() <= MAX_MEMBERS && result.values().all(|a| !a.is_empty() && a.len() <= 512),
        "invalid or excessive upgrade membership"
    );
    Ok(result)
}
pub fn decode(bytes: &[u8]) -> Result<RecordV1> {
    ensure!(
        bytes.len() <= 256 * 1024,
        "upgrade authority record too large"
    );
    let record: RecordV1 = serde_json::from_slice(bytes)?;
    record.validate()?;
    Ok(record)
}
pub fn read(kv: &dyn KvEngine) -> Result<RecordV1> {
    kv.get(KEY, u64::MAX)?
        .map(|b| decode(&b))
        .transpose()
        .map(|r| r.unwrap_or_default())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OperationV1 {
    Start,
    Refresh,
    Finalize,
    Rollback,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandV1 {
    pub expected_revision: u64,
    pub membership: StoredMembership<u64, BasicNode>,
    pub session: Uuid,
    pub operation: OperationV1,
    /// Reports are collected directly by the leader over authenticated transport.
    /// Every fresh operation challenges all nodes using a new session nonce.
    pub reports: Vec<CapabilityV1>,
}

fn transition(
    old: &RecordV1,
    cmd: &CommandV1,
    current: &StoredMembership<u64, BasicNode>,
    index: u64,
) -> Result<RecordV1> {
    ensure!(
        old.revision == cmd.expected_revision,
        "stale upgrade revision; refresh status"
    );
    ensure!(
        &cmd.membership == current,
        "membership changed during upgrade probe"
    );
    ensure!(
        current.membership().get_joint_config().len() == 1,
        "upgrade requires stable membership"
    );
    let roster = members(current)?;
    ensure!(!roster.is_empty(), "upgrade requires membership");
    ensure!(
        old.phase != Phase::Finalized,
        "upgrade finalized; rollback is closed and membership is frozen"
    );
    if matches!(cmd.operation, OperationV1::Rollback) {
        return Ok(RecordV1 {
            revision: index,
            ..RecordV1::default()
        });
    }
    ensure!(
        !cmd.session.is_nil() && cmd.session != old.session,
        "stale capability challenge"
    );
    ensure!(
        cmd.reports.len() == roster.len(),
        "every voter and learner must report"
    );
    let mut reports = BTreeMap::new();
    for report in &cmd.reports {
        ensure!(
            roster.contains_key(&report.node_id),
            "unknown capability node"
        );
        report.validate(report.node_id, cmd.session)?;
        // R6a readers discard this optional field. Keep the immutable base
        // authority identical across both readers; admission has its own ledger.
        let mut report = report.clone();
        report.admission_version = 0;
        ensure!(
            reports.insert(report.node_id, report).is_none(),
            "duplicate capability report"
        );
    }
    if matches!(cmd.operation, OperationV1::Start | OperationV1::Finalize) {
        ensure!(
            reports.values().all(|r| r.ready_for_finalize),
            "upgrade preflight requires drained intents/2PC decisions and reconciled migration records on every node"
        );
    }
    let phase = match cmd.operation {
        OperationV1::Start => {
            ensure!(old.phase == Phase::Idle, "upgrade already active");
            Phase::RollingNodes
        }
        OperationV1::Refresh => {
            ensure!(
                matches!(old.phase, Phase::RollingNodes | Phase::ReadyToFinalize),
                "no active upgrade"
            );
            ensure!(
                &old.membership == current,
                "upgrade membership changed; roll back and restart"
            );
            Phase::ReadyToFinalize
        }
        OperationV1::Finalize => {
            ensure!(
                old.phase == Phase::ReadyToFinalize,
                "upgrade is not ready to finalize"
            );
            ensure!(
                &old.membership == current,
                "upgrade membership changed; roll back and restart"
            );
            Phase::Finalized
        }
        OperationV1::Rollback => unreachable!(),
    };
    Ok(RecordV1 {
        version: 1,
        revision: index,
        cluster_version: if phase == Phase::Finalized { 2 } else { 1 },
        phase,
        membership: current.clone(),
        reports,
        session: cmd.session,
    })
}

pub(crate) fn apply(
    kv: &dyn KvEngine,
    sm: &StateMachine,
    cmd: &CommandV1,
    index: u64,
) -> Result<ShardResponse> {
    ensure!(
        sm.meta_store.is_some(),
        "upgrade authority belongs to the meta group"
    );
    let mut identity = KEY.to_vec();
    identity.extend_from_slice(&index.to_be_bytes());
    let txn = TxnId(Uuid::new_v5(&Uuid::NAMESPACE_OID, &identity));
    kv.abort(txn)?;
    let old = read(kv)?;
    // Storage may have committed before the separate applied pointer persisted.
    if old.revision >= index {
        return Ok(ShardResponse {
            success: true,
            error: None,
        });
    }
    let next = match transition(&old, cmd, &sm.last_membership, index) {
        Ok(next) => next,
        Err(error) => {
            return Ok(ShardResponse {
                success: false,
                error: Some(error.to_string()),
            });
        }
    };
    kv.write_intent(
        txn,
        Bytes::from_static(KEY),
        Bytes::from(serde_json::to_vec(&next)?),
    )?;
    kv.commit(txn, index)?;
    tracing::info!(revision=index, phase=?next.phase, cluster_version=next.cluster_version, "upgrade authority committed");
    Ok(ShardResponse {
        success: true,
        error: None,
    })
}

/// Reads the replicated authority directly; no stale in-memory projection.
pub struct DurableSnapshotCompatibility(pub Arc<dyn KvEngine>);
impl SnapshotCompatibility for DurableSnapshotCompatibility {
    fn finalized_cluster_version(&self) -> Result<u64> {
        Ok(read(self.0.as_ref())?.cluster_version)
    }
    fn member_snapshot_versions(&self) -> Result<BTreeMap<u64, u16>> {
        let authority = read(self.0.as_ref())?;
        let ledger = admission::read(self.0.as_ref())?;
        admission::approved(&authority, ledger.as_ref())?;
        let mut versions: BTreeMap<_, _> = authority
            .reports
            .into_iter()
            .map(|(id, r)| (id, r.snapshot_version))
            .collect();
        if let Some(ledger) = ledger {
            versions.extend(
                ledger
                    .admitted
                    .into_iter()
                    .map(|(id, m)| (id, m.report.snapshot_version)),
            );
        }
        Ok(versions)
    }
}

pub(crate) fn validate_snapshot(
    kv: &dyn KvEngine,
    bytes: &[u8],
    meta: Option<&NodusSnapshotMeta>,
) -> Result<()> {
    let next = decode(bytes)?;
    let old = read(kv)?;
    ensure!(
        next.revision >= old.revision && next.cluster_version >= old.cluster_version,
        "snapshot rolls back upgrade authority"
    );
    if old.phase == Phase::Finalized || next.revision == old.revision {
        ensure!(next == old, "snapshot changes immutable upgrade authority");
    }
    if let Some(meta) = meta {
        ensure!(
            meta.last_log_id.is_some_and(|l| l.index >= next.revision),
            "upgrade revision exceeds snapshot applied pointer"
        );
    }
    Ok(())
}

/// A preflight observation, collected at each source and carried in the replicated
/// command. Never inspect node-local shard state to decide Raft apply outcomes.
pub fn preflight(kv: &dyn KvEngine) -> Result<bool> {
    let migration = kv
        .scan(
            nodus_storage_api::KeyRange {
                start: Bytes::from_static(b"\x01migration/"),
                end: Bytes::from_static(b"\x01migration0"),
            },
            u64::MAX,
        )?
        .next()
        .transpose()?;
    let scope = nodus_storage_api::SnapshotScope {
        exclude_raft: true,
        ..Default::default()
    };
    let pending = kv.snapshot_rows(&scope)?.iter().any(|row| {
        let key = if row.key.starts_with(b"shard-") {
            row.key
                .iter()
                .position(|b| *b == 0)
                .map_or(row.key.as_ref(), |i| &row.key[i + 1..])
        } else {
            row.key.as_ref()
        };
        scope.owns(key) && row.versions.iter().any(|v| v.is_intent)
    });
    let decision = kv
        .scan(
            nodus_storage_api::KeyRange {
                start: Bytes::from_static(b"\0txn2pc\0"),
                end: Bytes::from_static(b"\0txn2pc\x01"),
            },
            u64::MAX,
        )?
        .next()
        .transpose()?;
    Ok(!pending && migration.is_none() && decision.is_none())
}

#[cfg(test)]
mod tests;
