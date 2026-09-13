//! Additive admission after finalization. Approval precedes learner creation;
//! promotion intent precedes joint consensus. One operation may be pending.
use super::*;
use std::collections::BTreeSet;

pub const KEY: &[u8] = b"\x01upgrade/admission/v1/state";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberV1 {
    pub address: String,
    pub report: CapabilityV1,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum StageV1 {
    Approved,
    Promoting,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanV1 {
    pub operation: Uuid,
    pub node_id: u64,
    pub base: StoredMembership<u64, BasicNode>,
    pub stage: StageV1,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordV1 {
    pub version: u16,
    pub revision: u64,
    pub authority_revision: u64,
    pub admitted: BTreeMap<u64, MemberV1>,
    pub pending: Option<PlanV1>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OperationV1 {
    Approve { node_id: u64, address: String },
    Promote,
    Complete,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandV1 {
    pub expected_revision: u64,
    pub operation_id: Uuid,
    pub membership: StoredMembership<u64, BasicNode>,
    pub challenge: Uuid,
    pub reports: Vec<CapabilityV1>,
    pub action: OperationV1,
}

pub fn validate_address(address: &str) -> Result<()> {
    ensure!(
        !address.is_empty()
            && address.len() <= 512
            && !address.contains(['/', '@', '?', '#'])
            && !address.chars().any(char::is_whitespace),
        "invalid member address; use host:port"
    );
    let authority: axum::http::uri::Authority = address.parse()?;
    ensure!(
        authority.port_u16().is_some_and(|p| p > 0),
        "member address requires a nonzero port"
    );
    Ok(())
}
pub fn require_reader(report: &CapabilityV1) -> Result<()> {
    report.validate(report.node_id, report.challenge)?;
    ensure!(
        !report.challenge.is_nil() && report.admission_version >= 1,
        "node does not support admission protocol v1"
    );
    Ok(())
}
pub fn decode(bytes: &[u8]) -> Result<RecordV1> {
    ensure!(
        bytes.len() <= 512 * 1024,
        "admission record exceeds 512 KiB"
    );
    let record: RecordV1 = serde_json::from_slice(bytes)?;
    ensure!(
        record.version == 1
            && record.revision > record.authority_revision
            && !record.admitted.is_empty()
            && record.admitted.len() < MAX_MEMBERS,
        "invalid admission record version/revision/members"
    );
    let mut addresses = BTreeSet::new();
    for (id, member) in &record.admitted {
        validate_address(&member.address)?;
        ensure!(
            *id == member.report.node_id && addresses.insert(&member.address),
            "duplicate admission address or identity mismatch"
        );
        require_reader(&member.report)?;
    }
    if let Some(plan) = &record.pending {
        ensure!(
            !plan.operation.is_nil() && record.admitted.contains_key(&plan.node_id),
            "invalid pending admission"
        );
        ensure!(
            plan.base.membership().get_joint_config().len() == 1
                && !members(&plan.base)?.contains_key(&plan.node_id),
            "invalid admission base membership"
        );
    }
    Ok(record)
}
pub fn read(kv: &dyn KvEngine) -> Result<Option<RecordV1>> {
    kv.get(KEY, u64::MAX)?.map(|b| decode(&b)).transpose()
}

pub fn approved(
    authority: &super::RecordV1,
    record: Option<&RecordV1>,
) -> Result<BTreeMap<u64, String>> {
    let mut result = members(&authority.membership)?;
    if let Some(record) = record {
        ensure!(
            authority.phase == Phase::Finalized && record.authority_revision == authority.revision,
            "admission authority anchor mismatch"
        );
        for (id, member) in &record.admitted {
            ensure!(
                !result.contains_key(id) && !result.values().any(|a| a == &member.address),
                "admission reuses a node ID or address"
            );
            result.insert(*id, member.address.clone());
        }
        ensure!(
            result.len() <= MAX_MEMBERS,
            "admission exceeds member limit"
        );
        if let Some(plan) = &record.pending {
            let mut base = result.clone();
            base.remove(&plan.node_id);
            ensure!(
                members(&plan.base)? == base,
                "admission plan differs from approved roster"
            );
            let expected: BTreeSet<_> = authority
                .membership
                .membership()
                .voter_ids()
                .chain(
                    record
                        .admitted
                        .keys()
                        .copied()
                        .filter(|id| *id != plan.node_id),
                )
                .collect();
            ensure!(
                plan.base.membership().get_joint_config() == &[expected],
                "admission base changes approved voters"
            );
        }
    }
    Ok(result)
}
pub fn validate_stable(
    authority: &super::RecordV1,
    record: Option<&RecordV1>,
    current: &StoredMembership<u64, BasicNode>,
) -> Result<()> {
    let roster = approved(authority, record)?;
    ensure!(
        record.is_none_or(|r| r.pending.is_none()),
        "admission is still pending"
    );
    let voters: BTreeSet<_> = authority
        .membership
        .membership()
        .voter_ids()
        .chain(record.into_iter().flat_map(|r| r.admitted.keys().copied()))
        .collect();
    ensure!(
        members(current)? == roster && current.membership().get_joint_config() == &[voters],
        "membership differs from stable approved roster"
    );
    Ok(())
}

impl PlanV1 {
    pub fn target_voters(&self) -> BTreeSet<u64> {
        self.base
            .membership()
            .voter_ids()
            .chain([self.node_id])
            .collect()
    }
    pub fn complete(&self, current: &StoredMembership<u64, BasicNode>) -> bool {
        current.membership().get_joint_config() == &[self.target_voters()]
    }
}
/// Accept only the original roster, its added learner, or this plan's joint/final
/// configuration. Unknown removals, address changes and competing additions fail.
pub fn validate_progress(
    record: &RecordV1,
    current: &StoredMembership<u64, BasicNode>,
) -> Result<()> {
    let plan = record
        .pending
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no pending admission"))?;
    let base = members(&plan.base)?;
    let actual = members(current)?;
    let mut extended = base.clone();
    extended.insert(plan.node_id, record.admitted[&plan.node_id].address.clone());
    let configs = current.membership().get_joint_config();
    let original = plan.base.membership().get_joint_config();
    let target = plan.target_voters();
    let valid = match plan.stage {
        StageV1::Approved => configs == original && (actual == base || actual == extended),
        StageV1::Promoting => {
            actual == extended
                && (configs == original
                    || configs == &[original[0].clone(), target.clone()]
                    || configs == &[target])
        }
    };
    ensure!(valid, "membership changed outside the pending admission");
    Ok(())
}
fn validate_reports(
    cmd: &CommandV1,
    roster: &BTreeMap<u64, String>,
) -> Result<BTreeMap<u64, CapabilityV1>> {
    ensure!(
        !cmd.challenge.is_nil() && cmd.reports.len() == roster.len(),
        "fresh reports required from every voter, learner and candidate"
    );
    let mut reports = BTreeMap::new();
    for report in &cmd.reports {
        ensure!(
            roster.contains_key(&report.node_id),
            "unknown admission report"
        );
        report.validate(report.node_id, cmd.challenge)?;
        require_reader(report)?;
        ensure!(
            reports.insert(report.node_id, report.clone()).is_none(),
            "duplicate admission report"
        );
    }
    Ok(reports)
}
fn transition(
    authority: &super::RecordV1,
    old: Option<RecordV1>,
    cmd: &CommandV1,
    current: &StoredMembership<u64, BasicNode>,
    index: u64,
) -> Result<RecordV1> {
    ensure!(
        authority.phase == Phase::Finalized,
        "admission requires snapshot-v2 finalization"
    );
    ensure!(
        &cmd.membership == current
            && old.as_ref().map_or(0, |r| r.revision) == cmd.expected_revision,
        "stale admission revision or membership"
    );
    ensure!(
        !cmd.operation_id.is_nil(),
        "missing admission operation identity"
    );
    let roster = approved(authority, old.as_ref())?;
    let mut next = old.unwrap_or(RecordV1 {
        version: 1,
        revision: 0,
        authority_revision: authority.revision,
        admitted: BTreeMap::new(),
        pending: None,
    });
    match &cmd.action {
        OperationV1::Approve { node_id, address } => {
            ensure!(
                next.pending.is_none(),
                "another admission is pending; resume it first"
            );
            validate_stable(authority, (next.revision != 0).then_some(&next), current)?;
            ensure!(
                roster.len() < MAX_MEMBERS
                    && !roster.contains_key(node_id)
                    && !roster.values().any(|a| a == address),
                "node ID/address already reserved or member limit reached"
            );
            validate_address(address)?;
            let mut extended = roster;
            extended.insert(*node_id, address.clone());
            let reports = validate_reports(cmd, &extended)?;
            next.admitted.insert(
                *node_id,
                MemberV1 {
                    address: address.clone(),
                    report: reports[node_id].clone(),
                },
            );
            next.pending = Some(PlanV1 {
                operation: cmd.operation_id,
                node_id: *node_id,
                base: current.clone(),
                stage: StageV1::Approved,
            });
        }
        OperationV1::Promote | OperationV1::Complete => {
            validate_progress(&next, current)?;
            let plan = next
                .pending
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no pending admission"))?;
            ensure!(
                plan.operation == cmd.operation_id,
                "admission operation mismatch"
            );
            match cmd.action {
                OperationV1::Promote => {
                    ensure!(
                        plan.stage == StageV1::Approved && members(current)? == roster,
                        "candidate must first be a learner"
                    );
                    ensure!(
                        cmd.challenge != next.admitted[&plan.node_id].report.challenge,
                        "stale promotion report"
                    );
                    let reports = validate_reports(cmd, &roster)?;
                    next.admitted
                        .get_mut(&plan.node_id)
                        .ok_or_else(|| anyhow::anyhow!("candidate absent"))?
                        .report = reports[&plan.node_id].clone();
                    next.pending
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("plan absent"))?
                        .stage = StageV1::Promoting;
                }
                OperationV1::Complete => {
                    ensure!(
                        plan.stage == StageV1::Promoting && plan.complete(current),
                        "admission has not reached stable voter membership"
                    );
                    ensure!(
                        cmd.reports.is_empty(),
                        "completion must not rewrite reader evidence"
                    );
                    next.pending = None;
                }
                _ => unreachable!(),
            }
        }
    }
    next.revision = index;
    Ok(next)
}

pub(crate) fn apply(
    kv: &dyn KvEngine,
    sm: &StateMachine,
    cmd: &CommandV1,
    index: u64,
) -> Result<ShardResponse> {
    ensure!(sm.meta_store.is_some(), "admission belongs to meta group");
    let mut identity = KEY.to_vec();
    identity.extend_from_slice(&index.to_be_bytes());
    let txn = TxnId(Uuid::new_v5(&Uuid::NAMESPACE_OID, &identity));
    kv.abort(txn)?;
    let old = read(kv)?;
    if old.as_ref().is_some_and(|r| r.revision >= index) {
        return Ok(ShardResponse {
            success: true,
            error: None,
        });
    }
    let next = match transition(&super::read(kv)?, old, cmd, &sm.last_membership, index) {
        Ok(next) => next,
        Err(error) => {
            return Ok(ShardResponse {
                success: false,
                error: Some(error.to_string()),
            });
        }
    };
    let bytes = serde_json::to_vec(&next)?;
    decode(&bytes)?;
    kv.write_intent(txn, Bytes::from_static(KEY), Bytes::from(bytes))?;
    kv.commit(txn, index)?;
    tracing::info!(revision=index,operation=%cmd.operation_id,pending=?next.pending,"member admission committed");
    Ok(ShardResponse {
        success: true,
        error: None,
    })
}
pub(crate) fn validate_snapshot(kv: &dyn KvEngine, bytes: &[u8]) -> Result<RecordV1> {
    let next = decode(bytes)?;
    if let Some(old) = read(kv)? {
        ensure!(
            next.authority_revision == old.authority_revision && next.revision >= old.revision,
            "snapshot rolls back admission authority"
        );
        ensure!(
            next.revision != old.revision || next == old,
            "snapshot changes admission at the same revision"
        );
        for (id, member) in old.admitted {
            ensure!(
                next.admitted
                    .get(&id)
                    .is_some_and(|m| m.address == member.address),
                "snapshot removes or replaces an admitted identity"
            );
        }
    }
    Ok(next)
}
#[cfg(test)]
mod tests;
