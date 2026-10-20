//! Version 3 coordinator journal: the full copy/cutover spine. It extends the
//! V2 fencing state machine with destination preparation, checkpointed copy,
//! exact validation, conditional routing publication, and journaled cutover.
//! Every transition is a Raft apply on the meta group with operation, phase,
//! routing, and epoch revalidation; `Publishing` is the point of no return.
//! Participant sealing shares the V2 `CancelParticipant` semantics verbatim.
pub mod driver;

use super::*;
use nodus_catalog::ShardId;
use nodus_meta::ShardMap;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub const JOURNAL_PREFIX: &[u8] = b"\x01migration/v3/table/";

/// Bounded opaque resume cursor for the copy phase.
const MAX_CHECKPOINT_CURSOR: usize = 1024;
/// Placements are global; bound the journaled copies like every other map.
const MAX_PLACEMENTS: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanV3 {
    pub migration: MigrationPlan,
    /// Complete shard map at planning time (typed absence for an unsharded
    /// table); every routing revalidation compares against this content.
    pub expected_map: Option<ShardMap>,
    /// Full placements at planning time; conditional publication fails closed
    /// if the durable content drifted from this.
    pub expected_placements: HashMap<ShardId, String>,
    /// Routing the operation publishes at cutover (typed absence removes the
    /// map). Compared against the conditional publication payload at apply.
    pub destination_map: Option<ShardMap>,
    pub destination_placements: HashMap<ShardId, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhaseV3 {
    Planned,
    Fencing,
    Fenced,
    Preparing,
    Copying,
    Validating,
    Publishing,
    Completing,
    Done,
    Cancelling,
    Cancelled,
}

impl PhaseV3 {
    /// Success-path ordering; the cancellation path is off-order (`None`).
    fn ordinal(self) -> Option<u8> {
        Some(match self {
            PhaseV3::Planned => 0,
            PhaseV3::Fencing => 1,
            PhaseV3::Fenced => 2,
            PhaseV3::Preparing => 3,
            PhaseV3::Copying => 4,
            PhaseV3::Validating => 5,
            PhaseV3::Publishing => 6,
            PhaseV3::Completing => 7,
            PhaseV3::Done => 8,
            PhaseV3::Cancelling | PhaseV3::Cancelled => return None,
        })
    }
}

/// Durable copy progress for one source; a coordinator restart resumes from
/// the last checkpoint instead of restarting the copy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointV3 {
    pub cursor: Vec<u8>,
    pub complete: bool,
}

/// Exact per-range comparison outcome. A mismatch cancels, never publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationV3 {
    pub matches: bool,
    pub source_digest: u64,
    pub destination_digest: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordV3 {
    pub plan: PlanV3,
    pub phase: PhaseV3,
    /// Fencing acknowledgements: source group -> advanced epoch.
    pub acquired: BTreeMap<String, u64>,
    /// Cancellation seals: source group -> advanced epoch.
    pub cancelled: BTreeMap<String, u64>,
    /// Destination groups reporting a durable, serving replica set.
    pub prepared: BTreeSet<String>,
    /// Per-source durable copy progress.
    pub checkpoints: BTreeMap<String, CheckpointV3>,
    pub validation: Option<ValidationV3>,
    /// Cutover acknowledgements from every participant.
    pub completed: BTreeSet<String>,
    /// Destination groups removed during cancellation cleanup.
    pub cleaned: BTreeSet<String>,
    /// Protected read timestamp chosen for the copy; GC is blocked below it.
    /// Zero until the copy starts.
    pub protected_read_ts: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CommandV3 {
    Plan(PlanV3),
    Begin {
        table: TableId,
        operation: Uuid,
    },
    /// Reachable from every phase before `Publishing` commits. After the
    /// publication commits the operation can only complete.
    RequestCancel {
        table: TableId,
        operation: Uuid,
    },
    /// Phase-tagged participant acknowledgement (`cancelled` selects the
    /// cancellation seal map). `epoch` is always source_epoch + 1.
    Ack {
        table: TableId,
        operation: Uuid,
        source: String,
        epoch: u64,
        cancelled: bool,
    },
    /// A destination group reports a durable, serving replica set.
    PrepareAck {
        table: TableId,
        operation: Uuid,
        destination: String,
    },
    /// Durable copy progress marker bounding resume for one source.
    CopyCheckpoint {
        table: TableId,
        operation: Uuid,
        source: String,
        checkpoint: CheckpointV3,
    },
    /// Source/destination digest comparison; a mismatch cancels the operation.
    ValidationReport {
        table: TableId,
        operation: Uuid,
        report: ValidationV3,
    },
    /// Validating -> Publishing. The routing flip itself commits through the
    /// conditional shard-map/placement commands gated on this phase.
    Publish {
        table: TableId,
        operation: Uuid,
    },
    /// A participant observed the new map and opened its fence on the new
    /// epoch.
    CompleteAck {
        table: TableId,
        operation: Uuid,
        source: String,
    },
    /// Acknowledges removal of a prepared destination during cancellation; a
    /// cancellation is not `Cancelled` until cleanup is journaled.
    CleanupAck {
        table: TableId,
        operation: Uuid,
        destination: String,
    },
    /// Completes `phase` once its acknowledgement boundary is met.
    /// `protected_read_ts` is required exactly when finishing `Preparing`.
    Finish {
        table: TableId,
        operation: Uuid,
        phase: PhaseV3,
        #[serde(default)]
        protected_read_ts: Option<u64>,
    },
    /// Seal even a never-acquired source; identical semantics to the V2
    /// participant-level command.
    CancelParticipant {
        operation: Uuid,
        expected_epoch: u64,
    },
}

pub fn key(table: TableId) -> Vec<u8> {
    [JOURNAL_PREFIX, table.to_string().as_bytes()].concat()
}

/// The data-group id hosting a shard, matching the server's naming convention
/// (also used by the coverage check in the parent module).
pub fn destination_group(shard: ShardId) -> String {
    format!("shard-{shard}")
}

/// Placements made durable before destination group formation: the expected
/// content plus every destination shard's final placement. The Publishing flip
/// later replaces this with `destination_placements` (sources dropped).
pub fn prepare_placements(plan: &PlanV3) -> HashMap<ShardId, String> {
    let mut placements = plan.expected_placements.clone();
    if let Some(map) = &plan.destination_map {
        for shard in &map.shards {
            if let Some(node) = plan.destination_placements.get(&shard.id) {
                placements.insert(shard.id, node.clone());
            }
        }
    }
    placements
}

fn valid_destinations(plan: &PlanV3) -> bool {
    let bounded = |placements: &HashMap<ShardId, String>| placements.len() <= MAX_PLACEMENTS;
    if !bounded(&plan.expected_placements) || !bounded(&plan.destination_placements) {
        return false;
    }
    let Some(map) = &plan.destination_map else {
        // A plan publishing no map (e.g. drop-side merges) has no derivable
        // destination groups; the planner's destination list stands.
        return true;
    };
    let ids: std::collections::HashSet<ShardId> = map.shards.iter().map(|s| s.id).collect();
    let groups: BTreeSet<String> = map.shards.iter().map(|s| destination_group(s.id)).collect();
    map.table_id == plan.migration.table_id
        && ids.len() == map.shards.len()
        && groups.iter().collect::<BTreeSet<_>>() == plan.migration.destinations.iter().collect()
        && map
            .shards
            .iter()
            .all(|shard| plan.destination_placements.contains_key(&shard.id))
}

/// Decode-time invariants: a corrupt or inconsistent journal fails closed
/// rather than being interpreted. Every post-`Fencing` phase proves the
/// acknowledgement boundaries of the phases before it.
fn validate(record: &RecordV3) -> Result<()> {
    let plan = &record.plan;
    anyhow::ensure!(
        valid_plan(&plan.migration) && valid_destinations(plan),
        "invalid V3 coordinator plan"
    );
    for acks in [&record.acquired, &record.cancelled] {
        anyhow::ensure!(
            acks.iter().all(|(source, epoch)| plan
                .migration
                .source_epochs
                .get(source)
                .and_then(|e| e.checked_add(1))
                == Some(*epoch)),
            "invalid V3 coordinator acknowledgement"
        );
    }
    let in_plan = |groups: &BTreeSet<String>, all: &[String]| {
        groups.len() <= all.len() && groups.iter().all(|g| all.contains(g))
    };
    anyhow::ensure!(
        in_plan(&record.prepared, &plan.migration.destinations)
            && in_plan(&record.cleaned, &plan.migration.destinations),
        "invalid V3 destination acknowledgement"
    );
    anyhow::ensure!(
        record.checkpoints.len() <= plan.migration.sources.len()
            && record
                .checkpoints
                .keys()
                .all(|s| plan.migration.sources.contains(s))
            && record
                .checkpoints
                .values()
                .all(|c| c.cursor.len() <= MAX_CHECKPOINT_CURSOR),
        "invalid V3 copy checkpoint"
    );
    anyhow::ensure!(
        in_plan(&record.completed, &plan.migration.sources),
        "invalid V3 cutover acknowledgement"
    );
    if let Some(report) = &record.validation {
        anyhow::ensure!(
            report.matches == (report.source_digest == report.destination_digest),
            "inconsistent V3 validation report"
        );
    }
    let sources = plan.migration.sources.len();
    let destinations = plan.migration.destinations.len();
    if let Some(ordinal) = record.phase.ordinal() {
        if ordinal >= PhaseV3::Fenced.ordinal().unwrap() {
            anyhow::ensure!(
                record.acquired.len() == sources,
                "V3 journal past fencing without every source acquired"
            );
        }
        if ordinal >= PhaseV3::Copying.ordinal().unwrap() {
            anyhow::ensure!(
                record.prepared.len() == destinations,
                "V3 copy phase without every destination prepared"
            );
            anyhow::ensure!(
                record.protected_read_ts > 0,
                "V3 copy phase without a protected read timestamp"
            );
        }
        if ordinal >= PhaseV3::Validating.ordinal().unwrap() {
            anyhow::ensure!(
                plan.migration
                    .sources
                    .iter()
                    .all(|s| record.checkpoints.get(s).is_some_and(|c| c.complete)),
                "V3 validation phase with an incomplete copy"
            );
        }
        if ordinal >= PhaseV3::Publishing.ordinal().unwrap() {
            anyhow::ensure!(
                record.validation.is_some_and(|v| v.matches),
                "V3 publication without a matching validation report"
            );
        }
        if record.phase == PhaseV3::Done {
            anyhow::ensure!(
                record.completed.len() == sources,
                "V3 completion without every cutover acknowledgement"
            );
        }
    }
    if record.phase == PhaseV3::Cancelled {
        anyhow::ensure!(
            record.cancelled.len() == sources,
            "V3 cancellation without every source sealed"
        );
        anyhow::ensure!(
            record.prepared.iter().all(|d| record.cleaned.contains(d)),
            "V3 cancellation without destination cleanup"
        );
    }
    Ok(())
}

pub fn decode(bytes: &[u8]) -> Result<RecordV3> {
    match nodus_common::versioned::decode(bytes) {
        nodus_common::versioned::Envelope::Versioned {
            version: 3,
            payload,
        } => {
            let record: RecordV3 = serde_json::from_slice(payload)?;
            validate(&record)?;
            Ok(record)
        }
        _ => bail!("unsupported V3 coordinator record envelope"),
    }
}

pub fn read_record_v3(kv: &dyn KvEngine, table: TableId) -> Result<Option<RecordV3>> {
    let record = kv
        .get(&key(table), u64::MAX)?
        .map(|bytes| decode(&bytes))
        .transpose()?;
    anyhow::ensure!(
        record
            .as_ref()
            .is_none_or(|r| r.plan.migration.table_id == table),
        "V3 coordinator key does not match table"
    );
    Ok(record)
}

fn canonical_placements(placements: &HashMap<ShardId, String>) -> BTreeMap<String, String> {
    placements
        .iter()
        .map(|(shard, group)| (shard.to_string(), group.clone()))
        .collect()
}

/// Routing revalidation compares the expected map *and* placements content.
/// Valid before destination preparation flips placements (Plan/Begin/fence
/// completion).
pub fn routing_matches(plan: &PlanV3, meta: &dyn MetaStore) -> Result<bool> {
    if canonical(&plan.expected_map)? != canonical(&current_map(meta, plan.migration.table_id)?)? {
        return Ok(false);
    }
    Ok(canonical_placements(&plan.expected_placements)
        == canonical_placements(&meta.get_shard_placements()?))
}

/// Cutover-point revalidation: the map is still the expected content, but
/// placements already carry the destination entries written during Preparing.
/// Used by the `Publish` journal transition.
pub fn cutover_routing_matches(plan: &PlanV3, meta: &dyn MetaStore) -> Result<bool> {
    if canonical(&plan.expected_map)? != canonical(&current_map(meta, plan.migration.table_id)?)? {
        return Ok(false);
    }
    Ok(canonical_placements(&prepare_placements(plan))
        == canonical_placements(&meta.get_shard_placements()?))
}

/// Compares the current durable shard map against `map` (canonical content).
pub fn map_matches(map: &Option<ShardMap>, meta: &dyn MetaStore, table: TableId) -> Result<bool> {
    Ok(canonical(map)? == canonical(&current_map(meta, table)?)?)
}

/// Compares the current durable placements against `placements` (canonical).
pub fn placements_match(
    placements: &HashMap<ShardId, String>,
    meta: &dyn MetaStore,
) -> Result<bool> {
    Ok(canonical_placements(placements) == canonical_placements(&meta.get_shard_placements()?))
}

/// The lowest protected read timestamp of any in-flight V3 copy, for GC
/// watermark clamping. Journals at `Done`/`Cancelled` (or before the copy
/// start) release their protection. Decode failures fail closed (error).
pub fn protected_gc_watermark(kv: &dyn KvEngine) -> Result<Option<u64>> {
    let range = nodus_storage_api::KeyRange {
        start: Bytes::from_static(b"\x01migration/v3/"),
        end: Bytes::from_static(b"\x01migration/v30"),
    };
    let mut protected = None;
    for row in kv.scan(range, u64::MAX)? {
        let record = decode(&row?.value)?;
        if record.protected_read_ts > 0
            && !matches!(record.phase, PhaseV3::Done | PhaseV3::Cancelled)
        {
            protected = Some(protected.map_or(record.protected_read_ts, |p: u64| {
                p.min(record.protected_read_ts)
            }));
        }
    }
    Ok(protected)
}

fn save(kv: &dyn KvEngine, record: &RecordV3, index: u64) -> Result<()> {
    let key = key(record.plan.migration.table_id);
    write_version(kv, &key, record, index, 3)
}

fn past(record: &RecordV3, phase: PhaseV3) -> bool {
    match (record.phase.ordinal(), phase.ordinal()) {
        (Some(current), Some(target)) => current > target,
        _ => false,
    }
}

fn apply_plan(
    kv: &dyn KvEngine,
    plan: &PlanV3,
    current: Option<RecordV3>,
    index: u64,
    meta: &Arc<dyn MetaStore>,
) -> Result<ShardResponse> {
    let migration = &plan.migration;
    if serde_json::to_vec(plan)?.len() > 128 * 1024
        || !valid_plan(migration)
        || !valid_destinations(plan)
    {
        return Ok(reject("invalid or oversized migration plan"));
    }
    if let Some(record) = &current {
        if record.plan.migration.operation_id == migration.operation_id {
            return Ok(
                if serde_json::to_value(&record.plan)? == serde_json::to_value(plan)? {
                    accepted()
                } else {
                    reject("operation ID reused with a different plan")
                },
            );
        }
        if !matches!(record.phase, PhaseV3::Done | PhaseV3::Cancelled) {
            return Ok(reject("table already has an active migration"));
        }
    }
    if read_record(kv, migration.table_id)?.is_some() {
        return Ok(reject(
            "V1 journal requires explicit reconciliation before V3 planning",
        ));
    }
    if let Some(v2) = read_record_v2(kv, migration.table_id)?
        && v2.phase != PhaseV2::Cancelled
    {
        return Ok(reject(
            "V2 journal requires completion or cancellation before V3 planning",
        ));
    }
    if !routing_matches(plan, meta.as_ref())? {
        return Ok(reject("source routing no longer matches the plan"));
    }
    if !coverage_matches(migration, &plan.expected_map, meta)? {
        return Ok(reject("plan does not cover every row/index source group"));
    }
    // The meta fence epoch is readable at apply (this IS the meta group); a
    // plan carrying a stale one could never fence and would wedge at Fencing
    // with no way forward, so reject it while planning is still retryable.
    // Data-group epochs are stable between a table's operations (per-table
    // journal exclusion), so meta is the only racy epoch.
    let planned_meta_epoch = migration.source_epochs.get("shard-meta");
    let current_meta_epoch = crate::migration::read_fence(kv)?.map_or(0, |f| f.epoch);
    if planned_meta_epoch != Some(&current_meta_epoch) {
        return Ok(reject(
            "stale meta fence epoch in plan; re-plan against current state",
        ));
    }
    save(
        kv,
        &RecordV3 {
            plan: plan.clone(),
            phase: PhaseV3::Planned,
            acquired: BTreeMap::new(),
            cancelled: BTreeMap::new(),
            prepared: BTreeSet::new(),
            checkpoints: BTreeMap::new(),
            validation: None,
            completed: BTreeSet::new(),
            cleaned: BTreeSet::new(),
            protected_read_ts: 0,
        },
        index,
    )?;
    Ok(accepted())
}

pub fn apply_v3(
    kv: &dyn KvEngine,
    command: &CommandV3,
    index: u64,
    meta: Option<&Arc<dyn MetaStore>>,
) -> Result<ShardResponse> {
    if let CommandV3::CancelParticipant {
        operation,
        expected_epoch,
    } = command
    {
        return apply_cancel_participant(kv, *operation, *expected_epoch, index, meta.is_some());
    }
    let Some(meta) = meta else {
        return Ok(reject("coordinator journal belongs to the meta group"));
    };
    let (table, operation) = match command {
        CommandV3::Plan(plan) => (plan.migration.table_id, plan.migration.operation_id),
        CommandV3::Begin { table, operation }
        | CommandV3::RequestCancel { table, operation }
        | CommandV3::Ack {
            table, operation, ..
        }
        | CommandV3::PrepareAck {
            table, operation, ..
        }
        | CommandV3::CopyCheckpoint {
            table, operation, ..
        }
        | CommandV3::ValidationReport {
            table, operation, ..
        }
        | CommandV3::Publish { table, operation }
        | CommandV3::CompleteAck {
            table, operation, ..
        }
        | CommandV3::CleanupAck {
            table, operation, ..
        }
        | CommandV3::Finish {
            table, operation, ..
        } => (*table, *operation),
        CommandV3::CancelParticipant { .. } => unreachable!(),
    };
    clean_control_intent(kv, &key(table), index)?;
    let current = read_record_v3(kv, table)?;
    if let CommandV3::Plan(plan) = command {
        return apply_plan(kv, plan, current, index, meta);
    }
    let Some(mut record) = current else {
        return Ok(reject("migration journal is absent"));
    };
    if record.plan.migration.operation_id != operation {
        return Ok(reject("stale coordinator operation"));
    }
    match command {
        CommandV3::Begin { .. } => {
            if past(&record, PhaseV3::Planned) {
                return Ok(accepted());
            }
            if record.phase != PhaseV3::Planned || !routing_matches(&record.plan, meta.as_ref())? {
                return Ok(reject("cannot begin stale or cancelled migration"));
            }
            record.phase = PhaseV3::Fencing;
        }
        CommandV3::RequestCancel { .. } => match record.phase {
            PhaseV3::Cancelling | PhaseV3::Cancelled => return Ok(accepted()),
            PhaseV3::Publishing | PhaseV3::Completing | PhaseV3::Done => {
                return Ok(reject(
                    "publication committed; the operation can only complete",
                ));
            }
            _ => record.phase = PhaseV3::Cancelling,
        },
        CommandV3::Ack {
            source,
            epoch,
            cancelled,
            ..
        } => {
            if record
                .plan
                .migration
                .source_epochs
                .get(source)
                .and_then(|e| e.checked_add(1))
                != Some(*epoch)
            {
                return Ok(reject("invalid source acknowledgement"));
            }
            let (phase, acks) = if *cancelled {
                (PhaseV3::Cancelling, &mut record.cancelled)
            } else {
                (PhaseV3::Fencing, &mut record.acquired)
            };
            if acks.get(source) == Some(epoch) {
                return Ok(accepted()); // idempotent retry of a recorded ack
            }
            if record.phase != phase {
                return Ok(reject("acknowledgement belongs to an obsolete phase"));
            }
            acks.insert(source.clone(), *epoch);
        }
        CommandV3::PrepareAck { destination, .. } => {
            if !record.plan.migration.destinations.contains(destination) {
                return Ok(reject("unknown destination acknowledgement"));
            }
            if record.prepared.contains(destination) {
                return Ok(accepted());
            }
            if record.phase != PhaseV3::Preparing {
                return Ok(reject(
                    "destination prepare acknowledgement belongs to an obsolete phase",
                ));
            }
            record.prepared.insert(destination.clone());
        }
        CommandV3::CopyCheckpoint {
            source, checkpoint, ..
        } => {
            if !record.plan.migration.sources.contains(source)
                || checkpoint.cursor.len() > MAX_CHECKPOINT_CURSOR
            {
                return Ok(reject("invalid copy checkpoint"));
            }
            if record.checkpoints.get(source) == Some(checkpoint) {
                return Ok(accepted());
            }
            if record.phase != PhaseV3::Copying {
                return Ok(reject("copy checkpoint belongs to an obsolete phase"));
            }
            record
                .checkpoints
                .insert(source.clone(), checkpoint.clone());
        }
        CommandV3::ValidationReport { report, .. } => {
            if report.matches != (report.source_digest == report.destination_digest) {
                return Ok(reject("inconsistent validation report"));
            }
            if record.validation.as_ref() == Some(report) {
                return Ok(accepted());
            }
            if record.phase != PhaseV3::Validating {
                return Ok(reject("validation report belongs to an obsolete phase"));
            }
            record.validation = Some(*report);
            if !report.matches {
                // A mismatch cancels, never publishes.
                record.phase = PhaseV3::Cancelling;
            }
        }
        CommandV3::Publish { .. } => {
            if past(&record, PhaseV3::Validating) {
                return Ok(accepted());
            }
            if record.phase != PhaseV3::Validating {
                return Ok(reject("cannot publish outside the validating phase"));
            }
            if !record.validation.is_some_and(|v| v.matches) {
                return Ok(reject("publication requires a matching validation report"));
            }
            if !cutover_routing_matches(&record.plan, meta.as_ref())? {
                return Ok(reject("routing changed before publication"));
            }
            record.phase = PhaseV3::Publishing;
        }
        CommandV3::CompleteAck { source, .. } => {
            if !record.plan.migration.sources.contains(source) {
                return Ok(reject("unknown cutover acknowledgement"));
            }
            if record.completed.contains(source) {
                return Ok(accepted());
            }
            if record.phase != PhaseV3::Completing {
                return Ok(reject(
                    "cutover acknowledgement belongs to an obsolete phase",
                ));
            }
            record.completed.insert(source.clone());
        }
        CommandV3::CleanupAck { destination, .. } => {
            if !record.plan.migration.destinations.contains(destination) {
                return Ok(reject("unknown destination cleanup acknowledgement"));
            }
            if record.cleaned.contains(destination) {
                return Ok(accepted());
            }
            if record.phase != PhaseV3::Cancelling {
                return Ok(reject(
                    "destination cleanup acknowledgement belongs to an obsolete phase",
                ));
            }
            record.cleaned.insert(destination.clone());
        }
        CommandV3::Finish {
            phase,
            protected_read_ts,
            ..
        } => {
            if *phase == PhaseV3::Cancelling {
                if record.phase == PhaseV3::Cancelled {
                    return Ok(accepted());
                }
                if record.phase != PhaseV3::Cancelling {
                    return Ok(reject("cancellation finish belongs to an obsolete phase"));
                }
                if record.cancelled.len() != record.plan.migration.sources.len() {
                    return Ok(reject("not every source acknowledged cancellation"));
                }
                if !placements_match(&record.plan.expected_placements, meta.as_ref())? {
                    return Ok(reject(
                        "cancellation has not restored the pre-operation placements",
                    ));
                }
                if !record.prepared.iter().all(|d| record.cleaned.contains(d)) {
                    return Ok(reject("prepared destinations are not cleaned up"));
                }
                record.phase = PhaseV3::Cancelled;
            } else {
                let Some(target) = phase.ordinal() else {
                    return Ok(reject("invalid finish phase"));
                };
                if protected_read_ts.is_some() && *phase != PhaseV3::Preparing {
                    return Ok(reject(
                        "protected read timestamp only accompanies the copy start",
                    ));
                }
                if record.phase.ordinal().is_some_and(|o| o > target) {
                    return Ok(accepted()); // idempotent replay of a committed transition
                }
                if record.phase != *phase {
                    return Ok(reject("finish belongs to an obsolete phase"));
                }
                match phase {
                    PhaseV3::Fencing => {
                        if record.acquired.len() != record.plan.migration.sources.len() {
                            return Ok(reject("not every participant acknowledged fencing"));
                        }
                        if !routing_matches(&record.plan, meta.as_ref())? {
                            return Ok(reject("routing changed before fence completion"));
                        }
                        record.phase = PhaseV3::Fenced;
                    }
                    PhaseV3::Fenced => record.phase = PhaseV3::Preparing,
                    PhaseV3::Preparing => {
                        if record.prepared.len() != record.plan.migration.destinations.len() {
                            return Ok(reject("not every destination acknowledged preparation"));
                        }
                        let Some(ts) = protected_read_ts.filter(|ts| *ts > 0) else {
                            return Ok(reject("copy requires a protected read timestamp"));
                        };
                        record.protected_read_ts = ts;
                        record.phase = PhaseV3::Copying;
                    }
                    PhaseV3::Copying => {
                        if record
                            .plan
                            .migration
                            .sources
                            .iter()
                            .any(|s| !record.checkpoints.get(s).is_some_and(|c| c.complete))
                        {
                            return Ok(reject("copy is not complete for every source"));
                        }
                        record.phase = PhaseV3::Validating;
                    }
                    PhaseV3::Publishing => {
                        // The conditional routing flip must have committed.
                        if canonical(&record.plan.destination_map)?
                            != canonical(&current_map(
                                meta.as_ref(),
                                record.plan.migration.table_id,
                            )?)?
                            || canonical_placements(&record.plan.destination_placements)
                                != canonical_placements(&meta.get_shard_placements()?)
                        {
                            return Ok(reject("routing publication has not committed"));
                        }
                        record.phase = PhaseV3::Completing;
                    }
                    PhaseV3::Completing => {
                        if record.completed.len() != record.plan.migration.sources.len() {
                            return Ok(reject("not every participant acknowledged cutover"));
                        }
                        record.phase = PhaseV3::Done;
                    }
                    _ => return Ok(reject("phase has no finish transition")),
                }
            }
        }
        _ => unreachable!(),
    }
    tracing::info!(%table, %operation, phase = ?record.phase, "V3 coordinator transition applied");
    save(kv, &record, index)?;
    Ok(accepted())
}

/// Distinct fail-closed rejection for conditional routing publication.
fn conditional_reject(reason: &str) -> ShardResponse {
    ShardResponse {
        success: false,
        error: Some(format!(
            "conditional routing publication rejected: {reason}"
        )),
    }
}

/// Common gates for both conditional routing commands: the V3 journal must own
/// the operation, and a fence owned by another operation closes the meta
/// participant to this write. Phase-token legality is per command.
fn conditional_journal(
    kv: &dyn KvEngine,
    table: TableId,
    operation: Uuid,
) -> Result<std::result::Result<RecordV3, ShardResponse>> {
    let Some(record) = read_record_v3(kv, table)? else {
        return Ok(Err(conditional_reject(
            "no V3 journal owns this publication",
        )));
    };
    if let Some(fence) = read_fence(kv)?
        && fence.closed
        && fence.operation_id != operation
    {
        return Ok(Err(conditional_reject(
            "meta participant is fenced by another operation",
        )));
    }
    if record.plan.migration.operation_id != operation {
        return Ok(Err(conditional_reject("stale operation")));
    }
    Ok(Ok(record))
}

/// Applies the new shard map only if the current durable content equals the
/// expected content carried in the command, inside the journal's publication
/// step. A replayed publication whose content already landed is accepted
/// without rewriting; any other drift fails closed. The map flips only at the
/// `Publishing` step.
pub fn apply_conditional_map(
    kv: &dyn KvEngine,
    table: TableId,
    operation: Uuid,
    phase: PhaseV3,
    expected: &Option<ShardMap>,
    map: &ShardMap,
    meta: Option<&Arc<dyn MetaStore>>,
) -> Result<ShardResponse> {
    let Some(meta) = meta else {
        return Ok(conditional_reject(
            "conditional routing belongs to the meta group",
        ));
    };
    if map.table_id != table || phase != PhaseV3::Publishing {
        return Ok(conditional_reject(
            "shard map does not match the journal table or phase token",
        ));
    }
    let record = match conditional_journal(kv, table, operation)? {
        Ok(record) => record,
        Err(rejection) => return Ok(rejection),
    };
    if canonical(&record.plan.destination_map)? != canonical(&Some(map.clone()))?
        || canonical(&record.plan.expected_map)? != canonical(expected)?
    {
        return Ok(conditional_reject(
            "publication payload does not match the journaled plan",
        ));
    }
    let current = current_map(meta.as_ref(), table)?;
    if past(&record, PhaseV3::Validating) && canonical(&current)? == canonical(&Some(map.clone()))?
    {
        return Ok(accepted()); // idempotent replay of an applied publication
    }
    if record.phase != PhaseV3::Publishing || canonical(&current)? != canonical(expected)? {
        return Ok(conditional_reject(
            "current routing does not match the expected content",
        ));
    }
    meta.update_shard_map(map.clone())?;
    tracing::info!(%table, %operation, "conditional shard map publication applied");
    Ok(accepted())
}

/// Same conditional discipline for the global placement record, with three
/// legal steps: `Preparing` adds the destination placements before group
/// formation (payload must be the derived [`prepare_placements`] content),
/// `Publishing` commits the final cutover placements, and `Cancelling`
/// restores the pre-operation placements after the preparation flip.
pub fn apply_conditional_placements(
    kv: &dyn KvEngine,
    table: TableId,
    operation: Uuid,
    phase: PhaseV3,
    expected: &HashMap<ShardId, String>,
    placements: &HashMap<ShardId, String>,
    meta: Option<&Arc<dyn MetaStore>>,
) -> Result<ShardResponse> {
    let Some(meta) = meta else {
        return Ok(conditional_reject(
            "conditional routing belongs to the meta group",
        ));
    };
    let record = match conditional_journal(kv, table, operation)? {
        Ok(record) => record,
        Err(rejection) => return Ok(rejection),
    };
    let (expected_content, payload_content) = match phase {
        PhaseV3::Preparing => (
            record.plan.expected_placements.clone(),
            prepare_placements(&record.plan),
        ),
        PhaseV3::Publishing => (
            prepare_placements(&record.plan),
            record.plan.destination_placements.clone(),
        ),
        PhaseV3::Cancelling => (
            prepare_placements(&record.plan),
            record.plan.expected_placements.clone(),
        ),
        _ => {
            return Ok(conditional_reject("stale journal phase token"));
        }
    };
    if canonical_placements(&payload_content) != canonical_placements(placements)
        || canonical_placements(&expected_content) != canonical_placements(expected)
    {
        return Ok(conditional_reject(
            "publication payload does not match the journaled plan",
        ));
    }
    let current = meta.get_shard_placements()?;
    if canonical_placements(&current) == canonical_placements(placements) {
        return Ok(accepted()); // the requested content already landed
    }
    if record.phase != phase || canonical_placements(&current) != canonical_placements(expected) {
        return Ok(conditional_reject(
            "current routing does not match the expected content",
        ));
    }
    meta.update_shard_placements(placements)?;
    tracing::info!(%table, %operation, phase = ?phase, "conditional placements publication applied");
    Ok(accepted())
}
