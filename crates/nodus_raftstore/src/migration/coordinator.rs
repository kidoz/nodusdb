//! Version 2 coordinator journal. Acknowledgements are persisted separately
//! from RPC completion; recovery never infers success from a missing response.
pub mod driver;

use super::*;
use nodus_meta::{MetaStore, ShardMap, ShardMapNotFound};
use std::collections::{BTreeMap, BTreeSet};

pub const JOURNAL_PREFIX: &[u8] = b"\x01migration/v2/table/";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanV2 {
    pub migration: MigrationPlan,
    pub expected_map: Option<ShardMap>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhaseV2 {
    Planned,
    Fencing,
    Fenced,
    Cancelling,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordV2 {
    pub plan: PlanV2,
    pub phase: PhaseV2,
    pub acquired: BTreeMap<String, u64>,
    pub cancelled: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CommandV2 {
    Plan(PlanV2),
    Begin {
        table: TableId,
        operation: Uuid,
    },
    RequestCancel {
        table: TableId,
        operation: Uuid,
    },
    Ack {
        table: TableId,
        operation: Uuid,
        source: String,
        epoch: u64,
        cancelled: bool,
    },
    Finish {
        table: TableId,
        operation: Uuid,
        cancelled: bool,
    },
    /// Seal even a never-acquired source, so delayed Acquire cannot resurrect
    /// the cancelled operation. Pending transactions may delay this seal.
    CancelParticipant {
        operation: Uuid,
        expected_epoch: u64,
    },
}

pub fn key(table: TableId) -> Vec<u8> {
    [JOURNAL_PREFIX, table.to_string().as_bytes()].concat()
}

pub fn decode(bytes: &[u8]) -> Result<RecordV2> {
    match nodus_common::versioned::decode(bytes) {
        nodus_common::versioned::Envelope::Versioned {
            version: 2,
            payload,
        } => {
            let record: RecordV2 = serde_json::from_slice(payload)?;
            anyhow::ensure!(
                valid_plan(&record.plan.migration),
                "invalid coordinator plan"
            );
            for acks in [&record.acquired, &record.cancelled] {
                anyhow::ensure!(
                    acks.iter().all(|(source, epoch)| record
                        .plan
                        .migration
                        .source_epochs
                        .get(source)
                        .and_then(|e| e.checked_add(1))
                        == Some(*epoch)),
                    "invalid coordinator acknowledgement"
                );
            }
            anyhow::ensure!(
                record.phase != PhaseV2::Fenced
                    || record.acquired.len() == record.plan.migration.sources.len(),
                "incomplete fenced journal"
            );
            anyhow::ensure!(
                record.phase != PhaseV2::Cancelled
                    || record.cancelled.len() == record.plan.migration.sources.len(),
                "incomplete cancelled journal"
            );
            Ok(record)
        }
        _ => bail!("unsupported coordinator record envelope"),
    }
}

pub fn read_record_v2(kv: &dyn KvEngine, table: TableId) -> Result<Option<RecordV2>> {
    let record = kv
        .get(&key(table), u64::MAX)?
        .map(|bytes| decode(&bytes))
        .transpose()?;
    anyhow::ensure!(
        record
            .as_ref()
            .is_none_or(|r| r.plan.migration.table_id == table),
        "coordinator key does not match table"
    );
    Ok(record)
}

pub fn current_map(meta: &dyn MetaStore, table: TableId) -> Result<Option<ShardMap>> {
    match meta.get_shard_map(table) {
        Ok(map) => Ok(Some(map)),
        Err(e) if e.downcast_ref::<ShardMapNotFound>().is_some() => Ok(None),
        Err(e) => Err(e),
    }
}

fn canonical(map: &Option<ShardMap>) -> Result<serde_json::Value> {
    let mut map = map.clone();
    if let Some(map) = &mut map {
        map.shards.sort_by_key(|s| s.id.to_string());
    }
    Ok(serde_json::to_value(map)?)
}

pub fn routing_matches(plan: &PlanV2, meta: &dyn MetaStore) -> Result<bool> {
    Ok(canonical(&plan.expected_map)? == canonical(&current_map(meta, plan.migration.table_id)?)?)
}

fn save(kv: &dyn KvEngine, record: &RecordV2, index: u64) -> Result<()> {
    let key = key(record.plan.migration.table_id);
    write_version(kv, &key, record, index, 2)
}

pub(super) fn apply_v2(
    kv: &dyn KvEngine,
    command: &CommandV2,
    index: u64,
    meta: Option<&std::sync::Arc<dyn MetaStore>>,
) -> Result<ShardResponse> {
    if let CommandV2::CancelParticipant {
        operation,
        expected_epoch,
    } = command
    {
        clean_control_intent(kv, FENCE_KEY, index)?;
        let Some(epoch) = expected_epoch.checked_add(1) else {
            return Ok(reject("epoch exhausted"));
        };
        let current = read_fence(kv)?;
        if let Some(f) = &current {
            if f.epoch > epoch {
                return Ok(accepted());
            } // the old acquire is permanently stale
            if f.epoch == epoch && f.operation_id != *operation {
                return Ok(accepted());
            }
            if f.operation_id == *operation && f.epoch == epoch {
                if !f.closed {
                    return Ok(accepted());
                }
                return apply_control(
                    kv,
                    &MigrationCommandV1::Release {
                        operation_id: *operation,
                        epoch,
                    },
                    index,
                    meta.is_some(),
                );
            }
            if f.closed {
                return Ok(reject("another operation owns the source"));
            }
        }
        if current.as_ref().map_or(0, |f| f.epoch) != *expected_epoch {
            return Ok(reject("unexpected cancellation epoch"));
        }
        if !quiescent(kv, meta.is_some())? {
            return Ok(reject("pending transactions prevent cancellation seal"));
        }
        write(
            kv,
            FENCE_KEY,
            &ParticipantFence {
                operation_id: *operation,
                epoch,
                closed: false,
            },
            index,
        )?;
        return Ok(accepted());
    }
    let Some(meta) = meta else {
        return Ok(reject("coordinator journal belongs to the meta group"));
    };
    let (table, operation) = match command {
        CommandV2::Plan(plan) => (plan.migration.table_id, plan.migration.operation_id),
        CommandV2::Begin { table, operation }
        | CommandV2::RequestCancel { table, operation }
        | CommandV2::Ack {
            table, operation, ..
        }
        | CommandV2::Finish {
            table, operation, ..
        } => (*table, *operation),
        CommandV2::CancelParticipant { .. } => unreachable!(),
    };
    clean_control_intent(kv, &key(table), index)?;
    let current = read_record_v2(kv, table)?;
    if let CommandV2::Plan(plan) = command {
        let p = &plan.migration;
        if serde_json::to_vec(plan)?.len() > 128 * 1024 || !valid_plan(p) {
            return Ok(reject("invalid or oversized migration plan"));
        }
        if let Some(record) = &current {
            if record.plan.migration.operation_id == operation {
                return Ok(
                    if serde_json::to_value(&record.plan)? == serde_json::to_value(plan)? {
                        accepted()
                    } else {
                        reject("operation ID reused with a different plan")
                    },
                );
            }
            if record.phase != PhaseV2::Cancelled {
                return Ok(reject("table already has an active migration"));
            }
        }
        if read_record(kv, table)?.is_some() {
            return Ok(reject(
                "V1 journal requires explicit reconciliation before V2 planning",
            ));
        }
        if !routing_matches(plan, meta.as_ref())? {
            return Ok(reject("source routing no longer matches the plan"));
        }
        // Validate complete coverage, not just descriptor IDs. Metadata stores
        // are consulted at apply, so a stale planner cannot pass a local check.
        use nodus_sharding::ShardRouter;
        nodus_sharding::CatalogShardRouter::new(meta.clone()).snapshot(table)?;
        let mut expected = BTreeSet::from(["shard-meta".to_string()]); // includes current secondary index namespace
        if let Some(map) = &plan.expected_map {
            expected.extend(map.shards.iter().map(|s| format!("shard-{}", s.id)));
        }
        if p.sources.first().map(String::as_str) != Some("shard-meta")
            || expected != p.sources.iter().cloned().collect()
        {
            return Ok(reject("plan does not cover every row/index source group"));
        }
        save(
            kv,
            &RecordV2 {
                plan: plan.clone(),
                phase: PhaseV2::Planned,
                acquired: BTreeMap::new(),
                cancelled: BTreeMap::new(),
            },
            index,
        )?;
        return Ok(accepted());
    }
    let Some(mut record) = current else {
        return Ok(reject("migration journal is absent"));
    };
    if record.plan.migration.operation_id != operation {
        return Ok(reject("stale coordinator operation"));
    }
    match command {
        CommandV2::Begin { .. } => {
            if matches!(record.phase, PhaseV2::Fencing | PhaseV2::Fenced) {
                return Ok(accepted());
            }
            if record.phase != PhaseV2::Planned || !routing_matches(&record.plan, meta.as_ref())? {
                return Ok(reject("cannot begin stale or cancelled migration"));
            }
            record.phase = PhaseV2::Fencing;
        }
        CommandV2::RequestCancel { .. } => {
            if record.phase == PhaseV2::Cancelled {
                return Ok(accepted());
            }
            record.phase = PhaseV2::Cancelling;
        }
        CommandV2::Ack {
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
                (PhaseV2::Cancelling, &mut record.cancelled)
            } else {
                (PhaseV2::Fencing, &mut record.acquired)
            };
            if record.phase != phase {
                return Ok(reject("acknowledgement belongs to an obsolete phase"));
            }
            acks.insert(source.clone(), *epoch);
        }
        CommandV2::Finish { cancelled, .. } => {
            let (from, to, count) = if *cancelled {
                (
                    PhaseV2::Cancelling,
                    PhaseV2::Cancelled,
                    record.cancelled.len(),
                )
            } else {
                (PhaseV2::Fencing, PhaseV2::Fenced, record.acquired.len())
            };
            if record.phase == to {
                return Ok(accepted());
            }
            if record.phase != from || count != record.plan.migration.sources.len() {
                return Ok(reject("not every participant acknowledged this phase"));
            }
            if !cancelled && !routing_matches(&record.plan, meta.as_ref())? {
                return Ok(reject("routing changed before fence completion"));
            }
            record.phase = to;
        }
        _ => unreachable!(),
    }
    tracing::info!(%table, %operation, phase = ?record.phase, "coordinator transition applied");
    save(kv, &record, index)?;
    Ok(accepted())
}
