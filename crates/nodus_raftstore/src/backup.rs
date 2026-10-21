//! Cluster backup job journal, replicated on the meta group. One versioned
//! record per backup job makes a cluster backup atomic and recoverable: the
//! job is journaled before any export, heartbeats while the exporter works,
//! and reaches `Completed` only after the manifest's own commit point (every
//! object verified). A job that never completes is never restorable; the meta
//! leader's sweep fails jobs whose heartbeat proves the creator died.
//!
//! The journal folds under the migration cluster level: only binaries
//! reporting `migration_version >= 3` (this lineage) populate a finalized
//! level-3 cluster, and they all decode these records; older binaries fail
//! closed on the unknown command variant.
use anyhow::{Result, ensure};
use bytes::Bytes;
use nodus_storage_api::KvEngine;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ShardResponse;

/// Journal key prefix: `\x01backup/v1/job/{job_id}`.
pub const JOURNAL_PREFIX: &[u8] = b"\x01backup/v1/job/";
const JOURNAL_PREFIX_END: &[u8] = b"\x01backup/v1/job0";
/// Retained terminal records are bounded; the oldest are evicted past this.
const MAX_TERMINAL_JOBS: usize = 64;
const MAX_CLUSTER_ID: usize = 128;
const MAX_BACKUP_ID: usize = 128;
const MAX_REASON: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PhaseV1 {
    Exporting,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum KindV1 {
    Full,
    Incremental { parent: String },
}

/// One journaled backup job. `protected_ts` pins MVCC GC for the job's
/// lifetime: an incremental export reads the version window above its parent's
/// snapshot, so the parent timestamp must survive reclamation until the job
/// terminates.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobV1 {
    pub job_id: Uuid,
    /// The manifest's backup id — journal and repository share one identity.
    pub backup_id: String,
    /// Node running the export.
    pub creator: u64,
    pub cluster_id: String,
    pub kind: KindV1,
    pub backup_ts: u64,
    pub protected_ts: Option<u64>,
    /// Exporter heartbeat, wall-clock milliseconds (the exporter's clock; the
    /// sweep compares it against the meta leader's clock — the staleness bound
    /// dwarfs any sane skew).
    pub updated_at_ms: u64,
    pub phase: PhaseV1,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CommandV1 {
    Plan(Box<JobV1>),
    Heartbeat { job: Uuid, at_ms: u64 },
    Complete { job: Uuid },
    Fail { job: Uuid, reason: String },
    Cancel { job: Uuid },
}

fn key(job: Uuid) -> Vec<u8> {
    [JOURNAL_PREFIX, job.to_string().as_bytes()].concat()
}

fn accepted() -> ShardResponse {
    ShardResponse {
        success: true,
        error: None,
    }
}

fn reject(reason: &str) -> ShardResponse {
    ShardResponse {
        success: false,
        error: Some(reason.into()),
    }
}

pub fn decode(bytes: &[u8]) -> Result<JobV1> {
    match nodus_common::versioned::decode(bytes) {
        nodus_common::versioned::Envelope::Versioned {
            version: 1,
            payload,
        } => {
            let job: JobV1 = serde_json::from_slice(payload)?;
            validate(&job)?;
            Ok(job)
        }
        nodus_common::versioned::Envelope::Versioned { version, .. } => {
            anyhow::bail!("unsupported backup journal version {version}")
        }
        nodus_common::versioned::Envelope::Legacy(_) => {
            anyhow::bail!("backup journal requires a versioned envelope")
        }
    }
}

fn validate(job: &JobV1) -> Result<()> {
    ensure!(!job.job_id.is_nil(), "backup job id is nil");
    ensure!(
        !job.backup_id.is_empty()
            && job.backup_id.len() <= MAX_BACKUP_ID
            && job.cluster_id.len() <= MAX_CLUSTER_ID
            && job.error.as_ref().map_or(0, String::len) <= MAX_REASON,
        "backup journal field out of bounds"
    );
    ensure!(
        job.phase != PhaseV1::Exporting || job.error.is_none(),
        "an exporting job carries no error"
    );
    ensure!(
        job.protected_ts
            .is_none_or(|ts| job.backup_ts == 0 || ts <= job.backup_ts),
        "protected timestamp beyond the backup timestamp"
    );
    Ok(())
}

fn save(kv: &dyn KvEngine, job: &JobV1, index: u64) -> Result<()> {
    let payload = serde_json::to_vec(job)?;
    let bytes = nodus_common::versioned::encode(1, &payload);
    let txn = nodus_storage_api::TxnId(Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        &[key(job.job_id), index.to_be_bytes().to_vec()].concat(),
    ));
    kv.abort(txn)?;
    kv.write_intent(txn, Bytes::from(key(job.job_id)), Bytes::from(bytes))?;
    kv.commit(txn, index)?;
    Ok(())
}

pub fn read_job(kv: &dyn KvEngine, job: Uuid) -> Result<Option<JobV1>> {
    kv.get(&key(job), u64::MAX)?.map(|b| decode(&b)).transpose()
}

/// Every journaled job, terminal or not, oldest first.
pub fn list_jobs(kv: &dyn KvEngine) -> Result<Vec<JobV1>> {
    let mut jobs = Vec::new();
    for row in kv.scan(
        nodus_storage_api::KeyRange {
            start: Bytes::from_static(JOURNAL_PREFIX),
            end: Bytes::from_static(JOURNAL_PREFIX_END),
        },
        u64::MAX,
    )? {
        jobs.push(decode(&row?.value)?);
    }
    jobs.sort_by_key(|j| j.updated_at_ms);
    Ok(jobs)
}

/// The GC pin from in-flight incremental exports: the minimum protected
/// timestamp across non-terminal jobs that carry one.
pub fn protected_gc_watermark(kv: &dyn KvEngine) -> Result<Option<u64>> {
    let mut pin = None;
    for job in list_jobs(kv)? {
        if !matches!(job.phase, PhaseV1::Exporting) {
            continue;
        }
        if let Some(ts) = job.protected_ts {
            pin = Some(pin.map_or(ts, |p: u64| p.min(ts)));
        }
    }
    Ok(pin)
}

pub fn apply(kv: &dyn KvEngine, command: &CommandV1, index: u64) -> Result<ShardResponse> {
    match command {
        CommandV1::Plan(job) => {
            validate(job)?;
            ensure!(
                job.phase == PhaseV1::Exporting,
                "a planned job starts exporting"
            );
            if let Some(existing) = read_job(kv, job.job_id)? {
                return Ok(if &existing == job.as_ref() {
                    accepted() // idempotent replay
                } else {
                    reject("backup job id reused with different content")
                });
            }
            for other in list_jobs(kv)? {
                if other.phase == PhaseV1::Exporting {
                    return Ok(reject("another backup job is in flight"));
                }
            }
            // Symmetric exclusion: a migration that could still drive routing
            // changes must not interleave with a cluster export.
            if crate::migration::active_journal_table(kv)?.is_some() {
                return Ok(reject("a migration is in flight; backup waits"));
            }
            save(kv, job, index)?;
            // Bound retained terminal history; eviction is a real MVCC
            // tombstone, so readers never see a partial value.
            let mut terminal: Vec<JobV1> = list_jobs(kv)?
                .into_iter()
                .filter(|j| j.phase != PhaseV1::Exporting)
                .collect();
            if terminal.len() > MAX_TERMINAL_JOBS {
                terminal.sort_by_key(|j| j.updated_at_ms);
                for evicted in terminal.iter().take(terminal.len() - MAX_TERMINAL_JOBS) {
                    let txn = nodus_storage_api::TxnId(Uuid::new_v5(
                        &Uuid::NAMESPACE_OID,
                        &[b"backup-evict".as_slice(), evicted.job_id.as_bytes()].concat(),
                    ));
                    kv.abort(txn)?;
                    kv.delete_intent(txn, Bytes::from(key(evicted.job_id)))?;
                    kv.commit(txn, index)?;
                }
            }
            Ok(accepted())
        }
        CommandV1::Heartbeat { job, at_ms } => {
            let Some(mut record) = read_job(kv, *job)? else {
                return Ok(reject("backup journal is absent"));
            };
            if record.phase != PhaseV1::Exporting {
                return Ok(accepted()); // terminal: heartbeat is moot
            }
            if *at_ms > record.updated_at_ms {
                record.updated_at_ms = *at_ms;
                save(kv, &record, index)?;
            }
            Ok(accepted())
        }
        CommandV1::Complete { job } => {
            let Some(mut record) = read_job(kv, *job)? else {
                return Ok(reject("backup journal is absent"));
            };
            match record.phase {
                PhaseV1::Completed => Ok(accepted()),
                PhaseV1::Exporting => {
                    record.phase = PhaseV1::Completed;
                    save(kv, &record, index)?;
                    Ok(accepted())
                }
                PhaseV1::Failed | PhaseV1::Cancelled => {
                    Ok(reject("a terminated backup job cannot complete"))
                }
            }
        }
        CommandV1::Fail { job, reason } => {
            ensure!(reason.len() <= MAX_REASON, "failure reason too long");
            let Some(mut record) = read_job(kv, *job)? else {
                return Ok(reject("backup journal is absent"));
            };
            match record.phase {
                PhaseV1::Failed | PhaseV1::Cancelled => Ok(accepted()),
                PhaseV1::Completed => Ok(reject("a completed backup job cannot fail")),
                PhaseV1::Exporting => {
                    record.phase = PhaseV1::Failed;
                    record.error = Some(reason.clone());
                    save(kv, &record, index)?;
                    Ok(accepted())
                }
            }
        }
        CommandV1::Cancel { job } => {
            let Some(mut record) = read_job(kv, *job)? else {
                return Ok(reject("backup journal is absent"));
            };
            match record.phase {
                PhaseV1::Cancelled => Ok(accepted()),
                PhaseV1::Exporting => {
                    record.phase = PhaseV1::Cancelled;
                    save(kv, &record, index)?;
                    Ok(accepted())
                }
                _ => Ok(reject("a terminated backup job cannot be cancelled")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv() -> nodus_storage_mem::MemKvEngine {
        nodus_storage_mem::MemKvEngine::new()
    }

    fn job(id: u128, phase: PhaseV1) -> JobV1 {
        JobV1 {
            job_id: Uuid::from_u128(id),
            backup_id: format!("backup-{id}"),
            creator: 1,
            cluster_id: "test-cluster".into(),
            kind: KindV1::Full,
            backup_ts: 100,
            protected_ts: None,
            updated_at_ms: 1000,
            phase,
            error: None,
        }
    }

    fn plan(id: u128) -> CommandV1 {
        CommandV1::Plan(Box::new(job(id, PhaseV1::Exporting)))
    }

    #[test]
    fn happy_path_plan_heartbeat_complete_with_idempotent_replays() {
        let kv = kv();
        assert!(apply(&kv, &plan(1), 1).unwrap().success);
        // Identical replay is accepted; a different body for the same id is not.
        assert!(apply(&kv, &plan(1), 2).unwrap().success);
        let mut other = job(1, PhaseV1::Exporting);
        other.backup_id = "different".into();
        assert!(
            !apply(&kv, &CommandV1::Plan(Box::new(other)), 3)
                .unwrap()
                .success
        );

        let j = read_job(&kv, Uuid::from_u128(1)).unwrap().unwrap();
        assert_eq!(j.phase, PhaseV1::Exporting);

        // Heartbeats advance monotonically.
        assert!(
            apply(
                &kv,
                &CommandV1::Heartbeat {
                    job: Uuid::from_u128(1),
                    at_ms: 500
                },
                4
            )
            .unwrap()
            .success
        );
        assert!(
            apply(
                &kv,
                &CommandV1::Heartbeat {
                    job: Uuid::from_u128(1),
                    at_ms: 2000
                },
                5
            )
            .unwrap()
            .success
        );
        let j = read_job(&kv, Uuid::from_u128(1)).unwrap().unwrap();
        assert_eq!(j.updated_at_ms, 2000);

        assert!(
            apply(
                &kv,
                &CommandV1::Complete {
                    job: Uuid::from_u128(1)
                },
                6
            )
            .unwrap()
            .success
        );
        assert!(
            apply(
                &kv,
                &CommandV1::Complete {
                    job: Uuid::from_u128(1)
                },
                7
            )
            .unwrap()
            .success
        );
        assert_eq!(
            read_job(&kv, Uuid::from_u128(1)).unwrap().unwrap().phase,
            PhaseV1::Completed
        );
        // Terminal jobs reject fail/cancel and ignore heartbeats.
        assert!(
            !apply(
                &kv,
                &CommandV1::Fail {
                    job: Uuid::from_u128(1),
                    reason: "x".into()
                },
                8
            )
            .unwrap()
            .success
        );
        assert!(
            apply(
                &kv,
                &CommandV1::Heartbeat {
                    job: Uuid::from_u128(1),
                    at_ms: 3000
                },
                9
            )
            .unwrap()
            .success
        );
        assert_eq!(
            read_job(&kv, Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .updated_at_ms,
            2000
        );
    }

    #[test]
    fn one_exporting_job_at_a_time_and_terminal_records_free_the_slot() {
        let kv = kv();
        assert!(apply(&kv, &plan(1), 1).unwrap().success);
        assert!(!apply(&kv, &plan(2), 2).unwrap().success);
        assert!(
            apply(
                &kv,
                &CommandV1::Fail {
                    job: Uuid::from_u128(1),
                    reason: "upload died".into()
                },
                3
            )
            .unwrap()
            .success
        );
        assert!(apply(&kv, &plan(2), 4).unwrap().success);
        assert!(
            apply(
                &kv,
                &CommandV1::Cancel {
                    job: Uuid::from_u128(2)
                },
                5
            )
            .unwrap()
            .success
        );
        assert!(apply(&kv, &plan(3), 6).unwrap().success);
        let failed = read_job(&kv, Uuid::from_u128(1)).unwrap().unwrap();
        assert_eq!(failed.phase, PhaseV1::Failed);
        assert_eq!(failed.error.as_deref(), Some("upload died"));
        assert_eq!(
            read_job(&kv, Uuid::from_u128(2)).unwrap().unwrap().phase,
            PhaseV1::Cancelled
        );
    }

    #[test]
    fn unknown_or_absent_jobs_fail_closed() {
        let kv = kv();
        for cmd in [
            CommandV1::Heartbeat {
                job: Uuid::from_u128(9),
                at_ms: 1,
            },
            CommandV1::Complete {
                job: Uuid::from_u128(9),
            },
            CommandV1::Fail {
                job: Uuid::from_u128(9),
                reason: "x".into(),
            },
            CommandV1::Cancel {
                job: Uuid::from_u128(9),
            },
        ] {
            assert!(!apply(&kv, &cmd, 1).unwrap().success);
        }
    }

    #[test]
    fn decode_fails_closed_on_legacy_or_future_envelopes() {
        assert!(decode(b"legacy-bytes").is_err());
        let future = nodus_common::versioned::encode(99, b"{}");
        assert!(decode(&future).is_err());
    }

    #[test]
    fn protected_gc_watermark_pins_inflight_incrementals_only() {
        let kv = kv();
        assert_eq!(protected_gc_watermark(&kv).unwrap(), None);
        let mut inc = job(1, PhaseV1::Exporting);
        inc.kind = KindV1::Incremental {
            parent: "backup-0".into(),
        };
        inc.protected_ts = Some(41);
        assert!(
            apply(&kv, &CommandV1::Plan(Box::new(inc)), 1)
                .unwrap()
                .success
        );
        // A full export carries no pin.
        assert!(
            apply(
                &kv,
                &CommandV1::Fail {
                    job: Uuid::from_u128(1),
                    reason: "x".into()
                },
                2
            )
            .unwrap()
            .success
        );
        assert_eq!(protected_gc_watermark(&kv).unwrap(), None);

        let mut inc = job(2, PhaseV1::Exporting);
        inc.kind = KindV1::Incremental {
            parent: "backup-1".into(),
        };
        inc.protected_ts = Some(41);
        assert!(
            apply(&kv, &CommandV1::Plan(Box::new(inc)), 3)
                .unwrap()
                .success
        );
        assert_eq!(protected_gc_watermark(&kv).unwrap(), Some(41));
        assert!(
            apply(
                &kv,
                &CommandV1::Complete {
                    job: Uuid::from_u128(2)
                },
                4
            )
            .unwrap()
            .success
        );
        assert_eq!(protected_gc_watermark(&kv).unwrap(), None);
    }

    #[test]
    fn terminal_history_is_bounded_by_eviction() {
        let kv = kv();
        for i in 0..(MAX_TERMINAL_JOBS + 3) as u128 {
            assert!(
                apply(&kv, &plan(1000 + i), (2 * i + 1) as u64)
                    .unwrap()
                    .success
            );
            assert!(
                apply(
                    &kv,
                    &CommandV1::Complete {
                        job: Uuid::from_u128(1000 + i)
                    },
                    (2 * i + 2) as u64
                )
                .unwrap()
                .success
            );
        }
        let jobs = list_jobs(&kv).unwrap();
        assert!(jobs.len() <= MAX_TERMINAL_JOBS + 1);
        // The oldest terminal records were evicted.
        assert!(read_job(&kv, Uuid::from_u128(1000)).unwrap().is_none());
        assert!(read_job(&kv, Uuid::from_u128(1001)).unwrap().is_none());
        assert!(read_job(&kv, Uuid::from_u128(1002)).unwrap().is_some());
    }
}
