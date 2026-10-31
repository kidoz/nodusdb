//! Production [`BackendV3`] over the real multi-Raft manager: the journal and
//! conditional routing writes go through the meta group (via the router, so a
//! remote meta leader is reached by capability-checked mTLS forwarding), and
//! copy/validation read source namespaces behind a read barrier on the group.
//!
//! Destination formation is placement-driven: a locally placed group is formed
//! replicated (`MultiRaftManager::form_replicated`); a remotely placed one is
//! verified serving via its leader read index while the owner's reconcile loop
//! forms it. Fencing, copy, and decommission still require sources hosted on
//! this node — cross-node copy lands with the rebalance operation. Every
//! effect is idempotent so a re-driven step after a timeout never
//! double-applies.
use crate::{
    multi_raft::{META_SHARD, MultiRaftManager},
    raft_router::RaftRouter,
};
use anyhow::{Context, Result, anyhow, ensure};
use bytes::Bytes;
use nodus_catalog::TableId;
use nodus_raftstore::{
    ShardCommand,
    migration::{
        self,
        coordinator::v3::{
            self, CheckpointV3, CommandV3, PlanV3, RecordV3, ValidationV3,
            driver::{BackendV3, RoutingState},
        },
    },
};
use nodus_storage_api::{KeyRange, KvEngine, KvPair, NamespacedKvEngine};
use std::sync::Arc;
use uuid::Uuid;

/// Rows copied per journaled checkpoint: large enough to bound Raft traffic,
/// small enough to keep one step well inside its deadline.
const COPY_BATCH_ROWS: usize = 256;

/// Journal/discovery step deadline, shared by the discovery worker.
pub(crate) const STEP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(120);
const COPY_CONCURRENCY: usize = 16;

#[derive(Clone)]
pub struct ProductionBackendV3 {
    manager: Arc<MultiRaftManager>,
    router: RaftRouter,
}

impl ProductionBackendV3 {
    pub fn new(manager: Arc<MultiRaftManager>, router: RaftRouter) -> Self {
        Self { manager, router }
    }

    /// Submits a command through the router on a blocking thread (the router's
    /// sync handle waits via `blocking_recv`).
    async fn submit(&self, group: &str, command: ShardCommand) -> Result<()> {
        let router = self.router.clone();
        let group = group.to_string();
        tokio::task::spawn_blocking(move || router.submit(&group, command))
            .await
            .map_err(|e| anyhow!("migration submit task failed: {e}"))?
    }

    /// Waits until this node's replica of `group` has applied every write
    /// committed before the call, so a local namespace scan reflects the
    /// quorum-committed content.
    async fn barrier(&self, group: &str) -> Result<()> {
        let router = self.router.clone();
        let group = group.to_string();
        tokio::task::spawn_blocking(move || router.read_barrier(&group))
            .await
            .map_err(|e| anyhow!("read barrier task failed: {e}"))?
    }

    /// The committed `(key, value, version)` multiset of one locally hosted
    /// group at `ts`, restricted to the migrating table's row keys. The meta
    /// group is unnamespaced — it holds rows of tables that have no shard map
    /// yet, and secondary-index entries permanently — while data groups are
    /// namespaced views of the same store.
    fn multiset(&self, group: &str, table: TableId, ts: u64) -> Result<Vec<(Bytes, Bytes, u64)>> {
        let rows = if group == META_SHARD {
            self.manager
                .base_kv()
                .scan(user_range(), ts)?
                .collect::<Result<Vec<KvPair>>>()?
        } else {
            NamespacedKvEngine::new(self.manager.base_kv(), group)
                .scan(user_range(), ts)?
                .collect::<Result<Vec<KvPair>>>()?
        };
        Ok(rows
            .into_iter()
            .filter(|p| is_table_key(table, &p.key))
            .map(|p| (p.key, p.value, p.version))
            .collect())
    }
}

impl BackendV3 for ProductionBackendV3 {
    async fn load(&self, table: TableId) -> Result<RecordV3> {
        let raft = self
            .manager
            .get(META_SHARD)
            .await
            .ok_or_else(|| anyhow!("meta group unavailable"))?;
        // Only the current meta leader drives; the journal is reloaded behind
        // a ReadIndex barrier so a resumed driver never acts on stale state.
        raft.ensure_linearizable().await?;
        let machine = self.manager.machine(META_SHARD).await?;
        let kv = machine.read().await.kv.clone();
        let kv = kv.ok_or_else(|| anyhow!("meta storage unavailable"))?;
        v3::read_record_v3(kv.as_ref(), table)?.ok_or_else(|| anyhow!("migration journal absent"))
    }

    async fn submit_journal(&self, command: CommandV3) -> Result<()> {
        self.submit(META_SHARD, ShardCommand::MigrationV3(command))
            .await
    }

    async fn submit_conditional(&self, command: ShardCommand) -> Result<()> {
        self.submit(META_SHARD, command).await
    }

    async fn routing_state(&self, plan: &PlanV3) -> Result<RoutingState> {
        let machine = self.manager.machine(META_SHARD).await?;
        let meta = machine.read().await.meta_store.clone();
        let meta = meta.ok_or_else(|| anyhow!("meta store unavailable"))?;
        let table = plan.migration.table_id;
        let map_destination = v3::map_matches(&plan.destination_map, meta.as_ref(), table)?;
        Ok(RoutingState {
            map_matches_expected: v3::map_matches(&plan.expected_map, meta.as_ref(), table)?,
            placements_match_expected: v3::placements_match(
                &plan.expected_placements,
                meta.as_ref(),
            )?,
            placements_prepared: v3::placements_match(
                &v3::prepare_placements(plan),
                meta.as_ref(),
            )?,
            map_matches_destination: map_destination,
            published: map_destination
                && v3::placements_match(&plan.destination_placements, meta.as_ref())?,
        })
    }

    async fn fence_source(&self, source: &str, operation: Uuid, expected_epoch: u64) -> Result<()> {
        self.submit(
            source,
            ShardCommand::MigrationV1(migration::MigrationCommandV1::Acquire {
                operation_id: operation,
                expected_epoch,
            }),
        )
        .await
    }

    async fn seal_source(&self, source: &str, operation: Uuid, expected_epoch: u64) -> Result<()> {
        self.submit(
            source,
            ShardCommand::MigrationV3(CommandV3::CancelParticipant {
                operation,
                expected_epoch,
            }),
        )
        .await
    }

    async fn reopen_source(&self, source: &str, operation: Uuid, epoch: u64) -> Result<()> {
        self.submit(
            source,
            ShardCommand::MigrationV1(migration::MigrationCommandV1::Release {
                operation_id: operation,
                epoch,
            }),
        )
        .await
    }

    async fn prepare_destination(&self, plan: &PlanV3, destination: &str) -> Result<()> {
        let shard = plan
            .destination_map
            .as_ref()
            .and_then(|map| {
                map.shards
                    .iter()
                    .find(|s| v3::destination_group(s.id) == destination)
            })
            .ok_or_else(|| anyhow!("destination {destination} is not in the destination map"))?;
        let placed = plan
            .destination_placements
            .get(&shard.id)
            .ok_or_else(|| anyhow!("destination {destination} has no placement"))?;
        let node = self.manager.node_id().to_string();
        if *placed == node {
            // Form the group replicated across the current roster; idempotent
            // and convergent, so a re-driven prepare never double-applies.
            self.manager.form_replicated(destination).await?;
            // Serving means the group has a leader (this node or a peer).
            let raft = self
                .manager
                .get(destination)
                .await
                .ok_or_else(|| anyhow!("destination {destination} did not form"))?;
            let mut metrics = raft.metrics();
            let served = tokio::time::timeout(STEP_DEADLINE, async {
                loop {
                    if metrics.borrow().current_leader.is_some() {
                        return;
                    }
                    if metrics.changed().await.is_err() {
                        return;
                    }
                }
            })
            .await;
            ensure!(
                served.is_ok() && metrics.borrow().current_leader.is_some(),
                "destination {destination} has no leader yet"
            );
            Ok(())
        } else if self.manager.hosts(destination) {
            // Placed elsewhere but already roster-replicated here (the group
            // was formed while this node was a member): serving means any
            // leader, verified locally. This is the path a resumed driver
            // takes after the placement owner died mid-operation.
            let raft = self
                .manager
                .get(destination)
                .await
                .ok_or_else(|| anyhow!("destination {destination} did not form"))?;
            let mut metrics = raft.metrics();
            let served = tokio::time::timeout(STEP_DEADLINE, async {
                loop {
                    if metrics.borrow().current_leader.is_some() {
                        return;
                    }
                    if metrics.changed().await.is_err() {
                        return;
                    }
                }
            })
            .await;
            ensure!(
                served.is_ok() && metrics.borrow().current_leader.is_some(),
                "destination {destination} has no leader yet"
            );
            Ok(())
        } else {
            // Not hosted here: the placement owner's reconcile loop forms it.
            // Until it serves there, this step fails and is retried next tick.
            let machine = self.manager.machine(META_SHARD).await?;
            let members = nodus_raftstore::upgrade::members(&machine.read().await.last_membership)?;
            let placed_id: u64 = placed
                .parse()
                .map_err(|_| anyhow!("invalid placement node id {placed}"))?;
            let addr = members
                .get(&placed_id)
                .ok_or_else(|| anyhow!("placement node {placed} is not in the cluster roster"))?;
            self.manager
                .transport()
                .group_read_index(addr, destination)
                .await?;
            Ok(())
        }
    }

    async fn cleanup_destination(&self, destination: &str) -> Result<()> {
        self.manager.remove_group(destination).await
    }

    async fn choose_protected_ts(&self, record: &RecordV3) -> Result<u64> {
        // Above every source's visible committed timestamp, without opening a
        // transaction (which would pin the GC watermark via its read_ts). The
        // meta source is included: rows of a not-yet-mapped table live there.
        let table = record.plan.migration.table_id;
        let mut max_version = 0;
        for source in &record.plan.migration.sources {
            self.barrier(source).await?;
            for row in self.multiset(source, table, u64::MAX)? {
                max_version = max_version.max(row.2);
            }
        }
        let ts = max_version + 1;
        // Reserve the destination clock above the sources' committed versions.
        self.manager.clock().observe_durable(ts)?;
        Ok(ts)
    }

    async fn copy_batch(&self, record: &RecordV3, source: &str) -> Result<CheckpointV3> {
        let table = record.plan.migration.table_id;
        let ts = record.protected_read_ts;
        // The fence quiesced the source, but this node's replica may still lag;
        // barrier before reading so the copy never silently misses rows.
        self.barrier(source).await?;
        let cursor = record
            .checkpoints
            .get(source)
            .map(|c| c.cursor.clone())
            .unwrap_or_default();
        let start = if cursor.is_empty() {
            Bytes::from_static(&[1])
        } else {
            Bytes::from([cursor.as_slice(), &[0]].concat())
        };
        let range = KeyRange {
            start,
            end: Bytes::from(vec![255u8; 64]),
        };
        // The meta group is unnamespaced; data groups are namespaced views.
        // Either way only the table's row keys move — secondary-index entries
        // stay on meta permanently (routing never moves them).
        let pairs: Vec<KvPair> = if source == META_SHARD {
            self.manager
                .base_kv()
                .scan(range, ts)?
                .collect::<Result<Vec<_>>>()?
        } else {
            NamespacedKvEngine::new(self.manager.base_kv(), source)
                .scan(range, ts)?
                .collect::<Result<Vec<_>>>()?
        };
        let mut batch: Vec<KvPair> = Vec::new();
        let mut more = false;
        for row in pairs {
            if !is_table_key(table, &row.key) {
                continue;
            }
            if batch.len() == COPY_BATCH_ROWS {
                more = true;
                break;
            }
            batch.push(row);
        }
        let destination_map = record
            .plan
            .destination_map
            .as_ref()
            .ok_or_else(|| anyhow!("copy requires a destination map"))?;
        let checkpoint_cursor = batch.last().map(|row| row.key.to_vec()).unwrap_or(cursor);
        let mut copies = tokio::task::JoinSet::<Result<()>>::new();
        for row in batch {
            while copies.len() >= COPY_CONCURRENCY {
                let _ = copies
                    .join_next()
                    .await
                    .context("migration copy task failed")??;
            }
            let shard = destination_map
                .shards
                .iter()
                .find(|s| {
                    (s.start_key.is_empty() || row.key.as_ref() >= s.start_key.as_slice())
                        && (s.end_key.is_empty() || row.key.as_ref() < s.end_key.as_slice())
                })
                .ok_or_else(|| anyhow!("destination map does not cover a copied key"))?;
            let group = v3::destination_group(shard.id);
            if self.manager.get(&group).await.is_none() {
                // A restarted (or newly elected) driver reloads the durable
                // replica; formation itself is prepare_destination's job.
                self.manager.get_or_create_data(&group).await?;
            }
            let backend = self.clone();
            let operation = record.plan.migration.operation_id;
            copies.spawn(async move {
                // Epoch discipline on the destination: fresh groups sit at epoch 0.
                // The deterministic txn id makes a re-driven batch idempotent.
                let txn_id = Uuid::new_v5(
                    &Uuid::NAMESPACE_OID,
                    &[operation.as_bytes(), row.key.as_ref()].concat(),
                );
                for mutation in [
                    migration::EpochMutationV1::Put {
                        txn_id,
                        key: row.key.to_vec(),
                        value: row.value.to_vec(),
                    },
                    migration::EpochMutationV1::Commit {
                        txn_id,
                        commit_ts: row.version,
                    },
                ] {
                    backend
                        .submit(&group, ShardCommand::EpochWriteV1 { epoch: 0, mutation })
                        .await?;
                }
                Ok(())
            });
        }
        while let Some(result) = copies.join_next().await {
            result??;
        }
        Ok(CheckpointV3 {
            cursor: checkpoint_cursor,
            complete: !more,
        })
    }

    async fn validate(&self, record: &RecordV3) -> Result<ValidationV3> {
        let table = record.plan.migration.table_id;
        let ts = record.protected_read_ts;
        let collect = |groups: &[String]| -> Result<Vec<(Bytes, Bytes, u64)>> {
            let mut entries = Vec::new();
            for group in groups {
                entries.extend(self.multiset(group, table, ts)?);
            }
            Ok(entries)
        };
        for group in record
            .plan
            .migration
            .sources
            .iter()
            .chain(&record.plan.migration.destinations)
        {
            self.barrier(group).await?;
        }
        let source_digest = digest(&collect(&record.plan.migration.sources)?);
        let destination_digest = digest(&collect(&record.plan.migration.destinations)?);
        Ok(ValidationV3 {
            matches: source_digest == destination_digest,
            source_digest,
            destination_digest,
        })
    }

    async fn source_hosted(&self, source: &str) -> Result<bool> {
        Ok(self.manager.hosts(source))
    }

    async fn decommission_source(&self, source: &str) -> Result<()> {
        self.manager.remove_group(source).await
    }
}

fn user_range() -> KeyRange {
    KeyRange {
        start: Bytes::from_static(&[1]),
        end: Bytes::from(vec![255u8; 64]),
    }
}

/// Row keys of the migrating table only: never control (`\x01…`) records, and
/// never another table's rows sharing a namespace.
fn is_table_key(table: TableId, key: &[u8]) -> bool {
    !key.starts_with(b"\x01") && crate::raft_kv::parse_table_id(key) == Some(table)
}

/// Deterministic FNV-1a digest over the sorted key/version/value multiset.
fn digest(entries: &[(Bytes, Bytes, u64)]) -> u64 {
    let mut sorted = entries.to_vec();
    sorted.sort();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut mix = |bytes: &[u8]| {
        for b in bytes {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    mix(&(sorted.len() as u64).to_be_bytes());
    for (key, value, version) in &sorted {
        mix(&(key.len() as u64).to_be_bytes());
        mix(key);
        mix(&version.to_be_bytes());
        mix(&(value.len() as u64).to_be_bytes());
        mix(value);
    }
    hash
}
