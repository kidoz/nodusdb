//! Driver-level tests for the V3 copy/cutover spine: a fixture `BackendV3`
//! over real in-process Raft groups moves committed data between groups,
//! journaled through the meta group's V3 coordinator.
use super::*;
use crate::{
    multi_raft::{META_SHARD, MultiRaftManager},
    raft_kv::RaftKvEngine,
    raft_router::RaftRouter,
};
use nodus_meta::{MetaStore, ShardMap};
use nodus_raftstore::migration::coordinator::v3::{
    self, CheckpointV3, CommandV3 as C3, PhaseV3 as P3, PlanV3, RecordV3, ValidationV3,
    driver::{self, BackendV3, RoutingState},
};
use nodus_sharding::ShardOrchestrator;
use nodus_storage_api::{KvPair, NamespacedKvEngine};
use nodus_txn::TxnManager;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

/// Small batches so a multi-key copy crosses several journaled checkpoints.
const BATCH: usize = 4;

pub(super) struct DriverFixture {
    pub(super) manager: Arc<MultiRaftManager>,
    pub(super) router: RaftRouter,
    pub(super) engine: Arc<RaftKvEngine>,
    pub(super) meta: Arc<dyn MetaStore>,
    pub(super) clock: Arc<nodus_txn::MemTxnManager>,
    pub(super) base: Arc<dyn KvEngine>,
}

impl DriverFixture {
    pub(super) async fn new(base: Arc<dyn KvEngine>, dir: &std::path::Path) -> Self {
        let clock = Arc::new(nodus_txn::MemTxnManager::new());
        let manager = Arc::new(MultiRaftManager::new(
            1,
            "127.0.0.1:0".into(),
            Arc::new(openraft::Config::default().validate().unwrap()),
            nodus_raftstore::server::RaftState::new(),
            base.clone(),
            None,
            clock.clone(),
            Some(dir.to_path_buf()),
            Default::default(),
        ));
        let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
        let meta: Arc<dyn MetaStore> = Arc::new(nodus_meta::PersistentMetaStore::new(base.clone()));
        let raft = manager
            .create_meta(base.clone(), catalog.clone(), catalog.clone(), meta.clone())
            .await
            .unwrap();
        let _ = raft
            .initialize(BTreeMap::from([(
                1u64,
                openraft::BasicNode::new("127.0.0.1:0"),
            )]))
            .await;
        raft.wait(Some(Duration::from_secs(10)))
            .current_leader(1, "meta election")
            .await
            .unwrap();
        let mut router = RaftRouter::spawn(manager.clone());
        router.enable_migration_for_test();
        let engine = Arc::new(RaftKvEngine {
            local: base.clone(),
            router: router.clone(),
            shard_router: Arc::new(nodus_sharding::CatalogShardRouter::new(meta.clone())),
            manager: manager.clone(),
            txn_groups: std::sync::Mutex::new(HashMap::new()),
            metrics: Default::default(),
        });
        Self {
            manager,
            router,
            engine,
            meta,
            clock,
            base,
        }
    }

    /// Hosts (or recovers) a data group led by this node.
    pub(super) async fn host_group(&self, group: &str) {
        let raft = self.manager.get_or_create_data(group).await.unwrap();
        let _ = raft
            .initialize(BTreeMap::from([(
                1u64,
                openraft::BasicNode::new("127.0.0.1:0"),
            )]))
            .await;
        raft.wait(Some(Duration::from_secs(10)))
            .current_leader(1, "data group election")
            .await
            .unwrap();
    }

    pub(super) async fn write_group(&self, group: &str, cmd: ShardCommand) {
        if self.manager.get(group).await.is_none() {
            self.host_group(group).await;
        }
        let raft = self.manager.get(group).await.unwrap();
        let response = raft.client_write(cmd).await.unwrap().data;
        assert!(response.success, "{}", response.error.unwrap_or_default());
    }

    /// Seeds committed rows (`{table}:{pk}`) into a group through its Raft.
    pub(super) async fn seed(
        &self,
        group: &str,
        table: TableId,
        rows: &[(String, String)],
        base_ts: u64,
    ) {
        self.host_group(group).await;
        for (i, (pk, value)) in rows.iter().enumerate() {
            let txn = Uuid::new_v4().to_string();
            let key = format!("{table}:{pk}").into_bytes();
            self.write_group(
                group,
                ShardCommand::PutIntent {
                    txn_id: txn.clone(),
                    key,
                    value: value.clone().into_bytes(),
                    shard_id: Some(group.to_string()),
                },
            )
            .await;
            self.write_group(
                group,
                ShardCommand::CommitTxn {
                    txn_id: txn,
                    commit_ts: base_ts + i as u64,
                    shard_id: Some(group.to_string()),
                },
            )
            .await;
        }
    }

    pub(super) async fn command(&self, cmd: C3) -> ShardResponse {
        self.manager
            .get(META_SHARD)
            .await
            .unwrap()
            .client_write(ShardCommand::MigrationV3(cmd))
            .await
            .unwrap()
            .data
    }

    pub(super) async fn journal(&self, table: TableId) -> RecordV3 {
        v3::read_record_v3(self.base.as_ref(), table)
            .unwrap()
            .unwrap()
    }

    /// The committed `(key, value, version)` multiset of one group at `ts`.
    pub(super) fn multiset(
        &self,
        group: &str,
        table: TableId,
        ts: u64,
    ) -> Vec<(Bytes, Bytes, u64)> {
        let ns = NamespacedKvEngine::new(self.base.clone(), group);
        ns.scan(user_range(), ts)
            .unwrap()
            .map(|r| r.unwrap())
            .filter(|p| is_table_key(table, &p.key))
            .map(|p| (p.key, p.value, p.version))
            .collect()
    }

    pub(super) async fn drive(&self, table: TableId) -> P3 {
        driver::run(self, table, Duration::from_secs(5), 400)
            .await
            .unwrap()
    }
}

pub(super) fn user_range() -> nodus_storage_api::KeyRange {
    nodus_storage_api::KeyRange {
        start: Bytes::from_static(&[1]),
        end: Bytes::from(vec![255u8; 64]),
    }
}

/// Row keys of the migrating table only: never control (`\x01…`) records, and
/// never another table's rows sharing a namespace.
pub(super) fn is_table_key(table: TableId, key: &[u8]) -> bool {
    !key.starts_with(b"\x01") && crate::raft_kv::parse_table_id(key) == Some(table)
}

/// Deterministic FNV-1a digest over the sorted key/version/value multiset.
pub(super) fn digest(entries: &[(Bytes, Bytes, u64)]) -> u64 {
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

impl BackendV3 for DriverFixture {
    async fn load(&self, table: TableId) -> anyhow::Result<RecordV3> {
        let raft = self
            .manager
            .get(META_SHARD)
            .await
            .ok_or_else(|| anyhow::anyhow!("meta group unavailable"))?;
        // Only the current meta leader drives; the journal is reloaded behind
        // a ReadIndex barrier so a resumed driver never acts on stale state.
        raft.ensure_linearizable().await?;
        let machine = self.manager.machine(META_SHARD).await?;
        let kv = machine.read().await.kv.clone();
        let kv = kv.ok_or_else(|| anyhow::anyhow!("meta storage unavailable"))?;
        v3::read_record_v3(kv.as_ref(), table)?
            .ok_or_else(|| anyhow::anyhow!("migration journal absent"))
    }

    async fn submit_journal(&self, command: C3) -> anyhow::Result<()> {
        let response = self.command(command).await;
        anyhow::ensure!(
            response.success,
            "{}",
            response
                .error
                .unwrap_or_else(|| "journal command rejected".into())
        );
        Ok(())
    }

    async fn submit_conditional(&self, command: ShardCommand) -> anyhow::Result<()> {
        let response = self
            .manager
            .get(META_SHARD)
            .await
            .unwrap()
            .client_write(command)
            .await
            .unwrap()
            .data;
        anyhow::ensure!(
            response.success,
            "{}",
            response
                .error
                .unwrap_or_else(|| "conditional publication rejected".into())
        );
        Ok(())
    }

    async fn routing_state(&self, plan: &PlanV3) -> anyhow::Result<RoutingState> {
        let machine = self.manager.machine(META_SHARD).await?;
        let meta = machine.read().await.meta_store.clone();
        let meta = meta.ok_or_else(|| anyhow::anyhow!("meta store unavailable"))?;
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

    async fn fence_source(
        &self,
        source: &str,
        operation: Uuid,
        expected_epoch: u64,
    ) -> anyhow::Result<()> {
        if source != META_SHARD {
            self.host_group(source).await;
        }
        let raft = self.manager.get(source).await.unwrap();
        let response = raft
            .client_write(ShardCommand::MigrationV1(Control::Acquire {
                operation_id: operation,
                expected_epoch,
            }))
            .await?
            .data;
        anyhow::ensure!(response.success, response.error.unwrap_or_default());
        Ok(())
    }

    async fn seal_source(
        &self,
        source: &str,
        operation: Uuid,
        expected_epoch: u64,
    ) -> anyhow::Result<()> {
        if source != META_SHARD {
            self.host_group(source).await;
        }
        let raft = self.manager.get(source).await.unwrap();
        let response = raft
            .client_write(ShardCommand::MigrationV3(C3::CancelParticipant {
                operation,
                expected_epoch,
            }))
            .await?
            .data;
        anyhow::ensure!(response.success, response.error.unwrap_or_default());
        Ok(())
    }

    async fn reopen_source(&self, source: &str, operation: Uuid, epoch: u64) -> anyhow::Result<()> {
        if source != META_SHARD {
            self.host_group(source).await;
        }
        let raft = self.manager.get(source).await.unwrap();
        let response = raft
            .client_write(ShardCommand::MigrationV1(Control::Release {
                operation_id: operation,
                epoch,
            }))
            .await?
            .data;
        anyhow::ensure!(response.success, response.error.unwrap_or_default());
        Ok(())
    }

    async fn prepare_destination(&self, _plan: &PlanV3, destination: &str) -> anyhow::Result<()> {
        self.host_group(destination).await;
        // A durable, serving replica set: this node's replica leads and its
        // log is applied.
        let raft = self.manager.get(destination).await.unwrap();
        anyhow::ensure!(
            raft.metrics().borrow().current_leader == Some(1),
            "destination {destination} has no leader"
        );
        Ok(())
    }

    async fn cleanup_destination(&self, destination: &str) -> anyhow::Result<()> {
        self.manager.remove_group(destination).await
    }

    async fn choose_protected_ts(&self, record: &RecordV3) -> anyhow::Result<u64> {
        // Above every source's visible committed timestamp, without opening a
        // transaction (which would pin the GC watermark via its read_ts).
        let table = record.plan.migration.table_id;
        let mut max_version = 0;
        for source in &record.plan.migration.sources {
            for row in self.multiset(source, table, u64::MAX) {
                max_version = max_version.max(row.2);
            }
        }
        let ts = max_version + 1;
        // Reserve the destination clock above the sources' committed versions.
        self.clock.observe_durable(ts)?;
        Ok(ts)
    }

    async fn copy_batch(&self, record: &RecordV3, source: &str) -> anyhow::Result<CheckpointV3> {
        let table = record.plan.migration.table_id;
        let ts = record.protected_read_ts;
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
        let range = nodus_storage_api::KeyRange {
            start,
            end: Bytes::from(vec![255u8; 64]),
        };
        // Meta is unnamespaced; only the table's row keys move — index
        // entries stay on meta permanently (routing never moves them).
        let kv: Arc<dyn KvEngine> = if source == META_SHARD {
            self.base.clone()
        } else {
            Arc::new(NamespacedKvEngine::new(self.base.clone(), source))
        };
        let mut batch: Vec<KvPair> = Vec::new();
        let mut more = false;
        for row in kv.scan(range, ts)? {
            let row = row?;
            if !is_table_key(table, &row.key) {
                continue;
            }
            if batch.len() == BATCH {
                more = true;
                break;
            }
            batch.push(row);
        }
        let destination_map = record
            .plan
            .destination_map
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("copy requires a destination map"))?;
        for row in &batch {
            let shard = destination_map
                .shards
                .iter()
                .find(|s| {
                    (s.start_key.is_empty() || row.key.as_ref() >= s.start_key.as_slice())
                        && (s.end_key.is_empty() || row.key.as_ref() < s.end_key.as_slice())
                })
                .ok_or_else(|| anyhow::anyhow!("destination map does not cover a copied key"))?;
            let group = v3::destination_group(shard.id);
            // Epoch discipline on the destination: fresh groups sit at epoch 0.
            // The deterministic txn id makes a re-driven batch idempotent.
            let txn_id = Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                &[
                    record.plan.migration.operation_id.as_bytes(),
                    row.key.as_ref(),
                ]
                .concat(),
            );
            let mutation = |mutation| ShardCommand::EpochWriteV1 { epoch: 0, mutation };
            // A reopened driver (post-crash) must re-host the destination.
            if self.manager.get(&group).await.is_none() {
                self.host_group(&group).await;
            }
            let raft = self.manager.get(&group).await.unwrap();
            for cmd in [
                mutation(Mutation::Put {
                    txn_id,
                    key: row.key.to_vec(),
                    value: row.value.to_vec(),
                }),
                mutation(Mutation::Commit {
                    txn_id,
                    commit_ts: row.version,
                }),
            ] {
                let response = raft.client_write(cmd).await?.data;
                anyhow::ensure!(response.success, response.error.unwrap_or_default());
            }
        }
        Ok(CheckpointV3 {
            cursor: batch.last().map(|r| r.key.to_vec()).unwrap_or(cursor),
            complete: !more,
        })
    }

    async fn validate(&self, record: &RecordV3) -> anyhow::Result<ValidationV3> {
        let table = record.plan.migration.table_id;
        let ts = record.protected_read_ts;
        let collect = |groups: &[String]| {
            let mut entries = Vec::new();
            for group in groups {
                entries.extend(self.multiset(group, table, ts));
            }
            entries
        };
        let source_digest = digest(&collect(&record.plan.migration.sources));
        let destination_digest = digest(&collect(&record.plan.migration.destinations));
        Ok(ValidationV3 {
            matches: source_digest == destination_digest,
            source_digest,
            destination_digest,
        })
    }

    async fn source_hosted(&self, source: &str) -> anyhow::Result<bool> {
        Ok(self.manager.hosts(source))
    }

    async fn decommission_source(&self, source: &str) -> anyhow::Result<()> {
        self.manager.remove_group(source).await
    }
}

pub(super) fn split_plan(
    fx: &DriverFixture,
    table: TableId,
    source: nodus_catalog::ShardId,
) -> PlanV3 {
    let orchestrator = ShardOrchestrator::new(fx.meta.clone());
    let plan = orchestrator
        .plan_split(table, source, format!("{table}:m").into_bytes())
        .unwrap();
    let source_group = v3::destination_group(source);
    PlanV3 {
        migration: MigrationPlan {
            operation_id: Uuid::new_v4(),
            table_id: table,
            source_epochs: BTreeMap::from([(META_SHARD.to_string(), 0), (source_group.clone(), 0)]),
            sources: vec![META_SHARD.to_string(), source_group],
            destinations: vec![
                v3::destination_group(plan.left.id),
                v3::destination_group(plan.right.id),
            ],
        },
        expected_map: Some(fx.meta.get_shard_map(table).unwrap()),
        expected_placements: fx.meta.get_shard_placements().unwrap(),
        destination_map: Some(plan.new_map),
        destination_placements: HashMap::from([
            (plan.left.id, "1".to_string()),
            (plan.right.id, "1".to_string()),
        ]),
    }
}

pub(super) async fn new_mem_fixture() -> (tempfile::TempDir, DriverFixture) {
    let dir = tempfile::tempdir().unwrap();
    let base: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
    let fx = DriverFixture::new(base, dir.path()).await;
    (dir, fx)
}

/// Asserts the terminal state of a successful split/merge: exact multiset at
/// the destination, flipped routing, reopened writers, sources gone.
pub(super) async fn assert_completed(
    fx: &DriverFixture,
    table: TableId,
    plan: &PlanV3,
    expected_rows: &[(Bytes, Bytes, u64)],
) {
    let journal = fx.journal(table).await;
    assert_eq!(journal.phase, P3::Done);
    assert!(journal.protected_read_ts > 0);
    // Exact key/version multiset at the destinations.
    let mut copied = Vec::new();
    for destination in &plan.migration.destinations {
        copied.extend(fx.multiset(destination, table, u64::MAX));
    }
    copied.sort();
    let mut expected = expected_rows.to_vec();
    expected.sort();
    assert_eq!(copied, expected, "exact multiset must move");
    // Routing equals the journaled destination content.
    let map = fx.meta.get_shard_map(table).unwrap();
    assert_eq!(
        serde_json::to_value(&map).unwrap(),
        serde_json::to_value(plan.destination_map.clone().unwrap()).unwrap()
    );
    assert_eq!(
        fx.meta.get_shard_placements().unwrap(),
        plan.destination_placements
    );
    // Source fences reopened at the operation's epoch; row sources are gone.
    for source in &plan.migration.sources {
        let ns = if source == META_SHARD {
            fx.base.clone()
        } else {
            Arc::new(NamespacedKvEngine::new(fx.base.clone(), source)) as Arc<dyn KvEngine>
        };
        let fence = read_fence(ns.as_ref()).unwrap().unwrap();
        assert!(!fence.closed, "source {source} reopened");
        assert_eq!(fence.epoch, 1);
        assert_eq!(fence.operation_id, plan.migration.operation_id);
        if source != META_SHARD {
            assert!(!fx.manager.hosts(source), "source {source} decommissioned");
        }
    }
    // Writers succeed against the new map.
    let engine = fx.engine.clone();
    let (key, value) = (format!("{table}:post").into_bytes(), b"after".to_vec());
    tokio::task::spawn_blocking(move || {
        let txn = TxnId::new();
        engine
            .write_intent(txn, Bytes::from(key.clone()), Bytes::from(value.clone()))
            .unwrap();
        engine.commit(txn, 1_000_000).unwrap();
        let mut found = false;
        for pair in engine
            .scan(
                nodus_storage_api::KeyRange {
                    start: Bytes::from(key.clone()),
                    end: Bytes::from([key.as_slice(), &[0]].concat()),
                },
                2_000_000,
            )
            .unwrap()
        {
            let pair = pair.unwrap();
            assert_eq!(pair.key.as_ref(), key.as_slice());
            assert_eq!(pair.value.as_ref(), value.as_slice());
            found = true;
        }
        assert!(found, "post-cutover write readable through the new map");
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_driver_split_moves_the_exact_multiset() {
    let (_dir, fx) = new_mem_fixture().await;
    let table = TableId::new();
    let orchestrator = ShardOrchestrator::new(fx.meta.clone());
    let source = orchestrator.init_single_shard(table).unwrap();
    orchestrator.move_shard(source, "1").unwrap();
    let source_group = v3::destination_group(source);
    let rows: Vec<(String, String)> = (0..10)
        .map(|i| (format!("k{i:02}"), format!("v{i}")))
        .collect();
    fx.seed(&source_group, table, &rows, 100).await;
    let before = fx.multiset(&source_group, table, u64::MAX);

    let plan = split_plan(&fx, table, source);
    assert!(fx.command(C3::Plan(plan.clone())).await.success);

    // Drive to Copying: writers against the fenced source are rejected.
    let mut phase = P3::Planned;
    for _ in 0..100 {
        phase = driver::resume(&fx, table, Duration::from_secs(5))
            .await
            .unwrap();
        if phase == P3::Copying {
            break;
        }
    }
    assert_eq!(phase, P3::Copying);
    let engine = fx.engine.clone();
    let blocked = tokio::task::spawn_blocking(move || {
        engine.write_intent(
            TxnId::new(),
            Bytes::from(format!("{table}:blocked")),
            Bytes::from_static(b"nope"),
        )
    })
    .await
    .unwrap();
    let error = blocked.unwrap_err().to_string();
    assert!(
        error.contains("fenced") || error.contains("epoch"),
        "writes to a fenced source must fail with an epoch error: {error}"
    );

    assert_eq!(fx.drive(table).await, P3::Done);
    assert_completed(&fx, table, &plan, &before).await;
    fx.manager.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_driver_merge_moves_the_exact_multiset() {
    let (_dir, fx) = new_mem_fixture().await;
    let table = TableId::new();
    let orchestrator = ShardOrchestrator::new(fx.meta.clone());
    let first = orchestrator.init_single_shard(table).unwrap();
    orchestrator.move_shard(first, "1").unwrap();
    let (left, right) = orchestrator
        .split(table, first, format!("{table}:m").into_bytes())
        .unwrap();
    let left_group = v3::destination_group(left);
    let right_group = v3::destination_group(right);
    let left_rows: Vec<(String, String)> =
        (0..5).map(|i| (format!("a{i}"), format!("l{i}"))).collect();
    let right_rows: Vec<(String, String)> =
        (0..5).map(|i| (format!("z{i}"), format!("r{i}"))).collect();
    fx.seed(&left_group, table, &left_rows, 100).await;
    fx.seed(&right_group, table, &right_rows, 200).await;
    let mut before = fx.multiset(&left_group, table, u64::MAX);
    before.extend(fx.multiset(&right_group, table, u64::MAX));

    let merge = orchestrator.plan_merge(table, left, right).unwrap();
    let plan = PlanV3 {
        migration: MigrationPlan {
            operation_id: Uuid::new_v4(),
            table_id: table,
            source_epochs: BTreeMap::from([
                (META_SHARD.to_string(), 0),
                (left_group.clone(), 0),
                (right_group.clone(), 0),
            ]),
            sources: vec![META_SHARD.to_string(), left_group, right_group],
            destinations: vec![v3::destination_group(merge.merged.id)],
        },
        expected_map: Some(fx.meta.get_shard_map(table).unwrap()),
        expected_placements: fx.meta.get_shard_placements().unwrap(),
        destination_map: Some(merge.new_map),
        destination_placements: HashMap::from([(merge.merged.id, "1".to_string())]),
    };
    assert!(fx.command(C3::Plan(plan.clone())).await.success);
    if std::env::var("V3_DEBUG").is_ok() {
        for _ in 0..60 {
            let phase = driver::resume(&fx, table, Duration::from_secs(5))
                .await
                .unwrap();
            let j = fx.journal(table).await;
            eprintln!(
                "phase={phase:?} prepared={:?} checkpoints={:?} validation={:?} completed={:?}",
                j.prepared, j.checkpoints, j.validation, j.completed
            );
            if matches!(phase, P3::Done | P3::Cancelled) {
                break;
            }
        }
        fx.manager.shutdown_all().await;
        return;
    }
    assert_eq!(fx.drive(table).await, P3::Done);
    assert_completed(&fx, table, &plan, &before).await;
    fx.manager.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_driver_init_publishes_the_first_shard_without_a_copy() {
    let (_dir, fx) = new_mem_fixture().await;
    let table = TableId::new();
    let now = chrono::Utc::now();
    let shard = nodus_catalog::ShardDescriptor {
        id: nodus_catalog::ShardId::new(),
        name: "shard-0".into(),
        version: 1,
        created_at: now,
        updated_at: now,
        state: nodus_catalog::DescriptorState::Public,
        table_id: table,
        start_key: vec![],
        end_key: vec![],
    };
    let destination = ShardMap {
        table_id: table,
        shards: vec![shard.clone()],
    };
    let plan = PlanV3 {
        migration: MigrationPlan {
            operation_id: Uuid::new_v4(),
            table_id: table,
            source_epochs: BTreeMap::from([(META_SHARD.to_string(), 0)]),
            sources: vec![META_SHARD.to_string()],
            destinations: vec![v3::destination_group(shard.id)],
        },
        expected_map: None,
        expected_placements: HashMap::new(),
        destination_map: Some(destination),
        destination_placements: HashMap::from([(shard.id, "1".to_string())]),
    };
    assert!(fx.command(C3::Plan(plan.clone())).await.success);
    assert_eq!(fx.drive(table).await, P3::Done);
    // The vacuous copy left no data; routing now serves the new group.
    assert_completed(&fx, table, &plan, &[]).await;
    fx.manager.shutdown_all().await;
}

/// Subprocess crash between two copy checkpoints: the reopened fixture resumes
/// from the journaled checkpoint and completes with the exact multiset intact.
#[test]
fn v3_driver_crash_between_checkpoints_resumes_from_the_journal() {
    const CHILD_DIR: &str = "NODUS_V3_DRIVER_CRASH_DIR";
    let table = TableId(Uuid::from_u128(977));
    if let Some(dir) = std::env::var_os(CHILD_DIR) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = std::path::PathBuf::from(dir);
            let base: Arc<dyn KvEngine> =
                Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(dir.join("kv"), None).unwrap());
            let fx = DriverFixture::new(base.clone(), &dir).await;
            let orchestrator = ShardOrchestrator::new(fx.meta.clone());
            let source = orchestrator.init_single_shard(table).unwrap();
            orchestrator.move_shard(source, "1").unwrap();
            let source_group = v3::destination_group(source);
            let rows: Vec<(String, String)> = (0..10)
                .map(|i| (format!("k{i:02}"), format!("v{i}")))
                .collect();
            fx.seed(&source_group, table, &rows, 100).await;
            let plan = split_plan(&fx, table, source);
            assert!(fx.command(C3::Plan(plan)).await.success);
            // Stop with one journaled checkpoint but the copy incomplete.
            loop {
                let phase = driver::resume(&fx, table, Duration::from_secs(5))
                    .await
                    .unwrap();
                let journal = fx.journal(table).await;
                let mid_copy = journal
                    .checkpoints
                    .get(&source_group)
                    .is_some_and(|c| !c.complete && !c.cursor.is_empty());
                if phase == P3::Copying && mid_copy {
                    break;
                }
                assert!(
                    !matches!(phase, P3::Done | P3::Cancelled),
                    "copy finished before the crash point: {phase:?}"
                );
            }
            // Exit before shutdown: no Drop, no Raft stop.
            std::process::exit(0);
        });
        unreachable!();
    }
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "migration_tests::driver::v3_driver_crash_between_checkpoints_resumes_from_the_journal",
            "--nocapture",
        ])
        .env(CHILD_DIR, dir.path())
        .status()
        .unwrap();
    assert!(status.success());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let base: Arc<dyn KvEngine> = Arc::new(
            nodus_storage_lsm::LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap(),
        );
        // The journaled mid-copy state survived the abrupt exit.
        let journal = v3::read_record_v3(base.as_ref(), table).unwrap().unwrap();
        assert_eq!(journal.phase, P3::Copying);
        let checkpoint = journal.checkpoints.values().next().unwrap();
        assert!(!checkpoint.complete && !checkpoint.cursor.is_empty());
        let source = journal.plan.migration.sources[1].clone();
        let expected = {
            let ns = NamespacedKvEngine::new(base.clone(), &source);
            let mut rows: Vec<_> = ns
                .scan(user_range(), u64::MAX)
                .unwrap()
                .map(|r| r.unwrap())
                .filter(|p| is_table_key(table, &p.key))
                .map(|p| (p.key, p.value, p.version))
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(expected.len(), 10);
        let fx = DriverFixture::new(base, dir.path()).await;
        assert_eq!(fx.drive(table).await, P3::Done);
        assert_completed(&fx, table, &journal.plan, &expected).await;
        fx.manager.shutdown_all().await;
    });
}
