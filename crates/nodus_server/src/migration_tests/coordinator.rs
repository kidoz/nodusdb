use super::*;
use crate::{
    migration_coordinator::MigrationCoordinator,
    multi_raft::{META_SHARD, MultiRaftManager},
    raft_kv::RaftKvEngine,
    raft_router::RaftRouter,
};
use nodus_raftstore::migration::coordinator::{
    CommandV2 as C, PhaseV2 as P, PlanV2, read_record_v2,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
    time::Duration,
};

struct Fixture {
    manager: Arc<MultiRaftManager>,
    engine: Arc<RaftKvEngine>,
    catalog: Arc<nodus_catalog::MemoryCatalog>,
    clock: Arc<nodus_txn::MemTxnManager>,
}

impl Fixture {
    async fn new(kv: Arc<dyn KvEngine>, dir: &std::path::Path) -> Self {
        let clock = Arc::new(nodus_txn::MemTxnManager::new());
        let manager = Arc::new(MultiRaftManager::new(
            1,
            "127.0.0.1:0".into(),
            Arc::new(openraft::Config::default().validate().unwrap()),
            nodus_raftstore::server::RaftState::new(),
            kv.clone(),
            None,
            clock.clone(),
            Some(dir.to_path_buf()),
            Default::default(),
        ));
        let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
        let meta = Arc::new(nodus_meta::MemMetaStore::new());
        let raft = manager
            .create_meta(kv.clone(), catalog.clone(), catalog.clone(), meta.clone())
            .await
            .unwrap();
        let _ = raft
            .initialize(BTreeMap::from([(
                1,
                openraft::BasicNode::new("127.0.0.1:0"),
            )]))
            .await;
        raft.wait(Some(Duration::from_secs(10)))
            .current_leader(1, "fixture election")
            .await
            .unwrap();
        let mut router = RaftRouter::spawn(manager.clone());
        router.enable_migration_for_test();
        let engine = Arc::new(RaftKvEngine {
            local: kv,
            router,
            shard_router: Arc::new(nodus_sharding::CatalogShardRouter::new(meta)),
            manager: manager.clone(),
            txn_groups: Mutex::new(HashMap::new()),
            metrics: Default::default(),
        });
        Self {
            manager,
            engine,
            catalog,
            clock,
        }
    }

    async fn command(&self, cmd: ShardCommand) -> ShardResponse {
        self.manager
            .get(META_SHARD)
            .await
            .unwrap()
            .client_write(cmd)
            .await
            .unwrap()
            .data
    }

    async fn control(&self, cmd: C) {
        assert!(self.command(ShardCommand::MigrationV2(cmd)).await.success);
    }

    fn coordinator(&self) -> MigrationCoordinator {
        MigrationCoordinator::new(self.manager.clone(), self.engine.router.clone())
    }
}

fn plan_v2(table: TableId) -> PlanV2 {
    PlanV2 {
        migration: MigrationPlan {
            table_id: table,
            operation_id: Uuid::new_v4(),
            sources: vec![META_SHARD.into()],
            source_epochs: BTreeMap::from([(META_SHARD.into(), 0)]),
            destinations: vec!["destination".into()],
        },
        expected_map: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_cancellation_seals_unacknowledged_acquires() {
    let dir = tempfile::tempdir().unwrap();
    let fx = Fixture::new(Arc::new(nodus_storage_mem::MemKvEngine::new()), dir.path()).await;
    let table = TableId::new();
    let plan = plan_v2(table);
    let operation = plan.migration.operation_id;
    fx.control(C::Plan(plan)).await;
    fx.control(C::Begin { table, operation }).await;
    // Acquire committed; its response/ack is lost when the coordinator dies.
    assert!(
        fx.command(ShardCommand::MigrationV1(Control::Acquire {
            operation_id: operation,
            expected_epoch: 0
        }))
        .await
        .success
    );
    assert!(
        read_record_v2(fx.engine.local.as_ref(), table)
            .unwrap()
            .unwrap()
            .acquired
            .is_empty()
    );
    fx.control(C::RequestCancel { table, operation }).await;
    let restarted = fx.coordinator();
    assert_eq!(restarted.resume(table).await.unwrap(), P::Cancelling);
    assert_eq!(restarted.resume(table).await.unwrap(), P::Cancelled);
    assert!(
        !read_fence(fx.engine.local.as_ref())
            .unwrap()
            .unwrap()
            .closed
    );
    assert!(
        !fx.command(ShardCommand::MigrationV1(Control::Acquire {
            operation_id: operation,
            expected_epoch: 0
        }))
        .await
        .success
    );
    // Also cancel a never-acquired source. The epoch must advance to seal late RPCs.
    let mut next = plan_v2(table);
    next.migration.source_epochs.insert(META_SHARD.into(), 1);
    let operation = next.migration.operation_id;
    fx.control(C::Plan(next)).await;
    fx.control(C::RequestCancel { table, operation }).await;
    assert_eq!(restarted.resume(table).await.unwrap(), P::Cancelling);
    assert_eq!(restarted.resume(table).await.unwrap(), P::Cancelled);
    assert_eq!(
        read_fence(fx.engine.local.as_ref()).unwrap().unwrap().epoch,
        2
    );
    assert!(
        !fx.command(ShardCommand::MigrationV1(Control::Acquire {
            operation_id: operation,
            expected_epoch: 1
        }))
        .await
        .success
    );
    let production =
        MigrationCoordinator::new(fx.manager.clone(), RaftRouter::spawn(fx.manager.clone()));
    assert!(
        production
            .resume(table)
            .await
            .unwrap_err()
            .to_string()
            .contains("activation")
    );
    fx.manager.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_waits_for_live_transactions_and_stale_savepoints_fail() {
    use nodus_storage_api::IntentReplacement as R;
    let dir = tempfile::tempdir().unwrap();
    let fx = Fixture::new(Arc::new(nodus_storage_mem::MemKvEngine::new()), dir.path()).await;
    let table = TableId::new();
    let plan = plan_v2(table);
    let operation = plan.migration.operation_id;
    fx.control(C::Plan(plan)).await;
    let txn = TxnId::new();
    let e = fx.engine.clone();
    tokio::task::spawn_blocking(move || {
        e.write_intent(
            txn,
            Bytes::from_static(b"row"),
            Bytes::from_static(b"value"),
        )
        .unwrap()
    })
    .await
    .unwrap();
    fx.control(C::RequestCancel { table, operation }).await;
    assert!(fx.coordinator().resume(table).await.is_err());
    let e = fx.engine.clone();
    tokio::task::spawn_blocking(move || {
        e.replace_intent(txn, Bytes::from_static(b"row"), R::Clear)
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(fx.coordinator().resume(table).await.unwrap(), P::Cancelling);
    assert_eq!(fx.coordinator().resume(table).await.unwrap(), P::Cancelled);
    let e = fx.engine.clone();
    tokio::task::spawn_blocking(move || {
        assert!(
            e.replace_intent(
                txn,
                Bytes::from_static(b"row"),
                R::Put(Bytes::from_static(b"bad"))
            )
            .is_err()
        );
        assert!(
            e.write_intent(
                txn,
                Bytes::from_static(b"new-key"),
                Bytes::from_static(b"bad")
            )
            .is_err()
        );
        assert!(e.delete_intent(txn, Bytes::from_static(b"row")).is_err());
        assert!(e.commit(txn, 100).is_err());
        e.abort(txn).unwrap();
        let fresh = TxnId::new();
        e.write_intent(
            fresh,
            Bytes::from_static(b"row"),
            Bytes::from_static(b"good"),
        )
        .unwrap();
        e.commit(fresh, 101).unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        fx.engine.get(b"row", 101).unwrap(),
        Some(Bytes::from_static(b"good"))
    );
    assert_eq!(fx.engine.get(b"new-key", 101).unwrap(), None);
    fx.manager.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_resumes_after_abrupt_process_exit_before_ack() {
    const ENV: &str = "NODUS_R3B_COORDINATOR_CRASH_DIR";
    let table = TableId(Uuid::from_u128(301));
    if let Ok(path) = std::env::var(ENV) {
        let dir = std::path::Path::new(&path);
        let kv = Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(dir, None).unwrap());
        let fx = Fixture::new(kv, dir).await;
        let plan = plan_v2(table);
        let operation = plan.migration.operation_id;
        fx.control(C::Plan(plan)).await;
        fx.control(C::Begin { table, operation }).await;
        assert!(
            fx.command(ShardCommand::MigrationV1(Control::Acquire {
                operation_id: operation,
                expected_epoch: 0
            }))
            .await
            .success
        );
        std::process::exit(0); // no Drop, no coordinator Ack, no Raft shutdown
    }
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact", "migration_tests::coordinator::coordinator_resumes_after_abrupt_process_exit_before_ack", "--nocapture"]).env(ENV, dir.path()).status().unwrap();
    assert!(status.success());
    let kv = Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(dir.path(), None).unwrap());
    assert!(read_fence(kv.as_ref()).unwrap().unwrap().closed);
    assert!(
        read_record_v2(kv.as_ref(), table)
            .unwrap()
            .unwrap()
            .acquired
            .is_empty()
    );
    let fx = Fixture::new(kv, dir.path()).await;
    assert_eq!(fx.coordinator().resume(table).await.unwrap(), P::Fencing);
    assert_eq!(fx.coordinator().resume(table).await.unwrap(), P::Fenced);
    let record = read_record_v2(fx.engine.local.as_ref(), table)
        .unwrap()
        .unwrap();
    assert_eq!(record.acquired, BTreeMap::from([(META_SHARD.into(), 1)]));
    fx.manager.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_ack_is_recovered_from_the_journal() {
    use nodus_raftstore::migration::coordinator::{
        RecordV2,
        driver::{self, Backend},
    };
    struct LoseAck(MigrationCoordinator);
    impl Backend for LoseAck {
        async fn load(&self, table: TableId) -> anyhow::Result<RecordV2> {
            self.0.load(table).await
        }
        async fn routing_matches(&self, plan: &PlanV2) -> anyhow::Result<bool> {
            self.0.routing_matches(plan).await
        }
        async fn submit(&self, group: &str, command: ShardCommand) -> anyhow::Result<()> {
            if matches!(command, ShardCommand::MigrationV2(C::Ack { .. })) {
                std::future::pending::<()>().await;
            }
            self.0.submit(group, command).await
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let fx = Fixture::new(Arc::new(nodus_storage_mem::MemKvEngine::new()), dir.path()).await;
    let table = TableId::new();
    let plan = plan_v2(table);
    let operation = plan.migration.operation_id;
    fx.control(C::Plan(plan)).await;
    fx.control(C::Begin { table, operation }).await;
    assert!(
        driver::resume(&LoseAck(fx.coordinator()), table, Duration::from_secs(1))
            .await
            .unwrap_err()
            .to_string()
            .contains("timed out")
    );
    assert!(
        read_fence(fx.engine.local.as_ref())
            .unwrap()
            .unwrap()
            .closed
    );
    assert!(
        read_record_v2(fx.engine.local.as_ref(), table)
            .unwrap()
            .unwrap()
            .acquired
            .is_empty()
    );
    assert_eq!(fx.coordinator().resume(table).await.unwrap(), P::Fencing);
    assert_eq!(fx.coordinator().resume(table).await.unwrap(), P::Fenced);
    fx.manager.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_savepoint_repair_and_fencing_share_the_apply_lock() {
    let dir = tempfile::tempdir().unwrap();
    let fx = Fixture::new(Arc::new(nodus_storage_mem::MemKvEngine::new()), dir.path()).await;
    for i in 0..8 {
        let group = format!("race-{i}");
        let raft = fx.manager.get_or_create_data(&group).await.unwrap();
        raft.initialize(BTreeMap::from([(
            1,
            openraft::BasicNode::new("127.0.0.1:0"),
        )]))
        .await
        .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .current_leader(1, "race election")
            .await
            .unwrap();
        let txn = TxnId::new();
        let operation_id = Uuid::new_v4();
        let router = RaftRouter::spawn(fx.manager.clone());
        let repair_group = group.clone();
        let repair = tokio::task::spawn_blocking(move || {
            router.repair_legacy(
                &repair_group,
                txn,
                Bytes::from_static(b"row"),
                nodus_storage_api::IntentReplacement::Put(Bytes::from_static(b"restored")),
            )
        });
        let (repair, fence) = tokio::join!(
            repair,
            raft.client_write(ShardCommand::MigrationV1(Control::Acquire {
                operation_id,
                expected_epoch: 0
            }))
        );
        let repaired = repair.unwrap().is_ok();
        let fenced = fence.unwrap().data.success;
        assert_ne!(repaired, fenced, "repair and fence must never both succeed");
        let kv = NamespacedKvEngine::new(fx.engine.local.clone(), &group);
        assert_eq!(kv.has_pending_intents(b"").unwrap(), repaired);
        assert_eq!(read_fence(&kv).unwrap().is_some_and(|f| f.closed), fenced);
    }
    fx.manager.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_routing_forces_cancellation_instead_of_fencing() {
    let dir = tempfile::tempdir().unwrap();
    let fx = Fixture::new(Arc::new(nodus_storage_mem::MemKvEngine::new()), dir.path()).await;
    let table = TableId::new();
    fx.control(C::Plan(plan_v2(table))).await;
    let machine = fx.manager.machine(META_SHARD).await.unwrap();
    let meta = machine.read().await.meta_store.clone().unwrap();
    nodus_sharding::ShardOrchestrator::new(meta)
        .init_single_shard(table)
        .unwrap();
    assert_eq!(fx.coordinator().resume(table).await.unwrap(), P::Cancelling);
    assert!(read_fence(fx.engine.local.as_ref()).unwrap().is_none());
    fx.coordinator().resume(table).await.unwrap();
    assert_eq!(fx.coordinator().resume(table).await.unwrap(), P::Cancelled);
    assert!(
        !read_fence(fx.engine.local.as_ref())
            .unwrap()
            .unwrap()
            .closed
    );
    fx.manager.shutdown_all().await;
}

#[tokio::test]
async fn coordinator_validates_acks_routing_decisions_and_snapshot_versions() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::new(
        Arc::new(nodus_storage_mem::MemKvEngine::new()),
        dir.path().join("source"),
    )
    .await;
    let table = TableId::new();
    let plan = plan_v2(table);
    let operation = plan.migration.operation_id;
    let mut invalid = plan.clone();
    invalid.migration.sources.push("unknown".into());
    invalid.migration.source_epochs.insert("unknown".into(), 0);
    assert!(
        !h.apply(ShardCommand::MigrationV2(C::Plan(invalid)))
            .await
            .success
    );
    assert!(
        h.apply(ShardCommand::MigrationV2(C::Plan(plan)))
            .await
            .success
    );
    assert!(
        !h.control(Control::Plan(plan_v2(table).migration))
            .await
            .success
    );

    assert!(
        h.apply(ShardCommand::MigrationV2(C::Begin { table, operation }))
            .await
            .success
    );
    assert!(
        !h.apply(ShardCommand::MigrationV2(C::Finish {
            table,
            operation,
            cancelled: false
        }))
        .await
        .success
    );
    assert!(
        !h.apply(ShardCommand::MigrationV2(C::Ack {
            table,
            operation,
            source: "unknown".into(),
            epoch: 1,
            cancelled: false
        }))
        .await
        .success
    );
    // No live intents, but a durable 2PC decision still prevents fencing.
    let txn = TxnId::new();
    h.kv.write_intent(
        txn,
        Bytes::from_static(b"\x00txn2pc\x00fixture"),
        Bytes::from_static(b"decision"),
    )
    .unwrap();
    h.kv.commit(txn, 100).unwrap();
    assert!(
        !h.control(Control::Acquire {
            operation_id: operation,
            expected_epoch: 0
        })
        .await
        .success
    );
    assert!(
        !h.apply(ShardCommand::MigrationV2(C::CancelParticipant {
            operation,
            expected_epoch: 0
        }))
        .await
        .success
    );
    let snapshot = h.store.build_snapshot().await.unwrap();
    let mut receiver = Harness::new(
        Arc::new(nodus_storage_mem::MemKvEngine::new()),
        dir.path().join("receiver"),
    )
    .await;
    receiver
        .store
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(
        read_record_v2(receiver.kv.as_ref(), table)
            .unwrap()
            .unwrap()
            .phase,
        P::Fencing
    );
    let journal = nodus_raftstore::migration::coordinator::key(table);
    let txn = TxnId::new();
    receiver
        .kv
        .write_intent(
            txn,
            Bytes::from(journal),
            Bytes::from(nodus_common::versioned::encode(99, b"{}")),
        )
        .unwrap();
    receiver.kv.commit(txn, 1000).unwrap();
    assert!(read_record_v2(receiver.kv.as_ref(), table).is_err());
    assert!(receiver.store.build_snapshot().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_savepoints_preserve_rows_indexes_and_original_epoch() {
    use nodus_catalog::*;
    use nodus_executor::{ExecutionContext, Executor, MemExecutor};
    let dir = tempfile::tempdir().unwrap();
    let fx = Fixture::new(
        Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(dir.path(), None).unwrap()),
        dir.path(),
    )
    .await;
    let principal = PrincipalId::new();
    fx.catalog
        .create_role(CreateRoleRequest {
            id: principal,
            name: "admin".into(),
            principal_type: PrincipalType::User,
            database_id: None,
        })
        .unwrap();
    fx.catalog
        .grant_privilege(GrantPrivilegeRequest {
            id: GrantId::new(),
            principal_id: principal,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
    let executor = Arc::new(MemExecutor::new(
        fx.catalog.clone(),
        Arc::new(crate::raft_catalog::RaftCatalogWriter {
            reader: fx.catalog.clone(),
            router: fx.engine.router.clone(),
            shard_id: META_SHARD.into(),
        }),
        Arc::new(nodus_authz::DefaultAuthzEngine::new(fx.catalog.clone())),
        Arc::new(nodus_audit::MemoryAuditSink::new()),
        fx.engine.clone(),
        fx.clock.clone(),
    ));
    let query = move |sql: &'static str| {
        let executor = executor.clone();
        tokio::task::spawn_blocking(move || {
            let ctx = ExecutionContext {
                session_id: "sql-migration".into(),
                principal_id: principal,
                active_roles: vec![],
                authz_catalog_version: 1,
            };
            let stmt = nodus_sql::parse_sql(sql)?.remove(0);
            executor.execute_logical(&ctx, nodus_executor::plan_statement(&stmt, &[])?)
        })
    };
    let database = fx
        .catalog
        .create_database(CreateDatabaseRequest {
            id: DatabaseId::new(),
            name: "default".into(),
            owner_role_id: None,
        })
        .unwrap();
    fx.catalog
        .create_schema(CreateSchemaRequest {
            id: SchemaId::new(),
            database_id: database.id,
            name: "public".into(),
            owner_role_id: None,
            managed_access: false,
        })
        .unwrap();
    query("CREATE TABLE t (id INT PRIMARY KEY, v INT UNIQUE)")
        .await
        .unwrap()
        .unwrap();
    let table = fx.catalog.get_table("default", "public", "t").unwrap().id;
    let machine = fx.manager.machine(META_SHARD).await.unwrap();
    let meta = machine.read().await.meta_store.clone().unwrap();
    let shard = nodus_sharding::ShardOrchestrator::new(meta.clone())
        .init_single_shard(table)
        .unwrap();
    let group = MultiRaftManager::data_group_id(shard);
    let data_raft = fx.manager.get_or_create_data(&group).await.unwrap();
    data_raft
        .initialize(BTreeMap::from([(
            1,
            openraft::BasicNode::new("127.0.0.1:0"),
        )]))
        .await
        .unwrap();
    data_raft
        .wait(Some(Duration::from_secs(10)))
        .current_leader(1, "data election")
        .await
        .unwrap();

    query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap()
        .unwrap();
    query("BEGIN").await.unwrap().unwrap();
    query("SAVEPOINT s").await.unwrap().unwrap();
    query("UPDATE t SET v = 20 WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    query("INSERT INTO t VALUES (2, 30)")
        .await
        .unwrap()
        .unwrap();
    query("DELETE FROM t WHERE id = 1").await.unwrap().unwrap();
    query("ROLLBACK TO SAVEPOINT s").await.unwrap().unwrap();
    query("COMMIT").await.unwrap().unwrap();
    let rows = query("SELECT id, v FROM t WHERE v = 10")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(
        rows.rows[0].values,
        vec![
            nodus_executor::Value::Int(1),
            nodus_executor::Value::Int(10)
        ]
    );
    assert!(
        query("SELECT * FROM t WHERE v = 30")
            .await
            .unwrap()
            .unwrap()
            .rows
            .is_empty()
    );
    // A savepoint that clears every intent leaves a live transaction with an
    // old epoch. Cancellation may seal the now-quiescent participant.
    query("BEGIN").await.unwrap().unwrap();
    query("SAVEPOINT s").await.unwrap().unwrap();
    query("INSERT INTO t VALUES (3, 40)")
        .await
        .unwrap()
        .unwrap();
    query("ROLLBACK TO SAVEPOINT s").await.unwrap().unwrap();
    let mut plan = plan_v2(table);
    plan.expected_map = Some(meta.get_shard_map(table).unwrap());
    plan.migration.sources.push(group.clone());
    plan.migration.source_epochs.insert(group, 0);
    let operation = plan.migration.operation_id;
    fx.control(C::Plan(plan)).await;
    fx.control(C::Begin { table, operation }).await;
    for _ in 0..3 {
        fx.coordinator().resume(table).await.unwrap();
    } // fence meta and row group
    assert_eq!(
        read_record_v2(fx.engine.local.as_ref(), table)
            .unwrap()
            .unwrap()
            .phase,
        P::Fenced
    );
    assert!(
        query("ALTER TABLE t ADD COLUMN blocked INT")
            .await
            .unwrap()
            .is_err()
    );
    fx.control(C::RequestCancel { table, operation }).await;
    for _ in 0..3 {
        fx.coordinator().resume(table).await.unwrap();
    }
    assert_eq!(
        read_record_v2(fx.engine.local.as_ref(), table)
            .unwrap()
            .unwrap()
            .phase,
        P::Cancelled
    );
    assert!(
        query("INSERT INTO t VALUES (4, 50)")
            .await
            .unwrap()
            .is_err()
    );
    query("ROLLBACK").await.unwrap().unwrap();
    query("INSERT INTO t VALUES (3, 40)")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        query("SELECT * FROM t WHERE v = 40")
            .await
            .unwrap()
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(
        query("SELECT * FROM t WHERE v = 50")
            .await
            .unwrap()
            .unwrap()
            .rows
            .is_empty()
    );
    fx.manager.shutdown_all().await;
}
