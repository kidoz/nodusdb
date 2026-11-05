use super::*;
use nodus_catalog::*;
use nodus_executor::{ExecutionContext, Executor, MemExecutor, Value};
use nodus_meta::MetaStore;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_rows_indexes_and_savepoints_commit_through_tracked_participants() {
    let dir = tempfile::tempdir().unwrap();
    let c = Cluster::open(dir.path()).await;
    let principal = PrincipalId::new();
    c.catalog
        .create_role(CreateRoleRequest {
            id: principal,
            name: "admin".into(),
            principal_type: PrincipalType::User,
            database_id: None,
            attributes: Default::default(),
        })
        .unwrap();
    c.catalog
        .grant_privilege(GrantPrivilegeRequest {
            id: GrantId::new(),
            principal_id: principal,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
            grantable: false,
            grantor: None,
        })
        .unwrap();
    let database = c
        .catalog
        .create_database(CreateDatabaseRequest {
            id: DatabaseId::new(),
            name: "default".into(),
            owner_role_id: None,
        })
        .unwrap();
    c.catalog
        .create_schema(CreateSchemaRequest {
            id: SchemaId::new(),
            database_id: database.id,
            name: "public".into(),
            owner_role_id: None,
            managed_access: false,
        })
        .unwrap();
    let executor = Arc::new(MemExecutor::new(
        c.catalog.clone(),
        Arc::new(crate::raft_catalog::RaftCatalogWriter {
            reader: c.catalog.clone(),
            router: c.engines[0].router.clone(),
            shard_id: META_SHARD.into(),
        }),
        Arc::new(nodus_authz::DefaultAuthzEngine::new(c.catalog.clone())),
        Arc::new(nodus_audit::MemoryAuditSink::new()),
        c.engines[0].clone(),
        c.clock.clone(),
    ));
    let engine = c.engines[0].clone();
    let groups = c.groups.clone();
    let query = move |sql: &'static str| {
        let executor = executor.clone();
        let engine = engine.clone();
        let groups = groups.clone();
        tokio::task::spawn_blocking(move || {
            // Make read visibility independent of learner catch-up timing.
            for group in groups {
                engine.router.read_barrier(&group)?;
            }
            engine.router.read_barrier(META_SHARD)?;
            let ctx = ExecutionContext {
                session_id: "tracked-sql".into(),
                principal_id: principal,
                active_roles: vec![],
                authz_catalog_version: 1,
                ..Default::default()
            };
            let stmt = nodus_sql::parse_sql(sql)?.remove(0);
            executor.execute_logical(&ctx, nodus_executor::plan_statement(&stmt, &[])?)
        })
    };
    query("CREATE TABLE tracked_t (id INT PRIMARY KEY, v INT UNIQUE)")
        .await
        .unwrap()
        .unwrap();
    query("CREATE TABLE tracked_u (id INT PRIMARY KEY, v INT UNIQUE)")
        .await
        .unwrap()
        .unwrap();
    for (i, name) in ["tracked_t", "tracked_u"].into_iter().enumerate() {
        let table = c.catalog.get_table("default", "public", name).unwrap();
        let mut map = c.mapping.get_shard_map(c.tables[i]).unwrap();
        map.table_id = table.id;
        for shard in &mut map.shards {
            shard.table_id = table.id;
        }
        c.mapping.update_shard_map(map).unwrap();
    }
    for sql in [
        "BEGIN",
        "INSERT INTO tracked_t VALUES (1, 10)",
        "INSERT INTO tracked_u VALUES (1, 20)",
        "SAVEPOINT s",
        "UPDATE tracked_t SET v = 11 WHERE id = 1",
        "DELETE FROM tracked_u WHERE id = 1",
        "INSERT INTO tracked_u VALUES (2, 30)",
        "ROLLBACK TO SAVEPOINT s",
        "COMMIT",
    ] {
        query(sql).await.unwrap().unwrap();
    }
    for (sql, expected) in [
        ("SELECT id, v FROM tracked_t WHERE v = 10", 10),
        ("SELECT id, v FROM tracked_u WHERE v = 20", 20),
    ] {
        let result = query(sql).await.unwrap().unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].values,
            vec![Value::Int(1), Value::Int(expected)]
        );
    }
    assert!(
        query("SELECT * FROM tracked_t WHERE v = 11")
            .await
            .unwrap()
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(
        query("SELECT * FROM tracked_u WHERE v = 30")
            .await
            .unwrap()
            .unwrap()
            .rows
            .is_empty()
    );
    // The row groups and the meta-group indexes all acquired durable tracked
    // participant decisions through the same production coordinator path.
    for group in [META_SHARD, c.groups[0].as_str(), c.groups[1].as_str()] {
        assert!(
            c.engines[0]
                .engine_for(group)
                .scan(
                    KeyRange {
                        start: Bytes::from_static(participant::PREFIX),
                        end: Bytes::from_static(b"\x01txn-participant0")
                    },
                    u64::MAX
                )
                .unwrap()
                .next()
                .is_some()
        );
    }
    drop(query);
    c.shutdown().await;
}
