use nodus_testkit::TestServer;
use std::time::Duration;
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=nodus password=nodus dbname=default",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port()
    );
    for _ in 0..30 {
        if let Ok((client, connection)) = tokio_postgres::connect(&conn_str, NoTls).await {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("PGWire server did not start in time");
}

fn rows_of(msgs: &[SimpleQueryMessage]) -> Vec<&tokio_postgres::SimpleQueryRow> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_catalog_introspection() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA orm_test;")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE TABLE orm_test.users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT);",
        )
        .await
        .unwrap();

    // Query pg_namespace
    let msgs = client
        .simple_query("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 'orm_test';")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1, "Expected 1 namespace row");
    assert_eq!(rows[0].get("nspname"), Some("orm_test"));

    // Query pg_class (tables)
    let msgs = client
        .simple_query(
            "SELECT c.relname \n\
         FROM pg_catalog.pg_class c \n\
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \n\
         WHERE n.nspname = 'orm_test' AND c.relname = 'users';",
        )
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1, "Expected 1 pg_class row");
    assert_eq!(rows[0].get("relname"), Some("users"));

    // Query pg_attribute (columns)
    let msgs = client
        .simple_query(
            "SELECT a.attname, a.attnotnull \n\
         FROM pg_catalog.pg_attribute a \n\
         JOIN pg_catalog.pg_class c ON a.attrelid = c.oid \n\
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \n\
         WHERE n.nspname = 'orm_test' AND c.relname = 'users' AND a.attnum > 0;",
        )
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 3, "Expected 3 pg_attribute rows");

    // Sort or just check existence
    let mut cols: Vec<Option<&str>> = rows.iter().map(|r| r.get("attname")).collect();
    cols.sort();
    assert_eq!(cols, vec![Some("age"), Some("id"), Some("name")]);

    // Query pg_type (types)
    let msgs = client
        .simple_query("SELECT typname FROM pg_catalog.pg_type WHERE oid = 23;")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1, "Expected 1 pg_type row");
    assert_eq!(rows[0].get("typname"), Some("int4"));
}

// pgjdbc DatabaseMetaData and DataGrip/IntelliJ introspection scan these
// catalogs; they must answer with zero rows rather than error.
#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_catalog_introspection_only_tables_are_empty() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    for (query, table) in [
        (
            "SELECT locktype, database, relation, pid, mode, granted FROM pg_catalog.pg_locks;",
            "pg_locks",
        ),
        (
            "SELECT objoid, classoid, objsubid, description FROM pg_catalog.pg_description;",
            "pg_description",
        ),
        (
            "SELECT objoid, classoid, description FROM pg_catalog.pg_shdescription;",
            "pg_shdescription",
        ),
        (
            "SELECT adrelid, adnum, adbin FROM pg_catalog.pg_attrdef;",
            "pg_attrdef",
        ),
    ] {
        let msgs = client.simple_query(query).await.unwrap();
        assert_eq!(rows_of(&msgs).len(), 0, "Expected {} to be empty", table);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_catalog_and_information_schema_metadata_surface() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA catalog_more;")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE TABLE catalog_more.items (
                id INT PRIMARY KEY,
                code TEXT UNIQUE,
                price NUMERIC CHECK (price > 0),
                payload JSONB
            );",
        )
        .await
        .unwrap();
    client
        .simple_query("CREATE INDEX idx_items_payload ON catalog_more.items (payload);")
        .await
        .unwrap();

    let msgs = client
        .simple_query("SELECT datname FROM pg_catalog.pg_database WHERE datname = 'default';")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("datname"), Some("default"));

    let msgs = client
        .simple_query(
            "SELECT name, setting FROM pg_catalog.pg_settings \
             WHERE name IN ('server_version_num', 'TimeZone') ORDER BY name;",
        )
        .await
        .unwrap();
    assert_eq!(rows_of(&msgs).len(), 2);

    let msgs = client
        .simple_query("SELECT rolname FROM pg_catalog.pg_roles WHERE rolname = 'nodus';")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("rolname"), Some("nodus"));

    let msgs = client
        .simple_query(
            "SELECT collname FROM pg_catalog.pg_collation WHERE collname IN ('C', 'default');",
        )
        .await
        .unwrap();
    assert_eq!(rows_of(&msgs).len(), 2);

    let msgs = client
        .simple_query("SELECT amname FROM pg_catalog.pg_am WHERE amname = 'btree';")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("amname"), Some("btree"));

    let msgs = client
        .simple_query("SELECT oprname FROM pg_catalog.pg_operator WHERE oprname = '=';")
        .await
        .unwrap();
    assert!(!rows_of(&msgs).is_empty());

    let msgs = client
        .simple_query("SELECT castsource, casttarget FROM pg_catalog.pg_cast;")
        .await
        .unwrap();
    assert!(!rows_of(&msgs).is_empty());

    let msgs = client
        .simple_query(
            "SELECT con.conname, con.contype \
             FROM pg_catalog.pg_constraint con \
             JOIN pg_catalog.pg_class cls ON con.conrelid = cls.oid \
             JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid \
             WHERE nsp.nspname = 'catalog_more' AND cls.relname = 'items';",
        )
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert!(
        rows.iter().any(|row| row.get("contype") == Some("p")),
        "expected primary key constraint"
    );
    assert!(
        rows.iter().any(|row| row.get("contype") == Some("u")),
        "expected unique constraint"
    );
    assert!(
        rows.iter().any(|row| row.get("contype") == Some("c")),
        "expected check constraint"
    );

    let msgs = client
        .simple_query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'catalog_more' AND table_name = 'items';",
        )
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("table_name"), Some("items"));

    let msgs = client
        .simple_query(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = 'catalog_more' AND table_name = 'items';",
        )
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 4);
    assert!(
        rows.iter()
            .any(|row| row.get("column_name") == Some("payload")
                && row.get("data_type") == Some("jsonb"))
    );

    let msgs = client
        .simple_query(
            "SELECT constraint_name, constraint_type FROM information_schema.table_constraints \
             WHERE table_schema = 'catalog_more' AND table_name = 'items';",
        )
        .await
        .unwrap();
    assert!(!rows_of(&msgs).is_empty());

    let msgs = client
        .simple_query(
            "SELECT indexname FROM pg_catalog.pg_indexes \
             WHERE schemaname = 'catalog_more' AND tablename = 'items';",
        )
        .await
        .unwrap();
    assert!(
        rows_of(&msgs)
            .iter()
            .any(|row| row.get("indexname") == Some("idx_items_payload"))
    );

    let msgs = client
        .simple_query(
            "SELECT index_name FROM information_schema.indexes \
             WHERE table_schema = 'catalog_more' AND table_name = 'items';",
        )
        .await
        .unwrap();
    assert!(
        rows_of(&msgs)
            .iter()
            .any(|row| row.get("index_name") == Some("idx_items_payload"))
    );
}

// Unknown pg_catalog relations must fail with SQLSTATE 42P01 (undefined
// table), which introspecting clients handle quietly, not XX000.
#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_catalog_unknown_relation_is_undefined_table() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    let err = client
        .simple_query("SELECT * FROM pg_catalog.pg_no_such_table;")
        .await
        .expect_err("unknown pg_catalog relation should error");
    let db_err = err.as_db_error().expect("expected a database error");
    assert_eq!(
        db_err.code(),
        &tokio_postgres::error::SqlState::UNDEFINED_TABLE
    );
    assert!(
        db_err.message().contains("pg_no_such_table"),
        "message should name the missing relation: {}",
        db_err.message()
    );
}

/// `pg_locks` stays empty under autocommit but reports a `transactionid` row for
/// an open explicit transaction — the signal DataGrip/JetBrains use to find
/// sessions holding a transaction open. The implicit per-statement transaction
/// that wraps the `pg_locks` query itself must not appear.
#[tokio::test(flavor = "multi_thread")]
async fn test_pg_locks_reports_explicit_transactions_only() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    // Autocommit: the query's own implicit transaction is not reported.
    let msgs = client
        .simple_query("SELECT transactionid FROM pg_catalog.pg_locks;")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&msgs).len(),
        0,
        "pg_locks should be empty outside an explicit transaction"
    );

    // Inside an explicit BEGIN block, exactly one transactionid lock shows up.
    client.simple_query("BEGIN;").await.unwrap();
    let msgs = client
        .simple_query("SELECT locktype, mode, granted, transactionid FROM pg_catalog.pg_locks;")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1, "expected one lock row inside a transaction");
    assert_eq!(rows[0].get(0), Some("transactionid"));
    assert_eq!(rows[0].get(1), Some("ExclusiveLock"));
    assert_eq!(rows[0].get(2), Some("t"));
    assert!(
        rows[0].get(3).is_some_and(|v| !v.is_empty()),
        "transactionid should be populated"
    );

    client.simple_query("COMMIT;").await.unwrap();

    // After COMMIT the lock is released again.
    let msgs = client
        .simple_query("SELECT transactionid FROM pg_catalog.pg_locks;")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&msgs).len(),
        0,
        "pg_locks should be empty after COMMIT"
    );
}

/// IDE/driver introspection (DataGrip/pgjdbc) probes several `pg_catalog`
/// relations NodusDB does not model. They must resolve (not error with 42P01):
/// the standard tablespaces and the UTC timezone are advertised; role
/// membership and event triggers are present but empty.
#[tokio::test(flavor = "multi_thread")]
async fn test_pg_catalog_introspection_relations_resolve() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    // These exist with their two/one built-in rows.
    let msgs = client
        .simple_query("SELECT spcname FROM pg_catalog.pg_tablespace ORDER BY oid;")
        .await
        .expect("pg_tablespace resolves");
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0), Some("pg_default"));
    assert_eq!(rows[1].get(0), Some("pg_global"));

    let msgs = client
        .simple_query("SELECT name, utc_offset FROM pg_catalog.pg_timezone_names;")
        .await
        .expect("pg_timezone_names resolves");
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("UTC"));

    // These resolve but are empty.
    for (query, table) in [
        (
            "SELECT roleid, member FROM pg_catalog.pg_auth_members;",
            "pg_auth_members",
        ),
        (
            "SELECT evtname, evtevent FROM pg_catalog.pg_event_trigger;",
            "pg_event_trigger",
        ),
    ] {
        let msgs = client
            .simple_query(query)
            .await
            .unwrap_or_else(|e| panic!("{table} should resolve, got: {e}"));
        assert_eq!(rows_of(&msgs).len(), 0, "{table} should be empty");
    }
}
