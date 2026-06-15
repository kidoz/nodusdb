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
            "SELECT adrelid, adnum, adbin FROM pg_catalog.pg_attrdef;",
            "pg_attrdef",
        ),
    ] {
        let msgs = client.simple_query(query).await.unwrap();
        assert_eq!(rows_of(&msgs).len(), 0, "Expected {} to be empty", table);
    }
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
