//! SQL compatibility through the real Rust PostgreSQL driver. Procedures and
//! PL/pgSQL are deliberately outside this suite's scope.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{FutureExt, SinkExt, TryStreamExt};
use nodus_testkit::TestServer;
use tokio_postgres::{Client, NoTls, error::SqlState, types::Type};

// Bound the whole client flow so a protocol regression fails instead of hanging.
async fn with_client<F>(test: F)
where
    F: AsyncFnOnce(&mut Client),
{
    tokio::time::timeout(Duration::from_secs(30), async {
        // An explicitly supplied disposable PostgreSQL reference is optional;
        // the normal test target always starts a fresh NodusDB server.
        let reference = std::env::var("NODUS_SQL_REFERENCE_URL").ok();
        let server = if reference.is_none() {
            Some(TestServer::start().await.expect("start isolated NodusDB"))
        } else {
            None
        };
        let config = if let Some(url) = &reference {
            url.parse::<tokio_postgres::Config>()
                .expect("parse reference URL")
        } else {
            let server = server.as_ref().unwrap();
            let mut config = tokio_postgres::Config::new();
            config
                .host(server.pgwire_addr.ip().to_string())
                .port(server.pgwire_addr.port())
                .user("nodus")
                .password("nodus")
                .dbname("default");
            config
        };
        let (mut client, connection) = config
            .connect(NoTls)
            .await
            .expect("connect Rust PostgreSQL driver");
        let connection = tokio::spawn(connection);
        // Each reference case gets its own schema, including under parallel tests.
        let schema = format!("rust_compat_{}", uuid::Uuid::new_v4().simple());
        if reference.is_some() {
            client
                .batch_execute(&format!(
                    "CREATE SCHEMA {schema}; SET search_path TO {schema}"
                ))
                .await
                .unwrap();
        }
        let result = AssertUnwindSafe(test(&mut client)).catch_unwind().await;
        if reference.is_some() {
            client
                .batch_execute(&format!("ROLLBACK; DROP SCHEMA {schema} CASCADE"))
                .await
                .expect("clean reference schema");
        }
        drop(client);
        connection
            .await
            .expect("connection task completes")
            .expect("connection closes cleanly");
        if let Some(server) = server {
            server.shutdown().await;
        }
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    })
    .await
    .expect("Rust driver SQL case exceeded 30 seconds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inferred_parameter_types() {
    with_client(async |client| {
        client
            .batch_execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, enabled BOOLEAN)",
            )
            .await
            .unwrap();
        let statement = client
            .prepare("INSERT INTO items (id, name, enabled) VALUES ($1, $2, $3)")
            .await
            .unwrap();
        assert_eq!(statement.params(), &[Type::INT4, Type::TEXT, Type::BOOL]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_parameters_and_dml_returning() {
    with_client(async |client| {
        client
            .batch_execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, enabled BOOLEAN)",
            )
            .await
            .unwrap();
        let insert = client.prepare_typed(
            "INSERT INTO items (id, name, enabled) VALUES ($1, $2, $3) RETURNING id, name, enabled",
            &[Type::INT4, Type::TEXT, Type::BOOL],
        ).await.unwrap();
        assert_eq!(insert.params(), &[Type::INT4, Type::TEXT, Type::BOOL]);
        assert_eq!(insert.columns()[0].type_(), &Type::INT4);
        assert_eq!(insert.columns()[1].type_(), &Type::TEXT);
        assert_eq!(insert.columns()[2].type_(), &Type::BOOL);
        let name = "O'Reilly — Привет $1; SELECT 99";
        let row = client
            .query_one(&insert, &[&1_i32, &name, &true])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>("id"), 1);
        assert_eq!(row.get::<_, String>("name"), name);
        assert!(row.get::<_, bool>("enabled"));
        let row = client
            .query_one(&insert, &[&2_i32, &"nullable", &Option::<bool>::None])
            .await
            .unwrap();
        assert_eq!(row.get::<_, Option<bool>>("enabled"), None);

        let update = client
            .prepare_typed(
                "UPDATE items SET name = $1 WHERE id = $2 RETURNING id, name",
                &[Type::TEXT, Type::INT4],
            )
            .await
            .unwrap();
        assert_eq!(update.params(), &[Type::TEXT, Type::INT4]);
        let row = client
            .query_one(&update, &[&"updated", &1_i32])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>(0), 1);
        assert_eq!(row.get::<_, String>(1), "updated");
        let delete = client
            .prepare_typed(
                "DELETE FROM items WHERE id = $1 RETURNING id",
                &[Type::INT4],
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .query_one(&delete, &[&2_i32])
                .await
                .unwrap()
                .get::<_, i32>(0),
            2
        );
        assert_eq!(
            client
                .execute_typed("DELETE FROM items WHERE id = $1", &[(&1_i32, Type::INT4)])
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            client
                .execute_typed("DELETE FROM items WHERE id = $1", &[(&1_i32, Type::INT4)])
                .await
                .unwrap(),
            0
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_api_savepoints_and_error_recovery() {
    with_client(async |client| {
        client.batch_execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL CHECK (balance >= 0))").await.unwrap();
        let mut tx = client.transaction().await.unwrap();
        tx.execute_typed("INSERT INTO accounts VALUES ($1, $2)", &[(&1_i32, Type::INT4), (&100_i32, Type::INT4)]).await.unwrap();
        let savepoint = tx.savepoint("before_debit").await.unwrap();
        let error = savepoint.execute("UPDATE accounts SET balance = -1 WHERE id = 1", &[]).await.unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::CHECK_VIOLATION));
        let error = savepoint.query("SELECT id FROM accounts", &[]).await.unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
        savepoint.rollback().await.unwrap();
        assert_eq!(tx.query_one("SELECT balance FROM accounts WHERE id = 1", &[]).await.unwrap().get::<_, i32>(0), 100);
        tx.execute_typed("UPDATE accounts SET balance = $1 WHERE id = $2", &[(&75_i32, Type::INT4), (&1_i32, Type::INT4)]).await.unwrap();
        tx.commit().await.unwrap();
        let tx = client.transaction().await.unwrap();
        tx.execute("DELETE FROM accounts WHERE id = 1", &[]).await.unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(client.query_one("SELECT balance FROM accounts WHERE id = 1", &[]).await.unwrap().get::<_, i32>(0), 75);
        // Autocommit errors must also leave the connection usable.
        let error = client.execute("INSERT INTO accounts VALUES (1, 0)", &[]).await.unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::UNIQUE_VIOLATION));
        assert_eq!(client.query("SELECT id FROM accounts", &[]).await.unwrap().len(), 1);
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cursor_api_resumes_without_lost_or_duplicate_rows() {
    with_client(async |client| {
        client.batch_execute("CREATE TABLE cursor_rows (id INTEGER PRIMARY KEY, label TEXT); INSERT INTO cursor_rows VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')").await.unwrap();
        let tx = client.transaction().await.unwrap();
        let statement = tx.prepare_typed("SELECT id, label FROM cursor_rows WHERE id >= $1 ORDER BY id", &[Type::INT4]).await.unwrap();
        let portal = tx.bind(&statement, &[&2_i32]).await.unwrap();
        let mut actual = Vec::new();
        for expected_len in [2, 2, 0] {
            let rows = tx.query_portal(&portal, 2).await.unwrap();
            assert_eq!(rows.len(), expected_len);
            actual.extend(rows.into_iter().map(|row| (row.get::<_, i32>(0), row.get::<_, String>(1))));
        }
        assert_eq!(actual, [(2, "two".into()), (3, "three".into()), (4, "four".into()), (5, "five".into())]);
        tx.commit().await.unwrap();
    }).await;
}

async fn relational_fixture(client: &Client) {
    client.batch_execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT); CREATE TABLE staff (id INTEGER PRIMARY KEY, dept_id INTEGER, score INTEGER); INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Support'), (3, 'Empty'); INSERT INTO staff VALUES (10, 1, 30), (11, 1, 20), (12, 2, NULL)").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_outer_join_typed_results() {
    with_client(async |client| {
        relational_fixture(client).await;
        let rows = client.query("SELECT d.name, s.id FROM departments d LEFT JOIN staff s ON d.id = s.dept_id ORDER BY d.id, s.id", &[]).await.unwrap();
        let actual: Vec<_> = rows.iter().map(|r| (r.get::<_, String>(0), r.get::<_, Option<i32>>(1))).collect();
        assert_eq!(actual, [("Engineering".into(), Some(10)), ("Engineering".into(), Some(11)), ("Support".into(), Some(12)), ("Empty".into(), None)]);
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_group_by_and_having() {
    with_client(async |client| {
        relational_fixture(client).await;
        let row = client.query_one("SELECT dept_id, COUNT(*) AS n, SUM(score) AS total FROM staff GROUP BY dept_id HAVING COUNT(*) > 1", &[]).await.unwrap();
        assert_eq!(row.get::<_, i32>(0), 1);
        assert_eq!(row.get::<_, i64>(1), 2);
        assert_eq!(row.get::<_, i64>(2), 50);
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_parameterized_cte() {
    with_client(async |client| {
        relational_fixture(client).await;
        let rows = client.query_typed("WITH selected AS (SELECT id FROM staff WHERE score >= $1) SELECT id FROM selected ORDER BY id", &[(&20_i32, Type::INT4)]).await.unwrap();
        assert_eq!(rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), [10, 11]);
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_null_predicates() {
    with_client(async |client| {
        relational_fixture(client).await;
        let rows = client
            .query(
                "SELECT id FROM staff WHERE score NOT IN (20, NULL) ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert!(
            rows.is_empty(),
            "NOT IN with NULL is unknown for nonmatching values"
        );
        let row = client
            .query_one("SELECT id FROM staff WHERE score IS NULL", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>(0), 12);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_stream_api_round_trip() {
    with_client(async |client| {
        client
            .batch_execute("CREATE TABLE copy_rows (id INTEGER PRIMARY KEY, label TEXT)")
            .await
            .unwrap();
        let sink = client
            .copy_in("COPY copy_rows (id, label) FROM STDIN")
            .await
            .unwrap();
        tokio::pin!(sink);
        // Split a row across chunks; include escaped tab and SQL NULL.
        sink.send(Bytes::from_static(b"1\tone\n2\ttwo\\t"))
            .await
            .unwrap();
        sink.send(Bytes::from_static(b"parts\n3\t\\N\n"))
            .await
            .unwrap();
        assert_eq!(sink.finish().await.unwrap(), 3);
        let rows = client
            .query("SELECT id, label FROM copy_rows ORDER BY id", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get::<_, String>(1), "one");
        assert_eq!(rows[1].get::<_, String>(1), "two\tparts");
        assert_eq!(rows[2].get::<_, Option<String>>(1), None);
        let stream = client
            .copy_out("COPY copy_rows (id, label) TO STDOUT")
            .await
            .unwrap();
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        let output: Vec<u8> = chunks.into_iter().flatten().collect();
        // COPY table output has no ordering guarantee.
        let text = String::from_utf8(output).unwrap();
        let mut lines: Vec<_> = text.lines().collect();
        lines.sort_unstable();
        assert_eq!(lines, ["1\tone", "2\ttwo\\tparts", "3\t\\N"]);
    })
    .await;
}
