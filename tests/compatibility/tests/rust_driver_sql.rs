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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_in_honours_header_delimiter_and_null_options() {
    // Unsupported options are rejected; see the nodus_import decoder tests.
    with_client(async |client| {
        client
            .batch_execute("CREATE TABLE copy_opts (id INTEGER PRIMARY KEY, label TEXT)")
            .await
            .unwrap();
        let sink = client
            .copy_in("COPY copy_opts (id, label) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'NA')")
            .await
            .unwrap();
        tokio::pin!(sink);
        sink.send(Bytes::from_static(b"id;label\n1;\"a;b\"\n2;NA\n3;\"\"\n"))
            .await
            .unwrap();
        assert_eq!(sink.finish().await.unwrap(), 3);
        let rows = client
            .query("SELECT id, label FROM copy_opts ORDER BY id", &[])
            .await
            .unwrap();
        let labels: Vec<Option<String>> = rows.iter().map(|r| r.get(1)).collect();
        assert_eq!(labels, [Some("a;b".to_string()), None, Some(String::new())]);

    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inferred_bindings_execute_and_ignore_quoted_placeholders() {
    with_client(async |client| {
        client
            .batch_execute("CREATE TABLE inferred (id INTEGER PRIMARY KEY, label TEXT)")
            .await
            .unwrap();
        let insert = client
            .prepare("INSERT INTO inferred VALUES ($1, $2)")
            .await
            .unwrap();
        assert_eq!(insert.params(), &[Type::INT4, Type::TEXT]);
        assert_eq!(
            client
                .execute(&insert, &[&1_i32, &"literal $99"])
                .await
                .unwrap(),
            1
        );
        let select = client
            .prepare("SELECT label FROM inferred WHERE id = $1 /* $200 */")
            .await
            .unwrap();
        assert_eq!(select.params(), &[Type::INT4]);
        assert_eq!(
            client
                .query_one(&select, &[&1_i32])
                .await
                .unwrap()
                .get::<_, String>(0),
            "literal $99"
        );
        assert!(
            client
                .prepare("SELECT '$123'")
                .await
                .unwrap()
                .params()
                .is_empty()
        );
        let update = client
            .prepare("UPDATE inferred SET label = $1 WHERE id = $2")
            .await
            .unwrap();
        assert_eq!(update.params(), &[Type::TEXT, Type::INT4]);
        assert_eq!(
            client
                .execute(&update, &[&"updated", &1_i32])
                .await
                .unwrap(),
            1
        );
        let delete = client
            .prepare("DELETE FROM inferred WHERE id = $1")
            .await
            .unwrap();
        assert_eq!(client.execute(&delete, &[&1_i32]).await.unwrap(), 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_transaction_commit_aborts_writes_in_both_protocols() {
    with_client(async |client| {
        client
            .batch_execute("CREATE TABLE aborted (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        client.batch_execute("BEGIN").await.unwrap();
        let error = client
            .copy_out("COPY missing_copy_table TO STDOUT")
            .await
            .err()
            .expect("missing COPY table fails");
        assert_eq!(error.code(), Some(&SqlState::UNDEFINED_TABLE));
        let error = client
            .query("SELECT id FROM aborted", &[])
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
        client.batch_execute("ROLLBACK").await.unwrap();
        for simple in [false, true] {
            client
                .batch_execute("BEGIN; INSERT INTO aborted VALUES (1)")
                .await
                .unwrap();
            let error = client
                .execute("INSERT INTO aborted VALUES (1)", &[])
                .await
                .unwrap_err();
            assert_eq!(error.code(), Some(&SqlState::UNIQUE_VIOLATION));
            if simple {
                let error = client
                    .simple_query("SELECT id FROM aborted")
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
                client.batch_execute("COMMIT").await.unwrap();
            } else {
                let error = client
                    .query("SELECT id FROM aborted", &[])
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
                client.execute("COMMIT", &[]).await.unwrap();
            }
            assert!(
                client
                    .query("SELECT id FROM aborted", &[])
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_all_portal_stays_exhausted() {
    with_client(async |client| {
        client
            .batch_execute(
                "CREATE TABLE fetch_all (id INTEGER); INSERT INTO fetch_all VALUES (1), (2)",
            )
            .await
            .unwrap();
        let tx = client.transaction().await.unwrap();
        let statement = tx
            .prepare("SELECT id FROM fetch_all ORDER BY id")
            .await
            .unwrap();
        let portal = tx.bind(&statement, &[]).await.unwrap();
        assert_eq!(tx.query_portal(&portal, 0).await.unwrap().len(), 2);
        assert!(tx.query_portal(&portal, 0).await.unwrap().is_empty());
        tx.commit().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_aggregate_result_metadata_is_stable() {
    with_client(async |client| {
        client
            .batch_execute("CREATE TABLE aggregates (n INTEGER)")
            .await
            .unwrap();
        let statement = client
            .prepare("SELECT COUNT(*), SUM(n) FROM aggregates")
            .await
            .unwrap();
        assert_eq!(
            statement
                .columns()
                .iter()
                .map(|c| c.type_().clone())
                .collect::<Vec<_>>(),
            [Type::INT8, Type::INT8]
        );
        let row = client.query_one(&statement, &[]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
        assert_eq!(row.get::<_, Option<i64>>(1), None);
        client
            .execute("INSERT INTO aggregates VALUES (20), (30)", &[])
            .await
            .unwrap();
        let row = client.query_one(&statement, &[]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), 2);
        assert_eq!(row.get::<_, i64>(1), 50);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_and_csv_copy_out_preserve_nulls_and_empty_strings() {
    with_client(async |client| {
        client.batch_execute("CREATE TABLE exports (id INTEGER PRIMARY KEY, label TEXT); INSERT INTO exports VALUES (1, 'quoted'), (2, ''), (3, NULL)").await.unwrap();
        let stream = client.copy_out("COPY (SELECT id, label FROM exports ORDER BY id) TO STDOUT (FORMAT BINARY)").await.unwrap();
        let stream = tokio_postgres::binary_copy::BinaryCopyOutStream::new(stream, &[Type::INT4, Type::TEXT]);
        tokio::pin!(stream);
        let mut rows = Vec::new();
        while let Some(row) = stream.try_next().await.unwrap() {
            rows.push((row.get::<i32>(0), row.get::<Option<String>>(1)));
        }
        assert_eq!(rows, [(1, Some("quoted".into())), (2, Some("".into())), (3, None)]);
        let empty = client.copy_out("COPY (SELECT id, label FROM exports WHERE id < 0) TO STDOUT (FORMAT BINARY)").await.unwrap();
        let empty = tokio_postgres::binary_copy::BinaryCopyOutStream::new(empty, &[Type::INT4, Type::TEXT]);
        tokio::pin!(empty);
        assert!(empty.try_next().await.unwrap().is_none());
        let stream = client.copy_out("COPY (SELECT id, label FROM exports ORDER BY id) TO STDOUT (FORMAT CSV, HEADER)").await.unwrap();
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        let output: Vec<_> = chunks.into_iter().flatten().collect();
        // CSV permits both quoted and unquoted ordinary values. Decode it
        // rather than requiring PostgreSQL and NodusDB to choose identical quoting.
        let text = String::from_utf8(output).unwrap();
        let csv = nodus_import::CopySpec::new("exports", vec![], nodus_import::CopyFormat::Csv);
        let cells = nodus_import::decode_rows(&text, &csv).unwrap();
        assert_eq!(cells.len(), 4);
        assert_eq!(cells[0], [nodus_import::Cell::Text("id".into()), nodus_import::Cell::Text("label".into())]);
        assert_eq!(cells[2][1], nodus_import::Cell::Text("".into()));
        assert_eq!(cells[3][1], nodus_import::Cell::Null);
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_list_names_and_types_follow_postgres() {
    with_client(async |client| {
        client
            .batch_execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, price NUMERIC(10,2)); \
                 INSERT INTO items VALUES (1, 'a', 1.50), (2, NULL, 2.00)",
            )
            .await
            .unwrap();
        let cases: &[(&str, &[(&str, Type)])] = &[
            (
                "SELECT 1, 2147483648, 'a', true",
                &[
                    ("?column?", Type::INT4),
                    ("?column?", Type::INT8),
                    ("?column?", Type::TEXT),
                    ("?column?", Type::BOOL),
                ],
            ),
            (
                "SELECT now(), current_date, current_user, version(), gen_random_uuid()",
                &[
                    ("now", Type::TIMESTAMPTZ),
                    ("current_date", Type::DATE),
                    ("current_user", Type::NAME),
                    ("version", Type::TEXT),
                    ("gen_random_uuid", Type::UUID),
                ],
            ),
            (
                "SELECT 1::bigint, 'x'::text, '2024-01-01'::date, '{}'::jsonb",
                &[
                    ("int8", Type::INT8),
                    ("text", Type::TEXT),
                    ("date", Type::DATE),
                    ("jsonb", Type::JSONB),
                ],
            ),
            (
                "SELECT length('a'), upper('a'), md5('a'), random(), (SELECT 1) AS one",
                &[
                    ("length", Type::INT4),
                    ("upper", Type::TEXT),
                    ("md5", Type::TEXT),
                    ("random", Type::FLOAT8),
                    ("one", Type::INT4),
                ],
            ),
            (
                "SELECT id, upper(label), id + 1, label AS l, count(*) OVER () FROM items",
                &[
                    ("id", Type::INT4),
                    ("upper", Type::TEXT),
                    ("?column?", Type::INT4),
                    ("l", Type::TEXT),
                    ("count", Type::INT8),
                ],
            ),
            (
                "SELECT count(*), sum(id) AS total, max(label) FROM items",
                &[
                    ("count", Type::INT8),
                    ("total", Type::INT8),
                    ("max", Type::TEXT),
                ],
            ),
        ];
        for (sql, expected) in cases {
            let statement = client.prepare(sql).await.unwrap();
            let described: Vec<(String, Type)> = statement
                .columns()
                .iter()
                .map(|c| (c.name().to_string(), c.type_().clone()))
                .collect();
            let expected: Vec<(String, Type)> = expected
                .iter()
                .map(|(name, ty)| (name.to_string(), ty.clone()))
                .collect();
            assert_eq!(described, expected, "{sql}");
            client.query(&statement, &[]).await.unwrap();
        }

        // Session functions see the session, and each item is computed on its own.
        let row = client
            .query_one(
                "SELECT current_user, now() = transaction_timestamp(), \
                 current_setting('DateStyle'), upper('a'), upper('b')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, &str>(0), "nodus");
        assert!(row.get::<_, bool>(1));
        assert_eq!(row.get::<_, &str>(2), "ISO, MDY");
        assert_eq!((row.get::<_, &str>(3), row.get::<_, &str>(4)), ("A", "B"));
        let rows = client
            .query(
                "SELECT id, (SELECT max(price) FROM items) AS top, ARRAY[id, id * 10] \
                 FROM items ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        let arrays: Vec<Vec<i32>> = rows.iter().map(|r| r.get(2)).collect();
        assert_eq!(arrays, [vec![1, 10], vec![2, 20]]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expression_errors_carry_postgres_sqlstates() {
    with_client(async |client| {
        client
            .batch_execute("CREATE TABLE nums (n INTEGER); INSERT INTO nums VALUES (1), (0)")
            .await
            .unwrap();
        let cases = [
            ("SELECT 1/0", SqlState::DIVISION_BY_ZERO),
            ("SELECT 10/n FROM nums", SqlState::DIVISION_BY_ZERO),
            (
                "SELECT n FROM nums WHERE 10/n > 1",
                SqlState::DIVISION_BY_ZERO,
            ),
            ("SELECT 'abc'::int", SqlState::INVALID_TEXT_REPRESENTATION),
            (
                "SELECT 'maybe'::boolean",
                SqlState::INVALID_TEXT_REPRESENTATION,
            ),
            (
                "SELECT DATE '2024-02-30'",
                SqlState::DATETIME_FIELD_OVERFLOW,
            ),
            (
                "SELECT sqrt(-1)",
                SqlState::INVALID_ARGUMENT_FOR_POWER_FUNCTION,
            ),
            ("SELECT ln(0)", SqlState::INVALID_ARGUMENT_FOR_LOG),
            ("SELECT no_such_function(1)", SqlState::UNDEFINED_FUNCTION),
            (
                "SELECT no_such_function(n) FROM nums",
                SqlState::UNDEFINED_FUNCTION,
            ),
            ("SELECT no_such_column", SqlState::UNDEFINED_COLUMN),
            (
                "SELECT (SELECT n FROM nums)",
                SqlState::CARDINALITY_VIOLATION,
            ),
            (
                "SELECT current_setting('no_such.setting')",
                SqlState::UNDEFINED_OBJECT,
            ),
        ];
        for (sql, state) in cases {
            let simple = client.batch_execute(sql).await.unwrap_err();
            assert_eq!(
                simple.code(),
                Some(&state),
                "simple protocol: {sql}: {simple:?}"
            );
            let extended = client.query(sql, &[]).await.unwrap_err();
            assert_eq!(
                extended.code(),
                Some(&state),
                "extended protocol: {sql}: {extended:?}"
            );
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn numeric_values_and_serial_keys_round_trip() {
    use rust_decimal::Decimal;
    use std::str::FromStr;
    with_client(async |client| {
        client
            .batch_execute(
                "CREATE TABLE prices (id SERIAL PRIMARY KEY, amount NUMERIC(10,2), \
                 qty INT, total NUMERIC GENERATED ALWAYS AS (amount * qty) STORED)",
            )
            .await
            .unwrap();
        // Exact decimals in both directions, binary on the wire.
        let insert = client
            .prepare("INSERT INTO prices (amount, qty) VALUES ($1, $2) RETURNING id, amount, total")
            .await
            .unwrap();
        assert_eq!(insert.params(), [Type::NUMERIC, Type::INT4]);
        let first = client
            .query_one(&insert, &[&Decimal::from_str("19.995").unwrap(), &3i32])
            .await
            .unwrap();
        assert_eq!(first.get::<_, i32>(0), 1);
        assert_eq!(first.get::<_, Decimal>(1).to_string(), "20.00");
        assert_eq!(first.get::<_, Decimal>(2).to_string(), "60.00");
        let second = client
            .query_one(&insert, &[&Decimal::from_str("0.10").unwrap(), &2i32])
            .await
            .unwrap();
        assert_eq!(second.get::<_, i32>(0), 2);
        let row = client
            .query_one(
                "SELECT sum(amount), avg(amount), 0.1 + 0.2, 10 / 4.0 FROM prices",
                &[],
            )
            .await
            .unwrap();
        let decimals: Vec<String> = (0..4)
            .map(|i| row.get::<_, Decimal>(i).to_string())
            .collect();
        assert_eq!(
            decimals,
            ["20.10", "10.0500000000000000", "0.3", "2.5000000000000000"]
        );
        // Out-of-range values are rejected rather than stored.
        let overflow = client
            .execute(
                "INSERT INTO prices (amount, qty) VALUES (123456789.1, 1)",
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(overflow.code(), Some(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE));
        let too_wide = client
            .execute(
                "INSERT INTO prices (amount, qty) VALUES (1, 2147483648)",
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(too_wide.code(), Some(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE));
    })
    .await;
}
