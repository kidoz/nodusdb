use bytes::Bytes;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use nodus_testkit::TestServer;
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_postgres::types::Type;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use uuid::Uuid;

async fn write_startup(stream: &mut TcpStream) {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608_i32.to_be_bytes());
    body.extend_from_slice(b"user\0nodus\0database\0default\0\0");

    let len = (body.len() + 4) as i32;
    stream.write_all(&len.to_be_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
}

async fn write_frontend_message(stream: &mut TcpStream, message_type: u8, body: &[u8]) {
    stream.write_all(&[message_type]).await.unwrap();
    stream
        .write_all(&((body.len() + 4) as i32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(body).await.unwrap();
}

async fn read_backend_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut message_type = [0_u8; 1];
    timeout(Duration::from_secs(5), stream.read_exact(&mut message_type))
        .await
        .expect("timed out reading backend message type")
        .unwrap();
    let mut len = [0_u8; 4];
    timeout(Duration::from_secs(5), stream.read_exact(&mut len))
        .await
        .expect("timed out reading backend message length")
        .unwrap();
    let body_len = i32::from_be_bytes(len) as usize - 4;
    let mut body = vec![0_u8; body_len];
    timeout(Duration::from_secs(5), stream.read_exact(&mut body))
        .await
        .expect("timed out reading backend message body")
        .unwrap();
    (message_type[0], body)
}

fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    for part in parts {
        mac.update(part);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&mac.finalize().into_bytes());
    out
}

fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&h.finalize());
    out
}

fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut u = hmac_sha256(password, &[salt, &1u32.to_be_bytes()]);
    let mut dk = u;
    for _ in 1..iterations {
        u = hmac_sha256(password, &[&u]);
        for i in 0..32 {
            dk[i] ^= u[i];
        }
    }
    dk
}

/// Drives a raw-socket SCRAM-SHA-256 client handshake to authenticate as `nodus`,
/// mirroring what a real PostgreSQL driver does, so the wire-level tests below
/// can use a plain `TcpStream` while the server speaks SASL.
async fn scram_authenticate(stream: &mut TcpStream, password: &str) {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;

    // Expect AuthenticationSASL (R, code 10).
    let (mt, body) = read_backend_message(stream).await;
    assert_eq!(mt, b'R');
    assert_eq!(i32::from_be_bytes([body[0], body[1], body[2], body[3]]), 10);

    // client-first. PostgreSQL takes the username from the startup packet, so the
    // SCRAM `n=` field is empty.
    let client_nonce = Uuid::new_v4().simple().to_string();
    let client_first_bare = format!("n=,r={client_nonce}");
    let client_first = format!("n,,{client_first_bare}");
    let mut init = Vec::new();
    init.extend_from_slice(b"SCRAM-SHA-256\0");
    init.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
    init.extend_from_slice(client_first.as_bytes());
    write_frontend_message(stream, b'p', &init).await;

    // Expect SASLContinue (R, code 11) carrying the server-first message.
    let (mt, body) = read_backend_message(stream).await;
    assert_eq!(mt, b'R');
    assert_eq!(i32::from_be_bytes([body[0], body[1], body[2], body[3]]), 11);
    let server_first = String::from_utf8(body[4..].to_vec()).unwrap();
    let mut combined = "";
    let mut salt_b64 = "";
    let mut iterations = 0u32;
    for field in server_first.split(',') {
        if let Some(v) = field.strip_prefix("r=") {
            combined = v;
        } else if let Some(v) = field.strip_prefix("s=") {
            salt_b64 = v;
        } else if let Some(v) = field.strip_prefix("i=") {
            iterations = v.parse().unwrap();
        }
    }
    let salt = b64.decode(salt_b64).unwrap();
    let salted = pbkdf2_sha256(password.as_bytes(), &salt, iterations);
    let client_key = hmac_sha256(&salted, &[b"Client Key"]);
    let stored_key = sha256(&client_key);
    let without_proof = format!("c=biws,r={combined}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let client_sig = hmac_sha256(&stored_key, &[auth_message.as_bytes()]);
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_sig[i];
    }
    let client_final = format!("{without_proof},p={}", b64.encode(proof));
    write_frontend_message(stream, b'p', client_final.as_bytes()).await;

    // Expect SASLFinal (R, code 12) then AuthenticationOk (R, code 0).
    let (mt, body) = read_backend_message(stream).await;
    assert_eq!(mt, b'R');
    assert_eq!(i32::from_be_bytes([body[0], body[1], body[2], body[3]]), 12);
    let (mt, body) = read_backend_message(stream).await;
    assert_eq!(mt, b'R');
    assert_eq!(i32::from_be_bytes([body[0], body[1], body[2], body[3]]), 0);
}

async fn open_raw_pgwire(server: &TestServer) -> TcpStream {
    let mut stream = TcpStream::connect(server.pgwire_addr).await.unwrap();
    write_startup(&mut stream).await;
    scram_authenticate(&mut stream, "nodus").await;
    // Drain the post-auth parameter status / backend key data up to ReadyForQuery.
    loop {
        let (message_type, _body) = read_backend_message(&mut stream).await;
        if message_type == b'Z' {
            return stream;
        }
    }
}

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

#[tokio::test(flavor = "multi_thread")]
async fn test_wire_simple_query_batch_returns_each_result_before_ready() {
    let server = TestServer::start().await.expect("server starts");
    let mut stream = open_raw_pgwire(&server).await;

    write_frontend_message(&mut stream, b'Q', b"SELECT 1; SELECT 2;\0").await;

    let mut sequence = Vec::new();
    loop {
        let (message_type, _) = read_backend_message(&mut stream).await;
        sequence.push(message_type);
        if message_type == b'Z' {
            break;
        }
    }

    assert_eq!(sequence, [b'T', b'D', b'C', b'T', b'D', b'C', b'Z']);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wire_binary_copy_response_reports_format_and_columns() {
    let server = TestServer::start().await.expect("server starts");
    let mut stream = open_raw_pgwire(&server).await;

    write_frontend_message(
        &mut stream,
        b'Q',
        b"COPY copy_binary_wire (id, name) FROM STDIN (FORMAT BINARY)\0",
    )
    .await;

    let (message_type, body) = read_backend_message(&mut stream).await;
    assert_eq!(message_type, b'G');
    assert_eq!(body[0], 1, "COPY format should be binary");
    let columns = i16::from_be_bytes([body[1], body[2]]);
    assert_eq!(columns, 2);
    assert_eq!(i16::from_be_bytes([body[3], body[4]]), 1);
    assert_eq!(i16::from_be_bytes([body[5], body[6]]), 1);

    write_frontend_message(&mut stream, b'c', &[]).await;
    let (complete, _) = read_backend_message(&mut stream).await;
    let (ready, _) = read_backend_message(&mut stream).await;
    assert_eq!((complete, ready), (b'C', b'Z'));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_extended_portal_row_limits_suspend_and_resume() {
    let server = TestServer::start().await.expect("server starts");
    let mut client = connect(&server).await;

    client
        .simple_query("CREATE TABLE portal_rows (id INT PRIMARY KEY);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO portal_rows (id) VALUES (1), (2), (3);")
        .await
        .unwrap();

    let tx = client.transaction().await.unwrap();
    let stmt = tx
        .prepare("SELECT id FROM portal_rows ORDER BY id;")
        .await
        .unwrap();
    let portal = tx.bind(&stmt, &[]).await.unwrap();

    let first = tx.query_portal(&portal, 2).await.unwrap();
    assert_eq!(first.len(), 2);
    let first_id: i32 = first[0].get(0);
    let second_id: i32 = first[1].get(0);
    assert_eq!((first_id, second_id), (1, 2));

    let second = tx.query_portal(&portal, 2).await.unwrap();
    assert_eq!(second.len(), 1);
    let third_id: i32 = second[0].get(0);
    assert_eq!(third_id, 3);

    tx.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cancel_request_is_accepted_without_killing_idle_session() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client.cancel_token().cancel_query(NoTls).await.unwrap();

    let rows = client.query("SELECT 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let value: i32 = rows[0].get(0);
    assert_eq!(value, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_statement_timeout_returns_query_cancelled_and_keeps_session_alive() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("SET statement_timeout = 1")
        .await
        .unwrap();
    let err = client
        .simple_query("SELECT pg_sleep(1)")
        .await
        .expect_err("pg_sleep should exceed statement_timeout");
    let db_err = err.as_db_error().expect("expected database error");
    assert_eq!(
        db_err.code(),
        &tokio_postgres::error::SqlState::QUERY_CANCELED
    );

    client
        .simple_query("SET statement_timeout = 0")
        .await
        .unwrap();
    let rows = client.query("SELECT 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_savepoints_rollback_release_and_read_your_writes() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE TABLE savepoint_rows (id INT PRIMARY KEY, name TEXT);")
        .await
        .unwrap();

    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("INSERT INTO savepoint_rows (id, name) VALUES (1, 'kept');")
        .await
        .unwrap();
    let rows = client
        .query("SELECT name FROM savepoint_rows WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "transaction should read its own insert");
    let name: &str = rows[0].get(0);
    assert_eq!(name, "kept");

    client.simple_query("SAVEPOINT sp1").await.unwrap();
    client
        .simple_query("INSERT INTO savepoint_rows (id, name) VALUES (2, 'rolled_back');")
        .await
        .unwrap();
    client
        .simple_query("ROLLBACK TO SAVEPOINT sp1")
        .await
        .unwrap();
    client.simple_query("RELEASE SAVEPOINT sp1").await.unwrap();
    client.simple_query("COMMIT").await.unwrap();

    let rows = client
        .query("SELECT id, name FROM savepoint_rows ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    let name: &str = rows[0].get(1);
    assert_eq!((id, name), (1, "kept"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_returning_reports_declared_types_for_generated_key_paths() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE TABLE returning_keys (id INT PRIMARY KEY, label TEXT);")
        .await
        .unwrap();
    let stmt = client
        .prepare_typed(
            "INSERT INTO returning_keys (id, label) VALUES ($1, $2) RETURNING id, label;",
            &[Type::INT4, Type::TEXT],
        )
        .await
        .unwrap();
    let columns = stmt.columns();
    assert_eq!(columns[0].type_(), &Type::INT4);
    assert_eq!(columns[1].type_(), &Type::TEXT);

    let rows = client.query(&stmt, &[&7_i32, &"seven"]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    let label: &str = rows[0].get(1);
    assert_eq!((id, label), (7, "seven"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ssl_and_gss_negotiation_are_refused_cleanly() {
    let server = TestServer::start().await.expect("server starts");

    for magic in [80877103_i32, 80877104_i32] {
        let mut stream = TcpStream::connect(server.pgwire_addr).await.unwrap();
        let mut packet = Vec::with_capacity(8);
        packet.extend_from_slice(&8_i32.to_be_bytes());
        packet.extend_from_slice(&magic.to_be_bytes());
        stream.write_all(&packet).await.unwrap();

        let mut response = [0_u8; 1];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(response, [b'N']);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_copy_protocol_in_and_out_complete_cleanly() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .batch_execute("CREATE TABLE copy_sink (id int, label text)")
        .await
        .unwrap();

    let mut sink = Box::pin(
        client
            .copy_in::<_, Bytes>("COPY copy_sink (id, label) FROM STDIN")
            .await
            .unwrap(),
    );
    sink.as_mut()
        .send(Bytes::from_static(b"1\talpha\n2\tbeta\n"))
        .await
        .unwrap();
    let copied = sink.as_mut().finish().await.unwrap();
    assert_eq!(copied, 2);

    // The rows must actually persist, not just be counted on the wire.
    let rows = client
        .query("SELECT id, label FROM copy_sink ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let label: String = rows[0].get("label");
    assert_eq!(label, "alpha");

    let mut out = Box::pin(client.copy_out("COPY copy_sink TO STDOUT").await.unwrap());
    assert!(out.next().await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_startup_parameters_and_ready_state_basics() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    let rows = client.query("SHOW search_path", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let search_path: &str = rows[0].get(0);
    assert_eq!(search_path, "public");

    client.simple_query("BEGIN").await.unwrap();
    client.simple_query("SELECT 1").await.unwrap();
    client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_declared_type_oids_binary_values_and_nulls() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query(
            "CREATE TABLE driver_types (
                id INT PRIMARY KEY,
                uid UUID,
                raw BYTEA,
                event_date DATE,
                event_time TIME,
                event_ts TIMESTAMP,
                event_tstz TIMESTAMPTZ,
                payload JSONB,
                tags TEXT[],
                nums INT[],
                obj_oid OID,
                type_name NAME,
                amount NUMERIC(12, 2),
                reg REGTYPE,
                maybe_int INT
            );",
        )
        .await
        .unwrap();

    let uid = Uuid::parse_str("7b1eb2d8-c2d1-4d3a-9cf6-55f1436cfe6f").unwrap();
    let raw = vec![0xde, 0xad, 0xbe, 0xef];
    let event_date = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();
    let event_time = NaiveTime::from_hms_micro_opt(12, 34, 56, 789).unwrap();
    let event_ts = NaiveDateTime::new(event_date, event_time);
    let event_tstz = Utc.with_ymd_and_hms(2026, 6, 15, 9, 34, 56).unwrap();
    let payload = json!({"user": "alice", "active": true, "clicks": 5});
    let tags = vec!["login".to_string(), "signup".to_string()];
    let nums = vec![10_i32, 20_i32, 30_i32];
    let obj_oid = 23_u32;
    let type_name = "driver_type".to_string();

    let insert = client
        .prepare_typed(
            "INSERT INTO driver_types
             (id, uid, raw, event_date, event_time, event_ts, event_tstz, payload, tags, nums, obj_oid, type_name, amount, reg, maybe_int)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 42.50, 'int4', $13);",
            &[
                Type::INT4,
                Type::UUID,
                Type::BYTEA,
                Type::DATE,
                Type::TIME,
                Type::TIMESTAMP,
                Type::TIMESTAMPTZ,
                Type::JSONB,
                Type::TEXT_ARRAY,
                Type::INT4_ARRAY,
                Type::OID,
                Type::NAME,
                Type::INT4,
            ],
        )
        .await
        .unwrap();

    client
        .execute(
            &insert,
            &[
                &1_i32,
                &uid,
                &raw,
                &event_date,
                &event_time,
                &event_ts,
                &event_tstz,
                &payload,
                &tags,
                &nums,
                &obj_oid,
                &type_name,
                &Option::<i32>::None,
            ],
        )
        .await
        .unwrap();

    let select = client
        .prepare(
            "SELECT uid, raw, event_date, event_time, event_ts, event_tstz, payload, tags, nums, obj_oid, type_name, maybe_int
             FROM driver_types WHERE id = 1;",
        )
        .await
        .unwrap();

    let column_types = select
        .columns()
        .iter()
        .map(|c| c.type_().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        column_types,
        vec![
            Type::UUID,
            Type::BYTEA,
            Type::DATE,
            Type::TIME,
            Type::TIMESTAMP,
            Type::TIMESTAMPTZ,
            Type::JSONB,
            Type::TEXT_ARRAY,
            Type::INT4_ARRAY,
            Type::OID,
            Type::NAME,
            Type::INT4,
        ]
    );

    let row = client.query_one(&select, &[]).await.unwrap();
    assert_eq!(row.get::<_, Uuid>(0), uid);
    assert_eq!(row.get::<_, Vec<u8>>(1), raw);
    assert_eq!(row.get::<_, NaiveDate>(2), event_date);
    assert_eq!(row.get::<_, NaiveTime>(3), event_time);
    assert_eq!(row.get::<_, NaiveDateTime>(4), event_ts);
    assert_eq!(row.get::<_, chrono::DateTime<Utc>>(5), event_tstz);
    assert_eq!(row.get::<_, serde_json::Value>(6), payload);
    assert_eq!(row.get::<_, Vec<String>>(7), tags);
    assert_eq!(row.get::<_, Vec<i32>>(8), nums);
    assert_eq!(row.get::<_, u32>(9), obj_oid);
    assert_eq!(row.get::<_, String>(10), type_name);
    assert_eq!(row.get::<_, Option<i32>>(11), None);

    let meta = client
        .prepare("SELECT amount, reg FROM driver_types WHERE id = 1;")
        .await
        .unwrap();
    assert_eq!(meta.columns()[0].type_(), &Type::NUMERIC);
    assert_eq!(meta.columns()[0].type_modifier(), ((12 << 16) | 2) + 4);
    assert_eq!(meta.columns()[1].type_(), &Type::REGTYPE);
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
async fn test_arbitrary_table_sql() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO products (sku, name, price) VALUES ('s1', 'Widget', '9.99');")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO products (sku, name, price) VALUES ('s2', 'Gadget', '19.99');")
        .await
        .unwrap();

    // SELECT * returns every row with all columns in table order.
    let msgs = client
        .simple_query("SELECT * FROM products;")
        .await
        .unwrap();
    assert_eq!(rows_of(&msgs).len(), 2);

    // Projection + filter on the primary key.
    let msgs = client
        .simple_query("SELECT name, price FROM products WHERE sku = 's2';")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), "Gadget");
    assert_eq!(rows[0].get(1).unwrap(), "19.99");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_where_order_limit() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE TABLE scores (id INT PRIMARY KEY, points INT);")
        .await
        .unwrap();
    for (id, pts) in [("1", "30"), ("2", "10"), ("3", "50"), ("4", "20")] {
        client
            .simple_query(&format!(
                "INSERT INTO scores (id, points) VALUES ({id}, {pts});"
            ))
            .await
            .unwrap();
    }

    // Comparison + ORDER BY DESC + LIMIT, numeric (not lexical) ordering.
    let msgs = client
        .simple_query("SELECT id FROM scores WHERE points > 15 ORDER BY points DESC LIMIT 2;")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0).unwrap(), "3"); // 50
    assert_eq!(rows[1].get(0).unwrap(), "1"); // 30

    // Conjunction (AND) of predicates.
    let msgs = client
        .simple_query("SELECT id FROM scores WHERE points >= 10 AND id < 3;")
        .await
        .unwrap();
    let mut ids: Vec<String> = rows_of(&msgs)
        .iter()
        .map(|r| r.get(0).unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["1", "2"]);

    // UPDATE then DELETE through the wire.
    let msgs = client
        .simple_query("UPDATE scores SET points = 0 WHERE id = 2;")
        .await
        .unwrap();
    assert!(matches!(&msgs[0], SimpleQueryMessage::CommandComplete(_)));
    client
        .simple_query("DELETE FROM scores WHERE id = 4;")
        .await
        .unwrap();
    let msgs = client.simple_query("SELECT id FROM scores;").await.unwrap();
    assert_eq!(rows_of(&msgs).len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pgwire_transactions_and_drop() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE TABLE tx_test (id INT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();

    // Begin, insert, rollback
    client.simple_query("BEGIN;").await.unwrap();
    client
        .simple_query("INSERT INTO tx_test (id, val) VALUES (1, 'kept'), (2, 'lost');")
        .await
        .unwrap();
    client.simple_query("ROLLBACK;").await.unwrap();

    // Table should be empty
    let msgs = client.simple_query("SELECT * FROM tx_test;").await.unwrap();
    assert_eq!(rows_of(&msgs).len(), 0);

    // Begin, insert, commit
    client.simple_query("BEGIN;").await.unwrap();
    client
        .simple_query("INSERT INTO tx_test (id, val) VALUES (1, 'kept');")
        .await
        .unwrap();
    client.simple_query("COMMIT;").await.unwrap();

    // Table should have 1 row
    let msgs = client.simple_query("SELECT * FROM tx_test;").await.unwrap();
    assert_eq!(rows_of(&msgs).len(), 1);
}
#[tokio::test(flavor = "multi_thread")]
async fn test_pgwire_queries() {
    let server = TestServer::start().await.expect("Failed to start server");
    let mut is_up = false;
    let mut client_opt = None;
    let conn_str = format!(
        "host={} port={} user=nodus password=nodus dbname=default",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port()
    );
    for _ in 0..30 {
        if let Ok((client, connection)) = tokio_postgres::connect(&conn_str, NoTls).await {
            is_up = true;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            client_opt = Some(client);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(is_up, "PGWire server did not start in time");
    let client = client_opt.unwrap();

    // SELECT 1
    let rows = client.simple_query("SELECT 1;").await.unwrap();
    println!("Received rows: {:?}", rows);
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert_eq!(row.get(0).unwrap(), "1");
    } else {
        panic!("Expected a row");
    }

    // SELECT 'hello'
    let rows = client.simple_query("SELECT 'hello';").await.unwrap();
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert_eq!(row.get(0).unwrap(), "hello");
    } else {
        panic!("Expected a row");
    }

    // SELECT version()
    let rows = client.simple_query("SELECT version();").await.unwrap();
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert!(row.get(0).unwrap().contains("PostgreSQL"));
    } else {
        panic!("Expected a row");
    }

    // SHOW search_path
    let rows = client.simple_query("SHOW search_path;").await.unwrap();
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert_eq!(row.get(0).unwrap(), "public");
    } else {
        panic!("Expected a row");
    }

    // Execute the vertical slice pipeline
    client
        .simple_query("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT NOT NULL);")
        .await
        .unwrap();
    client.simple_query("BEGIN;").await.unwrap();
    client
        .simple_query("INSERT INTO users (id, name) VALUES ('u1', 'alice');")
        .await
        .unwrap();
    client.simple_query("COMMIT;").await.unwrap();

    let rows = client
        .simple_query("SELECT id, name FROM users WHERE id = 'u1';")
        .await
        .unwrap();
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert_eq!(row.get(0).unwrap(), "u1");
        assert_eq!(row.get(1).unwrap(), "alice");
    } else {
        panic!("Expected a row");
    }
}

/// Helper: run a simple query and return the first column of the first data row.
async fn show_value(client: &tokio_postgres::Client, sql: &str) -> String {
    let rows = client.simple_query(sql).await.unwrap();
    for msg in &rows {
        if let SimpleQueryMessage::Row(row) = msg {
            return row.get(0).unwrap_or_default().to_string();
        }
    }
    panic!("no row returned for `{sql}`");
}

/// `SET` must persist in the session and `SHOW` must reflect it, including the
/// built-in defaults and `= DEFAULT` reset — the session-variable round-trip
/// drivers and ORMs depend on.
#[tokio::test(flavor = "multi_thread")]
async fn test_session_variables_persist_and_reset() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    // Defaults before any SET.
    assert_eq!(show_value(&client, "SHOW search_path").await, "public");
    assert_eq!(show_value(&client, "SHOW client_encoding").await, "UTF8");
    assert_eq!(show_value(&client, "SHOW application_name").await, "");

    // SET persists and SHOW reflects (quotes stripped from the reported value).
    client
        .simple_query("SET application_name TO 'nodus_app'")
        .await
        .unwrap();
    assert_eq!(
        show_value(&client, "SHOW application_name").await,
        "nodus_app"
    );

    client
        .simple_query("SET search_path TO myschema")
        .await
        .unwrap();
    assert_eq!(show_value(&client, "SHOW search_path").await, "myschema");

    // SET ... = DEFAULT reverts to the built-in default.
    client
        .simple_query("SET search_path = DEFAULT")
        .await
        .unwrap();
    assert_eq!(show_value(&client, "SHOW search_path").await, "public");

    // The SQL-standard `SET TIME ZONE <x>` spelling persists like `SET timezone`.
    client
        .simple_query("SET TIME ZONE 'America/New_York'")
        .await
        .unwrap();
    assert_eq!(
        show_value(&client, "SHOW TimeZone").await,
        "America/New_York"
    );
    client.simple_query("SET TIME ZONE DEFAULT").await.unwrap();
    assert_eq!(show_value(&client, "SHOW TimeZone").await, "UTC");
}

/// Session variables are per-connection: one client's `SET` must not leak into
/// another's view.
#[tokio::test(flavor = "multi_thread")]
async fn test_session_variables_are_isolated_per_connection() {
    let server = TestServer::start().await.expect("server starts");
    let client_a = connect(&server).await;
    let client_b = connect(&server).await;

    client_a
        .simple_query("SET application_name TO 'only_a'")
        .await
        .unwrap();

    assert_eq!(
        show_value(&client_a, "SHOW application_name").await,
        "only_a"
    );
    assert_eq!(show_value(&client_b, "SHOW application_name").await, "");
}

/// A successful `SET` of a GUC_REPORT variable emits a `ParameterStatus` ('S')
/// message before `ReadyForQuery` ('Z'), as PostgreSQL does, so drivers can
/// track the new value. Non-reportable variables do not.
#[tokio::test(flavor = "multi_thread")]
async fn test_reportable_set_emits_parameter_status() {
    let server = TestServer::start().await.expect("server starts");
    let mut stream = open_raw_pgwire(&server).await;

    write_frontend_message(&mut stream, b'Q', b"SET TimeZone TO 'America/New_York'\0").await;

    let mut saw_parameter_status = false;
    loop {
        let (message_type, body) = read_backend_message(&mut stream).await;
        if message_type == b'S' {
            saw_parameter_status = true;
            assert!(
                body.windows(b"TimeZone\0".len())
                    .any(|w| w == b"TimeZone\0"),
                "ParameterStatus should name TimeZone"
            );
            assert!(
                body.windows(b"America/New_York\0".len())
                    .any(|w| w == b"America/New_York\0"),
                "ParameterStatus should carry the new value"
            );
        }
        if message_type == b'Z' {
            break;
        }
    }
    assert!(
        saw_parameter_status,
        "expected a ParameterStatus after SET TimeZone"
    );

    // A non-reportable variable must NOT trigger ParameterStatus.
    write_frontend_message(&mut stream, b'Q', b"SET statement_timeout TO 0\0").await;
    loop {
        let (message_type, _) = read_backend_message(&mut stream).await;
        assert_ne!(
            message_type, b'S',
            "statement_timeout is not a GUC_REPORT variable"
        );
        if message_type == b'Z' {
            break;
        }
    }
}
