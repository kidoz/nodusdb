use nodus_testkit::TestServer;
use std::time::Duration;
use tokio_postgres::NoTls;

async fn connect(addr: &std::net::SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=nodus password=nodus dbname=default",
        addr.ip(),
        addr.port()
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
    panic!("could not connect to pgwire");
}

#[tokio::test(flavor = "multi_thread")]
async fn pgwire_sessions_are_per_connection_and_killable() {
    let server = TestServer::start().await.expect("server starts");

    // Two independent connections => two distinct registered sessions.
    let client_a = connect(&server.pgwire_addr).await;
    let client_b = connect(&server.pgwire_addr).await;
    client_a.simple_query("SELECT 1").await.unwrap();
    client_b.simple_query("SELECT 1").await.unwrap();

    let sessions = server.registry.list();
    assert_eq!(
        sessions.len(),
        2,
        "each connection registers its own session"
    );
    assert_ne!(sessions[0].session_id, sessions[1].session_id);
    // The most recent statement is visible to the inspector.
    assert!(sessions.iter().all(|s| s.current_query.is_some()));

    // Killing every session causes in-flight connections to fail their next query.
    for s in &sessions {
        assert!(server.registry.kill(&s.session_id));
    }
    assert!(
        client_a.simple_query("SELECT 1").await.is_err(),
        "a killed session must reject further queries"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_session_api_lists_and_kills() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server.pgwire_addr).await;
    client.simple_query("SELECT 1").await.unwrap();

    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);

    let sessions: serde_json::Value = http
        .get(format!("{base}/api/v1/sessions"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = sessions.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let id = arr[0]["session_id"].as_str().unwrap().to_string();
    // The authenticated principal is surfaced by name, not just its opaque id.
    assert_eq!(arr[0]["user_name"].as_str(), Some("nodus"));

    let killed: bool = http
        .post(format!("{base}/api/v1/sessions/{id}/kill"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(killed);
    assert!(server.registry.is_cancelled(&id));

    // Unknown id returns false.
    let missing: bool = http
        .post(format!("{base}/api/v1/sessions/does-not-exist/kill"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!missing);
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_audit_api_returns_events() {
    let server = TestServer::start().await.expect("server starts");
    // The seeded superuser runs a statement that triggers an authz decision.
    let client = connect(&server.pgwire_addr).await;
    client
        .simple_query("CREATE TABLE t (id UUID PRIMARY KEY, name TEXT NOT NULL);")
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);

    let all: serde_json::Value = http
        .get(format!("{base}/api/v1/audit"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!all.as_array().unwrap().is_empty(), "expected audit events");

    // Filter by result=Success returns only successful decisions.
    let success: serde_json::Value = http
        .get(format!("{base}/api/v1/audit?result=Success"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = success.as_array().unwrap();
    assert!(!arr.is_empty());
    assert!(arr.iter().all(|e| e["result"] == "Success"));
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_authz_explain_denies_unknown_principal() {
    let server = TestServer::start().await.expect("server starts");
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    let principal = "00000000-0000-0000-0000-000000000001";

    // System-level check for an unknown principal: deny-by-default with steps.
    let body: serde_json::Value = http
        .get(format!(
            "{base}/api/v1/authz/explain?principal={principal}&action=SELECT"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["is_allowed"], false);
    assert!(!body["steps"].as_array().unwrap().is_empty());

    // Unknown table is reported clearly.
    let body: serde_json::Value = http
        .get(format!(
            "{base}/api/v1/authz/explain?principal={principal}&action=SELECT&table=missing"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["is_allowed"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_backup_api_create_list_verify_restore() {
    let server = TestServer::start().await.expect("server starts");
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);

    let created: serde_json::Value = http
        .post(format!("{base}/api/v1/backups"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = created["backup_id"]
        .as_str()
        .expect("backup_id")
        .to_string();
    assert_eq!(created["status"], "Completed");

    let list: serde_json::Value = http
        .get(format!("{base}/api/v1/backups"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(list.as_array().unwrap().iter().any(|v| v == &id));

    let verified: serde_json::Value = http
        .post(format!("{base}/api/v1/backups/{id}/verify"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(verified["verified"], true);

    let restored: serde_json::Value = http
        .post(format!("{base}/api/v1/backups/{id}/restore"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // Backup now captures a catalog snapshot, the audit trail, and the KV dump.
    assert_eq!(restored["restored"], 3);
    let names = restored["objects"].as_array().unwrap();
    assert!(names.iter().any(|n| n == "catalog.json"));
    assert!(names.iter().any(|n| n == "kv_data.json"));
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_upgrade_api_drives_lifecycle() {
    let server = TestServer::start().await.expect("server starts");
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);

    // Idle initially.
    let st: serde_json::Value = http
        .get(format!("{base}/api/v1/upgrade"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(st["phase"], "Idle");

    let r = http
        .post(format!("{base}/api/v1/upgrade/start?target=0.2.0"))
        .send()
        .await
        .unwrap();
    println!("UPGRADE START RESPONSE: {:?}", r.text().await);
    // Single node: report it upgraded to reach ReadyToFinalize.
    let res = http
        .post(format!("{base}/api/v1/upgrade/node-upgraded?node=n1"))
        .send()
        .await
        .unwrap();
    println!("NODE UPGRADED STATUS: {}", res.status());
    let st: serde_json::Value = res.json().await.unwrap();
    assert_eq!(st["phase"], "ReadyToFinalize");

    let st: serde_json::Value = http
        .post(format!("{base}/api/v1/upgrade/finalize"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(st["phase"], "Finalized");
    // The gated feature is enabled only after finalization.
    assert_eq!(st["feature_gates"]["new_storage_format"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_shard_api_init_split() {
    let server = TestServer::start().await.expect("server starts");
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    let table = "11111111-1111-1111-1111-111111111111";

    let init: serde_json::Value = http
        .post(format!("{base}/api/v1/shards/{table}/init"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let shard_id = init["shard_id"].as_str().expect("shard_id").to_string();

    let map: serde_json::Value = http
        .get(format!("{base}/api/v1/shards/{table}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(map["shards"].as_array().unwrap().len(), 1);

    let split: serde_json::Value = http
        .post(format!("{base}/api/v1/shards/{table}/split"))
        .query(&[("shard", shard_id.as_str()), ("key", "50")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(split["left"].is_string() && split["right"].is_string());

    let map: serde_json::Value = http
        .get(format!("{base}/api/v1/shards/{table}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(map["shards"].as_array().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_node_drain_makes_unready() {
    let server = TestServer::start().await.expect("server starts");
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);

    // Ready before draining.
    let r = http
        .get(format!("{base}/readyz"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(r.status().is_success());

    let drained: serde_json::Value = http
        .post(format!("{base}/api/v1/node/drain"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(drained["draining"], true);

    // After draining, readiness fails (503) so LBs stop routing here.
    let r = http.get(format!("{base}/readyz")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 503);
    // Liveness still OK.
    let h = http
        .get(format!("{base}/healthz"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(h.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_api_requires_token_when_configured() {
    let mut config = nodus_config::NodusConfig::default();
    config.admin.token = Some("s3cr3t".into());
    let server = TestServer::start_with_config(config)
        .await
        .expect("server starts");
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);

    // No token → 401.
    let r = http
        .get(format!("{base}/api/v1/sessions"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 401);

    // Wrong token → 401.
    let r = http
        .get(format!("{base}/api/v1/sessions"))
        .header("Authorization", "Bearer nope")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 401);

    // Correct token → 200.
    let r = http
        .get(format!("{base}/api/v1/sessions"))
        .header("Authorization", "Bearer s3cr3t")
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());

    // Health/readiness are not behind the admin token.
    let r = http
        .get(format!("{base}/readyz"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(r.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_audit_persists_to_configured_file() {
    let path = std::env::temp_dir().join(format!("nodus_audit_it_{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    config.audit.file_path = Some(path.to_string_lossy().to_string());

    let server = TestServer::start_with_config(config)
        .await
        .expect("server starts");
    let client = connect(&server.pgwire_addr).await;
    client
        .simple_query("CREATE TABLE t (id UUID PRIMARY KEY, name TEXT NOT NULL);")
        .await
        .unwrap();

    // The audit query API reads from the same durable sink.
    let http = reqwest::Client::new();
    let events: serde_json::Value = http
        .get(format!("http://{}/api/v1/audit", server.http_addr))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!events.as_array().unwrap().is_empty());

    // And the events were written to the configured JSONL file.
    let contents = std::fs::read_to_string(&path).expect("audit file exists");
    println!("AUDIT CONTENTS: {}", contents);
    assert!(contents.contains("\"action\":\"CREATE\""));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn pgwire_rejects_bad_password() {
    let server = TestServer::start().await.expect("server starts");
    let bad = format!(
        "host={} port={} user=nodus password=wrong dbname=default",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port()
    );
    // Give the listener a moment, then a wrong password must be refused.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        tokio_postgres::connect(&bad, NoTls).await.is_err(),
        "authentication must reject an incorrect password"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_backup_pitr_restore() {
    let data_dir = std::env::temp_dir().join(format!("nodus_data_{}", std::process::id()));
    let backup_dir = std::env::temp_dir().join(format!("nodus_bk_pitr_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);

    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    config.storage.data_dir = Some(data_dir.to_string_lossy().to_string());
    config.backup.repository_uri = format!("file://{}", backup_dir.to_string_lossy());

    println!("Starting TestServer");
    let server = TestServer::start_with_config(config.clone())
        .await
        .expect("server starts");
    println!("TestServer started, connecting");
    let client = connect(&server.pgwire_addr).await;

    println!("Executing CREATE");
    client
        .execute("CREATE TABLE pitr (id INT PRIMARY KEY, val TEXT);", &[])
        .await
        .unwrap();
    println!("Executing INSERT 1");
    client
        .execute("INSERT INTO pitr (id, val) VALUES (1, 'one');", &[])
        .await
        .unwrap();

    // Give it a moment to commit properly
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Take full backup
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);

    println!("Creating backup");
    let backup_resp: serde_json::Value = http
        .post(format!("{base}/api/v1/backups"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let backup_id = backup_resp["backup_id"].as_str().unwrap().to_string();
    println!("Backup ID: {}", backup_id);

    println!("Executing INSERT 2");
    client
        .execute("INSERT INTO pitr (id, val) VALUES (2, 'two');", &[])
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let target_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    println!("Executing INSERT 3");
    client
        .execute("INSERT INTO pitr (id, val) VALUES (3, 'three');", &[])
        .await
        .unwrap();

    // Wait for WAL archiver to tick and flush/archive (interval is 1s, we wait 3s)
    println!("Waiting for WAL archiver...");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Shut down old server
    drop(client);
    drop(server);

    // Clear data dir to simulate a fresh cluster
    let _ = std::fs::remove_dir_all(&data_dir);

    // Start a new server
    println!("Starting new TestServer for restore");
    let server2 = TestServer::start_with_config(config)
        .await
        .expect("new server starts");
    let http2 = reqwest::Client::new();
    let base2 = format!("http://{}", server2.http_addr);

    // Restore to target_ts
    println!("Sending Restore request");
    let restore_resp: serde_json::Value = http2
        .post(format!(
            "{base2}/api/v1/backups/{backup_id}/restore?target_ts={target_ts}"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    println!("Restore Response: {:?}", restore_resp);

    assert!(
        restore_resp.get("error").is_none(),
        "Restore failed: {:?}",
        restore_resp
    );

    // Give Raft a moment to apply all commits
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let client2 = connect(&server2.pgwire_addr).await;
    let msgs = client2
        .query("SELECT id FROM pitr ORDER BY id;", &[])
        .await
        .unwrap();
    assert_eq!(msgs.len(), 2, "Expected exactly 2 rows after PITR restore");

    let id1: i32 = msgs[0].get(0);
    let id2: i32 = msgs[1].get(0);
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);

    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_role_and_grant_api_round_trip() {
    let server = TestServer::start().await.expect("server starts");
    let base = format!("http://{}", server.http_addr);
    let http = reqwest::Client::new();

    // A table to grant on.
    let client = connect(&server.pgwire_addr).await;
    client
        .simple_query("CREATE TABLE reports (id INT PRIMARY KEY)")
        .await
        .unwrap();

    // Create a role.
    let created: serde_json::Value = http
        .post(format!("{base}/api/v1/roles"))
        .json(&serde_json::json!({ "name": "analyst" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["created"], serde_json::json!(true));

    // It shows up in the role list.
    let roles: serde_json::Value = http
        .get(format!("{base}/api/v1/roles"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        roles
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["name"] == serde_json::json!("analyst")),
        "created role should be listed"
    );

    // Grant SELECT on the table to the role.
    let grant_body = serde_json::json!({
        "principal": "analyst",
        "privilege": "SELECT",
        "database": "default",
        "schema": "public",
        "table": "reports",
    });
    let granted: serde_json::Value = http
        .post(format!("{base}/api/v1/grants"))
        .json(&grant_body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(granted["granted"], serde_json::json!(true));

    // The grant is listed, resolved to the role + table names.
    let grants: serde_json::Value = http
        .get(format!("{base}/api/v1/grants"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        grants.as_array().unwrap().iter().any(|g| {
            g["principal"] == serde_json::json!("analyst")
                && g["privilege"] == serde_json::json!("SELECT")
        }),
        "grant should be listed with resolved principal name"
    );

    // Revoke it again.
    let revoked: serde_json::Value = http
        .delete(format!("{base}/api/v1/grants"))
        .json(&grant_body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(revoked["revoked"], serde_json::json!(true));
}
