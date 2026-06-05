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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

    http.post(format!("{base}/api/v1/upgrade/start?target=0.2.0"))
        .send()
        .await
        .unwrap();
    // Single node: report it upgraded to reach ReadyToFinalize.
    let st: serde_json::Value = http
        .post(format!("{base}/api/v1/upgrade/node-upgraded?node=n1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
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

#[tokio::test]
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

#[tokio::test]
async fn admin_node_drain_makes_unready() {
    let server = TestServer::start().await.expect("server starts");
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);

    // Ready before draining.
    let r = http.get(format!("{base}/readyz")).send().await.unwrap();
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
    let h = http.get(format!("{base}/healthz")).send().await.unwrap();
    assert!(h.status().is_success());
}

#[tokio::test]
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
    let r = http.get(format!("{base}/readyz")).send().await.unwrap();
    assert!(r.status().is_success());
}

#[tokio::test]
async fn durable_audit_persists_to_configured_file() {
    let path = std::env::temp_dir().join(format!("nodus_audit_it_{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut config = nodus_config::NodusConfig::default();
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
    assert!(contents.contains("CREATE_TABLE"));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
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
