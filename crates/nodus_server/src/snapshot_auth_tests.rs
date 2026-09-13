//! Real pgwire/SCRAM and admin Basic-auth checks across catalog checkpoint install.
use super::*;
use nodus_catalog::{MemoryCatalog, PrincipalId, RevokePrivilegeRequest};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_catalog_replacement_keeps_bootstrap_login_and_respects_revocation() {
    let mut config = NodusConfig::default();
    config.admin.password = Some("snapshot-test-password".into());
    config.admin.token = Some("snapshot-test-token".into());
    let (stop, shutdown) = tokio::sync::watch::channel(());
    let server = run_server_with_config(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        config,
        shutdown,
    )
    .await
    .unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    tokio::time::timeout(Duration::from_secs(15), async {
        while !http
            .get(format!("{base}/readyz"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let url = format!(
        "host=127.0.0.1 port={} user=nodus password=snapshot-test-password dbname=nodus",
        server.pgwire_addr.port()
    );
    let (old_client, old_connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let old_task = tokio::spawn(old_connection);
    let old_id = server.catalog.get_principal_by_name("nodus").unwrap().id;

    let incoming = MemoryCatalog::new();
    incoming
        .import_snapshot(server.catalog.export_snapshot())
        .unwrap();
    let id = PrincipalId::new();
    incoming
        .create_role(CreateRoleRequest {
            id,
            name: "nodus".into(),
            principal_type: PrincipalType::User,
            database_id: None,
        })
        .unwrap();
    incoming
        .grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
    // Exercise the same atomic catalog projection replacement used by Raft
    // snapshots. Durability/transport are covered by the process matrix.
    server
        .catalog
        .install_raft_catalog(&incoming.export_raft_catalog().unwrap(), &mut || Ok(()))
        .unwrap();
    assert_ne!(old_id, id);
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let task = tokio::spawn(connection);
    client.simple_query("SELECT 42").await.unwrap();
    assert!(server.registry.list().iter().any(|s| s.principal_id == id));
    assert!(
        server
            .registry
            .list()
            .iter()
            .any(|s| s.principal_id == old_id),
        "existing sessions must not be rebound"
    );
    assert!(
        old_client
            .simple_query("CREATE TABLE stale_session (id INT)")
            .await
            .is_err()
    );
    assert_eq!(
        http.get(format!("{base}/api/v1/grants"))
            .basic_auth("nodus", Some("snapshot-test-password"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        http.get(format!("{base}/api/v1/grants"))
            .basic_auth("nodus", Some("wrong-password"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );

    assert_eq!(
        http.post(format!("{base}/api/v1/node/take-leadership/missing"))
            .basic_auth("nodus", Some("snapshot-test-password"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let events: serde_json::Value = http
        .get(format!("{base}/api/v1/audit"))
        .basic_auth("nodus", Some("snapshot-test-password"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        events
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["actor"] == serde_json::json!(id)
                && event["action"] == "ManageNode"
                && event["result"] == "Failure"
                && event["resource"] == serde_json::json!(ResourceRef::System)
                && event["reason"] == "POST /api/v1/node/take-leadership/missing")
    );

    server
        .catalog
        .revoke_privilege(RevokePrivilegeRequest {
            principal_id: id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
    assert!(
        tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .is_err()
    );
    assert_eq!(
        http.get(format!("{base}/api/v1/grants"))
            .basic_auth("nodus", Some("snapshot-test-password"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert!(
        client
            .simple_query("CREATE TABLE revoked_session (id INT)")
            .await
            .is_err()
    );
    drop((client, old_client));
    task.abort();
    old_task.abort();
    let _ = stop.send(());
    let _ = server.pgwire_task.await;
    let _ = server.http_task.await;
    for task in server.background_tasks {
        let _ = task.await;
    }
}
