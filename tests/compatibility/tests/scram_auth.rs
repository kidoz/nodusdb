//! End-to-end SCRAM-SHA-256 authentication coverage driven by a real PostgreSQL
//! client (`tokio-postgres`). The server only advertises SASL/SCRAM-SHA-256, so a
//! successful connection proves the full challenge/response — including the
//! server-signature the client verifies — works against an actual driver. This
//! runs in-process (no JVM), so it always exercises the live auth code.

use nodus_testkit::TestServer;
use tokio_postgres::NoTls;

async fn try_connect(server: &TestServer, user: &str, password: &str) -> Result<(), String> {
    let conn_str = format!(
        "host={} port={} user={user} password={password} dbname=default",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port(),
    );
    match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            // A live, authenticated session must serve a trivial query.
            client
                .simple_query("SELECT 1;")
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        }
        Err(e) => Err(e.to_string()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn scram_accepts_correct_password_and_rejects_wrong_one() {
    let server = TestServer::start().await.expect("server starts");

    // The default superuser password set by `TestServer` is `nodus`.
    try_connect(&server, "nodus", "nodus")
        .await
        .expect("SCRAM auth should succeed with the correct password");

    let wrong = try_connect(&server, "nodus", "not-the-password").await;
    assert!(
        wrong.is_err(),
        "SCRAM auth must reject an incorrect password"
    );

    let unknown = try_connect(&server, "ghost", "whatever").await;
    assert!(unknown.is_err(), "SCRAM auth must reject an unknown user");

    server.shutdown().await;
}
