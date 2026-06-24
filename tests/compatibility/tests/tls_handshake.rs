//! End-to-end TLS coverage for the pgwire listener.
//!
//! The other compatibility suites connect in plaintext; the loaders for
//! `nodus_server`'s TLS acceptor are unit-tested, but nothing drove a real
//! negotiated handshake. These tests generate a throwaway self-signed
//! certificate, start the server with TLS enabled, and connect through a
//! genuine rustls-backed `tokio-postgres` client with `sslmode=require` — the
//! accepted-TLS path a driver actually takes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nodus_testkit::TestServer;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;

/// A temp directory holding the generated cert/key, cleaned up on drop.
struct CertFixture {
    dir: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
    cert_der: CertificateDer<'static>,
}

impl Drop for CertFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Generates a self-signed cert valid for `localhost`/`127.0.0.1` and writes the
/// PEM cert+key to a unique temp directory.
fn generate_cert() -> CertFixture {
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("generate self-signed cert");

    let dir = std::env::temp_dir().join(format!("nodus_tls_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert");
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).expect("write key");

    CertFixture {
        dir,
        cert_path,
        key_path,
        cert_der: cert.cert.der().clone(),
    }
}

/// Starts a TestServer with TLS terminated using the fixture's cert/key.
async fn start_tls_server(fixture: &CertFixture) -> TestServer {
    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    config.tls.enabled = true;
    config.tls.cert_path = Some(fixture.cert_path.to_string_lossy().into_owned());
    config.tls.key_path = Some(fixture.key_path.to_string_lossy().into_owned());
    TestServer::start_with_config(config)
        .await
        .expect("server starts with TLS")
}

/// Builds a rustls client connector that trusts exactly the given cert.
fn tls_connector_trusting(cert_der: &CertificateDer<'static>) -> MakeRustlsConnect {
    let mut roots = RootCertStore::empty();
    roots.add(cert_der.clone()).expect("add root cert");
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    MakeRustlsConnect::new(config)
}

fn conn_str(server: &TestServer, sslmode: &str) -> String {
    format!(
        "host=127.0.0.1 port={} user=nodus password=nodus dbname=default sslmode={sslmode}",
        server.pgwire_addr.port()
    )
}

/// A driver requesting TLS against a trusted cert completes the handshake and
/// runs queries over the encrypted channel.
#[tokio::test(flavor = "multi_thread")]
async fn test_tls_handshake_and_query_round_trip() {
    let fixture = generate_cert();
    let server = start_tls_server(&fixture).await;
    let tls = tls_connector_trusting(&fixture.cert_der);

    let (client, connection) = tokio_postgres::connect(&conn_str(&server, "require"), tls)
        .await
        .expect("TLS connect succeeds");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let rows = client.query("SELECT 1", &[]).await.expect("query over TLS");
    assert_eq!(rows.len(), 1);
    let value: i32 = rows[0].get(0);
    assert_eq!(value, 1);

    drop(client);
    let _ = conn_handle.await;
}

/// TLS is genuinely validated: a client that does not trust the server's cert is
/// rejected at the handshake rather than silently downgraded.
#[tokio::test(flavor = "multi_thread")]
async fn test_tls_handshake_rejects_untrusted_cert() {
    let fixture = generate_cert();
    let server = start_tls_server(&fixture).await;

    // Trust an unrelated cert, not the server's.
    let other = generate_cert();
    let tls = tls_connector_trusting(&other.cert_der);

    let result = tokio_postgres::connect(&conn_str(&server, "require"), tls).await;
    assert!(
        result.is_err(),
        "handshake must fail when the server cert is untrusted"
    );
}

/// A TLS-enabled listener still serves plaintext clients (`sslmode=disable`):
/// enabling TLS adds the negotiated path without forcing it. This documents the
/// current accept-both behavior the driver matrix relies on.
#[tokio::test(flavor = "multi_thread")]
async fn test_tls_enabled_server_still_accepts_plaintext() {
    let fixture = generate_cert();
    let server = start_tls_server(&fixture).await;

    for _ in 0..30 {
        if let Ok((client, connection)) =
            tokio_postgres::connect(&conn_str(&server, "disable"), NoTls).await
        {
            let handle = tokio::spawn(async move {
                let _ = connection.await;
            });
            let rows = client
                .query("SELECT 1", &[])
                .await
                .expect("plaintext query");
            assert_eq!(rows.len(), 1);
            drop(client);
            let _ = handle.await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("plaintext connection to TLS-enabled server did not succeed");
}
