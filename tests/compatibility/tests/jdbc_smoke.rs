use nodus_testkit::TestServer;
use std::process::Command;
use std::time::Duration;
use tokio_postgres::NoTls;

#[tokio::test(flavor = "multi_thread")]
async fn test_java_jdbc_driver() {
    let server = TestServer::start().await.expect("server starts");

    // Give the server a moment to start and accept connections via pure rust client first to be sure
    let conn_str = format!(
        "host={} port={} user=nodus password=nodus dbname=default",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port()
    );
    let mut is_up = false;
    for _ in 0..30 {
        if let Ok((client, connection)) = tokio_postgres::connect(&conn_str, NoTls).await {
            is_up = true;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let _ = client.simple_query("SELECT 1;").await;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(is_up, "PGWire server did not start in time");

    // Construct the standard pgjdbc URL
    let jdbc_url = format!(
        "jdbc:postgresql://{}:{}/default",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port()
    );

    // Skip (don't fail) on machines without a JVM; CI installs one.
    if Command::new("java").arg("-version").output().is_err() {
        eprintln!("skipping JDBC suite: `java` not found on PATH");
        return;
    }

    // Run `./gradlew test` in the java project directory. Cargo runs test
    // binaries with the package root as CWD, so resolve via CARGO_MANIFEST_DIR.
    let jdbc_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("jdbc");
    let status = Command::new(jdbc_dir.join("gradlew"))
        .arg("test")
        .current_dir(&jdbc_dir)
        .env("NODUS_JDBC_URL", jdbc_url)
        .status()
        .expect("Failed to execute Gradle.");

    assert!(status.success(), "JDBC Java test suite failed!");
}
