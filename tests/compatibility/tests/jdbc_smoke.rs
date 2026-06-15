use nodus_testkit::TestServer;
use std::process::Command;
use std::time::Duration;
use tokio_postgres::NoTls;

const DEFAULT_PGJDBC_VERSIONS: &[&str] = &["42.7.11", "42.7.7", "42.6.2"];

fn matrix_values(env_name: &str, defaults: &[&str]) -> Vec<String> {
    std::env::var(env_name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| defaults.iter().map(|value| (*value).to_owned()).collect())
}

async fn wait_for_server(server: &TestServer) {
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
            let _ = client.simple_query("SELECT 1;").await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("PGWire server did not start in time");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_java_jdbc_driver_matrix() {
    // Skip (don't fail) on machines without a JVM; CI installs one.
    if Command::new("java").arg("-version").output().is_err() {
        eprintln!("skipping JDBC suite: `java` not found on PATH");
        return;
    }

    // Run `./gradlew test` in the java project directory. Cargo runs test
    // binaries with the package root as CWD, so resolve via CARGO_MANIFEST_DIR.
    let jdbc_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("jdbc");

    for version in matrix_values("NODUS_PGJDBC_VERSIONS", DEFAULT_PGJDBC_VERSIONS) {
        let server = TestServer::start().await.expect("server starts");
        wait_for_server(&server).await;

        // Construct the standard pgjdbc URL.
        let jdbc_url = format!(
            "jdbc:postgresql://{}:{}/default",
            server.pgwire_addr.ip(),
            server.pgwire_addr.port()
        );
        eprintln!("running pgJDBC compatibility suite with org.postgresql:postgresql:{version}");
        let output = Command::new(jdbc_dir.join("gradlew"))
            .arg("test")
            .arg("--rerun-tasks")
            .current_dir(&jdbc_dir)
            .env("NODUS_JDBC_URL", &jdbc_url)
            .env("NODUS_PGJDBC_VERSION", &version)
            .output()
            .expect("Failed to execute Gradle.");

        if !output.status.success() {
            eprintln!(
                "pgJDBC {version} stdout:\n{}",
                String::from_utf8_lossy(&output.stdout)
            );
            eprintln!(
                "pgJDBC {version} stderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        assert!(
            output.status.success(),
            "JDBC Java test suite failed for pgJDBC {version}!"
        );
    }
}
