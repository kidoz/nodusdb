use nodus_testkit::TestServer;
use std::process::Command;
use std::time::Duration;
use tokio_postgres::NoTls;

#[tokio::test(flavor = "multi_thread")]
async fn test_dotnet_npgsql_driver() {
    let server = TestServer::start().await.expect("server starts");

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

    if Command::new("dotnet").arg("--info").output().is_err() {
        eprintln!("skipping Npgsql suite: `dotnet` not found on PATH");
        return;
    }

    let npgsql_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("npgsql");
    let npgsql_conn = format!(
        "Host={};Port={};Username=nodus;Password=nodus;Database=default;Pooling=true;Maximum Pool Size=4;Timeout=5;Command Timeout=5;Application Name=nodus-npgsql-compat;Server Compatibility Mode=NoTypeLoading",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port()
    );

    let output = Command::new("dotnet")
        .arg("test")
        .arg("--nologo")
        .current_dir(&npgsql_dir)
        .env("NODUS_NPGSQL_CONNECTION_STRING", npgsql_conn)
        .output()
        .expect("Failed to execute dotnet test.");

    if !output.status.success() {
        eprintln!(
            "dotnet test stdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        eprintln!(
            "dotnet test stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert!(output.status.success(), "Npgsql .NET test suite failed!");
}
