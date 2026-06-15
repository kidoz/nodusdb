use nodus_testkit::TestServer;
use std::process::Command;
use std::time::Duration;
use tokio_postgres::NoTls;

const DEFAULT_NPGSQL_MATRIX: &[(&str, &str)] = &[("10.0.3", "10.0.2"), ("9.0.4", "9.0.4")];

fn npgsql_matrix() -> Vec<(String, String)> {
    std::env::var("NODUS_NPGSQL_MATRIX")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|entry| {
                    let mut parts = entry.split(':').map(str::trim);
                    let npgsql = parts.next().unwrap_or_default();
                    let efcore = parts.next().unwrap_or(npgsql);
                    (npgsql.to_owned(), efcore.to_owned())
                })
                .filter(|(npgsql, efcore)| !npgsql.is_empty() && !efcore.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| {
            DEFAULT_NPGSQL_MATRIX
                .iter()
                .map(|(npgsql, efcore)| ((*npgsql).to_owned(), (*efcore).to_owned()))
                .collect()
        })
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dotnet_npgsql_driver_matrix() {
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

    for (npgsql_version, efcore_version) in npgsql_matrix() {
        eprintln!(
            "running Npgsql compatibility suite with Npgsql {npgsql_version}, EF provider {efcore_version}"
        );
        let output = Command::new("dotnet")
            .arg("test")
            .arg("--nologo")
            .arg(format!("-p:NpgsqlVersion={npgsql_version}"))
            .arg(format!("-p:NpgsqlEfCoreVersion={efcore_version}"))
            .current_dir(&npgsql_dir)
            .env("NODUS_NPGSQL_CONNECTION_STRING", &npgsql_conn)
            .output()
            .expect("Failed to execute dotnet test.");

        if !output.status.success() {
            eprintln!(
                "Npgsql {npgsql_version} stdout:\n{}",
                String::from_utf8_lossy(&output.stdout)
            );
            eprintln!(
                "Npgsql {npgsql_version} stderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        assert!(
            output.status.success(),
            "Npgsql .NET test suite failed for Npgsql {npgsql_version}, EF provider {efcore_version}!"
        );
    }
}
