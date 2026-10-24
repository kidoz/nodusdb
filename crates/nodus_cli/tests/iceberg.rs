#![cfg(feature = "iceberg")]

// Share the on-disk Iceberg/REST fixture, not the connector implementation.
#[allow(dead_code)]
#[path = "../../nodus_iceberg/tests/support/mod.rs"]
mod support;

use arrow_array::Int64Array;
use arrow_ipc::reader::StreamReader;
use support::Fixture;

fn command(fixture: &Fixture, operation: &str) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_nodus_cli"));
    command.args([
        "iceberg",
        operation,
        "--catalog-uri",
        &fixture.uri,
        "--warehouse",
        "test-warehouse",
        "--namespace",
        "analytics",
        "--table",
        "events",
        "--storage",
        "local",
    ]);
    command.env("NODUS_ICEBERG_TOKEN", "catalog-token");
    command.env("NODUS_ADMIN_TOKEN", "admin-token-must-not-reach-catalog");
    command
}

#[tokio::test]
async fn cli_inspects_and_publishes_typed_arrow_without_admin_credentials() {
    let fixture = Fixture::new().await;
    let result = command(&fixture, "inspect").output().await.unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(summary["snapshot_id"], 20);
    assert_eq!(summary["version"], 1);
    assert!(!String::from_utf8_lossy(&result.stdout).contains("catalog-token"));

    let output = fixture.directory.path().join("rows.arrow");
    let result = command(&fixture, "scan")
        .arg("--output")
        .arg(&output)
        .args([
            "--snapshot-id",
            "10",
            "--limit",
            "1",
            "--column",
            "name",
            "--column",
            "id",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(summary["rows"], 1);
    assert_eq!(summary["snapshot_id"], 10);
    let mut arrow = StreamReader::try_new(std::fs::File::open(output).unwrap(), None).unwrap();
    assert_eq!(arrow.schema().field(0).name(), "name");
    let batch = arrow.next().unwrap().unwrap();
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    assert!(arrow.next().is_none());
    assert!(
        fixture
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(method, _)| method == "GET")
    );
}

#[tokio::test]
async fn cli_preserves_existing_files_and_does_not_publish_failed_reads() {
    let fixture = Fixture::new().await;
    let output = fixture.directory.path().join("rows.arrow");
    std::fs::write(&output, "preserve-me").unwrap();
    let result = command(&fixture, "scan")
        .arg("--output")
        .arg(&output)
        .output()
        .await
        .unwrap();
    assert!(!result.status.success());
    assert_eq!(std::fs::read_to_string(&output).unwrap(), "preserve-me");
    std::fs::remove_file(&output).unwrap();
    std::fs::remove_file(fixture.directory.path().join("data-20.parquet")).unwrap();
    let result = command(&fixture, "scan")
        .arg("--output")
        .arg(&output)
        .output()
        .await
        .unwrap();
    assert!(!result.status.success());
    assert!(!output.exists());
    assert!(!String::from_utf8_lossy(&result.stderr).contains("catalog-token"));
    assert!(
        std::fs::read_dir(fixture.directory.path())
            .unwrap()
            .all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".tmp")
            })
    );
}

#[tokio::test]
async fn cli_empty_table_writes_a_valid_schema_only_stream() {
    let fixture = Fixture::new().await;
    fixture.make_empty();
    let output = fixture.directory.path().join("empty.arrow");
    let result = command(&fixture, "scan")
        .arg("--output")
        .arg(&output)
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let mut arrow = StreamReader::try_new(std::fs::File::open(output).unwrap(), None).unwrap();
    assert_eq!(arrow.schema().fields().len(), 2);
    assert!(arrow.next().is_none());
}
