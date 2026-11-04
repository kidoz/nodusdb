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

#[tokio::test]
async fn worker_rejects_a_filtered_legacy_request_before_source_access() {
    use nodus_common::iceberg::{
        self as wire, Profile, Request, Response, ScanLiteral, ScanOp, ScanPredicate,
    };
    use tokio::io::AsyncWriteExt;
    let fixture = Fixture::new_s3().await;
    let request = Request {
        version: 1,
        profile: Profile {
            uri: fixture.uri.clone(),
            warehouse: Some("test-warehouse".into()),
            bearer_token: Some("catalog-token".into()),
            storage_properties: Default::default(),
        },
        namespace: vec!["analytics".into()],
        table: "events".into(),
        expected: None,
        columns: vec!["id".into()],
        limit: 10,
        timeout_ms: 1000,
        address_space_bytes: 2 * 1024 * 1024 * 1024,
        predicates: vec![ScanPredicate {
            field_id: 1,
            column: "id".into(),
            op: ScanOp::Eq,
            value: ScanLiteral::Int(20),
        }],
    };
    let mut frame = vec![];
    wire::write_frame(&mut frame, &request).unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_nodus_cli"))
        .args(["iceberg", "worker"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(&frame).await.unwrap();
    drop(stdin);
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(result.status.success());
    let response: Response = wire::read_frame(&mut result.stdout.as_slice()).unwrap();
    assert!(
        matches!(response, Response::Error(message) if message == "unsupported Iceberg worker version")
    );
    assert!(fixture.state.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_v2_returns_raw_candidates_and_pruning_statistics() {
    use nodus_common::iceberg::{
        self as wire, Descriptor, Profile, Request, Response, ScanLiteral, ScanOp, ScanPredicate,
    };
    use tokio::io::AsyncWriteExt;
    async fn invoke(request: &Request) -> Vec<Response> {
        let mut bytes = vec![];
        wire::write_frame(&mut bytes, request).unwrap();
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_nodus_cli"))
            .args(["iceberg", "worker"])
            .env_clear()
            .env("TOKIO_WORKER_THREADS", "1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(&bytes).await.unwrap();
        drop(stdin);
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(15), child.wait_with_output())
                .await
                .unwrap()
                .unwrap();
        assert!(result.status.success());
        let mut input = result.stdout.as_slice();
        let mut frames = vec![];
        while !input.is_empty() {
            frames.push(wire::read_frame(&mut input).unwrap());
        }
        frames
    }
    let fixture = Fixture::new_s3().await;
    let mut request = Request {
        version: wire::WORKER_VERSION,
        profile: Profile {
            uri: fixture.uri.clone(),
            warehouse: Some("test-warehouse".into()),
            bearer_token: Some("catalog-token".into()),
            storage_properties: Default::default(),
        },
        namespace: vec!["analytics".into()],
        table: "events".into(),
        expected: None,
        columns: vec![],
        predicates: vec![],
        limit: 100,
        timeout_ms: 10000,
        address_space_bytes: 2 * 1024 * 1024 * 1024,
    };
    let inspection = invoke(&request).await;
    let Response::Schema {
        version,
        table_uuid,
        fields,
        ..
    } = &inspection[0]
    else {
        panic!("{inspection:?}");
    };
    assert_eq!(*version, 2);
    request.expected = Some(Descriptor {
        version: 1,
        profile: "lake".into(),
        namespace: request.namespace.clone(),
        table: request.table.clone(),
        table_uuid: table_uuid.clone(),
        fields: fields.clone(),
    });
    request.columns = vec!["id".into()];
    request.predicates = vec![ScanPredicate {
        field_id: 1,
        column: "id".into(),
        op: ScanOp::Eq,
        value: ScanLiteral::Int(21),
    }];
    let result = invoke(&request).await;
    assert!(
        matches!(result.last(), Some(Response::Done { rows: 3 })),
        "{result:?}"
    );
    assert!(result.iter().any(|frame| matches!(frame, Response::ScanStats(stats) if stats.planned_files == 1 && stats.rows_read == 3)));
    let ids: Vec<_> = result
        .into_iter()
        .filter_map(|frame| match frame {
            Response::Row(row) => Some(row[0].clone()),
            _ => None,
        })
        .collect();
    // Missing file metrics prevent file pruning in this fixture. SQL filtering
    // belongs to the executor; the worker must not consume LIMIT before it.
    assert_eq!(
        ids,
        [Some("20".into()), Some("21".into()), Some("22".into())]
    );
}
