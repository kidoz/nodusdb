mod support;

use std::time::Duration;

use arrow_array::{Array, Int64Array, StringArray};
use nodus_iceberg::{Error, SnapshotReader};
use serde_json::json;
use support::Fixture;

#[tokio::test]
async fn v1_manifest_lists_and_deleted_data_entries_are_read_correctly() {
    let fixture = Fixture::new_v1().await;
    let reader = SnapshotReader::open(fixture.config(), fixture.options())
        .await
        .unwrap();
    assert_eq!(reader.summary().format_version, 1);
    let mut batches = reader.scan().await.unwrap();
    let mut rows = 0;
    while let Some(batch) = batches.next_batch().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 3);
}

#[tokio::test]
async fn s3_range_reads_use_catalog_vended_credentials() {
    let fixture = Fixture::new_s3().await;
    let mut batches = SnapshotReader::open(fixture.config(), fixture.options())
        .await
        .unwrap()
        .scan()
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = batches.next_batch().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 3);
    let requests = fixture.state.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|(_, path)| path.contains("data-20.parquet"))
    );
    assert!(
        requests
            .iter()
            .all(|(method, _)| method == "GET" || method == "HEAD")
    );
}

#[tokio::test]
async fn real_rest_discovery_parquet_projection_nulls_and_snapshot_pinning() {
    let fixture = Fixture::new().await;
    let mut options = fixture.options();
    options.columns = vec!["name".into(), "id".into()];
    let reader = SnapshotReader::open(fixture.config(), options)
        .await
        .unwrap();
    assert_eq!(reader.summary().snapshot_id, Some(20));
    // Advancing/replacing the catalog response after open cannot change this scan.
    fixture.make_empty();
    let mut batches = reader.scan().await.unwrap();
    let mut ids = vec![];
    let mut names = vec![];
    while let Some(batch) = batches.next_batch().await.unwrap() {
        assert!(batch.num_rows() <= 2);
        assert_eq!(batch.schema().field(0).name(), "name");
        let id = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let name = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            ids.push(id.value(row));
            names.push(if name.is_null(row) {
                None
            } else {
                Some(name.value(row).to_owned())
            });
        }
    }
    assert_eq!(ids, [20, 21, 22]);
    assert_eq!(names, [Some("alpha".into()), None, Some("".into())]);
    let requests = fixture.state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|(method, _)| method == "GET"));
}

#[tokio::test]
async fn retained_snapshot_and_exact_row_limit() {
    let fixture = Fixture::new().await;
    let mut options = fixture.options();
    options.snapshot_id = Some(10);
    options.limit = Some(1);
    let reader = SnapshotReader::open(fixture.config(), options)
        .await
        .unwrap();
    assert_eq!(reader.summary().snapshot_id, Some(10));
    let mut batches = reader.scan().await.unwrap();
    let batch = batches.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    assert!(batches.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn empty_table_still_validates_projection_and_snapshot() {
    let fixture = Fixture::new().await;
    fixture.make_empty();
    let reader = SnapshotReader::open(fixture.config(), fixture.options())
        .await
        .unwrap();
    assert_eq!(reader.summary().snapshot_id, None);
    assert_eq!(reader.schema().fields().len(), 2);
    assert!(
        reader
            .scan()
            .await
            .unwrap()
            .next_batch()
            .await
            .unwrap()
            .is_none()
    );
    let mut options = fixture.options();
    options.columns = vec!["absent".into()];
    assert!(matches!(
        SnapshotReader::open(fixture.config(), options).await,
        Err(Error::InvalidOptions(_))
    ));
    let mut options = fixture.options();
    options.snapshot_id = Some(999);
    assert!(matches!(
        SnapshotReader::open(fixture.config(), options).await,
        Err(Error::InvalidOptions(_))
    ));
}

#[tokio::test]
async fn fail_closed_for_deletes_and_v3() {
    let fixture = Fixture::new().await;
    fixture.add_delete_manifest().await;
    let reader = SnapshotReader::open(fixture.config(), fixture.options())
        .await
        .unwrap();
    assert!(matches!(
        reader.scan().await,
        Err(Error::Unsupported("snapshots with delete manifests"))
    ));
    fixture.make_empty();
    {
        let mut response = fixture.state.response.lock().unwrap();
        response["metadata"]["format-version"] = json!(3);
        response["metadata"]["next-row-id"] = json!(0);
    }
    assert!(matches!(
        SnapshotReader::open(fixture.config(), fixture.options()).await,
        Err(Error::Unsupported(_))
    ));
}

#[tokio::test]
async fn auth_failure_missing_table_and_deadline_are_errors_without_remote_secrets() {
    let fixture = Fixture::new().await;
    let mut config = fixture.config();
    config.bearer_token = Some("wrong-token".into());
    let error = SnapshotReader::open(config, fixture.options())
        .await
        .err()
        .unwrap();
    let message = format!("{error:?} {error}");
    assert!(!message.contains("wrong-token"));
    assert!(!message.contains("secret must not leak"));
    *fixture.state.status.lock().unwrap() = axum::http::StatusCode::NOT_FOUND;
    assert!(matches!(
        SnapshotReader::open(fixture.config(), fixture.options()).await,
        Err(Error::Operation { .. })
    ));
    *fixture.state.delay.lock().unwrap() = Duration::from_millis(100);
    let mut config = fixture.config();
    config.timeout = Duration::from_millis(10);
    assert!(
        SnapshotReader::open(config, fixture.options())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn missing_data_is_an_error_and_idle_consumers_expire() {
    let fixture = Fixture::new().await;
    let reader = SnapshotReader::open(fixture.config(), fixture.options())
        .await
        .unwrap();
    std::fs::remove_file(fixture.directory.path().join("data-20.parquet")).unwrap();
    let mut batches = reader.scan().await.unwrap();
    assert!(batches.next_batch().await.is_err());
    assert!(batches.next_batch().await.unwrap().is_none());

    let fixture = Fixture::new().await;
    let mut config = fixture.config();
    config.timeout = Duration::from_millis(200);
    let mut batches = SnapshotReader::open(config, fixture.options())
        .await
        .unwrap()
        .scan()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(matches!(batches.next_batch().await, Err(Error::Timeout)));
}

#[tokio::test]
async fn local_validation_precedes_network_access() {
    let fixture = Fixture::new().await;
    let mut options = fixture.options();
    options.table = "../other?secret".into();
    assert!(matches!(
        SnapshotReader::open(fixture.config(), options).await,
        Err(Error::InvalidOptions(_))
    ));
    let mut config = fixture.config();
    config
        .storage_properties
        .insert("token".into(), "must-not-be-used".into());
    assert!(matches!(
        SnapshotReader::open(config, fixture.options()).await,
        Err(Error::InvalidOptions(_))
    ));
    assert!(fixture.state.requests.lock().unwrap().is_empty());
}
