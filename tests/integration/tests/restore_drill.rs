use anyhow::{Context, Result, bail};
use nodus_backup::{BackupOrchestrator, BackupRepository, FsBackupRepository, S3BackupRepository};
use nodus_testkit::TestServer;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_postgres::{Client, NoTls};

const ADMIN_TOKEN: &str = "restore-drill-admin-token";
const INDEX: &str = "restore_drill_val_idx";
const READER: &str = "drill_reader";
const READER_PASSWORD: &str = "drill-reader-secret";

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

async fn connect(addr: std::net::SocketAddr, user: &str, password: &str) -> Result<Client> {
    let connection_string = format!(
        "host={} port={} user={user} password={password} dbname=default",
        addr.ip(),
        addr.port()
    );
    for attempt in 0..50 {
        match tokio_postgres::connect(&connection_string, NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return Ok(client);
            }
            Err(error) if attempt == 49 => return Err(error.into()),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    unreachable!()
}

fn server_config(data_dir: &Path, repository_uri: &str) -> nodus_config::NodusConfig {
    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    config.admin.token = Some(ADMIN_TOKEN.into());
    config.admin.allow_insecure = false;
    config.storage.data_dir = Some(data_dir.to_string_lossy().into_owned());
    config.storage.allow_ephemeral = false;
    config.backup.repository_uri = repository_uri.into();
    config
}

async fn response_json(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let body = response.json::<Value>().await?;
    if !status.is_success() {
        bail!("admin request returned HTTP {status}: {body}");
    }
    Ok(body)
}

async fn create_backup(
    http: &reqwest::Client,
    addr: std::net::SocketAddr,
    parent_backup_id: Option<&str>,
) -> Result<String> {
    let mut request = http
        .post(format!("http://{addr}/api/v1/backups"))
        .bearer_auth(ADMIN_TOKEN);
    if let Some(parent) = parent_backup_id {
        request = request.query(&[("parent_backup_id", parent)]);
    }
    let body = response_json(request.send().await?).await?;
    if body["status"] != "Completed" {
        bail!("backup did not complete: {body}");
    }
    body["backup_id"]
        .as_str()
        .map(str::to_owned)
        .context("backup response omitted backup_id")
}

async fn restore_backup(
    http: &reqwest::Client,
    addr: std::net::SocketAddr,
    backup_id: &str,
    target_ts: Option<u64>,
) -> Result<Value> {
    let mut request = http
        .post(format!("http://{addr}/api/v1/backups/{backup_id}/restore"))
        .bearer_auth(ADMIN_TOKEN);
    if let Some(target) = target_ts {
        request = request.query(&[("target_ts", target)]);
    }
    let body = response_json(request.send().await?).await?;
    if body.get("error").is_some() {
        bail!("restore failed: {body}");
    }
    Ok(body)
}

async fn verify_backup(
    http: &reqwest::Client,
    addr: std::net::SocketAddr,
    backup_id: &str,
) -> Result<()> {
    let body = response_json(
        http.post(format!("http://{addr}/api/v1/backups/{backup_id}/verify"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await?,
    )
    .await?;
    if body["verified"] != true {
        bail!("backup verification failed: {body}");
    }
    Ok(())
}

async fn finalize_snapshot_v2(http: &reqwest::Client, addr: std::net::SocketAddr) -> Result<()> {
    for (path, expected_phase) in [
        ("start?target=snapshot-v2", "RollingNodes"),
        ("node-upgraded?node=1", "ReadyToFinalize"),
        ("finalize", "Finalized"),
    ] {
        let body = response_json(
            http.post(format!("http://{addr}/api/v1/upgrade/{path}"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await?,
        )
        .await?;
        if body["phase"] != expected_phase {
            bail!("upgrade transition to {expected_phase} failed: {body}");
        }
    }
    Ok(())
}

fn repository_from_uri(uri: &str) -> Result<Arc<dyn BackupRepository>> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        return Ok(Arc::new(S3BackupRepository::from_env(bucket, prefix)?));
    }
    let path = uri.strip_prefix("file://").unwrap_or(uri);
    Ok(Arc::new(FsBackupRepository::new(path)))
}

async fn wait_for_archive(repository_uri: &str, through_ts: u64) -> Result<()> {
    let orchestrator = BackupOrchestrator::new(repository_from_uri(repository_uri)?);
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last_error = None;
    loop {
        match orchestrator.load_wal_archive_indexes().await {
            Ok(indexes)
                if indexes.iter().any(|index| {
                    index
                        .last_commit_ts
                        .is_some_and(|commit_ts| commit_ts >= through_ts)
                }) =>
            {
                return Ok(());
            }
            Ok(_) => {}
            Err(error) => last_error = Some(error.to_string()),
        }
        if Instant::now() >= deadline {
            bail!(
                "WAL archive did not reach {through_ts} within 45 seconds: {}",
                last_error.unwrap_or_else(|| "required commit is not archived".into())
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

struct Validation {
    rows: Vec<(i32, String)>,
    missing_rows: usize,
    unexpected_rows: usize,
    index_ok: bool,
    permission_ok: bool,
}

async fn validate_state(
    addr: std::net::SocketAddr,
    expected: &[(i32, String)],
) -> Result<Validation> {
    let admin = connect(addr, "nodus", "nodus").await?;
    let rows = admin
        .query("SELECT id, val FROM restore_drill ORDER BY id", &[])
        .await?;
    let actual: Vec<(i32, String)> = rows
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, String>(1)))
        .collect();
    let missing_rows = expected
        .iter()
        .filter(|expected_row| !actual.contains(expected_row))
        .count();
    let unexpected_rows = actual
        .iter()
        .filter(|actual_row| !expected.contains(actual_row))
        .count();

    let indexes = admin
        .query(
            "SELECT indexname FROM pg_catalog.pg_indexes WHERE schemaname = 'public' AND tablename = 'restore_drill'",
            &[],
        )
        .await?;
    let index_ok = indexes.iter().any(|row| row.get::<_, String>(0) == INDEX);

    let privilege = admin
        .query_one(
            "SELECT has_table_privilege('drill_reader', 'restore_drill', 'SELECT')",
            &[],
        )
        .await?;
    let has_privilege = privilege.get::<_, bool>(0);
    drop(admin);

    let reader = connect(addr, READER, READER_PASSWORD).await?;
    let reader_rows = reader
        .query("SELECT id, val FROM restore_drill ORDER BY id", &[])
        .await?;
    let reader_actual: Vec<(i32, String)> = reader_rows
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, String>(1)))
        .collect();
    let permission_ok = has_privilege && reader_actual == expected;

    Ok(Validation {
        rows: actual,
        missing_rows,
        unexpected_rows,
        index_ok,
        permission_ok,
    })
}

async fn run_scenario(
    root: &Path,
    repository_uri: &str,
    name: &str,
    backup_id: &str,
    target_ts: Option<u64>,
    expected: &[(i32, String)],
) -> Result<Value> {
    let data_dir = root.join(name);
    let config = server_config(&data_dir, repository_uri);
    let server = TestServer::start_with_config(config.clone()).await?;
    let http = reqwest::Client::new();
    verify_backup(&http, server.http_addr, backup_id).await?;

    let total_started = Instant::now();
    let restore_started = Instant::now();
    let restore = restore_backup(&http, server.http_addr, backup_id, target_ts).await?;
    let restore_api_ms = restore_started.elapsed().as_millis() as u64;
    let restored = validate_state(server.pgwire_addr, expected).await?;
    server.shutdown().await;

    let restarted = TestServer::start_with_config(config).await?;
    let persisted = validate_state(restarted.pgwire_addr, expected).await?;
    restarted.shutdown().await;
    let total_recovery_ms = total_started.elapsed().as_millis() as u64;

    let missing_rows = restored.missing_rows.max(persisted.missing_rows);
    let unexpected_rows = restored.unexpected_rows.max(persisted.unexpected_rows);
    let passed = missing_rows == 0
        && unexpected_rows == 0
        && restored.index_ok
        && persisted.index_ok
        && restored.permission_ok
        && persisted.permission_ok;
    Ok(json!({
        "scenario": name,
        "backup_id": backup_id,
        "target_ts": target_ts,
        "passed": passed,
        "restore_api_ms": restore_api_ms,
        "total_recovery_ms": total_recovery_ms,
        "restart_verified": true,
        "index_verified": restored.index_ok && persisted.index_ok,
        "permission_verified": restored.permission_ok && persisted.permission_ok,
        "observed_data_loss_rows": missing_rows,
        "unexpected_rows": unexpected_rows,
        "expected_rows": expected.iter().map(|(id, value)| format!("{id}:{value}")).collect::<Vec<_>>(),
        "actual_rows": persisted.rows.iter().map(|(id, value)| format!("{id}:{value}")).collect::<Vec<_>>(),
        "restore_response": restore,
    }))
}

async fn execute_drill(repository_uri: &str, scenarios: &mut Vec<Value>) -> Result<()> {
    let root = tempfile::tempdir()?;
    let source_dir = root.path().join("source");
    let source = TestServer::start_with_config(server_config(&source_dir, repository_uri)).await?;
    let http = reqwest::Client::new();
    finalize_snapshot_v2(&http, source.http_addr).await?;
    let client = connect(source.pgwire_addr, "nodus", "nodus").await?;
    for statement in [
        "CREATE TABLE restore_drill (id INT PRIMARY KEY, val TEXT)",
        "CREATE INDEX restore_drill_val_idx ON restore_drill (val)",
        "CREATE ROLE drill_read_group",
        "CREATE ROLE drill_reader LOGIN PASSWORD 'drill-reader-secret'",
        "GRANT drill_read_group TO drill_reader",
        "GRANT SELECT ON restore_drill TO drill_read_group",
        "INSERT INTO restore_drill (id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        "DELETE FROM restore_drill WHERE id = 2",
    ] {
        client.simple_query(statement).await?;
    }

    let full_id = create_backup(&http, source.http_addr, None).await?;
    for statement in [
        "UPDATE restore_drill SET val = 'one-inc' WHERE id = 1",
        "INSERT INTO restore_drill (id, val) VALUES (2, 'two-inc'), (4, 'four-inc')",
        "DELETE FROM restore_drill WHERE id = 3",
    ] {
        client.simple_query(statement).await?;
    }
    let incremental_id = create_backup(&http, source.http_addr, Some(&full_id)).await?;

    client
        .simple_query("INSERT INTO restore_drill (id, val) VALUES (5, 'five-pitr')")
        .await?;
    client
        .simple_query("UPDATE restore_drill SET val = 'four-pitr' WHERE id = 4")
        .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let target_ts = now_micros();
    tokio::time::sleep(Duration::from_millis(200)).await;
    client
        .simple_query("INSERT INTO restore_drill (id, val) VALUES (6, 'after-target')")
        .await?;
    client
        .simple_query("DELETE FROM restore_drill WHERE id = 1")
        .await?;
    let archive_through = now_micros();
    drop(client);
    source.shutdown().await;
    wait_for_archive(repository_uri, archive_through).await?;

    let full_expected = vec![(1, "one".into()), (3, "three".into())];
    let incremental_expected = vec![
        (1, "one-inc".into()),
        (2, "two-inc".into()),
        (4, "four-inc".into()),
    ];
    let pitr_expected = vec![
        (1, "one-inc".into()),
        (2, "two-inc".into()),
        (4, "four-pitr".into()),
        (5, "five-pitr".into()),
    ];

    for (name, backup_id, target, expected) in [
        ("full", full_id.as_str(), None, &full_expected),
        (
            "incremental",
            incremental_id.as_str(),
            None,
            &incremental_expected,
        ),
        (
            "pitr",
            incremental_id.as_str(),
            Some(target_ts),
            &pitr_expected,
        ),
    ] {
        let scenario = run_scenario(
            root.path(),
            repository_uri,
            name,
            backup_id,
            target,
            expected,
        )
        .await?;
        let passed = scenario["passed"] == true;
        scenarios.push(scenario);
        if !passed {
            bail!("{name} restore drill found a semantic mismatch");
        }
    }
    Ok(())
}

fn report_path(drill: &str) -> PathBuf {
    let variable = if drill == "s3" {
        "NODUS_S3_RESTORE_DRILL_REPORT"
    } else {
        "NODUS_RESTORE_DRILL_REPORT"
    };
    std::env::var_os(variable).map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/restore-drill")
                .join(format!("{drill}.json"))
        },
        PathBuf::from,
    )
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path.parent().context("report path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("report path has no file name")?;
    let temporary = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
    std::fs::write(&temporary, contents)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn persist_report(path: &Path, report: &mut Value) -> Result<()> {
    let previous = std::fs::read_to_string(path)
        .ok()
        .and_then(|body| serde_json::from_str::<Value>(&body).ok());
    let now_seconds = now_micros() / 1_000_000;
    let passed = report["status"] == "passed";
    let last_success = if passed {
        Some(now_seconds)
    } else {
        previous
            .as_ref()
            .and_then(|value| value["last_success_timestamp_seconds"].as_u64())
    };
    let last_failure = if passed {
        previous
            .as_ref()
            .and_then(|value| value["last_failure_timestamp_seconds"].as_u64())
    } else {
        Some(now_seconds)
    };
    if let Some(timestamp) = last_success {
        report["last_success_timestamp_seconds"] = json!(timestamp);
    }
    if let Some(timestamp) = last_failure {
        report["last_failure_timestamp_seconds"] = json!(timestamp);
    }
    atomic_write(path, serde_json::to_string_pretty(report)?.as_bytes())?;

    let drill = report["drill"].as_str().context("report omitted drill")?;
    let mut metrics = format!(
        "# HELP nodus_restore_drill_success Whether the latest restore drill passed.\n\
         # TYPE nodus_restore_drill_success gauge\n\
         nodus_restore_drill_success{{drill=\"{drill}\"}} {}\n",
        if passed { 1 } else { 0 }
    );
    if let Some(timestamp) = last_success {
        metrics.push_str(&format!(
            "# HELP nodus_restore_drill_last_success_timestamp_seconds Unix time of the last successful restore drill.\n\
             # TYPE nodus_restore_drill_last_success_timestamp_seconds gauge\n\
             nodus_restore_drill_last_success_timestamp_seconds{{drill=\"{drill}\"}} {timestamp}\n"
        ));
    }
    if let Some(timestamp) = last_failure {
        metrics.push_str(&format!(
            "# HELP nodus_restore_drill_last_failure_timestamp_seconds Unix time of the last failed restore drill.\n\
             # TYPE nodus_restore_drill_last_failure_timestamp_seconds gauge\n\
             nodus_restore_drill_last_failure_timestamp_seconds{{drill=\"{drill}\"}} {timestamp}\n"
        ));
    }
    let data_loss: u64 = report["scenarios"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|scenario| scenario["observed_data_loss_rows"].as_u64())
        .sum();
    metrics.push_str(&format!(
        "# HELP nodus_restore_drill_observed_data_loss_rows Expected rows missing after the latest drill.\n\
         # TYPE nodus_restore_drill_observed_data_loss_rows gauge\n\
         nodus_restore_drill_observed_data_loss_rows{{drill=\"{drill}\"}} {data_loss}\n"
    ));
    metrics.push_str(
        "# HELP nodus_restore_drill_recovery_time_seconds Restore, validation, and restart time by scenario.\n\
         # TYPE nodus_restore_drill_recovery_time_seconds gauge\n",
    );
    for scenario in report["scenarios"].as_array().into_iter().flatten() {
        let name = scenario["scenario"].as_str().unwrap_or("unknown");
        let seconds = scenario["total_recovery_ms"].as_u64().unwrap_or(0) as f64 / 1000.0;
        metrics.push_str(&format!(
            "nodus_restore_drill_recovery_time_seconds{{drill=\"{drill}\",scenario=\"{name}\"}} {seconds}\n"
        ));
    }
    atomic_write(&path.with_extension("prom"), metrics.as_bytes())
}

async fn run_and_report(drill: &str, repository_uri: &str) -> Result<()> {
    let path = report_path(drill);
    let started_at = now_micros();
    let mut scenarios = Vec::new();
    let result = execute_drill(repository_uri, &mut scenarios).await;
    let mut report = json!({
        "schema_version": 1,
        "drill": drill,
        "repository": if drill == "s3" { "s3" } else { "filesystem" },
        "repository_uri": repository_uri,
        "started_at_unix_micros": started_at,
        "finished_at_unix_micros": now_micros(),
        "status": if result.is_ok() { "passed" } else { "failed" },
        "error": result.as_ref().err().map(ToString::to_string),
        "scenarios": scenarios,
    });
    persist_report(&path, &mut report)?;
    println!("restore drill report: {}", path.display());
    result
}

#[tokio::test(flavor = "multi_thread")]
async fn persistent_full_incremental_pitr_restore_drill() {
    let root = std::env::temp_dir().join(format!(
        "nodus-restore-drill-repository-{}-{}",
        std::process::id(),
        now_micros()
    ));
    let uri = format!("file://{}", root.to_string_lossy());
    let result = run_and_report("filesystem", &uri).await;
    if result.is_ok() {
        let _ = std::fs::remove_dir_all(&root);
    } else {
        eprintln!("preserve failed drill repository: {}", root.display());
    }
    result.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_full_incremental_pitr_restore_drill() {
    let Ok(endpoint) = std::env::var("NODUS_S3_ENDPOINT") else {
        eprintln!("skipping live S3 restore drill: NODUS_S3_ENDPOINT is not set");
        return;
    };
    let bucket = std::env::var("NODUS_S3_BUCKET")
        .expect("NODUS_S3_BUCKET is required with NODUS_S3_ENDPOINT");
    for variable in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ENDPOINT_URL",
        "AWS_ALLOW_HTTP",
    ] {
        assert!(
            std::env::var_os(variable).is_some(),
            "{variable} is required for the live S3 restore drill"
        );
    }
    assert_eq!(endpoint, std::env::var("AWS_ENDPOINT_URL").unwrap());
    let prefix = format!("restore-drill-{}-{}", std::process::id(), now_micros());
    let uri = format!("s3://{bucket}/{prefix}");
    run_and_report("s3", &uri).await.unwrap();
}
