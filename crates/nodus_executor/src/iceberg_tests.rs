use super::*;
use nodus_common::iceberg::{STORED_FUNCTION, Settings};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

struct Fixture {
    directory: std::path::PathBuf,
    settings: Settings,
}
impl Fixture {
    fn new(mode: &str) -> Self {
        let directory =
            std::env::temp_dir().join(format!("nodus-iceberg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let worker = directory.join("worker");
        let source = include_str!("iceberg_test_worker.py")
            .replace("MODE = 'normal'", &format!("MODE = '{mode}'"));
        std::fs::write(&worker, source).unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
        let profiles = directory.join("profiles.json");
        std::fs::write(&profiles, r#"{"version":1,"profiles":{"lake":{"uri":"http://invalid.local","warehouse":null,"bearer_token":"private-canary","storage_properties":{}}}}"#).unwrap();
        std::fs::set_permissions(&profiles, std::fs::Permissions::from_mode(0o600)).unwrap();
        let settings = Settings {
            enabled: true,
            worker_executable: worker.to_str().unwrap().into(),
            profiles_file: profiles.to_str().unwrap().into(),
            ..Default::default()
        };
        Self {
            directory,
            settings,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
const REGISTER: &str =
    "CREATE VIEW lake_events AS SELECT * FROM nodus_iceberg('lake', 'interop', 'events')";

#[test]
fn iceberg_registration_projection_types_authorization_and_read_only() {
    let fixture = Fixture::new("normal");
    let (mut exec, ctx) = executor_with_kv(Arc::new(nodus_storage_mem::MemKvEngine::new()));
    let audit = Arc::new(MemoryAuditSink::new());
    Arc::get_mut(&mut exec).unwrap().audit = audit.clone();
    exec.configure_iceberg(fixture.settings.clone()).unwrap();
    sql(&exec, &ctx, REGISTER).unwrap();
    let table = exec
        .catalog_reader
        .get_table("default", "public", "lake_events")
        .unwrap();
    let stored = table.view_query.as_ref().unwrap();
    assert!(stored.contains(STORED_FUNCTION));
    assert!(!stored.contains("private-canary") && !stored.contains("invalid.local"));
    // Existing persisted plan enum/fields decode without a new table kind.
    let legacy_shape: LogicalPlan = serde_json::from_str(stored).unwrap();
    assert!(
        exec.execute_logical(&ctx, legacy_shape)
            .unwrap_err()
            .to_string()
            .contains("Unsupported table function")
    );
    let out = sql(
        &exec,
        &ctx,
        "SELECT name AS label, id, active, id FROM lake_events LIMIT 2",
    )
    .unwrap();
    assert_eq!(out.columns, ["label", "id", "active", "id"]);
    assert_eq!(out.types, ["TEXT", "BIGINT", "BOOLEAN", "BIGINT"]);
    assert_eq!(render_row(&out.rows[0]), ["alpha", "1", "t", "1"]);
    assert!(matches!(out.rows[1].values[0], Value::Null));
    for statement in [
        "INSERT INTO lake_events VALUES (3, 'x', true)",
        "UPDATE lake_events SET name = 'x'",
        "DELETE FROM lake_events",
        "TRUNCATE lake_events",
        "CREATE INDEX bad_index ON lake_events (id)",
        "ALTER TABLE lake_events RENAME COLUMN name TO bad_name",
        "MERGE INTO lake_events t USING (VALUES (3)) AS s(id) ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
        "ALTER TABLE lake_events ADD COLUMN bad INT",
        "SELECT * FROM lake_events WHERE id = 1",
        "SELECT count(*) FROM lake_events",
        "SELECT * FROM lake_events ORDER BY id",
        "SELECT * FROM lake_events OFFSET 1",
    ] {
        assert!(
            sql(&exec, &ctx, statement).is_err(),
            "unexpected success: {statement}"
        );
    }
    let user = exec
        .catalog_writer
        .create_role(CreateRoleRequest {
            id: PrincipalId::new(),
            name: "reader".into(),
            principal_type: PrincipalType::User,
            database_id: None,
            attributes: Default::default(),
        })
        .unwrap();
    let user_ctx = ctx_for(user.id);
    assert!(
        sql(&exec, &user_ctx, "SELECT * FROM lake_events")
            .unwrap_err()
            .to_string()
            .contains("permission denied")
    );
    assert!(
        sql(&exec, &user_ctx, REGISTER)
            .unwrap_err()
            .to_string()
            .contains("permission denied")
    );
    exec.catalog_writer
        .grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: user.id,
            resource: ResourceRef::Table(table.id),
            privilege: "SELECT".into(),
            grantable: false,
            grantor: None,
        })
        .unwrap();
    assert_eq!(
        sql(&exec, &user_ctx, "SELECT * FROM lake_events LIMIT 1")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(
        !audit
            .query(&AuditQuery {
                action: Some("SELECT".into()),
                result: Some("Denied".into()),
                ..Default::default()
            })
            .unwrap()
            .is_empty()
    );
    assert!(
        !audit
            .query(&AuditQuery {
                action: Some("SELECT".into()),
                result: Some("Success".into()),
                ..Default::default()
            })
            .unwrap()
            .is_empty()
    );
    exec.configure_iceberg(Settings::default()).unwrap();
    assert!(
        sql(&exec, &ctx, "SELECT * FROM lake_events")
            .unwrap_err()
            .to_string()
            .contains("not enabled")
    );
    sql(&exec, &ctx, "DROP VIEW lake_events").unwrap();
}

#[test]
fn iceberg_failed_registration_is_atomic_and_secrets_are_redacted() {
    let fixture = Fixture::new("error");
    let (exec, ctx) = executor_with_kv(Arc::new(nodus_storage_mem::MemKvEngine::new()));
    exec.configure_iceberg(fixture.settings.clone()).unwrap();
    let error = sql(&exec, &ctx, REGISTER).unwrap_err().to_string();
    assert!(!error.contains("private-canary"));
    assert!(
        exec.catalog_reader
            .get_table("default", "public", "lake_events")
            .is_err()
    );
    std::fs::set_permissions(
        &fixture.settings.profiles_file,
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(
        exec.configure_iceberg(fixture.settings.clone())
            .unwrap_err()
            .to_string()
            .contains("chmod 600")
    );
}

#[test]
fn iceberg_limits_drift_and_limit_zero() {
    for mode in ["normal", "drift"] {
        let fixture = Fixture::new(mode);
        let (exec, ctx) = executor_with_kv(Arc::new(nodus_storage_mem::MemKvEngine::new()));
        let mut settings = fixture.settings.clone();
        settings.max_rows = 1;
        exec.configure_iceberg(settings).unwrap();
        sql(&exec, &ctx, REGISTER).unwrap();
        assert!(sql(&exec, &ctx, "SELECT * FROM lake_events").is_err());
        assert!(sql(&exec, &ctx, "SELECT * FROM lake_events LIMIT 2").is_err());
        let out = sql(&exec, &ctx, "SELECT name FROM lake_events LIMIT 0").unwrap();
        assert!(out.rows.is_empty());
        assert_eq!(out.types, ["TEXT"]);
    }
}

#[test]
fn iceberg_worker_deadline_cancel_and_decoder_memory_are_bounded() {
    for mode in ["stall", "memory"] {
        let fixture = Fixture::new(mode);
        let (exec, mut ctx) = executor_with_kv(Arc::new(nodus_storage_mem::MemKvEngine::new()));
        let mut settings = fixture.settings.clone();
        settings.timeout_seconds = 1;
        settings.worker_memory_mib = 64;
        exec.configure_iceberg(settings).unwrap();
        let start = Instant::now();
        let error = sql(&exec, &ctx, REGISTER).unwrap_err().to_string();
        assert!(
            error.contains(if mode == "stall" {
                "statement timeout"
            } else {
                "memory limit"
            }),
            "{error}"
        );
        assert!(start.elapsed() < Duration::from_secs(3));
        let cancelled = Arc::new(AtomicBool::new(true));
        ctx.cancellation = Some(Arc::new(move || cancelled.load(Ordering::Acquire)));
        assert!(
            sql(&exec, &ctx, REGISTER)
                .unwrap_err()
                .to_string()
                .contains("user request")
        );
        assert_eq!(
            sql(&exec, &ctx_for(ctx.principal_id), "SELECT 1")
                .unwrap()
                .rows
                .len(),
            1
        );
    }
}

#[test]
fn iceberg_result_byte_limit_and_concurrent_scan_admission() {
    let fixture = Fixture::new("normal");
    let (exec, ctx) = executor_with_kv(Arc::new(nodus_storage_mem::MemKvEngine::new()));
    let mut settings = fixture.settings.clone();
    settings.max_result_bytes = 1;
    exec.configure_iceberg(settings).unwrap();
    sql(&exec, &ctx, REGISTER).unwrap();
    assert!(
        sql(&exec, &ctx, "SELECT * FROM lake_events LIMIT 1")
            .unwrap_err()
            .to_string()
            .contains("max_result_bytes")
    );

    let fixture = Fixture::new("stall");
    let mut settings = fixture.settings.clone();
    settings.max_concurrent_scans = 1;
    exec.configure_iceberg(settings).unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    let mut first_ctx = ctx.clone();
    first_ctx.session_id = "first-worker".into();
    first_ctx.cancellation = Some(Arc::new(move || flag.load(Ordering::Acquire)));
    let executor = exec.clone();
    let first = std::thread::spawn(move || {
        sql(
            &executor,
            &first_ctx,
            "CREATE VIEW first_scan AS SELECT * FROM nodus_iceberg('lake', 'interop', 'events')",
        )
    });
    let marker = fixture.directory.join("worker.started");
    let start = Instant::now();
    while !marker.exists() && start.elapsed() < Duration::from_secs(3) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(marker.exists(), "worker did not receive its request");
    let error = sql(
        &exec,
        &ctx,
        "CREATE VIEW second_scan AS SELECT * FROM nodus_iceberg('lake', 'interop', 'events')",
    )
    .unwrap_err();
    assert!(error.to_string().contains("concurrent scan limit"));
    cancelled.store(true, Ordering::Release);
    assert!(
        first
            .join()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("user request")
    );
    let pid = std::fs::read_to_string(marker).unwrap();
    assert!(
        !std::process::Command::new("/bin/ps")
            .args(["-p", pid.trim()])
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap()
            .success(),
        "worker was not reaped"
    );
}

#[test]
fn iceberg_ambiguous_quoted_column_names_fail_before_publication() {
    let fixture = Fixture::new("ambiguous");
    let (exec, ctx) = executor_with_kv(Arc::new(nodus_storage_mem::MemKvEngine::new()));
    exec.configure_iceberg(fixture.settings.clone()).unwrap();
    let error = sql(&exec, &ctx, REGISTER).unwrap_err().to_string();
    assert!(error.contains("column names must be ASCII identifiers"));
    assert!(error.contains("0A000"));
    assert!(
        exec.catalog_reader
            .get_table("default", "public", "lake_events")
            .is_err()
    );
}
