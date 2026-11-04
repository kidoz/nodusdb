use std::path::Path;
use std::process::{Command, Output};

fn migrate(dir: &Path, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_nodus_cli"))
        .args(["storage", "migrate-legacy", "--data-dir"])
        .arg(dir)
        .args(["--active-wal", "1"])
        .args(extra)
        // Offline storage commands must not construct an HTTP client or use
        // an unrelated token from the environment.
        .env("NODUS_ADMIN_TOKEN", "invalid\nheader")
        .output()
        .unwrap()
}

#[test]
fn offline_cli_migrates_explicit_legacy_wal_without_admin_client() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("1.log"), []).unwrap();
    let result = migrate(dir.path(), &[]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["status"], "migrated");
    assert_eq!(report["active_wal"], 1);
    nodus_storage_lsm::LsmKvEngine::with_wal(dir.path(), None).unwrap();
    let manifest = std::fs::read(dir.path().join("MANIFEST")).unwrap();
    assert!(!migrate(dir.path(), &[]).status.success());
    assert_eq!(
        std::fs::read(dir.path().join("MANIFEST")).unwrap(),
        manifest
    );
}

#[test]
fn offline_cli_rejects_corrupt_wal_without_repair() {
    let dir = tempfile::tempdir().unwrap();
    let original = b"incomplete WAL frame";
    std::fs::write(dir.path().join("1.log"), original).unwrap();
    let result = migrate(dir.path(), &[]);
    assert!(!result.status.success());
    assert_eq!(std::fs::read(dir.path().join("1.log")).unwrap(), original);
    assert!(!dir.path().join("MANIFEST").exists());
}

#[test]
fn invalid_migration_key_is_not_echoed_or_published() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("1.log"), []).unwrap();
    let key = tempfile::NamedTempFile::new().unwrap();
    let secret = "secret-that-is-not-a-hexadecimal-key";
    std::fs::write(key.path(), secret).unwrap();
    let result = migrate(
        dir.path(),
        &["--wal-key-file", key.path().to_str().unwrap()],
    );
    assert!(!result.status.success());
    assert!(!String::from_utf8_lossy(&result.stderr).contains(secret));
    assert!(!dir.path().join("MANIFEST").exists());
}
