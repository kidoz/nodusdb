//! Per-statement session context for scalar functions whose result depends on
//! who runs the statement and when: `current_user`, `now()`,
//! `current_setting()`, and the like.
//!
//! The executor installs an environment for each statement it runs; nested
//! executions (subqueries, CTEs) see the same one. Evaluation outside a
//! statement (while planning) has no environment, and session functions report
//! that they are unavailable there instead of guessing.

use std::cell::RefCell;
use std::collections::HashMap;

pub(crate) struct SessionEnv {
    /// The session's authenticated role name.
    pub(crate) user: String,
    /// Effective run-time settings: the session's `SET` overrides (lowercase
    /// names). Unset variables fall back to their built-in defaults.
    pub(crate) settings: HashMap<String, String>,
    /// Settings `set_config` changed in this statement, applied when it
    /// succeeds: name, value (`None`: the default), and `is_local`.
    pub(crate) staged_settings: Vec<(String, Option<String>, bool)>,
    /// Whether the statement runs in an explicit transaction.
    pub(crate) in_explicit_txn: bool,
    /// Start of the current transaction (`now()`), microseconds since the epoch.
    pub(crate) transaction_micros: i64,
    /// Start of the current statement (`statement_timestamp()`).
    pub(crate) statement_micros: i64,
    /// A stable per-session process id for `pg_backend_pid()`.
    pub(crate) backend_pid: i64,
    /// The session's id, which keys per-session state such as `currval`.
    pub(crate) session_id: String,
    /// Sequence access for `nextval`, `setval`, `currval`, and `lastval`.
    pub(crate) sequences: Option<std::sync::Arc<crate::sequences::SequenceStore>>,
    /// The catalog, for functions that describe objects by OID
    /// (`pg_get_indexdef`, `pg_get_constraintdef`).
    pub(crate) catalog: Option<std::sync::Arc<dyn nodus_catalog::CatalogReader>>,
    /// The stored data as the statement reads it, for the functions that
    /// measure relations (`pg_table_size`).
    pub(crate) storage: Option<(
        std::sync::Arc<dyn nodus_storage_api::KvEngine>,
        nodus_storage_api::Timestamp,
    )>,
}

thread_local! {
    static ENV: RefCell<Option<SessionEnv>> = const { RefCell::new(None) };
}

/// Restores the previous environment when the statement that installed this
/// one finishes, so a nested execution cannot clear its caller's.
pub(crate) struct EnvGuard {
    previous: Option<SessionEnv>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        ENV.with(|env| *env.borrow_mut() = self.previous.take());
    }
}

/// Installs `env` for the rest of the current statement.
pub(crate) fn install(env: SessionEnv) -> EnvGuard {
    EnvGuard {
        previous: ENV.with(|slot| slot.borrow_mut().replace(env)),
    }
}

/// Runs `f` with the current statement's environment, if one is installed.
pub(crate) fn with<R>(f: impl FnOnce(Option<&SessionEnv>) -> R) -> R {
    ENV.with(|env| f(env.borrow().as_ref()))
}

/// The effective value of a run-time setting, or `None` if it is unknown.
pub(crate) fn setting(name: &str) -> Option<String> {
    let key = name.trim().to_ascii_lowercase();
    with(|env| env.and_then(|e| e.settings.get(&key).cloned()))
        .or_else(|| crate::session_vars::default_session_var(&key).map(str::to_owned))
}

/// `set_config(name, value, is_local)`: the setting takes the value for the
/// rest of the statement at once, and in the session when it succeeds (a
/// local one only inside a transaction block, until it ends).
pub(crate) fn stage_setting(name: &str, value: Option<String>, local: bool) {
    ENV.with(|slot| {
        if let Some(env) = slot.borrow_mut().as_mut() {
            let key = name.trim().to_ascii_lowercase();
            match &value {
                Some(value) => env.settings.insert(key.clone(), value.clone()),
                None => env.settings.remove(&key),
            };
            if !local || env.in_explicit_txn {
                env.staged_settings.push((key, value, local));
            }
        }
    });
}

/// Takes the settings `set_config` staged in this statement.
pub(crate) fn take_staged_settings() -> Vec<(String, Option<String>, bool)> {
    ENV.with(|slot| {
        slot.borrow_mut()
            .as_mut()
            .map(|env| std::mem::take(&mut env.staged_settings))
            .unwrap_or_default()
    })
}

/// Microseconds since the Unix epoch, now.
pub(crate) fn wall_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}
