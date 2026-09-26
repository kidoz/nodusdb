//! Functions about the session: advisory locks, notifications, privilege
//! inquiries, object-name lookups (`to_regclass`), and backend signals.

use crate::eval_error::raise;
use crate::session_env;
use crate::{Value, render};
use nodus_authz::Action;
use nodus_catalog::ResourceRef;

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => render(other),
    }
}

/// A `void` result, which shows as nothing.
pub(crate) fn void() -> Value {
    Value::Text(String::new())
}

/// The return types of the functions here.
pub(crate) fn return_type(name: &str) -> Option<&'static str> {
    Some(match name {
        "PG_ADVISORY_LOCK"
        | "PG_ADVISORY_LOCK_SHARED"
        | "PG_ADVISORY_XACT_LOCK"
        | "PG_ADVISORY_XACT_LOCK_SHARED"
        | "PG_ADVISORY_UNLOCK_ALL"
        | "PG_NOTIFY"
        | "PG_SLEEP" => "VOID",
        "PG_TRY_ADVISORY_LOCK"
        | "PG_TRY_ADVISORY_LOCK_SHARED"
        | "PG_TRY_ADVISORY_XACT_LOCK"
        | "PG_TRY_ADVISORY_XACT_LOCK_SHARED"
        | "PG_ADVISORY_UNLOCK"
        | "PG_ADVISORY_UNLOCK_SHARED"
        | "HAS_TABLE_PRIVILEGE"
        | "HAS_SCHEMA_PRIVILEGE"
        | "HAS_DATABASE_PRIVILEGE"
        | "HAS_COLUMN_PRIVILEGE"
        | "HAS_ANY_COLUMN_PRIVILEGE"
        | "HAS_SEQUENCE_PRIVILEGE"
        | "HAS_FUNCTION_PRIVILEGE"
        | "PG_HAS_ROLE"
        | "PG_CANCEL_BACKEND"
        | "PG_TERMINATE_BACKEND" => "BOOLEAN",
        "TO_REGCLASS" => "REGCLASS",
        "TO_REGTYPE" => "REGTYPE",
        "TO_REGNAMESPACE" => "REGNAMESPACE",
        "TO_REGROLE" => "REGROLE",
        "TO_REGPROC" => "REGPROC",
        "TO_REGPROCEDURE" => "REGPROCEDURE",
        "PG_POSTMASTER_START_TIME" | "PG_CONF_LOAD_TIME" => "TIMESTAMPTZ",
        "PG_TRIGGER_DEPTH" => "INTEGER",
        "PG_CURRENT_XACT_ID_IF_ASSIGNED" => "BIGINT",
        _ => return None,
    })
}

/// Evaluates one of the functions here; `None` for another name or arity.
pub(crate) fn dispatch(name: &str, args: &[Value]) -> Option<Value> {
    let arg = |i: usize| args.get(i).unwrap_or(&Value::Null);
    Some(match name {
        "PG_ADVISORY_LOCK"
        | "PG_ADVISORY_LOCK_SHARED"
        | "PG_ADVISORY_XACT_LOCK"
        | "PG_ADVISORY_XACT_LOCK_SHARED"
        | "PG_TRY_ADVISORY_LOCK"
        | "PG_TRY_ADVISORY_LOCK_SHARED"
        | "PG_TRY_ADVISORY_XACT_LOCK"
        | "PG_TRY_ADVISORY_XACT_LOCK_SHARED"
        | "PG_ADVISORY_UNLOCK"
        | "PG_ADVISORY_UNLOCK_SHARED"
            if matches!(args.len(), 1 | 2) =>
        {
            advisory(name, args)
        }
        "PG_ADVISORY_UNLOCK_ALL" if args.is_empty() => {
            if let Some((locks, session)) = locks_and_session() {
                locks.unlock_all(&session);
            }
            void()
        }
        "PG_NOTIFY" if args.len() == 2 => {
            session_env::stage_notification(text(arg(0)), text(arg(1)));
            void()
        }
        "HAS_TABLE_PRIVILEGE" | "HAS_SEQUENCE_PRIVILEGE" | "HAS_ANY_COLUMN_PRIVILEGE"
            if matches!(args.len(), 2 | 3) =>
        {
            let (user, rest) = split_user(args, 3);
            match table_resource(&text(&rest[0])) {
                Ok(resource) => privilege(user, resource, &text(&rest[1]), |p| match p {
                    "SELECT" | "REFERENCES" => Some(Action::Select),
                    "INSERT" => Some(Action::Insert),
                    "UPDATE" | "TRIGGER" | "MAINTAIN" | "USAGE" => Some(Action::Update),
                    "DELETE" | "TRUNCATE" => Some(Action::Delete),
                    _ => None,
                }),
                Err(e) => raise(e),
            }
        }
        "HAS_COLUMN_PRIVILEGE" if matches!(args.len(), 3 | 4) => {
            let (user, rest) = split_user(args, 4);
            let table = text(&rest[0]);
            let column = text(&rest[1]);
            let exists = with_table(&table, |t| t.columns.iter().any(|c| c.name == column));
            match (table_resource(&table), exists) {
                (Err(e), _) => raise(e),
                (_, Some(false)) => raise(format!(
                    "column \"{column}\" of relation \"{table}\" does not exist"
                )),
                (Ok(resource), _) => privilege(user, resource, &text(&rest[2]), |p| match p {
                    "SELECT" | "REFERENCES" => Some(Action::Select),
                    "INSERT" => Some(Action::Insert),
                    "UPDATE" => Some(Action::Update),
                    _ => None,
                }),
            }
        }
        "HAS_SCHEMA_PRIVILEGE" if matches!(args.len(), 2 | 3) => {
            let (user, rest) = split_user(args, 3);
            let schema = text(&rest[0]);
            let catalog = session_env::with(|env| env.and_then(|e| e.catalog.clone()));
            match catalog.and_then(|c| c.get_schema("default", &schema).ok()) {
                Some(found) => privilege(
                    user,
                    ResourceRef::Schema(found.id),
                    &text(&rest[1]),
                    |p| match p {
                        "USAGE" => Some(Action::Usage),
                        "CREATE" => Some(Action::CreateTable),
                        _ => None,
                    },
                ),
                None if crate::MemExecutor::is_virtual_schema(&schema) => Value::Bool(true),
                None => raise(format!("schema \"{schema}\" does not exist")),
            }
        }
        "HAS_DATABASE_PRIVILEGE" if matches!(args.len(), 2 | 3) => {
            let (user, rest) = split_user(args, 3);
            let database = text(&rest[0]);
            let catalog = session_env::with(|env| env.and_then(|e| e.catalog.clone()));
            match catalog.and_then(|c| c.get_database(&database).ok()) {
                Some(found) => {
                    // Every role may create temporary tables.
                    let privileges = text(&rest[1]).to_ascii_uppercase();
                    if privileges
                        .split(',')
                        .all(|p| matches!(p.trim(), "TEMP" | "TEMPORARY"))
                    {
                        Value::Bool(true)
                    } else {
                        privilege(
                            user,
                            ResourceRef::Database(found.id),
                            &privileges,
                            |p| match p {
                                "CONNECT" => Some(Action::Connect),
                                "CREATE" => Some(Action::CreateSchema),
                                "TEMP" | "TEMPORARY" => Some(Action::Connect),
                                _ => None,
                            },
                        )
                    }
                }
                None => raise(format!("database \"{database}\" does not exist")),
            }
        }
        "HAS_FUNCTION_PRIVILEGE" if matches!(args.len(), 2 | 3) => {
            let (_, rest) = split_user(args, 3);
            let function = text(&rest[0]);
            let base = function
                .split('(')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_uppercase();
            if !crate::functions::is_known(base.strip_prefix("PG_CATALOG.").unwrap_or(&base)) {
                return Some(raise(format!("function \"{function}\" does not exist")));
            }
            match text(&rest[1]).to_ascii_uppercase().trim() {
                "EXECUTE" => Value::Bool(true),
                other => raise(format!(
                    "unrecognized privilege type: \"{}\"",
                    other.to_ascii_lowercase()
                )),
            }
        }
        "PG_HAS_ROLE" if matches!(args.len(), 2 | 3) => {
            let (user, rest) = split_user(args, 3);
            let role = text(&rest[0]);
            let user = user.unwrap_or_else(current_user);
            let catalog = session_env::with(|env| env.and_then(|e| e.catalog.clone()));
            let Some(catalog) = catalog else {
                return Some(Value::Bool(user == role));
            };
            let Ok(role_principal) = catalog.get_principal_by_name(&role) else {
                return Some(raise(format!("role \"{role}\" does not exist")));
            };
            let member = catalog.get_principal_by_name(&user).is_ok_and(|p| {
                p.id == role_principal.id
                    || is_superuser(&user)
                    || catalog
                        .get_effective_principals(p.id)
                        .is_ok_and(|all| all.contains(&role_principal.id))
            });
            Value::Bool(member)
        }
        // A type NodusDB does not know is no type (not text).
        "TO_REGTYPE" if args.len() == 1 => {
            let name = text(arg(0));
            let oid = crate::MemExecutor::pg_type_oid(&name);
            if oid == 25
                && !matches!(
                    name.trim().to_ascii_lowercase().as_str(),
                    "text" | "pg_catalog.text"
                )
            {
                Value::Null
            } else {
                crate::planner::try_cast(Value::Text(name), "REGTYPE").unwrap_or(Value::Null)
            }
        }
        "TO_REGCLASS" | "TO_REGNAMESPACE" if args.len() == 1 => {
            let kind = &name[3..];
            crate::planner::try_cast(Value::Text(text(arg(0))), kind).unwrap_or(Value::Null)
        }
        "TO_REGROLE" if args.len() == 1 => {
            let role = text(arg(0));
            let catalog = session_env::with(|env| env.and_then(|e| e.catalog.clone()));
            match catalog.map(|c| c.get_principal_by_name(&role).is_ok()) {
                Some(true) => Value::Text(role),
                _ => Value::Null,
            }
        }
        "TO_REGPROC" | "TO_REGPROCEDURE" if args.len() == 1 => {
            let function = text(arg(0));
            let base = function
                .split('(')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_uppercase();
            if crate::functions::is_known(base.strip_prefix("PG_CATALOG.").unwrap_or(&base)) {
                Value::Text(function)
            } else {
                Value::Null
            }
        }
        // Only the session's own backend is signalled; others are not known here.
        "PG_CANCEL_BACKEND" | "PG_TERMINATE_BACKEND" if matches!(args.len(), 1 | 2) => {
            let pid = match arg(0) {
                Value::Int(pid) => *pid,
                other => text(other).trim().parse().unwrap_or(0),
            };
            let own = session_env::with(|env| env.map(|e| e.backend_pid)) == Some(pid);
            if !own {
                session_env::notice(
                    crate::error_fields::DbError::new(format!(
                        "PID {pid} is not a PostgreSQL backend process"
                    ))
                    .severity("WARNING"),
                );
            }
            Value::Bool(own)
        }
        "PG_POSTMASTER_START_TIME" | "PG_CONF_LOAD_TIME" if args.is_empty() => {
            chrono::DateTime::from_timestamp_micros(server_started()).map_or(Value::Null, |dt| {
                crate::datetime::Temporal::TimestampTz(dt.naive_utc()).to_value()
            })
        }
        "PG_TRIGGER_DEPTH" if args.is_empty() => Value::Int(0),
        "PG_CURRENT_XACT_ID_IF_ASSIGNED" if args.is_empty() => {
            session_env::with(|env| env.map(|e| e.transaction_micros))
                .map_or(Value::Null, Value::Int)
        }
        _ => return None,
    })
}

/// When the server started, microseconds since the epoch; noted when its
/// executor is made.
pub(crate) fn server_started() -> i64 {
    static STARTED: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *STARTED.get_or_init(session_env::wall_micros)
}

fn locks_and_session() -> Option<(std::sync::Arc<crate::advisory::AdvisoryLocks>, String)> {
    session_env::with(|env| env.and_then(|e| Some((e.advisory.clone()?, e.session_id.clone()))))
}

/// The advisory lock functions.
fn advisory(name: &str, args: &[Value]) -> Value {
    let Some((locks, session)) = locks_and_session() else {
        return raise(format!(
            "{} cannot be evaluated here",
            name.to_ascii_lowercase()
        ));
    };
    let number = |v: &Value| match v {
        Value::Int(i) => Some(*i),
        other => text(other).trim().parse::<i64>().ok(),
    };
    let key = match args {
        [one] => number(one).map(|k| (false, k)),
        [high, low] => number(high)
            .zip(number(low))
            .map(|(h, l)| (true, (h << 32) | (l & 0xffff_ffff))),
        _ => None,
    };
    let Some(key) = key else {
        return raise("invalid advisory lock key");
    };
    let exclusive = !name.contains("SHARED");
    if name.starts_with("PG_ADVISORY_UNLOCK") {
        let released = locks.unlock(&session, key, exclusive);
        if !released {
            let mode = if exclusive {
                "ExclusiveLock"
            } else {
                "ShareLock"
            };
            session_env::notice(
                crate::error_fields::DbError::new(format!("you don't own a lock of type {mode}"))
                    .severity("WARNING"),
            );
        }
        return Value::Bool(released);
    }
    let transaction = name.contains("XACT");
    if name.starts_with("PG_TRY") {
        return Value::Bool(locks.try_lock(&session, key, exclusive, transaction));
    }
    // Waits no longer than the statement may still run.
    let limit = session_env::setting("statement_timeout")
        .and_then(|t| crate::session_vars::duration_millis(&t))
        .filter(|ms| *ms > 0);
    let started = session_env::with(|env| env.map(|e| e.statement_micros)).unwrap_or_default();
    let deadline = limit.map(|ms| {
        let spent = (session_env::wall_micros() - started).max(0) as u64;
        std::time::Instant::now()
            + std::time::Duration::from_micros((ms * 1_000).saturating_sub(spent))
    });
    if locks.lock(&session, key, exclusive, transaction, deadline) {
        void()
    } else {
        raise("canceling statement due to statement timeout")
    }
}

/// The optional leading user argument of the privilege functions, and the
/// rest.
fn split_user(args: &[Value], full: usize) -> (Option<String>, &[Value]) {
    if args.len() == full {
        let user = match &args[0] {
            Value::Int(10) => "nodus".to_string(),
            other => text(other),
        };
        (Some(user), &args[1..])
    } else {
        (None, args)
    }
}

fn current_user() -> String {
    session_env::with(|env| env.map(|e| e.user.clone())).unwrap_or_default()
}

fn is_superuser(user: &str) -> bool {
    user == "nodus"
}

/// The resource a table name means, or why there is none.
fn table_resource(name: &str) -> Result<ResourceRef, String> {
    let (db, schema, relation) =
        crate::planner::parse_object_name(name).map_err(|e| e.to_string())?;
    let catalog = session_env::with(|env| env.and_then(|e| e.catalog.clone()))
        .ok_or_else(|| "the catalog is not available here".to_string())?;
    match catalog.get_table(db, schema, relation) {
        Ok(table) => Ok(ResourceRef::Table(table.id)),
        // The system catalogs are readable by everyone.
        Err(_) if crate::MemExecutor::is_pg_catalog_virtual_table_name(relation) => Ok(
            ResourceRef::Database(catalog.get_database(db).map_err(|e| e.to_string())?.id),
        ),
        Err(_) => Err(format!("relation \"{name}\" does not exist")),
    }
}

fn with_table<T>(name: &str, f: impl FnOnce(&nodus_catalog::TableDescriptor) -> T) -> Option<T> {
    let (db, schema, relation) = crate::planner::parse_object_name(name).ok()?;
    let catalog = session_env::with(|env| env.and_then(|e| e.catalog.clone()))?;
    catalog.get_table(db, schema, relation).ok().map(|t| f(&t))
}

/// Whether the user (the session's, else the one named) holds any of the
/// comma-separated `privileges` (`WITH GRANT OPTION` aside) on `resource`.
fn privilege(
    user: Option<String>,
    resource: ResourceRef,
    privileges: &str,
    action: impl Fn(&str) -> Option<Action>,
) -> Value {
    let mut actions = Vec::new();
    for privilege in privileges.split(',') {
        let upper = privilege.trim().to_ascii_uppercase();
        let upper = upper.trim_end_matches("WITH GRANT OPTION").trim();
        match action(upper) {
            Some(a) => actions.push(a),
            None => {
                return raise(format!(
                    "unrecognized privilege type: \"{}\"",
                    privilege.trim()
                ));
            }
        }
    }
    let Some((authz, session_principal)) =
        session_env::with(|env| env.and_then(|e| e.authz.clone()))
    else {
        return Value::Bool(true);
    };
    let principal = match &user {
        Some(name) => {
            let catalog = session_env::with(|env| env.and_then(|e| e.catalog.clone()));
            match catalog.map(|c| c.get_principal_by_name(name)) {
                Some(Ok(p)) => p.id,
                _ => return raise(format!("role \"{name}\" does not exist")),
            }
        }
        None => session_principal,
    };
    if is_superuser(&user.unwrap_or_else(current_user)) {
        return Value::Bool(true);
    }
    let allowed = actions.into_iter().any(|action| {
        authz
            .authorize(nodus_authz::AuthzRequest {
                principal_id: principal,
                active_roles: vec![],
                action,
                resource: resource.clone(),
                context: nodus_authz::AuthzContext { database_id: None },
            })
            .is_ok_and(|d| d.allowed)
    });
    Value::Bool(allowed)
}
