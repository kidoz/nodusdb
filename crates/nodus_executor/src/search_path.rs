//! Resolving relation names as PostgreSQL does: an unqualified name is the
//! first relation of that name in the session's temporary schema, then the
//! schemas of its `search_path` in order; `pg_temp` names the session's own
//! temporary schema.

use std::collections::HashSet;
use std::sync::Mutex;

/// A schema name that lives as long as the process: there are only as many
/// as schemas the sessions name.
fn intern(name: &str) -> &'static str {
    static NAMES: Mutex<Option<HashSet<&'static str>>> = Mutex::new(None);
    let mut names = NAMES.lock().unwrap_or_else(|e| e.into_inner());
    let names = names.get_or_insert_with(HashSet::new);
    if let Some(found) = names.get(name) {
        return found;
    }
    let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
    names.insert(leaked);
    leaked
}

/// The name of a session's temporary schema, `pg_temp_<n>`.
pub(crate) fn temp_schema_of(backend_pid: i64) -> String {
    format!("pg_temp_{backend_pid}")
}

/// The statement's session's temporary schema, while it runs.
pub(crate) fn temp_schema() -> Option<&'static str> {
    crate::session_env::with(|env| env.map(|e| e.backend_pid))
        .map(|pid| intern(&temp_schema_of(pid)))
}

/// Whether a schema is some session's temporary schema.
pub(crate) fn is_temp_schema(schema: &str) -> bool {
    schema.starts_with("pg_temp_")
}

/// A schema as written, with `pg_temp` standing for the session's own
/// temporary schema.
pub(crate) fn schema_named(name: &str) -> &str {
    if name.eq_ignore_ascii_case("pg_temp") {
        temp_schema().unwrap_or("pg_temp")
    } else {
        name
    }
}

/// The schemas of the session's `search_path`, as written (`$user` as the
/// session's user, `pg_temp` as its temporary schema).
pub(crate) fn search_path() -> Vec<String> {
    let user = crate::session_env::with(|env| env.map(|e| e.user.clone()));
    crate::session_env::setting("search_path")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .filter_map(|s| match s.as_str() {
            "$user" => user.clone(),
            _ => Some(schema_named(&s).to_string()),
        })
        .collect()
}

/// The schemas of the `search_path` that exist, in order.
pub(crate) fn existing_search_path() -> Vec<String> {
    let catalog = crate::session_env::with(|env| env.and_then(|e| e.catalog.clone()));
    search_path()
        .into_iter()
        .filter(|schema| {
            schema == "pg_catalog"
                || catalog
                    .as_ref()
                    .is_none_or(|c| c.get_schema("default", schema).is_ok())
        })
        .collect()
}

/// The schema an unqualified relation name means: the session's temporary
/// schema when it has the relation, else the first schema of the
/// `search_path` that does, else (to create it) the first that exists.
/// Outside a statement, `public`.
pub(crate) fn schema_for(relation: &str) -> &'static str {
    let Some(catalog) = crate::session_env::with(|env| env.and_then(|e| e.catalog.clone())) else {
        return "public";
    };
    let has = |schema: &str| catalog.get_table("default", schema, relation).is_ok();
    if let Some(temp) = temp_schema()
        && has(temp)
    {
        return temp;
    }
    let schemas = existing_search_path();
    if let Some(schema) = schemas.iter().find(|s| has(s)) {
        return intern(schema);
    }
    schemas
        .into_iter()
        .find(|s| s != "pg_catalog" && !is_temp_schema(s))
        .map_or("public", |s| intern(&s))
}
