//! Per-session GUC (run-time configuration) variables.
//!
//! PostgreSQL keeps `SET`/`SHOW` state per connection. NodusDB mirrors that with
//! a `session_id`-keyed overlay on [`MemExecutor`] (the same keying used for
//! active transactions), so one connection's `SET search_path` can't leak into
//! another's. Values not explicitly set fall back to [`default_session_var`],
//! which matches what the startup `ParameterStatus` burst and `pg_settings`
//! advertise. The overlay is dropped when the session ends (see
//! `MemExecutor::end_session`) so it can't grow without bound.

/// Built-in default a freshly connected session reports for a known GUC.
///
/// Returns `None` for variables NodusDB does not model; `SHOW` of an unknown
/// variable then yields an empty string (PostgreSQL would raise
/// `unrecognized configuration parameter`, but several drivers probe optional
/// GUCs and tolerate a blank answer better than an error).
pub(crate) fn default_session_var(lower_name: &str) -> Option<&'static str> {
    Some(match lower_name {
        // NodusDB resolves unqualified names in a single `public` schema today,
        // so the effective search path is just `public` (not `"$user", public`).
        "search_path" => "public",
        "application_name" => "",
        "client_encoding" => "UTF8",
        "datestyle" => "ISO, MDY",
        "timezone" => "UTC",
        "intervalstyle" => "postgres",
        "standard_conforming_strings" => "on",
        "integer_datetimes" => "on",
        "bytea_output" => "hex",
        "server_encoding" => "UTF8",
        "server_version" => "18.0",
        "server_version_num" => "180000",
        "is_superuser" => "on",
        "session_authorization" => "nodus",
        "transaction_isolation" => "read committed",
        "default_transaction_isolation" => "read committed",
        "transaction_read_only" => "off",
        "default_transaction_read_only" => "off",
        "statement_timeout" => "0",
        "lc_collate" => "C",
        "lc_ctype" => "C",
        _ => return None,
    })
}

/// Normalizes a value as it arrives from the planner (which renders the parsed
/// `SET` expression) into the bare string `SHOW` should echo: strips one layer
/// of surrounding single/double quotes and trims whitespace.
pub(crate) fn normalize_var_value(raw: &str) -> String {
    let trimmed = raw.trim();
    let bytes = trimmed.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_defaults_resolve() {
        assert_eq!(default_session_var("search_path"), Some("public"));
        assert_eq!(default_session_var("client_encoding"), Some("UTF8"));
        assert_eq!(
            default_session_var("default_transaction_read_only"),
            Some("off")
        );
    }

    #[test]
    fn unknown_var_has_no_default() {
        assert_eq!(default_session_var("not_a_real_guc"), None);
    }

    #[test]
    fn normalize_strips_one_quote_layer() {
        assert_eq!(normalize_var_value("'UTC'"), "UTC");
        assert_eq!(normalize_var_value("  'ISO, MDY' "), "ISO, MDY");
        assert_eq!(normalize_var_value("\"app\""), "app");
        assert_eq!(normalize_var_value("3"), "3");
        assert_eq!(normalize_var_value("read committed"), "read committed");
    }
}
