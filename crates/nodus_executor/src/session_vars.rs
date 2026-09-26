//! Per-session GUC (run-time configuration) variables.
//!
//! PostgreSQL keeps `SET`/`SHOW` state per connection. NodusDB mirrors that with
//! a `session_id`-keyed overlay on [`MemExecutor`] (the same keying used for
//! active transactions), so one connection's `SET search_path` can't leak into
//! another's. Values not explicitly set fall back to [`default_session_var`],
//! which matches what the startup `ParameterStatus` burst and `pg_settings`
//! advertise. The overlay is dropped when the session ends (see
//! `MemExecutor::end_session`) so it can't grow without bound.

/// One of PostgreSQL's run-time settings, as `pg_settings` describes it.
pub(crate) struct SettingInfo {
    /// The name as PostgreSQL spells it (`TimeZone`).
    pub(crate) name: &'static str,
    /// The value as `SHOW` reports it (`4MB`), and in the setting's unit.
    pub(crate) show: &'static str,
    pub(crate) setting: &'static str,
    /// When it may change: `user`, `superuser`, `postmaster`, `internal`, ...
    pub(crate) context: &'static str,
    pub(crate) vartype: &'static str,
    pub(crate) category: &'static str,
    pub(crate) short_desc: &'static str,
    pub(crate) unit: &'static str,
    pub(crate) min_val: &'static str,
    pub(crate) max_val: &'static str,
    /// An enum setting's values, as an array literal (`{debug5,...}`).
    pub(crate) enumvals: &'static str,
    pub(crate) boot_val: &'static str,
}

/// PostgreSQL 18's settings, with their defaults (generated from
/// `pg_settings`; paths and the server version are NodusDB's).
pub(crate) fn settings_table() -> &'static [SettingInfo] {
    static TABLE: std::sync::OnceLock<Vec<SettingInfo>> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| {
        include_str!("pg_settings.tsv")
            .lines()
            .filter_map(|line| {
                let f: Vec<&'static str> = line.split('\t').collect();
                let [
                    name,
                    show,
                    setting,
                    context,
                    vartype,
                    category,
                    short_desc,
                    unit,
                    min_val,
                    max_val,
                    enumvals,
                    boot_val,
                ] = f[..]
                else {
                    return None;
                };
                Some(SettingInfo {
                    name,
                    show,
                    setting,
                    context,
                    vartype,
                    category,
                    short_desc,
                    unit,
                    min_val,
                    max_val,
                    enumvals,
                    boot_val,
                })
            })
            .collect()
    })
}

/// The setting of that (case-insensitive) name.
pub(crate) fn setting_info(name: &str) -> Option<&'static SettingInfo> {
    let name = name.trim();
    settings_table()
        .iter()
        .find(|s| s.name.eq_ignore_ascii_case(name))
}

/// Built-in default a freshly connected session reports for a setting:
/// NodusDB's own for the few it models, else PostgreSQL's. `None` for a
/// name that is no setting.
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
        _ => return setting_info(lower_name).map(|s| s.show),
    })
}

/// What a `SET` does to a setting.
#[derive(Debug, PartialEq)]
pub(crate) enum SetAction {
    /// Takes this value.
    Set(String),
    /// Returns to its value at the session's start (`DEFAULT`, `LOCAL`).
    Reset,
}

/// Checks `SET name = raw` as PostgreSQL does and gives the value the
/// setting keeps: an unknown name, a setting that cannot change in a
/// session, and a value of the wrong kind are errors. A name with a dot is
/// a custom setting, which takes any value.
pub(crate) fn set_action(name: &str, raw: &str) -> Result<SetAction, String> {
    let key = name.trim().to_ascii_lowercase();
    let value = normalize_var_value(raw);
    if value.eq_ignore_ascii_case("default")
        || (key == "timezone" && value.eq_ignore_ascii_case("local"))
    {
        return Ok(SetAction::Reset);
    }
    if key.contains('.') {
        return Ok(SetAction::Set(value));
    }
    let info = setting_info(&key)
        .ok_or_else(|| format!("unrecognized configuration parameter \"{key}\""))?;
    let display = info.name;
    match info.context {
        "internal" => return Err(format!("parameter \"{display}\" cannot be changed")),
        "postmaster" => {
            return Err(format!(
                "parameter \"{display}\" cannot be changed without restarting the server"
            ));
        }
        "sighup" => return Err(format!("parameter \"{display}\" cannot be changed now")),
        "backend" | "superuser-backend" => {
            return Err(format!(
                "parameter \"{display}\" cannot be set after connection start"
            ));
        }
        _ => {}
    }
    let invalid = || format!("invalid value for parameter \"{display}\": \"{value}\"");
    Ok(SetAction::Set(match info.vartype {
        "bool" => match value.to_ascii_lowercase().as_str() {
            "on" | "true" | "yes" | "1" | "t" | "y" => "on".to_string(),
            "off" | "false" | "no" | "0" | "f" | "n" => "off".to_string(),
            _ => return Err(format!("parameter \"{display}\" requires a Boolean value")),
        },
        "enum" => {
            let values = info.enumvals.trim_matches(|c| c == '{' || c == '}');
            values
                .split(',')
                .map(|v| v.trim_matches('"'))
                .find(|v| v.eq_ignore_ascii_case(&value))
                .map(str::to_string)
                .ok_or_else(|| {
                    crate::error_fields::DbError::new(invalid())
                        .hint(format!(
                            "Available values: {}.",
                            values.split(',').collect::<Vec<_>>().join(", ")
                        ))
                        .into_text()
                })?
        }
        "integer" | "real" => {
            let number = value
                .trim_end_matches(|c: char| c.is_ascii_alphabetic())
                .trim();
            if number.parse::<f64>().is_err() {
                return Err(invalid());
            }
            value
        }
        _ => canonical_setting_value(&key, &value),
    }))
}

/// A time setting (`statement_timeout`) in milliseconds: a number in
/// milliseconds, or with a unit (`100ms`, `5s`, `2min`, `1h`, `1d`).
pub(crate) fn duration_millis(value: &str) -> Option<u64> {
    let value = normalize_var_value(value);
    let split = value
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(value.len());
    let number: f64 = value[..split].trim().parse().ok()?;
    let per = match value[split..].trim().to_ascii_lowercase().as_str() {
        "" | "ms" => 1.0,
        "us" => 0.001,
        "s" => 1_000.0,
        "min" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => return None,
    };
    Some((number * per).max(0.0) as u64)
}

/// The settings PostgreSQL reports to the client (`ParameterStatus`) when
/// they change, under the names it reports them by.
pub(crate) fn reported_name(lower_name: &str) -> Option<&'static str> {
    Some(match lower_name {
        "application_name" => "application_name",
        "client_encoding" => "client_encoding",
        "datestyle" => "DateStyle",
        "default_transaction_read_only" => "default_transaction_read_only",
        "intervalstyle" => "IntervalStyle",
        "search_path" => "search_path",
        "standard_conforming_strings" => "standard_conforming_strings",
        "timezone" => "TimeZone",
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

/// The name `SHOW` reports a setting under: PostgreSQL's spelling of the few
/// mixed-case settings, else the lower-case name.
pub(crate) fn setting_display_name(name: &str) -> String {
    let key = name.trim().to_ascii_lowercase();
    match key.as_str() {
        "datestyle" => "DateStyle".to_string(),
        "intervalstyle" => "IntervalStyle".to_string(),
        "timezone" => "TimeZone".to_string(),
        _ => key,
    }
}

/// PostgreSQL's canonical spelling of a setting's value, as `SHOW` and the
/// ParameterStatus echo report it: `DateStyle` becomes `<style>, <order>`
/// (`set datestyle = iso` shows `ISO, MDY`) and the UTC/GMT zone names are
/// upper-cased. Other values are returned unchanged.
pub fn canonical_setting_value(name: &str, value: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "datestyle" => {
            let (mut style, mut order) = (None, None);
            for word in value
                .split([',', ' '])
                .map(str::trim)
                .filter(|w| !w.is_empty())
            {
                match word.to_ascii_lowercase().as_str() {
                    "iso" => style = Some("ISO"),
                    "sql" => style = Some("SQL"),
                    "postgres" => style = Some("Postgres"),
                    "german" => style = Some("German"),
                    "ymd" => order = Some("YMD"),
                    "dmy" | "euro" | "european" => order = Some("DMY"),
                    "mdy" | "us" | "noneuro" | "noneuropean" => order = Some("MDY"),
                    _ => return value.to_string(),
                }
            }
            format!("{}, {}", style.unwrap_or("ISO"), order.unwrap_or("MDY"))
        }
        "timezone" if value.eq_ignore_ascii_case("utc") || value.eq_ignore_ascii_case("gmt") => {
            value.to_ascii_uppercase()
        }
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setting_values_take_canonical_spelling() {
        assert_eq!(canonical_setting_value("DateStyle", "iso"), "ISO, MDY");
        assert_eq!(canonical_setting_value("datestyle", "iso, dmy"), "ISO, DMY");
        assert_eq!(
            canonical_setting_value("datestyle", "German"),
            "German, MDY"
        );
        assert_eq!(canonical_setting_value("timezone", "utc"), "UTC");
        assert_eq!(
            canonical_setting_value("timezone", "Europe/Paris"),
            "Europe/Paris"
        );
        assert_eq!(
            canonical_setting_value("search_path", "myschema"),
            "myschema"
        );
    }

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
        assert_eq!(default_session_var("work_mem"), Some("4MB"));
        assert_eq!(setting_info("timezone").map(|s| s.name), Some("TimeZone"));
    }

    #[test]
    fn durations_read_their_units() {
        assert_eq!(duration_millis("100ms"), Some(100));
        assert_eq!(duration_millis("'5s'"), Some(5_000));
        assert_eq!(duration_millis("2min"), Some(120_000));
        assert_eq!(duration_millis("250"), Some(250));
        assert_eq!(duration_millis("fast"), None);
    }

    #[test]
    fn sets_are_checked_as_postgresql_checks_them() {
        let set = |name: &str, value: &str| set_action(name, value);
        assert_eq!(
            set("enable_seqscan", "false"),
            Ok(SetAction::Set("off".into()))
        );
        assert_eq!(
            set("client_min_messages", "WARNING"),
            Ok(SetAction::Set("warning".into()))
        );
        assert_eq!(set("work_mem", "'64MB'"), Ok(SetAction::Set("64MB".into())));
        assert_eq!(
            set("myapp.user_id", "'42'"),
            Ok(SetAction::Set("42".into()))
        );
        assert_eq!(set("timezone", "LOCAL"), Ok(SetAction::Reset));
        assert_eq!(set("work_mem", "DEFAULT"), Ok(SetAction::Reset));
        assert!(
            set("nosuch_param", "1")
                .unwrap_err()
                .contains("unrecognized configuration parameter")
        );
        assert!(
            set("port", "1")
                .unwrap_err()
                .contains("without restarting the server")
        );
        assert!(
            set("server_version", "1")
                .unwrap_err()
                .contains("cannot be changed")
        );
        assert!(
            set("enable_seqscan", "maybe")
                .unwrap_err()
                .contains("requires a Boolean value")
        );
        assert!(
            set("client_min_messages", "loud")
                .unwrap_err()
                .contains("invalid value")
        );
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
