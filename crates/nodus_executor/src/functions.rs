//! Built-in scalar functions.
//!
//! [`is_known`] is the single registry the planner consults: a call to any
//! other function is rejected rather than evaluated to a placeholder. Most
//! functions are strict — a NULL argument yields NULL without calling them —
//! except those PostgreSQL defines otherwise (see [`NON_STRICT`]). Domain and
//! input errors fail the statement through [`crate::eval_error`].

use crate::eval_error::raise;
use crate::numeric::Numeric;
use crate::session_env;
use crate::value::{Value, render, values_equal};

/// Functions that receive NULL arguments instead of short-circuiting to NULL.
const NON_STRICT: &[&str] = &[
    // `ts_headline` is strict, but its planner-assigned shape carries a
    // NULL config slot when the call has none; the arm sorts it out.
    crate::result_types::TS_HEADLINE,
    // `xmlconcat` is not strict: it drops NULL arguments, and is NULL only
    // when every argument is.
    "XMLCONCAT",
    // The SQL/XML constructors: a NULL argument is skipped (or, for `xmlpi`,
    // a NULL value), and the target-name and attribute-name checks come
    // before the NULL checks.
    crate::result_types::XMLPI,
    crate::result_types::XMLROOT,
    crate::result_types::XMLELEMENT,
    crate::result_types::XMLFOREST,
    crate::result_types::XMLATTRIBUTES,
    // Range constructors read a NULL bound as unbounded; a multirange
    // constructor refuses a NULL among its members.
    "__MULTIRANGE_BUILD__",
    "INT4RANGE",
    "INT8RANGE",
    "NUMRANGE",
    "DATERANGE",
    "TSRANGE",
    "TSTZRANGE",
    "__SLICE__",
    "ARRAY",
    "COALESCE",
    "NULLIF",
    "GREATEST",
    "LEAST",
    "CONCAT",
    "CONCAT_WS",
    "FORMAT",
    "NUM_NULLS",
    "NUM_NONNULLS",
    "QUOTE_NULLABLE",
    "JSON_BUILD_OBJECT",
    "JSONB_BUILD_OBJECT",
    "JSON_BUILD_ARRAY",
    "JSONB_BUILD_ARRAY",
    "TO_JSON",
    "TO_JSONB",
    "__RECORD__",
    "__DATETIME__",
    "ARRAY_APPEND",
    "ARRAY_PREPEND",
    "ARRAY_CAT",
    "ARRAY_POSITION",
    "ARRAY_POSITIONS",
    "ARRAY_REMOVE",
    "ARRAY_REPLACE",
    "ARRAY_TO_STRING",
    "STRING_TO_ARRAY",
    "CURRENT_SETTING",
    "PG_TYPEOF",
    "FORMAT_TYPE",
    "JSONB_SET_LAX",
];

/// Every function name (upper-cased, without a `pg_catalog.` qualifier) the
/// evaluator implements.
pub(crate) fn is_known(name: &str) -> bool {
    crate::value::is_visibility_fn(name)
        || matches!(
            name,
            // Strings.
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH" | "BIT_LENGTH"
                | "UPPER" | "LOWER" | "INITCAP" | "CASEFOLD" | "SUBSTR" | "SUBSTRING"
                | "STRPOS" | "OVERLAY" | "TRIM" | "BTRIM" | "LTRIM" | "RTRIM" | "LPAD"
                | "RPAD" | "REPLACE" | "TRANSLATE" | "REPEAT" | "REVERSE" | "SPLIT_PART" | "MD5"
                | "LEFT" | "RIGHT" | "CONCAT" | "CONCAT_WS" | "FORMAT" | "QUOTE_IDENT"
                | "QUOTE_LITERAL" | "QUOTE_NULLABLE" | "ASCII" | "CHR" | "TO_HEX" | "TO_BIN"
                | "TO_OCT" | "STARTS_WITH" | "REGEXP_REPLACE" | "REGEXP_MATCH" | "REGEXP_LIKE"
                | "REGEXP_COUNT" | "REGEXP_SUBSTR" | "REGEXP_SPLIT_TO_ARRAY" | "REGEXP_INSTR"
                | "STRING_TO_ARRAY" | "ARRAY_TO_STRING"
                // Conditionals.
                | "ARRAY" | "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" | "NUM_NULLS" | "NUM_NONNULLS"
                // Math.
                | "ABS" | "SIGN" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNC" | "MOD"
                | "DIV" | "POWER" | "POW" | "SQRT" | "CBRT" | "EXP" | "LN" | "LOG" | "LOG10"
                | "PI" | "DEGREES" | "RADIANS" | "SIN" | "COS" | "TAN" | "COT" | "ASIN"
                | "ACOS" | "ATAN" | "ATAN2" | "SINH" | "COSH" | "TANH" | "GCD" | "LCM"
                | "FACTORIAL" | "RANDOM" | "WIDTH_BUCKET" | "SCALE" | "MIN_SCALE" | "TRIM_SCALE"
                | "ASINH" | "ACOSH" | "ATANH" | "SIND" | "COSD" | "TAND" | "COTD" | "ASIND"
                | "ACOSD" | "ATAND" | "ATAN2D" | "SETSEED" | "RANDOM_NORMAL"
                | crate::result_types::REAL_TEXT | crate::result_types::REAL_NUMERIC | crate::result_types::BPCHAR_PAD
                | "TO_ASCII" | "UNISTR" | "NORMALIZE" | "__IS_NORMALIZED__"
                | "TIMEOFDAY" | "ARRAY_DIMS" | "ARRAY_FILL" | "GENERATE_SUBSCRIPTS"
                | "ENUM_RANGE" | "ENUM_FIRST" | "ENUM_LAST"
                | crate::user_types::ENUM_SORT | crate::user_types::ENUM_LABEL
                | crate::user_types::FIELD
                | "SHA224" | "SHA256" | "SHA384" | "SHA512" | "ENCODE" | "DECODE" | "CONVERT_TO"
                | "CONVERT_FROM" | "CONVERT" | "GET_BYTE" | "SET_BYTE" | "GET_BIT" | "SET_BIT"
                | "CRC32" | "CRC32C" | "PG_COLUMN_SIZE" | crate::result_types::INT_BYTEA
                | "BIT_COUNT" | crate::result_types::BITS
                | crate::result_types::BIT_AND | crate::result_types::BIT_OR
                | crate::result_types::BIT_XOR | crate::result_types::SHIFT_LEFT
                | crate::result_types::SHIFT_RIGHT | crate::result_types::BIT_NOT
                // Text search.
                | crate::result_types::TS_JSON_DOC
                | "SETWEIGHT" | "STRIP" | "NUMNODE" | "TSVECTOR_TO_ARRAY"
                | "ARRAY_TO_TSVECTOR" | "TSQUERY_PHRASE" | "TS_DELETE" | "TO_TSVECTOR"
                | "TO_TSQUERY" | "PLAINTO_TSQUERY" | "PHRASETO_TSQUERY" | "WEBSEARCH_TO_TSQUERY"
                | "JSON_TO_TSVECTOR" | "JSONB_TO_TSVECTOR" | "TS_RANK" | "TS_RANK_CD"
                | "TS_HEADLINE" | "TS_LEXIZE" | "TS_REWRITE" | "GET_CURRENT_TS_CONFIG"
                | "PG_TS_CONFIG_IS_VISIBLE" | "PG_TS_DICT_IS_VISIBLE"
                | "PG_TS_PARSER_IS_VISIBLE" | "PG_TS_TEMPLATE_IS_VISIBLE"
                // XML.
                | "XML_IS_WELL_FORMED" | "XML_IS_WELL_FORMED_DOCUMENT"
                | "XML_IS_WELL_FORMED_CONTENT" | "XMLCOMMENT" | "XMLTEXT" | "XMLCONCAT"
                | crate::result_types::XML_PARSE | crate::result_types::XML_SERIALIZE
                | crate::result_types::XML_IS_DOCUMENT | crate::result_types::XML_ERROR
                | crate::result_types::XML_OUT
                | crate::result_types::XMLPI | crate::result_types::XMLROOT
                | crate::result_types::XMLELEMENT | crate::result_types::XMLFOREST
                | crate::result_types::XMLATTRIBUTES
                | crate::result_types::XML_ESCAPE | crate::result_types::XML_ATTR_VALUE
                | crate::result_types::XML_SERIALIZE_TYPE
                // Dates and times.
                | "NOW" | "CURRENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP"
                | "STATEMENT_TIMESTAMP" | "CLOCK_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME"
                | "LOCALTIMESTAMP" | "LOCALTIME" | "DATE_TRUNC" | "AGE" | "DATE_PART"
                | "MAKE_DATE" | "MAKE_TIMESTAMP" | "TO_TIMESTAMP" | "TO_CHAR" | "TO_DATE"
                | "TO_NUMBER" | "MAKE_INTERVAL" | "MAKE_TIME" | "MAKE_TIMESTAMPTZ"
                | "JUSTIFY_DAYS" | "JUSTIFY_HOURS" | "JUSTIFY_INTERVAL" | "ISFINITE" | "DATE_BIN"
                | "TIMEZONE" | "OVERLAPS" | "__DATETIME__" | "__TZ_TEXT__" | "__JSON_TIME__"
                | "__EXCLUDE__" | "__SYMMETRIC_LOW__" | "__SYMMETRIC_HIGH__" | "GROUPING"
                | "__INTERVAL_SPAN__" | "PERCENTILE_CONT" | "PERCENTILE_DISC" | "MODE"
                // Locks, notifications, privileges, and backends.
                | "PG_ADVISORY_LOCK" | "PG_ADVISORY_LOCK_SHARED" | "PG_ADVISORY_XACT_LOCK"
                | "PG_ADVISORY_XACT_LOCK_SHARED" | "PG_TRY_ADVISORY_LOCK"
                | "PG_TRY_ADVISORY_LOCK_SHARED" | "PG_TRY_ADVISORY_XACT_LOCK"
                | "PG_TRY_ADVISORY_XACT_LOCK_SHARED" | "PG_ADVISORY_UNLOCK"
                | "PG_ADVISORY_UNLOCK_SHARED" | "PG_ADVISORY_UNLOCK_ALL" | "PG_NOTIFY"
                | "HAS_TABLE_PRIVILEGE" | "HAS_SCHEMA_PRIVILEGE" | "HAS_DATABASE_PRIVILEGE"
                | "HAS_COLUMN_PRIVILEGE" | "HAS_ANY_COLUMN_PRIVILEGE" | "HAS_SEQUENCE_PRIVILEGE"
                | "HAS_FUNCTION_PRIVILEGE" | "PG_HAS_ROLE" | "TO_REGCLASS" | "TO_REGTYPE"
                | "TO_REGNAMESPACE" | "TO_REGROLE" | "TO_REGPROC" | "TO_REGPROCEDURE"
                | "PG_CANCEL_BACKEND" | "PG_TERMINATE_BACKEND" | "PG_POSTMASTER_START_TIME"
                | "PG_CONF_LOAD_TIME" | "PG_TRIGGER_DEPTH" | "PG_CURRENT_XACT_ID_IF_ASSIGNED"
                // Session and system.
                | "VERSION" | "CURRENT_USER" | "SESSION_USER" | "CURRENT_ROLE" | "USER"
                | "CURRENT_DATABASE" | "CURRENT_CATALOG" | "CURRENT_SCHEMA" | "CURRENT_SCHEMAS"
                | "CURRENT_SETTING" | "SET_CONFIG" | "PG_BACKEND_PID" | "PG_TYPEOF" | "TXID_CURRENT"
                | "PG_CURRENT_XACT_ID" | "PG_SIZE_PRETTY" | "PG_ENCODING_TO_CHAR"
                | "PG_CLIENT_ENCODING" | "PG_IS_IN_RECOVERY" | "PG_SLEEP" | "PG_GET_USERBYID"
                | "INET_SERVER_ADDR" | "INET_SERVER_PORT" | "INET_CLIENT_ADDR"
                | "INET_CLIENT_PORT" | "OBJ_DESCRIPTION" | "COL_DESCRIPTION"
                | "SHOBJ_DESCRIPTION" | "FORMAT_TYPE" | "PG_GET_EXPR"
                | "PG_RELATION_IS_PUBLISHABLE" | "PG_GET_STATISTICSOBJDEF_COLUMNS"
                | "PG_GET_INDEXDEF" | "PG_GET_CONSTRAINTDEF" | "__OBJECT_NAME__"
                | "PG_GET_VIEWDEF" | "PG_RELATION_SIZE" | "PG_TABLE_SIZE" | "PG_INDEXES_SIZE"
                | "PG_TOTAL_RELATION_SIZE" | "PG_DATABASE_SIZE"
                // Sequences.
                | "NEXTVAL" | "CURRVAL" | "LASTVAL" | "SETVAL" | "PG_GET_SERIAL_SEQUENCE"
                | crate::sequences::SERIAL
                | "__IDENTITY__"
                // UUIDs.
                | "GEN_RANDOM_UUID" | "UUIDV4" | "UUIDV7" | "UUID_EXTRACT_VERSION"
                // JSON.
                | "TO_JSON" | "TO_JSONB" | "JSON_BUILD_OBJECT" | "JSONB_BUILD_OBJECT"
                | "JSON_BUILD_ARRAY" | "JSONB_BUILD_ARRAY" | "JSON_TYPEOF" | "JSONB_TYPEOF"
                | "JSON_ARRAY_LENGTH" | "JSONB_ARRAY_LENGTH" | "JSON_EXTRACT_PATH"
                | "JSONB_EXTRACT_PATH" | "JSON_EXTRACT_PATH_TEXT" | "JSONB_EXTRACT_PATH_TEXT"
                | "JSONB_SET" | "JSONB_STRIP_NULLS" | "JSON_STRIP_NULLS" | "JSONB_PRETTY"
                | "ROW_TO_JSON" | "ARRAY_TO_JSON" | "JSON_OBJECT" | "__RECORD__"
                // JSON path functions.
                | "JSONB_PATH_EXISTS" | "JSONB_PATH_EXISTS_TZ" | "JSONB_PATH_MATCH"
                | "JSONB_PATH_MATCH_TZ" | "JSONB_PATH_QUERY" | "JSONB_PATH_QUERY_TZ"
                | "JSONB_PATH_QUERY_ARRAY" | "JSONB_PATH_QUERY_ARRAY_TZ"
                | "JSONB_PATH_QUERY_FIRST" | "JSONB_PATH_QUERY_FIRST_TZ"
                // `(value).*`, a record expanded into the select list.
                | "NODUS_EXPAND_RECORD"
                // Arrays.
                | "ARRAY_LENGTH" | "CARDINALITY" | "ARRAY_APPEND" | "ARRAY_PREPEND"
                | "ARRAY_CAT" | "ARRAY_POSITION" | "ARRAY_POSITIONS" | "ARRAY_REMOVE"
                | "ARRAY_REPLACE" | "ARRAY_UPPER" | "ARRAY_LOWER" | "ARRAY_NDIMS"
                | "TRIM_ARRAY" | "ARRAY_SORT" | "ARRAY_REVERSE"
                // Network addresses and money.
                | "HOST" | "NETMASK" | "HOSTMASK" | "BROADCAST" | "NETWORK" | "MASKLEN"
                | "SET_MASKLEN" | "ABBREV" | "FAMILY" | "INET_SAME_FAMILY" | "INET_MERGE"
                | "MACADDR8_SET7BIT" | "TEXT" | "CASH_WORDS" | "CASHLARGER" | "CASHSMALLER"
                | "MONEY"
                | "__NET__" | "__NET_CAST__" | "__MONEY_TEXT__" | "__MAC_TRUNC__"
                | "__MULTIRANGE_FROM_RANGE__" | "__RANGE_GREATEST__"
                // Ranges.
                | "INT4RANGE" | "INT8RANGE" | "NUMRANGE" | "DATERANGE" | "TSRANGE" | "TSTZRANGE"
                | "INT4MULTIRANGE" | "INT8MULTIRANGE" | "NUMMULTIRANGE" | "DATEMULTIRANGE"
                | "TSMULTIRANGE" | "TSTZMULTIRANGE" | "MULTIRANGE"
                // The geometric types, their operators, and their functions.
                | "POINT" | "LSEG" | "BOX" | "PATH" | "POLYGON" | "LINE" | "CIRCLE"
                | "CENTER" | "RADIUS" | "DIAMETER" | "HEIGHT" | "WIDTH" | "DIAGONAL"
                | "AREA" | "NPOINTS" | "ISCLOSED" | "ISOPEN" | "PCLOSE" | "POPEN"
                | "BOUND_BOX"
                | "__GEO__" | "__GEO_CAST__" | "__GEO_FN__" | "__GEO_UNARY__"
                | "__BAD_COMPARISON__" | crate::result_types::BAD_ORDERING
                | "ISEMPTY" | "LOWER_INC" | "UPPER_INC" | "LOWER_INF" | "UPPER_INF"
                | "RANGE_MERGE"
                | "__RANGE__" | "__RANGE_LOWER__" | "__RANGE_UPPER__" | "__BAD_RANGE_CAST__"
                | "__BAD_OPERATOR__" | "__BAD_FUNCTION__"
                | crate::result_types::FUNC_NOT_UNIQUE
                | crate::result_types::TS_HEADLINE
                // Subscripts: `a[i]`, `a[lo:hi]`, `doc['key']`.
                | "__SUBSCRIPT__" | "__SLICE__"
                | crate::result_types::INTEGER_RANGE
                // Set-returning functions: a select list runs them as a lateral
                // table function, anywhere else they are an error.
                | "UNNEST" | "GENERATE_SERIES" | "JSONB_ARRAY_ELEMENTS" | "JSON_ARRAY_ELEMENTS"
                | "JSONB_ARRAY_ELEMENTS_TEXT" | "JSON_ARRAY_ELEMENTS_TEXT" | "REGEXP_SPLIT_TO_TABLE"
                | "REGEXP_MATCHES" | "STRING_TO_TABLE" | "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS"
                | "JSONB_EACH" | "JSONB_EACH_TEXT" | "JSON_EACH" | "JSON_EACH_TEXT"
                | "JSONB_TO_RECORD" | "JSON_TO_RECORD" | "JSONB_TO_RECORDSET" | "JSON_TO_RECORDSET"
                | "JSONB_INSERT" | "JSONB_SET_LAX"
                | "PG_PARTITION_ANCESTORS"
        )
}

/// The declared result type of a call, for describing result columns before
/// any row exists; `None` when it depends on more than the argument types
/// NodusDB tracks. `arg_types` are the arguments' types where known.
pub(crate) fn return_type(name: &str, arg_types: &[Option<String>]) -> Option<String> {
    let first_known = || arg_types.iter().flatten().next().cloned();
    // An element of an array is of its element type; a slice of the array's.
    let subscripted = arg_types.first().cloned().flatten();
    match name {
        crate::result_types::INTEGER_RANGE => return subscripted,
        "__SUBSCRIPT__" => {
            return subscripted.map(|t| t.strip_suffix("[]").map(str::to_string).unwrap_or(t));
        }
        "__SLICE__" => return subscripted,
        _ => {}
    }
    if let Some(ty) = crate::session_functions::return_type(name) {
        return Some(ty.to_string());
    }
    if crate::result_types::bitwise_arity(name).is_some() {
        return crate::result_types::bitwise_type(name, arg_types);
    }
    if let Some(ty) = math_return_type(name, arg_types) {
        return ty;
    }
    let bytea_argument = arg_types
        .first()
        .cloned()
        .flatten()
        .is_some_and(|t| crate::value::is_bytea_type(&t));
    match name {
        "SUBSTR" | "SUBSTRING" | "OVERLAY" | "TRIM" | "BTRIM" | "LTRIM" | "RTRIM" | "REVERSE"
            if bytea_argument =>
        {
            return Some("BYTEA".into());
        }
        "SHA224"
        | "SHA256"
        | "SHA384"
        | "SHA512"
        | "DECODE"
        | "CONVERT_TO"
        | "CONVERT"
        | "SET_BYTE"
        | "SET_BIT"
        | crate::result_types::INT_BYTEA => return Some("BYTEA".into()),
        "ENCODE" | "CONVERT_FROM" | "REGEXP_SUBSTR" | "UNISTR" | "NORMALIZE" => {
            return Some("TEXT".into());
        }
        "GET_BYTE" | "GET_BIT" | "REGEXP_COUNT" | "REGEXP_INSTR" | "PG_COLUMN_SIZE" => {
            return Some("INTEGER".into());
        }
        "CRC32" | "CRC32C" | "BIT_COUNT" => return Some("BIGINT".into()),
        "TIMEOFDAY" | "ARRAY_DIMS" => return Some("TEXT".into()),
        "ARRAY_FILL" => {
            return arg_types
                .first()
                .cloned()
                .flatten()
                .map(|t| format!("{t}[]"));
        }
        "CURRENT_TIMESTAMP" if arg_types.len() == 1 => return Some("TIMESTAMPTZ".into()),
        "LOCALTIMESTAMP" if arg_types.len() == 1 => return Some("TIMESTAMP".into()),
        "LOCALTIME" if arg_types.len() == 1 => return Some("TIME".into()),
        "CURRENT_TIME" if arg_types.len() == 1 => return Some("TIMETZ".into()),
        crate::result_types::BITS => {
            return match arg_types.first() {
                Some(_) => match name {
                    _ if arg_types.len() == 4 => arg_types.get(1).cloned().flatten(),
                    _ => Some("INTEGER".into()),
                },
                None => None,
            };
        }
        "__IS_NORMALIZED__" | "REGEXP_LIKE" => return Some("BOOLEAN".into()),
        "REGEXP_MATCH" | "REGEXP_SPLIT_TO_ARRAY" => return Some("TEXT[]".into()),
        _ => {}
    }
    Some(
        match name {
            "LENGTH"
            | "CHAR_LENGTH"
            | "CHARACTER_LENGTH"
            | "OCTET_LENGTH"
            | "BIT_LENGTH"
            | "STRPOS"
            | "ASCII"
            | "ARRAY_LENGTH"
            | "CARDINALITY"
            | "ARRAY_POSITION"
            | "ARRAY_UPPER"
            | "ARRAY_LOWER"
            | "ARRAY_NDIMS"
            | "PG_BACKEND_PID"
            | "UUID_EXTRACT_VERSION"
            | "JSON_ARRAY_LENGTH"
            | "JSONB_ARRAY_LENGTH"
            | "NUM_NULLS"
            | "NUM_NONNULLS"
            | "INET_SERVER_PORT"
            | "INET_CLIENT_PORT" => "INTEGER",
            "TXID_CURRENT"
            | "PG_CURRENT_XACT_ID"
            | "NEXTVAL"
            | crate::sequences::SERIAL
            | "CURRVAL"
            | "LASTVAL"
            | "SETVAL" => "BIGINT",
            "PG_GET_SERIAL_SEQUENCE" => "TEXT",
            "RANDOM" | "PI" | "DATE_PART" => "DOUBLE PRECISION",
            "NOW"
            | "CURRENT_TIMESTAMP"
            | "TRANSACTION_TIMESTAMP"
            | "STATEMENT_TIMESTAMP"
            | "CLOCK_TIMESTAMP"
            | "TO_TIMESTAMP" => "TIMESTAMPTZ",
            "LOCALTIMESTAMP" | "MAKE_TIMESTAMP" => "TIMESTAMP",
            "CURRENT_DATE" | "MAKE_DATE" | "TO_DATE" => "DATE",
            "LOCALTIME" | "MAKE_TIME" => "TIME",
            "CURRENT_TIME" => "TIMETZ",
            "AGE" | "MAKE_INTERVAL" | "JUSTIFY_DAYS" | "JUSTIFY_HOURS" | "JUSTIFY_INTERVAL" => {
                "INTERVAL"
            }
            "MAKE_TIMESTAMPTZ" => "TIMESTAMPTZ",
            "TO_CHAR"
            | "__TZ_TEXT__"
            | "__JSON_TIME__"
            | crate::result_types::REAL_TEXT
            | crate::result_types::BPCHAR_PAD => "TEXT",
            crate::result_types::REAL_NUMERIC => "NUMERIC",
            "GROUPING" => "INTEGER",
            "__INTERVAL_SPAN__" => "NUMERIC",
            "TO_NUMBER" => "NUMERIC",
            "ISFINITE" | "OVERLAPS" => "BOOLEAN",
            "DATE_BIN" => return arg_types.get(1).cloned().flatten(),
            // Zoned time becomes local time, and local time zoned.
            "TIMEZONE" => {
                let value_type = arg_types.get(1).cloned().flatten();
                if value_type
                    .as_deref()
                    .is_some_and(crate::timezone::is_zoned_time_type)
                {
                    return Some("TIMETZ".into());
                }
                return match value_type.and_then(|t| crate::datetime::Kind::of_type(&t)) {
                    Some(crate::datetime::Kind::TimestampTz | crate::datetime::Kind::Date) => {
                        Some("TIMESTAMP".into())
                    }
                    Some(crate::datetime::Kind::Timestamp) => Some("TIMESTAMPTZ".into()),
                    Some(crate::datetime::Kind::Time) => Some("TIMETZ".into()),
                    _ => None,
                };
            }
            // A date truncates as a zoned timestamp, and so does a
            // truncation in a named zone.
            "DATE_TRUNC" if arg_types.len() == 3 => "TIMESTAMPTZ",
            "DATE_TRUNC" => {
                return Some(match arg_types.get(1).cloned().flatten() {
                    Some(t)
                        if crate::datetime::Kind::of_type(&t)
                            == Some(crate::datetime::Kind::Date) =>
                    {
                        "TIMESTAMPTZ".into()
                    }
                    Some(t) => t,
                    None => "TIMESTAMP".into(),
                });
            }
            "GEN_RANDOM_UUID" | "UUIDV4" | "UUIDV7" => "UUID",
            "__TSLEN__" | "NUMNODE" => "INTEGER",
            "SETWEIGHT" | "STRIP" | "TSVECTOR_TO_ARRAY" | "ARRAY_TO_TSVECTOR" | "TS_DELETE" => {
                "TSVECTOR"
            }
            "TO_TSVECTOR" | "JSON_TO_TSVECTOR" | "JSONB_TO_TSVECTOR" => "TSVECTOR",
            "TS_RANK" | "TS_RANK_CD" => "REAL",
            "TS_HEADLINE" | "__TS_HEADLINE__" => "TEXT",
            "TS_LEXIZE" => "TEXT[]",
            "TS_REWRITE" => "TSQUERY",
            "GET_CURRENT_TS_CONFIG" => "REGCONFIG",
            "XML_IS_WELL_FORMED" | "XML_IS_WELL_FORMED_DOCUMENT" | "XML_IS_WELL_FORMED_CONTENT" => {
                "BOOLEAN"
            }
            "XMLCOMMENT" | "XMLTEXT" | "XMLCONCAT" | crate::result_types::XML_PARSE => "XML",
            crate::result_types::XML_SERIALIZE | crate::result_types::XML_OUT => "TEXT",
            crate::result_types::XMLPI
            | crate::result_types::XMLROOT
            | crate::result_types::XMLELEMENT
            | crate::result_types::XMLFOREST
            | crate::result_types::XMLATTRIBUTES => "XML",
            crate::result_types::XML_ESCAPE
            | crate::result_types::XML_ATTR_VALUE
            | crate::result_types::XML_SERIALIZE_TYPE => "TEXT",
            "PG_TS_CONFIG_IS_VISIBLE"
            | "PG_TS_DICT_IS_VISIBLE"
            | "PG_TS_PARSER_IS_VISIBLE"
            | "PG_TS_TEMPLATE_IS_VISIBLE" => "BOOLEAN",
            "TO_TSQUERY" | "PLAINTO_TSQUERY" | "PHRASETO_TSQUERY" | "WEBSEARCH_TO_TSQUERY" => {
                "TSQUERY"
            }
            "TSQUERY_PHRASE" | "__TSNOT__" => "TSQUERY",
            "__TSCMP__" => "BOOLEAN",
            "__TS__" => match arg_types.first().cloned().flatten().as_deref() {
                Some("&&") | Some("||") | Some("<->") => "TSQUERY",
                _ => "BOOLEAN",
            },
            "TO_JSONB" | "JSONB_BUILD_OBJECT" | "JSONB_BUILD_ARRAY" | "JSONB_EXTRACT_PATH"
            | "JSONB_SET" | "JSONB_STRIP_NULLS" | "JSONB_INSERT" | "JSONB_SET_LAX" => "JSONB",
            "JSONB_PATH_EXISTS"
            | "JSONB_PATH_EXISTS_TZ"
            | "JSONB_PATH_MATCH"
            | "JSONB_PATH_MATCH_TZ" => "BOOLEAN",
            "JSONB_PATH_QUERY"
            | "JSONB_PATH_QUERY_TZ"
            | "JSONB_PATH_QUERY_ARRAY"
            | "JSONB_PATH_QUERY_ARRAY_TZ"
            | "JSONB_PATH_QUERY_FIRST"
            | "JSONB_PATH_QUERY_FIRST_TZ" => "JSONB",
            "TO_JSON" | "JSON_BUILD_OBJECT" | "JSON_BUILD_ARRAY" | "JSON_EXTRACT_PATH"
            | "JSON_STRIP_NULLS" | "ROW_TO_JSON" | "ARRAY_TO_JSON" | "JSON_OBJECT" => "JSON",
            "__RECORD__" => "RECORD",
            "INT4MULTIRANGE" | "INT8MULTIRANGE" | "NUMMULTIRANGE" | "DATEMULTIRANGE"
            | "TSMULTIRANGE" | "TSTZMULTIRANGE" => {
                return crate::ranges::Kind::of_multirange(name)
                    .map(|kind| kind.multirange_name().to_string());
            }
            "__MULTIRANGE_BUILD__" => {
                // The subtype rides as the first argument; the result is the
                // multirange over it.
                return arg_types
                    .first()
                    .cloned()
                    .flatten()
                    .and_then(|t| crate::ranges::Kind::of(&t))
                    .map(|kind| kind.multirange_name().to_string());
            }
            "INT4RANGE" | "INT8RANGE" | "NUMRANGE" | "DATERANGE" | "TSRANGE" | "TSTZRANGE" => {
                return Some(name.to_string());
            }
            "ISEMPTY" | "LOWER_INC" | "UPPER_INC" | "LOWER_INF" | "UPPER_INF" => {
                return Some("BOOL".into());
            }
            // `range_merge` of a multirange is a range of its subtype.
            "RANGE_MERGE" => {
                return arg_types.first().cloned().flatten().map(|t| {
                    crate::multiranges::kind_of(&t)
                        .map_or(t.clone(), |kind| kind.name().to_string())
                });
            }
            // `lower`/`upper` of a range have its subtype's type (appended by
            // the planner as a literal).
            "__RANGE_LOWER__" | "__RANGE_UPPER__" => {
                return arg_types.get(1).cloned().flatten();
            }
            "__BAD_RANGE_CAST__" => return None,
            "__MONEY_TEXT__" | "CASH_WORDS" => return Some("TEXT".into()),
            "CASHLARGER" | "CASHSMALLER" => return Some("MONEY".into()),
            "MACADDR8_SET7BIT" => return Some("MACADDR8".into()),
            "HOST" => return Some("INET".into()),
            "NETMASK" | "HOSTMASK" | "BROADCAST" | "NETWORK" | "INET_MERGE" => {
                return arg_types.first().cloned().flatten().or(Some("INET".into()));
            }
            "MASKLEN" | "FAMILY" => return Some("INTEGER".into()),
            "SET_MASKLEN" | "ABBREV" => return arg_types.first().cloned().flatten(),
            "INET_SAME_FAMILY" => return Some("BOOL".into()),
            "__MAC_TRUNC__" => return arg_types.first().cloned().flatten(),
            "PG_IS_IN_RECOVERY" | "STARTS_WITH" => "BOOLEAN",
            name if crate::value::is_visibility_fn(name) => "BOOLEAN",
            "UPPER"
            | "LOWER"
            | "CASEFOLD"
            | "INITCAP"
            | "SUBSTR"
            | "SUBSTRING"
            | "LEFT"
            | "RIGHT"
            | "LPAD"
            | "RPAD"
            | "BTRIM"
            | "LTRIM"
            | "RTRIM"
            | "REPLACE"
            | "TRANSLATE"
            | "REPEAT"
            | "REVERSE"
            | "SPLIT_PART"
            | "MD5"
            | "CONCAT"
            | "CONCAT_WS"
            | "FORMAT"
            | "QUOTE_IDENT"
            | "QUOTE_LITERAL"
            | "QUOTE_NULLABLE"
            | "CHR"
            | "TO_HEX"
            | "REGEXP_REPLACE"
            | "OVERLAY"
            | "VERSION"
            | "CURRENT_DATABASE"
            | "CURRENT_SCHEMA"
            | "CURRENT_SETTING"
            | "SET_CONFIG"
            | "PG_TYPEOF"
            | "PG_SIZE_PRETTY"
            | "PG_ENCODING_TO_CHAR"
            | "PG_CLIENT_ENCODING"
            | "ARRAY_TO_STRING"
            | "JSON_TYPEOF"
            | "JSONB_TYPEOF"
            | "JSON_EXTRACT_PATH_TEXT"
            | "JSONB_EXTRACT_PATH_TEXT"
            | "JSONB_PRETTY"
            | "FORMAT_TYPE"
            | "PG_GET_EXPR"
            | "PG_GET_USERBYID"
            | "OBJ_DESCRIPTION"
            | "COL_DESCRIPTION"
            | "SHOBJ_DESCRIPTION" => "TEXT",
            "CURRENT_USER" | "SESSION_USER" | "CURRENT_ROLE" | "USER" | "CURRENT_CATALOG" => "NAME",
            "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" | "__SYMMETRIC_LOW__"
            | "__SYMMETRIC_HIGH__" => return first_known(),
            "ARRAY" => return first_known().map(|element| format!("{element}[]")),
            _ => return None,
        }
        .to_string(),
    )
}

/// The result type of a math function, by the variant PostgreSQL resolves
/// its argument types to: `None` for other functions.
fn math_return_type(name: &str, arg_types: &[Option<String>]) -> Option<Option<String>> {
    let kind = |t: &Option<String>| {
        let upper = t.as_deref().unwrap_or_default().to_ascii_uppercase();
        if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
            'n'
        } else if matches!(
            upper.as_str(),
            "REAL" | "FLOAT4" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" | "DOUBLE"
        ) {
            'f'
        } else if matches!(
            upper.as_str(),
            "SMALLINT" | "INT2" | "INTEGER" | "INT" | "INT4" | "BIGINT" | "INT8"
        ) {
            'i'
        } else {
            '?'
        }
    };
    let kinds: Vec<char> = arg_types.iter().map(kind).collect();
    let numeric = kinds.contains(&'n') && !kinds.contains(&'f');
    let known = !kinds.contains(&'?');
    let float_or_numeric = || {
        Some(if numeric {
            "NUMERIC".to_string()
        } else {
            "DOUBLE PRECISION".to_string()
        })
    };
    Some(match name {
        "POWER" | "POW" | "SQRT" | "EXP" | "LN" | "LOG10" | "CEIL" | "CEILING" | "FLOOR"
        | "SIGN"
            if known =>
        {
            float_or_numeric()
        }
        "LOG" if arg_types.len() == 2 => Some("NUMERIC".into()),
        "LOG" if known => float_or_numeric(),
        "ROUND" | "TRUNC" if arg_types.len() == 2 && kinds[0] != 'f' => Some("NUMERIC".into()),
        "ROUND" | "TRUNC" if known => float_or_numeric(),
        "DIV" | "FACTORIAL" | "TRIM_SCALE" => Some("NUMERIC".into()),
        "SCALE" | "MIN_SCALE" | "WIDTH_BUCKET" => Some("INTEGER".into()),
        "ABS" => arg_types.first().cloned().flatten(),
        "CBRT" | "PI" | "DEGREES" | "RADIANS" | "SIN" | "COS" | "TAN" | "COT" | "ASIN" | "ACOS"
        | "ATAN" | "ATAN2" | "SINH" | "COSH" | "TANH" | "ASINH" | "ACOSH" | "ATANH" | "SIND"
        | "COSD" | "TAND" | "COTD" | "ASIND" | "ACOSD" | "ATAND" | "ATAN2D" | "RANDOM_NORMAL" => {
            Some("DOUBLE PRECISION".into())
        }
        "RANDOM" if arg_types.len() == 2 => Some(
            if arg_types
                .iter()
                .flatten()
                .any(|t| kind(&Some(t.clone())) == 'n')
            {
                "NUMERIC".into()
            } else if arg_types
                .iter()
                .flatten()
                .any(|t| matches!(t.to_ascii_uppercase().as_str(), "BIGINT" | "INT8"))
            {
                "BIGINT".into()
            } else {
                "INTEGER".into()
            },
        ),
        "SETSEED" => Some("VOID".into()),
        _ => return None,
    })
}

/// Calls a built-in function. Unknown names and wrong argument counts fail the
/// statement.
pub(crate) fn call(name: &str, args: &[Value]) -> Value {
    if let Some(value) = crate::user_types::call(name, args) {
        return value;
    }
    if !NON_STRICT.contains(&name) && args.iter().any(|a| matches!(a, Value::Null)) {
        return Value::Null;
    }
    // A `json` value or a row is its text to a function that takes neither.
    let converted: Vec<Value>;
    let args = if !takes_json(name)
        && args
            .iter()
            .any(|a| matches!(a, Value::Json(_) | Value::Record(_)))
    {
        converted = args
            .iter()
            .map(|a| match a {
                Value::Json(_) | Value::Record(_) => Value::Text(render(a)),
                other => other.clone(),
            })
            .collect();
        &converted
    } else {
        args
    };
    match dispatch(name, args) {
        Some(value) => value,
        None => raise(
            crate::error_fields::DbError::new(format!(
                "function {}({}) does not exist",
                name.to_ascii_lowercase(),
                args.iter()
                    .map(crate::value::value_type_name)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
            .code("42883")
            .hint(
                "No function matches the given name and argument types. You might need to add \
                 explicit type casts.",
            )
            .into_text(),
        ),
    }
}

/// Whether a function takes `json` values and rows as they are: the JSON
/// functions, and those that pass a value through.
fn takes_json(name: &str) -> bool {
    name.starts_with("JSON")
        || name.starts_with("TO_JSON")
        || name.ends_with("_TO_JSON")
        || matches!(
            name,
            "COALESCE"
                | "NULLIF"
                | "GREATEST"
                | "LEAST"
                | "PG_TYPEOF"
                | "NUM_NULLS"
                | "NUM_NONNULLS"
                | "__RECORD__"
                // The vectors a JSON document builds take the document as a
                // value, not as text.
                | "TO_TSVECTOR"
                | "JSON_TO_TSVECTOR"
                | "JSONB_TO_TSVECTOR"
        )
}

/// `(expr)` without the parentheses when they enclose all of it.
fn unwrap_parens(expr: &str) -> Option<&str> {
    let inner = expr.strip_prefix('(')?.strip_suffix(')')?;
    let mut depth = 0i32;
    let mut quoted = false;
    for c in inner.chars() {
        match c {
            '\'' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => {
                depth -= 1;
                if depth < 0 {
                    return None;
                }
            }
            _ => {}
        }
    }
    (depth == 0).then_some(inner)
}

/// Text the catalog gives for an object, or NULL when there is none.
fn catalog_text(
    describe: impl FnOnce(&dyn nodus_catalog::CatalogReader) -> Option<String>,
) -> Value {
    session_env::with(|env| env.and_then(|e| e.catalog.clone()))
        .and_then(|catalog| describe(catalog.as_ref()))
        .map_or(Value::Null, Value::Text)
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => render(other),
    }
}

fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Float(f) => Some(f.round_ties_even() as i64),
        Value::Numeric(d) => d.to_i64(),
        Value::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Numeric(d) => Some(crate::value::decimal_to_f64(d)),
        Value::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn array(v: &Value) -> Option<Vec<Value>> {
    match v {
        Value::Array(items) => Some(items.clone()),
        Value::Text(s) => crate::value::parse_array_literal(s),
        _ => None,
    }
}

/// A bitwise operator on integers, computed in its result type (the last
/// argument, when known): a shift wraps at that type's width.
fn bitwise(name: &str, args: &[Value]) -> Result<Value, String> {
    use crate::result_types::{BIT_AND, BIT_NOT, BIT_OR, BIT_XOR, SHIFT_LEFT, SHIFT_RIGHT};
    let arity = crate::result_types::bitwise_arity(name).unwrap_or(2);
    let ty = match args.get(arity) {
        Some(Value::Text(t)) => t.to_ascii_uppercase(),
        _ => String::new(),
    };
    let operands = &args[..arity.min(args.len())];
    let symbol = match name {
        BIT_AND => "&",
        BIT_OR => "|",
        BIT_XOR => "#",
        SHIFT_LEFT => "<<",
        SHIFT_RIGHT => ">>",
        _ => "~",
    };
    // Bit strings, by their type.
    if crate::bits::bit_type(&ty).is_some() {
        let bits = |v: &Value| crate::bits::parse(&text(v));
        let a = bits(&operands[0])?;
        return Ok(Value::Text(match name {
            BIT_NOT => crate::bits::not(&a),
            SHIFT_LEFT | SHIFT_RIGHT => {
                crate::bits::shift(&a, int(&operands[1]).unwrap_or(0), name == SHIFT_LEFT)
            }
            _ => crate::bits::binary(symbol, &a, &bits(&operands[1])?)?,
        }));
    }
    let ints: Option<Vec<i64>> = operands
        .iter()
        .map(|v| match v {
            Value::Int(i) => Some(*i),
            _ => None,
        })
        .collect();
    let Some(ints) = ints else {
        let types: Vec<&str> = operands.iter().map(crate::value::value_type_name).collect();
        return Err(match types.as_slice() {
            [t] => format!("operator does not exist: {symbol} {t}"),
            [l, r] => format!("operator does not exist: {l} {symbol} {r}"),
            _ => format!("operator does not exist: {symbol}"),
        });
    };
    let (a, b) = (ints[0], ints.get(1).copied().unwrap_or(0));
    let _ = symbol;
    let width = match ty.as_str() {
        "SMALLINT" | "INT2" => 16,
        "INTEGER" | "INT" | "INT4" | "SERIAL" => 32,
        _ => 64,
    };
    Ok(Value::Int(match name {
        BIT_AND => a & b,
        BIT_OR => a | b,
        BIT_XOR => a ^ b,
        BIT_NOT => !a,
        // A 16-bit shift computes in 32 bits and keeps the low 16.
        SHIFT_LEFT if width == 16 => i64::from((a as i32).wrapping_shl(b as u32) as i16),
        SHIFT_RIGHT if width == 16 => i64::from((a as i32).wrapping_shr(b as u32) as i16),
        SHIFT_LEFT if width == 32 => i64::from((a as i32).wrapping_shl(b as u32)),
        SHIFT_RIGHT if width == 32 => i64::from((a as i32).wrapping_shr(b as u32)),
        SHIFT_LEFT => a.wrapping_shl(b as u32),
        _ => a.wrapping_shr(b as u32),
    }))
}

/// Functions whose (untyped) text arguments are `bytea` input.
const BYTEA_FUNCTIONS: &[&str] = &[
    "SHA224",
    "SHA256",
    "SHA384",
    "SHA512",
    "ENCODE",
    "DECODE",
    "CONVERT_TO",
    "CONVERT_FROM",
    "CONVERT",
    "GET_BYTE",
    "SET_BYTE",
    "GET_BIT",
    "SET_BIT",
    "CRC32",
    "CRC32C",
    crate::result_types::INT_BYTEA,
];

/// The `bytea` functions, and the string functions on `bytea` arguments;
/// `None` for other calls.
fn bytea_function(name: &str, args: &[Value]) -> Option<Value> {
    use sha2::Digest;
    let arity = |n: usize| args.len() == n;
    let arg = |i: usize| args.get(i).unwrap_or(&Value::Null);
    // A `bytea` argument's bytes; text is `bytea` input.
    let bytes = |v: &Value| -> Result<Vec<u8>, String> {
        match v {
            Value::Bytea(b) => Ok(b.clone()),
            Value::Text(t) => crate::bytea::parse_input(t),
            other => Ok(render(other).into_bytes()),
        }
    };
    let result = |r: Result<Value, String>| Some(r.unwrap_or_else(raise));
    let out_of_range = |index: i64, bits: usize| {
        format!(
            "index {index} out of valid range, 0..{}",
            bits.saturating_sub(1)
        )
    };
    match name {
        "LENGTH" | "OCTET_LENGTH" if arity(1) => {
            result(bytes(arg(0)).map(|b| Value::Int(b.len() as i64)))
        }
        "BIT_LENGTH" if arity(1) => result(bytes(arg(0)).map(|b| Value::Int(b.len() as i64 * 8))),
        "MD5" if arity(1) => result(bytes(arg(0)).map(|b| {
            use md5::Digest;
            Value::Text(
                md5::Md5::digest(&b)
                    .iter()
                    .map(|x| format!("{x:02x}"))
                    .collect(),
            )
        })),
        "SHA224" | "SHA256" | "SHA384" | "SHA512" if arity(1) => result(bytes(arg(0)).map(|b| {
            Value::Bytea(match name {
                "SHA224" => sha2::Sha224::digest(&b).to_vec(),
                "SHA256" => sha2::Sha256::digest(&b).to_vec(),
                "SHA384" => sha2::Sha384::digest(&b).to_vec(),
                _ => sha2::Sha512::digest(&b).to_vec(),
            })
        })),
        "BIT_COUNT" if arity(1) => result(
            bytes(arg(0)).map(|b| Value::Int(b.iter().map(|x| i64::from(x.count_ones())).sum())),
        ),
        "CRC32" | "CRC32C" if arity(1) => result(
            bytes(arg(0)).map(|b| Value::Int(i64::from(crate::bytea::crc32(&b, name == "CRC32C")))),
        ),
        "ENCODE" if arity(2) => result(
            bytes(arg(0)).and_then(|b| crate::bytea::encode(&b, &text(arg(1))).map(Value::Text)),
        ),
        "DECODE" if arity(2) => {
            result(crate::bytea::decode(&text(arg(0)), &text(arg(1))).map(Value::Bytea))
        }
        "CONVERT_TO" if arity(2) => {
            result(crate::bytea::convert_to(&text(arg(0)), &text(arg(1))).map(Value::Bytea))
        }
        "CONVERT_FROM" if arity(2) => result(
            bytes(arg(0))
                .and_then(|b| crate::bytea::convert_from(&b, &text(arg(1))).map(Value::Text)),
        ),
        "CONVERT" if arity(3) => result(bytes(arg(0)).and_then(|b| {
            crate::bytea::convert(&b, &text(arg(1)), &text(arg(2))).map(Value::Bytea)
        })),
        "GET_BYTE" | "GET_BIT" if arity(2) => result(bytes(arg(0)).and_then(|b| {
            let n = int(arg(1)).unwrap_or(-1);
            let bit = name == "GET_BIT";
            let size = if bit { b.len() * 8 } else { b.len() };
            if n < 0 || n as usize >= size {
                return Err(out_of_range(n, size));
            }
            let n = n as usize;
            Ok(Value::Int(if bit {
                i64::from(b[n / 8] >> (n % 8) & 1)
            } else {
                i64::from(b[n])
            }))
        })),
        "SET_BYTE" | "SET_BIT" if arity(3) => result(bytes(arg(0)).and_then(|mut b| {
            let n = int(arg(1)).unwrap_or(-1);
            let value = int(arg(2)).unwrap_or(0);
            let bit = name == "SET_BIT";
            let size = if bit { b.len() * 8 } else { b.len() };
            if n < 0 || n as usize >= size {
                return Err(out_of_range(n, size));
            }
            let n = n as usize;
            if bit {
                if !(0..=1).contains(&value) {
                    return Err("new bit must be 0 or 1".into());
                }
                let mask = 1u8 << (n % 8);
                if value == 1 {
                    b[n / 8] |= mask;
                } else {
                    b[n / 8] &= !mask;
                }
            } else {
                b[n] = value as u8;
            }
            Ok(Value::Bytea(b))
        })),
        crate::result_types::INT_BYTEA if arity(2) => {
            let n = int(arg(0))?;
            let width = int(arg(1))? as usize;
            Some(Value::Bytea(n.to_be_bytes()[8 - width..].to_vec()))
        }
        "STRPOS" if arity(2) => result(bytes(arg(0)).and_then(|haystack| {
            let needle = bytes(arg(1))?;
            Ok(Value::Int(if needle.is_empty() {
                1
            } else {
                haystack
                    .windows(needle.len())
                    .position(|w| w == needle.as_slice())
                    .map_or(0, |p| p as i64 + 1)
            }))
        })),
        "SUBSTR" | "SUBSTRING" if arity(2) || arity(3) => result(bytes(arg(0)).and_then(|b| {
            let start = int(arg(1)).unwrap_or(1);
            let end = match args.get(2) {
                Some(len) => {
                    let len = int(len).unwrap_or(0);
                    if len < 0 {
                        return Err("negative substring length not allowed".into());
                    }
                    start.saturating_add(len)
                }
                None => i64::MAX,
            };
            let from = (start.max(1) - 1) as usize;
            let to = (end.max(1) - 1).min(b.len() as i64) as usize;
            Ok(Value::Bytea(
                b.get(from..to.max(from)).unwrap_or(&[]).to_vec(),
            ))
        })),
        "OVERLAY" if arity(3) || arity(4) => result(bytes(arg(0)).and_then(|b| {
            let replacement = bytes(arg(1))?;
            let from = (int(arg(2)).unwrap_or(1).max(1) - 1) as usize;
            let len = match args.get(3) {
                Some(l) => int(l).unwrap_or(0).max(0) as usize,
                None => replacement.len(),
            };
            let mut out: Vec<u8> = b.iter().take(from).copied().collect();
            out.extend(replacement);
            out.extend(b.iter().skip(from + len));
            Ok(Value::Bytea(out))
        })),
        "TRIM" | "BTRIM" | "LTRIM" | "RTRIM" if arity(2) => result(bytes(arg(0)).and_then(|b| {
            let set = bytes(arg(1))?;
            let (mut from, mut to) = (0, b.len());
            if name != "RTRIM" {
                while from < to && set.contains(&b[from]) {
                    from += 1;
                }
            }
            if name != "LTRIM" {
                while to > from && set.contains(&b[to - 1]) {
                    to -= 1;
                }
            }
            Ok(Value::Bytea(b[from..to].to_vec()))
        })),
        "REVERSE" if arity(1) => {
            result(bytes(arg(0)).map(|b| Value::Bytea(b.into_iter().rev().collect())))
        }
        _ => None,
    }
}

/// A math function's argument as a numeric: an integer or numeric as it is,
/// a float as PostgreSQL converts it, and text as numeric input.
fn decimal(v: &Value) -> Option<Numeric> {
    match v {
        Value::Numeric(d) => Some(d.clone()),
        Value::Int(i) => Some(Numeric::from(*i)),
        Value::Float(f) => Some(Numeric::from_f64(*f)),
        Value::Text(s) => Numeric::parse(s).ok(),
        _ => None,
    }
}

/// Whether PostgreSQL resolves a math call with both float and numeric
/// variants to the numeric one: a numeric argument and no float.
fn numeric_call(args: &[Value]) -> bool {
    args.iter().any(|a| matches!(a, Value::Numeric(_)))
        && !args.iter().any(|a| matches!(a, Value::Float(_)))
}

fn numeric_result(result: Result<Numeric, String>) -> Value {
    result.map_or_else(raise, Value::Numeric)
}

/// PostgreSQL's degrees-to-radians factor.
const RADIANS_PER_DEGREE: f64 = 0.017_453_292_519_943_295;

/// `x / y` for floats, failing on overflow and underflow as PostgreSQL does.
fn float_quotient(x: f64, y: f64) -> Value {
    let r = x / y;
    if r.is_infinite() && x.is_finite() && y.is_finite() {
        raise("value out of range: overflow")
    } else if r == 0.0 && x != 0.0 && y.is_finite() {
        raise("value out of range: underflow")
    } else {
        Value::Float(r)
    }
}

/// `power(float8, float8)` and `^` on floats, with PostgreSQL's errors.
fn float_power(a: f64, b: f64) -> Value {
    if a.is_nan() {
        return Value::Float(if b == 0.0 { 1.0 } else { f64::NAN });
    }
    if b.is_nan() {
        return Value::Float(if a == 1.0 { 1.0 } else { f64::NAN });
    }
    if a == 0.0 && b < 0.0 {
        return raise("zero raised to a negative power is undefined");
    }
    if a < 0.0 && b.floor() != b {
        return raise("a negative number raised to a non-integer power yields a complex result");
    }
    let r = a.powf(b);
    if a.is_infinite() || b.is_infinite() {
        Value::Float(r)
    } else if r.is_infinite() {
        raise("value out of range: overflow")
    } else if r == 0.0 && a != 0.0 {
        raise("value out of range: underflow")
    } else {
        Value::Float(r)
    }
}

/// The trigonometric functions in degrees, as PostgreSQL computes them so
/// that the common angles come out exact (`sind(30)` is `0.5`).
mod degrees {
    use super::RADIANS_PER_DEGREE;

    pub(super) struct Constants {
        sin_30: f64,
        one_minus_cos_60: f64,
        asin_0_5: f64,
        acos_0_5: f64,
        pub(super) atan_1_0: f64,
        tan_45: f64,
        cot_45: f64,
    }

    pub(super) fn constants() -> &'static Constants {
        static CONSTANTS: std::sync::OnceLock<Constants> = std::sync::OnceLock::new();
        CONSTANTS.get_or_init(|| {
            let mut c = Constants {
                sin_30: (30.0 * RADIANS_PER_DEGREE).sin(),
                one_minus_cos_60: 1.0 - (60.0 * RADIANS_PER_DEGREE).cos(),
                asin_0_5: 0.5f64.asin(),
                acos_0_5: 0.5f64.acos(),
                atan_1_0: 1.0f64.atan(),
                tan_45: 0.0,
                cot_45: 0.0,
            };
            c.tan_45 = sind_q1(&c, 45.0) / cosd_q1(&c, 45.0);
            c.cot_45 = cosd_q1(&c, 45.0) / sind_q1(&c, 45.0);
            c
        })
    }

    fn sind_0_to_30(c: &Constants, x: f64) -> f64 {
        ((x * RADIANS_PER_DEGREE).sin() / c.sin_30) / 2.0
    }

    fn cosd_0_to_60(c: &Constants, x: f64) -> f64 {
        1.0 - ((1.0 - (x * RADIANS_PER_DEGREE).cos()) / c.one_minus_cos_60) / 2.0
    }

    fn sind_q1(c: &Constants, x: f64) -> f64 {
        if x <= 30.0 {
            sind_0_to_30(c, x)
        } else {
            cosd_0_to_60(c, 90.0 - x)
        }
    }

    fn cosd_q1(c: &Constants, x: f64) -> f64 {
        if x <= 60.0 {
            cosd_0_to_60(c, x)
        } else {
            sind_0_to_30(c, 90.0 - x)
        }
    }

    pub(super) fn asind_q1(x: f64) -> f64 {
        let c = constants();
        if x <= 0.5 {
            (x.asin() / c.asin_0_5) * 30.0
        } else {
            90.0 - (x.acos() / c.acos_0_5) * 60.0
        }
    }

    pub(super) fn acosd_q1(x: f64) -> f64 {
        let c = constants();
        if x <= 0.5 {
            90.0 - (x.asin() / c.asin_0_5) * 30.0
        } else {
            (x.acos() / c.acos_0_5) * 60.0
        }
    }

    /// `sind`, `cosd`, `tand`, or `cotd` of a finite angle (NaN passes).
    pub(super) fn trig(name: &str, x: f64) -> f64 {
        if x.is_nan() {
            return x;
        }
        let c = constants();
        // Reduce to the first quadrant, tracking the sign.
        let mut x = x % 360.0;
        let mut sign = 1.0;
        let cosine = name == "COSD";
        if x < 0.0 {
            x = -x;
            if !cosine {
                sign = -sign;
            }
        }
        if x > 180.0 {
            x = 360.0 - x;
            if !cosine {
                sign = -sign;
            }
        }
        if x > 90.0 {
            x = 180.0 - x;
            if name != "SIND" {
                sign = -sign;
            }
        }
        let result = match name {
            "SIND" => sign * sind_q1(c, x),
            "COSD" => sign * cosd_q1(c, x),
            "TAND" => sign * (sind_q1(c, x) / cosd_q1(c, x) / c.tan_45),
            _ => sign * (cosd_q1(c, x) / sind_q1(c, x) / c.cot_45),
        };
        // No minus zero (`tand(180)`).
        if result == 0.0 { 0.0 } else { result }
    }
}

/// A float result, or an error for a non-finite result of finite input.
fn float(x: f64) -> Value {
    if x.is_nan() {
        raise("input is out of range")
    } else {
        Value::Float(x)
    }
}

/// Keeps integer results integral when every numeric input was an integer.
fn numeric_like(args: &[Value], x: f64) -> Value {
    if args.iter().all(|a| matches!(a, Value::Int(_))) && x.fract() == 0.0 && x.abs() < 9.2e18 {
        Value::Int(x as i64)
    } else {
        Value::Float(x)
    }
}

fn dispatch(name: &str, args: &[Value]) -> Option<Value> {
    if let Some(value) = crate::session_functions::dispatch(name, args) {
        return Some(value);
    }
    if (BYTEA_FUNCTIONS.contains(&name) || args.iter().any(|a| matches!(a, Value::Bytea(_))))
        && let Some(value) = bytea_function(name, args)
    {
        return Some(value);
    }
    let arity = |n: usize| args.len() == n;
    let arg = |i: usize| args.get(i).unwrap_or(&Value::Null);
    Some(match name {
        // ---- Strings --------------------------------------------------------
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" if arity(1) => {
            Value::Int(text(arg(0)).chars().count() as i64)
        }
        "OCTET_LENGTH" if arity(1) => Value::Int(text(arg(0)).len() as i64),
        "BIT_LENGTH" if arity(1) => Value::Int(text(arg(0)).len() as i64 * 8),
        // Character by character, as the C library maps case: a character
        // whose upper case is several (`ß`) stays as it is.
        "UPPER" if arity(1) => Value::Text(text(arg(0)).chars().map(upper_char).collect()),
        "LOWER" if arity(1) => Value::Text(text(arg(0)).chars().map(lower_char).collect()),
        "CASEFOLD" if arity(1) => Value::Text(text(arg(0)).to_lowercase()),
        "INITCAP" if arity(1) => {
            let mut out = String::new();
            let mut word_start = true;
            for c in text(arg(0)).chars() {
                if c.is_alphanumeric() {
                    out.push(if word_start {
                        upper_char(c)
                    } else {
                        lower_char(c)
                    });
                    word_start = false;
                } else {
                    out.push(c);
                    word_start = true;
                }
            }
            Value::Text(out)
        }
        // `substring(string from pattern)`: the pattern's first
        // parenthesized part, or the whole match.
        "SUBSTR" | "SUBSTRING"
            if arity(2) && matches!(arg(1), Value::Text(t) if t.trim().parse::<i64>().is_err()) =>
        {
            match crate::pg_regex::compile(&text(arg(1)), "") {
                Ok(re) => match re.captures(&text(arg(0))) {
                    Some(caps) => caps
                        .get(if caps.len() > 1 { 1 } else { 0 })
                        .map_or(Value::Null, |m| Value::Text(m.as_str().to_string())),
                    None => Value::Null,
                },
                Err(e) => raise(e),
            }
        }
        // The bytes a value takes, as PostgreSQL stores a computed one (the
        // second argument names its type).
        "PG_COLUMN_SIZE" if arity(1) || arity(2) => {
            let ty = args
                .get(1)
                .map(text)
                .unwrap_or_default()
                .to_ascii_uppercase();
            // A text-search value is measured by its stored binary form.
            if matches!(ty.as_str(), "TSVECTOR" | "TSQUERY") {
                let size = if ty == "TSVECTOR" {
                    crate::textsearch::parse_tsvector(&text(arg(0)))
                        .ok()
                        .map(|v| v.stored_size())
                } else {
                    crate::textsearch::parse_tsquery(&text(arg(0)))
                        .ok()
                        .map(|q| q.stored_size())
                };
                return match size {
                    Some(size) => Some(Value::Int(size as i64)),
                    None => Some(raise("invalid input syntax")),
                };
            }
            let fixed = match ty.as_str() {
                "BOOLEAN" | "BOOL" | "\"CHAR\"" => Some(1),
                "SMALLINT" | "INT2" => Some(2),
                "INTEGER" | "INT" | "INT4" | "REAL" | "FLOAT4" | "DATE" | "OID" => Some(4),
                "BIGINT" | "INT8" | "DOUBLE PRECISION" | "FLOAT8" | "TIMESTAMP" | "TIMESTAMPTZ"
                | "TIME" => Some(8),
                "TIMETZ" => Some(12),
                "INTERVAL" | "UUID" => Some(16),
                _ => None,
            };
            Value::Int(match (fixed, arg(0)) {
                (Some(size), _) => size,
                (None, Value::Int(_)) => 4,
                (None, Value::Float(_)) => 8,
                (None, Value::Bool(_)) => 1,
                (None, Value::Numeric(d)) => {
                    let groups = d.to_binary().len() as i64 - 8;
                    4 + 2 + groups
                }
                (None, Value::Bytea(b)) => 4 + b.len() as i64,
                (None, other) => 4 + render(other).len() as i64,
            })
        }
        "TO_ASCII" if arity(1) || arity(2) => {
            raise("encoding conversion from UTF8 to ASCII not supported")
        }
        "UNISTR" if arity(1) => unistr(&text(arg(0))).map_or_else(raise, Value::Text),
        "NORMALIZE" if arity(1) || arity(2) => {
            use unicode_normalization::UnicodeNormalization;
            let s = text(arg(0));
            match args.get(1).map(text).as_deref().unwrap_or("NFC") {
                "NFC" => Value::Text(s.nfc().collect()),
                "NFD" => Value::Text(s.nfd().collect()),
                "NFKC" => Value::Text(s.nfkc().collect()),
                "NFKD" => Value::Text(s.nfkd().collect()),
                form => raise(format!("invalid normalization form: {form}")),
            }
        }
        "__IS_NORMALIZED__" if arity(2) => {
            use unicode_normalization::{is_nfc, is_nfd, is_nfkc, is_nfkd};
            let s = text(arg(0));
            Value::Bool(match text(arg(1)).as_str() {
                "NFD" => is_nfd(&s),
                "NFKC" => is_nfkc(&s),
                "NFKD" => is_nfkd(&s),
                _ => is_nfc(&s),
            })
        }
        "SUBSTR" | "SUBSTRING" if arity(2) || arity(3) => {
            let chars: Vec<char> = text(arg(0)).chars().collect();
            let start = int(arg(1))?;
            // Positions before 1 still consume the requested length.
            let end = match args.get(2) {
                Some(len) => {
                    let len = int(len)?;
                    if len < 0 {
                        return Some(raise("negative substring length not allowed"));
                    }
                    start.saturating_add(len)
                }
                None => i64::MAX,
            };
            let from = (start.max(1) - 1) as usize;
            let to = (end.max(1) - 1).min(chars.len() as i64) as usize;
            Value::Text(
                chars
                    .get(from..to.max(from))
                    .unwrap_or(&[])
                    .iter()
                    .collect(),
            )
        }
        "STRPOS" if arity(2) => {
            let (s, sub) = (text(arg(0)), text(arg(1)));
            Value::Int(
                s.find(&sub)
                    .map_or(0, |byte| s[..byte].chars().count() as i64 + 1),
            )
        }
        "OVERLAY" if arity(3) || arity(4) => {
            let s: Vec<char> = text(arg(0)).chars().collect();
            let replacement = text(arg(1));
            let from = int(arg(2))?.max(1) as usize - 1;
            let len = match args.get(3) {
                Some(l) => int(l)?.max(0) as usize,
                None => replacement.chars().count(),
            };
            let head: String = s.iter().take(from).collect();
            let tail: String = s.iter().skip(from + len).collect();
            Value::Text(format!("{head}{replacement}{tail}"))
        }
        "TRIM" | "BTRIM" | "LTRIM" | "RTRIM" if arity(1) || arity(2) => {
            let s = text(arg(0));
            let chars: Vec<char> = match args.get(1) {
                Some(set) => text(set).chars().collect(),
                None => vec![' '],
            };
            let trimmed = match name {
                "LTRIM" => s.trim_start_matches(&chars[..]),
                "RTRIM" => s.trim_end_matches(&chars[..]),
                _ => s.trim_matches(&chars[..]),
            };
            Value::Text(trimmed.to_string())
        }
        "LPAD" | "RPAD" if arity(2) || arity(3) => {
            let s: Vec<char> = text(arg(0)).chars().collect();
            let len = int(arg(1))?.max(0) as usize;
            let fill: Vec<char> = match args.get(2) {
                Some(f) => text(f).chars().collect(),
                None => vec![' '],
            };
            if s.len() >= len || fill.is_empty() {
                return Some(Value::Text(s.iter().take(len).collect()));
            }
            let pad: String = fill.iter().cycle().take(len - s.len()).collect();
            let s: String = s.into_iter().collect();
            Value::Text(if name == "LPAD" {
                format!("{pad}{s}")
            } else {
                format!("{s}{pad}")
            })
        }
        "REPLACE" if arity(3) => {
            let (s, from, to) = (text(arg(0)), text(arg(1)), text(arg(2)));
            Value::Text(if from.is_empty() {
                s
            } else {
                s.replace(&from, &to)
            })
        }
        "TRANSLATE" if arity(3) => {
            let from: Vec<char> = text(arg(1)).chars().collect();
            let to: Vec<char> = text(arg(2)).chars().collect();
            Value::Text(
                text(arg(0))
                    .chars()
                    .filter_map(|c| match from.iter().position(|&f| f == c) {
                        Some(i) => to.get(i).copied(),
                        None => Some(c),
                    })
                    .collect(),
            )
        }
        "REPEAT" if arity(2) => Value::Text(text(arg(0)).repeat(int(arg(1))?.max(0) as usize)),
        "REVERSE" if arity(1) => Value::Text(text(arg(0)).chars().rev().collect()),
        "MD5" if arity(1) => {
            use md5::Digest;
            let digest = md5::Md5::digest(text(arg(0)).as_bytes());
            Value::Text(digest.iter().map(|b| format!("{b:02x}")).collect())
        }
        "SPLIT_PART" if arity(3) => {
            let (s, delim) = (text(arg(0)), text(arg(1)));
            let n = int(arg(2))?;
            if n == 0 {
                return Some(raise("field position must not be zero"));
            }
            let parts: Vec<&str> = if delim.is_empty() {
                vec![s.as_str()]
            } else {
                s.split(delim.as_str()).collect()
            };
            let idx = if n > 0 { n - 1 } else { parts.len() as i64 + n };
            Value::Text(
                usize::try_from(idx)
                    .ok()
                    .and_then(|i| parts.get(i))
                    .unwrap_or(&"")
                    .to_string(),
            )
        }
        "LEFT" | "RIGHT" if arity(2) => {
            let chars: Vec<char> = text(arg(0)).chars().collect();
            let n = int(arg(1))?;
            let len = chars.len() as i64;
            let keep = if n >= 0 { n.min(len) } else { (len + n).max(0) } as usize;
            Value::Text(if name == "LEFT" {
                chars[..keep].iter().collect()
            } else {
                chars[chars.len() - keep..].iter().collect()
            })
        }
        "CONCAT" => Value::Text(
            args.iter()
                .filter(|v| !matches!(v, Value::Null))
                .map(text)
                .collect(),
        ),
        "CONCAT_WS" if !args.is_empty() => match arg(0) {
            Value::Null => Value::Null,
            sep => Value::Text(
                args[1..]
                    .iter()
                    .filter(|v| !matches!(v, Value::Null))
                    .map(text)
                    .collect::<Vec<_>>()
                    .join(&text(sep)),
            ),
        },
        "FORMAT" if !args.is_empty() => match arg(0) {
            Value::Null => Value::Null,
            fmt => format_text(&text(fmt), &args[1..]),
        },
        "QUOTE_IDENT" if arity(1) => Value::Text(quote_ident(&text(arg(0)))),
        "QUOTE_LITERAL" if arity(1) => Value::Text(quote_literal(&text(arg(0)))),
        "QUOTE_NULLABLE" if arity(1) => Value::Text(match arg(0) {
            Value::Null => "NULL".to_string(),
            v => quote_literal(&text(v)),
        }),
        "ASCII" if arity(1) => Value::Int(text(arg(0)).chars().next().map_or(0, |c| c as i64)),
        "CHR" if arity(1) => {
            let n = int(arg(0))?;
            match u32::try_from(n).ok().map(|u| (u, char::from_u32(u))) {
                _ if n < 0 => raise("character number must be positive"),
                Some((0, _)) => raise("null character not permitted"),
                Some((_, Some(c))) => Value::Text(c.to_string()),
                Some((u, None)) if u <= 0x10FFFF => {
                    raise(format!("requested character not valid for encoding: {n}"))
                }
                _ => raise(format!("requested character too large for encoding: {n}")),
            }
        }
        // A 32-bit integer (the second argument names the type) shows its
        // 32 bits.
        "TO_HEX" | "TO_BIN" | "TO_OCT" if arity(1) || arity(2) => {
            let n = int(arg(0))?;
            let narrow = matches!(
                args.get(1),
                Some(Value::Text(t)) if matches!(t.as_str(), "INTEGER" | "INT" | "INT4" | "SMALLINT" | "INT2")
            );
            let bits = if narrow {
                u64::from(n as u32)
            } else {
                n as u64
            };
            Value::Text(match name {
                "TO_HEX" => format!("{bits:x}"),
                "TO_BIN" => format!("{bits:b}"),
                _ => format!("{bits:o}"),
            })
        }
        "STARTS_WITH" if arity(2) => Value::Bool(text(arg(0)).starts_with(&text(arg(1)))),
        // `regexp_replace(s, p, r [, start [, n]] [, flags])`: an integer
        // fourth argument is the start, text the flags.
        "REGEXP_REPLACE" if (3..=6).contains(&args.len()) => {
            let positional = matches!(args.get(3), Some(Value::Int(_)));
            let (start, n, flags) = if positional {
                (
                    int(arg(3))?,
                    match args.get(4) {
                        Some(v) => Some(int(v)?),
                        None => None,
                    },
                    args.get(5).map(text).unwrap_or_default(),
                )
            } else {
                (1, None, args.get(3).map(text).unwrap_or_default())
            };
            crate::pg_regex::replace(
                &text(arg(0)),
                &text(arg(1)),
                &text(arg(2)),
                start,
                n,
                &flags,
            )
            .map_or_else(raise, Value::Text)
        }
        "REGEXP_MATCH" if arity(2) || arity(3) => {
            let flags = args.get(2).map(text).unwrap_or_default();
            if flags.contains('g') {
                return Some(raise(
                    crate::error_fields::DbError::new(
                        "regexp_match() does not support the \"global\" option",
                    )
                    .hint("Use the regexp_matches function instead.")
                    .into_text(),
                ));
            }
            match crate::pg_regex::compile(&text(arg(1)), &flags) {
                Ok(re) => re
                    .captures(&text(arg(0)))
                    .map_or(Value::Null, |caps| crate::pg_regex::match_array(&caps)),
                Err(e) => raise(e),
            }
        }
        "REGEXP_LIKE" if arity(2) || arity(3) => {
            let flags = args.get(2).map(text).unwrap_or_default();
            match crate::pg_regex::compile(&text(arg(1)), &flags) {
                Ok(re) => Value::Bool(re.is_match(&text(arg(0)))),
                Err(e) => raise(e),
            }
        }
        "REGEXP_COUNT" if (2..=4).contains(&args.len()) => {
            let start = args.get(2).map_or(Some(1), int)?;
            let flags = args.get(3).map(text).unwrap_or_default();
            crate::pg_regex::count(&text(arg(0)), &text(arg(1)), start, &flags)
                .map_or_else(raise, Value::Int)
        }
        "REGEXP_INSTR" if (2..=7).contains(&args.len()) => {
            let start = args.get(2).map_or(Some(1), int)?;
            let n = args.get(3).map_or(Some(1), int)?;
            let end_option = args.get(4).map_or(Some(0), int)?;
            let flags = args.get(5).map(text).unwrap_or_default();
            let subexpr = args.get(6).map_or(Some(0), int)?;
            crate::pg_regex::instr(
                &text(arg(0)),
                &text(arg(1)),
                start,
                n,
                end_option,
                &flags,
                subexpr,
            )
            .map_or_else(raise, Value::Int)
        }
        "REGEXP_SUBSTR" if (2..=6).contains(&args.len()) => {
            let start = args.get(2).map_or(Some(1), int)?;
            let n = args.get(3).map_or(Some(1), int)?;
            let flags = args.get(4).map(text).unwrap_or_default();
            let subexpr = args.get(5).map_or(Some(0), int)?;
            crate::pg_regex::substr(&text(arg(0)), &text(arg(1)), start, n, &flags, subexpr)
                .map_or_else(raise, |m| m.map_or(Value::Null, Value::Text))
        }
        "REGEXP_SPLIT_TO_ARRAY" if arity(2) || arity(3) => {
            let flags = args.get(2).map(text).unwrap_or_default();
            match crate::pg_regex::split(&text(arg(0)), &text(arg(1)), &flags) {
                Ok(pieces) => Value::Array(pieces.into_iter().map(Value::Text).collect()),
                Err(e) => raise(e.replace("regexp_split_to_table", "regexp_split_to_array")),
            }
        }
        "STRING_TO_ARRAY" if arity(2) || arity(3) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let s = text(arg(0));
            let null_str = args.get(2).filter(|v| !matches!(v, Value::Null)).map(text);
            let as_value = |p: String| match &null_str {
                Some(n) if &p == n => Value::Null,
                _ => Value::Text(p),
            };
            if s.is_empty() {
                return Some(Value::Array(Vec::new()));
            }
            Value::Array(match arg(1) {
                Value::Null => s.chars().map(|c| as_value(c.to_string())).collect(),
                d if text(d).is_empty() => vec![as_value(s)],
                d => s
                    .split(text(d).as_str())
                    .map(|p| as_value(p.to_string()))
                    .collect(),
            })
        }
        "ARRAY_TO_STRING" if arity(2) || arity(3) => {
            let (Some(items), false) = (array(arg(0)), matches!(arg(1), Value::Null)) else {
                return Some(Value::Null);
            };
            let null_str = args.get(2).filter(|v| !matches!(v, Value::Null)).map(text);
            let mut flat = Vec::new();
            flatten(items, &mut flat);
            Value::Text(
                flat.iter()
                    .filter_map(|v| match v {
                        Value::Null => null_str.clone(),
                        v => Some(text(v)),
                    })
                    .collect::<Vec<_>>()
                    .join(&text(arg(1))),
            )
        }

        // ---- Conditionals ---------------------------------------------------
        // `ARRAY[a, b, ...]` over per-row values.
        "ARRAY" => Value::Array(args.to_vec()),
        "COALESCE" => args
            .iter()
            .find(|v| !matches!(v, Value::Null))
            .cloned()
            .unwrap_or(Value::Null),
        "NULLIF" if arity(2) => {
            if !matches!(arg(0), Value::Null)
                && !matches!(arg(1), Value::Null)
                && crate::planner::apply_binary_op(
                    crate::ScalarBinaryOp::Eq,
                    arg(0).clone(),
                    arg(1).clone(),
                ) == Value::Bool(true)
            {
                Value::Null
            } else {
                arg(0).clone()
            }
        }
        "GREATEST" | "LEAST" if !args.is_empty() => {
            let mut best: Option<&Value> = None;
            for v in args.iter().filter(|v| !matches!(v, Value::Null)) {
                let better = match best {
                    None => true,
                    Some(b) => {
                        let op = if name == "GREATEST" {
                            crate::ScalarBinaryOp::Gt
                        } else {
                            crate::ScalarBinaryOp::Lt
                        };
                        crate::planner::apply_binary_op(op, v.clone(), b.clone())
                            == Value::Bool(true)
                    }
                };
                if better {
                    best = Some(v);
                }
            }
            best.cloned().unwrap_or(Value::Null)
        }
        "NUM_NULLS" => Value::Int(args.iter().filter(|v| matches!(v, Value::Null)).count() as i64),
        "NUM_NONNULLS" => {
            Value::Int(args.iter().filter(|v| !matches!(v, Value::Null)).count() as i64)
        }

        // ---- Math -------------------------------------------------------------
        crate::result_types::BPCHAR_PAD if arity(2) => Value::Text(crate::value::pad_character(
            &text(arg(0)),
            int(arg(1))? as usize,
        )),
        // Bit string operations: `__BITS__(operation, bits, ...)`.
        crate::result_types::BITS if args.len() >= 2 => {
            let bits = match crate::bits::parse(&text(arg(1))) {
                Ok(bits) => bits,
                Err(e) => return Some(raise(e)),
            };
            let index = |i: usize| -> Result<usize, String> {
                let n = args.get(i).and_then(int).unwrap_or(-1);
                if n < 0 || n as usize >= bits.len() {
                    Err(format!(
                        "bit index {n} out of valid range (0..{})",
                        bits.len().saturating_sub(1)
                    ))
                } else {
                    Ok(n as usize)
                }
            };
            let result: Result<Value, String> = match text(arg(0)).as_str() {
                "int" => crate::bits::to_int(&bits, int(arg(2))? as usize).map(Value::Int),
                "get_bit" => index(2).map(|i| Value::Int(i64::from(bits.as_bytes()[i] == b'1'))),
                "set_bit" => index(2).and_then(|i| match args.get(3).and_then(int) {
                    Some(v @ 0..=1) => {
                        let mut out = bits.clone().into_bytes();
                        out[i] = if v == 1 { b'1' } else { b'0' };
                        Ok(Value::Text(String::from_utf8(out).unwrap_or_default()))
                    }
                    _ => Err("new bit must be 0 or 1".into()),
                }),
                "bit_count" => Ok(Value::Int(
                    bits.bytes().filter(|b| *b == b'1').count() as i64
                )),
                "octet_length" => Ok(Value::Int(bits.len().div_ceil(8) as i64)),
                _ => Ok(Value::Int(bits.len() as i64)),
            };
            result.unwrap_or_else(raise)
        }
        crate::result_types::REAL_TEXT if arity(1) => match arg(0) {
            Value::Float(f) => Value::Text(crate::value::float4_text(*f as f32)),
            other => Value::Text(text(other)),
        },
        crate::result_types::REAL_NUMERIC if arity(1) => match arg(0) {
            Value::Float(f) => Value::Numeric(Numeric::from_f32(*f as f32)),
            other => other.clone(),
        },
        name if crate::result_types::bitwise_arity(name).is_some() => {
            bitwise(name, args).map_or_else(raise, |v| v)
        }
        "ABS" if arity(1) => match arg(0) {
            Value::Int(i) => i
                .checked_abs()
                .map_or_else(|| raise("bigint out of range"), Value::Int),
            Value::Numeric(d) => Value::Numeric(d.abs()),
            v => Value::Float(num(v)?.abs()),
        },
        // On a numeric these keep exact decimals, as PostgreSQL's numeric
        // variants do.
        "SIGN" if arity(1) && matches!(arg(0), Value::Numeric(_)) => {
            let d = decimal(arg(0))?;
            Value::Numeric(if d.is_nan() {
                Numeric::NaN
            } else {
                Numeric::from(d.signum())
            })
        }
        "CEIL" | "CEILING" | "FLOOR" if arity(1) && matches!(arg(0), Value::Numeric(_)) => {
            let d = decimal(arg(0))?;
            Value::Numeric(if name == "FLOOR" { d.floor() } else { d.ceil() })
        }
        "ROUND" | "TRUNC"
            if (arity(1) || arity(2))
                && (matches!(arg(0), Value::Numeric(_))
                    || (arity(2) && matches!(arg(0), Value::Int(_)))) =>
        {
            let d = decimal(arg(0))?;
            let digits = match args.get(1) {
                Some(n) => int(n)?,
                None => 0,
            };
            Value::Numeric(if name == "ROUND" {
                d.round(digits)
            } else {
                d.trunc(digits)
            })
        }
        "SCALE" | "MIN_SCALE" | "TRIM_SCALE" if arity(1) => {
            let d = decimal(arg(0))?;
            match name {
                "TRIM_SCALE" => Value::Numeric(d.normalize()),
                _ if !d.is_finite() => Value::Null,
                "SCALE" => Value::Int(i64::from(d.scale())),
                _ => Value::Int(i64::from(d.normalize().scale())),
            }
        }
        "SIGN" if arity(1) => {
            numeric_like(args, num(arg(0))?.signum() * f64::from(num(arg(0))? != 0.0))
        }
        "CEIL" | "CEILING" if arity(1) => numeric_like(args, num(arg(0))?.ceil()),
        "FLOOR" if arity(1) => numeric_like(args, num(arg(0))?.floor()),
        "ROUND" | "TRUNC" if arity(1) || arity(2) => {
            let x = num(arg(0))?;
            let digits = match args.get(1) {
                Some(d) => int(d)?,
                None => 0,
            };
            let factor = 10f64.powi(digits.clamp(-300, 300) as i32);
            let scaled = x * factor;
            // A float rounds half to even, as PostgreSQL's round(float8).
            let r = if name == "ROUND" {
                scaled.round_ties_even()
            } else {
                scaled.trunc()
            } / factor;
            if matches!(arg(0), Value::Int(_)) && digits >= 0 {
                arg(0).clone()
            } else {
                Value::Float(r)
            }
        }
        "MOD" if arity(2) => crate::planner::apply_binary_op(
            crate::ScalarBinaryOp::Mod,
            arg(0).clone(),
            arg(1).clone(),
        ),
        // `div` is numeric only: the quotient truncated to a whole number.
        "DIV" if arity(2) => numeric_result(decimal(arg(0))?.div_trunc(&decimal(arg(1))?)),
        "POWER" | "POW" if arity(2) && numeric_call(args) => {
            numeric_result(decimal(arg(0))?.power(&decimal(arg(1))?))
        }
        "POWER" | "POW" if arity(2) => float_power(num(arg(0))?, num(arg(1))?),
        "SQRT" if arity(1) && numeric_call(args) => numeric_result(decimal(arg(0))?.sqrt()),
        "EXP" if arity(1) && numeric_call(args) => numeric_result(decimal(arg(0))?.exp()),
        "LN" if arity(1) && numeric_call(args) => numeric_result(decimal(arg(0))?.ln()),
        "LOG" | "LOG10" if arity(1) && numeric_call(args) => {
            numeric_result(Numeric::from(10).log(&decimal(arg(0))?))
        }
        // `log(b, x)` is numeric only.
        "LOG" if arity(2) => numeric_result(decimal(arg(0))?.log(&decimal(arg(1))?)),
        "SQRT" if arity(1) => {
            let x = num(arg(0))?;
            if x < 0.0 {
                raise("cannot take square root of a negative number")
            } else {
                Value::Float(x.sqrt())
            }
        }
        "CBRT" if arity(1) => Value::Float(num(arg(0))?.cbrt()),
        "EXP" if arity(1) => {
            let x = num(arg(0))?;
            let r = x.exp();
            if x.is_finite() && r.is_infinite() {
                raise("value out of range: overflow")
            } else if x.is_finite() && r == 0.0 {
                raise("value out of range: underflow")
            } else {
                Value::Float(r)
            }
        }
        "LN" | "LOG" | "LOG10" if arity(1) => {
            let x = num(arg(0))?;
            if x == 0.0 {
                raise("cannot take logarithm of zero")
            } else if x < 0.0 {
                raise("cannot take logarithm of a negative number")
            } else if name == "LN" {
                Value::Float(x.ln())
            } else {
                Value::Float(x.log10())
            }
        }
        "PI" if arity(0) => Value::Float(std::f64::consts::PI),
        "DEGREES" if arity(1) => float_quotient(num(arg(0))?, RADIANS_PER_DEGREE),
        "RADIANS" if arity(1) => {
            let x = num(arg(0))?;
            let r = x * RADIANS_PER_DEGREE;
            if x.is_finite() && r.is_infinite() {
                raise("value out of range: overflow")
            } else if x != 0.0 && r == 0.0 {
                raise("value out of range: underflow")
            } else {
                Value::Float(r)
            }
        }
        "SIN" | "COS" | "TAN" | "COT" if arity(1) => {
            let x = num(arg(0))?;
            if x.is_infinite() {
                return Some(raise("input is out of range"));
            }
            Value::Float(match name {
                "SIN" => x.sin(),
                "COS" => x.cos(),
                "TAN" => x.tan(),
                _ => 1.0 / x.tan(),
            })
        }
        "ASIN" | "ACOS" | "ASIND" | "ACOSD" if arity(1) => {
            let x = num(arg(0))?;
            if !(-1.0..=1.0).contains(&x) && !x.is_nan() {
                return Some(raise("input is out of range"));
            }
            Value::Float(match name {
                "ASIN" => x.asin(),
                "ACOS" => x.acos(),
                "ASIND" if x >= 0.0 => degrees::asind_q1(x),
                "ASIND" => -degrees::asind_q1(-x),
                _ if x >= 0.0 => degrees::acosd_q1(x),
                _ => 90.0 + degrees::asind_q1(-x),
            })
        }
        "ATAN" if arity(1) => Value::Float(num(arg(0))?.atan()),
        "ATAN2" if arity(2) => Value::Float(num(arg(0))?.atan2(num(arg(1))?)),
        "ATAND" if arity(1) => {
            Value::Float(num(arg(0))?.atan() / degrees::constants().atan_1_0 * 45.0)
        }
        "ATAN2D" if arity(2) => {
            Value::Float(num(arg(0))?.atan2(num(arg(1))?) / degrees::constants().atan_1_0 * 45.0)
        }
        "SIND" | "COSD" | "TAND" | "COTD" if arity(1) => {
            let x = num(arg(0))?;
            if x.is_infinite() {
                return Some(raise("input is out of range"));
            }
            Value::Float(degrees::trig(name, x))
        }
        "SINH" if arity(1) => Value::Float(num(arg(0))?.sinh()),
        "COSH" if arity(1) => Value::Float(num(arg(0))?.cosh()),
        "TANH" if arity(1) => Value::Float(num(arg(0))?.tanh()),
        "ASINH" if arity(1) => Value::Float(num(arg(0))?.asinh()),
        "ACOSH" if arity(1) => {
            let x = num(arg(0))?;
            if x < 1.0 {
                raise("input is out of range")
            } else {
                Value::Float(x.acosh())
            }
        }
        "ATANH" if arity(1) => {
            let x = num(arg(0))?;
            if !(-1.0..=1.0).contains(&x) && !x.is_nan() {
                raise("input is out of range")
            } else if x.abs() == 1.0 {
                Value::Float(f64::INFINITY.copysign(x))
            } else {
                Value::Float(x.atanh())
            }
        }
        "GCD" | "LCM" if arity(2) && numeric_call(args) => {
            numeric_result(decimal(arg(0))?.gcd_lcm(&decimal(arg(1))?, name == "LCM"))
        }
        "GCD" | "LCM" if arity(2) => {
            let (a, b) = (int(arg(0))?.unsigned_abs(), int(arg(1))?.unsigned_abs());
            let gcd = {
                let (mut x, mut y) = (a, b);
                while y != 0 {
                    (x, y) = (y, x % y);
                }
                x
            };
            let out = if name == "GCD" {
                gcd
            } else if a == 0 || b == 0 {
                0
            } else {
                a / gcd * b
            };
            i64::try_from(out).map_or_else(|_| raise("bigint out of range"), Value::Int)
        }
        // A numeric, however many digits it takes.
        "FACTORIAL" if arity(1) => {
            let n = int(arg(0))?;
            if n < 0 {
                return Some(raise("factorial of a negative number is undefined"));
            }
            if n > 32177 {
                return Some(raise("value overflows numeric format"));
            }
            Value::Numeric(Numeric::new(
                (2..=n).fold(num_bigint::BigInt::from(1), |acc, k| acc * k),
                0,
            ))
        }
        "RANDOM" if arity(0) => Value::Float(crate::random::uniform()),
        "RANDOM" if arity(2) => {
            let (lo, hi) = (int(arg(0))?, int(arg(1))?);
            if lo > hi {
                return Some(raise(
                    "lower bound must be less than or equal to upper bound",
                ));
            }
            Value::Int(crate::random::int_range(lo, hi))
        }
        "RANDOM_NORMAL" if args.len() <= 2 => {
            let mean = args.first().map_or(Some(0.0), num)?;
            let stddev = args.get(1).map_or(Some(1.0), num)?;
            if stddev < 0.0 {
                return Some(raise("standard deviation cannot be negative"));
            }
            Value::Float(stddev * crate::random::normal() + mean)
        }
        "SETSEED" if arity(1) => {
            let seed = num(arg(0))?;
            if !(-1.0..=1.0).contains(&seed) {
                return Some(raise(format!(
                    "setseed parameter {} is out of allowed range [-1,1]",
                    crate::value::render(&Value::Float(seed))
                )));
            }
            crate::random::set_seed(seed);
            crate::session_functions::void()
        }
        "WIDTH_BUCKET" if arity(4) && numeric_call(&args[..3]) => {
            let bucket = |x: &Numeric, lo: &Numeric, hi: &Numeric, n: i64| -> Result<i64, String> {
                if x.is_nan() || lo.is_nan() || hi.is_nan() {
                    return Err("operand, lower bound, and upper bound cannot be NaN".into());
                }
                if !lo.is_finite() || !hi.is_finite() {
                    return Err("lower and upper bounds must be finite".into());
                }
                let inside = if lo < hi {
                    x >= lo && x < hi
                } else {
                    x <= lo && x > hi
                };
                let below = if lo < hi { x < lo } else { x > lo };
                if below {
                    return Ok(0);
                }
                if !inside {
                    return Ok(n + 1);
                }
                let scaled = &(x - lo) * &Numeric::from(n);
                Ok((scaled.div_trunc(&(hi - lo))?.to_i64().unwrap_or(0)) + 1)
            };
            let n = int(arg(3))?;
            if n <= 0 {
                return Some(raise("count must be greater than zero"));
            }
            let (x, lo, hi) = (decimal(arg(0))?, decimal(arg(1))?, decimal(arg(2))?);
            if lo == hi {
                return Some(raise("lower bound cannot equal upper bound"));
            }
            bucket(&x, &lo, &hi, n).map_or_else(raise, Value::Int)
        }
        "WIDTH_BUCKET" if arity(4) => {
            let (x, lo, hi, n) = (num(arg(0))?, num(arg(1))?, num(arg(2))?, int(arg(3))?);
            if n <= 0 {
                return Some(raise("count must be greater than zero"));
            }
            if x.is_nan() || lo.is_nan() || hi.is_nan() {
                return Some(raise("operand, lower bound, and upper bound cannot be NaN"));
            }
            if lo.is_infinite() || hi.is_infinite() {
                return Some(raise("lower and upper bounds must be finite"));
            }
            if lo == hi {
                return Some(raise("lower bound cannot equal upper bound"));
            }
            let fraction = if lo < hi {
                if x < lo {
                    return Some(Value::Int(0));
                } else if x >= hi {
                    return Some(Value::Int(n + 1));
                }
                (x - lo) / (hi - lo)
            } else {
                if x > lo {
                    return Some(Value::Int(0));
                } else if x <= hi {
                    return Some(Value::Int(n + 1));
                }
                (lo - x) / (lo - hi)
            };
            // The quotient could round to 1, which would name the next bucket.
            Value::Int(((n as f64 * fraction) as i64).min(n - 1) + 1)
        }
        // The bucket of an array of ascending lower bounds.
        "WIDTH_BUCKET" if arity(2) => {
            let thresholds = array(arg(1))?;
            if thresholds.iter().any(|t| matches!(t, Value::Null)) {
                return Some(raise("thresholds array must not contain NULLs"));
            }
            let (mut lo, mut hi) = (0, thresholds.len());
            while lo < hi {
                let mid = (lo + hi) / 2;
                if crate::value::compare(arg(0), &thresholds[mid]) != std::cmp::Ordering::Less {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            Value::Int(lo as i64)
        }

        // ---- Dates and times ----------------------------------------------------
        "NOW" | "CURRENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" if arity(0) => {
            timestamp(session_time(|e| e.transaction_micros)?, true)
        }
        // With a precision: rounded to that many fractional digits.
        "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" | "CURRENT_TIME" | "LOCALTIME" if arity(1) => {
            let precision = int(arg(0))?;
            if precision < 0 {
                return Some(raise(format!(
                    "{}({precision}) precision must not be negative",
                    name.to_ascii_uppercase()
                )));
            }
            let unit = 10i64.pow((6 - precision.min(6)) as u32);
            let micros = session_time(|e| e.transaction_micros)?;
            let rounded = (micros + unit / 2).div_euclid(unit) * unit;
            let Some(now) = chrono::DateTime::from_timestamp_micros(rounded) else {
                return Some(raise("timestamp out of range"));
            };
            if name == "CURRENT_TIMESTAMP" {
                timestamp(rounded, true)
            } else {
                let (local, offset) = crate::timezone::to_session_local(now.naive_utc());
                let ts = crate::value::format_timestamp(local, false);
                Value::Text(match name {
                    "LOCALTIMESTAMP" => ts,
                    "LOCALTIME" => ts[11..].to_string(),
                    _ => format!("{}{}", &ts[11..], crate::timezone::offset_text(offset)),
                })
            }
        }
        // The current time as text, in the Unix `date` form.
        "TIMEOFDAY" if arity(0) => {
            let Some(now) = chrono::DateTime::from_timestamp_micros(session_env::wall_micros())
            else {
                return Some(raise("timestamp out of range"));
            };
            let (local, _) = crate::timezone::to_session_local(now.naive_utc());
            let zone = crate::timezone::session_abbreviation(now.naive_utc());
            Value::Text(format!(
                "{} {zone}",
                local.format("%a %b %d %H:%M:%S%.6f %Y")
            ))
        }
        "ARRAY_DIMS" if arity(1) => {
            let items = array(arg(0))?;
            if items.is_empty() {
                return Some(Value::Null);
            }
            let mut dims = String::new();
            let mut level = Value::Array(items);
            while let Value::Array(items) = level {
                dims.push_str(&format!("[1:{}]", items.len()));
                level = items.into_iter().next().unwrap_or(Value::Null);
            }
            Value::Text(dims)
        }
        "ARRAY_FILL" if arity(2) || arity(3) => {
            let dims = array(arg(1))?;
            if dims.iter().any(|d| matches!(d, Value::Null)) {
                return Some(raise("dimension values cannot be null"));
            }
            let mut filled = arg(0).clone();
            for d in dims.iter().rev() {
                let n = int(d)?.max(0) as usize;
                filled = Value::Array(vec![filled; n]);
            }
            filled
        }
        "STATEMENT_TIMESTAMP" if arity(0) => timestamp(session_time(|e| e.statement_micros)?, true),
        "CLOCK_TIMESTAMP" if arity(0) => timestamp(session_env::wall_micros(), true),
        // The transaction's start as local time in the session's zone.
        "LOCALTIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "LOCALTIME" if arity(0) => {
            let micros = session_time(|e| e.transaction_micros)?;
            let Some(now) = chrono::DateTime::from_timestamp_micros(micros) else {
                return Some(raise("timestamp out of range"));
            };
            let (local, offset) = crate::timezone::to_session_local(now.naive_utc());
            let ts = crate::value::format_timestamp(local, false);
            Value::Text(match name {
                "LOCALTIMESTAMP" => ts,
                "CURRENT_DATE" => ts[..10].to_string(),
                "LOCALTIME" => ts[11..].to_string(),
                _ => format!("{}{}", &ts[11..], crate::timezone::offset_text(offset)),
            })
        }
        "__TZ_TEXT__" if arity(1) => {
            Value::Text(crate::timezone::session_timestamptz_text(&text(arg(0))))
        }
        // A timestamp's ISO 8601 text, as JSON writes it; a zoned one in the
        // session's zone, with its offset's minutes.
        "__JSON_TIME__" if arity(2) => {
            let value = text(arg(0));
            let (stamp, offset) = if text(arg(1)) == "TIMESTAMPTZ" {
                let local = crate::timezone::session_timestamptz_text(&value);
                match local.rfind(['+', '-']).filter(|at| *at > 10) {
                    Some(at) => (local[..at].to_string(), local[at..].to_string()),
                    None => (local, String::new()),
                }
            } else {
                (value, String::new())
            };
            let offset = if offset.len() == 3 {
                format!("{offset}:00")
            } else {
                offset
            };
            Value::Text(format!("{}{offset}", stamp.replacen(' ', "T", 1)))
        }
        "__INTERVAL_SPAN__" if arity(1) => match crate::datetime::Interval::parse(&text(arg(0))) {
            Some(iv) => Value::Numeric(Numeric::from(iv.span())),
            None => arg(0).clone(),
        },
        // Network-family operators and casts, with their type, as the
        // planner passes them.
        "__NET__" if arity(5) => {
            let Some(kind) = crate::net::Kind::of(&text(arg(3))) else {
                return Some(raise(format!(
                    "operator does not exist: {} {}",
                    crate::value::value_type_name(arg(1)),
                    text(arg(0))
                )));
            };
            let number = matches!(arg(4), Value::Bool(true));
            match kind {
                crate::net::Kind::Money => crate::net::money_operator(
                    &text(arg(0)),
                    &text(arg(1)),
                    &text(arg(2)),
                    !number,
                )
                .unwrap_or_else(raise),
                crate::net::Kind::MacAddr | crate::net::Kind::MacAddr8 => {
                    let op = text(arg(0));
                    match op.as_str() {
                        "=" | "<>" | "<" | ">" | "<=" | ">=" => {
                            let ord = crate::net::cmp_mac(
                                &text(arg(1)),
                                &text(arg(2)),
                                kind == crate::net::Kind::MacAddr8,
                            );
                            use std::cmp::Ordering::*;
                            let holds = match op.as_str() {
                                "=" => ord == Equal,
                                "<>" => ord != Equal,
                                "<" => ord == Less,
                                ">" => ord == Greater,
                                "<=" => ord != Greater,
                                _ => ord != Less,
                            };
                            Value::Bool(holds)
                        }
                        _ => raise(format!(
                            "operator does not exist: {} {}",
                            kind.name(),
                            op
                        )),
                    }
                }
                _ => crate::net::inet_operator(&text(arg(0)), &text(arg(1)), &text(arg(2)), number)
                    .unwrap_or_else(raise),
            }
        }
        "__NET_CAST__" if arity(3) => {
            let (Some(from), Some(to)) = (
                crate::net::Kind::of(&text(arg(0))),
                crate::net::Kind::of(&text(arg(1))),
            ) else {
                return Some(raise(format!(
                    "cannot cast type {} to {}",
                    text(arg(0)),
                    text(arg(1))
                )));
            };
            crate::net::cast_between(from, to, &text(arg(2))).unwrap_or_else(raise)
        }
        "__MONEY_TEXT__" if arity(1) => Value::Text(crate::net::money_as_text(&text(arg(0)))),
        // `range::multirange` wraps the range's text in braces.
        "__MULTIRANGE_FROM_RANGE__" if arity(2) => {
            let kind = crate::multiranges::kind_of(&text(arg(0)))
                .unwrap_or(crate::ranges::Kind::Int4);
            let range = text(arg(1));
            crate::multiranges::from_literal(kind, &format!("{{{range}}}"))
                .map(Value::Text)
                .unwrap_or_else(raise)
        }
        "CASH_WORDS" if arity(1) => match crate::net::money_cents(&text(arg(0))) {
            Some(cents) => Value::Text(crate::net::cash_words(cents)),
            None => raise(format!("invalid money value: {}", text(arg(0)))),
        },
        "CASHLARGER" | "CASHSMALLER" if arity(2) => {
            let (left, right) = (text(arg(0)), text(arg(1)));
            let ord = crate::net::cmp(crate::net::Kind::Money, &left, &right);
            let take_left = if name == "CASHLARGER" {
                ord != std::cmp::Ordering::Less
            } else {
                ord == std::cmp::Ordering::Less
            };
            Value::Text(if take_left { left } else { right })
        }
        "__MAC_TRUNC__" if arity(1) || arity(2) => {
            let kind = if arity(2) {
                crate::net::Kind::of(&text(arg(1)))
            } else {
                None
            };
            match kind {
                Some(crate::net::Kind::MacAddr8) => Value::Text(format!(
                    "{}:00:00:00:00:00",
                    text(arg(0)).split(':').take(3).collect::<Vec<_>>().join(":")
                )),
                _ => Value::Text(format!(
                    "{}:00:00:00",
                    text(arg(0)).split(':').take(3).collect::<Vec<_>>().join(":")
                )),
            }
        }
        "MACADDR8_SET7BIT" if arity(1) || arity(2) => {
            let bytes: Vec<u8> = text(arg(0))
                .split(':')
                .filter_map(|b| u8::from_str_radix(b, 16).ok())
                .collect();
            match bytes.len() {
                6 => {
                    let mut out = vec![bytes[0] ^ 0x02, bytes[1], bytes[2], 0xff, 0xfe];
                    out.extend_from_slice(&bytes[3..]);
                    Value::Text(out.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(":"))
                }
                8 => {
                    let mut out = bytes;
                    out[0] ^= 0x02;
                    Value::Text(out.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(":"))
                }
                _ => raise(format!(
                    "invalid input syntax for type macaddr8: \"{}\"",
                    text(arg(0))
                )),
            }
        }
        // Range operators, with their subtype and whether the right operand
        // is an element, as the planner passes them.
        "__RANGE__" if arity(5) => {
            let Some(kind) = crate::ranges::Kind::of(&text(arg(3))) else {
                return Some(raise(format!(
                    "operator does not exist: {} {}",
                    crate::value::value_type_name(arg(1)),
                    text(arg(0))
                )));
            };
            let (left, right) = (text(arg(1)), text(arg(2)));
            // A braced operand makes it a multirange operation.
            if crate::multiranges::is_multirange_text(&left)
                || crate::multiranges::is_multirange_text(&right)
            {
                crate::multiranges::operator(
                    &text(arg(0)),
                    kind,
                    &left,
                    &right,
                    matches!(arg(4), Value::Bool(true)),
                )
                .unwrap_or_else(raise)
            } else {
                crate::ranges::operator(
                    &text(arg(0)),
                    kind,
                    &left,
                    &right,
                    matches!(arg(4), Value::Bool(true)),
                )
                .unwrap_or_else(raise)
            }
        }
        "__RANGE_LOWER__" | "__RANGE_UPPER__" if arity(1) || arity(2) => {
            let lower = name == "__RANGE_LOWER__";
            if crate::multiranges::is_multirange_text(&text(arg(0))) {
                return Some(crate::multiranges::bound_value(&text(arg(0)), lower).unwrap_or_else(raise));
            }
            crate::ranges::bound_value(&text(arg(0)), name == "__RANGE_LOWER__")
                .unwrap_or_else(raise)
        }
        // `ts_headline` with its planner-assigned shape:
        // `(with-config, config, document, query, options)`. An empty
        // options text means "not given"; NULL anywhere is NULL, as the
        // strict function is.
        "__TS_HEADLINE__" if arity(5) => {
            let with_config = matches!(args[0], Value::Int(n) if n != 0);
            if args[2..].iter().any(|a| matches!(a, Value::Null))
                || (with_config && matches!(args[1], Value::Null))
            {
                return Some(Value::Null);
            }
            let config = if with_config {
                match to_ts_config(&args[1]) {
                    Ok(config) => config,
                    Err(error) => return Some(raise(error)),
                }
            } else {
                default_ts_config()
            };
            let query = match crate::textsearch::parse_tsquery(&text(&args[3])) {
                Ok(query) => query,
                Err(error) => return Some(raise(error)),
            };
            let options = text(&args[4]);
            match crate::ts_headline::ts_headline(config, &text(&args[2]), &query, Some(&options))
            {
                Ok(headline) => Value::Text(headline),
                Err(error) => raise(error),
            }
        }
        // The XML well-formedness checks: over a document, or (the general
        // form, and `_content`) over content that a DOCTYPE makes a document.
        "XML_IS_WELL_FORMED" | "XML_IS_WELL_FORMED_DOCUMENT" | "XML_IS_WELL_FORMED_CONTENT"
            if arity(1) =>
        {
            let mode = if name == "XML_IS_WELL_FORMED_DOCUMENT" {
                crate::xml::Mode::Document
            } else {
                crate::xml::Mode::Content
            };
            Value::Bool(crate::xml::is_well_formed(&text(&args[0]), mode))
        }
        // `xmlcomment(text)`: the text as a comment.
        "XMLCOMMENT" if arity(1) => match crate::xml::comment(&text(&args[0])) {
            Ok(value) => Value::Text(value),
            Err(error) => raise(crate::xml::error_text(error)),
        },
        // `xmltext(text)`: the text with XML's special characters escaped.
        "XMLTEXT" if arity(1) => Value::Text(crate::xml::escape_special(&text(&args[0]))),
        // An XML value's output form, where text is built from it.
        crate::result_types::XML_OUT if arity(1) => {
            Value::Text(crate::xml::output(&text(&args[0])))
        }
        // `xmlpi(name target [, value])`.
        crate::result_types::XMLPI if arity(1) || arity(2) => {
            let target = text(&args[0]);
            let value = match args.get(1) {
                None => None,
                Some(Value::Null) => return Some(Value::Null),
                Some(value) => Some(text(value)),
            };
            let target = crate::xml::identifier_to_xml_name(&target, false);
            match crate::xml::pi(&target, value.as_deref()) {
                Ok(value) => Value::Text(value),
                Err(error) => raise(crate::xml::error_text(error)),
            }
        }
        // `xmlroot(value, version, standalone)`.
        crate::result_types::XMLROOT if arity(3) => {
            let version = match &args[1] {
                Value::Null => None,
                value => Some(text(value)),
            };
            let standalone = match &args[2] {
                Value::Int(n) => *n as i32,
                _ => 3,
            };
            Value::Text(crate::xml::root(&text(&args[0]), version.as_deref(), standalone))
        }
        // `xmlelement(name, attributes, content...)`.
        // The name, the attributes (a NULL when none), then the content.
        crate::result_types::XMLELEMENT if args.len() >= 2 => {
            let attrs = match &args[1] {
                Value::Null => None,
                value => Some(text(value)),
            };
            let content: Vec<String> = args[2..]
                .iter()
                .filter(|a| !matches!(a, Value::Null))
                .map(text)
                .collect();
            let name = crate::xml::identifier_to_xml_name(&text(&args[0]), false);
            Value::Text(crate::xml::element(&name, attrs.as_deref(), &content))
        }
        // `xmlattributes(value, name, fully-escaped, ...)`: the rendered
        // attributes, with PostgreSQL's checks on the names.
        crate::result_types::XMLATTRIBUTES if args.len() >= 3 => {
            let mut items = Vec::new();
            let mut i = 0;
            while i + 2 < args.len() + 1 && i < args.len() {
                let name = match &args[i + 1] {
                    Value::Null => {
                        return Some(raise(
                            crate::error_fields::DbError::new(
                                "unnamed XML attribute value must be a column reference",
                            )
                            .code("42601")
                            .into_text(),
                        ));
                    }
                    value => text(value),
                };
                let fully_escaped = matches!(args[i + 2], Value::Bool(true));
                let mapped = crate::xml::identifier_to_xml_name(&name, fully_escaped);
                if items
                    .iter()
                    .any(|(other, other_escaped, _): &(String, bool, Option<String>)| {
                        crate::xml::identifier_to_xml_name(other, *other_escaped) == mapped
                    })
                {
                    return Some(raise(
                        crate::error_fields::DbError::new(format!(
                            "XML attribute name \"{mapped}\" appears more than once"
                        ))
                        .code("42601")
                        .into_text(),
                    ));
                }
                let value = match &args[i] {
                    Value::Null => None,
                    value => Some(text(value)),
                };
                items.push((name, fully_escaped, value));
                i += 3;
            }
            Value::Text(crate::xml::attributes(&items))
        }
        // `xmlforest(value, name, fully-escaped, ...)`.
        crate::result_types::XMLFOREST if args.len() >= 3 => {
            let mut items = Vec::new();
            let mut i = 0;
            while i + 2 < args.len() + 1 && i < args.len() {
                let name = match &args[i + 1] {
                    Value::Null => {
                        return Some(raise(
                            crate::error_fields::DbError::new(
                                "unnamed XML element value must be a column reference",
                            )
                            .code("42601")
                            .into_text(),
                        ));
                    }
                    value => text(value),
                };
                let fully_escaped = matches!(args[i + 2], Value::Bool(true));
                let value = match &args[i] {
                    Value::Null => None,
                    value => Some(text(value)),
                };
                items.push((name, fully_escaped, value));
                i += 3;
            }
            match crate::xml::forest(&items) {
                Some(value) => Value::Text(value),
                None => Value::Null,
            }
        }
        // An element's content as SQL/XML maps a text value: escaped, with an
        // array becoming an `<element>` per member.
        crate::result_types::XML_ESCAPE if arity(1) => {
            fn escape_value(value: &Value) -> String {
                match value {
                    Value::Array(items) => items
                        .iter()
                        .filter(|v| !matches!(v, Value::Null))
                        .map(|v| format!("<element>{}</element>", escape_value(v)))
                        .collect(),
                    value => crate::xml::escape_xml(&crate::xml::value_text(value)),
                }
            }
            Value::Text(escape_value(&args[0]))
        }
        // An attribute value: the type's text, escaped for the quotes.
        crate::result_types::XML_ATTR_VALUE if arity(1) => {
            Value::Text(crate::xml::attribute_value(&args[0]))
        }
        // The serialized text fitted to the target type, as the implicit cast
        // XMLSERIALIZE applies does: too long a value is refused.
        crate::result_types::XML_SERIALIZE_TYPE if arity(2) => {
            let target = text(&args[1]);
            match crate::value::fit_character(&text(&args[0]), &target, false) {
                Ok(value) => Value::Text(value),
                Err(message) => raise(
                    crate::error_fields::DbError::new(message).code("22001").into_text(),
                ),
            }
        }
        // `xmlconcat(...)`: the XML declarations merged into one, the rest
        // concatenated; NULL arguments are dropped, and all of them leave NULL.
        "XMLCONCAT" => {
            let parts: Vec<String> = args
                .iter()
                .filter_map(|a| match a {
                    Value::Null => None,
                    value => Some(text(value)),
                })
                .collect();
            if parts.is_empty() {
                return Some(Value::Null);
            }
            let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
            Value::Text(crate::xml::concat(&parts))
        }
        // `xmlparse(document|content <value>)`, as the parser rewrites it: the
        // value is checked and kept as it is.
        crate::result_types::XML_PARSE if arity(2) => {
            let mode = if matches!(args[1], Value::Bool(true)) {
                crate::xml::Mode::Document
            } else {
                crate::xml::Mode::Content
            };
            let value = text(&args[0]);
            match crate::xml::validate_text(&value, mode) {
                Ok(()) => Value::Text(value),
                Err(error) => raise(error),
            }
        }
        // `xmlserialize(content|document <value> AS text [INDENT])`, as the
        // parser rewrites it; the cast to the target type follows.
        crate::result_types::XML_SERIALIZE if arity(3) => {
            let mode = if matches!(args[1], Value::Bool(true)) {
                crate::xml::Mode::Document
            } else {
                crate::xml::Mode::Content
            };
            let indent = matches!(args[2], Value::Bool(true));
            match crate::xml::serialize(&text(&args[0]), mode, indent) {
                Ok(value) => Value::Text(value),
                Err(error) => raise(crate::xml::error_text(error)),
            }
        }
        // `IS DOCUMENT`.
        crate::result_types::XML_IS_DOCUMENT if arity(1) => {
            Value::Bool(crate::xml::is_document(&text(&args[0])))
        }
        // The XML type errors the planner reports before execution.
        crate::result_types::XML_ERROR if arity(2) => raise(
            crate::error_fields::DbError::new(text(&args[0]))
                .code(&text(&args[1]))
                .into_text(),
        ),
        // `ts_lexize(dictionary, token)`: the dictionary's lexemes for one
        // token; an empty array for a stop word.
        "TS_LEXIZE" if arity(2) => {
            if args.iter().any(|a| matches!(a, Value::Null)) {
                return Some(Value::Null);
            }
            let dictionary = match dictionary_of(&args[0]) {
                Ok(dictionary) => dictionary,
                Err(error) => return Some(raise(error)),
            };
            Value::Array(
                crate::ts_dict::lexize_token(dictionary, &text(&args[1]))
                    .into_iter()
                    .map(Value::Text)
                    .collect(),
            )
        }
        // `ts_rewrite(query, target, substitute)`: every occurrence of the
        // target replaced by the substitute.
        "TS_REWRITE" if arity(3) => {
            if args.iter().any(|a| matches!(a, Value::Null)) {
                return Some(Value::Null);
            }
            let (Ok(query), Ok(target), Ok(substitute)) = (
                crate::textsearch::parse_tsquery(&text(&args[0])),
                crate::textsearch::parse_tsquery(&text(&args[1])),
                crate::textsearch::parse_tsquery(&text(&args[2])),
            ) else {
                return Some(raise("invalid input syntax for type tsquery"));
            };
            Value::Text(crate::textsearch::print_tsquery(
                &crate::textsearch::query_rewrite(&query, &target, &substitute),
            ))
        }
        // `get_current_ts_config()`: the session's text search configuration.
        "GET_CURRENT_TS_CONFIG" if arity(0) => Value::Int(default_ts_config().oid()),
        // The catalog visibility tests of the shipped text search objects:
        // true for one of them, NULL for anything else.
        "PG_TS_CONFIG_IS_VISIBLE" | "PG_TS_DICT_IS_VISIBLE" | "PG_TS_PARSER_IS_VISIBLE"
        | "PG_TS_TEMPLATE_IS_VISIBLE"
            if arity(1) =>
        {
            let known = match name {
                "PG_TS_CONFIG_IS_VISIBLE" => crate::ts_dict::config_by_oid(int(arg(0))?).is_some(),
                "PG_TS_DICT_IS_VISIBLE" => {
                    crate::ts_dict::dictionary_by_oid(int(arg(0))?).is_some()
                }
                "PG_TS_PARSER_IS_VISIBLE" => int(arg(0))? == crate::ts_parse::PARSER_OID,
                _ => matches!(int(arg(0))?, 3727 | 13268),
            };
            if known {
                Value::Bool(true)
            } else {
                Value::Null
            }
        }
        // A call whose arguments fit more than one overload.
        "__FUNC_NOT_UNIQUE__" if args.len() >= 2 => raise(
            crate::error_fields::DbError::new(format!(
                "function {}({}) is not unique",
                text(arg(0)),
                args[1..]
                    .iter()
                    .map(crate::render)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
            .code("42725")
            .hint("Could not choose a best candidate function. You might need to add explicit type casts.")
            .into_text(),
        ),
        "__BAD_FUNCTION__" if args.len() >= 2 => raise(
            crate::error_fields::DbError::new(format!(
                "function {}({}) does not exist",
                text(arg(0)),
                args[1..]
                    .iter()
                    .map(crate::render)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
            .code("42883")
            .hint("No function matches the given name and argument types. You might need to add explicit type casts.")
            .into_text(),
        ),
        "__BAD_OPERATOR__" if arity(3) => {
            // A unary operator reads as `op type`, not `type op`; its hint
            // names one argument type, as PostgreSQL's does.
            let (left, op, right) = (text(arg(0)), text(arg(1)), text(arg(2)));
            let unary = left.is_empty() || right.is_empty();
            let shown = if left.is_empty() {
                format!("{op} {right}")
            } else if right.is_empty() {
                format!("{left} {op}")
            } else {
                format!("{left} {op} {right}")
            };
            raise(
                crate::error_fields::DbError::new(format!("operator does not exist: {shown}"))
                    .code("42883")
                    .hint(if unary {
                        "No operator matches the given name and argument type. You might need to add an explicit type cast."
                    } else {
                        "No operator matches the given name and argument types. You might need to add explicit type casts."
                    })
                    .into_text(),
            )
        }
        "__BAD_COMPARISON__" if arity(1) => raise(
            crate::error_fields::DbError::new(format!(
                "could not identify a comparison function for type {}",
                text(arg(0))
            ))
            .code("42883")
            .into_text(),
        ),
        // `ORDER BY` of a value with no ordering operator.
        crate::result_types::BAD_ORDERING if arity(1) => raise(
            crate::error_fields::DbError::new(format!(
                "could not identify an ordering operator for type {}",
                text(arg(0))
            ))
            .code("42883")
            .hint("Use an explicit ordering operator or modify the query.")
            .into_text(),
        ),
        "__BAD_RANGE_CAST__" if arity(2) => raise(
            crate::error_fields::DbError::new(format!(
                "cannot cast type {} to {}",
                text(arg(0)),
                text(arg(1))
            ))
            .code("42846")
            .into_text(),
        ),
        "HOST" | "NETMASK" | "HOSTMASK" | "BROADCAST" | "NETWORK" | "MASKLEN" | "SET_MASKLEN"
        | "ABBREV" | "FAMILY"
            if arity(1) || arity(2) =>
        {
            let kind = if arity(2) {
                crate::net::Kind::of(&text(arg(1))).unwrap_or(crate::net::Kind::Inet)
            } else {
                crate::net::Kind::Inet
            };
            match crate::net::inet_function(kind, name, &text(arg(0)), None) {
                Some(result) => result.unwrap_or_else(raise),
                None => raise(format!("function {} does not exist", name.to_ascii_lowercase())),
            }
        }
        "SET_MASKLEN" | "INET_SAME_FAMILY" | "INET_MERGE" if arity(2) || arity(3) => {
            let kind = if arity(3) {
                crate::net::Kind::of(&text(arg(2))).unwrap_or(crate::net::Kind::Inet)
            } else {
                crate::net::Kind::Inet
            };
            match crate::net::inet_function(kind, name, &text(arg(0)), Some(&text(arg(1)))) {
                Some(result) => result.unwrap_or_else(raise),
                None => raise(format!("function {} does not exist", name.to_ascii_lowercase())),
            }
        }
        // `text(x)` is any value's text.
        "TEXT" if arity(1) => arg(0).clone(),
        "ISEMPTY" if arity(1) => {
            if crate::multiranges::is_multirange_text(&text(arg(0))) {
                crate::multiranges::is_empty(&text(arg(0))).unwrap_or_else(raise)
            } else {
                crate::ranges::is_empty(&text(arg(0))).unwrap_or_else(raise)
            }
        }
        "LOWER_INC" | "UPPER_INC" if arity(1) => {
            if crate::multiranges::is_multirange_text(&text(arg(0))) {
                crate::multiranges::bound_inc(&text(arg(0)), name == "LOWER_INC")
                    .unwrap_or_else(raise)
            } else {
                crate::ranges::bound_inc(&text(arg(0)), name == "LOWER_INC").unwrap_or_else(raise)
            }
        }
        "LOWER_INF" | "UPPER_INF" if arity(1) => {
            if crate::multiranges::is_multirange_text(&text(arg(0))) {
                crate::multiranges::bound_inf(&text(arg(0)), name == "LOWER_INF")
                    .unwrap_or_else(raise)
            } else {
                crate::ranges::bound_inf(&text(arg(0)), name == "LOWER_INF").unwrap_or_else(raise)
            }
        }
        // Geometric operators, casts, and functions, with their types
        // appended by the planner.
        "__GEO__" if arity(5) => {
            let Some(kind) = crate::geometric::Kind::of(&text(arg(3))) else {
                return Some(raise(format!(
                    "operator does not exist: {} {}",
                    crate::value::value_type_name(arg(1)),
                    text(arg(0))
                )));
            };
            let right_kind = if text(arg(4)).is_empty() {
                None
            } else {
                crate::geometric::Kind::of(&text(arg(4)))
            };
            crate::geometric::operator(
                &text(arg(0)),
                kind,
                &text(arg(1)),
                &text(arg(2)),
                right_kind,
            )
            .unwrap_or_else(raise)
        }
        "__GEO_CAST__" if arity(3) => {
            let (from, to) = (
                crate::geometric::Kind::of(&text(arg(0))),
                crate::geometric::Kind::of(&text(arg(1))),
            );
            match (from, to) {
                (Some(from), Some(to)) => {
                    crate::geometric::cast_between(from, to, &text(arg(2))).unwrap_or_else(raise)
                }
                _ => raise(format!(
                    "cannot cast type {} to {}",
                    text(arg(0)),
                    text(arg(1))
                )),
            }
        }
        "__GEO_UNARY__" if arity(3) => {
            let Some(kind) = crate::geometric::Kind::of(&text(arg(2))) else {
                return Some(raise(format!(
                    "operator does not exist: {} {}",
                    text(arg(0)),
                    crate::value::value_type_name(arg(1))
                )));
            };
            crate::geometric::unary(&text(arg(0)), kind, &text(arg(1))).unwrap_or_else(raise)
        }
        "__GEO_FN__" if args.len() >= 4 => {
            let name = text(arg(0)).to_ascii_uppercase();
            let declared = if text(arg(2)).is_empty() {
                text(arg(1))
            } else {
                text(arg(2))
            };
            let Some(kind) = crate::geometric::Kind::of(&declared) else {
                return Some(raise(format!("unsupported geometric type {declared}")));
            };
            match crate::geometric::call(&name, kind, &args[3..]) {
                Some(result) => result.unwrap_or_else(raise),
                None => raise(format!("function {} does not exist", name.to_ascii_lowercase())),
            }
        }
        // `greatest`/`least` of a range family, with its type appended by
        // the planner: NULLs are skipped, as PostgreSQL skips them.
        "__RANGE_GREATEST__" if args.len() >= 3 => {
            let greatest = text(arg(0)) == "greatest";
            let kind_name = text(arg(1));
            let multirange = crate::multiranges::kind_of(&kind_name).is_some();
            let kind = crate::ranges::Kind::of(&kind_name)
                .or_else(|| crate::multiranges::kind_of(&kind_name))
                .unwrap_or(crate::ranges::Kind::Int4);
            let compare = |a: &Value, b: &Value| -> std::cmp::Ordering {
                let (Value::Text(a), Value::Text(b)) = (a, b) else {
                    return std::cmp::Ordering::Equal;
                };
                if multirange {
                    let (Ok(a), Ok(b)) =
                        (crate::multiranges::parse(a), crate::multiranges::parse(b))
                    else {
                        return std::cmp::Ordering::Equal;
                    };
                    crate::multiranges::cmp(kind, &a, &b)
                } else {
                    let (Ok(a), Ok(b)) = (crate::ranges::parse(a), crate::ranges::parse(b)) else {
                        return std::cmp::Ordering::Equal;
                    };
                    crate::ranges::cmp_ranges(kind, &a, &b)
                }
            };
            let mut best: Option<&Value> = None;
            for value in &args[2..] {
                if *value == Value::Null {
                    continue;
                }
                best = Some(match best {
                    None => value,
                    Some(best) => {
                        let ord = compare(value, best);
                        let take = if greatest {
                            ord == std::cmp::Ordering::Greater
                        } else {
                            ord == std::cmp::Ordering::Less
                        };
                        if take { value } else { best }
                    }
                });
            }
            best.cloned().unwrap_or(Value::Null)
        }
        // `range_merge` of one multirange spans its elements; PostgreSQL
        // has no one-range form.
        "RANGE_MERGE" if arity(1) => {
            let text = text(arg(0));
            if crate::multiranges::is_multirange_text(&text) {
                crate::multiranges::range_merge(&text).unwrap_or_else(raise)
            } else {
                raise(format!(
                    "function range_merge({}) does not exist",
                    crate::value::value_type_name(arg(0))
                ))
            }
        }
        "RANGE_MERGE" if arity(2) => {
            let kind = crate::ranges::infer_kind(&text(arg(0)), &text(arg(1)));
            match kind {
                Some(kind) => {
                    crate::ranges::range_merge(kind, &text(arg(0)), &text(arg(1)))
                        .unwrap_or_else(raise)
                }
                None => raise(format!(
                    "function range_merge({}, {}) does not exist",
                    crate::value::value_type_name(arg(0)),
                    crate::value::value_type_name(arg(1))
                )),
            }
        }
        // The multirange constructors, with their subtype appended by the
        // planner: `int4multirange(range...)`, `multirange(range)`.
        "__MULTIRANGE_BUILD__" if !args.is_empty() => {
            let kind = crate::ranges::Kind::of(&text(arg(0)))
                .or_else(|| crate::multiranges::kind_of(&text(arg(0))))
                .unwrap_or(crate::ranges::Kind::Int4);
            // No member is the empty multirange; one NULL is NULL; a NULL
            // among several is refused.
            if args.len() == 1 {
                Value::Text("{}".to_string())
            } else if args[1..].iter().all(|v| *v == Value::Null) {
                Value::Null
            } else if args[1..].iter().any(|v| *v == Value::Null) {
                raise(
                    crate::error_fields::DbError::new(
                        "multirange values cannot contain null members",
                    )
                    .code("22004")
                    .into_text(),
                )
            } else {
                let mut elements = Vec::new();
                for value in &args[1..] {
                    let value = text(value);
                    if crate::multiranges::is_multirange_text(&value) {
                        // A multirange copies (and merges) as it is.
                        match crate::multiranges::parse(&value) {
                            Ok(ranges) => elements.extend(ranges),
                            Err(error) => return Some(raise(error)),
                        }
                        continue;
                    }
                    match crate::ranges::from_literal(kind, &value)
                        .and_then(|canonical| crate::ranges::parse(&canonical))
                    {
                        Ok(crate::ranges::Range::Empty) => {}
                        Ok(range) => elements.push(range),
                        Err(error) => return Some(raise(error)),
                    }
                }
                Value::Text(crate::multiranges::format(
                    &crate::multiranges::merge(kind, elements),
                ))
            }
        }
        // The range constructors: `int4range(lower, upper [, flags])`, and
        // the copy constructor `int4range(range)`.
        "INT4RANGE" | "INT8RANGE" | "NUMRANGE" | "DATERANGE" | "TSRANGE" | "TSTZRANGE"
            if matches!(args.len(), 1 | 2 | 3) =>
        {
            let kind = crate::ranges::Kind::of(name).expect("matched by name");
            if let [value] = args {
                match value {
                    Value::Text(text) => crate::ranges::from_literal(kind, text)
                        .map(Value::Text)
                        .unwrap_or_else(raise),
                    other => raise(format!(
                        "cannot cast type {} to {}",
                        crate::value::value_type_name(other),
                        kind.name()
                    )),
                }
            } else {
                let flags = if args.len() == 3 {
                    text(arg(2))
                } else {
                    "[)".to_string()
                };
                crate::ranges::from_bounds(kind, Some(arg(0)), Some(arg(1)), &flags)
                    .map(Value::Text)
                    .unwrap_or_else(raise)
            }
        }
        // A grouped query resolves `GROUPING(...)` per grouping set first.
        "GROUPING" => raise(
            "arguments to GROUPING must be grouping expressions of the associated query level",
        ),
        // `BETWEEN SYMMETRIC`'s bounds: the lesser and the greater.
        "__SYMMETRIC_LOW__" | "__SYMMETRIC_HIGH__" if arity(2) => {
            let less = crate::compare(arg(0), arg(1)) != std::cmp::Ordering::Greater;
            if (name == "__SYMMETRIC_LOW__") == less {
                arg(0).clone()
            } else {
                arg(1).clone()
            }
        }
        "OVERLAPS" if arity(4) => crate::datetime::overlaps(args, &[]).unwrap_or_else(raise),
        // With the arguments' static types, as the planner passes them.
        "OVERLAPS" if arity(8) => {
            let kinds: Vec<_> = args[4..]
                .iter()
                .map(|t| crate::datetime::Kind::of_type(&text(t)))
                .collect();
            crate::datetime::overlaps(&args[..4], &kinds).unwrap_or_else(raise)
        }
        "DATE_TRUNC" if arity(2) => {
            crate::datetime::trunc(&text(arg(0)), arg(1), None).unwrap_or_else(raise)
        }
        // Truncates the local time in the zone, then reads it back there.
        "DATE_TRUNC" if arity(3) => crate::timezone::at_time_zone(arg(2), arg(1), None, None)
            .and_then(|local| {
                crate::datetime::trunc(
                    &text(arg(0)),
                    &local,
                    Some(crate::datetime::Kind::Timestamp),
                )
            })
            .and_then(|truncated| crate::timezone::at_time_zone(arg(2), &truncated, None, None))
            .unwrap_or_else(raise),
        "AGE" if arity(2) => crate::datetime::age(arg(0), arg(1)).unwrap_or_else(raise),
        "AGE" if arity(1) => {
            let today = text(&timestamp(session_time(|e| e.transaction_micros)?, false));
            let midnight = Value::Text(format!("{} 00:00:00", &today[..10]));
            crate::datetime::age(&midnight, arg(0)).unwrap_or_else(raise)
        }
        "DATE_PART" if arity(2) => {
            crate::datetime::extract(&text(arg(0)), arg(1), None, true).unwrap_or_else(raise)
        }
        // `__DATETIME__(op, left, right, left_type, right_type)`: a date/time
        // operator, by its operands' declared types.
        "__DATETIME__" if arity(5) => {
            let kind = |v: &Value| crate::datetime::Kind::of_type(&text(v));
            crate::datetime::arith(&text(arg(0)), arg(1), kind(arg(3)), arg(2), kind(arg(4)))
                .unwrap_or_else(raise)
        }
        "TO_CHAR" if arity(2) => {
            crate::datetime_format::to_char(arg(0), &text(arg(1)), None).unwrap_or_else(raise)
        }
        "TO_DATE" if arity(2) => {
            crate::datetime_format::parse_by_template(&text(arg(0)), &text(arg(1)), true)
                .unwrap_or_else(raise)
        }
        "TO_TIMESTAMP" if arity(2) => {
            crate::datetime_format::parse_by_template(&text(arg(0)), &text(arg(1)), false)
                .unwrap_or_else(raise)
        }
        "TO_NUMBER" if arity(2) => {
            crate::datetime_format::parse_number(&text(arg(0)), &text(arg(1))).unwrap_or_else(raise)
        }
        // `make_interval(years, months, weeks, days, hours, mins, secs)`.
        "MAKE_INTERVAL" if args.len() <= 7 => {
            let part = |i: usize| args.get(i).map_or(Some(0.0), num);
            let (years, months, weeks, days) = (part(0)?, part(1)?, part(2)?, part(3)?);
            let (hours, minutes, seconds) = (part(4)?, part(5)?, part(6)?);
            let interval = crate::datetime::Interval::new(
                (years * 12.0 + months) as i64,
                (weeks * 7.0 + days) as i64,
                (hours * 3_600e6 + minutes * 60e6 + seconds * 1e6).round() as i64,
            );
            Value::Text(interval.format())
        }
        "MAKE_TIME" if arity(3) => {
            crate::datetime::make_time(int(arg(0))?, int(arg(1))?, num(arg(2))?)
                .unwrap_or_else(raise)
        }
        "MAKE_TIMESTAMPTZ" if arity(6) || arity(7) => {
            let (y, mo, d, h, mi) = (
                int(arg(0))?,
                int(arg(1))?,
                int(arg(2))?,
                int(arg(3))?,
                int(arg(4))?,
            );
            let secs = num(arg(5))?;
            let local =
                chrono::NaiveDate::from_ymd_opt(y as i32, mo as u32, d as u32).and_then(|date| {
                    date.and_hms_micro_opt(
                        h as u32,
                        mi as u32,
                        secs.trunc() as u32,
                        (secs.fract() * 1e6).round() as u32,
                    )
                });
            let Some(local) = local else {
                return Some(raise("date/time field value out of range"));
            };
            let zone = match args.get(6) {
                Some(zone) => match crate::timezone::Zone::resolve(&text(zone)) {
                    Ok(zone) => zone,
                    Err(e) => return Some(raise(e)),
                },
                None => {
                    return Some(
                        crate::datetime::Temporal::TimestampTz(
                            crate::timezone::from_session_local(local),
                        )
                        .to_value(),
                    );
                }
            };
            let utc = local - chrono::Duration::seconds(zone.offset_at_local(local));
            crate::datetime::Temporal::TimestampTz(utc).to_value()
        }
        "JUSTIFY_DAYS" | "JUSTIFY_HOURS" | "JUSTIFY_INTERVAL" if arity(1) => {
            match crate::datetime::Interval::parse(&text(arg(0))) {
                Some(interval) => Value::Text(
                    match name {
                        "JUSTIFY_DAYS" => interval.justify_days(),
                        "JUSTIFY_HOURS" => interval.justify_hours(),
                        _ => interval.justify(),
                    }
                    .format(),
                ),
                None => raise(format!(
                    "invalid input syntax for type interval: \"{}\"",
                    text(arg(0))
                )),
            }
        }
        "ISFINITE" if arity(1) => Value::Bool(crate::datetime::is_finite(arg(0))?),
        "DATE_BIN" if arity(3) => {
            crate::datetime::date_bin(arg(0), arg(1), arg(2)).unwrap_or_else(raise)
        }
        "TIMEZONE" if arity(2) => {
            crate::timezone::at_time_zone(arg(0), arg(1), None, None).unwrap_or_else(raise)
        }
        // With the arguments' static types, as the planner passes them.
        "TIMEZONE" if arity(4) => {
            let (zone_type, value_type) = (text(arg(2)), text(arg(3)));
            crate::timezone::at_time_zone(
                arg(0),
                arg(1),
                Some(zone_type.as_str()).filter(|t| !t.is_empty()),
                Some(value_type.as_str()).filter(|t| !t.is_empty()),
            )
            .unwrap_or_else(raise)
        }
        "MAKE_DATE" if arity(3) => {
            let (y, m, d) = (int(arg(0))?, int(arg(1))?, int(arg(2))?);
            match chrono::NaiveDate::from_ymd_opt(y as i32, m as u32, d as u32) {
                Some(date) => Value::Text(date.format("%Y-%m-%d").to_string()),
                None => raise(format!("date field value out of range: {y}-{m:02}-{d:02}")),
            }
        }
        "MAKE_TIMESTAMP" if arity(6) => {
            let (y, mo, d, h, mi) = (
                int(arg(0))?,
                int(arg(1))?,
                int(arg(2))?,
                int(arg(3))?,
                int(arg(4))?,
            );
            let secs = num(arg(5))?;
            let micros = (secs.fract() * 1_000_000.0).round() as u32;
            match chrono::NaiveDate::from_ymd_opt(y as i32, mo as u32, d as u32).and_then(|date| {
                date.and_hms_micro_opt(h as u32, mi as u32, secs.trunc() as u32, micros)
            }) {
                Some(ts) => Value::Text(crate::value::format_timestamp(ts, false)),
                None => raise("date/time field value out of range"),
            }
        }
        "TO_TIMESTAMP" if arity(1) => {
            let secs = num(arg(0))?;
            if secs.is_nan() {
                raise("timestamp cannot be NaN")
            } else if secs.is_infinite() {
                crate::datetime::Temporal::Infinite(crate::datetime::Kind::TimestampTz, secs < 0.0)
                    .to_value()
            } else {
                timestamp((secs * 1_000_000.0).round() as i64, true)
            }
        }

        // ---- Session and system ---------------------------------------------------
        "VERSION" if arity(0) => Value::Text(format!(
            "PostgreSQL {} (NodusDB)",
            session_env::setting("server_version").unwrap_or_default()
        )),
        "CURRENT_USER" | "SESSION_USER" | "CURRENT_ROLE" | "USER" if arity(0) => {
            match session_env::with(|e| e.map(|e| e.user.clone())) {
                Some(user) => Value::Text(user),
                None => session_unavailable(name),
            }
        }
        "CURRENT_DATABASE" | "CURRENT_CATALOG" if arity(0) => Value::Text("default".to_string()),
        // The schemas of the search path that exist; the temporary one and
        // `pg_catalog` are implicit.
        "CURRENT_SCHEMA" if arity(0) => crate::search_path::existing_search_path()
            .into_iter()
            .find(|s| !crate::search_path::is_temp_schema(s) && s != "pg_catalog")
            .map_or(Value::Null, Value::Text),
        "CURRENT_SCHEMAS" if arity(1) => {
            let mut schemas: Vec<String> = crate::search_path::existing_search_path()
                .into_iter()
                .filter(|s| !crate::search_path::is_temp_schema(s) && s != "pg_catalog")
                .collect();
            if matches!(arg(0), Value::Bool(true)) {
                schemas.insert(0, "pg_catalog".to_string());
            }
            Value::Array(schemas.into_iter().map(Value::Text).collect())
        }
        "CURRENT_SETTING" if arity(1) || arity(2) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let name = text(arg(0));
            match session_env::setting(&name) {
                Some(value) => Value::Text(value),
                None if matches!(arg(1), Value::Bool(true)) => Value::Null,
                None => raise(format!("unrecognized configuration parameter \"{name}\"")),
            }
        }
        "PG_BACKEND_PID" if arity(0) => match session_env::with(|e| e.map(|e| e.backend_pid)) {
            Some(pid) => Value::Int(pid),
            None => session_unavailable(name),
        },
        "TXID_CURRENT" | "PG_CURRENT_XACT_ID" if arity(0) => {
            Value::Int(session_time(|e| e.transaction_micros)?)
        }
        "PG_TYPEOF" if arity(1) => Value::Text(
            match arg(0) {
                Value::Int(i) if i32::try_from(*i).is_ok() => "integer",
                Value::Int(_) => "bigint",
                Value::Float(_) => "double precision",
                Value::Numeric(_) => "numeric",
                Value::Text(_) => "text",
                Value::Bool(_) => "boolean",
                Value::Jsonb(_) => "jsonb",
                Value::Json(_) => "json",
                Value::Record(_) => "record",
                Value::Bytea(_) => "bytea",
                Value::Array(_) => "text[]",
                Value::Null => "unknown",
            }
            .to_string(),
        ),
        "PG_SIZE_PRETTY" if arity(1) => Value::Text(size_pretty(int(arg(0))?)),
        // A relation's forks other than its main one are empty.
        "PG_RELATION_SIZE" if arity(2) && text(arg(1)) != "main" => relation_size(arg(0), |_, _| 0),
        "PG_RELATION_SIZE" | "PG_TABLE_SIZE" if arity(1) || arity(2) => {
            relation_size(arg(0), |table, _| table)
        }
        "PG_INDEXES_SIZE" if arity(1) => relation_size(arg(0), |_, indexes| indexes),
        "PG_TOTAL_RELATION_SIZE" if arity(1) => {
            relation_size(arg(0), |table, indexes| table + indexes)
        }
        "PG_DATABASE_SIZE" if arity(1) => session_env::with(|env| {
            let env = env?;
            let (catalog, (kv, read_ts)) = (env.catalog.as_ref()?, env.storage.as_ref()?);
            let tables = catalog.list_all_tables("default").ok()?;
            Some(Value::Int(
                tables
                    .iter()
                    .map(|t| {
                        let (table, indexes) = stored_size(kv.as_ref(), *read_ts, t);
                        table + indexes
                    })
                    .sum(),
            ))
        })?,
        "PG_ENCODING_TO_CHAR" if arity(1) => {
            Value::Text(if int(arg(0))? == 6 { "UTF8" } else { "" }.to_string())
        }
        "PG_CLIENT_ENCODING" if arity(0) => Value::Text("UTF8".to_string()),
        "PG_IS_IN_RECOVERY" if arity(0) => Value::Bool(false),
        // ---- Sequences ---------------------------------------------------------
        // `__IDENTITY__(sequence, always)` is an identity column's default.
        "NEXTVAL" | crate::sequences::SERIAL if arity(1) => {
            sequence_op(|store, session| store.nextval(session, &text(arg(0))))
        }
        "__IDENTITY__" if arity(2) => {
            sequence_op(|store, session| store.nextval(session, &text(arg(0))))
        }
        "CURRVAL" if arity(1) => {
            sequence_op(|store, session| store.currval(session, &text(arg(0))))
        }
        "LASTVAL" if arity(0) => sequence_op(|store, session| store.lastval(session)),
        "SETVAL" if arity(2) || arity(3) => {
            let value = int(arg(1))?;
            let is_called = !matches!(arg(2), Value::Bool(false));
            sequence_op(|store, session| store.setval(session, &text(arg(0)), value, is_called))
        }
        "PG_GET_SERIAL_SEQUENCE" if arity(2) => {
            let table = text(arg(0));
            let column = text(arg(1)).to_ascii_lowercase();
            match session_env::with(|e| e.and_then(|e| e.sequences.clone())) {
                Some(store) => match store.owned_sequence(&table, &column) {
                    Ok(Some(name)) => Value::Text(name),
                    Ok(None) => Value::Null,
                    Err(e) => raise(e.to_string()),
                },
                None => session_unavailable(name),
            }
        }
        // Sleeps no longer than the statement may still run.
        "PG_SLEEP" if arity(1) => {
            let seconds = num(arg(0))?.min(1e9);
            let limit = session_env::setting("statement_timeout")
                .and_then(|t| crate::session_vars::duration_millis(&t))
                .filter(|ms| *ms > 0);
            let started = session_time(|e| e.statement_micros)?;
            let remaining = limit.map(|ms| {
                (ms as i64 * 1_000 - (session_env::wall_micros() - started)).max(0) as f64 / 1e6
            });
            match remaining {
                Some(remaining) if remaining < seconds => {
                    std::thread::sleep(std::time::Duration::from_secs_f64(remaining));
                    raise("canceling statement due to statement timeout")
                }
                _ => {
                    if seconds > 0.0 {
                        std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
                    }
                    crate::session_functions::void()
                }
            }
        }
        // `set_config(name, value, is_local)`: as `SET [LOCAL]`, returning
        // the value the setting takes.
        "SET_CONFIG" if arity(3) => {
            let name = text(arg(0));
            let action = crate::session_vars::set_action(&name, &text(arg(1)));
            match action {
                Ok(crate::session_vars::SetAction::Set(value)) => {
                    session_env::stage_setting(
                        &name,
                        Some(value.clone()),
                        matches!(arg(2), Value::Bool(true)),
                    );
                    Value::Text(value)
                }
                Ok(crate::session_vars::SetAction::Reset) => {
                    session_env::stage_setting(&name, None, matches!(arg(2), Value::Bool(true)));
                    session_env::setting(&name).map_or(Value::Null, Value::Text)
                }
                Err(e) => raise(e),
            }
        }
        // The catalog exposes one role, the bootstrap superuser (OID 10).
        "PG_GET_USERBYID" if arity(1) => Value::Text(match int(arg(0))? {
            10 => "nodus".to_string(),
            oid => format!("unknown (OID={oid})"),
        }),
        // The port the server listens on (its `port` setting).
        "INET_SERVER_PORT" if arity(0) => session_env::setting("port")
            .and_then(|p| p.parse().ok())
            .map_or(Value::Null, Value::Int),
        // Connections are not described to the executor; PostgreSQL reports
        // NULL the same way for a Unix-socket connection.
        "INET_SERVER_ADDR" | "INET_CLIENT_ADDR" | "INET_CLIENT_PORT" if arity(0) => Value::Null,
        // NodusDB has no COMMENT ON, so no object has a description.
        // Only relations and their columns take comments.
        "OBJ_DESCRIPTION" if arity(1) || arity(2) => {
            if arity(2) && text(arg(1)) != "pg_class" {
                return Some(
                    crate::schemas::schema_or_type_description(int(arg(0))?, &text(arg(1)))
                        .map_or(Value::Null, Value::Text),
                );
            }
            catalog_text(|catalog| {
                crate::MemExecutor::relation_by_oid(catalog, int(arg(0))?)?.comment
            })
        }
        // Column 0 is the relation itself.
        "COL_DESCRIPTION" if arity(2) => catalog_text(|catalog| {
            let relation = crate::MemExecutor::relation_by_oid(catalog, int(arg(0))?)?;
            match usize::try_from(int(arg(1))?).ok()? {
                0 => relation.comment,
                n => relation.columns.get(n - 1)?.comment.clone(),
            }
        }),
        "SHOBJ_DESCRIPTION" if arity(2) => Value::Null,
        "FORMAT_TYPE" if arity(2) => match arg(0) {
            Value::Null => Value::Null,
            oid => Value::Text(format_type(int(oid)?, int(arg(1)).filter(|m| *m >= 0))),
        },
        // Expressions are stored as their SQL text; pretty-printed, without
        // the parentheses around the whole of it.
        "PG_GET_EXPR" if arity(2) || arity(3) => {
            let expr = text(arg(0));
            match (
                arity(3) && matches!(arg(2), Value::Bool(true)),
                unwrap_parens(&expr),
            ) {
                (true, Some(inner)) => Value::Text(inner.to_string()),
                _ => Value::Text(expr),
            }
        }
        "PG_GET_INDEXDEF" if arity(1) || arity(3) => {
            let column = if arity(3) { int(arg(1))? } else { 0 };
            let pretty = arity(3) && matches!(arg(2), Value::Bool(true));
            catalog_text(|catalog| {
                crate::MemExecutor::index_definition(catalog, int(arg(0))?, column, pretty)
            })
        }
        // By OID or by (possibly qualified) name.
        "PG_GET_VIEWDEF" if arity(1) || arity(2) => catalog_text(|catalog| {
            let oid = match arg(0) {
                Value::Text(name) if name.trim().parse::<i64>().is_err() => {
                    crate::MemExecutor::relation_oid(catalog, name)?
                }
                other => int(other)?,
            };
            crate::MemExecutor::view_definition(catalog, oid)
        }),
        "__OBJECT_NAME__" if arity(2) => {
            let oid = int(arg(0))?;
            let kind = text(arg(1));
            match catalog_text(|catalog| crate::MemExecutor::object_name(catalog, &kind, oid)) {
                Value::Null => Value::Text(oid.to_string()),
                name => name,
            }
        }
        "PG_GET_CONSTRAINTDEF" if arity(1) || arity(2) => {
            let pretty = arity(2) && matches!(arg(1), Value::Bool(true));
            catalog_text(|catalog| {
                crate::MemExecutor::constraint_definition(catalog, int(arg(0))?, pretty)
            })
        }
        // Any table could be published (there are no publications).
        "PG_RELATION_IS_PUBLISHABLE" if arity(1) => Value::Bool(true),
        // There are no extended statistics objects.
        "PG_GET_STATISTICSOBJDEF_COLUMNS" if arity(1) => Value::Null,
        name if crate::value::is_visibility_fn(name) && arity(1) => Value::Bool(true),

        // ---- UUIDs ------------------------------------------------------------------
        "GEN_RANDOM_UUID" | "UUIDV4" if arity(0) => Value::Text(uuid::Uuid::new_v4().to_string()),
        "UUIDV7" if arity(0) => Value::Text(uuid_v7(session_env::wall_micros() / 1000).to_string()),
        "UUID_EXTRACT_VERSION" if arity(1) => match uuid::Uuid::parse_str(text(arg(0)).trim()) {
            Ok(u) if u.get_variant() == uuid::Variant::RFC4122 => {
                Value::Int(u.get_version_num() as i64)
            }
            Ok(_) => Value::Null,
            Err(_) => raise(format!(
                "invalid input syntax for type uuid: \"{}\"",
                text(arg(0))
            )),
        },

        // ---- JSON ---------------------------------------------------------------------
        "TO_JSON" if arity(1) => match arg(0) {
            Value::Null => Value::Null,
            v => Value::Json(json_of(v, false)),
        },
        "TO_JSONB" if arity(1) => match arg(0) {
            Value::Null => Value::Null,
            v => Value::Jsonb(to_json(v)),
        },
        "ROW_TO_JSON" if arity(1) || arity(2) => match arg(0) {
            v @ Value::Record(_) => Value::Json(json_of(v, pretty(args)?)),
            other => raise(format!(
                "function row_to_json({}) does not exist",
                crate::value::value_type_name(other)
            )),
        },
        "ARRAY_TO_JSON" if arity(1) || arity(2) => match arg(0) {
            v @ Value::Array(_) => Value::Json(json_of(v, pretty(args)?)),
            other => raise(format!(
                "function array_to_json({}) does not exist",
                crate::value::value_type_name(other)
            )),
        },
        // `__RECORD__(name, value, ...)`: a row with those fields.
        "__RECORD__" => Value::Record(
            args.chunks(2)
                .map(|pair| (text(&pair[0]), pair.get(1).cloned().unwrap_or(Value::Null)))
                .collect(),
        ),
        "JSON_BUILD_OBJECT" => {
            if args.len() % 2 != 0 {
                return Some(raise("argument list must have even number of elements"));
            }
            let mut out = String::from("{");
            for (i, pair) in args.chunks(2).enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                if let Err(e) = crate::json_text::key_json(&pair[0], &mut out) {
                    return Some(raise(e));
                }
                out.push_str(" : ");
                crate::json_text::value_json(&pair[1], false, &mut out);
            }
            out.push('}');
            Value::Json(out)
        }
        "JSON_BUILD_ARRAY" => {
            let items: Vec<String> = args.iter().map(|v| json_of(v, false)).collect();
            Value::Json(format!("[{}]", items.join(", ")))
        }
        // `json_object(text[])`, alternating keys and values or as pairs, or
        // `json_object(keys text[], values text[])`.
        "JSON_OBJECT" if arity(1) || arity(2) => {
            let pairs: Vec<(Value, Value)> = if arity(2) {
                let (keys, values) = (array(arg(0))?, array(arg(1))?);
                if keys.len() != values.len() {
                    return Some(raise("mismatched array dimensions"));
                }
                keys.into_iter().zip(values).collect()
            } else {
                let items = array(arg(0))?;
                if items
                    .iter()
                    .all(|i| matches!(i, Value::Array(pair) if pair.len() == 2))
                {
                    items
                        .into_iter()
                        .map(|i| match i {
                            Value::Array(mut pair) => {
                                let value = pair.pop().unwrap_or(Value::Null);
                                (pair.pop().unwrap_or(Value::Null), value)
                            }
                            other => (other, Value::Null),
                        })
                        .collect()
                } else if items.len() % 2 == 0
                    && !items.iter().any(|i| matches!(i, Value::Array(_)))
                {
                    items
                        .chunks(2)
                        .map(|pair| (pair[0].clone(), pair[1].clone()))
                        .collect()
                } else {
                    return Some(raise("array must have even number of elements"));
                }
            };
            let mut out = String::from("{");
            for (i, (key, value)) in pairs.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                if *key == Value::Null {
                    return Some(raise("null value not allowed for object key"));
                }
                crate::json_text::value_json(&Value::Text(text(key)), false, &mut out);
                out.push_str(" : ");
                match value {
                    Value::Null => out.push_str("null"),
                    v => crate::json_text::value_json(&Value::Text(text(v)), false, &mut out),
                }
            }
            out.push('}');
            Value::Json(out)
        }
        "JSONB_BUILD_OBJECT" => {
            if args.len() % 2 != 0 {
                return Some(raise("argument list must have even number of elements"));
            }
            let mut map = serde_json::Map::new();
            for pair in args.chunks(2) {
                if matches!(pair[0], Value::Null) {
                    return Some(raise("null value not allowed for object key"));
                }
                map.insert(text(&pair[0]), to_json(&pair[1]));
            }
            Value::Jsonb(serde_json::Value::Object(map))
        }
        "JSONB_BUILD_ARRAY" => {
            Value::Jsonb(serde_json::Value::Array(args.iter().map(to_json).collect()))
        }
        "JSON_TYPEOF" | "JSONB_TYPEOF" if arity(1) => {
            let json = json_arg(arg(0))?;
            Value::Text(
                match json {
                    serde_json::Value::Object(_) => "object",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Null => "null",
                }
                .to_string(),
            )
        }
        "JSON_ARRAY_LENGTH" | "JSONB_ARRAY_LENGTH" if arity(1) => match json_arg(arg(0))? {
            serde_json::Value::Array(items) => Value::Int(items.len() as i64),
            _ => raise("cannot get array length of a non-array"),
        },
        "JSON_EXTRACT_PATH"
        | "JSONB_EXTRACT_PATH"
        | "JSON_EXTRACT_PATH_TEXT"
        | "JSONB_EXTRACT_PATH_TEXT"
            if !args.is_empty() =>
        {
            let path = Value::Array(args[1..].to_vec());
            let op = if name.ends_with("_TEXT") {
                crate::ScalarBinaryOp::JsonPathText
            } else {
                crate::ScalarBinaryOp::JsonPath
            };
            // An untyped document is `json` to the `json` functions.
            let document = match arg(0) {
                Value::Text(s) if name.starts_with("JSON_") => Value::Json(s.clone()),
                other => other.clone(),
            };
            crate::planner::apply_binary_op(op, document, path)
        }
        // Text-search markers and functions. Their arguments are strict
        // (a NULL anywhere gives NULL).
        "__TSLEN__" if arity(1) => textsearch_function("length", args),
        "__TSCMP__" | "__TS__" if arity(4) => textsearch_marker(&name, args),
        "__TSNOT__" if arity(1) => textsearch_function("not", args),
        "STRIP" | "SETWEIGHT" | "NUMNODE" | "TSVECTOR_TO_ARRAY" | "ARRAY_TO_TSVECTOR"
        | "TSQUERY_PHRASE" | "TS_DELETE" =>
        {
            textsearch_function(&name.to_ascii_lowercase(), args)
        }
        // `to_tsquery` and its family: the dictionary layer's queries.
        "TO_TSQUERY" | "PLAINTO_TSQUERY" | "PHRASETO_TSQUERY" | "WEBSEARCH_TO_TSQUERY"
            if arity(1) || arity(2) =>
        {
            if args.iter().any(|a| matches!(a, Value::Null)) {
                return Some(Value::Null);
            }
            let config = if arity(2) {
                match to_ts_config(arg(0)) {
                    Ok(config) => config,
                    Err(error) => return Some(raise(error)),
                }
            } else {
                default_ts_config()
            };
            let text = text(args.last().expect("arg"));
            match &*name {
                "TO_TSQUERY" => crate::textsearch::to_tsquery(config, &text),
                "PLAINTO_TSQUERY" => crate::textsearch::plainto_tsquery(config, &text),
                "PHRASETO_TSQUERY" => crate::textsearch::phraseto_tsquery(config, &text),
                _ => crate::textsearch::websearch_to_tsquery(config, &text),
            }
            .map_or_else(raise, Value::Text)
        }
        // The JSON document of a `to_tsvector` call, read back from the text
        // a plan carries.
        crate::result_types::TS_JSON_DOC if arity(2) => match arg(0) {
            Value::Null => Value::Null,
            Value::Json(_) | Value::Jsonb(_) => arg(0).clone(),
            Value::Text(document) => {
                let as_json = text(args.get(1).expect("the kind")) == "json";
                match crate::json_text::parse(document) {
                    Ok(value) if as_json => Value::Json(document.clone()),
                    Ok(value) => Value::Jsonb(value),
                    Err(error) => raise(error),
                }
            }
            other => raise(format!(
                "cannot cast {} to json",
                crate::value::value_type_name(other)
            )),
        },
        // `ts_rank`/`ts_rank_cd`([weights,] vector, query [, method])`: the
        // vector's ranking against the query.
        "TS_RANK" | "TS_RANK_CD" if (2..=4).contains(&args.len()) => {
            if args.iter().any(|a| matches!(a, Value::Null)) {
                return Some(Value::Null);
            }
            // The call's shape: the third argument's kind decides between
            // `(weights, vector, query)` and `(vector, query, method)`.
            let (weights, at, method_at) = match (args.len(), args.get(2)) {
                (4, _) => (weights_of(arg(0)), 1, Some(3)),
                (3, Some(Value::Int(_))) => (crate::tsrank::DEFAULT_WEIGHTS, 0, Some(2)),
                (3, _) => (weights_of(arg(0)), 1, None),
                _ => (crate::tsrank::DEFAULT_WEIGHTS, 0, None),
            };
            let (Ok(vector), Ok(query)) = (
                crate::textsearch::parse_tsvector(&text(&args[at])),
                crate::textsearch::parse_tsquery(&text(&args[at + 1])),
            ) else {
                return Some(raise("invalid input syntax"));
            };
            let method = match method_at.map(|i| &args[i]) {
                Some(Value::Int(method)) => *method as i32,
                _ => crate::tsrank::RANK_NO_NORM,
            };
            let rank = if name == "TS_RANK" {
                crate::tsrank::calc_rank(&weights, &vector, &query, method)
            } else {
                crate::tsrank::calc_rank_cd(&weights, &vector, &query, method)
            };
            Value::Float(f64::from(rank))
        }
        // `to_tsvector([config,] text | json | jsonb)`: the dictionary
        // layer's vector; a JSON document contributes its strings.
        "TO_TSVECTOR" if arity(1) || arity(2) => {
            if args.iter().any(|a| matches!(a, Value::Null)) {
                return Some(Value::Null);
            }
            let config = match ts_config_of(args, arity(2)) {
                Ok(config) => config,
                Err(error) => return Some(raise(error)),
            };
            match args.last().expect("arg") {
                Value::Jsonb(document) => Value::Text(crate::ts_dict::jsonb_to_tsvector(
                    config,
                    document,
                    crate::ts_dict::JTI_STRING,
                )),
                Value::Json(document) => {
                    match crate::ts_dict::json_to_tsvector(
                        config,
                        document,
                        crate::ts_dict::JTI_STRING,
                    ) {
                        Ok(vector) => Value::Text(vector),
                        Err(error) => raise(error),
                    }
                }
                other => Value::Text(crate::ts_dict::to_tsvector(config, &text(other))),
            }
        }
        // `json_to_tsvector`/`jsonb_to_tsvector`([config,] document, flags)`:
        // the values the flags name.
        "JSON_TO_TSVECTOR" | "JSONB_TO_TSVECTOR" if arity(2) || arity(3) => {
            if args.iter().any(|a| matches!(a, Value::Null)) {
                return Some(Value::Null);
            }
            let config = match ts_config_of(args, arity(3)) {
                Ok(config) => config,
                Err(error) => return Some(raise(error)),
            };
            let Some(Value::Jsonb(flags_value)) = args.last() else {
                return Some(raise("wrong flag type, only arrays and scalars are allowed"));
            };
            if let Err(error) = crate::ts_dict::check_flags_shape(flags_value) {
                return Some(raise(error));
            }
            let flags = match crate::ts_dict::parse_index_flags(flags_value) {
                Ok(flags) => flags,
                Err(error) => return Some(raise(error)),
            };
            let document = &args[args.len() - 2];
            let vector = match (&*name, document) {
                ("JSONB_TO_TSVECTOR", Value::Jsonb(document)) => {
                    crate::ts_dict::jsonb_to_tsvector(config, document, flags)
                }
                (_, Value::Json(document)) => {
                    match crate::ts_dict::json_to_tsvector(config, document, flags) {
                        Ok(vector) => vector,
                        Err(error) => return Some(raise(error)),
                    }
                }
                _ => return Some(raise("invalid input syntax")),
            };
            Value::Text(vector)
        }
        // The jsonpath functions: all arguments are strict (a NULL anywhere
        // gives NULL), and the `_tz` spellings behave as the others do.
        // `jsonb_path_query` is set-returning and handled below.
        "JSONB_PATH_EXISTS" | "JSONB_PATH_EXISTS_TZ" | "JSONB_PATH_MATCH"
        | "JSONB_PATH_MATCH_TZ" | "JSONB_PATH_QUERY_ARRAY"
        | "JSONB_PATH_QUERY_ARRAY_TZ" | "JSONB_PATH_QUERY_FIRST"
        | "JSONB_PATH_QUERY_FIRST_TZ"
            if (2..=4).contains(&args.len()) =>
        {
            jsonpath_function(&name, &args)
        }
        "JSONB_SET" if arity(3) || arity(4) => {
            let mut json = json_arg(arg(0))?;
            let path: Vec<String> = array(arg(1))?.iter().map(text).collect();
            let create = !matches!(args.get(3), Some(Value::Bool(false)));
            let new_value = to_json(&parse_json_arg(arg(2)));
            json_set(&mut json, &path, new_value, create);
            Value::Jsonb(json)
        }
        // `jsonb_insert(target, path, value [, insert_after])`: a new array
        // element before (or after) the path's, or a new object key.
        "JSONB_INSERT" if arity(3) || arity(4) => {
            let mut json = json_arg(arg(0))?;
            let path: Vec<String> = array(arg(1))?.iter().map(text).collect();
            let after = matches!(args.get(3), Some(Value::Bool(true)));
            let new_value = to_json(&parse_json_arg(arg(2)));
            match json_insert(&mut json, &path, new_value, after) {
                Ok(()) => Value::Jsonb(json),
                Err(e) => raise(e),
            }
        }
        // `jsonb_set_lax`: as `jsonb_set`, but a NULL value is treated as its
        // last argument says (`use_json_null` by default).
        "JSONB_SET_LAX" if (3..=5).contains(&args.len()) => {
            if matches!(arg(0), Value::Null) || matches!(arg(1), Value::Null) {
                return Some(Value::Null);
            }
            let mut json = json_arg(arg(0))?;
            let path: Vec<String> = array(arg(1))?.iter().map(text).collect();
            let create = !matches!(args.get(3), Some(Value::Bool(false)));
            if matches!(arg(2), Value::Null) {
                let treatment = args.get(4).map_or("use_json_null".to_string(), text);
                match treatment.as_str() {
                    "raise_exception" => return Some(raise("JSON value must not be null")),
                    "return_target" => return Some(Value::Jsonb(json)),
                    "delete_key" => {
                        json_delete_path(&mut json, &path);
                        return Some(Value::Jsonb(json));
                    }
                    "use_json_null" => {
                        json_set(&mut json, &path, serde_json::Value::Null, create);
                        return Some(Value::Jsonb(json));
                    }
                    _ => {
                        return Some(raise(
                            "null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"",
                        ));
                    }
                }
            }
            let new_value = to_json(&parse_json_arg(arg(2)));
            json_set(&mut json, &path, new_value, create);
            Value::Jsonb(json)
        }
        "JSON_STRIP_NULLS" if arity(1) || arity(2) => {
            let in_arrays = matches!(args.get(1), Some(Value::Bool(true)));
            match arg(0) {
                Value::Json(text) | Value::Text(text) => {
                    match crate::json_text::strip_nulls_text(text, in_arrays) {
                        Some(stripped) => Value::Json(stripped),
                        None => raise("invalid input syntax for type json"),
                    }
                }
                other => Value::Json(crate::json_text::strip_nulls_text(
                    &json_of(other, false),
                    in_arrays,
                )?),
            }
        }
        "JSONB_STRIP_NULLS" if arity(1) || arity(2) => {
            let mut json = json_arg(arg(0))?;
            strip_nulls(&mut json, matches!(args.get(1), Some(Value::Bool(true))));
            Value::Jsonb(json)
        }
        "JSONB_PRETTY" if arity(1) => {
            Value::Text(crate::json_text::jsonb_pretty(&json_arg(arg(0))?))
        }

        "UNNEST"
        | "GENERATE_SERIES"
        | "JSONB_ARRAY_ELEMENTS"
        | "JSON_ARRAY_ELEMENTS"
        | "JSONB_ARRAY_ELEMENTS_TEXT"
        | "JSON_ARRAY_ELEMENTS_TEXT"
        | "REGEXP_SPLIT_TO_TABLE"
        | "REGEXP_MATCHES"
        | "STRING_TO_TABLE"
        | "GENERATE_SUBSCRIPTS"
        | "JSONB_OBJECT_KEYS"
        | "JSON_OBJECT_KEYS"
        | "JSONB_EACH"
        | "JSONB_EACH_TEXT"
        | "JSON_EACH"
        | "JSON_EACH_TEXT"
        | "JSONB_TO_RECORD"
        | "JSON_TO_RECORD"
        | "JSONB_TO_RECORDSET"
        | "JSON_TO_RECORDSET"
        | "JSONB_PATH_QUERY"
        | "JSONB_PATH_QUERY_TZ"
        | "PG_PARTITION_ANCESTORS" => raise(format!(
            "set-returning function {}() is not allowed here",
            name.to_ascii_lowercase()
        )),
        // An integer result checked against its type's range.
        crate::result_types::INTEGER_RANGE if arity(2) => match arg(0) {
            Value::Int(v) => {
                let ty = text(arg(1));
                let (min, max) = crate::value::integer_range(&ty);
                if (min..=max).contains(v) {
                    Value::Int(*v)
                } else {
                    raise(format!("{} out of range", crate::value::sql_type_name(&ty)))
                }
            }
            other => other.clone(),
        },

        // ---- Subscripts ---------------------------------------------------------------
        "__SUBSCRIPT__" if arity(2) => match arg(0) {
            // `jsonb` subscripting: an object field or an array element.
            Value::Jsonb(json) => {
                let step = match (json, arg(1)) {
                    (serde_json::Value::Object(map), key) => map.get(&text(key)).cloned(),
                    (serde_json::Value::Array(items), index) => int(index).and_then(|i| {
                        let i = if i < 0 { items.len() as i64 + i } else { i };
                        usize::try_from(i).ok().and_then(|i| items.get(i).cloned())
                    }),
                    _ => None,
                };
                step.map_or(Value::Null, Value::Jsonb)
            }
            base => {
                let items = array(base)?;
                let index = int(arg(1))?;
                usize::try_from(index)
                    .ok()
                    .filter(|&i| i >= 1)
                    .and_then(|i| items.get(i - 1).cloned())
                    .unwrap_or(Value::Null)
            }
        },
        "__SLICE__" if arity(3) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let items = array(arg(0))?;
            let bound = |v: &Value, default: i64| match v {
                Value::Null => Some(default),
                v => int(v),
            };
            let low = bound(arg(1), 1)?.max(1);
            let high = bound(arg(2), items.len() as i64)?.min(items.len() as i64);
            Value::Array(if low > high {
                Vec::new()
            } else {
                items[(low - 1) as usize..high as usize].to_vec()
            })
        }

        // ---- Arrays -------------------------------------------------------------------
        "ARRAY_LENGTH" if arity(2) => {
            let items = array(arg(0))?;
            let dim = int(arg(1))?;
            match dimension_lengths(&items).get((dim - 1).max(0) as usize) {
                Some(&len) if dim >= 1 && len > 0 => Value::Int(len as i64),
                _ => Value::Null,
            }
        }
        "ARRAY_UPPER" if arity(2) => {
            let items = array(arg(0))?;
            let dim = int(arg(1))?;
            match dimension_lengths(&items).get((dim - 1).max(0) as usize) {
                Some(&len) if dim >= 1 && len > 0 => Value::Int(len as i64),
                _ => Value::Null,
            }
        }
        "ARRAY_LOWER" if arity(2) => {
            let items = array(arg(0))?;
            let dim = int(arg(1))?;
            match dimension_lengths(&items).get((dim - 1).max(0) as usize) {
                Some(&len) if dim >= 1 && len > 0 => Value::Int(1),
                _ => Value::Null,
            }
        }
        "ARRAY_NDIMS" if arity(1) => {
            let dims = dimension_lengths(&array(arg(0))?);
            if dims.first().is_some_and(|&n| n > 0) {
                Value::Int(dims.len() as i64)
            } else {
                Value::Null
            }
        }
        "CARDINALITY" if arity(1) => {
            let mut flat = Vec::new();
            flatten(array(arg(0))?, &mut flat);
            Value::Int(flat.len() as i64)
        }
        "ARRAY_APPEND" if arity(2) => {
            let mut items = if matches!(arg(0), Value::Null) {
                Vec::new()
            } else {
                array(arg(0))?
            };
            items.push(arg(1).clone());
            Value::Array(items)
        }
        "ARRAY_PREPEND" if arity(2) => {
            let mut items = if matches!(arg(1), Value::Null) {
                Vec::new()
            } else {
                array(arg(1))?
            };
            items.insert(0, arg(0).clone());
            Value::Array(items)
        }
        "ARRAY_CAT" if arity(2) => match (arg(0), arg(1)) {
            (Value::Null, Value::Null) => Value::Null,
            (Value::Null, b) => Value::Array(array(b)?),
            (a, Value::Null) => Value::Array(array(a)?),
            (a, b) => {
                let mut items = array(a)?;
                items.extend(array(b)?);
                Value::Array(items)
            }
        },
        "ARRAY_POSITION" | "ARRAY_POSITIONS" if arity(2) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let positions: Vec<i64> = array(arg(0))?
                .iter()
                .enumerate()
                .filter(|(_, v)| match (v, arg(1)) {
                    (Value::Null, Value::Null) => true,
                    (Value::Null, _) | (_, Value::Null) => false,
                    (v, target) => {
                        crate::planner::apply_binary_op(
                            crate::ScalarBinaryOp::Eq,
                            (*v).clone(),
                            target.clone(),
                        ) == Value::Bool(true)
                    }
                })
                .map(|(i, _)| i as i64 + 1)
                .collect();
            if name == "ARRAY_POSITION" {
                positions.first().map_or(Value::Null, |&p| Value::Int(p))
            } else {
                Value::Array(positions.into_iter().map(Value::Int).collect())
            }
        }
        "ARRAY_REMOVE" | "ARRAY_REPLACE" if arity(2) || arity(3) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let target = arg(1);
            let is_target = |v: &Value| match (v, target) {
                (Value::Null, Value::Null) => true,
                (Value::Null, _) | (_, Value::Null) => false,
                (v, t) => values_equal(v, t),
            };
            let items = array(arg(0))?;
            Value::Array(if name == "ARRAY_REMOVE" {
                items.into_iter().filter(|v| !is_target(v)).collect()
            } else {
                items
                    .into_iter()
                    .map(|v| if is_target(&v) { arg(2).clone() } else { v })
                    .collect()
            })
        }
        "TRIM_ARRAY" if arity(2) => {
            let mut items = array(arg(0))?;
            let n = int(arg(1))?;
            if n < 0 || n as usize > items.len() {
                return Some(raise(
                    "number of elements to trim must be between 0 and the array length",
                ));
            }
            items.truncate(items.len() - n as usize);
            Value::Array(items)
        }
        "ARRAY_SORT" if (1..=3).contains(&args.len()) => {
            let mut items = array(arg(0))?;
            let descending = matches!(args.get(1), Some(Value::Bool(true)));
            let nulls_first = match args.get(2) {
                Some(Value::Bool(b)) => *b,
                _ => descending,
            };
            items.sort_by(|a, b| match (a, b) {
                (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                (Value::Null, _) => {
                    if nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
                (_, Value::Null) => {
                    if nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                (a, b) => {
                    let ord = crate::value::compare(a, b);
                    if descending { ord.reverse() } else { ord }
                }
            });
            Value::Array(items)
        }
        "ARRAY_REVERSE" if arity(1) => {
            let mut items = array(arg(0))?;
            items.reverse();
            Value::Array(items)
        }
        _ => return None,
    })
}

/// A session value that is only defined while a statement runs.
fn session_unavailable(name: &str) -> Value {
    raise(format!(
        "{}() cannot be evaluated here",
        name.to_ascii_lowercase()
    ))
}

/// Runs a sequence operation for the statement's session; its errors fail
/// the statement.
fn sequence_op(
    op: impl FnOnce(&crate::sequences::SequenceStore, &str) -> anyhow::Result<i64>,
) -> Value {
    let env = session_env::with(|e| {
        e.and_then(|e| e.sequences.clone().map(|s| (s, e.session_id.clone())))
    });
    match env {
        Some((store, session)) => match op(&store, &session) {
            Ok(value) => Value::Int(value),
            Err(e) => raise(e.to_string()),
        },
        None => raise("sequence functions cannot be evaluated here"),
    }
}

fn session_time(pick: impl Fn(&session_env::SessionEnv) -> i64) -> Option<i64> {
    Some(session_env::with(|e| e.map(&pick)).unwrap_or_else(session_env::wall_micros))
}

/// Renders microseconds since the epoch as PostgreSQL timestamp text in UTC.
fn timestamp(micros: i64, with_zone: bool) -> Value {
    match chrono::DateTime::from_timestamp_micros(micros) {
        Some(dt) => Value::Text(crate::value::format_timestamp(dt.naive_utc(), with_zone)),
        None => raise("timestamp out of range"),
    }
}

/// `format_type(oid, typmod)`: the SQL name of a type, with its modifier.
/// A type's name by OID, as `regtype` prints it.
pub(crate) fn format_type_name(oid: i64) -> String {
    format_type(oid, None)
}

fn format_type(oid: i64, typmod: Option<i64>) -> String {
    let base = match oid {
        16 => "boolean",
        17 => "bytea",
        18 => "\"char\"",
        19 => "name",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        24 => "regproc",
        25 => "text",
        26 => "oid",
        114 => "json",
        142 => "xml",
        700 => "real",
        701 => "double precision",
        // `bpchar` with no length is not `character`, which means
        // `character(1)`.
        1042 => match typmod {
            Some(m) if m >= 4 => return format!("character({})", m - 4),
            Some(_) => "bpchar",
            None => "character",
        },
        1560 => match typmod {
            Some(m) if m >= 0 => return format!("bit({m})"),
            _ => "bit",
        },
        1562 => match typmod {
            Some(m) if m >= 0 => return format!("bit varying({m})"),
            _ => "bit varying",
        },
        1043 => match typmod {
            Some(m) => return format!("character varying({})", m - 4),
            None => "character varying",
        },
        1082 => "date",
        1083 => "time without time zone",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        1186 => "interval",
        1266 => "time with time zone",
        2278 => "void",
        1700 => match typmod {
            Some(m) => {
                let m = m - 4;
                return format!("numeric({},{})", (m >> 16) & 0xffff, m & 0xffff);
            }
            None => "numeric",
        },
        2205 => "regclass",
        2206 => "regtype",
        2950 => "uuid",
        3802 => "jsonb",
        4072 => "jsonpath",
        3614 => "tsvector",
        3615 => "tsquery",
        3734 => "regconfig",
        3769 => "regdictionary",
        4089 => "regnamespace",
        4096 => "regrole",
        1000 => "boolean[]",
        1005 => "smallint[]",
        1007 => "integer[]",
        1009 => "text[]",
        1014 => "bpchar[]",
        1015 => "character varying[]",
        1016 => "bigint[]",
        1021 => "real[]",
        1022 => "double precision[]",
        1028 => "oid[]",
        1115 => "timestamp without time zone[]",
        1182 => "date[]",
        1185 => "timestamp with time zone[]",
        1231 => "numeric[]",
        199 => "json[]",
        2951 => "uuid[]",
        3807 => "jsonb[]",
        4073 => "jsonpath[]",
        143 => "xml[]",
        3643 => "tsvector[]",
        3645 => "tsquery[]",
        3904 => "int4range",
        3926 => "int8range",
        3906 => "numrange",
        3912 => "daterange",
        3908 => "tsrange",
        3910 => "tstzrange",
        600 => "point",
        601 => "lseg",
        602 => "path",
        603 => "box",
        604 => "polygon",
        628 => "line",
        718 => "circle",
        1017 => "point[]",
        1018 => "lseg[]",
        1019 => "path[]",
        1020 => "box[]",
        1027 => "polygon[]",
        629 => "line[]",
        719 => "circle[]",
        4451 => "int4multirange",
        4536 => "int8multirange",
        4532 => "nummultirange",
        4535 => "datemultirange",
        4533 => "tsmultirange",
        4534 => "tstzmultirange",
        6150 => "int4multirange[]",
        6157 => "int8multirange[]",
        6151 => "nummultirange[]",
        6155 => "datemultirange[]",
        6152 => "tsmultirange[]",
        6153 => "tstzmultirange[]",
        869 => "inet",
        650 => "cidr",
        829 => "macaddr",
        774 => "macaddr8",
        790 => "money",
        1041 => "inet[]",
        651 => "cidr[]",
        1040 => "macaddr[]",
        775 => "macaddr8[]",
        791 => "money[]",
        3905 => "int4range[]",
        3927 => "int8range[]",
        3907 => "numrange[]",
        3913 => "daterange[]",
        3909 => "tsrange[]",
        3911 => "tstzrange[]",
        _ => {
            return match crate::user_types::type_of_oid(oid) {
                Some((t, false)) => t.display_name(),
                Some((t, true)) => format!("{}[]", t.display_name()),
                None => "???".to_string(),
            };
        }
    };
    base.to_string()
}

fn flatten(items: Vec<Value>, out: &mut Vec<Value>) {
    for item in items {
        match item {
            Value::Array(inner) => flatten(inner, out),
            v => out.push(v),
        }
    }
}

/// Lengths of each dimension of a (rectangular) nested array.
fn dimension_lengths(items: &[Value]) -> Vec<usize> {
    let mut dims = vec![items.len()];
    if let Some(Value::Array(inner)) = items.first() {
        dims.extend(dimension_lengths(inner));
    }
    dims
}

/// The keywords an identifier must be quoted to be: PostgreSQL's reserved,
/// column-name, and type-or-function-name keywords.
const QUOTED_KEYWORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "between",
    "bigint",
    "binary",
    "bit",
    "boolean",
    "both",
    "case",
    "cast",
    "char",
    "character",
    "check",
    "coalesce",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "dec",
    "decimal",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "exists",
    "extract",
    "false",
    "fetch",
    "float",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "greatest",
    "group",
    "grouping",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "inout",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "isnull",
    "join",
    "json",
    "json_array",
    "json_arrayagg",
    "json_exists",
    "json_object",
    "json_objectagg",
    "json_query",
    "json_scalar",
    "json_serialize",
    "json_table",
    "json_value",
    "lateral",
    "leading",
    "least",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "merge_action",
    "national",
    "natural",
    "nchar",
    "none",
    "normalize",
    "not",
    "notnull",
    "null",
    "nullif",
    "numeric",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "out",
    "outer",
    "overlaps",
    "overlay",
    "placing",
    "position",
    "precision",
    "primary",
    "real",
    "references",
    "returning",
    "right",
    "row",
    "select",
    "session_user",
    "setof",
    "similar",
    "smallint",
    "some",
    "substring",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "time",
    "timestamp",
    "to",
    "trailing",
    "treat",
    "trim",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "values",
    "varchar",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
    "xmlattributes",
    "xmlconcat",
    "xmlelement",
    "xmlexists",
    "xmlforest",
    "xmlnamespaces",
    "xmlparse",
    "xmlpi",
    "xmlroot",
    "xmlserialize",
    "xmltable",
];

/// A character in upper case when that is one character.
fn upper_char(c: char) -> char {
    let mut upper = c.to_uppercase();
    match (upper.next(), upper.next()) {
        (Some(u), None) => u,
        _ => c,
    }
}

/// A character in lower case (its first character, for the few that
/// lower-case to several).
fn lower_char(c: char) -> char {
    c.to_lowercase().next().unwrap_or(c)
}

/// `unistr(text)`: the text with its Unicode escapes (`\0041`,
/// `\+01F600`, `\u0041`, `\U0001F600`) and doubled backslashes read.
fn unistr(s: &str) -> Result<String, String> {
    let invalid = || {
        crate::error_fields::DbError::new("invalid Unicode escape")
            .hint("Unicode escapes must be \\XXXX, \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX.")
            .into_text()
    };
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    let mut high: Option<u32> = None;
    while i < chars.len() {
        if chars[i] != '\\' {
            if high.is_some() {
                return Err(invalid());
            }
            out.push(chars[i]);
            i += 1;
            continue;
        }
        if chars.get(i + 1) == Some(&'\\') {
            out.push('\\');
            i += 2;
            continue;
        }
        let (skip, digits) = match chars.get(i + 1) {
            Some('+') => (2, 6),
            Some('u') => (2, 4),
            Some('U') => (2, 8),
            _ => (1, 4),
        };
        let hex: String = chars.iter().skip(i + skip).take(digits).collect();
        if hex.len() != digits || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(invalid());
        }
        let code = u32::from_str_radix(&hex, 16).map_err(|_| invalid())?;
        i += skip + digits;
        let code = match (high.take(), code) {
            (None, 0xD800..=0xDBFF) => {
                high = Some(code);
                continue;
            }
            (Some(h), 0xDC00..=0xDFFF) => 0x10000 + ((h - 0xD800) << 10) + (code - 0xDC00),
            (Some(_), _) | (None, 0xDC00..=0xDFFF) => return Err(invalid()),
            (None, code) => code,
        };
        out.push(char::from_u32(code).ok_or_else(invalid)?);
    }
    if high.is_some() {
        return Err(invalid());
    }
    Ok(out)
}

fn quote_ident(s: &str) -> String {
    let plain = s
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !QUOTED_KEYWORDS.contains(&s);
    if plain {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

fn quote_literal(s: &str) -> String {
    if s.contains('\\') {
        format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
    } else {
        format!("'{}'", s.replace('\'', "''"))
    }
}

/// `format()` with `%s`, `%I`, `%L`, `%%`, `n$` positions, and `-`/width.
fn format_text(fmt: &str, args: &[Value]) -> Value {
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    let mut next_arg = 0usize;
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }
        let mut spec = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '$' || c == '-' {
                spec.push(c);
                chars.next();
            } else {
                break;
            }
        }
        let Some(kind) = chars.next() else {
            return raise("unterminated format() type specifier");
        };
        let (position, flags) = match spec.split_once('$') {
            Some((pos, rest)) => match pos.parse::<usize>() {
                Ok(p) if p >= 1 => (p - 1, rest.to_string()),
                _ => {
                    return raise("format specifies argument 0, but arguments are numbered from 1");
                }
            },
            None => (next_arg, spec),
        };
        next_arg = position + 1;
        let Some(value) = args.get(position) else {
            return raise("too few arguments for format()");
        };
        let rendered = match kind {
            's' => match value {
                Value::Null => String::new(),
                v => text(v),
            },
            'I' => match value {
                Value::Null => {
                    return raise("null values cannot be formatted as an SQL identifier");
                }
                v => quote_ident(&text(v)),
            },
            'L' => match value {
                Value::Null => "NULL".to_string(),
                v => quote_literal(&text(v)),
            },
            other => return raise(format!("unrecognized format() type specifier \"{other}\"")),
        };
        let left = flags.starts_with('-');
        let width: usize = flags.trim_start_matches('-').parse().unwrap_or(0);
        let pad = width.saturating_sub(rendered.chars().count());
        if left {
            out.push_str(&rendered);
            out.push_str(&" ".repeat(pad));
        } else {
            out.push_str(&" ".repeat(pad));
            out.push_str(&rendered);
        }
    }
    Value::Text(out)
}

/// A value as JSON, as `to_jsonb` renders it.
pub(crate) fn to_json(v: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        Value::Null => J::Null,
        Value::Int(i) => J::from(*i),
        Value::Float(f) => serde_json::Number::from_f64(*f).map_or(J::Null, J::Number),
        // Exactly, as `numeric` prints it (`1.50`); NaN and the infinities
        // as strings.
        Value::Numeric(d) => d
            .to_string()
            .parse::<serde_json::Number>()
            .map_or_else(|_| J::String(d.to_string()), J::Number),
        Value::Bool(b) => J::Bool(*b),
        Value::Text(s) => J::String(s.clone()),
        Value::Bytea(bytes) => J::String(crate::bytea::hex_text(bytes)),
        Value::Array(items) => J::Array(items.iter().map(to_json).collect()),
        Value::Jsonb(j) => j.clone(),
        Value::Json(text) => {
            crate::json_text::parse(text).unwrap_or_else(|_| J::String(text.clone()))
        }
        Value::Record(fields) => J::Object(
            fields
                .iter()
                .map(|(name, value)| (name.clone(), to_json(value)))
                .collect(),
        ),
    }
}

/// A value as `json` text, as `to_json` writes it.
pub(crate) fn json_of(value: &Value, pretty: bool) -> String {
    let mut out = String::new();
    crate::json_text::value_json(value, pretty, &mut out);
    out
}

/// The optional `pretty` flag of `row_to_json` and `array_to_json`.
fn pretty(args: &[Value]) -> Option<bool> {
    match args.get(1) {
        None => Some(false),
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => None,
    }
}

/// The weights of a `ts_rank` call, with PostgreSQL's errors for an array
/// of the wrong shape, a short one, a NULL element, or an out-of-range
/// weight. A negative weight takes the default for its slot.
fn weights_of(value: &Value) -> [f32; 4] {
    // Each failure stages the error and returns a placeholder for the
    // caller to discard.
    let bad = |message: &str, code: &str| -> [f32; 4] {
        crate::eval_error::raise(
            crate::error_fields::DbError::new(message)
                .code(code)
                .into_text(),
        );
        crate::tsrank::DEFAULT_WEIGHTS
    };
    // An untyped literal is the array's text, as PostgreSQL resolves it.
    let parsed;
    let value = match value {
        Value::Text(text) => match crate::value::parse_array_literal(text) {
            Some(items) => {
                parsed = Value::Array(items);
                &parsed
            }
            None => return bad("array of weight must be one-dimensional", "2202E"),
        },
        other => other,
    };
    let Value::Array(items) = value else {
        return bad("array of weight must be one-dimensional", "2202E");
    };
    if items
        .iter()
        .any(|item| matches!(item, Value::Array(_) | Value::Null))
    {
        return if items.iter().any(|item| matches!(item, Value::Null)) {
            bad("array of weight must not contain nulls", "22004")
        } else {
            bad("array of weight must be one-dimensional", "2202E")
        };
    }
    if items.len() < 4 {
        return bad("array of weight is too short", "2202E");
    }
    let mut weights = crate::tsrank::DEFAULT_WEIGHTS;
    for (i, item) in items.iter().take(4).enumerate() {
        let weight = match item {
            Value::Float(f) => *f as f32,
            Value::Int(n) => *n as f32,
            Value::Numeric(d) => d.to_string().parse::<f32>().unwrap_or(0.0),
            // An untyped array literal keeps its elements as text.
            Value::Text(text) => match text.trim().parse::<f32>() {
                Ok(weight) => weight,
                Err(_) => {
                    crate::eval_error::raise(
                        crate::error_fields::DbError::new(format!(
                            "invalid input syntax for type real: \"{text}\""
                        ))
                        .code("22P02")
                        .into_text(),
                    );
                    return crate::tsrank::DEFAULT_WEIGHTS;
                }
            },
            _ => return bad("array of weight must be one-dimensional", "2202E"),
        };
        weights[i] = if weight >= 0.0 {
            weight
        } else {
            crate::tsrank::DEFAULT_WEIGHTS[i]
        };
        if weights[i] > 1.0 {
            return bad("weight out of range", "22023");
        }
    }
    weights
}

/// The configuration of a `to_tsvector`-family call: the first argument
/// when there is one, otherwise the session's default.
fn ts_config_of(args: &[Value], with_config: bool) -> Result<crate::ts_dict::Config, String> {
    if with_config {
        to_ts_config(&args[0])
    } else {
        Ok(default_ts_config())
    }
}

/// The configuration a `to_tsvector` call names: a `regconfig` value (an
/// OID), or an untyped literal that resolves like a name.
fn to_ts_config(value: &Value) -> Result<crate::ts_dict::Config, String> {
    match value {
        Value::Int(oid) => crate::pg_catalog::text_search_config_name(*oid)
            .and_then(crate::ts_dict::Config::of)
            .ok_or_else(|| format!("cache lookup failed for text search configuration {oid}")),
        Value::Text(name) => crate::ts_dict::config_of_qualified_name(name),
        other => Err(format!(
            "cannot cast {} to regconfig",
            crate::value::value_type_name(other)
        )),
    }
}

/// The dictionary a `regdictionary` value names: an OID, or an untyped
/// literal that resolves like a name.
fn dictionary_of(value: &Value) -> Result<crate::ts_dict::Config, String> {
    match value {
        Value::Int(oid) => crate::ts_dict::dictionary_by_oid(*oid)
            .ok_or_else(|| format!("cache lookup failed for text search dictionary {oid}")),
        Value::Text(name) => crate::ts_dict::config_of_dictionary_name(name),
        other => Err(format!(
            "cannot cast {} to regdictionary",
            crate::value::value_type_name(other)
        )),
    }
}

/// The session's `default_text_search_config`, as `to_tsvector(text)` reads
/// it.
pub(crate) fn default_ts_config() -> crate::ts_dict::Config {
    let name = crate::session_env::setting("default_text_search_config")
        .unwrap_or_else(|| "pg_catalog.english".to_string());
    crate::ts_dict::Config::of(&name).unwrap_or(crate::ts_dict::Config::English)
}

/// A text-search operator rewritten to a marker once the declared types are
/// known: comparisons compare with the type's own ordering, and the others
/// build or test vectors and queries.
fn textsearch_marker(name: &str, args: &[Value]) -> Value {
    if args.iter().any(|a| matches!(a, Value::Null)) {
        return Value::Null;
    }
    let symbol = text(&args[0]);
    let comparison = name == crate::result_types::TS_CMP;
    if comparison {
        let (Some(left), Some(right)) = (text_arg_at(args, 1), text_arg_at(args, 2)) else {
            return raise("invalid input syntax for type tsvector");
        };
        let kind = text(&args[3]);
        let ord = if kind == "tsvector" {
            match (
                crate::textsearch::parse_tsvector(&left),
                crate::textsearch::parse_tsvector(&right),
            ) {
                (Ok(l), Ok(r)) => crate::textsearch::tsvector_cmp(&l, &r),
                _ => return raise("invalid input syntax for type tsvector"),
            }
        } else {
            match (
                crate::textsearch::parse_tsquery(&left),
                crate::textsearch::parse_tsquery(&right),
            ) {
                (Ok(l), Ok(r)) => crate::textsearch::tsquery_cmp(&l, &r),
                _ => return raise("invalid input syntax for type tsquery"),
            }
        };
        let result = match symbol.as_str() {
            "=" => ord == std::cmp::Ordering::Equal,
            "<>" => ord != std::cmp::Ordering::Equal,
            "<" => ord == std::cmp::Ordering::Less,
            "<=" => ord != std::cmp::Ordering::Greater,
            ">" => ord == std::cmp::Ordering::Greater,
            ">=" => ord != std::cmp::Ordering::Less,
            _ => return raise("unrecognized comparison"),
        };
        return Value::Bool(result);
    }
    match symbol.as_str() {
        "||" | "&&" | "<->" | "@>" | "<@" => {
            let kind = text(&args[3]);
            if kind == "tsvector" {
                let (Ok(l), Ok(r)) = (
                    crate::textsearch::parse_tsvector(&text_arg_at(args, 1).unwrap_or_default()),
                    crate::textsearch::parse_tsvector(&text_arg_at(args, 2).unwrap_or_default()),
                ) else {
                    return raise("invalid input syntax for type tsvector");
                };
                return match symbol.as_str() {
                    "||" => Value::Text(crate::textsearch::print_tsvector(
                        &crate::textsearch::tsvector_concat(&l, &r),
                    )),
                    _ => raise(format!(
                        "operator does not exist: tsvector {symbol} tsvector"
                    )),
                };
            }
            let (Ok(l), Ok(r)) = (
                crate::textsearch::parse_tsquery(&text_arg_at(args, 1).unwrap_or_default()),
                crate::textsearch::parse_tsquery(&text_arg_at(args, 2).unwrap_or_default()),
            ) else {
                return raise("invalid input syntax for type tsquery");
            };
            match symbol.as_str() {
                "||" => Value::Text(crate::textsearch::print_tsquery(
                    &crate::textsearch::query_or(&l, &r),
                )),
                "&&" => Value::Text(crate::textsearch::print_tsquery(
                    &crate::textsearch::query_and(&l, &r),
                )),
                "<->" => Value::Text(crate::textsearch::print_tsquery(
                    &crate::textsearch::query_phrase(&l, &r, 1),
                )),
                "@>" => Value::Bool(crate::textsearch::query_contains(&l, &r)),
                "<@" => Value::Bool(crate::textsearch::query_contained(&l, &r)),
                _ => raise(format!("operator does not exist: tsquery {symbol} tsquery")),
            }
        }
        "@@" => {
            let (Some(vector), Some(query)) = (text_arg_at(args, 1), text_arg_at(args, 2)) else {
                return raise("invalid input syntax for type tsvector");
            };
            let kind = text(&args[3]);
            let (vector, query) = if kind == "tsvector" {
                (vector, query)
            } else {
                (query, vector)
            };
            let (Ok(vector), Ok(query)) = (
                crate::textsearch::parse_tsvector(&vector),
                crate::textsearch::parse_tsquery(&query),
            ) else {
                return raise("invalid input syntax for type tsvector");
            };
            Value::Bool(crate::textsearch::matches(&vector, &query))
        }
        _ => raise(format!("unrecognized operator {symbol}")),
    }
}

fn text_arg_at(args: &[Value], index: usize) -> Option<String> {
    match args.get(index) {
        Some(Value::Text(t)) => Some(t.clone()),
        _ => None,
    }
}

/// A text-search function by name.
fn textsearch_function(name: &str, args: &[Value]) -> Value {
    // `array_to_tsvector` tolerates NULL elements; the rest are strict.
    if name != "array_to_tsvector" && args.iter().any(|a| matches!(a, Value::Null)) {
        return Value::Null;
    }
    let vector = |index: usize| -> Option<crate::textsearch::TsVector> {
        match args.get(index) {
            Some(Value::Text(t)) => match crate::textsearch::parse_tsvector(t) {
                Ok(v) => Some(v),
                Err(e) => {
                    crate::eval_error::raise(e);
                    None
                }
            },
            _ => None,
        }
    };
    let query = |index: usize| -> Option<crate::textsearch::QNode> {
        match args.get(index) {
            Some(Value::Text(t)) => match crate::textsearch::parse_tsquery(t) {
                Ok(q) => Some(q),
                Err(e) => {
                    crate::eval_error::raise(e);
                    None
                }
            },
            _ => None,
        }
    };
    match name {
        "length" => match vector(0) {
            Some(vector) => Value::Int(crate::textsearch::vector_length(&vector)),
            None => Value::Null,
        },
        "not" => match query(0) {
            Some(query) => Value::Text(crate::textsearch::print_tsquery(
                &crate::textsearch::query_not(&query),
            )),
            None => Value::Null,
        },
        "strip" => match vector(0) {
            Some(vector) => Value::Text(crate::textsearch::print_tsvector(
                &crate::textsearch::vector_strip(&vector),
            )),
            None => Value::Null,
        },
        "numnode" => match query(0) {
            Some(query) => Value::Int(crate::textsearch::query_numnode(&query)),
            None => Value::Null,
        },
        "tsvector_to_array" => match vector(0) {
            Some(vector) => Value::Array(
                crate::textsearch::vector_to_array(&vector)
                    .into_iter()
                    .map(Value::Text)
                    .collect(),
            ),
            None => Value::Null,
        },
        "array_to_tsvector" => {
            let Some(Value::Array(items)) = args.first() else {
                return raise("array_to_tsvector requires a text array");
            };
            let words: Vec<Option<String>> = items
                .iter()
                .map(|item| match item {
                    Value::Null => None,
                    Value::Text(t) => Some(t.clone()),
                    other => Some(render(other)),
                })
                .collect();
            match crate::textsearch::array_to_vector(&words) {
                Ok(vector) => Value::Text(crate::textsearch::print_tsvector(&vector)),
                Err(e) => crate::eval_error::raise(e),
            }
        }
        "ts_delete" => match (vector(0), args.get(1)) {
            (Some(vector), Some(Value::Text(word))) => {
                Value::Text(crate::textsearch::print_tsvector(
                    &crate::textsearch::vector_delete(&vector, &[word.clone()]),
                ))
            }
            (Some(vector), Some(Value::Array(items))) => {
                let words: Vec<String> = items
                    .iter()
                    .map(|item| match item {
                        Value::Text(t) => t.clone(),
                        other => render(other),
                    })
                    .collect();
                Value::Text(crate::textsearch::print_tsvector(
                    &crate::textsearch::vector_delete(&vector, &words),
                ))
            }
            _ => Value::Null,
        },
        "setweight" => {
            let Some(vector) = vector(0) else {
                return Value::Null;
            };
            let weight_text = match args.get(1) {
                Some(Value::Text(t)) => t.clone(),
                _ => return raise("setweight requires a weight character"),
            };
            let weight = match crate::textsearch::weight_letter(&weight_text) {
                Ok(w) => w,
                Err(e) => return crate::eval_error::raise(e),
            };
            let only: Option<Vec<String>> = match args.get(2) {
                None => None,
                Some(Value::Array(items)) => Some(
                    items
                        .iter()
                        .map(|item| match item {
                            Value::Text(t) => t.clone(),
                            other => render(other),
                        })
                        .collect(),
                ),
                Some(_) => None,
            };
            Value::Text(crate::textsearch::print_tsvector(
                &crate::textsearch::vector_setweight(&vector, weight, only.as_deref()),
            ))
        }
        "tsquery_phrase" => {
            let (Some(left), Some(right)) = (query(0), query(1)) else {
                return Value::Null;
            };
            let distance = match args.get(2) {
                None => 1,
                Some(Value::Int(n)) => *n as i32,
                _ => return raise("distance must be an integer"),
            };
            if !(0..=16384).contains(&distance) {
                return raise(
                    crate::error_fields::DbError::new(
                        "distance in phrase operator must be an integer value between zero and 16384 inclusive",
                    )
                    .code("22023")
                    .into_text(),
                );
            }
            Value::Text(crate::textsearch::print_tsquery(
                &crate::textsearch::query_phrase(&left, &right, distance as i16),
            ))
        }
        _ => raise(format!("unrecognized function {name}")),
    }
}

/// A `json`/`jsonb` argument as a document: text is JSON input (so `'1'` is
/// the number 1, as an untyped literal would be).
/// A `jsonb_path_*` call: `(target, path [, vars [, silent]])`. The callers
/// already checked the arity; this evaluates the path and shapes the result.
fn jsonpath_function(name: &str, args: &[Value]) -> Value {
    let parsed = match jsonpath_arguments(args) {
        Ok(Some(parsed)) => parsed,
        Ok(None) => return Value::Null,
        Err(message) => return raise(message),
    };
    let (target, path, vars, silent) = parsed;
    let result: std::result::Result<Value, String> = match name {
        "JSONB_PATH_EXISTS" | "JSONB_PATH_EXISTS_TZ" => {
            crate::jsonpath::exists(&target, &path, vars.as_ref(), silent)
                .map(|v| v.map_or(Value::Null, Value::Bool))
        }
        "JSONB_PATH_MATCH" | "JSONB_PATH_MATCH_TZ" => {
            crate::jsonpath::matches(&target, &path, vars.as_ref(), silent)
                .map(|v| v.map_or(Value::Null, Value::Bool))
        }
        "JSONB_PATH_QUERY_ARRAY" | "JSONB_PATH_QUERY_ARRAY_TZ" => {
            crate::jsonpath::query(&target, &path, vars.as_ref(), silent)
                .map(|items| Value::Jsonb(serde_json::Value::Array(items)))
        }
        _ => {
            // `jsonb_path_query_first`: the first item, or NULL.
            crate::jsonpath::query(&target, &path, vars.as_ref(), silent).map(|items| {
                match items.into_iter().next() {
                    Some(item) => Value::Jsonb(item),
                    None => Value::Null,
                }
            })
        }
    };
    match result {
        Ok(value) => value,
        Err(message) => raise(message),
    }
}

/// The evaluated arguments of a `jsonb_path_*` call: the document, the path
/// text, the `vars` object, and the `silent` flag. `Ok(None)` is a NULL
/// argument (the strict functions take it as NULL / no rows).
pub(crate) fn jsonpath_arguments(
    args: &[Value],
) -> std::result::Result<Option<(serde_json::Value, String, Option<serde_json::Value>, bool)>, String>
{
    if args.iter().take(4).any(|a| matches!(a, Value::Null)) {
        return Ok(None);
    }
    let target = json_document(&args[0])?;
    let path = text(&args[1]);
    let vars = match args.get(2) {
        None => None,
        Some(Value::Jsonb(j)) => Some(j.clone()),
        Some(Value::Text(t)) | Some(Value::Json(t)) => Some(crate::json_text::parse(t)?),
        Some(other) => crate::filter_eval::value_to_json(other),
    };
    if let Some(vars) = &vars
        && !vars.is_object()
    {
        return Err(
            crate::error_fields::DbError::new("\"vars\" argument is not an object")
                .code("22023")
                .detail(
                    "Jsonpath parameters should be encoded as key-value pairs of \"vars\" object.",
                )
                .into_text(),
        );
    }
    let silent = matches!(args.get(3), Some(Value::Bool(true)));
    Ok(Some((target, path, vars, silent)))
}

/// `jsonb_path_query`'s rows, for the set-returning table-function path.
pub(crate) fn jsonpath_query_rows(args: &[Value]) -> anyhow::Result<Vec<Value>> {
    match jsonpath_arguments(args) {
        Ok(None) => Ok(Vec::new()),
        Ok(Some((target, path, vars, silent))) => {
            crate::jsonpath::query(&target, &path, vars.as_ref(), silent)
                .map(|items| items.into_iter().map(Value::Jsonb).collect())
                .map_err(|e| anyhow::anyhow!(e))
        }
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

/// A `jsonb` document argument: `jsonb`, `json` text, or an untyped literal,
/// parsed with PostgreSQL's error for malformed JSON.
fn json_document(v: &Value) -> std::result::Result<serde_json::Value, String> {
    match v {
        Value::Jsonb(j) => Ok(j.clone()),
        Value::Text(s) | Value::Json(s) => crate::json_text::parse(s),
        other => crate::filter_eval::value_to_json(other)
            .ok_or_else(|| "invalid input syntax for type json".to_string()),
    }
}

fn json_arg(v: &Value) -> Option<serde_json::Value> {
    match v {
        Value::Text(s) | Value::Json(s) => crate::json_text::parse(s)
            .ok()
            .or_else(|| crate::filter_eval::value_to_json(v)),
        other => crate::filter_eval::value_to_json(other),
    }
}

/// A JSON argument given as text (`'{"a":1}'`) is parsed; other values stay.
fn parse_json_arg(v: &Value) -> Value {
    match v {
        Value::Text(s) | Value::Json(s) => {
            serde_json::from_str(s).map_or_else(|_| v.clone(), Value::Jsonb)
        }
        other => other.clone(),
    }
}

fn json_set(
    json: &mut serde_json::Value,
    path: &[String],
    new_value: serde_json::Value,
    create: bool,
) {
    let Some((key, rest)) = path.split_first() else {
        return;
    };
    match json {
        serde_json::Value::Object(map) => {
            if rest.is_empty() {
                if create || map.contains_key(key) {
                    map.insert(key.clone(), new_value);
                }
            } else if let Some(child) = map.get_mut(key) {
                json_set(child, rest, new_value, create);
            }
        }
        serde_json::Value::Array(items) => {
            let Ok(idx) = key.parse::<i64>() else {
                return;
            };
            let len = items.len() as i64;
            let pos = if idx < 0 { len + idx } else { idx };
            if rest.is_empty() {
                if (0..len).contains(&pos) {
                    items[pos as usize] = new_value;
                } else if create {
                    if pos >= len {
                        items.push(new_value);
                    } else {
                        items.insert(0, new_value);
                    }
                }
            } else if (0..len).contains(&pos) {
                json_set(&mut items[pos as usize], rest, new_value, create);
            }
        }
        _ => {}
    }
}

/// `jsonb_insert`: `value` inserted at `path` (before the element there,
/// or after it with `after`); an existing object key is an error.
fn json_insert(
    json: &mut serde_json::Value,
    path: &[String],
    value: serde_json::Value,
    after: bool,
) -> Result<(), String> {
    let Some((key, rest)) = path.split_first() else {
        return Ok(());
    };
    match json {
        serde_json::Value::Object(map) => {
            if rest.is_empty() {
                if map.contains_key(key) {
                    return Err(
                        crate::error_fields::DbError::new("cannot replace existing key")
                            .hint("Try using the function jsonb_set to replace key value.")
                            .into_text(),
                    );
                }
                map.insert(key.clone(), value);
                Ok(())
            } else if let Some(child) = map.get_mut(key) {
                json_insert(child, rest, value, after)
            } else {
                Ok(())
            }
        }
        serde_json::Value::Array(items) => {
            let Ok(idx) = key.parse::<i64>() else {
                return Err(format!(
                    "path element at position {} is not an integer: \"{key}\"",
                    path.len() - rest.len()
                ));
            };
            let len = items.len() as i64;
            let pos = if idx < 0 { len + idx } else { idx };
            if rest.is_empty() {
                let at = if pos < 0 {
                    0
                } else if pos >= len {
                    len
                } else if after {
                    pos + 1
                } else {
                    pos
                };
                items.insert(at as usize, value);
                Ok(())
            } else if (0..len).contains(&pos) {
                json_insert(&mut items[pos as usize], rest, value, after)
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

/// The document without the member at `path`, as `#-` removes it.
fn json_delete_path(json: &mut serde_json::Value, path: &[String]) {
    let Some((key, rest)) = path.split_first() else {
        return;
    };
    match json {
        serde_json::Value::Object(map) => {
            if rest.is_empty() {
                map.shift_remove(key);
            } else if let Some(child) = map.get_mut(key) {
                json_delete_path(child, rest);
            }
        }
        serde_json::Value::Array(items) => {
            let Ok(idx) = key.parse::<i64>() else {
                return;
            };
            let len = items.len() as i64;
            let pos = if idx < 0 { len + idx } else { idx };
            if (0..len).contains(&pos) {
                if rest.is_empty() {
                    items.remove(pos as usize);
                } else {
                    json_delete_path(&mut items[pos as usize], rest);
                }
            }
        }
        _ => {}
    }
}

fn strip_nulls(json: &mut serde_json::Value, in_arrays: bool) {
    match json {
        serde_json::Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            map.values_mut().for_each(|v| strip_nulls(v, in_arrays));
        }
        serde_json::Value::Array(items) => {
            if in_arrays {
                items.retain(|v| !v.is_null());
            }
            items.iter_mut().for_each(|v| strip_nulls(v, in_arrays));
        }
        _ => {}
    }
}

/// A size function over relation `rel` (an OID or a name): `measure` gets
/// the bytes of a table's rows and of its indexes; an index measures as a
/// table of its entries, with no indexes. NULL when no relation has the OID.
fn relation_size(rel: &Value, measure: impl Fn(i64, i64) -> i64) -> Value {
    let found = session_env::with(|env| {
        let env = env?;
        let (catalog, (kv, read_ts)) = (env.catalog.as_ref()?, env.storage.as_ref()?);
        let oid = match rel {
            Value::Text(name) if name.trim().parse::<i64>().is_err() => {
                match crate::MemExecutor::relation_oid(catalog.as_ref(), name) {
                    Some(oid) => oid,
                    None => return Some(Err(format!("relation \"{name}\" does not exist"))),
                }
            }
            other => int(other)?,
        };
        if let Some(table) = crate::MemExecutor::relation_by_oid(catalog.as_ref(), oid) {
            let (rows, indexes) = stored_size(kv.as_ref(), *read_ts, &table);
            return Some(Ok(measure(rows, indexes)));
        }
        let (_, index) = crate::MemExecutor::index_by_oid(catalog.as_ref(), oid)?;
        let bytes = pages(range_bytes(
            kv.as_ref(),
            *read_ts,
            &format!("i:{}", index.id),
        ));
        Some(Ok(measure(bytes, 0)))
    });
    match found {
        Some(Ok(bytes)) => Value::Int(bytes),
        Some(Err(message)) => raise(message),
        None => Value::Null,
    }
}

/// The bytes a table's rows and its indexes take, each in whole 8 kB pages
/// as PostgreSQL measures them.
fn stored_size(
    kv: &dyn nodus_storage_api::KvEngine,
    read_ts: nodus_storage_api::Timestamp,
    table: &nodus_catalog::TableDescriptor,
) -> (i64, i64) {
    let rows = pages(range_bytes(kv, read_ts, &table.id.to_string()));
    let indexes = table
        .indexes
        .iter()
        .map(|index| pages(range_bytes(kv, read_ts, &format!("i:{}", index.id))))
        .sum();
    (rows, indexes)
}

/// The bytes of the keys and values stored under `prefix:`.
fn range_bytes(
    kv: &dyn nodus_storage_api::KvEngine,
    read_ts: nodus_storage_api::Timestamp,
    prefix: &str,
) -> i64 {
    let range = nodus_storage_api::KeyRange {
        start: bytes::Bytes::from(format!("{prefix}:")),
        end: bytes::Bytes::from(format!("{prefix};")),
    };
    kv.scan(range, read_ts)
        .map(|pairs| {
            pairs
                .filter_map(Result::ok)
                .map(|pair| (pair.key.len() + pair.value.len()) as i64)
                .sum()
        })
        .unwrap_or(0)
}

/// `bytes` rounded up to whole 8 kB pages.
fn pages(bytes: i64) -> i64 {
    (bytes + 8191) / 8192 * 8192
}

fn size_pretty(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["bytes", "kB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    // PostgreSQL switches units once a value reaches 10240 of the current unit.
    while value.abs() >= 10_240.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{} {}", value.round() as i64, UNITS[unit])
}

/// A version-7 UUID for `ms` milliseconds since the epoch: time-ordered, with
/// the remaining bits random (RFC 9562).
fn uuid_v7(ms: i64) -> uuid::Uuid {
    let random = uuid::Uuid::new_v4().into_bytes();
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&(ms as u64).to_be_bytes()[2..]);
    bytes[6..].copy_from_slice(&random[6..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

/// A uniformly distributed value in `[0, 1)`.

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Value {
        Value::Text(s.into())
    }

    #[test]
    fn strings_follow_postgres_semantics() {
        assert_eq!(
            call("SUBSTR", &[t("hello"), Value::Int(0), Value::Int(2)]),
            t("h")
        );
        assert_eq!(call("SUBSTR", &[t("hello"), Value::Int(2)]), t("ello"));
        assert_eq!(
            call("SPLIT_PART", &[t("a,b,c"), t(","), Value::Int(-1)]),
            t("c")
        );
        assert_eq!(call("LEFT", &[t("hello"), Value::Int(-1)]), t("hell"));
        assert_eq!(call("LPAD", &[t("toolong"), Value::Int(3)]), t("too"));
        assert_eq!(call("INITCAP", &[t("hello wORLD")]), t("Hello World"));
        assert_eq!(call("TRANSLATE", &[t("abc"), t("ab"), t("x")]), t("xc"));
        assert_eq!(
            call(
                "FORMAT",
                &[
                    t("%s|%I|%L|[%-4s]"),
                    t("x"),
                    t("My Col"),
                    t("it's"),
                    t("ab")
                ]
            ),
            t("x|\"My Col\"|'it''s'|[ab  ]")
        );
        assert_eq!(
            call(
                "REGEXP_REPLACE",
                &[t("a1b22c"), t("[0-9]+"), t("#"), t("g")]
            ),
            t("a#b#c")
        );
        assert_eq!(
            call("CONCAT_WS", &[t("-"), t("a"), Value::Null, t("b")]),
            t("a-b")
        );
        assert_eq!(call("UPPER", &[Value::Null]), Value::Null);
    }

    #[test]
    fn math_domain_errors_fail_the_statement() {
        crate::eval_error::reset();
        assert_eq!(call("SQRT", &[Value::Int(16)]), Value::Float(4.0));
        call("SQRT", &[Value::Int(-1)]);
        assert!(crate::eval_error::check().is_err());
        call("LN", &[Value::Int(0)]);
        assert!(crate::eval_error::check().is_err());
        assert_eq!(
            call("GCD", &[Value::Int(12), Value::Int(18)]),
            Value::Int(6)
        );
        assert_eq!(call("LCM", &[Value::Int(4), Value::Int(6)]), Value::Int(12));
        assert_eq!(call("CEIL", &[Value::Float(4.2)]), Value::Float(5.0));
        assert_eq!(
            call(
                "WIDTH_BUCKET",
                &[
                    Value::Float(5.35),
                    Value::Float(0.024),
                    Value::Float(10.06),
                    Value::Int(5)
                ]
            ),
            Value::Int(3)
        );
        assert!(crate::eval_error::check().is_ok());
    }

    #[test]
    fn unknown_functions_and_wrong_arity_fail() {
        crate::eval_error::reset();
        call("NO_SUCH_FUNCTION", &[Value::Int(1)]);
        assert!(crate::eval_error::check().is_err());
        call("UPPER", &[t("a"), t("b")]);
        assert!(crate::eval_error::check().is_err());
    }

    #[test]
    fn arrays_and_json() {
        let arr = Value::Array(vec![Value::Int(3), Value::Int(1), Value::Int(2)]);
        assert_eq!(
            call("ARRAY_SORT", std::slice::from_ref(&arr)),
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
        assert_eq!(
            call("ARRAY_POSITION", &[arr.clone(), Value::Int(2)]),
            Value::Int(3)
        );
        assert_eq!(call("CARDINALITY", &[arr]), Value::Int(3));
        assert_eq!(
            call("JSONB_BUILD_OBJECT", &[t("a"), Value::Int(1)]),
            Value::Jsonb(serde_json::json!({"a": 1}))
        );
        assert_eq!(call("JSONB_TYPEOF", &[t("[]")]), t("array"));
        let v7 = call("UUIDV7", &[]);
        assert_eq!(call("UUID_EXTRACT_VERSION", &[v7]), Value::Int(7));
    }
}
