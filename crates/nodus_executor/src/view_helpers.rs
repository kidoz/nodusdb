//! OID, type, and descriptor helpers shared by the synthesized catalog views.
use crate::{MemExecutor, Value, parse_object_name};
use anyhow::Result;
use chrono::Utc;
use nodus_catalog::ColumnDescriptor;

impl MemExecutor {
    /// Builds a column descriptor for a synthesized pg_catalog table.
    pub(crate) fn virtual_column(name: &str, data_type: &str) -> ColumnDescriptor {
        ColumnDescriptor {
            id: nodus_catalog::ColumnId::new(),
            name: name.into(),
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: nodus_catalog::DescriptorState::Public,
            data_type: data_type.into(),
            nullable: true,
        }
    }

    pub(crate) fn virtual_columns(columns: &[(&str, &str)]) -> Vec<ColumnDescriptor> {
        columns
            .iter()
            .map(|(name, data_type)| Self::virtual_column(name, data_type))
            .collect()
    }

    pub(crate) fn stable_oid(seed: &str, base: i64) -> i64 {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in seed.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        base + (hash % 1_000_000_000) as i64
    }

    pub(crate) fn database_oid(db_name: &str) -> i64 {
        Self::stable_oid(&format!("database:{db_name}"), 10_000)
    }

    pub(crate) fn schema_oid(db_name: &str, schema_name: &str) -> i64 {
        match schema_name {
            "pg_catalog" => 11,
            "public" => 2200,
            "information_schema" => 13_337,
            _ => Self::stable_oid(&format!("schema:{db_name}.{schema_name}"), 20_000),
        }
    }

    pub(crate) fn table_oid(db_name: &str, schema_name: &str, table_name: &str) -> i64 {
        Self::stable_oid(
            &format!("table:{db_name}.{schema_name}.{table_name}"),
            100_000,
        )
    }

    pub(crate) fn index_oid(
        db_name: &str,
        schema_name: &str,
        table_name: &str,
        index_name: &str,
    ) -> i64 {
        Self::stable_oid(
            &format!("index:{db_name}.{schema_name}.{table_name}.{index_name}"),
            2_000_000_000,
        )
    }

    pub(crate) fn constraint_oid(
        db_name: &str,
        schema_name: &str,
        table_name: &str,
        constraint_name: &str,
    ) -> i64 {
        Self::stable_oid(
            &format!("constraint:{db_name}.{schema_name}.{table_name}.{constraint_name}"),
            1_000_000_000,
        )
    }

    pub(crate) fn pg_type_oid(data_type: &str) -> i64 {
        let normalized = data_type
            .trim()
            .trim_matches('"')
            .to_ascii_uppercase()
            .replace("CHARACTER VARYING", "VARCHAR")
            .replace("DOUBLE PRECISION", "DOUBLE")
            .replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ")
            .replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP");
        let is_array = normalized.ends_with("[]");
        let base = normalized
            .trim_end_matches("[]")
            .split('(')
            .next()
            .unwrap_or("TEXT")
            .trim();
        if is_array {
            return match base {
                "BOOL" | "BOOLEAN" => 1000,
                "BYTEA" => 1001,
                "PG_CHAR" => 1002,
                "CHAR" | "CHARACTER" | "BPCHAR" => 1014,
                "INT2" | "SMALLINT" => 1005,
                "INT4" | "INT" | "INTEGER" | "SERIAL" => 1007,
                "INT8" | "BIGINT" => 1016,
                "TEXT" => 1009,
                "VARCHAR" => 1015,
                "OID" => 1028,
                "FLOAT4" | "REAL" => 1021,
                "FLOAT8" | "FLOAT" | "DOUBLE" => 1022,
                "NUMERIC" | "DECIMAL" => 1231,
                "DATE" => 1182,
                "TIME" => 1183,
                "TIMESTAMP" => 1115,
                "TIMESTAMPTZ" => 1185,
                "UUID" => 2951,
                "JSON" => 199,
                "JSONB" => 3807,
                "REGTYPE" => 2211,
                _ => 1009,
            };
        }
        match base {
            "BOOL" | "BOOLEAN" => 16,
            "BYTEA" => 17,
            "PG_CHAR" => 18,
            "CHAR" | "CHARACTER" | "BPCHAR" => 1042,
            "INT2" | "SMALLINT" => 21,
            "INT4" | "INT" | "INTEGER" | "SERIAL" => 23,
            "INT8" | "BIGINT" => 20,
            "TEXT" => 25,
            "OID" => 26,
            "FLOAT4" | "REAL" => 700,
            "FLOAT8" | "FLOAT" | "DOUBLE" => 701,
            "VARCHAR" => 1043,
            "DATE" => 1082,
            "TIME" => 1083,
            "TIMESTAMP" => 1114,
            "TIMESTAMPTZ" => 1184,
            "NUMERIC" | "DECIMAL" => 1700,
            "UUID" => 2950,
            "JSON" => 114,
            "JSONB" => 3802,
            "NAME" => 19,
            "REGPROC" => 24,
            "REGOPER" => 2203,
            "REGOPERATOR" => 2204,
            "REGCLASS" => 2205,
            "REGTYPE" => 2206,
            "REGROLE" => 4096,
            "REGNAMESPACE" => 4089,
            "REGCONFIG" => 3734,
            "REGDICTIONARY" => 3769,
            _ => 25,
        }
    }

    pub(crate) fn pg_type_name(data_type: &str) -> String {
        match Self::pg_type_oid(data_type) {
            16 => "bool",
            17 => "bytea",
            18 => "char",
            19 => "name",
            20 => "int8",
            21 => "int2",
            23 => "int4",
            25 => "text",
            26 => "oid",
            700 => "float4",
            701 => "float8",
            1042 => "bpchar",
            1043 => "varchar",
            1082 => "date",
            1083 => "time",
            1114 => "timestamp",
            1184 => "timestamptz",
            1700 => "numeric",
            2206 => "regtype",
            2950 => "uuid",
            3802 => "jsonb",
            _ => "text",
        }
        .to_string()
    }

    pub(crate) fn pg_type_length(data_type: &str) -> i64 {
        match Self::pg_type_oid(data_type) {
            16 => 1,
            20 | 701 | 1083 | 1114 | 1184 => 8,
            18 => 1,
            21 => 2,
            23 | 26 | 700 | 1082 | 2206 => 4,
            2950 => 16,
            _ => -1,
        }
    }

    pub(crate) fn is_virtual_schema(schema_name: &str) -> bool {
        schema_name.eq_ignore_ascii_case("pg_catalog")
            || schema_name.eq_ignore_ascii_case("information_schema")
    }

    pub(crate) fn is_pg_catalog_virtual_table_name(table_name: &str) -> bool {
        matches!(
            table_name.to_ascii_lowercase().as_str(),
            "pg_database"
                | "pg_namespace"
                | "pg_class"
                | "pg_attribute"
                | "pg_index"
                | "pg_constraint"
                | "pg_type"
                | "pg_proc"
                | "pg_range"
                | "pg_settings"
                | "pg_roles"
                | "pg_user"
                | "pg_tables"
                | "pg_indexes"
                | "pg_attrdef"
                | "pg_description"
                | "pg_shdescription"
                | "pg_enum"
                | "pg_collation"
                | "pg_am"
                | "pg_operator"
                | "pg_cast"
                | "pg_locks"
        )
    }

    pub(crate) fn schema_name_by_id(
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        schema_id: nodus_catalog::SchemaId,
    ) -> String {
        schemas
            .iter()
            .find(|schema| schema.id == schema_id)
            .map(|schema| schema.name.clone())
            .unwrap_or_else(|| {
                let _ = db_name;
                "public".to_string()
            })
    }

    pub(crate) fn returning_types(
        columns: &[ColumnDescriptor],
        returning: &[String],
    ) -> Vec<String> {
        returning
            .iter()
            .map(|name| {
                columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(name))
                    .map(|column| column.data_type.clone())
                    .unwrap_or_else(|| "VARCHAR".to_string())
            })
            .collect()
    }
}
