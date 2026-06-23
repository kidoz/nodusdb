//! Synthesized `pg_catalog` and `information_schema` virtual tables, plus the
//! stable-OID and pg_type metadata helpers that back driver introspection.
//!
//! These methods build column descriptors and row sets from catalog data passed
//! in by the executor; they read no `MemExecutor` field state directly.

use crate::{MemExecutor, Value, parse_object_name};
use anyhow::Result;
use chrono::Utc;
use nodus_catalog::ColumnDescriptor;

impl MemExecutor {
    /// Builds a column descriptor for a synthesized pg_catalog table.
    fn virtual_column(name: &str, data_type: &str) -> ColumnDescriptor {
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

    fn virtual_columns(columns: &[(&str, &str)]) -> Vec<ColumnDescriptor> {
        columns
            .iter()
            .map(|(name, data_type)| Self::virtual_column(name, data_type))
            .collect()
    }

    fn stable_oid(seed: &str, base: i64) -> i64 {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in seed.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        base + (hash % 1_000_000_000) as i64
    }

    fn database_oid(db_name: &str) -> i64 {
        Self::stable_oid(&format!("database:{db_name}"), 10_000)
    }

    fn schema_oid(db_name: &str, schema_name: &str) -> i64 {
        match schema_name {
            "pg_catalog" => 11,
            "public" => 2200,
            "information_schema" => 13_337,
            _ => Self::stable_oid(&format!("schema:{db_name}.{schema_name}"), 20_000),
        }
    }

    fn table_oid(db_name: &str, schema_name: &str, table_name: &str) -> i64 {
        Self::stable_oid(
            &format!("table:{db_name}.{schema_name}.{table_name}"),
            100_000,
        )
    }

    fn index_oid(db_name: &str, schema_name: &str, table_name: &str, index_name: &str) -> i64 {
        Self::stable_oid(
            &format!("index:{db_name}.{schema_name}.{table_name}.{index_name}"),
            2_000_000_000,
        )
    }

    fn constraint_oid(
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

    fn pg_type_oid(data_type: &str) -> i64 {
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

    fn pg_type_name(data_type: &str) -> String {
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

    fn pg_type_length(data_type: &str) -> i64 {
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

    fn schema_name_by_id(
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

    fn pg_catalog_virtual_table(
        &self,
        db_name: &str,
        table_only: &str,
    ) -> Result<Option<(Vec<ColumnDescriptor>, Vec<Vec<Value>>)>> {
        let schemas = self
            .catalog_reader
            .list_schemas(db_name)
            .unwrap_or_default();
        let tables = self
            .catalog_reader
            .list_all_tables(db_name)
            .unwrap_or_default();
        let table = table_only.to_ascii_lowercase();
        let result = match table.as_str() {
            "pg_database" => {
                let cols = Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("datname", "NAME"),
                    ("datdba", "OID"),
                    ("encoding", "INT"),
                    ("datlocprovider", "PG_CHAR"),
                    ("datistemplate", "BOOL"),
                    ("datallowconn", "BOOL"),
                    ("datconnlimit", "INT"),
                    ("datcollate", "TEXT"),
                    ("datctype", "TEXT"),
                    ("daticulocale", "TEXT"),
                    ("datcollversion", "TEXT"),
                    ("datacl", "TEXT[]"),
                ]);
                let rows = vec![vec![
                    Value::Int(Self::database_oid(db_name)),
                    Value::Text(db_name.to_string()),
                    Value::Int(10),
                    Value::Int(6),
                    Value::Text("c".into()),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Int(-1),
                    Value::Text("C".into()),
                    Value::Text("C".into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]];
                Some((cols, rows))
            }
            "pg_namespace" => {
                let cols = Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("nspname", "NAME"),
                    ("nspowner", "OID"),
                    ("nspacl", "TEXT[]"),
                ]);
                let mut rows = vec![
                    vec![
                        Value::Int(Self::schema_oid(db_name, "pg_catalog")),
                        Value::Text("pg_catalog".into()),
                        Value::Int(10),
                        Value::Null,
                    ],
                    vec![
                        Value::Int(Self::schema_oid(db_name, "information_schema")),
                        Value::Text("information_schema".into()),
                        Value::Int(10),
                        Value::Null,
                    ],
                ];
                rows.extend(schemas.iter().map(|schema| {
                    vec![
                        Value::Int(Self::schema_oid(db_name, &schema.name)),
                        Value::Text(schema.name.clone()),
                        Value::Int(10),
                        Value::Null,
                    ]
                }));
                Some((cols, rows))
            }
            "pg_class" => {
                let cols = Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("relname", "NAME"),
                    ("relnamespace", "OID"),
                    ("reltype", "OID"),
                    ("reloftype", "OID"),
                    ("relowner", "OID"),
                    ("relam", "OID"),
                    ("relfilenode", "OID"),
                    ("reltablespace", "OID"),
                    ("relpages", "INT"),
                    ("reltuples", "FLOAT4"),
                    ("relallvisible", "INT"),
                    ("reltoastrelid", "OID"),
                    ("relhasindex", "BOOL"),
                    ("relisshared", "BOOL"),
                    ("relpersistence", "PG_CHAR"),
                    ("relkind", "PG_CHAR"),
                    ("relnatts", "INT"),
                    ("relchecks", "INT"),
                    ("relhasrules", "BOOL"),
                    ("relhastriggers", "BOOL"),
                    ("relhassubclass", "BOOL"),
                    ("relrowsecurity", "BOOL"),
                    ("relforcerowsecurity", "BOOL"),
                    ("relispopulated", "BOOL"),
                    ("relreplident", "PG_CHAR"),
                    ("relispartition", "BOOL"),
                    ("relrewrite", "OID"),
                    ("relfrozenxid", "INT"),
                    ("relminmxid", "INT"),
                    ("relacl", "TEXT[]"),
                    ("reloptions", "TEXT[]"),
                    ("relpartbound", "TEXT"),
                ]);
                let mut rows = Vec::new();
                for table in &tables {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    let oid = Self::table_oid(db_name, &schema_name, &table.name);
                    rows.push(vec![
                        Value::Int(oid),
                        Value::Text(table.name.clone()),
                        Value::Int(Self::schema_oid(db_name, &schema_name)),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(10),
                        Value::Int(0),
                        Value::Int(oid),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Float(0.0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Bool(!table.indexes.is_empty()),
                        Value::Bool(false),
                        Value::Text("p".into()),
                        Value::Text(if table.view_query.is_some() { "v" } else { "r" }.into()),
                        Value::Int(table.columns.len() as i64),
                        Value::Int(
                            table
                                .constraints
                                .iter()
                                .filter(|constraint| {
                                    matches!(
                                        constraint,
                                        nodus_catalog::TableConstraint::Check { .. }
                                    )
                                })
                                .count() as i64,
                        ),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Text("d".into()),
                        Value::Bool(false),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ]);
                    for index in &table.indexes {
                        let index_oid =
                            Self::index_oid(db_name, &schema_name, &table.name, &index.name);
                        rows.push(vec![
                            Value::Int(index_oid),
                            Value::Text(index.name.clone()),
                            Value::Int(Self::schema_oid(db_name, &schema_name)),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(10),
                            Value::Int(403),
                            Value::Int(index_oid),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Float(0.0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Text("p".into()),
                            Value::Text("i".into()),
                            Value::Int(index.key_columns.len() as i64),
                            Value::Int(0),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Text("n".into()),
                            Value::Bool(false),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                        ]);
                    }
                }
                Some((cols, rows))
            }
            "pg_attribute" => {
                let cols = Self::virtual_columns(&[
                    ("attrelid", "OID"),
                    ("attname", "NAME"),
                    ("atttypid", "OID"),
                    ("attstattarget", "INT"),
                    ("attlen", "INT"),
                    ("attnum", "INT"),
                    ("attndims", "INT"),
                    ("attcacheoff", "INT"),
                    ("atttypmod", "INT"),
                    ("attbyval", "BOOL"),
                    ("attstorage", "PG_CHAR"),
                    ("attalign", "PG_CHAR"),
                    ("attnotnull", "BOOL"),
                    ("atthasdef", "BOOL"),
                    ("atthasmissing", "BOOL"),
                    ("attidentity", "PG_CHAR"),
                    ("attgenerated", "PG_CHAR"),
                    ("attisdropped", "BOOL"),
                    ("attislocal", "BOOL"),
                    ("attinhcount", "INT"),
                    ("attcollation", "OID"),
                    ("attacl", "TEXT[]"),
                    ("attoptions", "TEXT[]"),
                    ("attfdwoptions", "TEXT[]"),
                    ("attmissingval", "TEXT"),
                ]);
                let mut rows = Vec::new();
                for table in &tables {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    let relid = Self::table_oid(db_name, &schema_name, &table.name);
                    for (idx, column) in table.columns.iter().enumerate() {
                        let type_oid = Self::pg_type_oid(&column.data_type);
                        rows.push(vec![
                            Value::Int(relid),
                            Value::Text(column.name.clone()),
                            Value::Int(type_oid),
                            Value::Int(-1),
                            Value::Int(Self::pg_type_length(&column.data_type)),
                            Value::Int((idx + 1) as i64),
                            Value::Int(if column.data_type.ends_with("[]") {
                                1
                            } else {
                                0
                            }),
                            Value::Int(-1),
                            Value::Int(-1),
                            Value::Bool(matches!(type_oid, 16 | 20 | 21 | 23 | 26 | 700 | 701)),
                            Value::Text("x".into()),
                            Value::Text("i".into()),
                            Value::Bool(!column.nullable),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Int(0),
                            Value::Int(100),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                        ]);
                    }
                }
                Some((cols, rows))
            }
            "pg_index" => {
                let cols = Self::virtual_columns(&[
                    ("indexrelid", "OID"),
                    ("indrelid", "OID"),
                    ("indnatts", "INT"),
                    ("indnkeyatts", "INT"),
                    ("indisunique", "BOOL"),
                    ("indisprimary", "BOOL"),
                    ("indisexclusion", "BOOL"),
                    ("indimmediate", "BOOL"),
                    ("indisclustered", "BOOL"),
                    ("indisvalid", "BOOL"),
                    ("indcheckxmin", "BOOL"),
                    ("indisready", "BOOL"),
                    ("indislive", "BOOL"),
                    ("indisreplident", "BOOL"),
                    ("indkey", "TEXT"),
                    ("indcollation", "TEXT"),
                    ("indclass", "TEXT"),
                    ("indoption", "TEXT"),
                    ("indexprs", "TEXT"),
                    ("indpred", "TEXT"),
                ]);
                let mut rows = Vec::new();
                for table in &tables {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    let relid = Self::table_oid(db_name, &schema_name, &table.name);
                    for index in &table.indexes {
                        let keys = index
                            .key_columns
                            .iter()
                            .filter_map(|key| {
                                table
                                    .columns
                                    .iter()
                                    .position(|column| column.id == key.column_id)
                                    .map(|pos| (pos + 1).to_string())
                            })
                            .collect::<Vec<_>>()
                            .join(" ");
                        rows.push(vec![
                            Value::Int(Self::index_oid(
                                db_name,
                                &schema_name,
                                &table.name,
                                &index.name,
                            )),
                            Value::Int(relid),
                            Value::Int(index.key_columns.len() as i64),
                            Value::Int(index.key_columns.len() as i64),
                            Value::Bool(index.unique),
                            Value::Bool(matches!(
                                index.index_type,
                                nodus_catalog::IndexType::Primary
                            )),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Bool(true),
                            Value::Bool(false),
                            Value::Text(keys),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            Value::Null,
                            Value::Null,
                        ]);
                    }
                }
                Some((cols, rows))
            }
            "pg_constraint" => {
                let cols = Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("conname", "NAME"),
                    ("connamespace", "OID"),
                    ("contype", "PG_CHAR"),
                    ("condeferrable", "BOOL"),
                    ("condeferred", "BOOL"),
                    ("convalidated", "BOOL"),
                    ("conrelid", "OID"),
                    ("contypid", "OID"),
                    ("conindid", "OID"),
                    ("conparentid", "OID"),
                    ("confrelid", "OID"),
                    ("confupdtype", "PG_CHAR"),
                    ("confdeltype", "PG_CHAR"),
                    ("confmatchtype", "PG_CHAR"),
                    ("conislocal", "BOOL"),
                    ("coninhcount", "INT"),
                    ("connoinherit", "BOOL"),
                    ("conkey", "INT[]"),
                    ("confkey", "INT[]"),
                    ("conpfeqop", "OID[]"),
                    ("conppeqop", "OID[]"),
                    ("conffeqop", "OID[]"),
                    ("confdelsetcols", "INT[]"),
                    ("conexclop", "OID[]"),
                    ("conbin", "TEXT"),
                ]);
                Some((cols, self.pg_constraint_rows(db_name, &schemas, &tables)))
            }
            "pg_type" => Some(self.pg_type_virtual_table(db_name)),
            "pg_proc" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("proname", "NAME"),
                    ("pronamespace", "OID"),
                    ("proowner", "OID"),
                    ("prolang", "OID"),
                    ("procost", "FLOAT4"),
                    ("prorows", "FLOAT4"),
                    ("provariadic", "OID"),
                    ("prosupport", "REGPROC"),
                    ("prokind", "PG_CHAR"),
                    ("prosecdef", "BOOL"),
                    ("proleakproof", "BOOL"),
                    ("proisstrict", "BOOL"),
                    ("proretset", "BOOL"),
                    ("provolatile", "PG_CHAR"),
                    ("proparallel", "PG_CHAR"),
                    ("pronargs", "INT"),
                    ("pronargdefaults", "INT"),
                    ("prorettype", "OID"),
                    ("proargtypes", "OID[]"),
                    ("proallargtypes", "OID[]"),
                    ("proargmodes", "PG_CHAR[]"),
                    ("proargnames", "TEXT[]"),
                    ("proargdefaults", "TEXT"),
                    ("protrftypes", "OID[]"),
                    ("prosrc", "TEXT"),
                    ("probin", "TEXT"),
                    ("prosqlbody", "TEXT"),
                    ("proconfig", "TEXT[]"),
                    ("proacl", "TEXT[]"),
                ]),
                vec![vec![
                    Value::Int(750),
                    Value::Text("array_recv".into()),
                    Value::Int(Self::schema_oid(db_name, "pg_catalog")),
                    Value::Int(10),
                    Value::Int(12),
                    Value::Float(1.0),
                    Value::Float(0.0),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Text("f".into()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Text("i".into()),
                    Value::Text("s".into()),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Array(Vec::new()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text("array_recv".into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]],
            )),
            "pg_range" => Some((
                Self::virtual_columns(&[
                    ("rngtypid", "OID"),
                    ("rngsubtype", "OID"),
                    ("rngmultitypid", "OID"),
                    ("rngcollation", "OID"),
                    ("rngsubopc", "OID"),
                    ("rngcanonical", "REGPROC"),
                    ("rngsubdiff", "REGPROC"),
                ]),
                Vec::new(),
            )),
            "pg_settings" => Some(self.pg_settings_virtual_table()),
            "pg_roles" => Some(self.pg_roles_virtual_table()),
            "pg_user" => Some(self.pg_user_virtual_table()),
            "pg_tables" => Some(self.pg_tables_virtual_table(db_name, &schemas, &tables)),
            "pg_indexes" => Some(self.pg_indexes_virtual_table(db_name, &schemas, &tables)),
            "pg_attrdef" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("adrelid", "OID"),
                    ("adnum", "INT"),
                    ("adbin", "TEXT"),
                ]),
                Vec::new(),
            )),
            "pg_description" => Some((
                Self::virtual_columns(&[
                    ("objoid", "OID"),
                    ("classoid", "OID"),
                    ("objsubid", "INT"),
                    ("description", "TEXT"),
                ]),
                Vec::new(),
            )),
            // Shared-object comments. NodusDB has no COMMENT ON support, so this
            // is synthesized empty like pg_description; pgjdbc/DataGrip join it
            // during introspection and tolerate zero rows.
            "pg_shdescription" => Some((
                Self::virtual_columns(&[
                    ("objoid", "OID"),
                    ("classoid", "OID"),
                    ("description", "TEXT"),
                ]),
                Vec::new(),
            )),
            "pg_enum" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("enumtypid", "OID"),
                    ("enumsortorder", "FLOAT4"),
                    ("enumlabel", "NAME"),
                ]),
                Vec::new(),
            )),
            "pg_collation" => Some(self.pg_collation_virtual_table(db_name)),
            "pg_am" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("amname", "NAME"),
                    ("amhandler", "REGPROC"),
                    ("amtype", "PG_CHAR"),
                ]),
                vec![vec![
                    Value::Int(403),
                    Value::Text("btree".into()),
                    Value::Text("-".into()),
                    Value::Text("i".into()),
                ]],
            )),
            "pg_operator" => Some(self.pg_operator_virtual_table(db_name)),
            "pg_cast" => Some(self.pg_cast_virtual_table()),
            "pg_locks" => Some((
                Self::virtual_columns(&[
                    ("locktype", "TEXT"),
                    ("database", "OID"),
                    ("relation", "OID"),
                    ("transactionid", "INT8"),
                    ("pid", "INT"),
                    ("mode", "TEXT"),
                    ("granted", "BOOL"),
                ]),
                Vec::new(),
            )),
            _ => None,
        };
        Ok(result)
    }

    fn pg_type_virtual_table(&self, db_name: &str) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("typname", "NAME"),
            ("typnamespace", "OID"),
            ("typowner", "OID"),
            ("typlen", "INT"),
            ("typbyval", "BOOL"),
            ("typtype", "PG_CHAR"),
            ("typcategory", "PG_CHAR"),
            ("typispreferred", "BOOL"),
            ("typisdefined", "BOOL"),
            ("typdelim", "PG_CHAR"),
            ("typrelid", "OID"),
            ("typelem", "OID"),
            ("typarray", "OID"),
            ("typinput", "REGPROC"),
            ("typoutput", "REGPROC"),
            ("typreceive", "REGPROC"),
            ("typsend", "REGPROC"),
            ("typmodin", "REGPROC"),
            ("typmodout", "REGPROC"),
            ("typanalyze", "REGPROC"),
            ("typalign", "PG_CHAR"),
            ("typstorage", "PG_CHAR"),
            ("typnotnull", "BOOL"),
            ("typbasetype", "OID"),
            ("typtypmod", "INT"),
            ("typndims", "INT"),
            ("typcollation", "OID"),
            ("typdefaultbin", "TEXT"),
            ("typdefault", "TEXT"),
            ("typacl", "TEXT[]"),
        ]);
        let pg_ns = Self::schema_oid(db_name, "pg_catalog");
        let type_specs = [
            (16, "bool", 1, 1000, "_bool"),
            (17, "bytea", -1, 1001, "_bytea"),
            (18, "char", 1, 1002, "_char"),
            (19, "name", 64, 1003, "_name"),
            (20, "int8", 8, 1016, "_int8"),
            (21, "int2", 2, 1005, "_int2"),
            (23, "int4", 4, 1007, "_int4"),
            (25, "text", -1, 1009, "_text"),
            (26, "oid", 4, 1028, "_oid"),
            (700, "float4", 4, 1021, "_float4"),
            (701, "float8", 8, 1022, "_float8"),
            (1042, "bpchar", -1, 1014, "_bpchar"),
            (1043, "varchar", -1, 1015, "_varchar"),
            (1082, "date", 4, 1182, "_date"),
            (1083, "time", 8, 1183, "_time"),
            (1114, "timestamp", 8, 1115, "_timestamp"),
            (1184, "timestamptz", 8, 1185, "_timestamptz"),
            (1700, "numeric", -1, 1231, "_numeric"),
            (2206, "regtype", 4, 2211, "_regtype"),
            (2950, "uuid", 16, 2951, "_uuid"),
            (3802, "jsonb", -1, 3807, "_jsonb"),
        ];
        let mut rows = Vec::new();
        for (oid, name, len, array, _) in type_specs {
            rows.push(vec![
                Value::Int(oid),
                Value::Text(name.into()),
                Value::Int(pg_ns),
                Value::Int(10),
                Value::Int(len),
                Value::Bool(matches!(len, 1 | 2 | 4 | 8)),
                Value::Text("b".into()),
                Value::Text("U".into()),
                Value::Bool(false),
                Value::Bool(true),
                Value::Text(",".into()),
                Value::Int(0),
                Value::Int(0),
                Value::Int(array),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Text("i".into()),
                Value::Text("p".into()),
                Value::Bool(false),
                Value::Int(0),
                Value::Int(-1),
                Value::Int(0),
                Value::Int(100),
                Value::Null,
                Value::Null,
                Value::Null,
            ]);
        }
        for (elem_oid, _, _, array_oid, array_name) in type_specs {
            rows.push(vec![
                Value::Int(array_oid),
                Value::Text(array_name.into()),
                Value::Int(pg_ns),
                Value::Int(10),
                Value::Int(-1),
                Value::Bool(false),
                Value::Text("a".into()),
                Value::Text("A".into()),
                Value::Bool(false),
                Value::Bool(true),
                Value::Text(",".into()),
                Value::Int(0),
                Value::Int(elem_oid),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(750),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Text("i".into()),
                Value::Text("x".into()),
                Value::Bool(false),
                Value::Int(0),
                Value::Int(-1),
                Value::Int(1),
                Value::Int(100),
                Value::Null,
                Value::Null,
                Value::Null,
            ]);
        }
        (cols, rows)
    }

    fn pg_settings_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("name", "TEXT"),
            ("setting", "TEXT"),
            ("unit", "TEXT"),
            ("category", "TEXT"),
            ("short_desc", "TEXT"),
            ("extra_desc", "TEXT"),
            ("context", "TEXT"),
            ("vartype", "TEXT"),
            ("source", "TEXT"),
            ("min_val", "TEXT"),
            ("max_val", "TEXT"),
            ("enumvals", "TEXT[]"),
            ("boot_val", "TEXT"),
            ("reset_val", "TEXT"),
            ("sourcefile", "TEXT"),
            ("sourceline", "INT"),
            ("pending_restart", "BOOL"),
        ]);
        let settings = [
            ("application_name", "", "string"),
            ("client_encoding", "UTF8", "string"),
            ("DateStyle", "ISO, MDY", "string"),
            ("integer_datetimes", "on", "bool"),
            ("IntervalStyle", "postgres", "string"),
            ("is_superuser", "on", "bool"),
            ("server_encoding", "UTF8", "string"),
            ("server_version", "16.0", "string"),
            ("server_version_num", "160000", "integer"),
            ("standard_conforming_strings", "on", "bool"),
            ("statement_timeout", "0", "integer"),
            ("TimeZone", "UTC", "string"),
        ];
        let rows = settings
            .into_iter()
            .map(|(name, setting, vartype)| {
                vec![
                    Value::Text(name.into()),
                    Value::Text(setting.into()),
                    Value::Null,
                    Value::Text("Client Connection Defaults".into()),
                    Value::Text(name.into()),
                    Value::Null,
                    Value::Text("user".into()),
                    Value::Text(vartype.into()),
                    Value::Text("default".into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text(setting.into()),
                    Value::Text(setting.into()),
                    Value::Null,
                    Value::Null,
                    Value::Bool(false),
                ]
            })
            .collect();
        (cols, rows)
    }

    fn pg_roles_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("rolname", "NAME"),
            ("rolsuper", "BOOL"),
            ("rolinherit", "BOOL"),
            ("rolcreaterole", "BOOL"),
            ("rolcreatedb", "BOOL"),
            ("rolcanlogin", "BOOL"),
            ("rolreplication", "BOOL"),
            ("rolconnlimit", "INT"),
            ("rolpassword", "TEXT"),
            ("rolvaliduntil", "TIMESTAMPTZ"),
            ("rolbypassrls", "BOOL"),
            ("rolconfig", "TEXT[]"),
        ]);
        let rows = vec![vec![
            Value::Int(10),
            Value::Text("nodus".into()),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(-1),
            Value::Text("********".into()),
            Value::Null,
            Value::Bool(false),
            Value::Null,
        ]];
        (cols, rows)
    }

    fn pg_user_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("usename", "NAME"),
            ("usesysid", "OID"),
            ("usecreatedb", "BOOL"),
            ("usesuper", "BOOL"),
            ("userepl", "BOOL"),
            ("usebypassrls", "BOOL"),
            ("passwd", "TEXT"),
            ("valuntil", "TIMESTAMPTZ"),
            ("useconfig", "TEXT[]"),
        ]);
        let rows = vec![vec![
            Value::Text("nodus".into()),
            Value::Int(10),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
            Value::Text("********".into()),
            Value::Null,
            Value::Null,
        ]];
        (cols, rows)
    }

    fn pg_tables_virtual_table(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("schemaname", "NAME"),
            ("tablename", "NAME"),
            ("tableowner", "NAME"),
            ("tablespace", "NAME"),
            ("hasindexes", "BOOL"),
            ("hasrules", "BOOL"),
            ("hastriggers", "BOOL"),
            ("rowsecurity", "BOOL"),
        ]);
        let rows = tables
            .iter()
            .filter(|table| table.view_query.is_none())
            .map(|table| {
                vec![
                    Value::Text(Self::schema_name_by_id(db_name, schemas, table.schema_id)),
                    Value::Text(table.name.clone()),
                    Value::Text("nodus".into()),
                    Value::Null,
                    Value::Bool(!table.indexes.is_empty()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                ]
            })
            .collect();
        (cols, rows)
    }

    fn pg_indexes_virtual_table(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("schemaname", "NAME"),
            ("tablename", "NAME"),
            ("indexname", "NAME"),
            ("tablespace", "NAME"),
            ("indexdef", "TEXT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for index in &table.indexes {
                let key_cols = index
                    .key_columns
                    .iter()
                    .filter_map(|key| {
                        table
                            .columns
                            .iter()
                            .find(|column| column.id == key.column_id)
                            .map(|column| column.name.clone())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                rows.push(vec![
                    Value::Text(schema_name.clone()),
                    Value::Text(table.name.clone()),
                    Value::Text(index.name.clone()),
                    Value::Null,
                    Value::Text(format!(
                        "CREATE {}INDEX {} ON {}.{} ({})",
                        if index.unique { "UNIQUE " } else { "" },
                        index.name,
                        schema_name,
                        table.name,
                        key_cols
                    )),
                ]);
            }
        }
        (cols, rows)
    }

    fn pg_constraint_rows(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            let relid = Self::table_oid(db_name, &schema_name, &table.name);
            let namespace = Self::schema_oid(db_name, &schema_name);
            for index in &table.indexes {
                if !index.unique {
                    continue;
                }
                let conname = index.name.clone();
                let key_nums = index
                    .key_columns
                    .iter()
                    .filter_map(|key| {
                        table
                            .columns
                            .iter()
                            .position(|column| column.id == key.column_id)
                            .map(|pos| Value::Int((pos + 1) as i64))
                    })
                    .collect::<Vec<_>>();
                rows.push(vec![
                    Value::Int(Self::constraint_oid(
                        db_name,
                        &schema_name,
                        &table.name,
                        &conname,
                    )),
                    Value::Text(conname.clone()),
                    Value::Int(namespace),
                    Value::Text(
                        if matches!(index.index_type, nodus_catalog::IndexType::Primary) {
                            "p"
                        } else {
                            "u"
                        }
                        .into(),
                    ),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Int(relid),
                    Value::Int(0),
                    Value::Int(Self::index_oid(
                        db_name,
                        &schema_name,
                        &table.name,
                        &index.name,
                    )),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Text("a".into()),
                    Value::Text("a".into()),
                    Value::Text("s".into()),
                    Value::Bool(true),
                    Value::Int(0),
                    Value::Bool(false),
                    Value::Array(key_nums),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]);
            }
            for (idx, constraint) in table.constraints.iter().enumerate() {
                match constraint {
                    nodus_catalog::TableConstraint::Check { name, expr } => {
                        let conname = name
                            .clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table.name, idx + 1));
                        rows.push(vec![
                            Value::Int(Self::constraint_oid(
                                db_name,
                                &schema_name,
                                &table.name,
                                &conname,
                            )),
                            Value::Text(conname),
                            Value::Int(namespace),
                            Value::Text("c".into()),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Int(relid),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Text("a".into()),
                            Value::Text("a".into()),
                            Value::Text("s".into()),
                            Value::Bool(true),
                            Value::Int(0),
                            Value::Bool(false),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Text(expr.clone()),
                        ]);
                    }
                    nodus_catalog::TableConstraint::ForeignKey {
                        name,
                        columns,
                        foreign_table,
                        referred_columns,
                    } => {
                        let conname = name.clone().unwrap_or_else(|| {
                            format!("{}_{}_fkey", table.name, columns.join("_"))
                        });
                        let (ref_db, ref_schema, ref_table) = parse_object_name(foreign_table)
                            .unwrap_or((db_name, "public", foreign_table));
                        let confrelid = Self::table_oid(ref_db, ref_schema, ref_table);
                        let conkey = columns
                            .iter()
                            .filter_map(|name| {
                                table
                                    .columns
                                    .iter()
                                    .position(|column| column.name == *name)
                                    .map(|pos| Value::Int((pos + 1) as i64))
                            })
                            .collect::<Vec<_>>();
                        let confkey = referred_columns
                            .iter()
                            .enumerate()
                            .map(|(pos, _)| Value::Int((pos + 1) as i64))
                            .collect::<Vec<_>>();
                        rows.push(vec![
                            Value::Int(Self::constraint_oid(
                                db_name,
                                &schema_name,
                                &table.name,
                                &conname,
                            )),
                            Value::Text(conname),
                            Value::Int(namespace),
                            Value::Text("f".into()),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Int(relid),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(confrelid),
                            Value::Text("a".into()),
                            Value::Text("a".into()),
                            Value::Text("s".into()),
                            Value::Bool(true),
                            Value::Int(0),
                            Value::Bool(false),
                            Value::Array(conkey),
                            Value::Array(confkey),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                        ]);
                    }
                }
            }
        }
        rows
    }

    fn pg_collation_virtual_table(
        &self,
        db_name: &str,
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("collname", "NAME"),
            ("collnamespace", "OID"),
            ("collowner", "OID"),
            ("collprovider", "PG_CHAR"),
            ("collisdeterministic", "BOOL"),
            ("collencoding", "INT"),
            ("collcollate", "TEXT"),
            ("collctype", "TEXT"),
            ("colliculocale", "TEXT"),
            ("collversion", "TEXT"),
        ]);
        let pg_ns = Self::schema_oid(db_name, "pg_catalog");
        let rows = vec![
            vec![
                Value::Int(100),
                Value::Text("default".into()),
                Value::Int(pg_ns),
                Value::Int(10),
                Value::Text("d".into()),
                Value::Bool(true),
                Value::Int(-1),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Int(950),
                Value::Text("C".into()),
                Value::Int(pg_ns),
                Value::Int(10),
                Value::Text("c".into()),
                Value::Bool(true),
                Value::Int(-1),
                Value::Text("C".into()),
                Value::Text("C".into()),
                Value::Null,
                Value::Null,
            ],
        ];
        (cols, rows)
    }

    fn pg_operator_virtual_table(&self, db_name: &str) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("oprname", "NAME"),
            ("oprnamespace", "OID"),
            ("oprowner", "OID"),
            ("oprkind", "PG_CHAR"),
            ("oprcanmerge", "BOOL"),
            ("oprcanhash", "BOOL"),
            ("oprleft", "OID"),
            ("oprright", "OID"),
            ("oprresult", "OID"),
            ("oprcom", "OID"),
            ("oprnegate", "OID"),
            ("oprcode", "REGPROC"),
            ("oprrest", "REGPROC"),
            ("oprjoin", "REGPROC"),
        ]);
        let ns = Self::schema_oid(db_name, "pg_catalog");
        let rows = [
            (96, "=", 23, 23, 16),
            (97, "<", 23, 23, 16),
            (521, ">", 23, 23, 16),
            (98, "=", 25, 25, 16),
        ]
        .into_iter()
        .map(|(oid, name, left, right, result)| {
            vec![
                Value::Int(oid),
                Value::Text(name.into()),
                Value::Int(ns),
                Value::Int(10),
                Value::Text("b".into()),
                Value::Bool(false),
                Value::Bool(name == "="),
                Value::Int(left),
                Value::Int(right),
                Value::Int(result),
                Value::Int(0),
                Value::Int(0),
                Value::Text("-".into()),
                Value::Text("-".into()),
                Value::Text("-".into()),
            ]
        })
        .collect();
        (cols, rows)
    }

    fn pg_cast_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("castsource", "OID"),
            ("casttarget", "OID"),
            ("castfunc", "OID"),
            ("castcontext", "PG_CHAR"),
            ("castmethod", "PG_CHAR"),
        ]);
        let casts = [
            (23, 20),
            (23, 25),
            (20, 25),
            (25, 23),
            (25, 20),
            (1043, 25),
            (25, 1043),
            (114, 3802),
        ];
        let rows = casts
            .into_iter()
            .enumerate()
            .map(|(idx, (source, target))| {
                vec![
                    Value::Int(10_000 + idx as i64),
                    Value::Int(source),
                    Value::Int(target),
                    Value::Int(0),
                    Value::Text("a".into()),
                    Value::Text("f".into()),
                ]
            })
            .collect();
        (cols, rows)
    }

    fn information_schema_virtual_table(
        &self,
        db_name: &str,
        table_only: &str,
    ) -> Result<Option<(Vec<ColumnDescriptor>, Vec<Vec<Value>>)>> {
        let schemas = self
            .catalog_reader
            .list_schemas(db_name)
            .unwrap_or_default();
        let tables = self
            .catalog_reader
            .list_all_tables(db_name)
            .unwrap_or_default();
        let result = match table_only.to_ascii_lowercase().as_str() {
            "tables" => Some(self.information_schema_tables(db_name, &schemas, &tables)),
            "columns" => Some(self.information_schema_columns(db_name, &schemas, &tables)),
            "table_constraints" | "constraints" => {
                Some(self.information_schema_table_constraints(db_name, &schemas, &tables))
            }
            "key_column_usage" => {
                Some(self.information_schema_key_column_usage(db_name, &schemas, &tables))
            }
            "constraint_column_usage" => Some(self.information_schema_constraint_column_usage()),
            "indexes" => Some(self.information_schema_indexes(db_name, &schemas, &tables)),
            "schemata" => Some(self.information_schema_schemata(db_name, &schemas)),
            _ => None,
        };
        Ok(result)
    }

    fn information_schema_tables(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("table_type", "TEXT"),
            ("self_referencing_column_name", "TEXT"),
            ("reference_generation", "TEXT"),
            ("user_defined_type_catalog", "TEXT"),
            ("user_defined_type_schema", "TEXT"),
            ("user_defined_type_name", "TEXT"),
            ("is_insertable_into", "TEXT"),
            ("is_typed", "TEXT"),
            ("commit_action", "TEXT"),
        ]);
        let rows = tables
            .iter()
            .map(|table| {
                vec![
                    Value::Text(db_name.into()),
                    Value::Text(Self::schema_name_by_id(db_name, schemas, table.schema_id)),
                    Value::Text(table.name.clone()),
                    Value::Text(
                        if table.view_query.is_some() {
                            "VIEW"
                        } else {
                            "BASE TABLE"
                        }
                        .into(),
                    ),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text("YES".into()),
                    Value::Text("NO".into()),
                    Value::Null,
                ]
            })
            .collect();
        (cols, rows)
    }

    fn information_schema_columns(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("column_name", "TEXT"),
            ("ordinal_position", "INT"),
            ("column_default", "TEXT"),
            ("is_nullable", "TEXT"),
            ("data_type", "TEXT"),
            ("character_maximum_length", "INT"),
            ("numeric_precision", "INT"),
            ("numeric_scale", "INT"),
            ("datetime_precision", "INT"),
            ("udt_catalog", "TEXT"),
            ("udt_schema", "TEXT"),
            ("udt_name", "TEXT"),
            ("is_identity", "TEXT"),
            ("identity_generation", "TEXT"),
            ("is_generated", "TEXT"),
            ("generation_expression", "TEXT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for (idx, column) in table.columns.iter().enumerate() {
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(table.name.clone()),
                    Value::Text(column.name.clone()),
                    Value::Int((idx + 1) as i64),
                    Value::Null,
                    Value::Text(if column.nullable { "YES" } else { "NO" }.into()),
                    Value::Text(column.data_type.to_ascii_lowercase()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text(db_name.into()),
                    Value::Text("pg_catalog".into()),
                    Value::Text(Self::pg_type_name(&column.data_type)),
                    Value::Text("NO".into()),
                    Value::Null,
                    Value::Text("NEVER".into()),
                    Value::Null,
                ]);
            }
        }
        (cols, rows)
    }

    fn information_schema_table_constraints(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("constraint_catalog", "TEXT"),
            ("constraint_schema", "TEXT"),
            ("constraint_name", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("constraint_type", "TEXT"),
            ("is_deferrable", "TEXT"),
            ("initially_deferred", "TEXT"),
            ("enforced", "TEXT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for index in &table.indexes {
                if index.unique {
                    rows.push(vec![
                        Value::Text(db_name.into()),
                        Value::Text(schema_name.clone()),
                        Value::Text(index.name.clone()),
                        Value::Text(schema_name.clone()),
                        Value::Text(table.name.clone()),
                        Value::Text(
                            if matches!(index.index_type, nodus_catalog::IndexType::Primary) {
                                "PRIMARY KEY"
                            } else {
                                "UNIQUE"
                            }
                            .into(),
                        ),
                        Value::Text("NO".into()),
                        Value::Text("NO".into()),
                        Value::Text("YES".into()),
                    ]);
                }
            }
            for (idx, constraint) in table.constraints.iter().enumerate() {
                let (name, constraint_type) = match constraint {
                    nodus_catalog::TableConstraint::Check { name, .. } => (
                        name.clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table.name, idx + 1)),
                        "CHECK",
                    ),
                    nodus_catalog::TableConstraint::ForeignKey { name, columns, .. } => (
                        name.clone().unwrap_or_else(|| {
                            format!("{}_{}_fkey", table.name, columns.join("_"))
                        }),
                        "FOREIGN KEY",
                    ),
                };
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(name),
                    Value::Text(schema_name.clone()),
                    Value::Text(table.name.clone()),
                    Value::Text(constraint_type.into()),
                    Value::Text("NO".into()),
                    Value::Text("NO".into()),
                    Value::Text("YES".into()),
                ]);
            }
        }
        (cols, rows)
    }

    fn information_schema_key_column_usage(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("constraint_catalog", "TEXT"),
            ("constraint_schema", "TEXT"),
            ("constraint_name", "TEXT"),
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("column_name", "TEXT"),
            ("ordinal_position", "INT"),
            ("position_in_unique_constraint", "INT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for index in &table.indexes {
                if !index.unique {
                    continue;
                }
                for (idx, key) in index.key_columns.iter().enumerate() {
                    if let Some(column) = table
                        .columns
                        .iter()
                        .find(|column| column.id == key.column_id)
                    {
                        rows.push(vec![
                            Value::Text(db_name.into()),
                            Value::Text(schema_name.clone()),
                            Value::Text(index.name.clone()),
                            Value::Text(db_name.into()),
                            Value::Text(schema_name.clone()),
                            Value::Text(table.name.clone()),
                            Value::Text(column.name.clone()),
                            Value::Int((idx + 1) as i64),
                            Value::Null,
                        ]);
                    }
                }
            }
        }
        (cols, rows)
    }

    fn information_schema_constraint_column_usage(
        &self,
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("column_name", "TEXT"),
            ("constraint_catalog", "TEXT"),
            ("constraint_schema", "TEXT"),
            ("constraint_name", "TEXT"),
        ]);
        (cols, Vec::new())
    }

    fn information_schema_indexes(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("index_name", "TEXT"),
            ("is_unique", "BOOL"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for index in &table.indexes {
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(table.name.clone()),
                    Value::Text(index.name.clone()),
                    Value::Bool(index.unique),
                ]);
            }
        }
        (cols, rows)
    }

    fn information_schema_schemata(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("catalog_name", "TEXT"),
            ("schema_name", "TEXT"),
            ("schema_owner", "TEXT"),
            ("default_character_set_catalog", "TEXT"),
            ("default_character_set_schema", "TEXT"),
            ("default_character_set_name", "TEXT"),
            ("sql_path", "TEXT"),
        ]);
        let rows = schemas
            .iter()
            .map(|schema| {
                vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema.name.clone()),
                    Value::Text("nodus".into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]
            })
            .collect();
        (cols, rows)
    }

    pub(crate) fn get_virtual_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_only: &str,
    ) -> Result<(Vec<ColumnDescriptor>, Vec<Vec<Value>>)> {
        if schema_name.eq_ignore_ascii_case("pg_catalog") {
            if let Some(table) = self.pg_catalog_virtual_table(db_name, table_only)? {
                return Ok(table);
            }
            anyhow::bail!("relation \"pg_catalog.{}\" does not exist", table_only);
        } else if schema_name.eq_ignore_ascii_case("information_schema") {
            if let Some(table) = self.information_schema_virtual_table(db_name, table_only)? {
                return Ok(table);
            }
            anyhow::bail!(
                "relation \"information_schema.{}\" does not exist",
                table_only
            );
        } else {
            anyhow::bail!("relation \"{}.{}\" does not exist", schema_name, table_only);
        }
    }
}
