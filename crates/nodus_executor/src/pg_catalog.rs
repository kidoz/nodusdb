//! Synthesized `pg_catalog` virtual tables (pg_class, pg_type, pg_settings, ...).
use crate::{MemExecutor, Value, parse_object_name};
use anyhow::Result;
use chrono::Utc;
use nodus_catalog::ColumnDescriptor;

impl MemExecutor {
    pub(crate) fn pg_catalog_virtual_table(
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
            // Enum types are not yet a NodusDB concept, so there are no labels to
            // list. The relation is still presented (with its real shape) because
            // pgjdbc/DataGrip join it during type introspection and tolerate zero
            // rows; populate this once `CREATE TYPE ... AS ENUM` lands.
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
            "pg_locks" => Some(self.pg_locks_virtual_table(db_name)),
            // The relations below exist so IDE/driver introspection
            // (DataGrip/pgjdbc) can join them without erroring. NodusDB does not
            // model these concepts yet, so they are presented with their real
            // shape and (mostly) zero rows.
            //
            // Timezone catalog: NodusDB operates in UTC, so advertise just that.
            "pg_timezone_names" => Some((
                Self::virtual_columns(&[
                    ("name", "TEXT"),
                    ("abbrev", "TEXT"),
                    ("utc_offset", "TEXT"),
                    ("is_dst", "BOOL"),
                ]),
                vec![vec![
                    Value::Text("UTC".into()),
                    Value::Text("UTC".into()),
                    Value::Text("00:00:00".into()),
                    Value::Bool(false),
                ]],
            )),
            // Role membership graph: no role-in-role membership is modeled yet.
            "pg_auth_members" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("roleid", "OID"),
                    ("member", "OID"),
                    ("grantor", "OID"),
                    ("admin_option", "BOOL"),
                    ("inherit_option", "BOOL"),
                    ("set_option", "BOOL"),
                ]),
                Vec::new(),
            )),
            // Tablespaces: NodusDB has no user tablespaces, but the two built-in
            // ones always exist in PostgreSQL and some tools assume them.
            "pg_tablespace" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("spcname", "NAME"),
                    ("spcowner", "OID"),
                    ("spcacl", "TEXT"),
                    ("spcoptions", "TEXT"),
                ]),
                vec![
                    vec![
                        Value::Int(1663),
                        Value::Text("pg_default".into()),
                        Value::Int(10),
                        Value::Null,
                        Value::Null,
                    ],
                    vec![
                        Value::Int(1664),
                        Value::Text("pg_global".into()),
                        Value::Int(10),
                        Value::Null,
                        Value::Null,
                    ],
                ],
            )),
            // Installed extensions. NodusDB has none, but PostgreSQL always ships
            // `plpgsql`, and tools list this relation to populate an extensions
            // view; advertise just that one so the view is non-empty and valid.
            "pg_extension" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("extname", "NAME"),
                    ("extowner", "OID"),
                    ("extnamespace", "OID"),
                    ("extrelocatable", "BOOL"),
                    ("extversion", "TEXT"),
                    ("extconfig", "TEXT"),
                    ("extcondition", "TEXT"),
                ]),
                vec![vec![
                    Value::Int(Self::stable_oid("extension:plpgsql", 13000)),
                    Value::Text("plpgsql".into()),
                    Value::Int(10),
                    Value::Int(11), // pg_catalog namespace
                    Value::Bool(false),
                    Value::Text("1.0".into()),
                    Value::Null,
                    Value::Null,
                ]],
            )),
            // Event triggers are not supported.
            "pg_event_trigger" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("evtname", "NAME"),
                    ("evtevent", "NAME"),
                    ("evtowner", "OID"),
                    ("evtfoid", "OID"),
                    ("evtenabled", "PG_CHAR"),
                    ("evttags", "TEXT"),
                ]),
                Vec::new(),
            )),
            _ => None,
        };
        Ok(result)
    }

    /// `pg_locks` synthesized from the in-flight *explicit* transactions.
    ///
    /// NodusDB has no general lock manager to expose, but DataGrip/JetBrains and
    /// other tools poll `pg_locks` to find long-running transactions. Each active
    /// `BEGIN` block contributes one `transactionid` row (the lock every backend
    /// holds on its own xid), which is enough for that "who is holding a
    /// transaction open" introspection. Autocommit statements run in throwaway
    /// implicit transactions and are intentionally omitted, so a bare
    /// `SELECT FROM pg_locks` reports nothing rather than its own xid.
    pub(crate) fn pg_locks_virtual_table(
        &self,
        db_name: &str,
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("locktype", "TEXT"),
            ("database", "OID"),
            ("relation", "OID"),
            ("transactionid", "INT8"),
            ("pid", "INT"),
            ("mode", "TEXT"),
            ("granted", "BOOL"),
        ]);
        let database = Self::database_oid(db_name);
        let rows = self
            .active_txns
            .read()
            .unwrap()
            .values()
            .filter(|txn| txn.explicit)
            .map(|txn| {
                // xid is a 32-bit counter in PostgreSQL; our txn id is a UUID, so
                // fold it to a stable positive integer for the INT8 column.
                let xid = Self::stable_oid(&txn.txn_id.0.to_string(), 0);
                vec![
                    Value::Text("transactionid".into()),
                    Value::Int(database),
                    Value::Null,
                    Value::Int(xid),
                    Value::Null,
                    Value::Text("ExclusiveLock".into()),
                    Value::Bool(true),
                ]
            })
            .collect();
        (cols, rows)
    }

    pub(crate) fn pg_type_virtual_table(
        &self,
        db_name: &str,
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
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

    pub(crate) fn pg_settings_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
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
            ("server_version", "18.0", "string"),
            ("server_version_num", "180000", "integer"),
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

    pub(crate) fn pg_roles_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
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

    pub(crate) fn pg_user_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
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

    pub(crate) fn pg_tables_virtual_table(
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

    pub(crate) fn pg_indexes_virtual_table(
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

    pub(crate) fn pg_constraint_rows(
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

    pub(crate) fn pg_collation_virtual_table(
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

    pub(crate) fn pg_operator_virtual_table(
        &self,
        db_name: &str,
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
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

    pub(crate) fn pg_cast_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
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
}
