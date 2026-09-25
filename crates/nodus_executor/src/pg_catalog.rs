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
                    let heap = table.view_query.is_none() && !crate::sequences::is_sequence(table);
                    rows.push(vec![
                        Value::Int(oid),
                        Value::Text(table.name.clone()),
                        Value::Int(Self::schema_oid(db_name, &schema_name)),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(10),
                        // A table's access method is heap.
                        Value::Int(if heap { 2 } else { 0 }),
                        Value::Int(oid),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Float(0.0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Bool(!table.indexes.is_empty()),
                        Value::Bool(false),
                        Value::Text("p".into()),
                        Value::Text(
                            if table.view_query.is_some() {
                                "v"
                            } else if table.materialized_query.is_some() {
                                "m"
                            } else if crate::sequences::is_sequence(table) {
                                "S"
                            } else {
                                "r"
                            }
                            .into(),
                        ),
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
                    ("attcompression", "PG_CHAR"),
                ]);
                let mut rows = Vec::new();
                for table in &tables {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    let relid = Self::table_oid(db_name, &schema_name, &table.name);
                    for (idx, column) in table.columns.iter().enumerate() {
                        let type_oid = Self::pg_type_oid(&column.data_type);
                        let default = crate::MemExecutor::column_default(column);
                        let identity = default
                            .as_ref()
                            .and_then(crate::sequences::identity_kind)
                            .map_or("", |always| if always { "a" } else { "d" });
                        let generated = if default
                            .as_ref()
                            .is_some_and(|d| Self::generation_expr(d).is_some())
                        {
                            "s"
                        } else {
                            ""
                        };
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
                            Value::Int(Self::pg_type_modifier(&column.data_type)),
                            Value::Bool(matches!(type_oid, 16 | 20 | 21 | 23 | 26 | 700 | 701)),
                            Value::Text(Self::pg_type_storage(&column.data_type).into()),
                            Value::Text("i".into()),
                            Value::Bool(!column.nullable),
                            Value::Bool(default.is_some() && identity.is_empty()),
                            Value::Bool(false),
                            Value::Text(identity.into()),
                            Value::Text(generated.into()),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Int(0),
                            Value::Int(100),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Text(String::new()),
                        ]);
                    }
                    // An index's attributes are its key columns.
                    for index in &table.indexes {
                        let index_oid =
                            Self::index_oid(db_name, &schema_name, &table.name, &index.name);
                        let keys = index
                            .key_columns
                            .iter()
                            .filter_map(|key| table.columns.iter().find(|c| c.id == key.column_id));
                        for (idx, column) in keys.enumerate() {
                            let type_oid = Self::pg_type_oid(&column.data_type);
                            rows.push(vec![
                                Value::Int(index_oid),
                                Value::Text(column.name.clone()),
                                Value::Int(type_oid),
                                Value::Int(-1),
                                Value::Int(Self::pg_type_length(&column.data_type)),
                                Value::Int((idx + 1) as i64),
                                Value::Int(0),
                                Value::Int(-1),
                                Value::Int(Self::pg_type_modifier(&column.data_type)),
                                Value::Bool(matches!(type_oid, 16 | 20 | 21 | 23 | 26 | 700 | 701)),
                                Value::Text(Self::pg_type_storage(&column.data_type).into()),
                                Value::Text("i".into()),
                                Value::Bool(false),
                                Value::Bool(false),
                                Value::Bool(false),
                                Value::Text(String::new()),
                                Value::Text(String::new()),
                                Value::Bool(false),
                                Value::Bool(true),
                                Value::Int(0),
                                Value::Int(0),
                                Value::Null,
                                Value::Null,
                                Value::Null,
                                Value::Null,
                                Value::Text(String::new()),
                            ]);
                        }
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
                    ("indkey", "INT[]"),
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
                        // `indkey` is the ordered list of 1-based column positions
                        // (attnums), as a real array so `unnest(i.indkey)`
                        // introspection returns one row per indexed column.
                        let keys: Vec<Value> = index
                            .key_columns
                            .iter()
                            .filter_map(|key| {
                                table
                                    .columns
                                    .iter()
                                    .position(|column| column.id == key.column_id)
                                    .map(|pos| Value::Int((pos + 1) as i64))
                            })
                            .collect();
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
                            Value::Array(keys),
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
            "pg_attrdef" => {
                let mut rows = Vec::new();
                for table in &tables {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    let relid = Self::table_oid(db_name, &schema_name, &table.name);
                    for (idx, column) in table.columns.iter().enumerate() {
                        // An identity column's generator is not a default.
                        let Some(default) = crate::MemExecutor::column_default(column)
                            .filter(|d| crate::sequences::identity_kind(d).is_none())
                        else {
                            continue;
                        };
                        rows.push(vec![
                            Value::Int(Self::stable_oid(
                                &format!(
                                    "attrdef:{db_name}.{schema_name}.{}.{}",
                                    table.name, column.name
                                ),
                                1_500_000_000,
                            )),
                            Value::Int(relid),
                            Value::Int((idx + 1) as i64),
                            Value::Text(Self::default_text(&default, &column.data_type)),
                        ]);
                    }
                }
                Some((
                    Self::virtual_columns(&[
                        ("oid", "OID"),
                        ("adrelid", "OID"),
                        ("adnum", "INT"),
                        ("adbin", "TEXT"),
                    ]),
                    rows,
                ))
            }
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
                vec![
                    vec![
                        Value::Int(2),
                        Value::Text("heap".into()),
                        Value::Text("heap_tableam_handler".into()),
                        Value::Text("t".into()),
                    ],
                    vec![
                        Value::Int(403),
                        Value::Text("btree".into()),
                        Value::Text("bthandler".into()),
                        Value::Text("i".into()),
                    ],
                ],
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
            // Timezone abbreviations. NodusDB operates in UTC, so advertise the
            // UTC/GMT abbreviations IDE introspection (DataGrip/pgjdbc) lists.
            "pg_timezone_abbrevs" => Some((
                Self::virtual_columns(&[
                    ("abbrev", "TEXT"),
                    ("utc_offset", "TEXT"),
                    ("is_dst", "BOOL"),
                ]),
                vec![
                    vec![
                        Value::Text("UTC".into()),
                        Value::Text("00:00:00".into()),
                        Value::Bool(false),
                    ],
                    vec![
                        Value::Text("GMT".into()),
                        Value::Text("00:00:00".into()),
                        Value::Bool(false),
                    ],
                ],
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
            // Procedural languages. NodusDB runs no user functions, but PostgreSQL
            // always ships these four, and IDE introspection joins against them;
            // advertise them so those joins resolve.
            "pg_language" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("lanname", "NAME"),
                    ("lanowner", "OID"),
                    ("lanispl", "BOOL"),
                    ("lanpltrusted", "BOOL"),
                    ("lanplcallfoid", "OID"),
                    ("laninline", "OID"),
                    ("lanvalidator", "OID"),
                    ("lanacl", "TEXT"),
                ]),
                vec![
                    vec![
                        Value::Int(12),
                        Value::Text("internal".into()),
                        Value::Int(10),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Null,
                    ],
                    vec![
                        Value::Int(13),
                        Value::Text("c".into()),
                        Value::Int(10),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Null,
                    ],
                    vec![
                        Value::Int(14),
                        Value::Text("sql".into()),
                        Value::Int(10),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Null,
                    ],
                    vec![
                        Value::Int(Self::stable_oid("language:plpgsql", 13500)),
                        Value::Text("plpgsql".into()),
                        Value::Int(10),
                        Value::Bool(true),
                        Value::Bool(true),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Null,
                    ],
                ],
            )),
            // Dependency graph: NodusDB does not track inter-object dependencies,
            // so the relation exists but is empty (tools tolerate no dependencies).
            // A `serial` column's sequence depends on the column
            // automatically, an identity column's internally.
            "pg_depend" => {
                let mut rows = Vec::new();
                for table in &tables {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    let relid = Self::table_oid(db_name, &schema_name, &table.name);
                    for (idx, column) in table.columns.iter().enumerate() {
                        let Some(default) = crate::MemExecutor::column_default(column) else {
                            continue;
                        };
                        let Some(sequence) = crate::sequences::default_sequence(&default) else {
                            continue;
                        };
                        let (seq_schema, seq_name) = match sequence.split_once('.') {
                            Some((schema, name)) => (schema.to_string(), name.to_string()),
                            None => (schema_name.clone(), sequence.clone()),
                        };
                        let deptype = if crate::sequences::identity_kind(&default).is_some() {
                            "i"
                        } else {
                            "a"
                        };
                        rows.push(vec![
                            Value::Int(PG_CLASS_OID),
                            Value::Int(Self::table_oid(db_name, &seq_schema, &seq_name)),
                            Value::Int(0),
                            Value::Int(PG_CLASS_OID),
                            Value::Int(relid),
                            Value::Int((idx + 1) as i64),
                            Value::Text(deptype.into()),
                        ]);
                    }
                    // An index depends on its columns (a unique one, like a
                    // constraint's, on the constraint instead).
                    for index in table.indexes.iter().filter(|i| !i.unique) {
                        let index_oid =
                            Self::index_oid(db_name, &schema_name, &table.name, &index.name);
                        for key in &index.key_columns {
                            if let Some(pos) =
                                table.columns.iter().position(|c| c.id == key.column_id)
                            {
                                rows.push(vec![
                                    Value::Int(PG_CLASS_OID),
                                    Value::Int(index_oid),
                                    Value::Int(0),
                                    Value::Int(PG_CLASS_OID),
                                    Value::Int(relid),
                                    Value::Int((pos + 1) as i64),
                                    Value::Text("a".into()),
                                ]);
                            }
                        }
                    }
                }
                Some((
                    Self::virtual_columns(&[
                        ("classid", "OID"),
                        ("objid", "OID"),
                        ("objsubid", "INT4"),
                        ("refclassid", "OID"),
                        ("refobjid", "OID"),
                        ("refobjsubid", "INT4"),
                        ("deptype", "PG_CHAR"),
                    ]),
                    rows,
                ))
            }
            // Foreign-data infrastructure: NodusDB has no FDWs, servers, or user
            // mappings, but IDE introspection lists these relations; present them
            // with their real shape and no rows.
            "pg_foreign_data_wrapper" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("fdwname", "NAME"),
                    ("fdwowner", "OID"),
                    ("fdwhandler", "OID"),
                    ("fdwvalidator", "OID"),
                    ("fdwacl", "TEXT"),
                    ("fdwoptions", "TEXT"),
                ]),
                Vec::new(),
            )),
            "pg_foreign_server" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("srvname", "NAME"),
                    ("srvowner", "OID"),
                    ("srvfdw", "OID"),
                    ("srvtype", "TEXT"),
                    ("srvversion", "TEXT"),
                    ("srvacl", "TEXT"),
                    ("srvoptions", "TEXT"),
                ]),
                Vec::new(),
            )),
            "pg_user_mapping" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("umuser", "OID"),
                    ("umserver", "OID"),
                    ("umoptions", "TEXT"),
                ]),
                Vec::new(),
            )),
            // Available extensions. NodusDB ships only the always-present plpgsql,
            // mirrored by pg_extension; advertise it here too so the extensions
            // browser is populated and valid.
            "pg_available_extensions" => Some((
                Self::virtual_columns(&[
                    ("name", "NAME"),
                    ("default_version", "TEXT"),
                    ("installed_version", "TEXT"),
                    ("comment", "TEXT"),
                ]),
                vec![vec![
                    Value::Text("plpgsql".into()),
                    Value::Text("1.0".into()),
                    Value::Text("1.0".into()),
                    Value::Text("PL/pgSQL procedural language".into()),
                ]],
            )),
            "pg_available_extension_versions" => Some((
                Self::virtual_columns(&[
                    ("name", "NAME"),
                    ("version", "TEXT"),
                    ("installed", "BOOL"),
                    ("superuser", "BOOL"),
                    ("trusted", "BOOL"),
                    ("relocatable", "BOOL"),
                    ("schema", "NAME"),
                    ("requires", "TEXT"),
                    ("comment", "TEXT"),
                ]),
                vec![vec![
                    Value::Text("plpgsql".into()),
                    Value::Text("1.0".into()),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Null,
                    Value::Null,
                    Value::Text("PL/pgSQL procedural language".into()),
                ]],
            )),
            // Access-method support catalogs (operator classes/families and their
            // operators/procedures). NodusDB has no user-defined opclasses, so
            // these exist with their real shape and no rows — enough for IDE
            // introspection to join across them without erroring.
            "pg_opclass" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("opcmethod", "OID"),
                    ("opcname", "NAME"),
                    ("opcnamespace", "OID"),
                    ("opcowner", "OID"),
                    ("opcfamily", "OID"),
                    ("opcintype", "OID"),
                    ("opcdefault", "BOOL"),
                    ("opckeytype", "OID"),
                ]),
                Vec::new(),
            )),
            "pg_opfamily" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("opfmethod", "OID"),
                    ("opfname", "NAME"),
                    ("opfnamespace", "OID"),
                    ("opfowner", "OID"),
                ]),
                Vec::new(),
            )),
            "pg_amop" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("amopfamily", "OID"),
                    ("amoplefttype", "OID"),
                    ("amoprighttype", "OID"),
                    ("amopstrategy", "INT2"),
                    ("amoppurpose", "PG_CHAR"),
                    ("amopopr", "OID"),
                    ("amopmethod", "OID"),
                    ("amopsortfamily", "OID"),
                ]),
                Vec::new(),
            )),
            "pg_amproc" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("amprocfamily", "OID"),
                    ("amproclefttype", "OID"),
                    ("amprocrighttype", "OID"),
                    ("amprocnum", "INT2"),
                    ("amproc", "REGPROC"),
                ]),
                Vec::new(),
            )),
            // Aggregate definitions. NodusDB's aggregates are built into the
            // executor, not catalogued, so this is presented empty.
            "pg_aggregate" => Some((
                Self::virtual_columns(&[
                    ("aggfnoid", "REGPROC"),
                    ("aggkind", "PG_CHAR"),
                    ("aggnumdirectargs", "INT2"),
                    ("aggtransfn", "REGPROC"),
                    ("aggfinalfn", "REGPROC"),
                    ("aggtranstype", "OID"),
                    ("agginitval", "TEXT"),
                ]),
                Vec::new(),
            )),
            // Sequences: NodusDB allocates identity values without a catalogued
            // sequence relation, so this is empty.
            "pg_sequence" => {
                let mut rows = Vec::new();
                for table in tables.iter().filter(|t| crate::sequences::is_sequence(t)) {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    // The committed options, whoever asks.
                    let Some(state) = self
                        .scan_rows(table.id, "")
                        .ok()
                        .and_then(|rows| rows.into_iter().next())
                        .and_then(|row| crate::sequences::SequenceState::from_row(&row).ok())
                    else {
                        continue;
                    };
                    rows.push(vec![
                        Value::Int(Self::table_oid(db_name, &schema_name, &table.name)),
                        Value::Int(Self::pg_type_oid(&state.data_type)),
                        Value::Int(state.start),
                        Value::Int(state.increment),
                        Value::Int(state.max),
                        Value::Int(state.min),
                        Value::Int(state.cache),
                        Value::Bool(state.cycle),
                    ]);
                }
                Some((
                    Self::virtual_columns(&[
                        ("seqrelid", "OID"),
                        ("seqtypid", "OID"),
                        ("seqstart", "INT8"),
                        ("seqincrement", "INT8"),
                        ("seqmax", "INT8"),
                        ("seqmin", "INT8"),
                        ("seqcache", "INT8"),
                        ("seqcycle", "BOOL"),
                    ]),
                    rows,
                ))
            }
            // Foreign tables: none (NodusDB has no FDWs).
            "pg_foreign_table" => Some((
                Self::virtual_columns(&[
                    ("ftrelid", "OID"),
                    ("ftserver", "OID"),
                    ("ftoptions", "TEXT"),
                ]),
                Vec::new(),
            )),
            // Rules, row-security policies, and triggers are not supported, so
            // these are present (so introspection joins resolve) but empty.
            "pg_rewrite" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("rulename", "NAME"),
                    ("ev_class", "OID"),
                    ("ev_type", "PG_CHAR"),
                    ("ev_enabled", "PG_CHAR"),
                    ("is_instead", "BOOL"),
                    ("ev_qual", "TEXT"),
                    ("ev_action", "TEXT"),
                ]),
                Vec::new(),
            )),
            "pg_policy" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("polname", "NAME"),
                    ("polrelid", "OID"),
                    ("polcmd", "PG_CHAR"),
                    ("polpermissive", "BOOL"),
                    ("polroles", "TEXT"),
                    ("polqual", "TEXT"),
                    ("polwithcheck", "TEXT"),
                ]),
                Vec::new(),
            )),
            "pg_trigger" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("tgrelid", "OID"),
                    ("tgparentid", "OID"),
                    ("tgname", "NAME"),
                    ("tgfoid", "OID"),
                    ("tgtype", "INT2"),
                    ("tgenabled", "PG_CHAR"),
                    ("tgisinternal", "BOOL"),
                    ("tgconstrrelid", "OID"),
                    ("tgconstrindid", "OID"),
                    ("tgconstraint", "OID"),
                    ("tgnargs", "INT2"),
                    ("tgargs", "TEXT"),
                    ("tgqual", "TEXT"),
                ]),
                Vec::new(),
            )),
            // No extended statistics, publications, or table inheritance.
            "pg_statistic_ext" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("stxrelid", "OID"),
                    ("stxname", "NAME"),
                    ("stxnamespace", "OID"),
                    ("stxowner", "OID"),
                    ("stxkeys", "INT2[]"),
                    ("stxstattarget", "INT2"),
                    ("stxkind", "TEXT[]"),
                    ("stxexprs", "TEXT"),
                ]),
                Vec::new(),
            )),
            "pg_publication" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("pubname", "NAME"),
                    ("pubowner", "OID"),
                    ("puballtables", "BOOL"),
                    ("pubinsert", "BOOL"),
                    ("pubupdate", "BOOL"),
                    ("pubdelete", "BOOL"),
                    ("pubtruncate", "BOOL"),
                    ("pubviaroot", "BOOL"),
                    ("pubgencols", "PG_CHAR"),
                ]),
                Vec::new(),
            )),
            "pg_publication_namespace" => Some((
                Self::virtual_columns(&[("oid", "OID"), ("pnpubid", "OID"), ("pnnspid", "OID")]),
                Vec::new(),
            )),
            "pg_publication_rel" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("prpubid", "OID"),
                    ("prrelid", "OID"),
                    ("prqual", "TEXT"),
                    ("prattrs", "INT2[]"),
                ]),
                Vec::new(),
            )),
            "pg_inherits" => Some((
                Self::virtual_columns(&[
                    ("inhrelid", "OID"),
                    ("inhparent", "OID"),
                    ("inhseqno", "INT"),
                    ("inhdetachpending", "BOOL"),
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
            // PostgreSQL 18 records each NOT NULL column as a constraint.
            for (idx, column) in table.columns.iter().enumerate() {
                if column.nullable || table.view_query.is_some() {
                    continue;
                }
                let conname = format!("{}_{}_not_null", table.name, column.name);
                rows.push(vec![
                    Value::Int(Self::constraint_oid(
                        db_name,
                        &schema_name,
                        &table.name,
                        &conname,
                    )),
                    Value::Text(conname),
                    Value::Int(namespace),
                    Value::Text("n".into()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Int(relid),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Text(" ".into()),
                    Value::Text(" ".into()),
                    Value::Text(" ".into()),
                    Value::Bool(true),
                    Value::Int(0),
                    Value::Bool(false),
                    Value::Array(vec![Value::Int((idx + 1) as i64)]),
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

    /// A value cast to an object-identifier type: its OID, found by name
    /// for text (`'t'::regclass`), with PostgreSQL's error when there is no
    /// such object.
    pub(crate) fn object_identifier(value: Value, kind: &str) -> Result<Value, String> {
        let name = match value {
            Value::Int(oid) => return Ok(Value::Int(oid)),
            Value::Text(s) => s,
            other => {
                return Err(format!(
                    "cannot cast {} to {}",
                    crate::value::value_type_name(&other),
                    kind.to_ascii_lowercase()
                ));
            }
        };
        if let Ok(oid) = name.trim().parse::<i64>() {
            return Ok(Value::Int(oid));
        }
        let catalog = crate::session_env::with(|env| env.and_then(|e| e.catalog.clone()));
        let found = match kind {
            "REGCLASS" => catalog.and_then(|c| Self::relation_oid(c.as_ref(), &name)),
            "REGNAMESPACE" => catalog
                .and_then(|c| c.get_schema("default", name.trim().trim_matches('"')).ok())
                .map(|s| Self::schema_oid("default", &s.name)),
            _ => Some(Self::pg_type_oid(&name)),
        };
        found.map(Value::Int).ok_or_else(|| match kind {
            "REGCLASS" => format!("relation \"{name}\" does not exist"),
            "REGNAMESPACE" => format!("schema \"{name}\" does not exist"),
            _ => format!("type \"{name}\" does not exist"),
        })
    }

    /// The OID of a relation (table, view, sequence, or index) by its
    /// possibly schema-qualified name; an unqualified one is looked up in
    /// `public`, then among the system catalogs.
    pub(crate) fn relation_oid(
        catalog: &dyn nodus_catalog::CatalogReader,
        name: &str,
    ) -> Option<i64> {
        let db = "default";
        let unquote = |s: &str| s.trim().trim_matches('"').to_string();
        let (schema, relation) = match name.trim().split_once('.') {
            Some((schema, relation)) => (unquote(schema), unquote(relation)),
            None => ("public".to_string(), unquote(name)),
        };
        if let Some(&(_, oid)) = SYSTEM_CATALOG_OIDS
            .iter()
            .find(|(catalog, _)| *catalog == relation)
            && (schema == "pg_catalog" || catalog.get_table(db, &schema, &relation).is_err())
        {
            return Some(oid);
        }
        if let Ok(table) = catalog.get_table(db, &schema, &relation) {
            return Some(Self::table_oid(db, &schema, &table.name));
        }
        let schemas = catalog.list_schemas(db).ok()?;
        catalog
            .list_all_tables(db)
            .ok()?
            .into_iter()
            .find_map(|table| {
                let table_schema = Self::schema_name_by_id(db, &schemas, table.schema_id);
                (table_schema == schema)
                    .then(|| table.indexes.iter().find(|i| i.name == relation))
                    .flatten()
                    .map(|index| Self::index_oid(db, &table_schema, &table.name, &index.name))
            })
    }

    /// An object identifier as its type prints it: a relation, type, or
    /// schema name (schema-qualified outside `public`), or the number when
    /// there is no such object.
    pub(crate) fn object_name(
        catalog: &dyn nodus_catalog::CatalogReader,
        kind: &str,
        oid: i64,
    ) -> Option<String> {
        let db = "default";
        let schemas = catalog.list_schemas(db).ok()?;
        match kind {
            "REGTYPE" => Some(crate::functions::format_type_name(oid)),
            "REGNAMESPACE" => schemas
                .iter()
                .find(|s| Self::schema_oid(db, &s.name) == oid)
                .map(|s| quote_ident(&s.name)),
            _ if let Some(&(name, _)) = SYSTEM_CATALOG_OIDS.iter().find(|(_, o)| *o == oid) => {
                Some(name.to_string())
            }
            _ => catalog
                .list_all_tables(db)
                .ok()?
                .into_iter()
                .find_map(|table| {
                    let schema = Self::schema_name_by_id(db, &schemas, table.schema_id);
                    let qualify = |name: &str| {
                        if schema == "public" {
                            quote_ident(name)
                        } else {
                            format!("{}.{}", quote_ident(&schema), quote_ident(name))
                        }
                    };
                    if Self::table_oid(db, &schema, &table.name) == oid {
                        return Some(qualify(&table.name));
                    }
                    table
                        .indexes
                        .iter()
                        .find(|i| Self::index_oid(db, &schema, &table.name, &i.name) == oid)
                        .map(|i| qualify(&i.name))
                }),
        }
    }

    /// A statement's result with its object-identifier columns (`regclass`,
    /// `regtype`, `regnamespace`) showing names rather than OIDs.
    pub(crate) fn name_object_identifiers(
        &self,
        mut out: crate::QueryOutput,
    ) -> crate::QueryOutput {
        let columns: Vec<(usize, &'static str)> = out
            .types
            .iter()
            .enumerate()
            .filter_map(|(i, t)| crate::value::object_identifier_type(t).map(|kind| (i, kind)))
            .collect();
        if columns.is_empty() {
            return out;
        }
        for row in &mut out.rows {
            for &(i, kind) in &columns {
                if let Some(Value::Int(oid)) = row.values.get(i)
                    && let Some(name) = Self::object_name(self.catalog_reader.as_ref(), kind, *oid)
                {
                    row.values[i] = Value::Text(name);
                }
            }
        }
        out
    }

    /// A column default as `pg_get_expr(adbin, ...)` shows it; a generated
    /// column's default is its generation expression.
    pub(crate) fn default_text(default: &crate::ScalarExpr, data_type: &str) -> String {
        let expr = Self::generation_expr(default).unwrap_or(default);
        match expr {
            // A literal default is stored as a value of the column's type.
            crate::ScalarExpr::Literal(Value::Text(s)) => format!(
                "'{}'::{}",
                s.replace('\'', "''"),
                crate::functions::format_type_name(Self::pg_type_oid(data_type))
            ),
            _ => crate::explain::deparse_scalar(expr, false),
        }
    }

    /// `pg_get_viewdef(view)`: the view's query, laid out as PostgreSQL
    /// pretty-prints it.
    pub(crate) fn view_definition(
        catalog: &dyn nodus_catalog::CatalogReader,
        oid: i64,
    ) -> Option<String> {
        let db = "default";
        let schemas = catalog.list_schemas(db).ok()?;
        let view = catalog.list_all_tables(db).ok()?.into_iter().find(|t| {
            let schema = Self::schema_name_by_id(db, &schemas, t.schema_id);
            (t.view_query.is_some() || t.materialized_query.is_some())
                && Self::table_oid(db, &schema, &t.name) == oid
        })?;
        let query = view
            .view_query
            .as_deref()
            .or(view.materialized_query.as_deref())?;
        let plan: crate::LogicalPlan = serde_json::from_str(query).ok()?;
        crate::explain::deparse_query(&plan).map(|sql| format!("{sql};"))
    }

    /// `pg_get_indexdef(oid [, column, pretty])`: the index's `CREATE INDEX`
    /// statement, or with a column number that key column.
    pub(crate) fn index_definition(
        catalog: &dyn nodus_catalog::CatalogReader,
        oid: i64,
        column: i64,
        pretty: bool,
    ) -> Option<String> {
        let db = "default";
        let schemas = catalog.list_schemas(db).ok()?;
        for table in catalog.list_all_tables(db).ok()? {
            let schema = Self::schema_name_by_id(db, &schemas, table.schema_id);
            let Some(index) = table
                .indexes
                .iter()
                .find(|i| Self::index_oid(db, &schema, &table.name, &i.name) == oid)
            else {
                continue;
            };
            let keys: Vec<String> = index
                .key_columns
                .iter()
                .filter_map(|key| {
                    let c = table.columns.iter().find(|c| c.id == key.column_id)?;
                    Some(format!(
                        "{}{}",
                        quote_ident(&c.name),
                        if key.descending { " DESC" } else { "" }
                    ))
                })
                .collect();
            if column > 0 {
                return keys.get(column as usize - 1).cloned();
            }
            let relation = if pretty && schema == "public" {
                quote_ident(&table.name)
            } else {
                format!("{}.{}", quote_ident(&schema), quote_ident(&table.name))
            };
            return Some(format!(
                "CREATE {}INDEX {} ON {relation} USING btree ({})",
                if index.unique { "UNIQUE " } else { "" },
                quote_ident(&index.name),
                keys.join(", ")
            ));
        }
        None
    }

    /// `pg_get_constraintdef(oid)`: the constraint's definition as `ALTER
    /// TABLE ... ADD CONSTRAINT` would take it.
    pub(crate) fn constraint_definition(
        catalog: &dyn nodus_catalog::CatalogReader,
        oid: i64,
        pretty: bool,
    ) -> Option<String> {
        let db = "default";
        let schemas = catalog.list_schemas(db).ok()?;
        for table in catalog.list_all_tables(db).ok()? {
            let schema = Self::schema_name_by_id(db, &schemas, table.schema_id);
            let is = |name: &str| Self::constraint_oid(db, &schema, &table.name, name) == oid;
            let column_list = |ids: &mut dyn Iterator<Item = nodus_catalog::ColumnId>| {
                ids.filter_map(|id| table.columns.iter().find(|c| c.id == id))
                    .map(|c| quote_ident(&c.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            for index in table.indexes.iter().filter(|i| i.unique) {
                if is(&index.name) {
                    let kind = if matches!(index.index_type, nodus_catalog::IndexType::Primary) {
                        "PRIMARY KEY"
                    } else {
                        "UNIQUE"
                    };
                    let columns = column_list(&mut index.key_columns.iter().map(|k| k.column_id));
                    return Some(format!("{kind} ({columns})"));
                }
            }
            for column in table.columns.iter().filter(|c| !c.nullable) {
                if is(&format!("{}_{}_not_null", table.name, column.name)) {
                    return Some(format!("NOT NULL {}", quote_ident(&column.name)));
                }
            }
            for (idx, constraint) in table.constraints.iter().enumerate() {
                match constraint {
                    nodus_catalog::TableConstraint::Check { name, expr } => {
                        let name = name
                            .clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table.name, idx + 1));
                        if is(&name) {
                            return Some(if pretty {
                                format!("CHECK ({expr})")
                            } else {
                                format!("CHECK (({expr}))")
                            });
                        }
                    }
                    nodus_catalog::TableConstraint::ForeignKey {
                        name,
                        columns,
                        foreign_table,
                        referred_columns,
                    } => {
                        let name = name.clone().unwrap_or_else(|| {
                            format!("{}_{}_fkey", table.name, columns.join("_"))
                        });
                        if is(&name) {
                            let quote = |names: &[String]| {
                                names
                                    .iter()
                                    .map(|n| quote_ident(n))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            };
                            return Some(format!(
                                "FOREIGN KEY ({}) REFERENCES {}({})",
                                quote(columns),
                                foreign_table,
                                quote(referred_columns)
                            ));
                        }
                    }
                }
            }
        }
        None
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

/// An identifier as PostgreSQL prints it: quoted unless it is a plain
/// lower-case name.
fn quote_ident(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// `pg_class`'s own OID, the class of relations in `pg_depend`.
const PG_CLASS_OID: i64 = 1259;

/// The system catalogs' fixed OIDs, as `'pg_class'::regclass` gives them.
const SYSTEM_CATALOG_OIDS: &[(&str, i64)] = &[
    ("pg_class", PG_CLASS_OID),
    ("pg_type", 1247),
    ("pg_attribute", 1249),
    ("pg_proc", 1255),
    ("pg_database", 1262),
    ("pg_tablespace", 1213),
    ("pg_authid", 1260),
    ("pg_namespace", 2615),
    ("pg_constraint", 2606),
    ("pg_index", 2610),
    ("pg_inherits", 2611),
    ("pg_language", 2612),
    ("pg_opclass", 2616),
    ("pg_operator", 2617),
    ("pg_rewrite", 2618),
    ("pg_trigger", 2620),
    ("pg_am", 2601),
    ("pg_attrdef", 2604),
    ("pg_cast", 2605),
    ("pg_depend", 2608),
    ("pg_description", 2609),
    ("pg_sequence", 2224),
    ("pg_policy", 3256),
    ("pg_statistic_ext", 3381),
    ("pg_collation", 3456),
    ("pg_extension", 3079),
    ("pg_publication", 6104),
];
