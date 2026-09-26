//! Synthesized `information_schema` virtual tables (tables, columns, constraints, ...).
use crate::{MemExecutor, Value, parse_object_name};
use anyhow::Result;
use chrono::Utc;
use nodus_catalog::ColumnDescriptor;

impl MemExecutor {
    pub(crate) fn information_schema_virtual_table(
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
            "constraint_column_usage" => {
                Some(self.information_schema_constraint_column_usage(db_name, &schemas, &tables))
            }
            "referential_constraints" => {
                Some(self.information_schema_referential_constraints(db_name, &schemas, &tables))
            }
            "check_constraints" => {
                Some(self.information_schema_check_constraints(db_name, &schemas, &tables))
            }
            "indexes" => Some(self.information_schema_indexes(db_name, &schemas, &tables)),
            "schemata" => Some(self.information_schema_schemata(db_name, &schemas)),
            _ => None,
        };
        Ok(result)
    }

    pub(crate) fn information_schema_tables(
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
        // Like PostgreSQL, `information_schema.tables` lists no sequences or
        // materialized views.
        let rows = tables
            .iter()
            .filter(|table| {
                !crate::sequences::is_sequence(table) && table.materialized_query.is_none()
            })
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

    pub(crate) fn information_schema_columns(
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
            ("numeric_precision_radix", "INT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for (idx, column) in table.columns.iter().enumerate() {
                let info = TypeInfo::of(&column.data_type);
                let default = Self::column_default(column);
                let identity = default.as_ref().and_then(crate::sequences::identity_kind);
                let generated = default
                    .as_ref()
                    .and_then(|d| Self::generation_expr(d).cloned());
                let default_text = default
                    .as_ref()
                    .filter(|_| identity.is_none() && generated.is_none())
                    .map(|d| Value::Text(Self::default_text(d, &column.data_type)))
                    .unwrap_or(Value::Null);
                let int = |v: Option<i64>| v.map_or(Value::Null, Value::Int);
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(table.name.clone()),
                    Value::Text(column.name.clone()),
                    Value::Int((idx + 1) as i64),
                    default_text,
                    Value::Text(if column.nullable { "YES" } else { "NO" }.into()),
                    Value::Text(info.data_type),
                    int(info.character_maximum_length),
                    int(info.numeric_precision),
                    int(info.numeric_scale),
                    int(info.datetime_precision),
                    Value::Text(db_name.into()),
                    Value::Text("pg_catalog".into()),
                    Value::Text(info.udt_name),
                    Value::Text(if identity.is_some() { "YES" } else { "NO" }.into()),
                    match identity {
                        Some(true) => Value::Text("ALWAYS".into()),
                        Some(false) => Value::Text("BY DEFAULT".into()),
                        None => Value::Null,
                    },
                    Value::Text(
                        if generated.is_some() {
                            "ALWAYS"
                        } else {
                            "NEVER"
                        }
                        .into(),
                    ),
                    generated
                        .map(|g| Value::Text(crate::explain::deparse_scalar(&g, false)))
                        .unwrap_or(Value::Null),
                    int(info.numeric_precision_radix),
                ]);
            }
        }
        (cols, rows)
    }

    pub(crate) fn information_schema_table_constraints(
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
            for index in &Self::table_indexes(table) {
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
            // A NOT NULL column has a not-null constraint, as in PostgreSQL 18.
            let not_null = table
                .columns
                .iter()
                .filter(|c| !c.nullable)
                .map(|c| (format!("{}_{}_not_null", table.name, c.name), "CHECK"));
            let constraints = table.constraints.iter().map(|constraint| {
                let constraint_type = match constraint {
                    nodus_catalog::TableConstraint::Check { .. } => "CHECK",
                    nodus_catalog::TableConstraint::ForeignKey { .. } => "FOREIGN KEY",
                };
                (constraint.effective_name(&table.name), constraint_type)
            });
            for (name, constraint_type) in not_null.chain(constraints) {
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

    pub(crate) fn information_schema_key_column_usage(
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
            for index in &Self::table_indexes(table) {
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
            // A foreign key's columns, each with its place in the key it
            // references.
            for constraint in &table.constraints {
                if let nodus_catalog::TableConstraint::ForeignKey { columns, .. } = constraint {
                    for (idx, column) in columns.iter().enumerate() {
                        rows.push(vec![
                            Value::Text(db_name.into()),
                            Value::Text(schema_name.clone()),
                            Value::Text(constraint.effective_name(&table.name)),
                            Value::Text(db_name.into()),
                            Value::Text(schema_name.clone()),
                            Value::Text(table.name.clone()),
                            Value::Text(column.clone()),
                            Value::Int((idx + 1) as i64),
                            Value::Int((idx + 1) as i64),
                        ]);
                    }
                }
            }
        }
        (cols, rows)
    }

    /// The columns each constraint uses: a key's or a CHECK's own, a NOT
    /// NULL's column, and a foreign key's referenced columns.
    pub(crate) fn information_schema_constraint_column_usage(
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
            ("constraint_catalog", "TEXT"),
            ("constraint_schema", "TEXT"),
            ("constraint_name", "TEXT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            let mut usage = |table_name: &str, column: &str, constraint: String| {
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(table_name.to_string()),
                    Value::Text(column.to_string()),
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(constraint),
                ]);
            };
            for column in table.columns.iter().filter(|c| !c.nullable) {
                usage(
                    &table.name,
                    &column.name,
                    format!("{}_{}_not_null", table.name, column.name),
                );
            }
            for index in Self::table_indexes(table).iter().filter(|i| i.unique) {
                for key in &index.key_columns {
                    if let Some(column) = table.columns.iter().find(|c| c.id == key.column_id) {
                        usage(&table.name, &column.name, index.name.clone());
                    }
                }
            }
            for constraint in &table.constraints {
                let name = constraint.effective_name(&table.name);
                match constraint {
                    nodus_catalog::TableConstraint::Check { expr, .. } => {
                        for column in &table.columns {
                            let mentioned = expr
                                .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                                .any(|word| word == column.name);
                            if mentioned {
                                usage(&table.name, &column.name, name.clone());
                            }
                        }
                    }
                    nodus_catalog::TableConstraint::ForeignKey {
                        foreign_table,
                        referred_columns,
                        ..
                    } => {
                        let parent = foreign_table.rsplit('.').next().unwrap_or(foreign_table);
                        for column in referred_columns {
                            usage(parent, column, name.clone());
                        }
                    }
                }
            }
        }
        (cols, rows)
    }

    /// Each foreign key, with the key it references and its actions.
    pub(crate) fn information_schema_referential_constraints(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("constraint_catalog", "TEXT"),
            ("constraint_schema", "TEXT"),
            ("constraint_name", "TEXT"),
            ("unique_constraint_catalog", "TEXT"),
            ("unique_constraint_schema", "TEXT"),
            ("unique_constraint_name", "TEXT"),
            ("match_option", "TEXT"),
            ("update_rule", "TEXT"),
            ("delete_rule", "TEXT"),
        ]);
        let rule = |action: nodus_catalog::ReferentialAction| {
            use nodus_catalog::ReferentialAction as A;
            match action {
                A::NoAction => "NO ACTION",
                A::Restrict => "RESTRICT",
                A::Cascade => "CASCADE",
                A::SetNull => "SET NULL",
                A::SetDefault => "SET DEFAULT",
            }
        };
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for constraint in &table.constraints {
                let nodus_catalog::TableConstraint::ForeignKey {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ..
                } = constraint
                else {
                    continue;
                };
                let (parent_schema, parent) = match parse_object_name(foreign_table) {
                    Ok((_, schema, table)) => (schema.to_string(), table.to_string()),
                    Err(_) => (schema_name.clone(), foreign_table.clone()),
                };
                // The parent's unique constraint over the referenced columns.
                let unique = tables.iter().find(|t| t.name == parent).and_then(|p| {
                    Self::table_indexes(p).into_iter().find(|i| {
                        i.unique
                            && i.key_columns.len() == referred_columns.len()
                            && i.key_columns.iter().all(|k| {
                                p.columns
                                    .iter()
                                    .find(|c| c.id == k.column_id)
                                    .is_some_and(|c| referred_columns.contains(&c.name))
                            })
                    })
                });
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(constraint.effective_name(&table.name)),
                    Value::Text(db_name.into()),
                    Value::Text(parent_schema),
                    unique.map_or(Value::Null, |i| Value::Text(i.name)),
                    Value::Text("NONE".into()),
                    Value::Text(rule(*on_update).into()),
                    Value::Text(rule(*on_delete).into()),
                ]);
            }
        }
        (cols, rows)
    }

    /// Each CHECK constraint, a column's NOT NULL among them, with its
    /// condition.
    pub(crate) fn information_schema_check_constraints(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("constraint_catalog", "TEXT"),
            ("constraint_schema", "TEXT"),
            ("constraint_name", "TEXT"),
            ("check_clause", "TEXT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            let not_null = table.columns.iter().filter(|c| !c.nullable).map(|c| {
                (
                    format!("{}_{}_not_null", table.name, c.name),
                    format!("{} IS NOT NULL", c.name),
                )
            });
            let checks = table
                .constraints
                .iter()
                .filter_map(|constraint| match constraint {
                    nodus_catalog::TableConstraint::Check { expr, .. } => {
                        Some((constraint.effective_name(&table.name), format!("({expr})")))
                    }
                    nodus_catalog::TableConstraint::ForeignKey { .. } => None,
                });
            for (name, clause) in not_null.chain(checks) {
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(name),
                    Value::Text(clause),
                ]);
            }
        }
        (cols, rows)
    }

    pub(crate) fn information_schema_indexes(
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
            for index in &Self::table_indexes(table) {
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

    pub(crate) fn information_schema_schemata(
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
}

/// A declared type as `information_schema.columns` describes it.
struct TypeInfo {
    data_type: String,
    udt_name: String,
    character_maximum_length: Option<i64>,
    numeric_precision: Option<i64>,
    numeric_precision_radix: Option<i64>,
    numeric_scale: Option<i64>,
    datetime_precision: Option<i64>,
}

impl TypeInfo {
    fn of(declared: &str) -> TypeInfo {
        let lower = declared.trim().to_ascii_lowercase();
        if let Some(element) = lower.strip_suffix("[]") {
            let element = TypeInfo::of(element.trim_end_matches("[]"));
            return TypeInfo {
                data_type: "ARRAY".into(),
                udt_name: format!("_{}", element.udt_name),
                ..TypeInfo::named("", "")
            };
        }
        let (base, args) = match lower.split_once('(') {
            Some((base, rest)) => (base.trim(), rest.trim_end_matches(')')),
            None => (lower.as_str(), ""),
        };
        let args: Vec<i64> = args
            .split(',')
            .filter_map(|a| a.trim().parse().ok())
            .collect();
        let binary = |bits: i64, scale: Option<i64>| TypeInfo {
            numeric_precision: Some(bits),
            numeric_precision_radix: Some(2),
            numeric_scale: scale,
            ..TypeInfo::named("", "")
        };
        let mut info = match base {
            "int" | "integer" | "int4" | "serial" | "serial4" => {
                binary(32, Some(0)).with("integer", "int4")
            }
            "smallint" | "int2" | "smallserial" | "serial2" => {
                binary(16, Some(0)).with("smallint", "int2")
            }
            "bigint" | "int8" | "bigserial" | "serial8" => {
                binary(64, Some(0)).with("bigint", "int8")
            }
            "real" | "float4" => binary(24, None).with("real", "float4"),
            "double precision" | "float8" | "float" | "double" => {
                binary(53, None).with("double precision", "float8")
            }
            "numeric" | "decimal" => TypeInfo {
                numeric_precision: args.first().copied(),
                numeric_precision_radix: Some(10),
                numeric_scale: args.first().map(|_| args.get(1).copied().unwrap_or(0)),
                ..TypeInfo::named("numeric", "numeric")
            },
            "varchar" | "character varying" => TypeInfo {
                character_maximum_length: args.first().copied(),
                ..TypeInfo::named("character varying", "varchar")
            },
            "char" | "character" | "bpchar" => TypeInfo {
                character_maximum_length: Some(args.first().copied().unwrap_or(1)),
                ..TypeInfo::named("character", "bpchar")
            },
            "text" => TypeInfo::named("text", "text"),
            "bool" | "boolean" => TypeInfo::named("boolean", "bool"),
            "date" => TypeInfo {
                datetime_precision: Some(0),
                ..TypeInfo::named("date", "date")
            },
            "time" | "time without time zone" => TypeInfo::timed("time without time zone", "time"),
            "timetz" | "time with time zone" => TypeInfo::timed("time with time zone", "timetz"),
            "timestamp" | "timestamp without time zone" => {
                TypeInfo::timed("timestamp without time zone", "timestamp")
            }
            "timestamptz" | "timestamp with time zone" => {
                TypeInfo::timed("timestamp with time zone", "timestamptz")
            }
            "interval" => TypeInfo::timed("interval", "interval"),
            other => TypeInfo::named(other, other),
        };
        if matches!(
            base,
            "time" | "timetz" | "timestamp" | "timestamptz" | "interval"
        ) && let Some(&precision) = args.first()
        {
            info.datetime_precision = Some(precision);
        }
        info
    }

    fn named(data_type: &str, udt_name: &str) -> TypeInfo {
        TypeInfo {
            data_type: data_type.into(),
            udt_name: udt_name.into(),
            character_maximum_length: None,
            numeric_precision: None,
            numeric_precision_radix: None,
            numeric_scale: None,
            datetime_precision: None,
        }
    }

    fn timed(data_type: &str, udt_name: &str) -> TypeInfo {
        TypeInfo {
            datetime_precision: Some(6),
            ..TypeInfo::named(data_type, udt_name)
        }
    }

    fn with(self, data_type: &str, udt_name: &str) -> TypeInfo {
        TypeInfo {
            data_type: data_type.into(),
            udt_name: udt_name.into(),
            ..self
        }
    }
}
