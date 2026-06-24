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
            "constraint_column_usage" => Some(self.information_schema_constraint_column_usage()),
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

    pub(crate) fn information_schema_constraint_column_usage(
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
