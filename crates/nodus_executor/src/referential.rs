//! Foreign keys: a new or changed row must reference an existing key, and
//! removing or changing a referenced key applies the key's `ON DELETE` /
//! `ON UPDATE` action to the rows referencing it.

use crate::constraints::key_tuple;
use crate::error_fields::DbError;
use crate::{ExecutionContext, MemExecutor, Value, parse_object_name, render};
use anyhow::Result;
use nodus_catalog::{ReferentialAction, TableConstraint, TableDescriptor};

/// A foreign key of `child` resolved against the table it references.
pub(crate) struct Reference {
    pub(crate) name: String,
    pub(crate) child: TableDescriptor,
    pub(crate) child_positions: Vec<usize>,
    pub(crate) parent: TableDescriptor,
    pub(crate) parent_positions: Vec<usize>,
    pub(crate) on_delete: ReferentialAction,
    pub(crate) on_update: ReferentialAction,
}

/// A table's columns and the keys a foreign key may reference: its
/// primary key and its unique constraints, by column names.
pub(crate) struct Keys {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) primary: Vec<String>,
    pub(crate) unique: Vec<Vec<String>>,
}

impl Keys {
    /// The keys of a table in the catalog.
    pub(crate) fn of(table: &TableDescriptor) -> Keys {
        let names = |ids: &[nodus_catalog::IndexColumn]| -> Vec<String> {
            ids.iter()
                .filter_map(|k| table.columns.iter().find(|c| c.id == k.column_id))
                .map(|c| c.name.clone())
                .collect()
        };
        let primary = MemExecutor::pk_positions_declared(table)
            .into_iter()
            .map(|p| table.columns[p].name.clone())
            .collect();
        let unique = table
            .indexes
            .iter()
            .filter(|i| {
                i.unique
                    && i.index_type != nodus_catalog::IndexType::Primary
                    && i.predicate.is_none()
                    && i.expressions.is_empty()
            })
            .map(|i| names(&i.key_columns))
            .collect();
        Keys {
            name: table.name.clone(),
            columns: table.columns.iter().map(|c| c.name.clone()).collect(),
            primary,
            unique,
        }
    }

    /// Whether `columns` are the columns of one of the keys.
    fn has_key(&self, columns: &[String]) -> bool {
        let same = |key: &Vec<String>| {
            key.len() == columns.len() && key.iter().all(|k| columns.contains(k))
        };
        (!self.primary.is_empty() && same(&self.primary)) || self.unique.iter().any(same)
    }
}

/// The name PostgreSQL gives an unnamed foreign key.
pub(crate) fn foreign_key_name(table: &str, columns: &[String]) -> String {
    format!("{table}_{}_fkey", columns.join("_"))
}

/// Values as a key's DETAIL lists them: `1, x`.
fn listed(values: &[Value]) -> String {
    values.iter().map(render).collect::<Vec<_>>().join(", ")
}

/// Column names as a key's DETAIL lists them: `a, b`.
fn listed_columns(table: &TableDescriptor, positions: &[usize]) -> String {
    positions
        .iter()
        .map(|&p| table.columns[p].name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether two keys are equal, value by value.
fn same_key(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| crate::values_equal(x, y))
}

/// A key as a set member: equal keys give equal strings.
fn key_string(key: &[Value]) -> String {
    key.iter()
        .map(|v| render(&crate::value::key_form(v)))
        .collect::<Vec<_>>()
        .join("\u{1}")
}

impl MemExecutor {
    /// Completes and checks `fk`, a foreign key of the table `child` is
    /// the keys of: it gets its name, a key naming no referenced columns
    /// references the primary key, and the referenced columns must be a
    /// primary key or unique constraint of the referenced table.
    pub(crate) fn prepare_foreign_key(
        &self,
        child: &Keys,
        child_schema: &str,
        fk: TableConstraint,
    ) -> Result<TableConstraint> {
        let TableConstraint::ForeignKey {
            name,
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            on_update,
        } = fk
        else {
            return Ok(fk);
        };
        for column in &columns {
            if !child.columns.contains(column) {
                anyhow::bail!(
                    "column \"{column}\" referenced in foreign key constraint does not exist"
                );
            }
        }
        let (db, schema, table) =
            parse_object_name(&foreign_table).unwrap_or(("default", "public", &foreign_table));
        let parent = if table == child.name && schema == child_schema {
            None
        } else {
            Some(
                self.catalog_reader
                    .get_table(db, schema, table)
                    .map_err(|_| anyhow::anyhow!("relation \"{table}\" does not exist"))?,
            )
        };
        let parent_keys = parent.as_ref().map(Keys::of);
        let parent_keys = parent_keys.as_ref().unwrap_or(child);
        let referred_columns = if referred_columns.is_empty() {
            if parent_keys.primary.is_empty() {
                anyhow::bail!("there is no primary key for referenced table \"{table}\"");
            }
            parent_keys.primary.clone()
        } else {
            for column in &referred_columns {
                if !parent_keys.columns.contains(column) {
                    anyhow::bail!(
                        "column \"{column}\" referenced in foreign key constraint does not exist"
                    );
                }
            }
            referred_columns
        };
        if referred_columns.len() != columns.len() {
            anyhow::bail!("number of referencing and referenced columns for foreign key disagree");
        }
        if !parent_keys.has_key(&referred_columns) {
            anyhow::bail!(
                "there is no unique constraint matching given keys for referenced table \"{table}\""
            );
        }
        Ok(TableConstraint::ForeignKey {
            name: Some(name.unwrap_or_else(|| foreign_key_name(&child.name, &columns))),
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            on_update,
        })
    }

    /// The name of the schema `table` is in.
    pub(crate) fn schema_name_of(&self, table: &TableDescriptor) -> String {
        self.catalog_reader
            .get_schema_by_id(table.schema_id)
            .map(|s| s.name)
            .unwrap_or_else(|_| "public".to_string())
    }

    /// `fk`, a foreign key of `child`, resolved against the table it names.
    fn resolve_reference(
        &self,
        child: &TableDescriptor,
        fk: &TableConstraint,
    ) -> Result<Option<Reference>> {
        let TableConstraint::ForeignKey {
            name,
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            on_update,
        } = fk
        else {
            return Ok(None);
        };
        let (db, schema, table) =
            parse_object_name(foreign_table).unwrap_or(("default", "public", foreign_table));
        let parent = if table == child.name && schema == self.schema_name_of(child) {
            child.clone()
        } else {
            self.catalog_reader.get_table(db, schema, table)?
        };
        let position = |t: &TableDescriptor, column: &String| {
            t.columns
                .iter()
                .position(|c| &c.name == column)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "column \"{column}\" of relation \"{}\" does not exist",
                        t.name
                    )
                })
        };
        let child_positions = columns
            .iter()
            .map(|c| position(child, c))
            .collect::<Result<Vec<_>>>()?;
        // A key written without its columns references the primary key.
        let parent_positions = if referred_columns.is_empty() {
            Self::pk_positions_declared(&parent)
        } else {
            referred_columns
                .iter()
                .map(|c| position(&parent, c))
                .collect::<Result<Vec<_>>>()?
        };
        if child_positions.len() != parent_positions.len() {
            anyhow::bail!("number of referencing and referenced columns for foreign key disagree");
        }
        Ok(Some(Reference {
            name: name
                .clone()
                .unwrap_or_else(|| foreign_key_name(&child.name, columns)),
            child: child.clone(),
            child_positions,
            parent,
            parent_positions,
            on_delete: *on_delete,
            on_update: *on_update,
        }))
    }

    /// Whether a foreign key references `table`.
    pub(crate) fn is_referenced(&self, table: &TableDescriptor) -> Result<bool> {
        Ok(!self.references_to(table)?.is_empty())
    }

    /// The foreign keys of every table that reference `parent`.
    /// They come in the order the referencing tables were created.
    pub(crate) fn references_to(&self, parent: &TableDescriptor) -> Result<Vec<Reference>> {
        let mut references = Vec::new();
        let mut tables = self.catalog_reader.list_all_tables("default")?;
        tables.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.name.cmp(&b.name)));
        for table in tables {
            for constraint in &table.constraints {
                if let TableConstraint::ForeignKey { foreign_table, .. } = constraint {
                    let names_parent = parse_object_name(foreign_table)
                        .map(|(_, _, t)| t == parent.name)
                        .unwrap_or(false);
                    if !names_parent {
                        continue;
                    }
                    if let Some(reference) = self.resolve_reference(&table, constraint)?
                        && reference.parent.id == parent.id
                    {
                        references.push(reference);
                    }
                }
            }
        }
        Ok(references)
    }

    /// Rejects `row`, a new row of `tbl` or one replacing `old_row`, when a
    /// foreign key of `tbl` finds no row with the key it references. A key
    /// with a NULL part references nothing, and a key a change leaves as it
    /// was is not checked again.
    pub(crate) fn check_references_from(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        row: &[Value],
        old_row: Option<&[Value]>,
    ) -> Result<()> {
        for constraint in &tbl.constraints {
            let Some(reference) = self.resolve_reference(tbl, constraint)? else {
                continue;
            };
            let Some(key) = key_tuple(row, &reference.child_positions) else {
                continue;
            };
            let unchanged = old_row
                .and_then(|old| key_tuple(old, &reference.child_positions))
                .is_some_and(|old| same_key(&old, &key));
            if unchanged {
                continue;
            }
            // A row may reference itself.
            let self_reference = reference.parent.id == tbl.id
                && key_tuple(row, &reference.parent_positions)
                    .is_some_and(|own| same_key(&own, &key));
            if self_reference {
                continue;
            }
            let found = self
                .scan_rows(reference.parent.id, &ctx.session_id)?
                .iter()
                .any(|parent| {
                    key_tuple(parent, &reference.parent_positions)
                        .is_some_and(|k| same_key(&k, &key))
                });
            if !found {
                return Err(DbError::new(format!(
                    "insert or update on table \"{}\" violates foreign key constraint \"{}\"",
                    tbl.name, reference.name
                ))
                .detail(format!(
                    "Key ({})=({}) is not present in table \"{}\".",
                    listed_columns(tbl, &reference.child_positions),
                    listed(&key),
                    reference.parent.name
                ))
                .schema(self.schema_name_of(tbl))
                .table(&tbl.name)
                .constraint(&reference.name)
                .into());
            }
        }
        Ok(())
    }

    /// The error for a removed or changed key of `reference`'s parent that
    /// a row still references.
    fn still_referenced(&self, reference: &Reference, key: &[Value]) -> anyhow::Error {
        DbError::new(format!(
            "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"",
            reference.parent.name, reference.name, reference.child.name
        ))
        .detail(format!(
            "Key ({})=({}) is still referenced from table \"{}\".",
            listed_columns(&reference.parent, &reference.parent_positions),
            listed(key),
            reference.child.name
        ))
        .schema(self.schema_name_of(&reference.child))
        .table(&reference.child.name)
        .constraint(&reference.name)
        .into()
    }

    /// The error for a removed or changed key of `reference`'s parent that
    /// a row references, under ON DELETE / ON UPDATE RESTRICT.
    fn restricted(&self, reference: &Reference, key: &[Value]) -> anyhow::Error {
        DbError::new(format!(
            "update or delete on table \"{}\" violates RESTRICT setting of foreign key constraint \"{}\" on table \"{}\"",
            reference.parent.name, reference.name, reference.child.name
        ))
        .detail(format!(
            "Key ({})=({}) is referenced from table \"{}\".",
            listed_columns(&reference.parent, &reference.parent_positions),
            listed(key),
            reference.child.name
        ))
        .schema(self.schema_name_of(&reference.child))
        .table(&reference.child.name)
        .constraint(&reference.name)
        .into()
    }

    /// Applies the foreign keys that reference `tbl` to the rows a statement
    /// removed from it and the rows it changed (as old and new): the rows
    /// referencing a key that is gone are deleted, updated along, or set to
    /// NULL or their defaults, as each key says, and then no reference may
    /// be left to a key `tbl` no longer has.
    pub(crate) fn enforce_references(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        removed: &[Vec<Value>],
        changed: &[(Vec<Value>, Vec<Value>)],
    ) -> Result<()> {
        if removed.is_empty() && changed.is_empty() {
            return Ok(());
        }
        let references = self.references_to(tbl)?;
        let mut unresolved = Vec::new();
        for reference in &references {
            // The keys that are gone, with the key each changed to.
            let gone: Vec<(Vec<Value>, Option<Vec<Value>>)> = removed
                .iter()
                .filter_map(|row| key_tuple(row, &reference.parent_positions).map(|k| (k, None)))
                .chain(changed.iter().filter_map(|(old, new)| {
                    let old_key = key_tuple(old, &reference.parent_positions)?;
                    let new_key: Vec<Value> = reference
                        .parent_positions
                        .iter()
                        .map(|&p| new.get(p).cloned().unwrap_or(Value::Null))
                        .collect();
                    (!same_key(&old_key, &new_key)).then_some((old_key, Some(new_key)))
                }))
                .collect();
            if gone.is_empty() {
                continue;
            }
            let mut child_removed = Vec::new();
            let mut child_changed = Vec::new();
            for (key, row) in self.scan_rows_keyed(reference.child.id, &ctx.session_id)? {
                let Some(tuple) = key_tuple(&row, &reference.child_positions) else {
                    continue;
                };
                let Some((old_key, new_key)) = gone.iter().find(|(k, _)| same_key(k, &tuple))
                else {
                    continue;
                };
                let action = if new_key.is_some() {
                    reference.on_update
                } else {
                    reference.on_delete
                };
                let mut updated = row.clone();
                match action {
                    ReferentialAction::NoAction => continue,
                    ReferentialAction::Restrict => {
                        return Err(self.restricted(reference, old_key));
                    }
                    ReferentialAction::Cascade => match new_key {
                        None => {
                            self.remove_row(ctx, &reference.child, &key, &row)?;
                            child_removed.push(row);
                            continue;
                        }
                        Some(new_key) => {
                            for (&p, value) in reference.child_positions.iter().zip(new_key) {
                                updated[p] = value.clone();
                            }
                        }
                    },
                    ReferentialAction::SetNull => {
                        for &p in &reference.child_positions {
                            updated[p] = Value::Null;
                        }
                    }
                    ReferentialAction::SetDefault => {
                        for &p in &reference.child_positions {
                            let column = &reference.child.columns[p];
                            updated[p] = Self::column_default(column)
                                .filter(|d| Self::generation_expr(d).is_none())
                                .map(|d| crate::eval_scalar_expr(&d, &[], &[]))
                                .map(|v| crate::value::coerce_for_column(&v, &column.data_type))
                                .unwrap_or(Value::Null);
                        }
                    }
                }
                self.check_not_null(&reference.child, &updated)?;
                self.replace_row(ctx, &reference.child, &key, &row, &updated)?;
                child_changed.push((row, updated));
            }
            let child = self.table_now(&reference.child)?;
            self.enforce_references(ctx, &child, &child_removed, &child_changed)?;
            if reference.on_delete == ReferentialAction::NoAction
                || reference.on_update == ReferentialAction::NoAction
            {
                unresolved.push(reference);
            }
        }
        // NO ACTION: at the end of the statement no row may reference a key
        // that is gone.
        for reference in unresolved {
            let keys: std::collections::HashSet<String> = self
                .scan_rows(tbl.id, &ctx.session_id)?
                .iter()
                .filter_map(|row| key_tuple(row, &reference.parent_positions))
                .map(|k| key_string(&k))
                .collect();
            for row in self.scan_rows(reference.child.id, &ctx.session_id)? {
                if let Some(tuple) = key_tuple(&row, &reference.child_positions)
                    && !keys.contains(&key_string(&tuple))
                {
                    return Err(self.still_referenced(reference, &tuple));
                }
            }
        }
        Ok(())
    }

    /// The current descriptor of `table`, which a statement may have changed.
    fn table_now(&self, table: &TableDescriptor) -> Result<TableDescriptor> {
        self.catalog_reader.get_table_by_id(table.id)
    }
}
