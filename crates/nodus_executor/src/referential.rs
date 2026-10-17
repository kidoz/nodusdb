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
    /// `DEFERRABLE INITIALLY DEFERRED` of the constraint (a `NO ACTION`
    /// check may then wait for `COMMIT`).
    pub(crate) deferrable: bool,
    pub(crate) initially_deferred: bool,
    /// `PERIOD`: the key's last column on each side is a period, and a
    /// parent row covers a child's when its period holds the child's.
    pub(crate) period: bool,
}

/// A table's columns and the keys a foreign key may reference: its
/// primary key and its unique constraints, by column names.
pub(crate) struct Keys {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    /// The columns' declared types, parallel to `columns`.
    pub(crate) types: Vec<String>,
    pub(crate) primary: Vec<String>,
    pub(crate) unique: Vec<Vec<String>>,
    /// The columns of the keys built `WITHOUT OVERLAPS`, primary and
    /// unique: only these may back a `PERIOD` foreign key.
    pub(crate) temporal: Vec<Vec<String>>,
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
        let primary: Vec<String> = MemExecutor::pk_positions_declared(table)
            .into_iter()
            .map(|p| table.columns[p].name.clone())
            .collect();
        let unique: Vec<Vec<String>> = table
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
        let mut temporal = Vec::new();
        if table
            .indexes
            .iter()
            .any(|i| i.index_type == nodus_catalog::IndexType::Primary && i.period.is_some())
        {
            temporal.push(primary.clone());
        }
        temporal.extend(
            table
                .indexes
                .iter()
                .filter(|i| {
                    i.unique
                        && i.index_type != nodus_catalog::IndexType::Primary
                        && i.period.is_some()
                })
                .map(|i| names(&i.key_columns)),
        );
        Keys {
            name: table.name.clone(),
            columns: table.columns.iter().map(|c| c.name.clone()).collect(),
            types: table.columns.iter().map(|c| c.data_type.clone()).collect(),
            primary,
            unique,
            temporal,
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

/// Whether the parent rows' periods, merged, hold the child's: PostgreSQL's
/// temporal foreign key test, where a child may span several adjacent
/// parent rows.
fn periods_cover(reference: &Reference, parents: &[Vec<Value>], child: &[Value]) -> bool {
    let n = child.len();
    if n == 0 || parents.iter().any(|parent| parent.len() != n) || child[n - 1] == Value::Null {
        return false;
    }
    let data_type = reference
        .parent
        .columns
        .get(reference.parent_positions[n - 1])
        .map_or("", |c| c.data_type.as_str());
    let range_kind = crate::ranges::Kind::of(data_type);
    let kind = match range_kind.or_else(|| crate::multiranges::kind_of(data_type)) {
        Some(kind) => kind,
        None => return false,
    };
    let mut elements = Vec::new();
    for parent in parents {
        if !parent[..n - 1]
            .iter()
            .zip(&child[..n - 1])
            .all(|(p, c)| crate::values_equal(p, c))
        {
            continue;
        }
        let Value::Text(text) = &parent[n - 1] else {
            continue;
        };
        if range_kind.is_some() {
            if let Ok(range) = crate::ranges::canonical_range(kind, text) {
                elements.push(range);
            }
        } else {
            elements.extend(crate::multiranges::parse(text).unwrap_or_default());
        }
    }
    let union = crate::multiranges::format(&crate::multiranges::merge(kind, elements));
    let Value::Text(child_period) = &child[n - 1] else {
        return false;
    };
    matches!(
        crate::multiranges::operator("@>", kind, &union, child_period, false),
        Ok(Value::Bool(true))
    )
}

/// A key as a set member: equal keys give equal strings.
pub(crate) fn key_string(key: &[Value]) -> String {
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
            deferrable,
            initially_deferred,
            enforced,
            period,
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
        // A `PERIOD` key: only NO ACTION is supported, the referenced key
        // must be a temporal one, and the two period columns must be ranges
        // of the same kind.
        if period {
            if on_delete != ReferentialAction::NoAction {
                anyhow::bail!(
                    "unsupported ON DELETE action for foreign key constraint using PERIOD"
                );
            }
            if on_update != ReferentialAction::NoAction {
                anyhow::bail!(
                    "unsupported ON UPDATE action for foreign key constraint using PERIOD"
                );
            }
            let effective = name
                .clone()
                .unwrap_or_else(|| foreign_key_name(&child.name, &columns));
            // The referenced columns must be a temporal key's.
            let mut wanted = referred_columns.clone();
            wanted.sort_unstable();
            let temporal_key = parent_keys.temporal.iter().any(|key| {
                let mut columns = key.clone();
                columns.sort_unstable();
                columns == wanted
            });
            if !temporal_key {
                anyhow::bail!(
                    "there is no unique constraint matching given keys for referenced table \"{table}\""
                );
            }
            let period_column = referred_columns.last().cloned().unwrap_or_default();
            let child_period = columns.last().cloned().unwrap_or_default();
            let child_type = child
                .columns
                .iter()
                .position(|c| c == &child_period)
                .and_then(|at| child.types.get(at))
                .cloned()
                .unwrap_or_default();
            let parent_type = parent_keys
                .columns
                .iter()
                .position(|c| c == &period_column)
                .and_then(|at| parent_keys.types.get(at))
                .cloned()
                .unwrap_or_default();
            if !period_kinds_match(&child_type, &parent_type) {
                let shown = |t: &str| crate::functions::format_type_name(Self::pg_type_oid(t));
                return Err(DbError::new(format!(
                    "foreign key constraint \"{effective}\" cannot be implemented"
                ))
                .detail(format!(
                    "Key columns \"{child_period}\" of the referencing table and \"{period_column}\" of the referenced table are of incompatible types: {} and {}.",
                    shown(&child_type),
                    shown(&parent_type)
                ))
                .into());
            }
        }
        // The referenced key may not be a deferrable one.
        if let Some(parent) = &parent {
            let names_of = |i: &nodus_catalog::IndexDescriptor| -> Vec<String> {
                let mut names: Vec<String> = i
                    .key_columns
                    .iter()
                    .filter_map(|k| {
                        parent
                            .columns
                            .iter()
                            .find(|c| c.id == k.column_id)
                            .map(|c| c.name.clone())
                    })
                    .collect();
                names.sort_unstable();
                names
            };
            let mut referred = referred_columns.clone();
            referred.sort_unstable();
            let primary: Vec<&nodus_catalog::IndexDescriptor> = parent
                .indexes
                .iter()
                .filter(|i| i.index_type == nodus_catalog::IndexType::Primary)
                .collect();
            let primary_deferrable = primary.iter().any(|i| i.deferrable);
            let deferrable_keys: Vec<Vec<String>> = parent
                .indexes
                .iter()
                .filter(|i| i.unique && i.deferrable)
                .map(&names_of)
                .collect();
            // A composite primary key is one index per column; its whole
            // column set counts as the key.
            if primary_deferrable {
                let mut whole: Vec<String> =
                    primary.iter().flat_map(|i| names_of(i)).collect::<Vec<_>>();
                whole.sort_unstable();
                whole.dedup();
                if whole == referred {
                    anyhow::bail!(
                        "cannot use a deferrable unique constraint for referenced table \"{table}\""
                    );
                }
            }
            if deferrable_keys.iter().any(|key| *key == referred) {
                anyhow::bail!(
                    "cannot use a deferrable unique constraint for referenced table \"{table}\""
                );
            }
        }
        Ok(TableConstraint::ForeignKey {
            name: Some(name.unwrap_or_else(|| foreign_key_name(&child.name, &columns))),
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            on_update,
            deferrable,
            initially_deferred,
            enforced,
            period,
        })
    }

    /// The name of the schema `table` is in.
    pub(crate) fn schema_name_of(&self, table: &TableDescriptor) -> String {
        self.catalog_reader
            .get_schema_by_id(table.schema_id)
            .map(|s| s.name)
            .unwrap_or_else(|_| "public".to_string())
    }

    /// Re-checks a deferred foreign key: every row of the child table must
    /// find its referenced key (PostgreSQL raises the violation when the
    /// constraint stops being deferred).
    pub(crate) fn check_reference_holds(
        &self,
        ctx: &ExecutionContext,
        reference: &Reference,
    ) -> Result<()> {
        if reference.period {
            let parents: Vec<Vec<Value>> = self
                .scan_rows(reference.parent.id, &ctx.session_id)?
                .iter()
                .filter_map(|row| key_tuple(row, &reference.parent_positions))
                .collect();
            for row in self.scan_rows(reference.child.id, &ctx.session_id)? {
                let Some(tuple) = key_tuple(&row, &reference.child_positions) else {
                    continue;
                };
                if !periods_cover(reference, &parents, &tuple) {
                    return Err(self.missing_key_error(&reference.child, reference, &tuple));
                }
            }
            return Ok(());
        }
        let keys: std::collections::HashSet<String> = self
            .scan_rows(reference.parent.id, &ctx.session_id)?
            .iter()
            .filter_map(|row| key_tuple(row, &reference.parent_positions))
            .map(|key| key_string(&key))
            .collect();
        for row in self.scan_rows(reference.child.id, &ctx.session_id)? {
            let Some(tuple) = key_tuple(&row, &reference.child_positions) else {
                continue;
            };
            if !keys.contains(&key_string(&tuple)) {
                return Err(self.missing_key_error(&reference.child, reference, &tuple));
            }
        }
        Ok(())
    }

    /// The error for a child row whose referenced key does not exist.
    fn missing_key_error(
        &self,
        child: &TableDescriptor,
        reference: &Reference,
        key: &[Value],
    ) -> anyhow::Error {
        DbError::new(format!(
            "insert or update on table \"{}\" violates foreign key constraint \"{}\"",
            child.name, reference.name
        ))
        .detail(format!(
            "Key ({})=({}) is not present in table \"{}\".",
            listed_columns(child, &reference.child_positions),
            listed(key),
            reference.parent.name
        ))
        .schema(self.schema_name_of(child))
        .table(&child.name)
        .constraint(&reference.name)
        .into()
    }

    /// `fk`, a foreign key of `child`, resolved against the table it names.
    pub(crate) fn resolve_reference(
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
            deferrable,
            initially_deferred,
            enforced,
            period,
        } = fk
        else {
            return Ok(None);
        };
        // `NOT ENFORCED`: the key is declared, not checked.
        if !enforced {
            return Ok(None);
        }
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
            deferrable: *deferrable,
            initially_deferred: *initially_deferred,
            period: *period,
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
            // A deferred key is checked when the transaction commits.
            if self.constraint_deferred(
                &ctx.session_id,
                &reference.name,
                reference.deferrable,
                reference.initially_deferred,
            ) {
                self.note_deferred_write(&ctx.session_id, tbl.id);
                continue;
            }
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
            let parents = self.scan_rows(reference.parent.id, &ctx.session_id)?;
            let found = if reference.period {
                let parent_keys: Vec<Vec<Value>> = parents
                    .iter()
                    .filter_map(|parent| key_tuple(parent, &reference.parent_positions))
                    .collect();
                periods_cover(&reference, &parent_keys, &key)
            } else {
                parents.iter().any(|parent| {
                    key_tuple(parent, &reference.parent_positions)
                        .is_some_and(|k| same_key(&k, &key))
                })
            };
            if !found {
                return Err(self.missing_key_error(tbl, &reference, &key));
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
            // A temporal key's only action is NO ACTION; the check after
            // the writes below decides.
            if reference.period {
                unresolved.push(reference);
                continue;
            }
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
        // that is gone — a deferred key waits for COMMIT instead.
        for reference in unresolved {
            if self.constraint_deferred(
                &ctx.session_id,
                &reference.name,
                reference.deferrable,
                reference.initially_deferred,
            ) {
                self.note_deferred_write(&ctx.session_id, reference.child.id);
                continue;
            }
            let parents: Vec<Vec<Value>> = self
                .scan_rows(tbl.id, &ctx.session_id)?
                .iter()
                .filter_map(|row| key_tuple(row, &reference.parent_positions))
                .collect();
            if reference.period {
                // The gone keys, as PostgreSQL's DETAIL reports them.
                let gone: Vec<Vec<Value>> = removed
                    .iter()
                    .filter_map(|row| key_tuple(row, &reference.parent_positions))
                    .chain(
                        changed
                            .iter()
                            .filter_map(|(old, _)| key_tuple(old, &reference.parent_positions)),
                    )
                    .collect();
                let child_period_type = reference
                    .child
                    .columns
                    .get(*reference.child_positions.last().unwrap_or(&0))
                    .map_or("", |c| c.data_type.as_str());
                for row in self.scan_rows(reference.child.id, &ctx.session_id)? {
                    let Some(tuple) = key_tuple(&row, &reference.child_positions) else {
                        continue;
                    };
                    if periods_cover(reference, &parents, &tuple) {
                        continue;
                    }
                    // A gone key that met the child, as PostgreSQL names it.
                    let key = gone
                        .iter()
                        .find(|key| {
                            key.len() == tuple.len()
                                && key[..key.len() - 1]
                                    .iter()
                                    .zip(&tuple[..tuple.len() - 1])
                                    .all(|(a, b)| crate::values_equal(a, b))
                                && match (&key[key.len() - 1], &tuple[tuple.len() - 1]) {
                                    (Value::Text(a), Value::Text(b)) => {
                                        crate::constraints::ranges_overlap(
                                            child_period_type,
                                            &Value::Text(a.clone()),
                                            &Value::Text(b.clone()),
                                        )
                                    }
                                    _ => false,
                                }
                        })
                        .unwrap_or(&tuple);
                    return Err(self.still_referenced(reference, key));
                }
                continue;
            }
            let keys: std::collections::HashSet<String> =
                parents.iter().map(|k| key_string(k)).collect();
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

/// Whether two period columns' types are comparable: ranges of the same
/// kind, or multiranges of the same kind.
fn period_kinds_match(a: &str, b: &str) -> bool {
    if let (Some(x), Some(y)) = (crate::ranges::Kind::of(a), crate::ranges::Kind::of(b)) {
        return x == y;
    }
    if let (Some(x), Some(y)) = (
        crate::multiranges::kind_of(a),
        crate::multiranges::kind_of(b),
    ) {
        return x == y;
    }
    false
}
