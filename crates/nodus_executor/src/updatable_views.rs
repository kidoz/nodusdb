//! Automatically updatable views. `INSERT`, `UPDATE`, and `DELETE` on a
//! view that reads one table (without aggregates, grouping, `DISTINCT`,
//! `LIMIT`, `WITH`, or set operations) write that table, as PostgreSQL's
//! views do. The view's columns that are its table's columns can be
//! written; its condition picks the rows an `UPDATE` or `DELETE` sees; and
//! with `WITH CHECK OPTION`, a row written through it must meet the
//! condition. Conditions and `RETURNING` read the rows as the view shows
//! them.

use anyhow::Result;
use nodus_authz::Action;
use nodus_catalog::{ResourceRef, TableDescriptor};

use crate::dml::Returning;
use crate::error_fields::DbError;
use crate::{
    ExecutionContext, FilterExpr, LogicalPlan, MemExecutor, ProjectionItem, QueryOutput,
    ScalarExpr, Value, parse_object_name,
};

/// What a statement does to a view, for its errors.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ViewCommand {
    Insert,
    Update,
    Delete,
}

impl ViewCommand {
    fn verb(self) -> &'static str {
        match self {
            Self::Insert => "insert into",
            Self::Update => "update",
            Self::Delete => "delete from",
        }
    }

    fn keyword(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }

    fn gerund(self) -> &'static str {
        match self {
            Self::Insert => "inserting into",
            Self::Update => "updating",
            Self::Delete => "deleting from",
        }
    }
}

/// A view as a statement writes it: the table it reads in the end, through
/// the views it reads (each a level, from the view written inward).
pub(crate) struct WritableView {
    pub(crate) view: TableDescriptor,
    pub(crate) base: TableDescriptor,
    levels: Vec<ViewLevel>,
    /// For each of the view's columns, the table column it shows, if it
    /// shows one rather than an expression.
    columns: Vec<Option<usize>>,
}

/// One view of the chain a written view reads its table through.
struct ViewLevel {
    view: TableDescriptor,
    /// The view's query, without its check option.
    plan: LogicalPlan,
    /// The relation the query reads, as it names it.
    base_ref: String,
    filter: Option<FilterExpr>,
    /// For each of the view's columns, the column of the relation it reads
    /// that it shows, if it shows one.
    columns: Vec<Option<usize>>,
    check_option: Option<String>,
}

/// The parts of a view's query that decide whether it can be written, or
/// why not (PostgreSQL's `DETAIL`).
struct ViewShape {
    plan: LogicalPlan,
    check_option: Option<String>,
}

fn view_shape(query: &str) -> Result<std::result::Result<ViewShape, &'static str>> {
    let plan: LogicalPlan = serde_json::from_str(query)?;
    let (plan, check_option) = match plan {
        LogicalPlan::ViewCheckOption { query, option } => (*query, Some(option)),
        other => (other, None),
    };
    let plan = match plan {
        LogicalPlan::Renamed { input, .. } => *input,
        other => other,
    };
    let not_single =
        "Views that do not select from a single table or view are not automatically updatable.";
    let reason = match &plan {
        LogicalPlan::Select {
            ctes,
            joins,
            projection,
            group_by,
            having,
            grouping_sets,
            limit,
            offset,
            distinct,
            group_exprs,
            distinct_on,
            ..
        } => {
            let aggregate = projection.iter().any(|item| match item {
                ProjectionItem::Aggregate(..) => true,
                ProjectionItem::Expr { expr, .. } => crate::scalar_has_aggregate(expr),
                _ => false,
            });
            let window = projection.iter().any(|item| match item {
                ProjectionItem::WindowFunction { .. } => true,
                ProjectionItem::Expr { expr, .. } => crate::planner::scalar_has_window(expr),
                _ => false,
            });
            if *distinct || !distinct_on.is_empty() {
                Some("Views containing DISTINCT are not automatically updatable.")
            } else if !group_by.is_empty() || grouping_sets.is_some() || !group_exprs.is_empty() {
                Some("Views containing GROUP BY are not automatically updatable.")
            } else if having.is_some() {
                Some("Views containing HAVING are not automatically updatable.")
            } else if !ctes.is_empty() {
                Some("Views containing WITH are not automatically updatable.")
            } else if limit.is_some() || offset.is_some() {
                Some("Views containing LIMIT or OFFSET are not automatically updatable.")
            } else if aggregate {
                Some("Views that return aggregate functions are not automatically updatable.")
            } else if window {
                Some("Views that return window functions are not automatically updatable.")
            } else if !joins.is_empty() {
                Some(not_single)
            } else {
                None
            }
        }
        LogicalPlan::SetOp { .. } => {
            Some("Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.")
        }
        LogicalPlan::With { .. } => Some("Views containing WITH are not automatically updatable."),
        _ => Some(not_single),
    };
    Ok(match reason {
        Some(reason) => Err(reason),
        None => Ok(ViewShape { plan, check_option }),
    })
}

/// A view's `information_schema.views` facts: whether it can be written,
/// and its check option (`NONE`, `LOCAL`, or `CASCADED`).
pub(crate) fn view_facts(view: &TableDescriptor) -> (bool, &'static str) {
    let Some(query) = &view.view_query else {
        return (false, "NONE");
    };
    match view_shape(query) {
        Ok(Ok(shape)) => (
            true,
            match shape.check_option.as_deref() {
                Some("local") => "LOCAL",
                Some(_) => "CASCADED",
                None => "NONE",
            },
        ),
        _ => (false, "NONE"),
    }
}

impl MemExecutor {
    /// `view` as `command` writes it, or PostgreSQL's error for a view it
    /// cannot write.
    pub(crate) fn writable_view(
        &self,
        view: &TableDescriptor,
        command: ViewCommand,
    ) -> Result<WritableView> {
        let query = view.view_query.as_deref().unwrap_or_default();
        let refuse = |detail: &str| -> anyhow::Error {
            DbError::new(format!("cannot {} view \"{}\"", command.verb(), view.name))
                .code("55000")
                .detail(detail)
                .hint(format!(
                    "To enable {} the view, provide an INSTEAD OF {} trigger or an unconditional ON {} DO INSTEAD rule.",
                    command.gerund(),
                    command.keyword(),
                    command.keyword()
                ))
                .into()
        };
        let shape = match view_shape(query)? {
            Ok(shape) => shape,
            Err(detail) => return Err(refuse(detail)),
        };
        let LogicalPlan::Select {
            table_name,
            projection,
            filter,
            ..
        } = &shape.plan
        else {
            unreachable!("view_shape accepts only a SELECT");
        };
        let (db_name, schema_name, table_only) = parse_object_name(table_name)?;
        let base = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        if base.materialized_query.is_some() || crate::sequences::is_sequence(&base) {
            return Err(refuse(
                "Views that do not select from a single table or view are not automatically updatable.",
            ));
        }
        // A view over a view writes through it.
        let inner = match base.view_query {
            Some(_) => Some(self.writable_view(&base, command)?),
            None => None,
        };
        let position = |name: &str| {
            let name = name.rsplit('.').next().unwrap_or(name);
            base.columns.iter().position(|c| c.name == name)
        };
        let mut columns = Vec::new();
        if projection.is_empty() {
            columns.extend((0..base.columns.len()).map(Some));
        }
        for item in projection {
            match item {
                ProjectionItem::Column(c) if c == "*" || c.ends_with(".*") => {
                    columns.extend((0..base.columns.len()).map(Some));
                }
                ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
                    columns.push(position(c));
                }
                ProjectionItem::Expr {
                    expr: ScalarExpr::Column(c),
                    ..
                } => columns.push(position(c)),
                _ => columns.push(None),
            }
        }
        columns.resize(view.columns.len(), None);
        let level = ViewLevel {
            view: view.clone(),
            plan: shape.plan.clone(),
            base_ref: table_name.clone(),
            filter: filter.clone(),
            columns: columns.clone(),
            check_option: shape.check_option,
        };
        Ok(match inner {
            Some(inner) => {
                let mut levels = vec![level];
                levels.extend(inner.levels);
                WritableView {
                    view: view.clone(),
                    base: inner.base,
                    levels,
                    columns: columns
                        .iter()
                        .map(|c| c.and_then(|c| inner.columns[c]))
                        .collect(),
                }
            }
            None => WritableView {
                view: view.clone(),
                base,
                levels: vec![level],
                columns,
            },
        })
    }

    /// The columns of the relation a level reads: the next level's view's,
    /// or the table's.
    fn level_input<'a>(wv: &'a WritableView, at: usize) -> &'a [nodus_catalog::ColumnDescriptor] {
        wv.levels
            .get(at + 1)
            .map_or(&wv.base.columns, |next| &next.view.columns)
    }

    /// Rows of the relation a level reads as the level's view shows them.
    fn level_rows(
        &self,
        ctx: &ExecutionContext,
        wv: &WritableView,
        at: usize,
        rows: &[Vec<Value>],
    ) -> Result<Vec<Vec<Value>>> {
        let level = &wv.levels[at];
        if level.columns.iter().all(Option::is_some) {
            return Ok(rows
                .iter()
                .map(|row| {
                    level
                        .columns
                        .iter()
                        .map(|p| row[p.unwrap_or_default()].clone())
                        .collect()
                })
                .collect());
        }
        // The view's query over just these rows, in their order.
        let LogicalPlan::Select {
            table_name,
            table_alias,
            projection,
            sample,
            ..
        } = &level.plan
        else {
            return Ok(Vec::new());
        };
        let plan = LogicalPlan::Select {
            only: false,
            ctes: Vec::new(),
            table_name: table_name.clone(),
            table_alias: table_alias.clone(),
            joins: Vec::new(),
            projection: projection.clone(),
            group_by: Vec::new(),
            filter: None,
            having: None,
            grouping_sets: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct: false,
            sort: Vec::new(),
            group_exprs: Vec::new(),
            distinct_on: Vec::new(),
            sample: sample.clone(),
        };
        let input = Self::level_input(wv, at);
        let bound = QueryOutput {
            columns: input.iter().map(|c| c.name.clone()).collect(),
            types: input.iter().map(|c| c.data_type.clone()).collect(),
            rows: rows
                .iter()
                .map(|values| crate::Row {
                    values: values.clone(),
                })
                .collect(),
            tag: String::new(),
        };
        let out = crate::cte_scope::isolated(|| {
            let _bound = crate::cte_scope::bind(&level.base_ref, bound);
            self.execute_logical_inner(ctx, plan)
        })?;
        Ok(out.rows.into_iter().map(|r| r.values).collect())
    }

    /// Rows of the view's table as the view shows them.
    fn view_rows(
        &self,
        ctx: &ExecutionContext,
        wv: &WritableView,
        rows: &[Vec<Value>],
    ) -> Result<Vec<Vec<Value>>> {
        let mut rows = rows.to_vec();
        for at in (0..wv.levels.len()).rev() {
            rows = self.level_rows(ctx, wv, at, &rows)?;
        }
        Ok(rows)
    }

    /// Whether a level's condition holds for a row of the relation it reads.
    fn level_shows(
        &self,
        ctx: &ExecutionContext,
        wv: &WritableView,
        at: usize,
        row: &[Value],
    ) -> bool {
        let level = &wv.levels[at];
        let input = Self::level_input(wv, at);
        let names: Vec<String> = input.iter().map(|c| c.name.clone()).collect();
        level.filter.is_none()
            || self.eval_filter(ctx, row, &names, input, level.filter.as_ref()) == Some(true)
    }

    /// The table's rows the view shows (each view's condition holds for
    /// them): each row's stored key, its values, and its values as the view
    /// shows them.
    fn view_targets(
        &self,
        ctx: &ExecutionContext,
        wv: &WritableView,
    ) -> Result<Vec<(String, Vec<Value>, Vec<Value>)>> {
        let stored = self.scan_rows_keyed(wv.base.id, &ctx.session_id)?;
        // Which stored rows are still shown, and how the last level shows them.
        let mut kept: Vec<usize> = (0..stored.len()).collect();
        let mut shown: Vec<Vec<Value>> = stored.iter().map(|(_, row)| row.clone()).collect();
        for at in (0..wv.levels.len()).rev() {
            let (keep, rows): (Vec<usize>, Vec<Vec<Value>>) = kept
                .into_iter()
                .zip(shown)
                .filter(|(_, row)| self.level_shows(ctx, wv, at, row))
                .unzip();
            shown = self.level_rows(ctx, wv, at, &rows)?;
            kept = keep;
        }
        Ok(kept
            .into_iter()
            .zip(shown)
            .map(|(i, view_row)| (stored[i].0.clone(), stored[i].1.clone(), view_row))
            .collect())
    }

    /// Rejects a table row written through the view that a view whose
    /// condition is checked would not show: one with a check option, and
    /// the views under one with `CASCADED`.
    fn check_view_option(
        &self,
        ctx: &ExecutionContext,
        wv: &WritableView,
        row: &[Value],
    ) -> Result<()> {
        let mut row = row.to_vec();
        for at in (0..wv.levels.len()).rev() {
            let level = &wv.levels[at];
            let checked = level.check_option.is_some()
                || wv.levels[..at]
                    .iter()
                    .any(|l| l.check_option.as_deref() == Some("cascaded"));
            if checked && !self.level_shows(ctx, wv, at, &row) {
                return Err(DbError::new(format!(
                    "new row violates check option for view \"{}\"",
                    level.view.name
                ))
                .code("44000")
                .detail(crate::constraints::failing_row(&row))
                .into());
            }
            if at > 0 {
                row = self
                    .level_rows(ctx, wv, at, std::slice::from_ref(&row))?
                    .pop()
                    .unwrap_or_default();
            }
        }
        Ok(())
    }

    /// The table column a view column written by `command` is, or
    /// PostgreSQL's error for one that is an expression.
    fn view_column(wv: &WritableView, name: &str, command: ViewCommand) -> Result<(usize, usize)> {
        let at = Self::column_position(&wv.view, name)?;
        match wv.columns[at] {
            Some(base) => Ok((at, base)),
            None => Err(DbError::new(format!(
                "cannot {} column \"{name}\" of view \"{}\"",
                if command == ViewCommand::Insert {
                    "insert into"
                } else {
                    "update"
                },
                wv.view.name
            ))
            .code("0A000")
            .detail("View columns that are not columns of their base relation are not updatable.")
            .into()),
        }
    }

    /// A view column's base column name; `None` for an expression column
    /// or an unknown name.
    fn base_column(wv: &WritableView, name: &str) -> Option<String> {
        let at = Self::column_position(&wv.view, name).ok()?;
        let base = wv.columns.get(at).copied().flatten()?;
        Some(wv.base.columns[base].name.clone())
    }

    /// `INSERT` into a view: into its table, through its columns.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exec_view_insert(
        &self,
        ctx: &ExecutionContext,
        wv: WritableView,
        (table_name, alias): (String, Option<String>),
        columns: Vec<String>,
        values_list: Vec<Vec<Value>>,
        returning: Returning,
        on_conflict: Option<crate::plan_types::OnConflictClause>,
        (default_cells, overriding): (Vec<Vec<bool>>, Option<&str>),
    ) -> Result<QueryOutput> {
        self.authorize(ctx, Action::Insert, ResourceRef::Table(wv.view.id))?;
        // `ON CONFLICT DO UPDATE` runs on the view's table: its targets and
        // the expressions' view columns become the table's columns.
        let on_conflict = match on_conflict {
            Some(crate::plan_types::OnConflictClause::DoUpdate {
                target,
                assignments,
                condition,
            }) => {
                let rename = |name: &str| -> String {
                    let (prefix, column) = name
                        .rsplit_once('.')
                        .map_or((None, name), |(prefix, column)| (Some(prefix), column));
                    match Self::base_column(&wv, column) {
                        Some(base) if prefix == Some("excluded") => format!("excluded.{base}"),
                        Some(base) => base,
                        None => name.to_string(),
                    }
                };
                fn map(rename: &dyn Fn(&str) -> String, expr: &ScalarExpr) -> ScalarExpr {
                    match expr {
                        ScalarExpr::Column(name) => ScalarExpr::Column(rename(name)),
                        other => other.map_children(&mut |e| map(rename, e)),
                    }
                }
                let mapped = |expr: &ScalarExpr| map(&rename, expr);
                Some(crate::plan_types::OnConflictClause::DoUpdate {
                    target: target.map(|target| match target {
                        crate::plan_types::ConflictTarget::Columns(cols) => {
                            crate::plan_types::ConflictTarget::Columns(
                                cols.iter().map(|c| rename(c)).collect(),
                            )
                        }
                        other => other,
                    }),
                    assignments: assignments
                        .iter()
                        .map(|(column, expr)| (rename(column), mapped(expr)))
                        .collect(),
                    condition: condition.as_ref().map(mapped),
                })
            }
            other => other,
        };
        // Without a column list, the view's columns that values are given for.
        let named: Vec<String> = if columns.is_empty() {
            let given = values_list.iter().map(Vec::len).max().unwrap_or(0);
            wv.view
                .columns
                .iter()
                .take(given)
                .map(|c| c.name.clone())
                .collect()
        } else {
            columns
        };
        let base_columns = named
            .iter()
            .map(|name| {
                Self::view_column(&wv, name, ViewCommand::Insert)
                    .map(|(_, base)| wv.base.columns[base].name.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let base_name = format!(
            "{}.{}",
            self.schema_name_of(&wv.base),
            quote_name(&wv.base.name)
        );
        let on_conflict = on_conflict.map(|clause| match clause {
            crate::plan_types::OnConflictClause::DoNothing {
                target: Some(crate::plan_types::ConflictTarget::Columns(cols)),
            } => crate::plan_types::OnConflictClause::DoNothing {
                target: Some(crate::plan_types::ConflictTarget::Columns(
                    cols.iter()
                        .map(|c| {
                            Self::view_column(&wv, c, ViewCommand::Insert).map_or_else(
                                |_| c.clone(),
                                |(_, b)| wv.base.columns[b].name.clone(),
                            )
                        })
                        .collect(),
                )),
            },
            other => other,
        });
        let written = self.exec_insert(
            ctx,
            base_name,
            base_columns,
            values_list,
            Returning::columns(vec!["*".to_string()]),
            on_conflict,
            (default_cells, overriding, None),
        )?;
        let rows: Vec<Vec<Value>> = written.rows.into_iter().map(|r| r.values).collect();
        for row in &rows {
            self.check_view_option(ctx, &wv, row)?;
        }
        let scope = self.target_scope(
            ctx,
            &wv.view,
            (&table_name, alias.as_deref()),
            None,
            &[],
            false,
        )?;
        let returned = scope.returning_positions(&returning, false)?;
        let view_rows = if returned.is_empty() {
            Vec::new()
        } else {
            self.view_rows(ctx, &wv, &rows)?
        };
        Ok(scope.returning_output(self, ctx, &returned, view_rows, written.tag))
    }

    /// `UPDATE` of a view: of the rows of its table it shows, through its
    /// columns.
    pub(crate) fn exec_view_update(
        &self,
        ctx: &ExecutionContext,
        wv: WritableView,
        (table_name, table_alias): (String, Option<String>),
        assignments: Vec<(String, ScalarExpr)>,
        from: Option<LogicalPlan>,
        joins: Vec<crate::Join>,
        filter: Option<FilterExpr>,
        returning: Returning,
    ) -> Result<QueryOutput> {
        self.authorize(ctx, Action::Update, ResourceRef::Table(wv.view.id))?;
        let targets = assignments
            .iter()
            .map(|(col, _)| Self::view_column(&wv, col, ViewCommand::Update))
            .collect::<Result<Vec<_>>>()?;
        let scope = self.target_scope(
            ctx,
            &wv.view,
            (&table_name, table_alias.as_deref()),
            from,
            &joins,
            false,
        )?;
        let returned = scope.returning_positions(&returning, false)?;
        let mut refs = Vec::new();
        for (_, expr) in &assignments {
            crate::filter_eval::scalar_column_refs(expr, &mut refs);
        }
        if let Some(filter) = &filter {
            crate::filter_eval::filter_column_refs(filter, &mut refs);
        }
        scope.check_refs(refs, &scope.names)?;
        let assignments: Vec<(String, ScalarExpr)> = assignments
            .iter()
            .map(|(column, expr)| (column.clone(), scope.check_ranges(expr)))
            .collect();
        let filter = filter.as_ref().map(|f| scope.check_filter_ranges(f));
        let referenced = self.is_referenced(&wv.base)?;
        let mut changed = Vec::new();
        let mut updated = 0;
        let mut returning_rows = Vec::new();
        for (key, row, view_row) in self.view_targets(ctx, &wv)? {
            let Some(joined) = scope.source_rows.iter().find_map(|source| {
                let mut joined = view_row.clone();
                joined.extend(source.iter().cloned());
                self.eval_filter(ctx, &joined, &scope.names, &scope.columns, filter.as_ref())
                    .unwrap_or(false)
                    .then_some(joined)
            }) else {
                continue;
            };
            // The values the view's columns take, for the table's columns.
            let base_assignments: Vec<(String, ScalarExpr)> = assignments
                .iter()
                .zip(&targets)
                .map(|((_, expr), (_, base))| {
                    let is_default = matches!(expr,
                        ScalarExpr::Function { name, args } if name == "__COLUMN_DEFAULT__" && args.is_empty());
                    let value = if is_default {
                        expr.clone()
                    } else {
                        ScalarExpr::Literal(self.eval_expr(ctx, expr, &joined, &scope.names))
                    };
                    (wv.base.columns[*base].name.clone(), value)
                })
                .collect();
            let new_row =
                self.apply_assignments(ctx, &wv.base, &base_assignments, &row, (&[], &[]))?;
            self.replace_row(ctx, &wv.base, &key, &row, &new_row)?;
            self.check_view_option(ctx, &wv, &new_row)?;
            updated += 1;
            if referenced {
                changed.push((row.clone(), new_row.clone()));
            }
            if !returned.is_empty() {
                let new_view = self
                    .view_rows(ctx, &wv, std::slice::from_ref(&new_row))?
                    .pop()
                    .unwrap_or_default();
                let mut out = joined;
                out.splice(..new_view.len(), new_view);
                out.extend(view_row);
                returning_rows.push(out);
            }
        }
        self.enforce_references(ctx, &wv.base, &[], &changed)?;
        Ok(scope.returning_output(
            self,
            ctx,
            &returned,
            returning_rows,
            format!("UPDATE {updated}"),
        ))
    }

    /// `DELETE` from a view: of the rows of its table it shows.
    pub(crate) fn exec_view_delete(
        &self,
        ctx: &ExecutionContext,
        wv: WritableView,
        (table_name, table_alias): (String, Option<String>),
        using: Option<LogicalPlan>,
        joins: Vec<crate::Join>,
        filter: Option<FilterExpr>,
        returning: Returning,
    ) -> Result<QueryOutput> {
        self.authorize(ctx, Action::Delete, ResourceRef::Table(wv.view.id))?;
        let scope = self.target_scope(
            ctx,
            &wv.view,
            (&table_name, table_alias.as_deref()),
            using,
            &joins,
            false,
        )?;
        let returned = scope.returning_positions(&returning, false)?;
        if let Some(filter) = &filter {
            let mut refs = Vec::new();
            crate::filter_eval::filter_column_refs(filter, &mut refs);
            scope.check_refs(refs, &scope.names)?;
        }
        let filter = filter.as_ref().map(|f| scope.check_filter_ranges(f));
        let referenced = self.is_referenced(&wv.base)?;
        let mut removed = Vec::new();
        let mut deleted = 0;
        let mut returning_rows = Vec::new();
        for (key, row, view_row) in self.view_targets(ctx, &wv)? {
            let Some(joined) = scope.source_rows.iter().find_map(|source| {
                let mut joined = view_row.clone();
                joined.extend(source.iter().cloned());
                self.eval_filter(ctx, &joined, &scope.names, &scope.columns, filter.as_ref())
                    .unwrap_or(false)
                    .then_some(joined)
            }) else {
                continue;
            };
            self.remove_row(ctx, &wv.base, &key, &row)?;
            deleted += 1;
            if referenced {
                removed.push(row.clone());
            }
            if !returned.is_empty() {
                let written = vec![Value::Null; view_row.len() + 1];
                returning_rows.push([joined, view_row, written].concat());
            }
        }
        self.enforce_references(ctx, &wv.base, &removed, &[])?;
        Ok(scope.returning_output(
            self,
            ctx,
            &returned,
            returning_rows,
            format!("DELETE {deleted}"),
        ))
    }
}

/// A name quoted when it needs quotes to be read back as it is.
fn quote_name(name: &str) -> String {
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
