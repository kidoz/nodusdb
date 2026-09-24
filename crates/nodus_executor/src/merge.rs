//! `MERGE`: joins a source relation to the target table and inserts, updates,
//! or deletes rows by the first `WHEN` clause that applies to each joined row.

use crate::*;
use anyhow::Result;
use nodus_authz::Action;
use nodus_catalog::ResourceRef;

impl MemExecutor {
    pub(crate) fn exec_merge(
        &self,
        ctx: &ExecutionContext,
        (table_name, table_alias): (String, Option<String>),
        source: LogicalPlan,
        on: Option<FilterExpr>,
        clauses: Vec<MergeClause>,
        returning: Vec<String>,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        for clause in &clauses {
            let action = match clause.action {
                MergeAction::Update(_) => Action::Update,
                MergeAction::Delete => Action::Delete,
                MergeAction::Insert { .. } => Action::Insert,
                MergeAction::Nothing => continue,
            };
            self.authorize(ctx, action, ResourceRef::Table(tbl.id))?;
        }
        let target = (table_name.as_str(), table_alias.as_deref());
        let scope = self.target_scope(ctx, &tbl, target, Some(source), true)?;
        let returning = scope.returning_positions(&returning, true)?;

        // What each part of the statement can name: a NOT MATCHED BY SOURCE
        // clause sees only the target row, a NOT MATCHED one only the
        // source row.
        let target_only = scope.names_without(false);
        let source_only = scope.names_without(true);
        let visible = |kind: MergeKind| match kind {
            MergeKind::Matched => &scope.names,
            MergeKind::NotMatchedBySource => &target_only,
            MergeKind::NotMatchedByTarget => &source_only,
        };
        // Every column the statement names must be one it can see.
        if let Some(on) = &on {
            let mut refs = Vec::new();
            crate::filter_eval::filter_column_refs(on, &mut refs);
            scope.check_refs(refs, &scope.names)?;
        }
        for clause in &clauses {
            let mut refs = Vec::new();
            if let Some(condition) = &clause.condition {
                crate::filter_eval::filter_column_refs(condition, &mut refs);
            }
            match &clause.action {
                MergeAction::Update(assignments) => {
                    for (column, expr) in assignments {
                        Self::column_position(&tbl, column)?;
                        crate::filter_eval::scalar_column_refs(expr, &mut refs);
                    }
                }
                MergeAction::Insert { columns, values } => {
                    for column in columns {
                        Self::column_position(&tbl, column)?;
                    }
                    let targets = if columns.is_empty() {
                        tbl.columns.len()
                    } else {
                        columns.len()
                    };
                    if values.len() > targets {
                        anyhow::bail!("INSERT has more expressions than target columns");
                    }
                    if !columns.is_empty() && values.len() < targets {
                        anyhow::bail!("INSERT has more target columns than expressions");
                    }
                    for expr in values.iter().flatten() {
                        crate::filter_eval::scalar_column_refs(expr, &mut refs);
                    }
                }
                MergeAction::Delete | MergeAction::Nothing => {}
            }
            scope.check_refs(refs, visible(clause.kind))?;
        }

        // The join, as pairs of target and source row indexes: each source
        // row with the target rows `on` matches it to, or alone when there are
        // none, then each target row no source row matches. It is computed in
        // full first, so no change the statement makes affects it.
        let targets = self.scan_rows_keyed(tbl.id, &ctx.session_id)?;
        let mut join = Vec::new();
        let mut target_matched = vec![false; targets.len()];
        for (s, source) in scope.source_rows.iter().enumerate() {
            let before = join.len();
            for (t, (_, target)) in targets.iter().enumerate() {
                let joined = [target.as_slice(), source].concat();
                if self
                    .eval_filter(ctx, &joined, &scope.names, &scope.columns, on.as_ref())
                    .unwrap_or(false)
                {
                    join.push((Some(t), Some(s)));
                    target_matched[t] = true;
                }
            }
            if join.len() == before {
                join.push((None, Some(s)));
            }
        }
        join.extend(
            (0..targets.len())
                .filter(|&t| !target_matched[t])
                .map(|t| (Some(t), None)),
        );

        let target_nulls = vec![Value::Null; tbl.columns.len()];
        let source_nulls = vec![Value::Null; scope.names.len() - tbl.columns.len()];
        let mut modified = vec![false; targets.len()];
        let mut changed = 0;
        let mut returning_rows = Vec::new();
        for (t, s) in join {
            let target = t.map_or(target_nulls.as_slice(), |t| targets[t].1.as_slice());
            let source = s.map_or(source_nulls.as_slice(), |s| scope.source_rows[s].as_slice());
            let kind = match (t, s) {
                (Some(_), Some(_)) => MergeKind::Matched,
                (Some(_), None) => MergeKind::NotMatchedBySource,
                _ => MergeKind::NotMatchedByTarget,
            };
            let joined = [target, source].concat();
            let names = visible(kind);
            let Some(action) = self.merge_action(ctx, &clauses, kind, &joined, names, &scope)?
            else {
                continue;
            };
            match (action, t) {
                (MergeAction::Nothing, _) => continue,
                (MergeAction::Insert { columns, values }, None) => {
                    let row: Vec<Value> = values
                        .iter()
                        .map(|v| match v {
                            Some(expr) => self.eval_expr(ctx, expr, &joined, names),
                            None => Value::Null,
                        })
                        .collect();
                    let defaults: Vec<bool> = values.iter().map(Option::is_none).collect();
                    // The inserted row in full, when RETURNING wants it.
                    let inserted = if returning.is_empty() {
                        Vec::new()
                    } else {
                        vec!["*".to_string()]
                    };
                    let out = self.exec_insert(
                        ctx,
                        table_name.clone(),
                        columns.clone(),
                        vec![row],
                        inserted,
                        None,
                        vec![defaults],
                    )?;
                    returning_rows.extend(
                        out.rows
                            .into_iter()
                            .map(|row| [row.values.as_slice(), source, &target_nulls].concat()),
                    );
                }
                (MergeAction::Update(_) | MergeAction::Delete, Some(t)) => {
                    // Only one of a target row's joined rows may change it.
                    if modified[t] {
                        anyhow::bail!("MERGE command cannot affect row a second time");
                    }
                    modified[t] = true;
                    let key = &targets[t].0;
                    if let MergeAction::Update(assignments) = action {
                        let row = self.apply_assignments(
                            ctx,
                            &tbl,
                            assignments,
                            target,
                            (&joined, names),
                        )?;
                        self.replace_row(ctx, &tbl, key, target, &row)?;
                        if !returning.is_empty() {
                            returning_rows.push([row.as_slice(), source, target].concat());
                        }
                    } else {
                        self.remove_row(ctx, &tbl, key, target)?;
                        if !returning.is_empty() {
                            returning_rows.push([joined.as_slice(), target].concat());
                        }
                    }
                }
                _ => anyhow::bail!("MERGE action does not apply to its clause"),
            }
            changed += 1;
        }
        Ok(scope.returning_output(&returning, returning_rows, format!("MERGE {changed}")))
    }

    /// The action of the first `kind` clause whose condition holds for
    /// `joined`, if any.
    fn merge_action<'a>(
        &self,
        ctx: &ExecutionContext,
        clauses: &'a [MergeClause],
        kind: MergeKind,
        joined: &[Value],
        names: &[String],
        scope: &crate::dml::TargetScope,
    ) -> Result<Option<&'a MergeAction>> {
        for clause in clauses.iter().filter(|c| c.kind == kind) {
            if self
                .eval_filter(
                    ctx,
                    joined,
                    names,
                    &scope.columns,
                    clause.condition.as_ref(),
                )
                .unwrap_or(false)
            {
                return Ok(Some(&clause.action));
            }
        }
        Ok(None)
    }
}
