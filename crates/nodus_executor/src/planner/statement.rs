//! Top-level statement planning (DDL/DML/query dispatch).
use super::*;
use crate::*;
use anyhow::Result;
use nodus_catalog::TableConstraint;

/// Resolves the bare column names referenced by an index-style constraint
/// (`UNIQUE`/`PRIMARY KEY`), whose columns are now `IndexColumn` entries
/// wrapping an `OrderByExpr`.
fn index_column_names(columns: &[sqlparser::ast::IndexColumn]) -> Vec<String> {
    columns
        .iter()
        .filter_map(|c| extract_col_name(&c.column.expr))
        .collect()
}

pub fn plan_statement(stmt: &sqlparser::ast::Statement, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::*;
    reset_expression_errors();
    match stmt {
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } => {
            let name = match schema_name {
                sqlparser::ast::SchemaName::Simple(name) => name.to_string(),
                _ => anyhow::bail!("Unsupported schema name format"),
            };
            Ok(LogicalPlan::CreateSchema {
                schema_name: name,
                if_not_exists: *if_not_exists,
            })
        }
        Statement::CreateTable(create_table) if create_table.query.is_some() => {
            let query = create_table
                .query
                .as_ref()
                .expect("guarded by the match arm");
            if !create_table.columns.is_empty() {
                anyhow::bail!("CREATE TABLE AS with a column list is not supported");
            }
            Ok(LogicalPlan::CreateTableAs {
                name: create_table.name.to_string(),
                query: Box::new(plan_query(query, params)?),
                if_not_exists: create_table.if_not_exists,
            })
        }
        Statement::CreateSequence {
            temporary,
            if_not_exists,
            name,
            data_type,
            sequence_options,
            ..
        } => {
            if *temporary {
                anyhow::bail!("temporary sequences are not supported");
            }
            Ok(LogicalPlan::CreateSequence {
                name: name.to_string(),
                if_not_exists: *if_not_exists,
                spec: sequence_spec(
                    data_type.as_ref().map(|t| t.to_string()),
                    sequence_options,
                    params,
                )?,
            })
        }
        Statement::CreateTable(create_table) => {
            let name = &create_table.name;
            let columns = &create_table.columns;
            let constraints = &create_table.constraints;
            let table_name = name.to_string();
            let mut cols = Vec::new();
            let mut tbl_constraints = Vec::new();
            for c in columns {
                let mut nullable = true;
                let mut unique = false;
                let mut primary = false;
                let mut default = None;
                let mut sequence = None;
                let mut data_type = c.data_type.to_string();
                // The sequence lives in the table's schema; a name that is not
                // all lower case is quoted so `nextval` resolves it exactly.
                let quote = |name: &str| {
                    if name.chars().any(|ch| ch.is_ascii_uppercase()) {
                        format!("\"{name}\"")
                    } else {
                        name.to_string()
                    }
                };
                let sequence_name = || match table_name.rsplit_once('.') {
                    Some((schema, table)) => format!(
                        "{schema}.{}",
                        quote(&crate::sequences::owned_sequence_name(
                            table.trim_matches('"'),
                            &c.name.value
                        ))
                    ),
                    None => quote(&crate::sequences::owned_sequence_name(
                        table_name.trim_matches('"'),
                        &c.name.value,
                    )),
                };
                // `serial` types are integers drawing from a sequence.
                let serial_type = match data_type.to_ascii_lowercase().as_str() {
                    "serial" | "serial4" => Some("integer"),
                    "bigserial" | "serial8" => Some("bigint"),
                    "smallserial" | "serial2" => Some("smallint"),
                    _ => None,
                };
                if let Some(integer_type) = serial_type {
                    data_type = integer_type.to_ascii_uppercase();
                    nullable = false;
                    sequence = Some(crate::sequences::SequenceSpec {
                        data_type: Some(integer_type.to_string()),
                        ..Default::default()
                    });
                    default = Some(ScalarExpr::Function {
                        name: "NEXTVAL".to_string(),
                        args: vec![ScalarExpr::Literal(crate::Value::Text(sequence_name()))],
                    });
                }
                for opt in &c.options {
                    match &opt.option {
                        // `GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY [(options)]`.
                        sqlparser::ast::ColumnOption::Generated {
                            generated_as:
                                generated_as @ (sqlparser::ast::GeneratedAs::Always
                                | sqlparser::ast::GeneratedAs::ByDefault),
                            sequence_options,
                            generation_expr: None,
                            ..
                        } => {
                            let integer_type = match data_type.to_ascii_lowercase().as_str() {
                                "smallint" | "int2" => "smallint",
                                "int" | "integer" | "int4" => "integer",
                                "bigint" | "int8" => "bigint",
                                other => anyhow::bail!(
                                    "identity column type must be smallint, integer, or bigint, not {other}"
                                ),
                            };
                            nullable = false;
                            sequence = Some(sequence_spec(
                                Some(integer_type.to_string()),
                                sequence_options.as_deref().unwrap_or(&[]),
                                params,
                            )?);
                            let always =
                                matches!(generated_as, sqlparser::ast::GeneratedAs::Always);
                            default = Some(ScalarExpr::Function {
                                name: "__IDENTITY__".to_string(),
                                args: vec![
                                    ScalarExpr::Literal(crate::Value::Text(sequence_name())),
                                    ScalarExpr::Literal(crate::Value::Bool(always)),
                                ],
                            });
                        }
                        // `GENERATED ALWAYS AS (expr) [STORED | VIRTUAL]`: computed
                        // from the row whenever it is written.
                        sqlparser::ast::ColumnOption::Generated {
                            generation_expr: Some(expr),
                            ..
                        } => {
                            let computed = lower_scalar(expr, params).ok_or_else(|| {
                                expression_error(expr, || {
                                    format!(
                                        "Unsupported generation expression for column {}",
                                        c.name.value
                                    )
                                })
                            })?;
                            if crate::subqueries::contains_subquery(&computed) {
                                anyhow::bail!(
                                    "cannot use subquery in column generation expression"
                                );
                            }
                            if scalar_has_aggregate(&computed) {
                                anyhow::bail!(
                                    "aggregate functions are not allowed in column generation expressions"
                                );
                            }
                            default = Some(ScalarExpr::Function {
                                name: "__GENERATED__".to_string(),
                                args: vec![computed],
                            });
                        }
                        sqlparser::ast::ColumnOption::Generated { .. } => {
                            anyhow::bail!(
                                "Unsupported GENERATED column option for column {}",
                                c.name.value
                            )
                        }
                        sqlparser::ast::ColumnOption::NotNull => nullable = false,
                        sqlparser::ast::ColumnOption::Default(_) if sequence.is_some() => {
                            anyhow::bail!(
                                "multiple default values specified for column \"{}\"",
                                c.name.value
                            )
                        }
                        sqlparser::ast::ColumnOption::Default(e) => {
                            let lowered = lower_scalar(e, params).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "Unsupported DEFAULT expression for column {}",
                                    c.name.value
                                )
                            })?;
                            if crate::subqueries::contains_subquery(&lowered) {
                                anyhow::bail!("cannot use subquery in DEFAULT expression");
                            }
                            default = Some(lowered);
                        }
                        // `PRIMARY KEY` column option implies unique + not-null.
                        sqlparser::ast::ColumnOption::PrimaryKey(_) => {
                            unique = true;
                            nullable = false;
                            primary = true;
                        }
                        sqlparser::ast::ColumnOption::Unique(_) => {
                            unique = true;
                        }
                        sqlparser::ast::ColumnOption::Check(check) => {
                            check_constraint_is_supported(&check.expr, params)?;
                            tbl_constraints.push(nodus_catalog::TableConstraint::Check {
                                name: opt.name.as_ref().map(|n| n.value.clone()),
                                expr: check.expr.to_string(),
                            });
                        }
                        sqlparser::ast::ColumnOption::ForeignKey(fk) => {
                            tbl_constraints.push(nodus_catalog::TableConstraint::ForeignKey {
                                name: opt.name.as_ref().map(|n| n.value.clone()),
                                columns: vec![c.name.value.clone()],
                                foreign_table: fk.foreign_table.to_string(),
                                referred_columns: fk
                                    .referred_columns
                                    .iter()
                                    .map(|i| i.value.clone())
                                    .collect(),
                            });
                        }
                        _ => {}
                    }
                }
                cols.push(crate::ColumnDef {
                    name: c.name.value.clone(),
                    data_type,
                    nullable,
                    unique,
                    primary,
                    default,
                    sequence,
                });
            }

            let mut unique_constraints = Vec::new();
            for tc in constraints {
                match tc {
                    sqlparser::ast::TableConstraint::Unique(uc) => {
                        let names = index_column_names(&uc.columns);
                        if let [col] = names.as_slice() {
                            if let Some(c) = cols.iter_mut().find(|c| &c.name == col) {
                                c.unique = true;
                            }
                        } else {
                            // Unique over the tuple, not over each column alone.
                            unique_constraints.push(names);
                        }
                    }
                    sqlparser::ast::TableConstraint::PrimaryKey(pk) => {
                        for col in index_column_names(&pk.columns) {
                            if let Some(c) = cols.iter_mut().find(|c| c.name == col) {
                                c.unique = true;
                                c.nullable = false;
                                c.primary = true;
                            }
                        }
                    }
                    sqlparser::ast::TableConstraint::Check(check) => {
                        check_constraint_is_supported(&check.expr, params)?;
                        tbl_constraints.push(nodus_catalog::TableConstraint::Check {
                            name: check.name.as_ref().map(|n| n.value.clone()),
                            expr: check.expr.to_string(),
                        });
                    }
                    sqlparser::ast::TableConstraint::ForeignKey(fk) => {
                        tbl_constraints.push(nodus_catalog::TableConstraint::ForeignKey {
                            name: fk.name.as_ref().map(|n| n.value.clone()),
                            columns: fk.columns.iter().map(|c| c.value.clone()).collect(),
                            foreign_table: fk.foreign_table.to_string(),
                            referred_columns: fk
                                .referred_columns
                                .iter()
                                .map(|i| i.value.clone())
                                .collect(),
                        });
                    }
                    _ => {}
                }
            }

            Ok(LogicalPlan::CreateTable {
                name: table_name,
                columns: cols,
                constraints: tbl_constraints,
                if_not_exists: create_table.if_not_exists,
                unique_constraints,
            })
        }
        Statement::CreateView(create_view) => Ok(LogicalPlan::CreateView {
            name: create_view.name.to_string(),
            query: Box::new(plan_query(&create_view.query, params)?),
        }),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            ..
        } => {
            let name = names
                .first()
                .ok_or_else(|| anyhow::anyhow!("DROP without a name"))?
                .to_string();
            match object_type {
                sqlparser::ast::ObjectType::Sequence => Ok(LogicalPlan::DropSequence {
                    names: names.iter().map(|n| n.to_string()).collect(),
                    if_exists: *if_exists,
                }),
                // Dropping only the first of several names would report the
                // others dropped too.
                _ if names.len() > 1 => {
                    anyhow::bail!("DROP of several objects in one statement is not supported")
                }
                sqlparser::ast::ObjectType::Table => Ok(LogicalPlan::DropTable {
                    name,
                    if_exists: *if_exists,
                }),
                sqlparser::ast::ObjectType::View => Ok(LogicalPlan::DropView {
                    name,
                    if_exists: *if_exists,
                }),
                sqlparser::ast::ObjectType::Schema => Ok(LogicalPlan::DropSchema {
                    schema_name: name,
                    if_exists: *if_exists,
                    cascade: *cascade,
                }),
                sqlparser::ast::ObjectType::Index => Ok(LogicalPlan::DropIndex {
                    name,
                    if_exists: *if_exists,
                }),
                _ => anyhow::bail!("DROP {object_type} is not supported"),
            }
        }
        Statement::CreateIndex(create_index) => {
            let idx_name = create_index
                .name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unnamed_idx".to_string());
            let cols = create_index
                .columns
                .iter()
                .filter_map(|c| extract_col_name(&c.column.expr))
                .collect();
            Ok(LogicalPlan::CreateIndex {
                name: idx_name,
                table_name: create_index.table_name.to_string(),
                columns: cols,
                unique: create_index.unique,
                if_not_exists: create_index.if_not_exists,
            })
        }
        Statement::CreateRole(create_role) => {
            let name = create_role
                .names
                .first()
                .ok_or_else(|| anyhow::anyhow!("CREATE ROLE without a name"))?
                .to_string();
            Ok(LogicalPlan::CreateRole { name })
        }
        Statement::Grant(grant) => {
            let privilege = match &grant.privileges {
                sqlparser::ast::Privileges::Actions(actions) => actions
                    .first()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "ALL".to_string()),
                _ => "ALL".to_string(),
            };
            let grantee = grant
                .grantees
                .first()
                .ok_or_else(|| anyhow::anyhow!("GRANT without grantee"))?
                .to_string();
            if let Some(GrantObjects::Tables(tables)) = &grant.objects {
                let object_name = tables
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("GRANT without table name"))?
                    .to_string();
                Ok(LogicalPlan::Grant {
                    privilege,
                    object_name,
                    grantee,
                })
            } else {
                anyhow::bail!("Unsupported GRANT target");
            }
        }
        Statement::Revoke(revoke) => {
            let privilege = match &revoke.privileges {
                sqlparser::ast::Privileges::Actions(actions) => actions
                    .first()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "ALL".to_string()),
                _ => "ALL".to_string(),
            };
            let revokee = revoke
                .grantees
                .first()
                .ok_or_else(|| anyhow::anyhow!("REVOKE without revokee"))?
                .to_string();
            if let Some(GrantObjects::Tables(tables)) = &revoke.objects {
                let object_name = tables
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("REVOKE without table name"))?
                    .to_string();
                Ok(LogicalPlan::Revoke {
                    privilege,
                    object_name,
                    revokee,
                })
            } else {
                anyhow::bail!("Unsupported REVOKE target");
            }
        }
        Statement::Insert(insert) => {
            let returning = plan_returning(&insert.returning, "")?;
            let table_name = match &insert.table {
                sqlparser::ast::TableObject::TableName(name) => name.to_string(),
                other => anyhow::bail!("Unsupported INSERT target: {other}"),
            };
            // Use the bare identifier, not `to_string()` (which re-quotes a quoted
            // ident like `"Id"`). CREATE TABLE stores unquoted names, so a quoted
            // INSERT column list — every client that quotes identifiers, e.g. EF
            // Core — must match against those.
            let cols: Vec<String> = insert
                .columns
                .iter()
                .map(|c| {
                    c.0.last()
                        .and_then(|part| part.as_ident())
                        .map(|ident| ident.value.clone())
                        .unwrap_or_else(|| c.to_string())
                })
                .collect();
            // An unquoted bare `DEFAULT` in a VALUES row means "use the
            // column default" (it parses as a plain identifier).
            let is_default_kw = |e: &sqlparser::ast::Expr| {
                matches!(e, sqlparser::ast::Expr::Identifier(id)
                    if id.quote_style.is_none() && id.value.eq_ignore_ascii_case("default"))
            };
            let mut values_list = Vec::new();
            let mut default_cells: Vec<Vec<bool>> = Vec::new();
            let mut any_default = false;
            let mut source = None;
            match &insert.source {
                Some(query)
                    if query.with.is_none() && matches!(&*query.body, SetExpr::Values(_)) =>
                {
                    let SetExpr::Values(vs) = &*query.body else {
                        unreachable!("guarded by the match arm");
                    };
                    for row in &vs.rows {
                        let mut row_values = Vec::new();
                        let mut row_defaults = Vec::new();
                        for e in &row.content {
                            if is_default_kw(e) {
                                row_values.push(crate::Value::Null);
                                row_defaults.push(true);
                                any_default = true;
                            } else {
                                row_values.push(values_cell(e, params)?);
                                row_defaults.push(false);
                            }
                        }
                        values_list.push(row_values);
                        default_cells.push(row_defaults);
                    }
                }
                // `INSERT ... SELECT` (or any other query body).
                Some(query) => source = Some(Box::new(plan_query(query, params)?)),
                // `INSERT INTO t DEFAULT VALUES` — one row of all defaults.
                None => {
                    values_list.push(Vec::new());
                    default_cells.push(Vec::new());
                }
            }
            if !any_default {
                default_cells.clear();
            }
            let on_conflict = match &insert.on {
                Some(sqlparser::ast::OnInsert::OnConflict(oc)) => {
                    use crate::plan_types::{ConflictTarget, OnConflictClause};
                    let target = oc.conflict_target.as_ref().map(|t| match t {
                        sqlparser::ast::ConflictTarget::Columns(cols) => {
                            ConflictTarget::Columns(cols.iter().map(|c| c.value.clone()).collect())
                        }
                        sqlparser::ast::ConflictTarget::OnConstraint(name) => {
                            ConflictTarget::Constraint(
                                name.0
                                    .last()
                                    .and_then(|p| p.as_ident())
                                    .map(|i| i.value.clone())
                                    .unwrap_or_else(|| name.to_string()),
                            )
                        }
                    });
                    Some(match &oc.action {
                        sqlparser::ast::OnConflictAction::DoNothing => {
                            OnConflictClause::DoNothing { target }
                        }
                        sqlparser::ast::OnConflictAction::DoUpdate(du) => {
                            let assignments = plan_assignments(&du.assignments, params)?
                                .into_iter()
                                .map(|(col, e)| (col, bind_excluded(&e)))
                                .collect();
                            let condition = du
                                .selection
                                .as_ref()
                                .map(|e| {
                                    lower_scalar(e, params)
                                        .map(|c| bind_excluded(&c))
                                        .ok_or_else(|| {
                                            anyhow::anyhow!(
                                                "Unsupported ON CONFLICT condition: {e}"
                                            )
                                        })
                                })
                                .transpose()?;
                            OnConflictClause::DoUpdate {
                                target,
                                assignments,
                                condition,
                            }
                        }
                    })
                }
                Some(other) => anyhow::bail!("Unsupported INSERT clause: {other}"),
                None => None,
            };
            Ok(LogicalPlan::Insert {
                table_name,
                columns: cols,
                values_list,
                returning,
                on_conflict,
                default_cells,
                source,
            })
        }
        // `WITH ... INSERT`: the CTEs scope the insert's source query.
        Statement::Query(query) if matches!(&*query.body, SetExpr::Insert(_)) => {
            let SetExpr::Insert(insert) = &*query.body else {
                unreachable!("guarded by the match arm");
            };
            let mut insert = insert.clone();
            if let (Some(with), Statement::Insert(ins)) = (&query.with, &mut insert) {
                match ins.source.as_mut() {
                    Some(source) if source.with.is_none() => source.with = Some(with.clone()),
                    _ => anyhow::bail!("WITH before this INSERT is not supported"),
                }
            }
            plan_statement(&insert, params)
        }
        // `WITH ... UPDATE / DELETE / MERGE`: the CTEs are readable anywhere
        // in the statement.
        Statement::Query(query)
            if matches!(
                &*query.body,
                SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_)
            ) =>
        {
            let (SetExpr::Update(statement)
            | SetExpr::Delete(statement)
            | SetExpr::Merge(statement)) = &*query.body
            else {
                unreachable!("guarded by the match arm");
            };
            let body = plan_statement(statement, params)?;
            match &query.with {
                Some(with) => Ok(LogicalPlan::With {
                    ctes: plan_ctes(with, params)?,
                    body: Box::new(body),
                }),
                None => Ok(body),
            }
        }
        Statement::Query(query) if select_into(query).is_some() => {
            // `SELECT ... INTO t` creates `t` from the query without its INTO.
            let (name, query) = select_into(query).expect("guarded by the match arm");
            Ok(LogicalPlan::CreateTableAs {
                name,
                query: Box::new(plan_query(&query, params)?),
                if_not_exists: false,
            })
        }
        Statement::Query(query) => plan_query(query, params),
        Statement::Update(update) => {
            if !update.table.joins.is_empty() {
                anyhow::bail!("UPDATE of a joined relation is not supported");
            }
            let (table_name, table_alias) = target_table(&update.table.relation)?;
            let from = match &update.from {
                Some(
                    UpdateTableFromKind::AfterSet(from) | UpdateTableFromKind::BeforeSet(from),
                ) => Some(Box::new(plan_relations(from, params)?)),
                None => None,
            };
            let returning = plan_returning(
                &update.returning,
                table_alias.as_ref().unwrap_or(&table_name),
            )?;
            Ok(LogicalPlan::Update {
                assignments: plan_assignments(&update.assignments, params)?,
                filter: parse_predicates(&update.selection, params)?,
                returning,
                table_name,
                table_alias,
                from,
            })
        }
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            if !delete.tables.is_empty()
                || tables.len() > 1
                || tables.iter().any(|t| !t.joins.is_empty())
            {
                anyhow::bail!("DELETE from several relations is not supported");
            }
            let relation = &tables
                .first()
                .ok_or_else(|| anyhow::anyhow!("DELETE without a table"))?
                .relation;
            let (table_name, table_alias) = target_table(relation)?;
            let using = match &delete.using {
                Some(using) => Some(Box::new(plan_relations(using, params)?)),
                None => None,
            };
            let returning = plan_returning(
                &delete.returning,
                table_alias.as_ref().unwrap_or(&table_name),
            )?;
            Ok(LogicalPlan::Delete {
                table_name,
                filter: parse_predicates(&delete.selection, params)?,
                returning,
                table_alias,
                using,
            })
        }
        Statement::Merge(merge) => plan_merge(merge, params),
        Statement::Explain {
            analyze,
            verbose: _,
            statement,
            format,
            options,
            ..
        } => {
            let plan = plan_statement(statement, params)?;
            if !matches!(
                plan,
                LogicalPlan::Select { .. }
                    | LogicalPlan::SetOp { .. }
                    | LogicalPlan::Values { .. }
                    | LogicalPlan::SelectLiteral { .. }
                    | LogicalPlan::Insert { .. }
                    | LogicalPlan::Update { .. }
                    | LogicalPlan::Delete { .. }
                    | LogicalPlan::Merge { .. }
                    | LogicalPlan::With { .. }
                    | LogicalPlan::CreateTableAs { .. }
            ) {
                anyhow::bail!(
                    "EXPLAIN of {} is not supported",
                    leading_keywords(&statement.to_string())
                );
            }
            Ok(LogicalPlan::Explain {
                plan: Box::new(plan),
                options: explain_options(*analyze, format.as_ref(), options.as_deref())?,
            })
        }
        Statement::Truncate(truncate) => Ok(LogicalPlan::Truncate {
            tables: truncate
                .table_names
                .iter()
                .map(|t| t.name.to_string())
                .collect(),
            restart_identity: matches!(
                truncate.identity,
                Some(sqlparser::ast::TruncateIdentityOption::Restart)
            ),
        }),
        // Statistics and space are managed by the storage engine.
        Statement::Analyze(_) => Ok(LogicalPlan::Noop {
            tag: "ANALYZE".to_string(),
        }),
        Statement::Vacuum(_) => Ok(LogicalPlan::Noop {
            tag: "VACUUM".to_string(),
        }),
        Statement::StartTransaction { .. } => Ok(LogicalPlan::Begin),
        Statement::Commit { .. } => Ok(LogicalPlan::Commit),
        Statement::Rollback { savepoint, .. } => {
            if let Some(name) = savepoint {
                Ok(LogicalPlan::RollbackToSavepoint {
                    name: name.value.clone(),
                })
            } else {
                Ok(LogicalPlan::Rollback)
            }
        }
        Statement::Savepoint { name } => Ok(LogicalPlan::Savepoint {
            name: name.value.clone(),
        }),
        Statement::ReleaseSavepoint { name } => Ok(LogicalPlan::ReleaseSavepoint {
            name: name.value.clone(),
        }),
        Statement::ShowVariable { variable } => {
            let var_name = variable
                .iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            Ok(LogicalPlan::ShowVariable { variable: var_name })
        }
        // The `SET` family of statements is now wrapped in `Statement::Set(Set)`.
        Statement::Set(set) => match set {
            sqlparser::ast::Set::SingleAssignment {
                variable, values, ..
            } => {
                let var_name = variable.to_string();
                // A list value (`SET search_path = a, b`) keeps its commas.
                let var_val = values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(LogicalPlan::SetVariable {
                    variable: var_name,
                    value: var_val,
                })
            }
            sqlparser::ast::Set::SetTransaction { .. } => Ok(LogicalPlan::SetVariable {
                variable: "transaction_isolation".to_string(),
                value: "read committed".to_string(),
            }),
            // `SET TIME ZONE <x>` is the SQL-standard spelling of `SET timezone = <x>`;
            // route it to the same per-session variable so it persists and `SHOW
            // TimeZone` reflects it (`DEFAULT`/`LOCAL` clear the override).
            sqlparser::ast::Set::SetTimeZone { value, .. } => Ok(LogicalPlan::SetVariable {
                variable: "timezone".to_string(),
                value: value.to_string(),
            }),
            other => anyhow::bail!("{} is not supported", leading_keywords(&other.to_string())),
        },
        Statement::Discard { .. } => Ok(LogicalPlan::Noop {
            tag: "DISCARD ALL".to_string(),
        }),
        Statement::Deallocate { .. } => Ok(LogicalPlan::Noop {
            tag: "DEALLOCATE".to_string(),
        }),
        Statement::AlterTable(alter_table) => {
            let table_name = alter_table.name.to_string();
            let op = alter_table
                .operations
                .first()
                .ok_or_else(|| anyhow::anyhow!("ALTER TABLE without operations"))?;
            let alter_op = match op {
                sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                    let mut nullable = true;
                    let mut default = None;
                    for opt in &column_def.options {
                        match &opt.option {
                            sqlparser::ast::ColumnOption::NotNull => nullable = false,
                            sqlparser::ast::ColumnOption::Default(e) => {
                                default = Some(lower_scalar(e, params).ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "Unsupported DEFAULT expression for column {}",
                                        column_def.name.value
                                    )
                                })?);
                            }
                            _ => {}
                        }
                    }
                    AlterTableOp::AddColumn {
                        name: column_def.name.value.clone(),
                        data_type: column_def.data_type.to_string(),
                        nullable,
                        default,
                    }
                }
                sqlparser::ast::AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => AlterTableOp::RenameColumn {
                    old_name: old_column_name.value.clone(),
                    new_name: new_column_name.value.clone(),
                },
                sqlparser::ast::AlterTableOperation::DropColumn { column_names, .. } => {
                    let name = column_names
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("DROP COLUMN without a column name"))?
                        .value
                        .clone();
                    AlterTableOp::DropColumn { name }
                }
                sqlparser::ast::AlterTableOperation::AlterColumn {
                    column_name,
                    op: sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. },
                } => AlterTableOp::AlterColumnType {
                    name: column_name.value.clone(),
                    data_type: data_type.to_string(),
                },
                sqlparser::ast::AlterTableOperation::RenameTable { table_name } => {
                    let new_name = match table_name {
                        sqlparser::ast::RenameTableNameKind::As(name)
                        | sqlparser::ast::RenameTableNameKind::To(name) => name.to_string(),
                    };
                    AlterTableOp::RenameTable { new_name }
                }
                _ => anyhow::bail!(
                    "ALTER TABLE ... {} is not supported",
                    leading_keywords(&op.to_string())
                ),
            };
            Ok(LogicalPlan::AlterTable {
                table_name,
                operation: alter_op,
            })
        }
        _ => anyhow::bail!("{} is not supported", leading_keywords(&stmt.to_string())),
    }
}

/// The options of an `EXPLAIN`: `ANALYZE` and `FORMAT` as keywords, or any
/// of `(ANALYZE, COSTS, TIMING, SUMMARY, FORMAT, ...)` in parentheses.
fn explain_options(
    analyze: bool,
    format: Option<&sqlparser::ast::AnalyzeFormatKind>,
    options: Option<&[sqlparser::ast::UtilityOption]>,
) -> Result<crate::explain::ExplainOptions> {
    use sqlparser::ast::{AnalyzeFormat, AnalyzeFormatKind};
    let mut out = crate::explain::ExplainOptions {
        analyze,
        ..Default::default()
    };
    let set_format = |out: &mut crate::explain::ExplainOptions, name: &str| -> Result<()> {
        match name.to_ascii_lowercase().as_str() {
            "text" => out.json = false,
            "json" => out.json = true,
            other => anyhow::bail!(
                "EXPLAIN format {} is not supported",
                other.to_ascii_uppercase()
            ),
        }
        Ok(())
    };
    if let Some(AnalyzeFormatKind::Keyword(f) | AnalyzeFormatKind::Assignment(f)) = format {
        let name = match f {
            AnalyzeFormat::JSON => "json",
            AnalyzeFormat::TEXT => "text",
            other => &other.to_string(),
        };
        set_format(&mut out, name)?;
    }
    for option in options.unwrap_or_default() {
        let arg = option.arg.as_ref().map(|a| {
            a.to_string()
                .trim_matches(|c| c == '\'' || c == '"')
                .to_ascii_lowercase()
        });
        let flag = || -> Result<bool> {
            match arg.as_deref() {
                None | Some("true" | "on" | "1") => Ok(true),
                Some("false" | "off" | "0") => Ok(false),
                Some(_) => anyhow::bail!(
                    "{} requires a Boolean value",
                    option.name.value.to_ascii_lowercase()
                ),
            }
        };
        match option.name.value.to_ascii_lowercase().as_str() {
            "analyze" => out.analyze = flag()?,
            "costs" => out.costs = flag()?,
            "timing" => out.timing = flag()?,
            "summary" => out.summary = Some(flag()?),
            "format" => set_format(&mut out, arg.as_deref().unwrap_or(""))?,
            // Details the executor has no counterpart for.
            "verbose" | "buffers" | "settings" | "wal" | "generic_plan" | "memory"
            | "serialize" => {}
            other => anyhow::bail!("unrecognized EXPLAIN option \"{other}\""),
        }
    }
    Ok(out)
}

/// The keywords a statement or clause starts with (`CREATE FUNCTION`,
/// `VACUUM`), which name it in an error without echoing the rest.
fn leading_keywords(sql: &str) -> String {
    let words: Vec<&str> = sql
        .split_whitespace()
        .take_while(|w| w.chars().all(|c| c.is_ascii_uppercase() || c == '_'))
        .take(4)
        .collect();
    if words.is_empty() {
        "this statement".to_string()
    } else {
        words.join(" ")
    }
}

/// Rejects a CHECK constraint the executor cannot evaluate, so it is never
/// stored and then silently left unenforced.
fn check_constraint_is_supported(expr: &sqlparser::ast::Expr, params: &[Value]) -> Result<()> {
    let filter = parse_filter_expr(expr, params)
        .map_err(|e| anyhow::anyhow!("Unsupported CHECK constraint `{expr}`: {e}"))?;
    if filter_has_subquery(&filter) {
        anyhow::bail!("cannot use subquery in check constraint");
    }
    Ok(())
}

/// Whether a condition runs a subquery anywhere.
fn filter_has_subquery(filter: &FilterExpr) -> bool {
    match filter {
        FilterExpr::InSubquery { .. }
        | FilterExpr::CompareSubquery { .. }
        | FilterExpr::Exists { .. }
        | FilterExpr::QuantifiedSubquery { .. } => true,
        FilterExpr::And(a, b) | FilterExpr::Or(a, b) => {
            filter_has_subquery(a) || filter_has_subquery(b)
        }
        FilterExpr::Not(a) => filter_has_subquery(a),
        FilterExpr::Scalar(e) => crate::subqueries::contains_subquery(e),
        FilterExpr::ExprCmp { left, right, .. } => {
            crate::subqueries::contains_subquery(left)
                || crate::subqueries::contains_subquery(right)
        }
        _ => false,
    }
}

/// Plans a `RETURNING` list as column names, qualified as written: `*` and
/// `x.*` expand at execution, `new.` names the written row, so it stands for
/// `target`, the name the table goes by, and `old.` the row as it was.
/// Expressions, and `old.` in an INSERT, are rejected rather than silently
/// omitted.
fn plan_returning(
    items: &Option<Vec<sqlparser::ast::SelectItem>>,
    target: &str,
) -> Result<Vec<String>> {
    use sqlparser::ast::{Expr, SelectItem, SelectItemQualifiedWildcardKind};
    let Some(items) = items else {
        return Ok(Vec::new());
    };
    // `old.` reads the row as it was, which an INSERT (no `target`) lacks.
    let is_old = |qualifier: &str| target.is_empty() && qualifier.eq_ignore_ascii_case("old");
    let qualify = |qualifier: &str| {
        if qualifier.eq_ignore_ascii_case("new") {
            target.to_string()
        } else if qualifier.eq_ignore_ascii_case("old") {
            "old".to_string()
        } else {
            qualifier.to_string()
        }
    };
    items
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard(_) => Ok("*".to_string()),
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _)
                if !is_old(&name.to_string()) =>
            {
                Ok(format!("{}.*", qualify(&name.to_string())))
            }
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => Ok(id.value.clone()),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts))
                if parts.len() == 2 && !is_old(&parts[0].value) =>
            {
                Ok(format!("{}.{}", qualify(&parts[0].value), parts[1].value))
            }
            other => anyhow::bail!("Unsupported RETURNING item: {other}"),
        })
        .collect()
}

/// Plans `MERGE INTO target USING source ON condition WHEN ...`.
fn plan_merge(merge: &sqlparser::ast::Merge, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::{
        MergeAction as Action, MergeClauseKind, MergeInsertKind, MergeUpdateKind, OutputClause,
        TableWithJoins,
    };
    let (table_name, table_alias) = target_table(&merge.table)?;
    let returning = match &merge.output {
        Some(OutputClause::Returning { select_items, .. }) => plan_returning(
            &Some(select_items.clone()),
            table_alias.as_ref().unwrap_or(&table_name),
        )?,
        Some(other) => anyhow::bail!("Unsupported MERGE clause: {other}"),
        None => Vec::new(),
    };
    let source = plan_relations(
        &[TableWithJoins {
            relation: merge.source.clone(),
            joins: Vec::new(),
        }],
        params,
    )?;
    let mut clauses: Vec<MergeClause> = Vec::with_capacity(merge.clauses.len());
    for clause in &merge.clauses {
        let kind = match clause.clause_kind {
            MergeClauseKind::Matched => MergeKind::Matched,
            MergeClauseKind::NotMatchedBySource => MergeKind::NotMatchedBySource,
            MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget => {
                MergeKind::NotMatchedByTarget
            }
        };
        if clauses
            .iter()
            .any(|c| c.kind == kind && c.condition.is_none())
        {
            anyhow::bail!("unreachable WHEN clause specified after unconditional WHEN clause");
        }
        let action = match &clause.action {
            Action::Update(update) => match &update.kind {
                MergeUpdateKind::Set(assignments)
                    if update.update_predicate.is_none() && update.delete_predicate.is_none() =>
                {
                    MergeAction::Update(plan_assignments(assignments, params)?)
                }
                _ => anyhow::bail!("Unsupported MERGE action: {}", clause.action),
            },
            Action::Delete { .. } => MergeAction::Delete,
            Action::DoNothing { .. } => MergeAction::Nothing,
            Action::Insert(insert) => {
                let MergeInsertKind::Values(values) = &insert.kind else {
                    anyhow::bail!("Unsupported MERGE action: {}", clause.action);
                };
                if insert.insert_predicate.is_some() {
                    anyhow::bail!("Unsupported MERGE action: {}", clause.action);
                }
                let [row] = values.rows.as_slice() else {
                    anyhow::bail!("MERGE INSERT must have exactly one VALUES row");
                };
                let values = row
                    .content
                    .iter()
                    .map(|e| match e {
                        sqlparser::ast::Expr::Identifier(id)
                            if id.quote_style.is_none()
                                && id.value.eq_ignore_ascii_case("default") =>
                        {
                            Ok(None)
                        }
                        _ => lower_scalar(e, params).map(Some).ok_or_else(|| {
                            expression_error(e, || format!("Unsupported expression in VALUES: {e}"))
                        }),
                    })
                    .collect::<Result<Vec<_>>>()?;
                let columns: Vec<String> = insert
                    .columns
                    .iter()
                    .map(|c| {
                        c.0.last()
                            .and_then(|part| part.as_ident())
                            .map(|ident| ident.value.clone())
                            .unwrap_or_else(|| c.to_string())
                    })
                    .collect();
                MergeAction::Insert { columns, values }
            }
        };
        clauses.push(MergeClause {
            kind,
            condition: parse_predicates(&clause.predicate, params)?,
            action,
        });
    }
    Ok(LogicalPlan::Merge {
        table_name,
        table_alias,
        source: Box::new(source),
        on: parse_predicates(&Some((*merge.on).clone()), params)?,
        clauses,
        returning,
    })
}

/// Plans `SET` assignments. A tuple target `(a, b) = (x, y)` expands to one
/// assignment per column. An assignment that cannot be evaluated is an error
/// rather than silently dropped.
fn plan_assignments(
    assignments: &[sqlparser::ast::Assignment],
    params: &[Value],
) -> Result<Vec<(String, ScalarExpr)>> {
    use sqlparser::ast::{AssignmentTarget, Expr};
    // Take the last identifier of the target, e.g. `t.col = ...` -> `col`.
    let column = |name: &sqlparser::ast::ObjectName| {
        name.0
            .last()
            .and_then(|p| p.as_ident())
            .map(|i| i.value.clone())
            .ok_or_else(|| anyhow::anyhow!("Unsupported assignment target: {name}"))
    };
    let mut out = Vec::new();
    for a in assignments {
        match &a.target {
            AssignmentTarget::ColumnName(name) => {
                out.push((column(name)?, assignment_value(&a.value, params)?));
            }
            AssignmentTarget::Tuple(names) => {
                let values: Vec<&Expr> = match &a.value {
                    Expr::Tuple(items) => items.iter().collect(),
                    Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("row") => match &f
                        .args
                    {
                        sqlparser::ast::FunctionArguments::List(list) => list
                            .args
                            .iter()
                            .map(|arg| match arg {
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(e),
                                ) => Ok(e),
                                other => Err(anyhow::anyhow!("Unsupported ROW argument: {other}")),
                            })
                            .collect::<Result<_>>()?,
                        _ => Vec::new(),
                    },
                    other => anyhow::bail!("Unsupported multi-column assignment source: {other}"),
                };
                if values.len() != names.len() {
                    anyhow::bail!("number of columns does not match number of values");
                }
                for (name, value) in names.iter().zip(values) {
                    out.push((column(name)?, assignment_value(value, params)?));
                }
            }
        }
    }
    Ok(out)
}

/// One assignment value; `DEFAULT` becomes a sentinel the executor resolves
/// to the column default.
fn assignment_value(value: &sqlparser::ast::Expr, params: &[Value]) -> Result<ScalarExpr> {
    if let sqlparser::ast::Expr::Identifier(id) = value
        && id.quote_style.is_none()
        && id.value.eq_ignore_ascii_case("default")
    {
        return Ok(ScalarExpr::Function {
            name: "__COLUMN_DEFAULT__".to_string(),
            args: vec![],
        });
    }
    // Lower the RHS so `SET n = n + 1` evaluates per row against its old values.
    lower_scalar(value, params)
        .ok_or_else(|| anyhow::anyhow!("Unsupported assignment value: {value}"))
}

/// Evaluates one `VALUES` cell: a literal or parameter, or else a constant
/// scalar expression (`1 + 1`, `'a' || 'b'`). A cell that cannot be evaluated,
/// or that names a column, is an error rather than NULL.
fn values_cell(e: &sqlparser::ast::Expr, params: &[Value]) -> Result<Value> {
    use sqlparser::ast::Expr;
    if !matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
        && let Some(v) = expr_to_value(e, params)
    {
        return Ok(v);
    }
    let expr = lower_scalar(e, params)
        .ok_or_else(|| anyhow::anyhow!("Unsupported VALUES expression: {e}"))?;
    if references_column(&expr) {
        anyhow::bail!("column \"{e}\" does not exist");
    }
    let expr = crate::result_types::check_integer_ranges(&expr, &|_: &str| None);
    let value = eval_scalar_expr(&expr, &[], &[]);
    crate::eval_error::check()?;
    Ok(value)
}

fn references_column(expr: &ScalarExpr) -> bool {
    matches!(expr, ScalarExpr::Column(_)) || expr.children().into_iter().any(references_column)
}

/// Resolves `EXCLUDED.<col>` (any case) to the `excluded.<col>` name under
/// which the proposed row is bound during `ON CONFLICT DO UPDATE`.
fn bind_excluded(expr: &ScalarExpr) -> ScalarExpr {
    match expr {
        ScalarExpr::Column(name) => match name.split_once('.') {
            Some((qualifier, col)) if qualifier.eq_ignore_ascii_case("excluded") => {
                ScalarExpr::Column(format!("excluded.{col}"))
            }
            _ => expr.clone(),
        },
        _ => expr.map_children(&mut |e| bind_excluded(e)),
    }
}

/// `SELECT ... INTO t`: the target table name and the query without `INTO`.
fn select_into(query: &sqlparser::ast::Query) -> Option<(String, sqlparser::ast::Query)> {
    use sqlparser::ast::SetExpr;
    let SetExpr::Select(select) = &*query.body else {
        return None;
    };
    let target = match select.into.as_ref()?.targets.as_slice() {
        [target] => target.to_string(),
        _ => return None,
    };
    let mut select = select.clone();
    select.into = None;
    let mut query = query.clone();
    query.body = Box::new(SetExpr::Select(select));
    Some((target, query))
}

/// Reads `CREATE SEQUENCE` (or identity column) options. Values must be
/// integer constants.
fn sequence_spec(
    data_type: Option<String>,
    options: &[sqlparser::ast::SequenceOptions],
    params: &[Value],
) -> Result<crate::sequences::SequenceSpec> {
    use sqlparser::ast::SequenceOptions as O;
    let integer = |e: &sqlparser::ast::Expr| -> Result<i64> {
        match expr_to_value(e, params) {
            Some(Value::Int(n)) => Ok(n),
            _ => anyhow::bail!("sequence option must be an integer constant: {e}"),
        }
    };
    let mut spec = crate::sequences::SequenceSpec {
        data_type,
        ..Default::default()
    };
    for option in options {
        match option {
            O::IncrementBy(e, _) => spec.increment = Some(integer(e)?),
            O::MinValue(e) => spec.min_value = e.as_ref().map(&integer).transpose()?,
            O::MaxValue(e) => spec.max_value = e.as_ref().map(&integer).transpose()?,
            O::StartWith(e, _) => spec.start = Some(integer(e)?),
            O::Cache(e) => spec.cache = Some(integer(e)?),
            // sqlparser's flag is set for `NO CYCLE`.
            O::Cycle(no_cycle) => spec.cycle = !no_cycle,
        }
    }
    Ok(spec)
}
