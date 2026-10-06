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
            use sqlparser::ast::SchemaName;
            // Without a name of its own, a schema is named after its owner.
            let (name, authorization) = match schema_name {
                SchemaName::Simple(name) => (name.to_string(), None),
                SchemaName::UnnamedAuthorization(role) => {
                    (role.value.clone(), Some(role.value.clone()))
                }
                SchemaName::NamedAuthorization(name, role) => {
                    (name.to_string(), Some(role.value.clone()))
                }
            };
            Ok(LogicalPlan::CreateSchema {
                schema_name: name,
                if_not_exists: *if_not_exists,
                authorization,
                elements: Vec::new(),
            })
        }
        Statement::AlterSchema(alter) => {
            use sqlparser::ast::AlterSchemaOperation as Op;
            let [operation] = alter.operations.as_slice() else {
                anyhow::bail!("ALTER SCHEMA with several operations is not supported");
            };
            let (new_name, owner) = match operation {
                Op::Rename { name } => (Some(name.to_string()), None),
                Op::OwnerTo { owner } => (None, Some(owner.to_string())),
                other => anyhow::bail!("ALTER SCHEMA {other} is not supported"),
            };
            Ok(LogicalPlan::AlterSchema {
                name: alter.name.to_string(),
                new_name,
                owner,
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
                name: temp_relation_name(&create_table.name, create_table.temporary)?,
                query: Box::new(plan_query(query, params)?),
                if_not_exists: create_table.if_not_exists,
                no_data: has_no_data_marker(&create_table.table_options),
                materialized: false,
            })
        }
        Statement::CreateSequence {
            temporary,
            if_not_exists,
            name,
            data_type,
            sequence_options,
            ..
        } => Ok(LogicalPlan::CreateSequence {
            name: temp_relation_name(name, *temporary)?,
            if_not_exists: *if_not_exists,
            spec: sequence_spec(
                data_type.as_ref().map(|t| t.to_string()),
                sequence_options,
                params,
            )?,
        }),
        Statement::CreateTable(create_table) => {
            let columns = &create_table.columns;
            let constraints = &create_table.constraints;
            let table_name = temp_relation_name(&create_table.name, create_table.temporary)?;
            let mut cols = Vec::new();
            let mut tbl_constraints = Vec::new();
            // The names given to key constraints, by their columns.
            let mut key_names: Vec<(Vec<String>, String)> = Vec::new();
            // Key constraints' `DEFERRABLE` / `INITIALLY DEFERRED` /
            // `NULLS NOT DISTINCT`, by their columns.
            let mut key_flags: Vec<(Vec<String>, bool, bool, bool)> = Vec::new();
            for c in columns {
                let mut nullable = true;
                let mut unique = false;
                let mut primary = false;
                // The column key's flags, pushed after its options are read
                // (a `NULLS NOT DISTINCT` marker may follow the constraint).
                let mut col_nnd = false;
                let mut col_key: Option<((bool, bool), bool)> = None;
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
                        name: crate::sequences::SERIAL.to_string(),
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
                        sqlparser::ast::ColumnOption::PrimaryKey(pk) => {
                            if let Some(name) = &opt.name {
                                key_names.push((vec![c.name.value.clone()], name.value.clone()));
                            }
                            col_key = Some((deferral(&pk.characteristics)?, false));
                            unique = true;
                            nullable = false;
                            primary = true;
                        }
                        sqlparser::ast::ColumnOption::Unique(uc) => {
                            // The marker name is the `NULLS NOT DISTINCT` a
                            // column clause was rewritten to.
                            if opt.name.as_ref().is_some_and(|name| {
                                name.value == nodus_sql::NULLS_NOT_DISTINCT_MARK
                            }) {
                                col_nnd = true;
                                unique = true;
                                continue;
                            }
                            if let Some(name) = &opt.name {
                                key_names.push((vec![c.name.value.clone()], name.value.clone()));
                            }
                            col_key =
                                Some((deferral(&uc.characteristics)?, nulls_not_distinct(uc)));
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
                            tbl_constraints.push(foreign_key(
                                opt.name.as_ref().or(fk.name.as_ref()),
                                vec![c.name.value.clone()],
                                fk,
                            )?);
                        }
                        _ => {}
                    }
                }
                if let Some((flags, nnd)) = col_key {
                    let nnd = nnd || col_nnd;
                    if flags != (false, false) || nnd {
                        key_flags.push((vec![c.name.value.clone()], flags.0, flags.1, nnd));
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
                        if let Some(name) = &uc.name {
                            key_names.push((names.clone(), name.value.clone()));
                        }
                        let flags = deferral(&uc.characteristics)?;
                        let nnd = nulls_not_distinct(uc);
                        if flags != (false, false) || nnd {
                            key_flags.push((names.clone(), flags.0, flags.1, nnd));
                        }
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
                        if let Some(name) = &pk.name {
                            key_names.push((index_column_names(&pk.columns), name.value.clone()));
                        }
                        let flags = deferral(&pk.characteristics)?;
                        if flags != (false, false) {
                            let names = index_column_names(&pk.columns);
                            key_flags.push((names, flags.0, flags.1, false));
                        }
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
                        tbl_constraints.push(foreign_key(
                            fk.name.as_ref(),
                            fk.columns.iter().map(|c| c.value.clone()).collect(),
                            fk,
                        )?);
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
                key_names,
                key_flags,
                on_commit: match create_table.on_commit {
                    Some(sqlparser::ast::OnCommit::Drop) => Some("DROP".to_string()),
                    Some(sqlparser::ast::OnCommit::DeleteRows) => Some("DELETE ROWS".to_string()),
                    _ => None,
                },
                inherits: create_table
                    .inherits
                    .clone()
                    .unwrap_or_default()
                    .iter()
                    .map(|name| name.to_string())
                    .collect(),
                partition_by: partition_by_text(create_table)?,
                partition_of: create_table.partition_of.as_ref().map(|n| n.to_string()),
                for_values: create_table
                    .for_values
                    .as_ref()
                    .map(for_values_text)
                    .transpose()?,
                like: match &create_table.like {
                    Some(
                        sqlparser::ast::CreateTableLikeKind::Parenthesized(like)
                        | sqlparser::ast::CreateTableLikeKind::Plain(like),
                    ) => vec![(
                        like.name.to_string(),
                        like.defaults == Some(sqlparser::ast::CreateTableLikeDefaults::Including),
                    )],
                    None => Vec::new(),
                },
            })
        }
        Statement::CreateView(create_view) => {
            let mut query = plan_query(&create_view.query, params)?;
            // `CREATE VIEW v (a, b) AS ...` names the view's columns.
            if !create_view.columns.is_empty() {
                query = LogicalPlan::Renamed {
                    input: Box::new(query),
                    columns: create_view
                        .columns
                        .iter()
                        .map(|c| c.name.value.clone())
                        .collect(),
                };
            }
            if create_view.materialized {
                return Ok(LogicalPlan::CreateTableAs {
                    name: create_view.name.to_string(),
                    query: Box::new(query),
                    if_not_exists: create_view.if_not_exists,
                    no_data: has_no_data_marker(&create_view.options),
                    materialized: true,
                });
            }
            Ok(LogicalPlan::CreateView {
                name: temp_relation_name(&create_view.name, create_view.temporary)?,
                query: Box::new(query),
                or_replace: create_view.or_replace,
                check_option: view_check_option(&create_view.options)?,
            })
        }
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
                sqlparser::ast::ObjectType::Table => Ok(LogicalPlan::DropTable {
                    names: names.iter().map(|n| n.to_string()).collect(),
                    if_exists: *if_exists,
                    materialized: false,
                    cascade: *cascade,
                }),
                sqlparser::ast::ObjectType::MaterializedView => Ok(LogicalPlan::DropTable {
                    names: names.iter().map(|n| n.to_string()).collect(),
                    if_exists: *if_exists,
                    materialized: true,
                    cascade: *cascade,
                }),
                sqlparser::ast::ObjectType::View => Ok(LogicalPlan::DropView {
                    names: names.iter().map(|n| n.to_string()).collect(),
                    if_exists: *if_exists,
                    cascade: *cascade,
                }),
                sqlparser::ast::ObjectType::Schema => Ok(LogicalPlan::DropSchema {
                    names: names.iter().map(|n| n.to_string()).collect(),
                    if_exists: *if_exists,
                    cascade: *cascade,
                }),
                sqlparser::ast::ObjectType::Type => Ok(LogicalPlan::DropType {
                    names: names.iter().map(|n| n.to_string()).collect(),
                    if_exists: *if_exists,
                    cascade: *cascade,
                    domain: false,
                }),
                sqlparser::ast::ObjectType::Role => Ok(LogicalPlan::DropRole {
                    names: names.iter().map(|n| n.to_string()).collect(),
                    if_exists: *if_exists,
                }),
                // Dropping only the first of several names would report the
                // others dropped too.
                _ if names.len() > 1 => {
                    anyhow::bail!("DROP of several objects in one statement is not supported")
                }
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
                .unwrap_or_default();
            // Each key part is a column, or an expression (named, for the
            // index's default name, as PostgreSQL names it).
            let mut cols = Vec::new();
            let mut expressions = Vec::new();
            for c in &create_index.columns {
                let mut expr = &c.column.expr;
                while let Expr::Nested(inner) = expr {
                    expr = inner;
                }
                match expr {
                    Expr::Identifier(id) => {
                        cols.push(id.value.clone());
                        expressions.push(None);
                    }
                    _ => {
                        lower_scalar(&c.column.expr, params).ok_or_else(|| {
                            anyhow::anyhow!(
                                "index expressions are not supported: {}",
                                c.column.expr
                            )
                        })?;
                        cols.push(crate::index_keys::expression_label(&c.column.expr));
                        expressions.push(Some(c.column.expr.to_string()));
                    }
                }
            }
            let descending = create_index
                .columns
                .iter()
                .map(|c| {
                    matches!(
                        c.column.options.sort,
                        Some(sqlparser::ast::OrderBySort::Desc)
                    )
                })
                .collect();
            let predicate = match &create_index.predicate {
                Some(condition) => {
                    parse_filter_expr(condition, params)?;
                    Some(condition.to_string())
                }
                None => None,
            };
            Ok(LogicalPlan::CreateIndex {
                name: idx_name,
                table_name: create_index.table_name.to_string(),
                columns: cols,
                unique: create_index.unique,
                nulls_not_distinct: create_index.nulls_distinct == Some(false),
                if_not_exists: create_index.if_not_exists,
                predicate,
                expressions,
                descending,
            })
        }
        Statement::CreateRole(create_role) => {
            let name = create_role
                .names
                .first()
                .ok_or_else(|| anyhow::anyhow!("CREATE ROLE without a name"))?
                .to_string();
            if create_role.names.len() > 1 {
                anyhow::bail!("CREATE ROLE with several names is not supported");
            }
            Ok(LogicalPlan::CreateRole {
                name,
                attributes: role_attributes(
                    create_role.login,
                    create_role.inherit,
                    create_role.bypassrls,
                    create_role.superuser,
                    create_role.create_db,
                    create_role.create_role,
                    create_role.replication,
                    &create_role.connection_limit,
                    &create_role.valid_until,
                    &create_role.password,
                    params,
                )?,
            })
        }
        Statement::Grant(grant) => {
            let privileges = grant_privileges(&grant.privileges);
            let grantees = grant.grantees.iter().map(|g| g.to_string()).collect();
            Ok(LogicalPlan::Grant {
                privileges,
                objects: grant_objects(grant.objects.as_ref(), params)?,
                grantees,
                with_grant_option: grant.with_grant_option,
            })
        }
        Statement::Revoke(revoke) => {
            let privileges = grant_privileges(&revoke.privileges);
            let grantees = revoke.grantees.iter().map(|g| g.to_string()).collect();
            Ok(LogicalPlan::Revoke {
                grant_option_for: revoke.grant_option_for,
                privileges,
                objects: grant_objects(revoke.objects.as_ref(), params)?,
                grantees,
            })
        }
        Statement::Set(sqlparser::ast::Set::SetRole { role_name, .. }) => {
            Ok(LogicalPlan::SetRole {
                role: role_name.as_ref().map(|r| r.value.clone()),
                session_authorization: false,
            })
        }
        Statement::Set(sqlparser::ast::Set::SetSessionAuthorization(param)) => match &param.kind {
            sqlparser::ast::SetSessionAuthorizationParamKind::User(name) => {
                Ok(LogicalPlan::SetRole {
                    role: Some(name.value.clone()),
                    session_authorization: true,
                })
            }
            _ => Ok(LogicalPlan::SetRole {
                role: None,
                session_authorization: true,
            }),
        },
        Statement::AlterRole { name, operation } => {
            use sqlparser::ast::AlterRoleOperation as Op;
            let action = match operation {
                Op::RenameRole { role_name } => crate::plan_types::AlterRoleAction::Rename {
                    name: role_name.value.clone(),
                },
                Op::WithOptions { options } => crate::plan_types::AlterRoleAction::Attributes {
                    patch: role_attrs_patch(options, params)?,
                },
                Op::Set {
                    config_name,
                    config_value,
                    in_database,
                } => {
                    if in_database.is_some() {
                        anyhow::bail!("ALTER ROLE ... IN DATABASE is not supported");
                    }
                    let value = match config_value {
                        sqlparser::ast::SetConfigValue::Default
                        | sqlparser::ast::SetConfigValue::FromCurrent => None,
                        sqlparser::ast::SetConfigValue::Value(expr) => {
                            Some(match expr_to_value(expr, params) {
                                Some(crate::Value::Text(text)) => text,
                                Some(value) => crate::render(&value),
                                None => expr.to_string().trim_matches('\'').to_string(),
                            })
                        }
                    };
                    match value {
                        Some(value) => crate::plan_types::AlterRoleAction::SetSetting {
                            name: config_name.to_string(),
                            value,
                        },
                        None => crate::plan_types::AlterRoleAction::ResetSetting {
                            name: config_name.to_string(),
                        },
                    }
                }
                Op::Reset {
                    config_name,
                    in_database,
                } => {
                    if in_database.is_some() {
                        anyhow::bail!("ALTER ROLE ... IN DATABASE is not supported");
                    }
                    crate::plan_types::AlterRoleAction::ResetSetting {
                        name: match config_name {
                            sqlparser::ast::ResetConfig::ALL => String::new(),
                            sqlparser::ast::ResetConfig::ConfigName(name) => name.to_string(),
                        },
                    }
                }
                other => anyhow::bail!("ALTER ROLE {other} is not supported"),
            };
            Ok(LogicalPlan::AlterRole {
                name: name.value.clone(),
                action,
            })
        }
        Statement::Insert(insert) => {
            let table_name = match &insert.table {
                sqlparser::ast::TableObject::TableName(name) => name.to_string(),
                other => anyhow::bail!("Unsupported INSERT target: {other}"),
            };
            let (returning, returning_exprs) =
                plan_returning(&insert.returning, ReturningOf::Insert, params)?;
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
            let alias = insert
                .table_alias
                .as_ref()
                .map(|a| a.alias.value.clone())
                .unwrap_or_default();
            let overriding = if alias.ends_with(nodus_sql::OVERRIDING_SYSTEM) {
                Some("SYSTEM".to_string())
            } else if alias.ends_with(nodus_sql::OVERRIDING_USER) {
                Some("USER".to_string())
            } else {
                None
            };
            let alias = alias
                .strip_suffix(nodus_sql::OVERRIDING_SYSTEM)
                .or_else(|| alias.strip_suffix(nodus_sql::OVERRIDING_USER))
                .unwrap_or(&alias)
                .to_string();
            Ok(LogicalPlan::Insert {
                table_name,
                columns: cols,
                values_list,
                returning,
                returning_exprs,
                on_conflict,
                default_cells,
                source,
                overriding,
                alias: (!alias.is_empty()).then_some(alias),
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
                no_data: false,
                materialized: false,
            })
        }
        // Maintenance the storage engine does itself.
        Statement::Query(query) if rewritten_call(query, nodus_sql::UTILITY_FUNCTION).is_some() => {
            match rewritten_call(query, nodus_sql::UTILITY_FUNCTION).as_deref() {
                Some([crate::Value::Text(tag)]) if tag == "PREPARE TRANSACTION" => Err(
                    crate::error_fields::DbError::new("prepared transactions are disabled")
                        .hint("Set \"max_prepared_transactions\" to a nonzero value.")
                        .into(),
                ),
                Some([crate::Value::Text(tag)]) => Ok(LogicalPlan::Noop { tag: tag.clone() }),
                _ => anyhow::bail!("malformed maintenance statement"),
            }
        }
        Statement::Query(query)
            if rewritten_call(query, nodus_sql::ALTER_SEQUENCE_FUNCTION).is_some() =>
        {
            let args = rewritten_call(query, nodus_sql::ALTER_SEQUENCE_FUNCTION)
                .expect("guarded by the match arm");
            let [
                crate::Value::Text(name),
                crate::Value::Bool(if_exists),
                crate::Value::Text(options),
            ] = args.as_slice()
            else {
                anyhow::bail!("malformed ALTER SEQUENCE");
            };
            Ok(LogicalPlan::AlterSequence {
                name: name.clone(),
                if_exists: *if_exists,
                change: sequence_change(options, params)?,
            })
        }
        Statement::Query(query)
            if rewritten_call(query, nodus_sql::SET_SCHEMA_FUNCTION).is_some() =>
        {
            match rewritten_call(query, nodus_sql::SET_SCHEMA_FUNCTION).as_deref() {
                Some(
                    [
                        crate::Value::Text(kind),
                        crate::Value::Bool(if_exists),
                        crate::Value::Text(name),
                        crate::Value::Text(schema),
                    ],
                ) => Ok(LogicalPlan::SetSchema {
                    kind: kind.clone(),
                    name: name.clone(),
                    schema: schema.clone(),
                    if_exists: *if_exists,
                }),
                _ => anyhow::bail!("malformed SET SCHEMA"),
            }
        }
        Statement::Query(query) if rewritten_call(query, nodus_sql::INHERIT_FUNCTION).is_some() => {
            match rewritten_call(query, nodus_sql::INHERIT_FUNCTION).as_deref() {
                Some(
                    [
                        crate::Value::Text(child),
                        crate::Value::Text(parent),
                        crate::Value::Bool(attach),
                    ],
                ) => Ok(LogicalPlan::AlterTable {
                    table_name: child.clone(),
                    operations: vec![AlterTableOp::Inherit {
                        parent: parent.clone(),
                        attach: *attach,
                    }],
                    if_exists: false,
                    only: false,
                }),
                _ => anyhow::bail!("malformed INHERIT"),
            }
        }
        Statement::Query(query)
            if rewritten_call(query, nodus_sql::GRANT_ROLE_FUNCTION).is_some() =>
        {
            match rewritten_call(query, nodus_sql::GRANT_ROLE_FUNCTION).as_deref() {
                Some(
                    [
                        crate::Value::Text(roles),
                        crate::Value::Text(members),
                        crate::Value::Text(mode),
                    ],
                ) => {
                    let split = |text: &str| -> Vec<String> {
                        text.split(',')
                            .map(|name| name.trim().to_string())
                            .filter(|name| !name.is_empty())
                            .collect()
                    };
                    Ok(LogicalPlan::GrantRole {
                        roles: split(roles),
                        members: split(members),
                        admin_option: matches!(mode.as_str(), "grant_admin" | "revoke_admin"),
                        admin_only: mode == "revoke_admin",
                        grant: mode.starts_with("grant"),
                    })
                }
                _ => anyhow::bail!("malformed GRANT role"),
            }
        }
        Statement::Query(query)
            if rewritten_call(query, nodus_sql::SET_CONSTRAINTS_FUNCTION).is_some() =>
        {
            match rewritten_call(query, nodus_sql::SET_CONSTRAINTS_FUNCTION).as_deref() {
                Some(
                    [
                        crate::Value::Bool(all),
                        crate::Value::Text(mode),
                        crate::Value::Text(names),
                    ],
                ) => Ok(LogicalPlan::SetConstraints {
                    all: *all,
                    names: names
                        .split(',')
                        .map(|n| n.trim().to_string())
                        .filter(|n| !n.is_empty())
                        .collect(),
                    deferred: mode.eq_ignore_ascii_case("deferred"),
                }),
                _ => anyhow::bail!("malformed SET CONSTRAINTS"),
            }
        }
        Statement::Query(query)
            if rewritten_call(query, nodus_sql::ALTER_CONSTRAINT_FUNCTION).is_some() =>
        {
            match rewritten_call(query, nodus_sql::ALTER_CONSTRAINT_FUNCTION).as_deref() {
                Some(
                    [
                        crate::Value::Text(table),
                        crate::Value::Text(name),
                        crate::Value::Bool(deferrable),
                        crate::Value::Bool(initially_deferred),
                    ],
                ) => Ok(LogicalPlan::AlterTable {
                    table_name: table.clone(),
                    operations: vec![AlterTableOp::AlterConstraint {
                        name: name.clone(),
                        deferrable: *deferrable,
                        initially_deferred: *initially_deferred,
                    }],
                    if_exists: false,
                    only: false,
                }),
                _ => anyhow::bail!("malformed ALTER CONSTRAINT"),
            }
        }
        Statement::Query(query)
            if rewritten_call(query, nodus_sql::ATTACH_PARTITION_FUNCTION).is_some() =>
        {
            match rewritten_call(query, nodus_sql::ATTACH_PARTITION_FUNCTION).as_deref() {
                Some(
                    [
                        crate::Value::Text(parent),
                        crate::Value::Text(partition),
                        crate::Value::Text(bound),
                    ],
                ) => Ok(LogicalPlan::AlterTable {
                    table_name: parent.clone(),
                    operations: vec![AlterTableOp::AttachPartition {
                        parent: parent.clone(),
                        partition: partition.clone(),
                        bound: bound.clone(),
                    }],
                    if_exists: false,
                    only: false,
                }),
                _ => anyhow::bail!("malformed ATTACH PARTITION"),
            }
        }
        Statement::Query(query)
            if rewritten_call(query, nodus_sql::DETACH_PARTITION_FUNCTION).is_some() =>
        {
            match rewritten_call(query, nodus_sql::DETACH_PARTITION_FUNCTION).as_deref() {
                Some([crate::Value::Text(parent), crate::Value::Text(partition)]) => {
                    Ok(LogicalPlan::AlterTable {
                        table_name: parent.clone(),
                        operations: vec![AlterTableOp::DetachPartition {
                            parent: parent.clone(),
                            partition: partition.clone(),
                        }],
                        if_exists: false,
                        only: false,
                    })
                }
                _ => anyhow::bail!("malformed DETACH PARTITION"),
            }
        }
        Statement::Query(query)
            if rewritten_call(query, nodus_sql::CREATE_SCHEMA_FUNCTION).is_some() =>
        {
            let parts = rewritten_call(query, nodus_sql::CREATE_SCHEMA_FUNCTION)
                .expect("guarded by the match arm");
            let mut plans = Vec::new();
            for part in &parts {
                let crate::Value::Text(text) = part else {
                    anyhow::bail!("malformed CREATE SCHEMA");
                };
                for statement in nodus_sql::parse_sql(text)? {
                    plans.push(plan_statement(&statement, params)?);
                }
            }
            let mut plans = plans.into_iter();
            match plans.next() {
                Some(LogicalPlan::CreateSchema {
                    schema_name,
                    if_not_exists,
                    authorization,
                    ..
                }) => Ok(LogicalPlan::CreateSchema {
                    schema_name,
                    if_not_exists,
                    authorization,
                    elements: plans.collect(),
                }),
                _ => anyhow::bail!("malformed CREATE SCHEMA"),
            }
        }
        Statement::Query(query) if rewritten_call(query, nodus_sql::DOMAIN_FUNCTION).is_some() => {
            match rewritten_call(query, nodus_sql::DOMAIN_FUNCTION).as_deref() {
                Some([crate::Value::Text(text)]) => plan_domain_statement(text),
                _ => anyhow::bail!("malformed domain statement"),
            }
        }
        Statement::Query(query) if rewritten_call(query, nodus_sql::TYPE_FUNCTION).is_some() => {
            match rewritten_call(query, nodus_sql::TYPE_FUNCTION).as_deref() {
                Some(
                    [
                        crate::Value::Text(operation),
                        crate::Value::Text(name),
                        crate::Value::Text(first),
                        crate::Value::Text(second),
                    ],
                ) => {
                    let change = match operation.as_str() {
                        "add" => crate::user_types::TypeChange::AddAttribute {
                            name: first.clone(),
                            data_type: second.clone(),
                        },
                        "drop" => crate::user_types::TypeChange::DropAttribute {
                            name: first.clone(),
                            if_exists: second == "true",
                        },
                        "rename" => crate::user_types::TypeChange::RenameAttribute {
                            from: first.clone(),
                            to: second.clone(),
                        },
                        _ => anyhow::bail!("malformed ALTER TYPE"),
                    };
                    Ok(LogicalPlan::AlterType {
                        name: name.clone(),
                        change,
                        domain: false,
                    })
                }
                _ => anyhow::bail!("malformed ALTER TYPE"),
            }
        }
        Statement::Query(query) if rewritten_call(query, nodus_sql::CURSOR_FUNCTION).is_some() => {
            let args = rewritten_call(query, nodus_sql::CURSOR_FUNCTION)
                .expect("guarded by the match arm");
            let [
                crate::Value::Text(command),
                crate::Value::Text(direction),
                crate::Value::Text(name),
            ] = args.as_slice()
            else {
                anyhow::bail!("malformed FETCH");
            };
            Ok(LogicalPlan::FetchCursor {
                name: name.clone(),
                direction: crate::cursors::FetchDirection::parse(direction)?,
                move_only: command == "MOVE",
            })
        }
        Statement::Query(query) if refresh_target(query).is_some() => {
            let (name, with_data) = refresh_target(query).expect("guarded by the match arm");
            Ok(LogicalPlan::RefreshMaterializedView { name, with_data })
        }
        Statement::Query(query) => plan_query(query, params),
        Statement::Update(update) => {
            if !update.table.joins.is_empty() {
                anyhow::bail!("UPDATE of a joined relation is not supported");
            }
            let (table_name, table_alias, only) = target_table(&update.table.relation)?;
            let from = match &update.from {
                Some(
                    UpdateTableFromKind::AfterSet(from) | UpdateTableFromKind::BeforeSet(from),
                ) => Some(Box::new(plan_relations(from, params)?)),
                None => None,
            };
            let (returning, returning_exprs) =
                plan_returning(&update.returning, ReturningOf::Change, params)?;
            Ok(LogicalPlan::Update {
                only,
                assignments: plan_assignments(&update.assignments, params)?,
                filter: parse_predicates(&update.selection, params)?,
                returning,
                returning_exprs,
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
            let (table_name, table_alias, only) = target_table(relation)?;
            let using = match &delete.using {
                Some(using) => Some(Box::new(plan_relations(using, params)?)),
                None => None,
            };
            let (returning, returning_exprs) =
                plan_returning(&delete.returning, ReturningOf::Change, params)?;
            Ok(LogicalPlan::Delete {
                only,
                table_name,
                filter: parse_predicates(&delete.selection, params)?,
                returning,
                returning_exprs,
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
        Statement::Comment {
            object_type,
            object_name,
            comment,
            ..
        } => {
            use sqlparser::ast::CommentObject as O;
            let kind = match object_type {
                O::Table => "TABLE",
                O::View => "VIEW",
                O::MaterializedView => "MATERIALIZED VIEW",
                O::Sequence => "SEQUENCE",
                O::Column => "COLUMN",
                O::Schema => "SCHEMA",
                O::Type => "TYPE",
                O::Domain => "DOMAIN",
                other => anyhow::bail!("COMMENT ON {other} is not supported"),
            };
            let mut parts: Vec<String> = object_name.0.iter().map(|p| p.to_string()).collect();
            let column = if kind == "COLUMN" {
                if parts.len() < 2 {
                    anyhow::bail!("column name must be qualified");
                }
                parts.pop()
            } else {
                None
            };
            Ok(LogicalPlan::Comment {
                kind: kind.to_string(),
                relation: parts.join("."),
                column,
                comment: comment.clone(),
            })
        }
        Statement::Truncate(truncate) => Ok(LogicalPlan::Truncate {
            // `ONLY` is carried as the mark the token rewriter uses for the
            // statement forms the parser cannot take.
            tables: truncate
                .table_names
                .iter()
                .map(|t| {
                    if t.only {
                        format!("{}{}", nodus_sql::ONLY_MARK, t.name)
                    } else {
                        t.name.to_string()
                    }
                })
                .collect(),
            restart_identity: matches!(
                truncate.identity,
                Some(sqlparser::ast::TruncateIdentityOption::Restart)
            ),
            cascade: matches!(
                truncate.cascade,
                Some(sqlparser::ast::CascadeOption::Cascade)
            ),
        }),
        // Statistics and space are managed by the storage engine.
        Statement::Analyze(_) => Ok(LogicalPlan::Noop {
            tag: "ANALYZE".to_string(),
        }),
        Statement::Vacuum(_) => Ok(LogicalPlan::Noop {
            tag: "VACUUM".to_string(),
        }),
        Statement::StartTransaction { modes, begin, .. } => {
            let (read_only, isolation) = transaction_modes(modes);
            Ok(LogicalPlan::Begin {
                read_only,
                isolation,
                start: !*begin,
            })
        }
        Statement::Commit { chain: true, .. } => Ok(LogicalPlan::Chain { rollback: false }),
        Statement::Rollback {
            chain: true,
            savepoint: None,
        } => Ok(LogicalPlan::Chain { rollback: true }),
        Statement::Reset(reset) => match &reset.reset {
            // `RESET ROLE` parses as a variable named `role`, which no
            // setting answers to.
            sqlparser::ast::Reset::ConfigurationParameter(name)
                if name.to_string().eq_ignore_ascii_case("role") =>
            {
                Ok(LogicalPlan::SetRole {
                    role: None,
                    session_authorization: false,
                })
            }
            sqlparser::ast::Reset::ConfigurationParameter(name) => Ok(LogicalPlan::ResetVariable {
                variable: Some(name.to_string()),
            }),
            sqlparser::ast::Reset::ALL => Ok(LogicalPlan::ResetVariable { variable: None }),
            sqlparser::ast::Reset::SessionAuthorization => Ok(LogicalPlan::SetRole {
                role: None,
                session_authorization: true,
            }),
        },
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
                scope,
                variable,
                values,
                ..
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
                    local: matches!(scope, Some(sqlparser::ast::ContextModifier::Local)),
                })
            }
            sqlparser::ast::Set::SetTransaction { modes, session, .. } => {
                let (read_only, isolation) = transaction_modes(modes);
                Ok(LogicalPlan::SetTransaction {
                    read_only,
                    isolation,
                    session: *session,
                })
            }
            // `SET TIME ZONE <x>` is the SQL-standard spelling of `SET timezone = <x>`;
            // route it to the same per-session variable so it persists and `SHOW
            // TimeZone` reflects it (`DEFAULT`/`LOCAL` clear the override).
            sqlparser::ast::Set::SetTimeZone { value, local } => Ok(LogicalPlan::SetVariable {
                variable: "timezone".to_string(),
                value: value.to_string(),
                local: *local,
            }),
            other => anyhow::bail!("{} is not supported", leading_keywords(&other.to_string())),
        },
        Statement::Discard { object_type } => Ok(LogicalPlan::Discard {
            what: object_type.to_string().to_ascii_uppercase(),
        }),
        Statement::Deallocate { name, .. } => Ok(LogicalPlan::Deallocate {
            name: (!name.value.eq_ignore_ascii_case("all") || name.quote_style.is_some())
                .then(|| name.value.clone()),
        }),
        Statement::CreateType {
            name,
            representation: Some(sqlparser::ast::UserDefinedTypeRepresentation::Enum { labels }),
        } => Ok(LogicalPlan::CreateType {
            name: name.to_string(),
            definition: crate::user_types::TypeDefinition::Enum {
                labels: labels
                    .iter()
                    .enumerate()
                    .map(|(i, label)| crate::user_types::EnumLabel {
                        label: label.value.clone(),
                        sort: (i + 1) as f64,
                    })
                    .collect(),
            },
        }),
        Statement::CreateType {
            name,
            representation:
                Some(sqlparser::ast::UserDefinedTypeRepresentation::Composite { attributes }),
        } => Ok(LogicalPlan::CreateType {
            name: name.to_string(),
            definition: crate::user_types::TypeDefinition::Composite {
                attributes: attributes
                    .iter()
                    .map(|a| crate::user_types::Attribute {
                        name: a.name.value.clone(),
                        data_type: a.data_type.to_string(),
                    })
                    .collect(),
            },
        }),
        Statement::CreateType { .. } => {
            anyhow::bail!("CREATE TYPE is not supported but for enums and composite types")
        }
        Statement::CreateDomain(domain) => {
            let mut definition = crate::user_types::DomainDefinition {
                base: domain.data_type.to_string(),
                default: domain.default.as_ref().map(|e| e.to_string()),
                ..Default::default()
            };
            for constraint in &domain.constraints {
                match constraint {
                    sqlparser::ast::TableConstraint::Check(check) => {
                        if matches!(&*check.expr, Expr::Identifier(id) if id.value == nodus_sql::DOMAIN_NOT_NULL)
                        {
                            definition.not_null = true;
                        } else {
                            definition.checks.push(crate::user_types::DomainCheck {
                                name: check
                                    .name
                                    .as_ref()
                                    .map(|n| n.value.clone())
                                    .unwrap_or_default(),
                                sql: check.expr.to_string(),
                            });
                        }
                    }
                    other => anyhow::bail!("domain constraint {other} is not supported"),
                }
            }
            Ok(LogicalPlan::CreateType {
                name: domain.name.to_string(),
                definition: crate::user_types::TypeDefinition::Domain(definition),
            })
        }
        Statement::AlterType(alter) => {
            use sqlparser::ast::{AlterTypeAddValuePosition as Position, AlterTypeOperation as Op};
            let change = match &alter.operation {
                Op::Rename(rename) => crate::user_types::TypeChange::Rename {
                    new_name: rename.new_name.value.clone(),
                },
                Op::AddValue(add) => crate::user_types::TypeChange::AddValue {
                    label: add.value.value.clone(),
                    if_not_exists: add.if_not_exists,
                    position: add.position.as_ref().map(|p| match p {
                        Position::Before(neighbor) => (true, neighbor.value.clone()),
                        Position::After(neighbor) => (false, neighbor.value.clone()),
                    }),
                },
                Op::RenameValue(rename) => crate::user_types::TypeChange::RenameValue {
                    from: rename.from.value.clone(),
                    to: rename.to.value.clone(),
                },
            };
            Ok(LogicalPlan::AlterType {
                name: alter.name.to_string(),
                change,
                domain: false,
            })
        }
        Statement::Declare { stmts } => {
            let [declare] = stmts.as_slice() else {
                anyhow::bail!("DECLARE of several cursors is not supported");
            };
            let (Some(sqlparser::ast::DeclareType::Cursor), Some(query), [name]) = (
                &declare.declare_type,
                &declare.for_query,
                declare.names.as_slice(),
            ) else {
                anyhow::bail!("DECLARE of a variable is not supported");
            };
            Ok(LogicalPlan::DeclareCursor {
                name: name.value.clone(),
                query: Box::new(plan_query(query, params)?),
                scroll: declare.scroll,
                hold: declare.hold == Some(true),
                binary: declare.binary == Some(true),
                statement: format!("{stmt};"),
            })
        }
        Statement::Close { cursor } => Ok(LogicalPlan::CloseCursor {
            name: match cursor {
                sqlparser::ast::CloseCursor::All => None,
                sqlparser::ast::CloseCursor::Specific { name } => Some(name.value.clone()),
            },
        }),
        Statement::Lock(lock) => Ok(LogicalPlan::LockTable {
            tables: lock.tables.iter().map(|t| t.name.to_string()).collect(),
        }),
        Statement::LISTEN { channel } => Ok(LogicalPlan::Listen {
            channel: channel.value.clone(),
        }),
        Statement::UNLISTEN { channel } => Ok(LogicalPlan::Unlisten {
            channel: (channel.value != "*").then(|| channel.value.clone()),
        }),
        Statement::NOTIFY { channel, payload } => Ok(LogicalPlan::Notify {
            channel: channel.value.clone(),
            payload: payload.clone().unwrap_or_default(),
        }),
        Statement::Prepare {
            name,
            data_types,
            statement,
        } => Ok(LogicalPlan::Prepare {
            name: name.value.clone(),
            param_types: data_types.iter().map(|t| t.to_string()).collect(),
            statement: statement.to_string(),
        }),
        Statement::Execute {
            name: Some(name),
            parameters,
            immediate: false,
            ..
        } => Ok(LogicalPlan::Execute {
            name: name.to_string(),
            params: parameters
                .iter()
                .map(|p| {
                    lower_scalar(p, params)
                        .ok_or_else(|| anyhow::anyhow!("Unsupported expression in EXECUTE: {p}"))
                })
                .collect::<Result<_>>()?,
        }),
        Statement::AlterTable(alter_table) => {
            let table_name = alter_table.name.to_string();
            if alter_table.operations.is_empty() {
                anyhow::bail!("ALTER TABLE without operations");
            }
            // Every operation is planned before any runs, so one NodusDB
            // cannot carry out stops the statement before it changes anything.
            let mut operations = Vec::new();
            for op in &alter_table.operations {
                operations.extend(plan_alter_table_op(op, params)?);
            }
            Ok(LogicalPlan::AlterTable {
                table_name,
                operations,
                if_exists: alter_table.if_exists,
                only: alter_table.only,
            })
        }
        Statement::AlterIndex {
            name,
            operation: sqlparser::ast::AlterIndexOperation::RenameIndex { index_name },
        } => Ok(LogicalPlan::RenameIndex {
            name: name.to_string(),
            new_name: index_name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .map_or_else(|| index_name.to_string(), |i| i.value.clone()),
        }),
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

/// `PARTITION BY RANGE|LIST|HASH (columns)`, as canonical text. The parser
/// hands the clause over as the call `RANGE (id)`.
fn partition_by_text(create_table: &sqlparser::ast::CreateTable) -> Result<Option<String>> {
    let Some(expr) = &create_table.partition_by else {
        return Ok(None);
    };
    let sqlparser::ast::Expr::Function(function) = &**expr else {
        anyhow::bail!("Unsupported PARTITION BY clause: {expr}");
    };
    let strategy = match function.name.to_string().to_ascii_uppercase().as_str() {
        "RANGE" => "RANGE",
        "LIST" => "LIST",
        "HASH" => "HASH",
        other => anyhow::bail!("unsupported partition strategy: {other}"),
    };
    let sqlparser::ast::FunctionArguments::List(list) = &function.args else {
        anyhow::bail!("Unsupported PARTITION BY clause: {expr}");
    };
    let mut columns = Vec::new();
    for arg in &list.args {
        match arg {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                sqlparser::ast::Expr::Identifier(ident),
            )) => columns.push(ident.value.clone()),
            other => anyhow::bail!("partition by expressions are not supported: {other}"),
        }
    }
    Ok(Some(format!("{strategy} ({})", columns.join(", "))))
}

/// A partition's bound, as PostgreSQL renders it.
fn for_values_text(for_values: &sqlparser::ast::ForValues) -> Result<String> {
    use sqlparser::ast::{ForValues, PartitionBoundValue};
    let value = |v: &PartitionBoundValue| match v {
        PartitionBoundValue::Expr(expr) => expr.to_string(),
        PartitionBoundValue::MinValue => "MINVALUE".to_string(),
        PartitionBoundValue::MaxValue => "MAXVALUE".to_string(),
    };
    let list =
        |values: &[PartitionBoundValue]| values.iter().map(value).collect::<Vec<_>>().join(", ");
    Ok(match for_values {
        ForValues::In(values) => format!(
            "FOR VALUES IN ({})",
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ForValues::From { from, to } => {
            format!("FOR VALUES FROM ({}) TO ({})", list(from), list(to))
        }
        ForValues::With { modulus, remainder } => {
            format!("FOR VALUES WITH (modulus {modulus}, remainder {remainder})")
        }
        ForValues::Default => "DEFAULT".to_string(),
    })
}

/// Whether `WITH [NO] DATA` said `NO DATA`; the SQL front end records it as
/// the storage parameter [`nodus_sql::NO_DATA_OPTION`].
/// Plans `ALTER DOMAIN` or `DROP DOMAIN`, which the SQL front end passes as
/// their text ([`nodus_sql::DOMAIN_FUNCTION`]).
fn plan_domain_statement(text: &str) -> Result<LogicalPlan> {
    use crate::user_types::TypeChange;
    use sqlparser::keywords::Keyword as K;
    use sqlparser::tokenizer::Token;
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let mut p = sqlparser::parser::Parser::new(&dialect).try_with_sql(text)?;
    if p.parse_keywords(&[K::DROP, K::DOMAIN]) {
        let if_exists = p.parse_keywords(&[K::IF, K::EXISTS]);
        let names = p.parse_comma_separated(|p| p.parse_object_name(false))?;
        let cascade = p.parse_keyword(K::CASCADE);
        if !cascade {
            p.parse_keyword(K::RESTRICT);
        }
        expect_end(&mut p)?;
        return Ok(LogicalPlan::DropType {
            names: names.iter().map(|n| n.to_string()).collect(),
            if_exists,
            cascade,
            domain: true,
        });
    }
    p.expect_keywords(&[K::ALTER, K::DOMAIN])?;
    let name = p.parse_object_name(false)?.to_string();
    let change = if p.parse_keywords(&[K::SET, K::DEFAULT]) {
        TypeChange::SetDefault(Some(p.parse_expr()?.to_string()))
    } else if p.parse_keywords(&[K::DROP, K::DEFAULT]) {
        TypeChange::SetDefault(None)
    } else if p.parse_keywords(&[K::SET, K::NOT, K::NULL]) {
        TypeChange::SetNotNull(true)
    } else if p.parse_keywords(&[K::DROP, K::NOT, K::NULL]) {
        TypeChange::SetNotNull(false)
    } else if p.parse_keyword(K::ADD) {
        let constraint = if p.parse_keyword(K::CONSTRAINT) {
            Some(p.parse_identifier()?.value)
        } else {
            None
        };
        if p.parse_keywords(&[K::NOT, K::NULL]) {
            TypeChange::SetNotNull(true)
        } else {
            p.expect_keyword(K::CHECK)?;
            p.expect_token(&Token::LParen)?;
            let expr = p.parse_expr()?;
            p.expect_token(&Token::RParen)?;
            TypeChange::AddConstraint {
                name: constraint,
                sql: expr.to_string(),
            }
        }
    } else if p.parse_keywords(&[K::DROP, K::CONSTRAINT]) {
        let if_exists = p.parse_keywords(&[K::IF, K::EXISTS]);
        TypeChange::DropConstraint {
            name: p.parse_identifier()?.value,
            if_exists,
        }
    } else if p.parse_keywords(&[K::RENAME, K::CONSTRAINT]) {
        let from = p.parse_identifier()?.value;
        p.expect_keyword(K::TO)?;
        TypeChange::RenameConstraint {
            from,
            to: p.parse_identifier()?.value,
        }
    } else if p.parse_keywords(&[K::RENAME, K::TO]) {
        TypeChange::Rename {
            new_name: p.parse_identifier()?.value,
        }
    } else if p.parse_keywords(&[K::OWNER, K::TO]) || p.parse_keyword(K::VALIDATE) {
        TypeChange::Nothing
    } else {
        anyhow::bail!("{} is not supported", text.to_ascii_uppercase());
    };
    if change != TypeChange::Nothing {
        expect_end(&mut p)?;
    }
    Ok(LogicalPlan::AlterType {
        name,
        change,
        domain: true,
    })
}

/// Fails, as the parser would, on anything after a statement's end.
fn expect_end(p: &mut sqlparser::parser::Parser) -> Result<()> {
    let next = p.peek_token();
    if next.token != sqlparser::tokenizer::Token::EOF {
        anyhow::bail!("syntax error at or near \"{}\"", next.token);
    }
    Ok(())
}

/// A view's `check_option` (`WITH (check_option = local)`, or `WITH [LOCAL
/// | CASCADED] CHECK OPTION` as the SQL front end writes it).
fn view_check_option(options: &sqlparser::ast::CreateTableOptions) -> Result<Option<String>> {
    let sqlparser::ast::CreateTableOptions::With(options) = options else {
        return Ok(None);
    };
    for option in options {
        if let sqlparser::ast::SqlOption::KeyValue { key, value } = option
            && key.value.eq_ignore_ascii_case("check_option")
        {
            let value = expr_to_value(value, &[])
                .map(|v| crate::value::render(&v).to_ascii_lowercase())
                .unwrap_or_default();
            if value != "local" && value != "cascaded" {
                anyhow::bail!("invalid value for enum option \"check_option\": {value}");
            }
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn has_no_data_marker(options: &sqlparser::ast::CreateTableOptions) -> bool {
    match options {
        sqlparser::ast::CreateTableOptions::With(options) => options.iter().any(|option| {
            matches!(option, sqlparser::ast::SqlOption::KeyValue { key, .. }
                if key.value == nodus_sql::NO_DATA_OPTION)
        }),
        _ => false,
    }
}

/// The view a `REFRESH MATERIALIZED VIEW` names, and whether `WITH DATA`;
/// the SQL front end writes the statement as a call of
/// [`nodus_sql::REFRESH_FUNCTION`].
fn refresh_target(query: &sqlparser::ast::Query) -> Option<(String, bool)> {
    use sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr,
    };
    let SetExpr::Select(select) = &*query.body else {
        return None;
    };
    let [SelectItem::UnnamedExpr(Expr::Function(function))] = select.projection.as_slice() else {
        return None;
    };
    if function.name.to_string() != nodus_sql::REFRESH_FUNCTION || !select.from.is_empty() {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let args: Vec<crate::Value> = list
        .args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr_to_value(e, &[]),
            _ => None,
        })
        .collect::<Option<_>>()?;
    match args.as_slice() {
        [crate::Value::Text(name), crate::Value::Bool(with_data)] => {
            Some((name.clone(), *with_data))
        }
        _ => None,
    }
}

/// The constant arguments of a query that is only a call of `function`, as
/// `nodus_sql` writes the statements the parser lacks.
fn rewritten_call(query: &sqlparser::ast::Query, function: &str) -> Option<Vec<Value>> {
    use sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr,
    };
    let SetExpr::Select(select) = &*query.body else {
        return None;
    };
    let [SelectItem::UnnamedExpr(Expr::Function(call))] = select.projection.as_slice() else {
        return None;
    };
    if call.name.to_string() != function || !select.from.is_empty() {
        return None;
    }
    let FunctionArguments::List(list) = &call.args else {
        return None;
    };
    list.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr_to_value(e, &[]),
            _ => None,
        })
        .collect()
}

/// What the options of `ALTER SEQUENCE` change. `RESTART`, `RENAME TO`,
/// `OWNER TO`, and `OWNED BY` are read here; the rest as `CREATE SEQUENCE`
/// takes them.
fn sequence_change(options: &str, params: &[Value]) -> Result<crate::sequences::SequenceChange> {
    use sqlparser::ast::SequenceOptions as O;
    use sqlparser::tokenizer::{Token, Tokenizer};
    let tokens: Vec<Token> = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, options)
        .tokenize()?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    let word = |i: usize| match tokens.get(i) {
        Some(Token::Word(w)) if w.quote_style.is_none() => Some(w.value.as_str()),
        _ => None,
    };
    let number = |i: usize| match tokens.get(i) {
        Some(Token::Number(n, _)) => n.parse::<i64>().ok(),
        _ => None,
    };
    let mut change = crate::sequences::SequenceChange::default();
    let mut rest: Vec<String> = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        match (word(i), word(i + 1)) {
            (Some("restart"), _) => {
                i += 1;
                if word(i) == Some("with") {
                    i += 1;
                }
                // A signed restart value.
                let negative = tokens.get(i) == Some(&Token::Minus);
                let at = if negative { i + 1 } else { i };
                match number(at) {
                    Some(n) => {
                        change.restart = Some(Some(if negative { -n } else { n }));
                        i = at + 1;
                    }
                    None => change.restart = Some(None),
                }
            }
            (Some("rename"), Some("to")) => {
                change.rename = match tokens.get(i + 2) {
                    Some(Token::Word(w)) => Some(w.value.clone()),
                    _ => anyhow::bail!("ALTER SEQUENCE ... RENAME TO needs a name"),
                };
                i += 3;
            }
            // NodusDB tracks no owners, and a serial column's sequence goes
            // with its table by name.
            (Some("owner"), Some("to")) => i += 3,
            (Some("owned"), Some("by")) => {
                i += 3;
                while tokens.get(i) == Some(&Token::Period) {
                    i += 2;
                }
            }
            (Some("set"), Some("logged" | "unlogged")) => i += 2,
            (Some("set"), Some("schema")) => {
                anyhow::bail!("ALTER SEQUENCE ... SET SCHEMA is not supported")
            }
            _ => {
                rest.push(tokens[i].to_string());
                i += 1;
            }
        }
    }
    if !rest.is_empty() {
        let sql = format!("CREATE SEQUENCE nodus_sequence_options {}", rest.join(" "));
        let mut statements = nodus_sql::parse_sql(&sql)?;
        let Some(sqlparser::ast::Statement::CreateSequence {
            data_type,
            sequence_options,
            ..
        }) = statements.pop()
        else {
            anyhow::bail!("malformed ALTER SEQUENCE options: {options}");
        };
        let integer = |e: &sqlparser::ast::Expr| -> Result<i64> {
            match expr_to_value(e, params) {
                Some(Value::Int(n)) => Ok(n),
                _ => anyhow::bail!("sequence option must be an integer constant: {e}"),
            }
        };
        change.data_type = data_type.map(|t| t.to_string());
        for option in &sequence_options {
            match option {
                O::IncrementBy(e, _) => change.increment = Some(integer(e)?),
                O::MinValue(e) => change.min_value = Some(e.as_ref().map(&integer).transpose()?),
                O::MaxValue(e) => change.max_value = Some(e.as_ref().map(&integer).transpose()?),
                O::StartWith(e, _) => change.start = Some(integer(e)?),
                O::Cache(e) => change.cache = Some(integer(e)?),
                // sqlparser's flag is set for `NO CYCLE`.
                O::Cycle(no_cycle) => change.cycle = Some(!no_cycle),
            }
        }
    }
    Ok(change)
}

/// A relation's name as created: a temporary one lives in the session's
/// temporary schema (`pg_temp`), which is the only schema it may name.
fn temp_relation_name(name: &sqlparser::ast::ObjectName, temporary: bool) -> Result<String> {
    let text = name.to_string();
    if !temporary {
        return Ok(text);
    }
    match text.rsplit_once('.') {
        None => Ok(format!("pg_temp.{text}")),
        Some((schema, _)) if schema.trim_matches('"').eq_ignore_ascii_case("pg_temp") => Ok(text),
        Some(_) => anyhow::bail!("cannot create temporary relation in non-temporary schema"),
    }
}

/// A transaction's access mode (`true` for `READ ONLY`) and isolation level
/// (`read committed`), where given.
fn transaction_modes(modes: &[sqlparser::ast::TransactionMode]) -> (Option<bool>, Option<String>) {
    use sqlparser::ast::{TransactionAccessMode, TransactionMode};
    let (mut read_only, mut isolation) = (None, None);
    for mode in modes {
        match mode {
            TransactionMode::AccessMode(access) => {
                read_only = Some(matches!(access, TransactionAccessMode::ReadOnly));
            }
            TransactionMode::IsolationLevel(level) => {
                isolation = Some(level.to_string().to_ascii_lowercase());
            }
        }
    }
    (read_only, isolation)
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

/// One `ALTER TABLE` operation, as the operations that carry it out: `ADD
/// COLUMN` with constraints adds the column, then each constraint.
fn plan_alter_table_op(
    op: &sqlparser::ast::AlterTableOperation,
    params: &[Value],
) -> Result<Vec<AlterTableOp>> {
    use sqlparser::ast::{AlterColumnOperation, AlterTableOperation as Op, ColumnOption};
    let cascade = |behavior: &Option<sqlparser::ast::DropBehavior>| {
        matches!(behavior, Some(sqlparser::ast::DropBehavior::Cascade))
    };
    Ok(match op {
        Op::AddColumn {
            column_def,
            if_not_exists,
            ..
        } => {
            let column = column_def.name.value.clone();
            let mut data_type = column_def.data_type.to_string();
            let mut nullable = true;
            let mut default = None;
            let mut constraints = Vec::new();
            let mut sequence = None;
            let mut identity = None;
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
            }
            for opt in &column_def.options {
                let name = opt.name.as_ref().map(|n| n.value.clone());
                match &opt.option {
                    ColumnOption::Generated {
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
                        identity =
                            Some(matches!(generated_as, sqlparser::ast::GeneratedAs::Always));
                    }
                    ColumnOption::Null => {}
                    ColumnOption::NotNull => nullable = false,
                    ColumnOption::Default(e) => {
                        default = Some(lower_scalar(e, params).ok_or_else(|| {
                            anyhow::anyhow!("Unsupported DEFAULT expression for column {column}")
                        })?);
                    }
                    ColumnOption::Unique(uc) => {
                        let (deferrable, initially_deferred) = deferral(&uc.characteristics)?;
                        constraints.push(NewConstraint::Unique {
                            name,
                            columns: vec![column.clone()],
                            deferrable,
                            initially_deferred,
                            nulls_not_distinct: nulls_not_distinct(uc),
                        })
                    }
                    ColumnOption::PrimaryKey(pk) => {
                        nullable = false;
                        let (deferrable, initially_deferred) = deferral(&pk.characteristics)?;
                        constraints.push(NewConstraint::PrimaryKey {
                            name,
                            columns: vec![column.clone()],
                            deferrable,
                            initially_deferred,
                        });
                    }
                    ColumnOption::Check(check) => {
                        check_constraint_is_supported(&check.expr, params)?;
                        constraints.push(NewConstraint::Check {
                            name,
                            expr: check.expr.to_string(),
                        });
                    }
                    ColumnOption::ForeignKey(fk) => {
                        constraints.push(NewConstraint::ForeignKey(foreign_key(
                            opt.name.as_ref().or(fk.name.as_ref()),
                            vec![column.clone()],
                            fk,
                        )?))
                    }
                    other => anyhow::bail!(
                        "ALTER TABLE ... ADD COLUMN with {} is not supported",
                        leading_keywords(&other.to_string())
                    ),
                }
            }
            std::iter::once(AlterTableOp::AddColumn {
                name: column,
                data_type,
                nullable,
                default,
                if_not_exists: *if_not_exists,
                sequence,
                identity,
            })
            .chain(
                constraints
                    .into_iter()
                    .map(|constraint| AlterTableOp::AddConstraint {
                        constraint,
                        not_valid: false,
                    }),
            )
            .collect()
        }
        Op::RenameColumn {
            old_column_name,
            new_column_name,
        } => vec![AlterTableOp::RenameColumn {
            old_name: old_column_name.value.clone(),
            new_name: new_column_name.value.clone(),
        }],
        Op::DropColumn {
            column_names,
            if_exists,
            drop_behavior,
            ..
        } => column_names
            .iter()
            .map(|name| AlterTableOp::DropColumn {
                name: name.value.clone(),
                if_exists: *if_exists,
                cascade: cascade(drop_behavior),
            })
            .collect(),
        Op::AlterColumn { column_name, op } => {
            let column = column_name.value.clone();
            vec![match op {
                AlterColumnOperation::SetDataType { data_type, .. } => {
                    AlterTableOp::AlterColumnType {
                        name: column,
                        data_type: data_type.to_string(),
                    }
                }
                AlterColumnOperation::SetNotNull => AlterTableOp::SetNotNull {
                    column,
                    not_null: true,
                },
                AlterColumnOperation::DropNotNull => AlterTableOp::SetNotNull {
                    column,
                    not_null: false,
                },
                AlterColumnOperation::SetDefault { value } => {
                    let lowered = lower_scalar(value, params).ok_or_else(|| {
                        anyhow::anyhow!("Unsupported DEFAULT expression for column {column}")
                    })?;
                    if crate::subqueries::contains_subquery(&lowered) {
                        anyhow::bail!("cannot use subquery in DEFAULT expression");
                    }
                    AlterTableOp::SetDefault {
                        column,
                        default: Some(lowered),
                    }
                }
                AlterColumnOperation::DropDefault => AlterTableOp::SetDefault {
                    column,
                    default: None,
                },
                other => anyhow::bail!(
                    "ALTER TABLE ... ALTER COLUMN ... {} is not supported",
                    leading_keywords(&other.to_string())
                ),
            }]
        }
        Op::AddConstraint {
            constraint,
            not_valid,
        } => {
            use sqlparser::ast::TableConstraint as C;
            let constraint = match constraint {
                C::Check(check) => {
                    check_constraint_is_supported(&check.expr, params)?;
                    NewConstraint::Check {
                        name: check.name.as_ref().map(|n| n.value.clone()),
                        expr: check.expr.to_string(),
                    }
                }
                C::Unique(unique) => {
                    let (deferrable, initially_deferred) = deferral(&unique.characteristics)?;
                    NewConstraint::Unique {
                        name: unique.name.as_ref().map(|n| n.value.clone()),
                        columns: index_column_names(&unique.columns),
                        deferrable,
                        initially_deferred,
                        nulls_not_distinct: nulls_not_distinct(unique),
                    }
                }
                C::PrimaryKey(pk) => {
                    let (deferrable, initially_deferred) = deferral(&pk.characteristics)?;
                    NewConstraint::PrimaryKey {
                        name: pk.name.as_ref().map(|n| n.value.clone()),
                        columns: index_column_names(&pk.columns),
                        deferrable,
                        initially_deferred,
                    }
                }
                C::ForeignKey(fk) => NewConstraint::ForeignKey(foreign_key(
                    fk.name.as_ref(),
                    fk.columns.iter().map(|c| c.value.clone()).collect(),
                    fk,
                )?),
                other => anyhow::bail!(
                    "ALTER TABLE ... ADD {} is not supported",
                    leading_keywords(&other.to_string())
                ),
            };
            vec![AlterTableOp::AddConstraint {
                constraint,
                not_valid: *not_valid,
            }]
        }
        Op::DropConstraint {
            if_exists,
            name,
            drop_behavior,
        } => vec![AlterTableOp::DropConstraint {
            name: name.value.clone(),
            if_exists: *if_exists,
            cascade: cascade(drop_behavior),
        }],
        Op::RenameConstraint { old_name, new_name } => vec![AlterTableOp::RenameConstraint {
            old_name: old_name.value.clone(),
            new_name: new_name.value.clone(),
        }],
        Op::ValidateConstraint { name } => vec![AlterTableOp::ValidateConstraint {
            name: name.value.clone(),
        }],
        Op::RenameTable { table_name: new } => {
            let new_name = match new {
                sqlparser::ast::RenameTableNameKind::As(name)
                | sqlparser::ast::RenameTableNameKind::To(name) => name.to_string(),
            };
            vec![AlterTableOp::RenameTable { new_name }]
        }
        Op::OwnerTo { new_owner } => vec![AlterTableOp::OwnerTo {
            owner: match new_owner {
                sqlparser::ast::Owner::Ident(ident) => ident.value.clone(),
                sqlparser::ast::Owner::CurrentRole
                | sqlparser::ast::Owner::CurrentUser
                | sqlparser::ast::Owner::SessionUser => "current_user".to_string(),
            },
        }],
        _ => anyhow::bail!(
            "ALTER TABLE ... {} is not supported",
            leading_keywords(&op.to_string())
        ),
    })
}

/// A `FOREIGN KEY` / `REFERENCES` constraint over `columns`. A key that
/// names no referenced columns references the primary key, resolved when
/// the constraint is created.
fn foreign_key(
    name: Option<&sqlparser::ast::Ident>,
    columns: Vec<String>,
    fk: &sqlparser::ast::ForeignKeyConstraint,
) -> Result<nodus_catalog::TableConstraint> {
    use nodus_catalog::ReferentialAction as A;
    use sqlparser::ast::ConstraintReferenceMatchKind as M;
    if matches!(fk.match_kind, Some(M::Full | M::Partial)) {
        anyhow::bail!("MATCH FULL and MATCH PARTIAL foreign keys are not supported");
    }
    let action = |action: Option<sqlparser::ast::ReferentialAction>| match action {
        None | Some(sqlparser::ast::ReferentialAction::NoAction) => A::NoAction,
        Some(sqlparser::ast::ReferentialAction::Restrict) => A::Restrict,
        Some(sqlparser::ast::ReferentialAction::Cascade) => A::Cascade,
        Some(sqlparser::ast::ReferentialAction::SetNull) => A::SetNull,
        Some(sqlparser::ast::ReferentialAction::SetDefault) => A::SetDefault,
    };
    let (deferrable, initially_deferred) = deferral(&fk.characteristics)?;
    Ok(nodus_catalog::TableConstraint::ForeignKey {
        name: name.map(|n| n.value.clone()),
        columns,
        foreign_table: fk.foreign_table.to_string(),
        referred_columns: fk
            .referred_columns
            .iter()
            .map(|i| i.value.clone())
            .collect(),
        on_delete: action(fk.on_delete),
        on_update: action(fk.on_update),
        deferrable,
        initially_deferred,
    })
}

/// Whether a `UNIQUE` constraint says `NULLS NOT DISTINCT` — read from the
/// parser's clause, or from the marker a column constraint is rewritten to
/// ([`nodus_sql::NULLS_NOT_DISTINCT_MARK`]).
fn nulls_not_distinct(constraint: &sqlparser::ast::UniqueConstraint) -> bool {
    matches!(
        constraint.nulls_distinct,
        sqlparser::ast::NullsDistinctOption::NotDistinct
    ) || constraint
        .index_name
        .as_ref()
        .is_some_and(|name| name.value == nodus_sql::NULLS_NOT_DISTINCT_MARK)
}

/// A constraint's `DEFERRABLE` / `INITIALLY DEFERRED` characteristics as a
/// `(deferrable, initially deferred)` pair. `INITIALLY DEFERRED` implies
/// `DEFERRABLE`, as in PostgreSQL.
fn deferral(
    characteristics: &Option<sqlparser::ast::ConstraintCharacteristics>,
) -> Result<(bool, bool)> {
    use sqlparser::ast::DeferrableInitial;
    let Some(c) = characteristics else {
        return Ok((false, false));
    };
    if matches!(c.enforced, Some(false)) {
        anyhow::bail!("NOT ENFORCED constraints are not supported");
    }
    let initially = matches!(c.initially, Some(DeferrableInitial::Deferred));
    if c.deferrable == Some(false) && initially {
        anyhow::bail!("constraint declared INITIALLY DEFERRED must be DEFERRABLE");
    }
    Ok((c.deferrable.unwrap_or(initially), initially))
}

/// The privilege names of a `GRANT` / `REVOKE`, uppercased as PostgreSQL
/// reports them; `ALL` covers `ALL [PRIVILEGES]`.
fn grant_privileges(privileges: &sqlparser::ast::Privileges) -> Vec<String> {
    match privileges {
        sqlparser::ast::Privileges::All { .. } => vec!["ALL".to_string()],
        sqlparser::ast::Privileges::Actions(actions) => actions
            .iter()
            .map(|action| match action {
                // PostgreSQL reports both spellings as TEMP.
                sqlparser::ast::Action::Temporary => "TEMP".to_string(),
                other => other.to_string().to_ascii_uppercase(),
            })
            .collect(),
    }
}

/// The objects a `GRANT` / `REVOKE` names.
fn grant_objects(
    objects: Option<&sqlparser::ast::GrantObjects>,
    _params: &[Value],
) -> Result<crate::plan_types::GrantObjectsPlan> {
    use crate::plan_types::GrantObjectsPlan;
    use sqlparser::ast::GrantObjects as O;
    let Some(objects) = objects else {
        anyhow::bail!("GRANT or REVOKE without objects");
    };
    // The identifier's own text: `"default"` names `default`, and a
    // qualified name keeps its qualifier.
    let names = |names: &[sqlparser::ast::ObjectName]| -> Vec<String> {
        names
            .iter()
            .map(|name| {
                name.0
                    .iter()
                    .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
                    .collect::<Vec<_>>()
                    .join(".")
            })
            .collect()
    };
    Ok(match objects {
        O::Tables(names_list) => GrantObjectsPlan::ByName {
            kind: "TABLE".to_string(),
            names: names(names_list),
        },
        O::Views(names_list) => GrantObjectsPlan::ByName {
            kind: "VIEW".to_string(),
            names: names(names_list),
        },
        O::Sequences(names_list) => GrantObjectsPlan::ByName {
            kind: "SEQUENCE".to_string(),
            names: names(names_list),
        },
        O::Schemas(names_list) => GrantObjectsPlan::ByName {
            kind: "SCHEMA".to_string(),
            names: names(names_list),
        },
        O::Databases(names_list) => GrantObjectsPlan::ByName {
            kind: "DATABASE".to_string(),
            names: names(names_list),
        },
        O::AllTablesInSchema { schemas } => GrantObjectsPlan::AllInSchema {
            kind: "TABLE".to_string(),
            schemas: names(schemas),
        },
        O::AllSequencesInSchema { schemas } => GrantObjectsPlan::AllInSchema {
            kind: "SEQUENCE".to_string(),
            schemas: names(schemas),
        },
        O::AllViewsInSchema { schemas } => GrantObjectsPlan::AllInSchema {
            kind: "VIEW".to_string(),
            schemas: names(schemas),
        },
        other => anyhow::bail!("GRANT or REVOKE target {other} is not supported"),
    })
}

/// A password expression as a SCRAM-SHA-256 verifier (never the plaintext).
fn password_verifier(password: &sqlparser::ast::Password, params: &[Value]) -> Result<String> {
    let sqlparser::ast::Password::Password(expr) = password else {
        anyhow::bail!("PASSWORD NULL is not supported; use ALTER ROLE ... PASSWORD NULL");
    };
    let Some(Value::Text(text)) = expr_to_value(expr, params) else {
        anyhow::bail!("PASSWORD must be a string literal");
    };
    let salt = uuid::Uuid::new_v4().as_bytes().to_vec();
    Ok(
        nodus_security::ScramKeys::derive(&text, salt, nodus_security::PBKDF2_ITERATIONS)
            .to_verifier_string(),
    )
}

/// A `CONNECTION LIMIT` / `VALID UNTIL` expression.
fn role_integer(expr: &sqlparser::ast::Expr, params: &[Value], what: &str) -> Result<i32> {
    let value = match expr {
        sqlparser::ast::Expr::UnaryOp { op, expr } => {
            let sqlparser::ast::UnaryOperator::Minus = op else {
                anyhow::bail!("{what} must be an integer constant");
            };
            match expr_to_value(expr, params) {
                Some(Value::Int(n)) => Value::Int(-n),
                _ => anyhow::bail!("{what} must be an integer constant"),
            }
        }
        other => expr_to_value(other, params)
            .ok_or_else(|| anyhow::anyhow!("{what} must be an integer constant"))?,
    };
    match value {
        Value::Int(n) => i32::try_from(n).map_err(|_| anyhow::anyhow!("{what} is out of range")),
        _ => anyhow::bail!("{what} must be an integer constant"),
    }
}

/// `CREATE ROLE`'s attributes.
#[allow(clippy::too_many_arguments)]
fn role_attributes(
    login: Option<bool>,
    inherit: Option<bool>,
    bypassrls: Option<bool>,
    superuser: Option<bool>,
    create_db: Option<bool>,
    create_role: Option<bool>,
    replication: Option<bool>,
    connection_limit: &Option<sqlparser::ast::Expr>,
    valid_until: &Option<sqlparser::ast::Expr>,
    password: &Option<sqlparser::ast::Password>,
    params: &[Value],
) -> Result<nodus_catalog::RoleAttributes> {
    let mut attributes = nodus_catalog::RoleAttributes::default();
    attributes.can_login = login.unwrap_or(false);
    attributes.inherit = inherit.unwrap_or(true);
    attributes.bypass_rls = bypassrls.unwrap_or(false);
    attributes.superuser = superuser.unwrap_or(false);
    attributes.create_db = create_db.unwrap_or(false);
    attributes.create_role = create_role.unwrap_or(false);
    attributes.replication = replication.unwrap_or(false);
    if let Some(limit) = connection_limit {
        attributes.connection_limit = role_integer(limit, params, "CONNECTION LIMIT")?;
    }
    if let Some(until) = valid_until {
        match expr_to_value(until, params) {
            Some(Value::Text(text)) => attributes.valid_until = Some(text),
            _ => anyhow::bail!("VALID UNTIL must be a string literal"),
        }
    }
    if let Some(password) = password {
        attributes.password = Some(password_verifier(password, params)?);
    }
    Ok(attributes)
}

/// `ALTER ROLE`'s option patch.
fn role_attrs_patch(
    options: &[sqlparser::ast::RoleOption],
    params: &[Value],
) -> Result<crate::plan_types::RoleAttrsPatch> {
    use sqlparser::ast::RoleOption as O;
    let mut patch = crate::plan_types::RoleAttrsPatch::default();
    for option in options {
        match option {
            O::Login(flag) => patch.can_login = Some(*flag),
            O::Inherit(flag) => patch.inherit = Some(*flag),
            O::BypassRLS(flag) => patch.bypass_rls = Some(*flag),
            O::SuperUser(flag) => patch.superuser = Some(*flag),
            O::CreateDB(flag) => patch.create_db = Some(*flag),
            O::CreateRole(flag) => patch.create_role = Some(*flag),
            O::Replication(flag) => patch.replication = Some(*flag),
            O::ConnectionLimit(expr) => {
                patch.connection_limit = Some(role_integer(expr, params, "CONNECTION LIMIT")?)
            }
            O::ValidUntil(expr) => match expr_to_value(expr, params) {
                Some(Value::Text(text)) => patch.valid_until = Some(Some(text)),
                Some(Value::Null) => patch.valid_until = Some(None),
                _ => anyhow::bail!("VALID UNTIL must be a string literal"),
            },
            O::Password(password) => match password {
                sqlparser::ast::Password::Password(expr) => {
                    patch.password = Some(password_verifier(password, params)?)
                }
                sqlparser::ast::Password::NullPassword => patch.password = None,
            },
        }
    }
    let _ = params;
    Ok(patch)
}

/// Which statement a `RETURNING` list belongs to.
#[derive(Clone, Copy, PartialEq)]
enum ReturningOf {
    Insert,
    Change,
    Merge,
}

/// Plans a `RETURNING` list: column items as names, qualified as written
/// (`*` and `x.*` expand at execution; `new.` names the row as written and
/// `old.` the row as it was, each null when there is none), and any other
/// item as an expression under its output name, which `returning` then
/// holds. The expressions come back parallel to the names, or empty when
/// there are none.
fn plan_returning(
    items: &Option<Vec<sqlparser::ast::SelectItem>>,
    of: ReturningOf,
    params: &[Value],
) -> Result<(Vec<String>, Vec<Option<crate::ReturningExpr>>)> {
    use sqlparser::ast::{Expr, SelectItem, SelectItemQualifiedWildcardKind};
    let Some(items) = items else {
        return Ok((Vec::new(), Vec::new()));
    };
    let qualify = |qualifier: &str| -> Result<String> {
        if qualifier.eq_ignore_ascii_case("new") || qualifier.eq_ignore_ascii_case("old") {
            Ok(qualifier.to_ascii_lowercase())
        } else {
            Ok(qualifier.to_string())
        }
    };
    let mut names = Vec::with_capacity(items.len());
    let mut exprs = Vec::with_capacity(items.len());
    for item in items {
        let (expr, alias) = match item {
            SelectItem::Wildcard(_) => {
                names.push("*".to_string());
                exprs.push(None);
                continue;
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _) => {
                names.push(format!("{}.*", qualify(&name.to_string())?));
                exprs.push(None);
                continue;
            }
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            other => anyhow::bail!("Unsupported RETURNING item: {other}"),
        };
        match (expr, &alias) {
            (Expr::Identifier(id), None) => {
                names.push(id.value.clone());
                exprs.push(None);
            }
            (Expr::CompoundIdentifier(parts), None) if parts.len() == 2 => {
                names.push(format!("{}.{}", qualify(&parts[0].value)?, parts[1].value));
                exprs.push(None);
            }
            _ => {
                let lowered = returning_expr(expr, of, params)?;
                if scalar_has_aggregate(&lowered) {
                    anyhow::bail!("aggregate functions are not allowed in RETURNING");
                }
                let mut failed = None;
                let lowered = map_columns(&lowered, &mut |name| match name.split_once('.') {
                    Some((q, column)) if !q.contains('.') => match qualify(q) {
                        Ok(q) => format!("{q}.{column}"),
                        Err(e) => {
                            failed = Some(e);
                            name.to_string()
                        }
                    },
                    _ => name.to_string(),
                });
                if let Some(e) = failed {
                    return Err(e);
                }
                names.push(alias.unwrap_or_else(|| default_output_name(expr)));
                exprs.push(Some(crate::ReturningExpr { expr: lowered }));
            }
        }
    }
    if exprs.iter().all(Option::is_none) {
        exprs.clear();
    }
    Ok((names, exprs))
}

/// The column MERGE's RETURNING list reads `merge_action()` from.
pub(crate) const MERGE_ACTION_COLUMN: &str = "\u{0}merge_action";

/// A RETURNING expression; `merge_action()` reads the action a MERGE took.
fn returning_expr(
    expr: &sqlparser::ast::Expr,
    of: ReturningOf,
    params: &[Value],
) -> Result<ScalarExpr> {
    if let sqlparser::ast::Expr::Function(function) = expr
        && function
            .name
            .to_string()
            .trim_start_matches("pg_catalog.")
            .eq_ignore_ascii_case("merge_action")
    {
        if of != ReturningOf::Merge {
            anyhow::bail!(
                "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command"
            );
        }
        return Ok(ScalarExpr::Column(MERGE_ACTION_COLUMN.to_string()));
    }
    lower_scalar(expr, params)
        .ok_or_else(|| expression_error(expr, || format!("Unsupported RETURNING item: {expr}")))
}

/// `expr` with each column name replaced by `rename`'s.
fn map_columns(expr: &ScalarExpr, rename: &mut dyn FnMut(&str) -> String) -> ScalarExpr {
    match expr {
        ScalarExpr::Column(name) => ScalarExpr::Column(rename(name)),
        _ => expr.map_children(&mut |e| map_columns(e, rename)),
    }
}

/// Plans `MERGE INTO target USING source ON condition WHEN ...`.
fn plan_merge(merge: &sqlparser::ast::Merge, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::{
        MergeAction as Action, MergeClauseKind, MergeInsertKind, MergeUpdateKind, OutputClause,
        TableWithJoins,
    };
    let (table_name, table_alias, only) = target_table(&merge.table)?;
    let (returning, returning_exprs) = match &merge.output {
        Some(OutputClause::Returning { select_items, .. }) => {
            plan_returning(&Some(select_items.clone()), ReturningOf::Merge, params)?
        }
        Some(other) => anyhow::bail!("Unsupported MERGE clause: {other}"),
        None => (Vec::new(), Vec::new()),
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
        only,
        table_name,
        table_alias,
        source: Box::new(source),
        on: parse_predicates(&Some((*merge.on).clone()), params)?,
        clauses,
        returning,
        returning_exprs,
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
    // A dotted target is a column's field (`p.a = ...`) or, for a column
    // that is not one of a composite type, the column qualified by its
    // table; the executor tells them apart.
    let column = |name: &sqlparser::ast::ObjectName| {
        name.0
            .iter()
            .map(|p| p.as_ident().map(|i| i.value.clone()))
            .collect::<Option<Vec<_>>>()
            .map(|parts| parts.join("."))
            .ok_or_else(|| anyhow::anyhow!("Unsupported assignment target: {name}"))
    };
    let mut out = Vec::new();
    for a in assignments {
        match &a.target {
            AssignmentTarget::ColumnName(name) => {
                out.push((column(name)?, assignment_value(&a.value, params)?));
            }
            // `SET (a, b) = (SELECT x, y ...)`: each column the matching
            // column of the (scalar) subquery.
            AssignmentTarget::Tuple(names) if matches!(a.value, Expr::Subquery(_)) => {
                let Expr::Subquery(query) = &a.value else {
                    unreachable!("guarded by the match arm");
                };
                let aliases: Vec<String> =
                    (1..=names.len()).map(|i| format!("\"__c{i}\"")).collect();
                for (i, name) in names.iter().enumerate() {
                    let sql = format!(
                        "(SELECT {} FROM ({query}) AS __multi({}))",
                        aliases[i],
                        aliases.join(", ")
                    );
                    let expr =
                        sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
                            .try_with_sql(&sql)
                            .and_then(|mut p| p.parse_expr())
                            .map_err(|e| {
                                anyhow::anyhow!("Unsupported multi-column assignment source: {e}")
                            })?;
                    out.push((column(name)?, assignment_value(&expr, params)?));
                }
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
