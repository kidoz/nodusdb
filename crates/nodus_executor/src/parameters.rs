//! Conservative PostgreSQL parameter inference from parsed expressions and
//! catalog columns. No query execution or SQL string substitution is involved.

use crate::{ExecutionContext, MemExecutor, parse_object_name};
use anyhow::{Result, bail};
use nodus_authz::Action;
use nodus_catalog::ResourceRef;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr, FromTable, Query, SelectItem, SetExpr, Statement,
    TableFactor, TableObject, Value,
};
use sqlparser::{
    dialect::PostgreSqlDialect,
    tokenizer::{Token, Tokenizer},
};
use std::collections::HashMap;

type Columns = HashMap<String, Option<String>>;

struct Inference {
    types: Vec<Option<String>>,
}

impl Inference {
    fn expression(
        &mut self,
        expr: &Expr,
        expected: Option<String>,
        columns: &Columns,
    ) -> Result<()> {
        match expr {
            Expr::Value(v) => {
                if let Value::Placeholder(name) = &v.value {
                    let index = parameter_index(name)?;
                    if let Some(ty) = expected {
                        // Keep the first contextual type; normal execution still
                        // validates the value in every place the parameter is used.
                        self.types[index - 1].get_or_insert(ty);
                    }
                }
            }
            Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
                self.expression(inner, expected, columns)?
            }
            Expr::Cast {
                expr, data_type, ..
            } => self.expression(expr, Some(data_type.to_string()), columns)?,
            Expr::BinaryOp { left, op, right } => {
                let boolean = matches!(op, BinaryOperator::And | BinaryOperator::Or);
                let left_type = expression_type(left, columns);
                let right_type = expression_type(right, columns);
                let context = if boolean {
                    Some("BOOLEAN".into())
                } else {
                    expected
                };
                self.expression(left, right_type.or_else(|| context.clone()), columns)?;
                self.expression(right, left_type.or(context), columns)?;
            }
            Expr::InList { expr, list, .. } => {
                let ty = expression_type(expr, columns);
                self.expression(expr, None, columns)?;
                for item in list {
                    self.expression(item, ty.clone(), columns)?;
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                let ty = expression_type(expr, columns);
                self.expression(low, ty.clone(), columns)?;
                self.expression(high, ty, columns)?;
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => self.expression(expr, None, columns)?,
            _ => {}
        }
        Ok(())
    }
}

fn parameter_index(name: &str) -> Result<usize> {
    let index = name
        .strip_prefix('$')
        .and_then(|n| n.parse::<usize>().ok())
        .filter(|n| (1..=65535).contains(n))
        .ok_or_else(|| anyhow::anyhow!("invalid parameter index {name}"))?;
    Ok(index)
}

fn expression_type(expr: &Expr, columns: &Columns) -> Option<String> {
    match expr {
        Expr::Identifier(id) => columns.get(&id.value).cloned().flatten(),
        Expr::CompoundIdentifier(ids) => columns
            .get(
                &ids.iter()
                    .map(|id| id.value.as_str())
                    .collect::<Vec<_>>()
                    .join("."),
            )
            .cloned()
            .flatten(),
        Expr::Nested(expr) => expression_type(expr, columns),
        Expr::Cast { data_type, .. } => Some(data_type.to_string()),
        Expr::Value(v) => match &v.value {
            Value::Boolean(_) => Some("BOOLEAN".into()),
            Value::Number(n, _) if n.parse::<i32>().is_ok() => Some("INTEGER".into()),
            // PostgreSQL string literals initially have unknown type.
            _ => None,
        },
        _ => None,
    }
}

impl MemExecutor {
    fn parameter_columns(
        &self,
        ctx: &ExecutionContext,
        table: &str,
        alias: Option<&str>,
        action: Action,
    ) -> Result<(Columns, Vec<String>)> {
        let (db, schema, name) = parse_object_name(table)?;
        let descriptor = self.catalog_reader.get_table(db, schema, name)?;
        self.authorize(ctx, action, ResourceRef::Table(descriptor.id))?;
        let mut columns = Columns::new();
        let mut ordered = Vec::new();
        for col in &descriptor.columns {
            ordered.push(col.data_type.clone());
            columns.insert(col.name.clone(), Some(col.data_type.clone()));
            columns.insert(
                format!("{}.{}", alias.unwrap_or(name), col.name),
                Some(col.data_type.clone()),
            );
        }
        Ok((columns, ordered))
    }

    fn relation_parameters(
        &self,
        ctx: &ExecutionContext,
        relation: &TableFactor,
        action: Action,
    ) -> Result<Columns> {
        if let TableFactor::Table { name, alias, .. } = relation {
            self.parameter_columns(
                ctx,
                &name.to_string(),
                alias.as_ref().map(|a| a.name.value.as_str()),
                action,
            )
            .map(|(columns, _)| columns)
        } else {
            Ok(Columns::new())
        }
    }

    fn query_parameters(
        &self,
        ctx: &ExecutionContext,
        query: &Query,
        inference: &mut Inference,
    ) -> Result<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.query_parameters(ctx, &cte.query, inference)?;
            }
            // CTE output binding remains with the query planner; do not look up
            // CTE names as physical catalog tables in the outer query.
            return Ok(());
        }
        if let SetExpr::Select(select) = query.body.as_ref() {
            let mut columns = Columns::new();
            for from in &select.from {
                for relation in
                    std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation))
                {
                    for (name, ty) in self.relation_parameters(ctx, relation, Action::Select)? {
                        if columns.contains_key(&name) {
                            columns.insert(name, None);
                        } else {
                            columns.insert(name, ty);
                        }
                    }
                }
            }
            if let Some(expr) = &select.selection {
                inference.expression(expr, Some("BOOLEAN".into()), &columns)?;
            }
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
                {
                    inference.expression(expr, None, &columns)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn infer_sql_parameters(
        &self,
        ctx: &ExecutionContext,
        sql: &str,
    ) -> Result<Vec<Option<String>>> {
        let mut count = 0;
        for token in Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize()? {
            if let Token::Placeholder(name) = token {
                count = count.max(parameter_index(&name)?);
            }
        }
        if count == 0 {
            return Ok(vec![]);
        }
        let mut inference = Inference {
            types: vec![None; count],
        };
        let statements = nodus_sql::parse_sql(sql)?;
        if statements.len() != 1 {
            bail!("prepared statement must contain exactly one statement");
        }
        match &statements[0] {
            Statement::Insert(insert) => {
                if let TableObject::TableName(name) = &insert.table {
                    let (columns, ordered) =
                        self.parameter_columns(ctx, &name.to_string(), None, Action::Insert)?;
                    if let Some(source) = &insert.source {
                        if let SetExpr::Values(values) = source.body.as_ref() {
                            for row in &values.rows {
                                for (i, expr) in row.iter().enumerate() {
                                    let ty = if insert.columns.is_empty() {
                                        ordered.get(i).cloned()
                                    } else {
                                        insert
                                            .columns
                                            .get(i)
                                            .and_then(|n| n.0.last())
                                            .and_then(|n| n.as_ident())
                                            .and_then(|n| columns.get(&n.value))
                                            .cloned()
                                            .flatten()
                                    };
                                    inference.expression(expr, ty, &columns)?;
                                }
                            }
                        }
                    }
                }
            }
            Statement::Update(update) => {
                let columns =
                    self.relation_parameters(ctx, &update.table.relation, Action::Update)?;
                for assignment in &update.assignments {
                    let ty = match &assignment.target {
                        AssignmentTarget::ColumnName(name) => name
                            .0
                            .last()
                            .and_then(|n| n.as_ident())
                            .and_then(|n| columns.get(&n.value))
                            .cloned()
                            .flatten(),
                        _ => None,
                    };
                    inference.expression(&assignment.value, ty, &columns)?;
                }
                if let Some(expr) = &update.selection {
                    inference.expression(expr, Some("BOOLEAN".into()), &columns)?;
                }
            }
            Statement::Delete(delete) => {
                let (FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables)) =
                    &delete.from;
                if let Some(table) = tables.first() {
                    let columns = self.relation_parameters(ctx, &table.relation, Action::Delete)?;
                    if let Some(expr) = &delete.selection {
                        inference.expression(expr, Some("BOOLEAN".into()), &columns)?;
                    }
                }
            }
            Statement::Query(query) => self.query_parameters(ctx, query, &mut inference)?,
            _ => {}
        }
        Ok(inference.types)
    }
}
