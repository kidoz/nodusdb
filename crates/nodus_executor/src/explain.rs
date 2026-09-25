//! `EXPLAIN`: how a statement runs, described in PostgreSQL's plan
//! vocabulary (scans, nested-loop joins, aggregation, sorting, limits) with
//! rough cost and row estimates, as text or JSON. `ANALYZE` runs the
//! statement and reports its actual rows and time.

use crate::*;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value as J, json};

/// The options of an `EXPLAIN`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainOptions {
    pub analyze: bool,
    pub costs: bool,
    pub timing: bool,
    /// `SUMMARY`: planning and execution times; on by default with ANALYZE.
    pub summary: Option<bool>,
    pub json: bool,
}

impl Default for ExplainOptions {
    fn default() -> Self {
        ExplainOptions {
            analyze: false,
            costs: true,
            timing: true,
            summary: None,
            json: false,
        }
    }
}

/// A plan node as `EXPLAIN` shows it.
struct Node {
    /// `Seq Scan`, `Nested Loop`, ...
    kind: String,
    /// The text-format headline, e.g. `Seq Scan on t a`.
    label: String,
    /// JSON-format properties naming what the node reads or does.
    props: Vec<(&'static str, J)>,
    /// What the node computes, e.g. `Filter: (a = 1)`: a line under the
    /// headline in text format, a property after the costs in JSON.
    details: Vec<(&'static str, String, J)>,
    rows: f64,
    width: u32,
    startup: f64,
    total: f64,
    /// `ANALYZE`'s measurements, as JSON properties.
    actual: Vec<(&'static str, J)>,
    children: Vec<Node>,
}

impl Node {
    fn new(kind: &str, label: impl Into<String>, rows: f64, width: u32) -> Node {
        Node {
            kind: kind.to_string(),
            label: label.into(),
            props: Vec::new(),
            details: Vec::new(),
            rows: rows.max(1.0).round(),
            width,
            startup: 0.0,
            total: 0.0,
            actual: Vec::new(),
            children: Vec::new(),
        }
    }

    /// A node over `children`, costed from them.
    fn over(kind: &str, label: impl Into<String>, children: Vec<Node>) -> Node {
        let (rows, width) = children.first().map_or((1.0, 0), |c| (c.rows, c.width));
        let mut node = Node::new(kind, label, rows, width);
        node.startup = children.first().map_or(0.0, |c| c.startup);
        node.total = children.iter().map(|c| c.total).sum::<f64>() + rows * CPU_TUPLE;
        node.children = children;
        node
    }

    fn detail(mut self, name: &'static str, text: String) -> Node {
        self.details.push((name, text.clone(), J::String(text)));
        self
    }

    /// A list detail, such as sort or grouping keys.
    fn keys(mut self, name: &'static str, keys: Vec<String>) -> Node {
        self.details.push((name, keys.join(", "), json!(keys)));
        self
    }

    fn prop(mut self, name: &'static str, value: J) -> Node {
        self.props.push((name, value));
        self
    }

    fn write_text(&self, depth: usize, opts: &ExplainOptions, actual: &str, out: &mut Vec<String>) {
        let mut line = if depth == 0 {
            self.label.clone()
        } else {
            format!("{}->  {}", " ".repeat(depth), self.label)
        };
        if opts.costs {
            line.push_str(&format!(
                "  (cost={:.2}..{:.2} rows={} width={})",
                self.startup, self.total, self.rows, self.width
            ));
        }
        line.push_str(actual);
        out.push(line);
        let inner = if depth == 0 { 2 } else { depth + 6 };
        for (name, text, _) in &self.details {
            out.push(format!("{}{name}: {text}", " ".repeat(inner)));
        }
        for child in &self.children {
            child.write_text(inner, opts, "", out);
        }
    }

    fn to_json(&self, opts: &ExplainOptions, parent: Option<&str>) -> J {
        let mut map = serde_json::Map::new();
        map.insert("Node Type".into(), J::String(self.kind.clone()));
        if let Some(parent) = parent {
            map.insert("Parent Relationship".into(), J::String(parent.to_string()));
        }
        map.insert("Parallel Aware".into(), J::Bool(false));
        map.insert("Async Capable".into(), J::Bool(false));
        for (name, value) in &self.props {
            map.insert((*name).to_string(), value.clone());
        }
        if opts.costs {
            map.insert("Startup Cost".into(), json!(round2(self.startup)));
            map.insert("Total Cost".into(), json!(round2(self.total)));
            map.insert("Plan Rows".into(), json!(self.rows as i64));
            map.insert("Plan Width".into(), json!(self.width));
        }
        for (name, value) in &self.actual {
            map.insert((*name).to_string(), value.clone());
        }
        map.insert("Disabled".into(), J::Bool(false));
        for (name, _, value) in &self.details {
            map.insert((*name).to_string(), value.clone());
        }
        if !self.children.is_empty() {
            let children = self
                .children
                .iter()
                .enumerate()
                .map(|(i, c)| c.to_json(opts, Some(if i == 0 { "Outer" } else { "Inner" })))
                .collect();
            map.insert("Plans".into(), J::Array(children));
        }
        J::Object(map)
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// PostgreSQL's default planner cost constants.
const CPU_TUPLE: f64 = 0.01;
const CPU_OPERATOR: f64 = 0.0025;
const PAGE: f64 = 1.0;

impl MemExecutor {
    pub(crate) fn exec_explain(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
        opts: ExplainOptions,
    ) -> Result<QueryOutput> {
        let planning = std::time::Instant::now();
        let mut root = self.explain_node(ctx, &plan, &[])?;
        let planning_ms = planning.elapsed().as_secs_f64() * 1000.0;
        let mut execution_ms = None;
        let mut actual = String::new();
        if opts.analyze {
            let started = std::time::Instant::now();
            let out = self.execute_logical_inner(ctx, plan)?;
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            execution_ms = Some(ms);
            // A data-modifying statement without RETURNING returns no rows.
            let rows = out.rows.len() as f64;
            actual = if opts.timing {
                format!(" (actual time=0.000..{ms:.3} rows={rows:.2} loops=1)")
            } else {
                format!(" (actual rows={rows:.2} loops=1)")
            };
            if opts.timing {
                root.actual.push(("Actual Startup Time", json!(0.0)));
                root.actual.push(("Actual Total Time", json!(round3(ms))));
            }
            root.actual.push(("Actual Rows", json!(rows)));
            root.actual.push(("Actual Loops", json!(1)));
        }
        let summary = opts.summary.unwrap_or(opts.analyze);
        if opts.json {
            let mut top = serde_json::Map::new();
            top.insert("Plan".into(), root.to_json(&opts, None));
            if summary {
                top.insert("Planning Time".into(), json!(round3(planning_ms)));
            }
            if let (true, Some(ms)) = (summary, execution_ms) {
                top.insert("Execution Time".into(), json!(round3(ms)));
            }
            let text = serde_json::to_string_pretty(&J::Array(vec![J::Object(top)]))?;
            return Ok(QueryOutput {
                columns: vec!["QUERY PLAN".to_string()],
                types: vec!["JSON".to_string()],
                rows: vec![Row {
                    values: vec![Value::Text(text)],
                }],
                tag: "EXPLAIN".to_string(),
            });
        }
        let mut lines = Vec::new();
        root.write_text(0, &opts, &actual, &mut lines);
        if summary {
            lines.push(format!("Planning Time: {planning_ms:.3} ms"));
        }
        if let (true, Some(ms)) = (summary, execution_ms) {
            lines.push(format!("Execution Time: {ms:.3} ms"));
        }
        Ok(QueryOutput {
            columns: vec!["QUERY PLAN".to_string()],
            types: vec!["TEXT".to_string()],
            rows: lines
                .into_iter()
                .map(|line| Row {
                    values: vec![Value::Text(line)],
                })
                .collect(),
            tag: "EXPLAIN".to_string(),
        })
    }

    /// The plan node for `plan`; `ctes` are the relations its enclosing
    /// queries bind by name.
    fn explain_node(
        &self,
        ctx: &ExecutionContext,
        plan: &LogicalPlan,
        ctes: &[(String, &LogicalPlan)],
    ) -> Result<Node> {
        Ok(match plan {
            LogicalPlan::Select {
                ctes: own,
                table_name,
                table_alias,
                joins,
                projection,
                group_by,
                filter,
                having,
                order_by,
                limit,
                offset,
                distinct,
                sort,
                group_exprs,
                distinct_on,
                ..
            } => {
                let mut scope: Vec<(String, &LogicalPlan)> = ctes.to_vec();
                scope.extend(own.iter().map(|(n, p)| (n.clone(), &**p)));
                let single = joins.is_empty();
                // A filter on one relation is shown on its scan.
                let scan_filter = if single { filter.as_ref() } else { None };
                let mut node = self.explain_relation(
                    ctx,
                    table_name,
                    table_alias.as_deref(),
                    scan_filter,
                    &scope,
                )?;
                for join in joins {
                    let right = match (&join.lateral, &join.table_fn) {
                        (Some(sub), _) => {
                            let child = self.explain_node(ctx, sub, &scope)?;
                            let alias = join.table_alias.as_deref().unwrap_or(&join.table_name);
                            Node::over(
                                "Subquery Scan",
                                format!("Subquery Scan on {}", relation_name(alias)),
                                vec![child],
                            )
                            .prop("Alias", json!(relation_name(alias)))
                        }
                        (None, Some(spec)) => function_scan(spec),
                        (None, None) => self.explain_relation(
                            ctx,
                            &join.table_name,
                            join.table_alias.as_deref(),
                            None,
                            &scope,
                        )?,
                    };
                    let (suffix, kind) = match join.join_type {
                        JoinType::LeftOuter => (" Left Join", "Left"),
                        JoinType::RightOuter => (" Right Join", "Right"),
                        JoinType::FullOuter => (" Full Join", "Full"),
                        JoinType::Inner | JoinType::Cross => ("", "Inner"),
                    };
                    let rows = match (&join.join_type, &join.condition) {
                        (JoinType::Cross, _) | (_, None) if join.using_columns.is_empty() => {
                            node.rows * right.rows
                        }
                        _ => node.rows.max(right.rows),
                    };
                    let width = node.width + right.width;
                    let total = node.total + node.rows * right.total;
                    let mut joined =
                        Node::new("Nested Loop", format!("Nested Loop{suffix}"), rows, width)
                            .prop("Join Type", json!(kind));
                    joined.total = total + rows * CPU_TUPLE;
                    let condition = match &join.condition {
                        Some(c) => Some(deparse_filter(c, true)),
                        None if !join.using_columns.is_empty() => Some(format!(
                            "({})",
                            join.using_columns
                                .iter()
                                .map(|c| format!(
                                    "{}.{c} = {}.{c}",
                                    relation_name(&node.label_relation()),
                                    relation_name(
                                        join.table_alias.as_deref().unwrap_or(&join.table_name)
                                    )
                                ))
                                .collect::<Vec<_>>()
                                .join(" AND ")
                        )),
                        None => None,
                    };
                    if let Some(condition) = condition {
                        joined = joined.detail("Join Filter", condition);
                    }
                    joined.children = vec![node, right];
                    node = joined;
                }
                if let (false, Some(f)) = (single, filter) {
                    node = node.detail("Filter", deparse_filter(f, true));
                    node.rows = (node.rows / 3.0).max(1.0).round();
                }
                let qualified = !single;
                let aggregated = !group_by.is_empty()
                    || having.is_some()
                    || projection.iter().any(|p| match p {
                        ProjectionItem::Aggregate(..) => true,
                        ProjectionItem::Expr { expr, .. } => scalar_has_aggregate(expr),
                        _ => false,
                    });
                if aggregated {
                    let input_rows = node.rows;
                    node = if group_by.is_empty() {
                        let mut agg = Node::over("Aggregate", "Aggregate", vec![node])
                            .prop("Strategy", json!("Plain"));
                        agg.rows = 1.0;
                        agg.startup = agg.total;
                        agg
                    } else {
                        let keys: Vec<String> = group_by
                            .iter()
                            .map(|g| {
                                group_exprs
                                    .iter()
                                    .find(|(name, _)| name == g)
                                    .map(|(_, e)| deparse_scalar(e, qualified))
                                    .unwrap_or_else(|| column(g, qualified))
                            })
                            .collect();
                        let mut agg = Node::over("Aggregate", "HashAggregate", vec![node])
                            .prop("Strategy", json!("Hashed"))
                            .keys("Group Key", keys);
                        agg.rows = (input_rows / 10.0).clamp(1.0, 200.0).round();
                        agg.startup = agg.total;
                        agg
                    };
                    node.width = projection_width(projection);
                    if let Some(h) = having {
                        node = node.detail("Filter", deparse_filter(h, qualified));
                    }
                }
                if projection
                    .iter()
                    .any(|p| matches!(p, ProjectionItem::WindowFunction { .. }))
                {
                    node = Node::over("WindowAgg", "WindowAgg", vec![node]);
                }
                if *distinct {
                    let keys: Vec<String> = projection
                        .iter()
                        .map(|p| projection_text(p, qualified))
                        .collect();
                    node = Node::over("Aggregate", "HashAggregate", vec![node])
                        .prop("Strategy", json!("Hashed"))
                        .keys("Group Key", keys);
                }
                let sort = if sort.is_empty() {
                    order_by.iter().cloned().map(SortKey::from_legacy).collect()
                } else {
                    sort.clone()
                };
                if !sort.is_empty() {
                    let keys: Vec<String> = sort
                        .iter()
                        .map(|k| sort_key_text(k, projection, qualified))
                        .collect();
                    let n = node.rows.max(2.0);
                    let mut sorted = Node::over("Sort", "Sort", vec![node]).keys("Sort Key", keys);
                    sorted.startup = sorted.total + n * n.log2() * 2.0 * CPU_OPERATOR;
                    sorted.total = sorted.startup + n * CPU_TUPLE;
                    node = sorted;
                }
                if !distinct_on.is_empty() {
                    node = Node::over("Unique", "Unique", vec![node]);
                }
                if limit.is_some() || offset.is_some() {
                    let rows = limit.map_or(node.rows, |l| node.rows.min(l as f64));
                    let mut limited = Node::over("Limit", "Limit", vec![node]);
                    limited.rows = rows.max(1.0);
                    node = limited;
                }
                node
            }
            LogicalPlan::SetOp {
                op,
                all,
                left,
                right,
            } => {
                let children = vec![
                    self.explain_node(ctx, left, ctes)?,
                    self.explain_node(ctx, right, ctes)?,
                ];
                match (op, all) {
                    (SetOpKind::Union, true) => Node::over("Append", "Append", children),
                    (SetOpKind::Union, false) => {
                        let append = Node::over("Append", "Append", children);
                        Node::over("Aggregate", "HashAggregate", vec![append])
                            .prop("Strategy", json!("Hashed"))
                    }
                    (SetOpKind::Intersect, _) => {
                        Node::over("SetOp", "HashSetOp Intersect", children)
                            .prop("Command", json!("Intersect"))
                    }
                    (SetOpKind::Except, _) => Node::over("SetOp", "HashSetOp Except", children)
                        .prop("Command", json!("Except")),
                }
            }
            LogicalPlan::Values { rows } => values_scan(rows.len()),
            LogicalPlan::SelectLiteral { filter, .. } => {
                let node = Node::new("Result", "Result", 1.0, 4);
                match filter {
                    Some(f) => node.detail("One-Time Filter", deparse_filter(f, false)),
                    None => node,
                }
            }
            LogicalPlan::TableFunction(spec) => function_scan(spec),
            LogicalPlan::Renamed { input, .. } => self.explain_node(ctx, input, ctes)?,
            LogicalPlan::RecursiveCte {
                seed,
                recursive_term,
                ..
            } => Node::over(
                "Recursive Union",
                "Recursive Union",
                vec![
                    self.explain_node(ctx, seed, ctes)?,
                    self.explain_node(ctx, recursive_term, ctes)?,
                ],
            ),
            LogicalPlan::InlineRows { rows, .. } => values_scan(rows.len()),
            LogicalPlan::Insert {
                table_name,
                values_list,
                source,
                on_conflict,
                ..
            } => {
                let child = match source {
                    Some(query) => self.explain_node(ctx, query, ctes)?,
                    None if values_list.len() > 1 => values_scan(values_list.len()),
                    None => Node::new("Result", "Result", 1.0, 4),
                };
                let mut node = Node::over(
                    "ModifyTable",
                    format!("Insert on {}", relation_name(table_name)),
                    vec![child],
                )
                .prop("Operation", json!("Insert"))
                .prop("Relation Name", json!(relation_name(table_name)));
                node.rows = 0.0;
                node.width = 0;
                if let Some(clause) = on_conflict {
                    let action = match clause {
                        crate::plan_types::OnConflictClause::DoNothing { .. } => "NOTHING",
                        crate::plan_types::OnConflictClause::DoUpdate { .. } => "UPDATE",
                    };
                    node = node.detail("Conflict Resolution", action.to_string());
                }
                node
            }
            LogicalPlan::Update {
                table_name,
                table_alias,
                filter,
                from,
                ..
            } => self.explain_modify(
                ctx,
                "Update",
                (table_name, table_alias.as_deref()),
                from.as_deref(),
                filter.as_ref(),
                ctes,
            )?,
            LogicalPlan::Delete {
                table_name,
                table_alias,
                filter,
                using,
                ..
            } => self.explain_modify(
                ctx,
                "Delete",
                (table_name, table_alias.as_deref()),
                using.as_deref(),
                filter.as_ref(),
                ctes,
            )?,
            LogicalPlan::Merge {
                table_name,
                table_alias,
                source,
                on,
                ..
            } => {
                let source = self.explain_node(ctx, source, ctes)?;
                let target =
                    self.explain_relation(ctx, table_name, table_alias.as_deref(), None, ctes)?;
                let mut join =
                    Node::over("Nested Loop", "Nested Loop Left Join", vec![source, target])
                        .prop("Join Type", json!("Left"));
                if let Some(on) = on {
                    join = join.detail("Join Filter", deparse_filter(on, true));
                }
                let mut node = Node::over(
                    "ModifyTable",
                    format!(
                        "Merge on {}",
                        scan_label_name(table_name, table_alias.as_deref())
                    ),
                    vec![join],
                )
                .prop("Operation", json!("Merge"))
                .prop("Relation Name", json!(relation_name(table_name)));
                node.rows = 0.0;
                node.width = 0;
                node
            }
            LogicalPlan::With { ctes: own, body } => {
                let mut scope: Vec<(String, &LogicalPlan)> = ctes.to_vec();
                scope.extend(own.iter().map(|(n, p)| (n.clone(), &**p)));
                self.explain_node(ctx, body, &scope)?
            }
            LogicalPlan::CreateTableAs { query, .. } => self.explain_node(ctx, query, ctes)?,
            _ => anyhow::bail!("EXPLAIN is not supported for this statement"),
        })
    }

    /// `Update on t` / `Delete on t` over the scan (joined to the relations
    /// the statement reads) that finds the rows.
    fn explain_modify(
        &self,
        ctx: &ExecutionContext,
        operation: &str,
        (table_name, table_alias): (&str, Option<&str>),
        source: Option<&LogicalPlan>,
        filter: Option<&FilterExpr>,
        ctes: &[(String, &LogicalPlan)],
    ) -> Result<Node> {
        let child = match source {
            None => self.explain_relation(ctx, table_name, table_alias, filter, ctes)?,
            Some(source) => {
                let target = self.explain_relation(ctx, table_name, table_alias, None, ctes)?;
                let source = self.explain_node(ctx, source, ctes)?;
                let mut join = Node::over("Nested Loop", "Nested Loop", vec![target, source])
                    .prop("Join Type", json!("Inner"));
                if let Some(f) = filter {
                    join = join.detail("Join Filter", deparse_filter(f, true));
                }
                join
            }
        };
        let mut node = Node::over(
            "ModifyTable",
            format!(
                "{operation} on {}",
                scan_label_name(table_name, table_alias)
            ),
            vec![child],
        )
        .prop("Operation", json!(operation))
        .prop("Relation Name", json!(relation_name(table_name)));
        node.rows = 0.0;
        node.width = 0;
        Ok(node)
    }

    /// The scan of one relation: a table (by index when the filter is an
    /// equality on an indexed column), a CTE or derived table, a view's
    /// query, or a catalog table.
    fn explain_relation(
        &self,
        ctx: &ExecutionContext,
        table_name: &str,
        table_alias: Option<&str>,
        filter: Option<&FilterExpr>,
        ctes: &[(String, &LogicalPlan)],
    ) -> Result<Node> {
        let with_filter = |mut node: Node| {
            if let Some(f) = filter {
                node = node.detail("Filter", deparse_filter(f, false));
                node.total += node.rows * CPU_OPERATOR;
                node.rows = (node.rows * selectivity(f)).max(1.0).round();
            }
            node
        };
        if let Some((name, plan)) = ctes.iter().rev().find(|(name, _)| name == table_name) {
            let child = self.explain_node(ctx, plan, ctes)?;
            // The wrapper a query over a set operation or VALUES list gets.
            if name == "\u{0}result" {
                return Ok(with_filter(child));
            }
            if let LogicalPlan::TableFunction(spec) = plan {
                return Ok(with_filter(function_scan(spec)));
            }
            let alias = relation_name(table_alias.unwrap_or(name));
            let node = Node::over(
                "Subquery Scan",
                format!("Subquery Scan on {alias}"),
                vec![child],
            )
            .prop("Alias", json!(alias));
            return Ok(with_filter(node));
        }
        let (db_name, schema_name, table_only) = parse_object_name(table_name)?;
        let virtual_schema = Self::is_virtual_schema(schema_name)
            || (schema_name.eq_ignore_ascii_case("public")
                && Self::is_pg_catalog_virtual_table_name(table_only));
        let label = scan_label_name(table_name, table_alias);
        if virtual_schema {
            let node = Node::new("Seq Scan", format!("Seq Scan on {label}"), 100.0, 64)
                .prop("Relation Name", json!(table_only))
                .prop("Alias", json!(table_alias.unwrap_or(table_only)));
            return Ok(with_filter(node));
        }
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Select, ResourceRef::Table(tbl.id))?;
        if let Some(vq) = &tbl.view_query {
            let plan: LogicalPlan = serde_json::from_str(vq)?;
            let child = crate::cte_scope::isolated(|| self.explain_node(ctx, &plan, &[]))?;
            let alias = relation_name(table_alias.unwrap_or(table_only));
            let node = Node::over(
                "Subquery Scan",
                format!("Subquery Scan on {alias}"),
                vec![child],
            )
            .prop("Alias", json!(alias));
            return Ok(with_filter(node));
        }
        let rows = self.scan_rows(tbl.id, &ctx.session_id)?.len() as f64;
        let width: u32 = tbl.columns.iter().map(|c| type_width(&c.data_type)).sum();
        let pages = (rows * width as f64 / 8192.0).ceil().max(1.0);
        // The executor looks up `col = value` on an indexed column by index.
        if let Some(FilterExpr::Predicate(Predicate {
            left,
            op: CompareOp::Eq,
            right: Operand::Literal(_),
        })) = filter
        {
            let name = left.rsplit('.').next().unwrap_or(left);
            if let Some(col) = tbl.columns.iter().find(|c| c.name == name)
                && let Some(idx) = tbl
                    .indexes
                    .iter()
                    .find(|i| i.key_columns.iter().any(|k| k.column_id == col.id))
            {
                let mut node = Node::new(
                    "Index Scan",
                    format!("Index Scan using {} on {label}", idx.name),
                    if idx.unique {
                        1.0
                    } else {
                        (rows * 0.005).max(1.0)
                    },
                    width,
                )
                .prop("Scan Direction", json!("Forward"))
                .prop("Index Name", json!(idx.name))
                .prop("Relation Name", json!(table_only))
                .prop("Alias", json!(table_alias.unwrap_or(table_only)))
                .detail(
                    "Index Cond",
                    deparse_filter(filter.expect("matched"), false),
                );
                node.startup = 0.15;
                node.total = 8.17;
                return Ok(node);
            }
        }
        let mut node = Node::new("Seq Scan", format!("Seq Scan on {label}"), rows, width)
            .prop("Relation Name", json!(table_only))
            .prop("Alias", json!(table_alias.unwrap_or(table_only)));
        node.total = pages * PAGE + rows * CPU_TUPLE;
        Ok(with_filter(node))
    }
}

impl Node {
    /// The relation a scan headline names (`Seq Scan on t a` -> `a`).
    fn label_relation(&self) -> String {
        self.label
            .rsplit(' ')
            .next()
            .unwrap_or(&self.label)
            .to_string()
    }
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

/// A relation's name without its schema, and the name PostgreSQL gives an
/// unaliased subquery.
fn relation_name(name: &str) -> String {
    if name.starts_with('\u{0}') {
        return "unnamed_subquery".to_string();
    }
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('"')
        .to_string()
}

/// `t` or `t a`, as a scan headline names a relation.
fn scan_label_name(table_name: &str, alias: Option<&str>) -> String {
    let name = relation_name(table_name);
    match alias {
        Some(alias) if alias != name => format!("{name} {alias}"),
        _ => name,
    }
}

fn function_scan(spec: &TableFnSpec) -> Node {
    let alias = spec.alias.clone().unwrap_or_else(|| spec.name.clone());
    let label = if alias == spec.name {
        format!("Function Scan on {}", spec.name)
    } else {
        format!("Function Scan on {} {alias}", spec.name)
    };
    let mut node = Node::new("Function Scan", label, 1000.0, 4)
        .prop("Function Name", json!(spec.name))
        .prop("Alias", json!(alias));
    node.total = 10.0;
    node
}

fn values_scan(rows: usize) -> Node {
    let mut node = Node::new(
        "Values Scan",
        "Values Scan on \"*VALUES*\"",
        rows as f64,
        32,
    )
    .prop("Alias", json!("*VALUES*"));
    node.total = rows as f64 * CPU_TUPLE;
    node
}

/// The planner's default fraction of rows a condition keeps.
fn selectivity(filter: &FilterExpr) -> f64 {
    match filter {
        FilterExpr::Predicate(Predicate {
            op: CompareOp::Eq, ..
        }) => 0.005,
        FilterExpr::IsNull(_) => 0.005,
        FilterExpr::And(a, b) => selectivity(a) * selectivity(b),
        _ => 0.333,
    }
}

/// The average stored width of a value of a declared type, in bytes.
fn type_width(data_type: &str) -> u32 {
    let t = data_type.trim().to_ascii_uppercase();
    if t.ends_with("[]") {
        32
    } else if t.contains("BIGINT") || t == "INT8" || t.contains("BIGSERIAL") {
        8
    } else if t.contains("SMALLINT") || t == "INT2" {
        2
    } else if t.contains("INT") || t.contains("SERIAL") || t == "DATE" || t == "REAL" {
        4
    } else if t.contains("DOUBLE") || t.contains("FLOAT") || t.starts_with("TIMESTAMP") {
        8
    } else if t.starts_with("BOOL") {
        1
    } else if t == "UUID" {
        16
    } else {
        32
    }
}

fn projection_width(projection: &[ProjectionItem]) -> u32 {
    (projection.len().max(1) * 8) as u32
}

/// A column reference as a plan shows it: without its qualifier when the
/// query reads one relation.
fn column(name: &str, qualified: bool) -> String {
    if qualified {
        name.to_string()
    } else {
        name.rsplit('.').next().unwrap_or(name).to_string()
    }
}

fn projection_text(item: &ProjectionItem, qualified: bool) -> String {
    match item {
        ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => column(c, qualified),
        ProjectionItem::Aggregate(op, arg) => {
            let arg = if arg.is_empty() || arg == "*" {
                "*".to_string()
            } else {
                column(arg, qualified)
            };
            format!("{}({arg})", op.sql_name())
        }
        ProjectionItem::Expr { expr, .. } => deparse_scalar(expr, qualified),
        _ => "?column?".to_string(),
    }
}

fn sort_key_text(key: &SortKey, projection: &[ProjectionItem], qualified: bool) -> String {
    let mut text = match &key.target {
        SortTarget::Output(i) => match projection.get(*i) {
            Some(item @ (ProjectionItem::Column(_) | ProjectionItem::AliasedColumn(..))) => {
                projection_text(item, qualified)
            }
            Some(item) => format!("({})", projection_text(item, qualified)),
            None => format!("{}", i + 1),
        },
        SortTarget::Name(n) => column(n, qualified),
        SortTarget::Expr(e) => format!("({})", deparse_scalar(e, qualified)),
    };
    if !key.ascending {
        text.push_str(" DESC");
    }
    match (key.nulls_first, key.ascending) {
        (Some(true), true) => text.push_str(" NULLS FIRST"),
        (Some(false), false) => text.push_str(" NULLS LAST"),
        _ => {}
    }
    text
}

fn literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Text(s) => format!("'{}'::text", s.replace('\'', "''")),
        Value::Bool(b) => b.to_string(),
        Value::Jsonb(j) => format!(
            "'{}'::jsonb",
            crate::json_text::jsonb_text(j).replace('\'', "''")
        ),
        Value::Array(_) => format!("'{}'", render(value).replace('\'', "''")),
        other => render(other),
    }
}

fn compare_op(op: &CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=",
        CompareOp::Ne => "<>",
        CompareOp::Lt => "<",
        CompareOp::Le => "<=",
        CompareOp::Gt => ">",
        CompareOp::Ge => ">=",
        CompareOp::Contains => "@>",
        CompareOp::ContainedBy => "<@",
    }
}

fn binary_op(op: &ScalarBinaryOp) -> &'static str {
    use ScalarBinaryOp as B;
    match op {
        B::Add => "+",
        B::Sub => "-",
        B::Mul => "*",
        B::Div => "/",
        B::Mod => "%",
        B::Eq => "=",
        B::NotEq => "<>",
        B::Lt => "<",
        B::LtEq => "<=",
        B::Gt => ">",
        B::GtEq => ">=",
        B::And => "AND",
        B::Or => "OR",
        B::Concat => "||",
        B::JsonGet => "->",
        B::JsonGetText => "->>",
        B::JsonPath => "#>",
        B::JsonPathText => "#>>",
        B::JsonHasKey => "?",
        B::JsonHasAnyKey => "?|",
        B::JsonHasAllKeys => "?&",
        B::Contains => "@>",
        B::ContainedBy => "<@",
        B::Overlap => "&&",
    }
}

fn operand(op: &Operand, qualified: bool) -> String {
    match op {
        Operand::Literal(v) => literal(v),
        Operand::Ident(c) => column(c, qualified),
    }
}

/// A condition as PostgreSQL prints it in a plan: `((a > 1) AND (b = 'x'::text))`.
pub(crate) fn deparse_filter(filter: &FilterExpr, qualified: bool) -> String {
    let f = |e: &FilterExpr| deparse_filter(e, qualified);
    match filter {
        FilterExpr::Predicate(p) => format!(
            "({} {} {})",
            column(&p.left, qualified),
            compare_op(&p.op),
            operand(&p.right, qualified)
        ),
        FilterExpr::And(..) | FilterExpr::Or(..) => {
            let is_and = matches!(filter, FilterExpr::And(..));
            let mut parts = Vec::new();
            flatten(filter, is_and, &mut parts);
            let joiner = if is_and { " AND " } else { " OR " };
            format!(
                "({})",
                parts.iter().map(|p| f(p)).collect::<Vec<_>>().join(joiner)
            )
        }
        FilterExpr::Not(inner) => format!("(NOT {})", f(inner)),
        FilterExpr::IsNull(c) => format!("({} IS NULL)", column(c, qualified)),
        FilterExpr::IsNotNull(c) => format!("({} IS NOT NULL)", column(c, qualified)),
        FilterExpr::InList {
            left,
            list,
            negated,
        } => format!(
            "({} {} ({}))",
            column(left, qualified),
            if *negated { "NOT IN" } else { "IN" },
            list.iter()
                .map(|o| operand(o, qualified))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        FilterExpr::InSubquery { left, negated, .. } => format!(
            "({}{} IN (SubPlan 1))",
            if *negated { "NOT " } else { "" },
            column(left, qualified)
        ),
        FilterExpr::CompareSubquery { left, op, .. } => {
            format!(
                "({} {} (SubPlan 1))",
                column(left, qualified),
                compare_op(op)
            )
        }
        FilterExpr::ExprCmp { left, op, right } => format!(
            "({} {} {})",
            deparse_scalar(left, qualified),
            compare_op(op),
            deparse_scalar(right, qualified)
        ),
        FilterExpr::Exists { negated, .. } => {
            if *negated {
                "(NOT EXISTS(SubPlan 1))".to_string()
            } else {
                "EXISTS(SubPlan 1)".to_string()
            }
        }
        FilterExpr::Scalar(e) => {
            let text = deparse_scalar(e, qualified);
            if text.starts_with('(') {
                text
            } else {
                format!("({text})")
            }
        }
        FilterExpr::QuantifiedSubquery { left, op, all, .. } => format!(
            "({} {} {} (SubPlan 1))",
            deparse_scalar(left, qualified),
            binary_op(op),
            if *all { "ALL" } else { "ANY" }
        ),
    }
}

fn flatten<'a>(filter: &'a FilterExpr, and: bool, out: &mut Vec<&'a FilterExpr>) {
    match filter {
        FilterExpr::And(a, b) if and => {
            flatten(a, and, out);
            flatten(b, and, out);
        }
        FilterExpr::Or(a, b) if !and => {
            flatten(a, and, out);
            flatten(b, and, out);
        }
        other => out.push(other),
    }
}

/// An expression as PostgreSQL prints it in a plan.
pub(crate) fn deparse_scalar(expr: &ScalarExpr, qualified: bool) -> String {
    let d = |e: &ScalarExpr| deparse_scalar(e, qualified);
    let list = |items: &[ScalarExpr]| items.iter().map(d).collect::<Vec<_>>().join(", ");
    match expr {
        ScalarExpr::Literal(v) => literal(v),
        ScalarExpr::Column(c) => column(c, qualified),
        ScalarExpr::Unary { op, expr } => match op {
            ScalarUnaryOp::Neg => format!("(- {})", d(expr)),
            ScalarUnaryOp::Not => format!("(NOT {})", d(expr)),
        },
        ScalarExpr::Binary { op, left, right } => {
            format!("({} {} {})", d(left), binary_op(op), d(right))
        }
        ScalarExpr::Cast { expr, target } => {
            format!("({})::{}", d(expr), target.to_ascii_lowercase())
        }
        // A range check shows as the expression it checks.
        ScalarExpr::Function { name, args }
            if name == crate::result_types::INTEGER_RANGE && !args.is_empty() =>
        {
            d(&args[0])
        }
        // A sequence argument is a `regclass`.
        ScalarExpr::Function { name, args } if name == "NEXTVAL" && args.len() == 1 => {
            match crate::sequences::default_sequence(expr) {
                Some(sequence) => format!("nextval('{sequence}'::regclass)"),
                None => format!("nextval({})", list(args)),
            }
        }
        // SQL value functions are written without parentheses.
        ScalarExpr::Function { name, args }
            if args.is_empty()
                && matches!(
                    name.as_str(),
                    "CURRENT_TIMESTAMP"
                        | "CURRENT_DATE"
                        | "CURRENT_TIME"
                        | "LOCALTIMESTAMP"
                        | "LOCALTIME"
                        | "CURRENT_USER"
                        | "SESSION_USER"
                        | "CURRENT_ROLE"
                        | "CURRENT_CATALOG"
                        | "CURRENT_SCHEMA"
                ) =>
        {
            name.clone()
        }
        ScalarExpr::Function { name, args } => {
            format!("{}({})", name.to_ascii_lowercase(), list(args))
        }
        ScalarExpr::IsNull { expr, negated } => format!(
            "({} IS {}NULL)",
            d(expr),
            if *negated { "NOT " } else { "" }
        ),
        ScalarExpr::Extract { field, expr } => {
            format!("EXTRACT({} FROM {})", field.to_ascii_lowercase(), d(expr))
        }
        ScalarExpr::Aggregate {
            op,
            arg,
            arg_expr,
            distinct,
            ..
        } => {
            let arg = match arg_expr {
                Some(e) => d(e),
                None if arg.is_empty() || arg == "*" => "*".to_string(),
                None => column(arg, qualified),
            };
            let distinct = if *distinct { "DISTINCT " } else { "" };
            format!("{}({distinct}{arg})", op.sql_name())
        }
        ScalarExpr::DateOffset {
            base,
            months,
            days,
            seconds,
        } => format!(
            "({} + '{months} mons {days} days {seconds} secs'::interval)",
            d(base)
        ),
        ScalarExpr::Case {
            operand,
            branches,
            else_result,
        } => {
            let mut text = "CASE".to_string();
            if let Some(o) = operand {
                text.push_str(&format!(" {}", d(o)));
            }
            for (when, then) in branches {
                text.push_str(&format!(" WHEN {} THEN {}", d(when), d(then)));
            }
            if let Some(e) = else_result {
                text.push_str(&format!(" ELSE {}", d(e)));
            }
            text.push_str(" END");
            text
        }
        ScalarExpr::PatternMatch {
            expr,
            pattern,
            kind,
            case_insensitive,
            negated,
            ..
        } => {
            let op = match (kind, case_insensitive, negated) {
                (PatternKind::Like, false, false) => "~~",
                (PatternKind::Like, false, true) => "!~~",
                (PatternKind::Like, true, false) => "~~*",
                (PatternKind::Like, true, true) => "!~~*",
                (PatternKind::Regex, false, false) => "~",
                (PatternKind::Regex, false, true) => "!~",
                (PatternKind::Regex, true, false) => "~*",
                (PatternKind::Regex, true, true) => "!~*",
                (PatternKind::SimilarTo, _, false) => "SIMILAR TO",
                (PatternKind::SimilarTo, _, true) => "NOT SIMILAR TO",
            };
            format!("({} {op} {})", d(expr), d(pattern))
        }
        ScalarExpr::IsDistinctFrom {
            left,
            right,
            negated,
        } => format!(
            "({} IS {}DISTINCT FROM {})",
            d(left),
            if *negated { "NOT " } else { "" },
            d(right)
        ),
        ScalarExpr::IsBool {
            expr,
            value,
            negated,
        } => format!(
            "({} IS {}{})",
            d(expr),
            if *negated { "NOT " } else { "" },
            match value {
                Some(true) => "TRUE",
                Some(false) => "FALSE",
                None => "UNKNOWN",
            }
        ),
        ScalarExpr::InList {
            expr,
            list: items,
            negated,
        } => format!(
            "({} {} (ARRAY[{}]))",
            d(expr),
            if *negated { "<> ALL" } else { "= ANY" },
            list(items)
        ),
        ScalarExpr::Quantified {
            left,
            op,
            right,
            all,
        } => format!(
            "({} {} {} ({}))",
            d(left),
            binary_op(op),
            if *all { "ALL" } else { "ANY" },
            d(right)
        ),
        ScalarExpr::Row(items) => format!("ROW({})", list(items)),
        ScalarExpr::Subquery { kind, .. } => match kind {
            SubqueryKind::Exists => "EXISTS(SubPlan 1)".to_string(),
            SubqueryKind::Array => "ARRAY(SubPlan 1)".to_string(),
            SubqueryKind::Scalar => "(SubPlan 1)".to_string(),
        },
    }
}

/// A query as PostgreSQL pretty-prints a view's definition:
/// ` SELECT a,\n    b\n   FROM t\n  WHERE a > 0`. `None` for a plan it
/// cannot write back as SQL.
pub(crate) fn deparse_query(plan: &LogicalPlan) -> Option<String> {
    let LogicalPlan::Select {
        table_name,
        table_alias,
        joins,
        projection,
        group_by,
        filter,
        having,
        sort,
        limit,
        offset,
        distinct,
        ..
    } = plan
    else {
        return None;
    };
    let qualified = !joins.is_empty();
    let unwrap = |text: String| match text.strip_prefix('(').and_then(|t| t.strip_suffix(')')) {
        Some(inner) if balanced(inner) => inner.to_string(),
        _ => text,
    };
    let items: Vec<String> = if projection.is_empty() {
        vec!["*".to_string()]
    } else {
        projection
            .iter()
            .map(|item| match item {
                ProjectionItem::AliasedColumn(c, alias) => {
                    format!("{} AS {alias}", column(c, qualified))
                }
                ProjectionItem::Expr {
                    expr,
                    alias: Some(alias),
                } => format!("{} AS {alias}", unwrap(deparse_scalar(expr, qualified))),
                other => unwrap(projection_text(other, qualified)),
            })
            .collect()
    };
    let mut sql = format!(
        " SELECT {}{}",
        if *distinct { "DISTINCT " } else { "" },
        items.join(",\n    ")
    );
    sql.push_str(&format!(
        "\n   FROM {}",
        scan_label_name_qualified(table_name, table_alias.as_deref())
    ));
    for join in joins {
        let kind = match join.join_type {
            JoinType::Inner => "JOIN",
            JoinType::LeftOuter => "LEFT JOIN",
            JoinType::RightOuter => "RIGHT JOIN",
            JoinType::FullOuter => "FULL JOIN",
            JoinType::Cross => "CROSS JOIN",
        };
        sql.push_str(&format!(
            "\n     {kind} {}",
            scan_label_name_qualified(&join.table_name, join.table_alias.as_deref())
        ));
        if let Some(condition) = &join.condition {
            sql.push_str(&format!(" ON {}", deparse_filter(condition, true)));
        }
    }
    if let Some(filter) = filter {
        sql.push_str(&format!(
            "\n  WHERE {}",
            unwrap(deparse_filter(filter, qualified))
        ));
    }
    if !group_by.is_empty() {
        let keys: Vec<String> = group_by.iter().map(|g| column(g, qualified)).collect();
        sql.push_str(&format!("\n  GROUP BY {}", keys.join(", ")));
    }
    if let Some(having) = having {
        sql.push_str(&format!(
            "\n HAVING {}",
            unwrap(deparse_filter(having, qualified))
        ));
    }
    if !sort.is_empty() {
        let keys: Vec<String> = sort
            .iter()
            .map(|k| sort_key_text(k, projection, qualified))
            .collect();
        sql.push_str(&format!("\n  ORDER BY {}", keys.join(", ")));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!("\n OFFSET {offset}"));
    }
    if let Some(limit) = limit {
        sql.push_str(&format!("\n LIMIT {limit}"));
    }
    Some(sql)
}

/// Whether `text` has balanced parentheses outside quotes.
fn balanced(text: &str) -> bool {
    let mut depth = 0i32;
    let mut quoted = false;
    for c in text.chars() {
        match c {
            '\'' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

/// `schema.t` or `schema.t a`, as a query names a relation.
fn scan_label_name_qualified(table_name: &str, alias: Option<&str>) -> String {
    match alias {
        Some(alias) if alias != relation_name(table_name) => format!("{table_name} {alias}"),
        _ => table_name.to_string(),
    }
}
