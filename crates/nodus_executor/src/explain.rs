//! `EXPLAIN`: how a statement runs, described in PostgreSQL's plan
//! vocabulary (scans, nested-loop joins, aggregation, sorting, limits) with
//! rough cost and row estimates, as text or JSON. `ANALYZE` runs the
//! statement and reports its actual rows and time.

use crate::plan_types::{FrameBound, FrameExclude, FrameSpec, WindowCall, WindowFrameUnits};
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

/// A pruned-away partition: a scan that reads nothing.
fn empty_scan() -> Node {
    let mut node =
        Node::new("Result", "Result", 1.0, 0).detail("One-Time Filter", "false".to_string());
    node.rows = 0.0;
    node.width = 0;
    node.startup = 0.0;
    node.total = 0.0;
    node
}

/// The lone partition that survived pruning, under the query's own name for
/// the parent.
fn single_scan(
    mut children: Vec<Node>,
    table: &nodus_catalog::TableDescriptor,
    parent_alias: &str,
) -> Node {
    let mut only = children.remove(0);
    only.label = format!("Seq Scan on {} {parent_alias}", relation_name(&table.name));
    only.props.retain(|(name, _)| *name != "Alias");
    // The scan reads the partition under the parent's name.
    only = only.prop("Alias", json!(parent_alias));
    only
}

/// An `Append` over the scans of a partitioned or inherited table.
fn append_scan(parent: &nodus_catalog::TableDescriptor, children: Vec<Node>) -> Node {
    let child_rows: f64 = children.iter().map(|c| c.rows).sum();
    let child_pages: f64 = children
        .iter()
        .map(|c| (c.rows * c.width as f64 / 8192.0).ceil().max(1.0))
        .sum();
    let mut node = Node::over("Append", "Append", children).prop("Subplans Removed", json!(0));
    node.rows = child_rows;
    node.width = width_of(parent);
    node.startup = 0.0;
    node.total = child_pages * PAGE + child_rows * CPU_TUPLE;
    node
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
    /// Lines under the headline that are not `name: value` (PostgreSQL's
    /// `Update on partition alias` result relations).
    notes: Vec<String>,
    rows: f64,
    width: u32,
    startup: f64,
    total: f64,
    /// `ANALYZE`'s measurements, as JSON properties.
    actual: Vec<(&'static str, J)>,
    children: Vec<Node>,
    /// The node's subplans (`InitPlan N`, `SubPlan N`): each is a tree
    /// printed under the details in text format, and a child with an
    /// `InitPlan`/`SubPlan` relationship and a `Subplan Name` in JSON.
    subplans: Vec<Node>,
}

impl Node {
    fn new(kind: &str, label: impl Into<String>, rows: f64, width: u32) -> Node {
        Node {
            kind: kind.to_string(),
            label: label.into(),
            props: Vec::new(),
            details: Vec::new(),
            notes: Vec::new(),
            rows: rows.max(1.0).round(),
            width,
            startup: 0.0,
            total: 0.0,
            actual: Vec::new(),
            children: Vec::new(),
            subplans: Vec::new(),
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

    /// A line under the headline, as written.
    fn note(mut self, line: impl Into<String>) -> Node {
        self.notes.push(line.into());
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
        for note in &self.notes {
            out.push(format!("{}{note}", " ".repeat(inner)));
        }
        // A subplan's tree hangs off its `InitPlan N` line, one indent in.
        for subplan in &self.subplans {
            let mut line = format!("{}{}", " ".repeat(inner), subplan.label);
            if opts.costs {
                line.push_str(&format!(
                    "  (cost={:.2}..{:.2} rows={} width={})",
                    subplan.startup, subplan.total, subplan.rows, subplan.width
                ));
            }
            out.push(line);
            for child in &subplan.children {
                child.write_text(inner + 2, opts, "", out);
            }
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
        // Join nodes name their inner side's uniqueness, as PostgreSQL does.
        if self.props.iter().any(|(name, _)| *name == "Join Type") {
            map.insert("Inner Unique".into(), J::Bool(false));
        }
        for (name, _, value) in &self.details {
            if *name == "Disabled" {
                map.insert((*name).to_string(), J::Bool(true));
            } else {
                map.insert((*name).to_string(), value.clone());
            }
        }
        let mut plans: Vec<J> = Vec::new();
        // `Append` (and the bitmap combines) list their inputs as members.
        let member_children = matches!(
            self.kind.as_str(),
            "Append" | "Merge Append" | "BitmapAnd" | "BitmapOr"
        );
        // A subplan's wrapper names it; PostgreSQL puts the name on the
        // subplan's own root node, before the node's children.
        for subplan in &self.subplans {
            let Some(root) = subplan.children.first() else {
                continue;
            };
            let relationship = if subplan.kind.starts_with("SubPlan") {
                "SubPlan"
            } else {
                "InitPlan"
            };
            let mut entry = root.to_json(opts, Some(relationship));
            if let J::Object(entry) = &mut entry {
                entry.insert("Subplan Name".into(), J::String(subplan.label.clone()));
            }
            plans.push(entry);
        }
        plans.extend(self.children.iter().enumerate().map(|(i, c)| {
            let relationship = if member_children {
                "Member"
            } else if i == 0 {
                "Outer"
            } else {
                "Inner"
            };
            c.to_json(opts, Some(relationship))
        }));
        if !plans.is_empty() {
            map.insert("Plans".into(), J::Array(plans));
        }
        J::Object(map)
    }
}

/// A hashed key as PostgreSQL prints it: a bare column, or the computed
/// expression wrapped the way a condition's operand is.
fn hash_key_text(key: &crate::joins::HashKey, qualified: bool) -> String {
    match key {
        crate::joins::HashKey::Column(name) => column(name, qualified),
        crate::joins::HashKey::Expr(expr) => deparse_scalar(expr, qualified),
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Reprints a correlated subplan's scan condition with its columns
/// qualified: the condition may name the enclosing query's columns, which
/// the subplan's own scan cannot tell apart unqualified.
fn requalify_subplan_filter(mut node: Node, plan: &LogicalPlan, subplans: &Subplans) -> Node {
    let LogicalPlan::Select { joins, filter, .. } = plan else {
        return node;
    };
    if !joins.is_empty() {
        return node;
    }
    let Some(filter) = filter else {
        return node;
    };
    fn walk(node: &mut Node, filter: &FilterExpr, subplans: &Subplans) {
        if matches!(
            node.kind.as_str(),
            "Seq Scan" | "Index Scan" | "Index Only Scan"
        ) && let Some(detail) = node
            .details
            .iter_mut()
            .find(|(name, _, _)| *name == "Filter")
        {
            let text = deparse_filter_with(filter, true, Some(subplans));
            detail.1 = text.clone();
            detail.2 = serde_json::json!(text);
        }
        for child in &mut node.children {
            walk(child, filter, subplans);
        }
    }
    walk(&mut node, filter, subplans);
    // Only the outer references name another query: the subquery's own
    // relations print bare, as they do in a scan of their own.
    let labels = crate::joins::relation_labels(plan);
    strip_labels(&mut node, &labels);
    node
}

/// Removes `label.` (outside quotes) from a node's details.
fn strip_labels(node: &mut Node, labels: &[String]) {
    for (_, text, json) in &mut node.details {
        let stripped = strip_qualified(text, labels);
        *text = stripped.clone();
        *json = serde_json::json!(stripped);
    }
    for child in &mut node.children {
        strip_labels(child, labels);
    }
}

/// `(u2.a = u1.a)` names the subquery's own column bare: `(a = u1.a)`.
fn strip_qualified(text: &str, labels: &[String]) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut quoted = false;
    while let Some(c) = chars.next() {
        if c == '\'' {
            quoted = !quoted;
            out.push(c);
            continue;
        }
        if !quoted {
            let ahead: String = std::iter::once(c).chain(chars.clone()).collect();
            let word_before = out.chars().last();
            if let Some(label) = labels.iter().find(|label| {
                ahead.starts_with(&format!("{label}."))
                    && word_before.is_none_or(|prev| !prev.is_alphanumeric() && prev != '_')
            }) {
                // `c` is the label's first character.
                for _ in 1..label.chars().count() {
                    chars.next();
                }
                chars.next();
                continue;
            }
        }
        out.push(c);
    }
    out
}

/// A collected subquery as a node: its `InitPlan N` / `SubPlan N` line over
/// the subquery's own plan tree.
fn subplan_node(entry: &SubplanEntry, tree: Node, correlated: bool) -> Node {
    // An uncorrelated `SubPlan` is read again for every outer row, so its
    // rows are materialized; a correlated one runs with each row's values,
    // an `InitPlan` is read once, and a hashed `SubPlan` keeps its values
    // in the hash table.
    let mut tree = tree;
    if !entry.name.starts_with("InitPlan")
        && !entry.hashed
        && !correlated
        && crate::session_env::setting("enable_material").as_deref() != Some("off")
        && !matches!(tree.kind.as_str(), "Index Scan" | "Index Only Scan")
    {
        tree = Node::over("Materialize", "Materialize", vec![tree]);
    }
    let mut wrapper = Node::new(&entry.name, entry.name.clone(), tree.rows, tree.width);
    wrapper.startup = tree.startup;
    wrapper.total = tree.total;
    wrapper.children = vec![tree];
    wrapper
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
                cte_hints,
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
                sample,
                only,
                ..
            } => {
                let mut scope: Vec<(String, &LogicalPlan)> = ctes.to_vec();
                scope.extend(own.iter().map(|(n, p)| (n.clone(), &**p)));
                let single = joins.is_empty();
                // An uncorrelated `EXISTS` is evaluated once, as the
                // `Result` node's one-time filter, out of the condition.
                let (hoisted_exists, filter_rest) = split_uncorrelated_exists(filter.as_ref());
                let filter = filter_rest.as_ref();
                // A CTE the query reads once, side-effect-free and
                // non-recursive — and that PostgreSQL can pull up — is
                // inlined into it, as PostgreSQL 12+ plans them; the rest
                // materialize once, and a `CTE Scan` reads them.
                let mut inlined: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                let mut labels: std::collections::HashMap<String, String> =
                    std::collections::HashMap::new();
                // Inside a CTE's body its own relations' columns print
                // qualified, as PostgreSQL's plan texts show them.
                if rendering_cte_body() {
                    let mut qualify = |prefix: &str| {
                        if let Ok((db, schema, table)) = parse_object_name(prefix)
                            && let Ok(tbl) = self.catalog_reader.get_table(db, schema, table)
                        {
                            for column in &tbl.columns {
                                labels
                                    .entry(column.name.clone())
                                    .or_insert(format!("{prefix}.{}", column.name));
                            }
                        }
                    };
                    qualify(table_alias.as_deref().unwrap_or(table_name));
                    for join in joins {
                        if join.lateral.is_none()
                            && join.semi_subquery.is_none()
                            && join.table_fn.is_none()
                        {
                            qualify(join.table_alias.as_deref().unwrap_or(&join.table_name));
                        }
                    }
                }
                let mut materialized: Vec<(String, &LogicalPlan)> = Vec::new();
                for (name, plan) in own {
                    let body = &**plan;
                    // A set operation's wrapper and a table function are
                    // not user CTEs: they render in place either way.
                    if name.starts_with('\u{0}') || matches!(body, LogicalPlan::TableFunction(_)) {
                        continue;
                    }
                    let mut references = (table_name.as_str() == name.as_str()) as usize
                        + joins
                            .iter()
                            .filter(|join| join.table_name.as_str() == name.as_str())
                            .count();
                    for join in joins {
                        if let Some(lateral) = &join.lateral {
                            references += plan_cte_references(lateral, name);
                        }
                    }
                    for (other, plan) in own {
                        if other != name {
                            references += plan_cte_references(plan, name);
                        }
                    }
                    let mut refs_subplans = Subplans::default();
                    collect_subplans(projection, &[], filter, having.as_ref(), &mut refs_subplans);
                    for (plan, _) in refs_subplans.collected() {
                        references += plan_cte_references(plan, name);
                    }
                    if references == 0 {
                        continue;
                    }
                    let hinted = cte_hints
                        .iter()
                        .find(|(hinted, _)| hinted == name)
                        .map(|(_, materialized)| *materialized);
                    if cte_materialized(body, name, references, hinted) || !cte_pullable(body) {
                        materialized.push((name.clone(), body));
                        continue;
                    }
                    inlined.insert(name.clone());
                    // Each reference to the CTE prints the body's columns.
                    let mut add_labels = |site: &str| {
                        for (output, target) in cte_outputs(body) {
                            labels.insert(format!("{site}.{output}"), target);
                        }
                    };
                    if table_name.as_str() == name.as_str() {
                        add_labels(table_alias.as_deref().unwrap_or(table_name));
                    }
                    for join in joins {
                        if join.table_name.as_str() == name.as_str() {
                            add_labels(join.table_alias.as_deref().unwrap_or(&join.table_name));
                        }
                    }
                }
                let shadowed: Vec<String> = own.iter().map(|(name, _)| name.clone()).collect();
                let _cte_scope = CteScopeGuard::extend(labels, inlined, &shadowed);

                // The plan's subqueries, numbered for the filter text.
                // A materialized CTE's block counts as one too.
                let mut subplans = Subplans::default();
                for (_, plan) in &materialized {
                    subplans.number(plan, true, false);
                }
                collect_subplans(
                    projection,
                    &hoisted_exists,
                    filter,
                    having.as_ref(),
                    &mut subplans,
                );
                // The relations on the left of the join being rendered,
                // which decide whose column references are whose.
                let mut joined_prefixes: Vec<String> =
                    vec![relation_name(table_alias.as_deref().unwrap_or(table_name))];
                // A `WHERE` conjunct one relation decides is shown at that
                // relation's scan, as PostgreSQL plans it; a null-rejecting
                // one on an outer join's null side first turns it into an
                // inner join.
                let inputs: Vec<Vec<String>> = std::iter::once(joined_prefixes.clone())
                    .chain(joins.iter().map(|join| match &join.semi_subquery {
                        Some(sub) => crate::joins::relation_quals(sub),
                        None => vec![relation_name(
                            join.table_alias.as_deref().unwrap_or(&join.table_name),
                        )],
                    }))
                    .collect();
                let pushed = crate::joins::pushed_filter(
                    filter,
                    &inputs,
                    &std::iter::once(true)
                        .chain(joins.iter().map(|join| {
                            join.semi_subquery.is_none()
                                && join.lateral.is_none()
                                && join.table_fn.is_none()
                        }))
                        .collect::<Vec<_>>(),
                    &joins
                        .iter()
                        .map(|join| join.join_type.clone())
                        .collect::<Vec<_>>(),
                );
                let reduced: Option<Vec<Join>> = pushed
                    .join_types
                    .iter()
                    .zip(joins.iter())
                    .any(|(join_type, join)| *join_type != join.join_type)
                    .then(|| {
                        joins
                            .iter()
                            .cloned()
                            .zip(pushed.join_types.iter())
                            .map(|(mut join, join_type)| {
                                join.join_type = join_type.clone();
                                join
                            })
                            .collect()
                    });
                let joins: &[Join] = reduced.as_deref().unwrap_or(joins);
                // A filter on one relation is shown on its scan.
                let scan_filter = if single { filter } else { None };
                // The columns the statement reads: a covering index is
                // preferred for them, and — when nothing later reads an
                // uncovered column — serves them alone.
                let sort_keys: Vec<crate::plan_types::SortKey> = if sort.is_empty() {
                    order_by.iter().cloned().map(SortKey::from_legacy).collect()
                } else {
                    sort.clone()
                };
                // What the outer query left for a pulled-up body of ours.
                let subordinate = take_subordinate();
                let needed_columns = match (
                    crate::index_keys::index_only_needed(projection, filter),
                    subordinate.needed,
                ) {
                    // A `SELECT *` body reads what the outer query reads —
                    // its own conditions included — as PostgreSQL's pull-up
                    // plans it.
                    (Some(crate::index_keys::NeededColumns::All), Some(outer)) => {
                        let mut refs = Vec::new();
                        if let Some(filter) = filter {
                            crate::filter_eval::filter_column_refs(filter, &mut refs);
                        }
                        Some(crate::index_keys::add_needed(outer, &refs))
                    }
                    (own, _) => own,
                }
                .map(|needed| {
                    crate::index_keys::add_needed(
                        needed,
                        &crate::index_keys::sort_columns(&sort_keys, projection)
                            .unwrap_or_default(),
                    )
                });
                let allow_index_only = single
                    && group_by.is_empty()
                    && distinct_on.is_empty()
                    && !*distinct
                    && having.is_none()
                    && sample.is_none();
                // A side's own conditions go on its scan, as PostgreSQL
                // pushes them.
                let with_side_filter = |mut child: Node, filter: Option<&FilterExpr>| {
                    let Some(filter) = filter else {
                        return child;
                    };
                    let text = deparse_filter(filter, false);
                    match child
                        .details
                        .iter_mut()
                        .find(|(name, _, _)| *name == "Filter")
                    {
                        Some((_, existing, json)) => {
                            *existing = format!("({existing} AND {text})");
                            *json = json!(existing.clone());
                        }
                        None => child = child.detail("Filter", text),
                    }
                    child
                };
                // An `ORDER BY` — or the innermost window's ordering — on an
                // index's leading column reads the index in its own order —
                // forward when ascending, backward when descending — as
                // PostgreSQL plans it, with no `WHERE` needed.
                let ordering: Option<(String, bool)> = if single
                    && sample.is_none()
                    && crate::session_env::setting("enable_seqscan").as_deref() == Some("off")
                {
                    ordering_index_column(&sort, projection).or_else(|| {
                        // A pulled-up body that adds no ordering of its own
                        // takes the outer query's, as PostgreSQL's plans show.
                        let mut calls: Vec<&WindowCall> = Vec::new();
                        for item in projection {
                            if let ProjectionItem::Expr { expr, .. } = item {
                                window_calls(expr, &mut calls);
                            }
                        }
                        (sort.is_empty()
                            && group_by.is_empty()
                            && !*distinct
                            && distinct_on.is_empty()
                            && having.is_none()
                            && limit.is_none()
                            && offset.is_none()
                            && calls.is_empty())
                        .then(|| subordinate.ordering.clone())
                        .flatten()
                    })
                } else {
                    None
                };
                let mut node = self.explain_relation_reading(
                    ctx,
                    table_name,
                    table_alias.as_deref(),
                    scan_filter,
                    ordering
                        .as_ref()
                        .map(|(name, backwards)| (name.as_str(), *backwards)),
                    &scope,
                    sample.as_ref(),
                    *only,
                    needed_columns.as_ref(),
                    allow_index_only,
                    Some(&subplans),
                )?;
                // The head relation's own `WHERE` conjuncts sit on its scan.
                if !single && let Some(head_filter) = &pushed.inputs[0] {
                    node = with_side_filter(node, Some(head_filter));
                }
                for (join_index, join) in joins.iter().enumerate() {
                    // The equalities between the sides hash (a `Hash Join`),
                    // unless the session turned hash joins off; anything
                    // else is the nested loop the executor runs, over a
                    // materialized inner — or an inner index read by the
                    // join's equality, as PostgreSQL plans it.
                    let left_prefixes = joined_prefixes.clone();
                    let right_prefixes = match &join.semi_subquery {
                        Some(sub) => crate::joins::relation_quals(sub),
                        None => vec![relation_name(
                            join.table_alias.as_deref().unwrap_or(&join.table_name),
                        )],
                    };
                    let left_refs: Vec<&str> = left_prefixes.iter().map(String::as_str).collect();
                    let right_refs: Vec<&str> = right_prefixes.iter().map(String::as_str).collect();
                    let hash = (crate::session_env::setting("enable_hashjoin").as_deref()
                        != Some("off"))
                    .then(|| {
                        // A `USING` join's named columns are equalities too.
                        let condition = match (&join.condition, join.using_columns.is_empty()) {
                            (Some(condition), _) => Some(condition.clone()),
                            (None, false) => Some(crate::joins::using_condition(
                                &join.using_columns,
                                &left_prefixes,
                                &right_prefixes,
                            )?),
                            (None, true) => None,
                        };
                        crate::joins::hash_plan(condition.as_ref(), &left_refs, &right_refs)
                    })
                    .flatten();
                    // A nested loop's inner side reads an index by the
                    // join's equality when one names its leading column.
                    let inner_index = if hash.is_none()
                        && join.semi_subquery.is_none()
                        && join.lateral.is_none()
                        && join.table_fn.is_none()
                        && join.sample.is_none()
                        && crate::cte_scope::lookup(&join.table_name).is_none()
                        && !matches!(join.join_type, JoinType::RightOuter | JoinType::FullOuter)
                    {
                        parse_object_name(&join.table_name)
                            .ok()
                            .and_then(|(j_db, j_sch, j_tbl_name)| {
                                (!Self::is_virtual_schema(&j_sch)).then_some(())?;
                                let tbl = self
                                    .catalog_reader
                                    .get_table(j_db.clone(), j_sch.clone(), j_tbl_name)
                                    .ok()?;
                                // The executor reads a plain base table this
                                // way: not a view, and no descendants to
                                // append.
                                (tbl.view_query.is_none()
                                    && (join.only
                                        || self.descendants(j_db, tbl.id).ok()?.is_empty()))
                                .then_some(tbl)
                            })
                            .and_then(|tbl| {
                                let found = tbl.indexes.iter().find_map(|idx| {
                                    crate::joins::inner_index_equality(
                                        join.condition.as_ref(),
                                        idx,
                                        &tbl,
                                        &left_refs,
                                        &right_refs,
                                    )
                                    .map(|(column, outer)| (idx.name.clone(), column, outer))
                                })?;
                                Some((found.0, found.1, found.2))
                            })
                    } else {
                        None
                    };
                    let right = match (&join.semi_subquery, &join.lateral, &join.table_fn) {
                        // A semi/anti join's inner side is its subquery's
                        // own relations, with no node of their own.
                        (Some(sub), _, _) => self.explain_node(ctx, sub, &scope)?,
                        (None, Some(sub), _) => {
                            let child = self.explain_node(ctx, sub, &scope)?;
                            let alias = join.table_alias.as_deref().unwrap_or(&join.table_name);
                            Node::over(
                                "Subquery Scan",
                                format!("Subquery Scan on {}", relation_name(alias)),
                                vec![child],
                            )
                            .prop("Alias", json!(relation_name(alias)))
                        }
                        (None, None, Some(spec)) => function_scan(spec),
                        (None, None, None) => match &inner_index {
                            Some((index_name, column, outer)) => Node::new(
                                "Index Scan",
                                format!(
                                    "Index Scan using {} on {}",
                                    index_name,
                                    scan_label_name(&join.table_name, join.table_alias.as_deref())
                                ),
                                node.rows.max(1.0) * 0.01,
                                node.width,
                            )
                            .prop("Scan Direction", json!("Forward"))
                            .prop("Index Name", json!(index_name))
                            .prop("Relation Name", json!(relation_name(&join.table_name)))
                            .prop(
                                "Alias",
                                json!(
                                    join.table_alias
                                        .as_deref()
                                        .map(str::to_string)
                                        .unwrap_or_else(|| relation_name(&join.table_name))
                                ),
                            )
                            .detail(
                                "Index Cond",
                                format!("({column} = {})", deparse_scalar(outer, true)),
                            ),
                            None => self.explain_relation(
                                ctx,
                                &join.table_name,
                                join.table_alias.as_deref(),
                                None,
                                &scope,
                                join.sample.as_ref(),
                                join.only,
                            )?,
                        },
                    };
                    let (suffix, kind) = match join.join_type {
                        JoinType::LeftOuter => (" Left Join", "Left"),
                        JoinType::RightOuter => (" Right Join", "Right"),
                        JoinType::FullOuter => (" Full Join", "Full"),
                        JoinType::Semi => (" Semi Join", "Semi"),
                        JoinType::Anti => (" Anti Join", "Anti"),
                        JoinType::Inner | JoinType::Cross => ("", "Inner"),
                    };

                    let rows = match (&join.join_type, &join.condition) {
                        (JoinType::Cross, _) | (_, None) if join.using_columns.is_empty() => {
                            node.rows * right.rows
                        }
                        _ => node.rows.max(right.rows),
                    };
                    // A semi/anti join emits the left row alone.
                    let width = match join.join_type {
                        JoinType::Semi | JoinType::Anti => node.width,
                        _ => node.width + right.width,
                    };
                    let total = node.total + node.rows * right.total;
                    // The hashed equalities (or the condition), and the
                    // condition checked on each matched pair.
                    // A right join prints as the swapped left join: its
                    // condition names the probe side first, as scanned.
                    let swapped_join = matches!(join.join_type, JoinType::RightOuter);
                    let (hashed_condition, condition) = match (&hash, &join.condition) {
                        (Some(plan), _) => {
                            let mut text = plan
                                .hash_pairs
                                .iter()
                                .map(|(left, right)| {
                                    if swapped_join {
                                        format!(
                                            "({} = {})",
                                            hash_key_text(right, true),
                                            hash_key_text(left, true)
                                        )
                                    } else {
                                        format!(
                                            "({} = {})",
                                            hash_key_text(left, true),
                                            hash_key_text(right, true)
                                        )
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(" AND ");
                            if plan.hash_pairs.len() > 1 {
                                text = format!("({text})");
                            }
                            (true, Some(text))
                        }
                        (None, Some(c)) => (false, Some(deparse_filter(c, true))),
                        (None, None) if !join.using_columns.is_empty() => (
                            false,
                            Some(format!(
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
                        ),
                        (None, None) => (false, None),
                    };
                    let pair_filter = hash.as_ref().and_then(|plan| plan.join_filter());
                    // With an inner index read, the equality is the index
                    // condition; each side's own conditions sit on its
                    // scan, as PostgreSQL plans them.
                    let (inner_only, outer_only, nested) = match (&hash, &inner_index) {
                        (None, Some(_)) => match crate::joins::classify(
                            join.condition.as_ref(),
                            &left_refs,
                            &right_refs,
                        ) {
                            Some(plan) => (
                                plan.right_filter(),
                                plan.left_filter(),
                                Some(plan.join_filter()),
                            ),
                            None => (None, None, None),
                        },
                        _ => (None, None, None),
                    };
                    // With an inner index read, the equality is the Index
                    // Cond and each side's own conditions sit on its scan;
                    // only what both sides decide together stays a filter.
                    let (condition, pair_filter) = match (&inner_index, nested) {
                        (Some(_), Some(filter)) => (filter.map(|f| deparse_filter(&f, true)), None),
                        _ => (condition, pair_filter),
                    };
                    // The member's own `WHERE` conjuncts sit on its scan
                    // (before a right join's swap, where it is `right`).
                    let right = match pushed.inputs.get(join_index + 1).and_then(Option::as_ref) {
                        Some(member_filter) => with_side_filter(right, Some(member_filter)),
                        None => right,
                    };
                    let swapped = swapped_join;
                    let (first, second) = if swapped {
                        (right, node)
                    } else {
                        (node, right)
                    };
                    let materialized = !swapped
                        && crate::session_env::setting("enable_material").as_deref() != Some("off")
                        && hash.is_none()
                        && !matches!(second.kind.as_str(), "Index Scan" | "Index Only Scan");
                    let (label_kind, label) = match (&hash, &join.join_type) {
                        (Some(_), JoinType::LeftOuter | JoinType::RightOuter) => {
                            ("Left", "Hash Left Join".to_string())
                        }
                        (Some(_), JoinType::FullOuter) => ("Full", "Hash Full Join".to_string()),
                        (Some(_), JoinType::Semi) => ("Semi", "Hash Semi Join".to_string()),
                        (Some(_), JoinType::Anti) => ("Anti", "Hash Anti Join".to_string()),
                        (Some(_), _) => ("Inner", "Hash Join".to_string()),
                        (None, _) => (kind, format!("Nested Loop{suffix}")),
                    };
                    let mut joined = Node::new(
                        if hash.is_some() {
                            "Hash Join"
                        } else {
                            "Nested Loop"
                        },
                        label,
                        rows,
                        width,
                    )
                    .prop("Join Type", json!(label_kind));
                    joined.total = total + rows * CPU_TUPLE;
                    // A nested loop chosen with the method disabled says so.
                    if hash.is_none()
                        && crate::session_env::setting("enable_nestloop").as_deref() == Some("off")
                    {
                        joined = joined.detail("Disabled", "true".to_string());
                    }
                    if let Some(condition) = condition {
                        joined = joined.detail(
                            if hashed_condition {
                                "Hash Cond"
                            } else {
                                "Join Filter"
                            },
                            condition,
                        );
                    }
                    if let Some(filter) = &pair_filter {
                        joined = joined.detail("Join Filter", deparse_filter(filter, true));
                    }
                    let (first, second) = match &hash {
                        Some(plan) => (
                            with_side_filter(first, plan.left_filter().as_ref()),
                            with_side_filter(second, plan.right_filter().as_ref()),
                        ),
                        None if inner_index.is_some() => (
                            with_side_filter(first, outer_only.as_ref()),
                            with_side_filter(second, inner_only.as_ref()),
                        ),
                        None => (first, second),
                    };
                    let mut children = vec![first];
                    match &hash {
                        Some(_) => {
                            let mut hashed = Node::over("Hash", "Hash", vec![second]);
                            hashed.total = 0.0;
                            children.push(hashed);
                        }
                        None if materialized => {
                            let mut materialize =
                                Node::over("Materialize", "Materialize", vec![second]);
                            materialize.total = 0.0;
                            children.push(materialize);
                        }
                        None => children.push(second),
                    }
                    joined.children = children;
                    node = joined;
                    joined_prefixes.push(right_prefixes[0].clone());
                }
                if let (false, Some(f)) = (single, pushed.residual.as_ref()) {
                    // A conjunct reading more than one input is a join
                    // qual, as PostgreSQL shows it.
                    let label = if crate::joins::spans_inputs(f, &inputs) {
                        "Join Filter"
                    } else {
                        "Filter"
                    };
                    node = merge_filter_detail(
                        node,
                        label,
                        deparse_filter_with(f, true, Some(&subplans)),
                    );
                    node.rows = (node.rows / 3.0).max(1.0).round();
                }
                // The subqueries the filter and projection read hang off the
                // node that reads them, above any sort or limit.
                let hoisted: Vec<&LogicalPlan> =
                    hoisted_exists.iter().map(|(plan, _)| plan).collect();
                let mut wrappers = Vec::new();
                for (plan, entry) in subplans.collected() {
                    if hoisted.iter().any(|h| fingerprint(h) == fingerprint(plan)) {
                        continue;
                    }
                    // A materialized CTE's plan is numbered here but
                    // rendered as its own `CTE c` block.
                    if materialized
                        .iter()
                        .any(|(_, block)| fingerprint(block) == fingerprint(plan))
                    {
                        continue;
                    }
                    let tree = self.explain_node(ctx, plan, &scope)?;
                    // A correlated subplan's conditions name the enclosing
                    // query's columns too, so they print qualified.
                    let correlated = crate::filter_eval::subquery_is_correlated(plan);
                    let tree = if correlated {
                        requalify_subplan_filter(tree, plan, &subplans)
                    } else {
                        tree
                    };
                    wrappers.push(subplan_node(entry, tree, correlated));
                }
                if !wrappers.is_empty() {
                    node.subplans = wrappers;
                }
                if !hoisted_exists.is_empty() {
                    let mut conditions = Vec::new();
                    let mut wrappers = Vec::new();
                    for (plan, negated) in &hoisted_exists {
                        let reference = subplans
                            .entry(plan)
                            .map(|entry| entry.name.clone())
                            .unwrap_or_else(|| "InitPlan 1".to_string());
                        conditions.push(if *negated {
                            format!("(NOT ({reference}).col1)")
                        } else {
                            format!("({reference}).col1")
                        });
                        let tree = self.explain_node(ctx, plan, &scope)?;
                        let entry = subplans.entry(plan).cloned();
                        let entry = entry.unwrap_or(SubplanEntry {
                            name: "InitPlan 1".to_string(),
                            hashed: false,
                        });
                        wrappers.push(subplan_node(&entry, tree, false));
                    }
                    let mut result = Node::over("Result", "Result", vec![node]);
                    result.total += result.rows * CPU_OPERATOR;
                    result.subplans = wrappers;
                    node = result.detail("One-Time Filter", conditions.join(" AND "));
                }
                let qualified = !single || rendering_cte_body();
                let sort: Vec<SortKey> = if sort.is_empty() {
                    order_by.iter().cloned().map(SortKey::from_legacy).collect()
                } else {
                    sort.clone()
                };
                // An index scan already reads the leading column in order —
                // forward or backward — so `ORDER BY` it adds no Sort, as
                // PostgreSQL plans it.
                let index_ordered = |node: &Node| -> Option<(String, bool)> {
                    if node.kind != "Index Scan" && node.kind != "Index Only Scan" {
                        return None;
                    }
                    let name = node
                        .props
                        .iter()
                        .find(|(name, _)| *name == "Index Name")
                        .and_then(|(_, value)| value.as_str())
                        .map(str::to_string)?;
                    let backwards = node
                        .props
                        .iter()
                        .any(|(name, value)| *name == "Scan Direction" && value == "Backward");
                    // The scan may belong to a pulled-up body: it names its
                    // own relation.
                    let relation = node
                        .props
                        .iter()
                        .find(|(name, _)| *name == "Relation Name")
                        .and_then(|(_, value)| value.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| table_name.to_string());
                    let (db, schema, table) = parse_object_name(&relation).ok()?;
                    let tbl = self.catalog_reader.get_table(db, schema, table).ok()?;
                    let idx = tbl.indexes.iter().find(|i| i.name == name)?;
                    let key = idx.key_columns.first()?;
                    if crate::index_keys::is_expression_key(key) {
                        return None;
                    }
                    let column = tbl.columns.iter().find(|c| c.id == key.column_id)?;
                    Some((column.name.clone(), !backwards))
                };
                let aggregated = !group_by.is_empty()
                    || having.is_some()
                    || projection.iter().any(|p| match p {
                        ProjectionItem::Aggregate(..) => true,
                        ProjectionItem::Expr { expr, .. } => scalar_has_aggregate(expr),
                        _ => false,
                    });
                if aggregated {
                    let input_rows = node.rows;
                    // A query of nothing but `min`/`max` over indexed
                    // columns reads the ends of their indexes: a `Result`
                    // over one `InitPlan` per aggregate, as PostgreSQL
                    // plans it.
                    let minmax = if single
                        && sample.is_none()
                        && filter.is_none()
                        && group_by.is_empty()
                        && let Ok((db, schema, table_only)) = parse_object_name(table_name)
                        && let Ok(tbl) = self.catalog_reader.get_table(db, schema, table_only)
                        && tbl.view_query.is_none()
                        && self.descendants(db, tbl.id).is_ok_and(|d| d.is_empty())
                    {
                        crate::index_keys::minmax_indexes(&tbl, projection)
                    } else {
                        None
                    };
                    node = if let Some(minmax) = minmax {
                        let label = scan_label_name(table_name, table_alias.as_deref());
                        let mut subplans = Vec::new();
                        for (number, (backwards, column, _, index_name)) in
                            minmax.iter().enumerate()
                        {
                            let mut scan = Node::new(
                                "Index Only Scan",
                                format!(
                                    "Index Only Scan{} using {} on {}",
                                    if *backwards { " Backward" } else { "" },
                                    index_name,
                                    label
                                ),
                                node.rows.max(1.0),
                                node.width,
                            )
                            .prop(
                                "Scan Direction",
                                json!(if *backwards { "Backward" } else { "Forward" }),
                            )
                            .prop("Index Name", json!(index_name))
                            .prop("Relation Name", json!(relation_name(table_name)))
                            .prop(
                                "Alias",
                                json!(scan_label_name(table_name, table_alias.as_deref())),
                            )
                            .detail("Index Cond", format!("({column} IS NOT NULL)"));
                            scan.startup = 0.15;
                            scan.total = 8.17;
                            let mut limit = Node::over("Limit", "Limit", vec![scan]);
                            limit.rows = 1.0;
                            let mut wrapper = Node::new(
                                &format!("InitPlan {}", number + 1),
                                format!("InitPlan {}", number + 1),
                                limit.rows,
                                limit.width,
                            );
                            wrapper.startup = limit.startup;
                            wrapper.total = limit.total;
                            wrapper.children = vec![limit];
                            subplans.push(wrapper);
                        }
                        let mut result = Node::new("Result", "Result", 1.0, node.width);
                        result.total = input_rows * CPU_TUPLE;
                        result.subplans = subplans;
                        result
                    } else if group_by.is_empty() {
                        let mut agg = Node::over("Aggregate", "Aggregate", vec![node])
                            .prop("Strategy", json!("Plain"))
                            .prop("Partial Mode", json!("Simple"));
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
                            .prop("Partial Mode", json!("Simple"))
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
                // Window functions evaluate in layers above the
                // aggregation: one `WindowAgg` per window clause, with the
                // sort its window's ordering needs. Clauses sharing an
                // ordering stack in the order written (the first innermost);
                // different orderings stack with the last written innermost,
                // and `OVER ()` outermost — as PostgreSQL plans them.
                let mut calls: Vec<&WindowCall> = Vec::new();
                for item in projection {
                    if let ProjectionItem::Expr { expr, .. } = item {
                        window_calls(expr, &mut calls);
                    }
                }
                for key in &sort {
                    if let SortTarget::Expr(expr) = &key.target {
                        window_calls(expr, &mut calls);
                    }
                }
                // The ordering the outermost window leaves the rows in,
                // which a matching `ORDER BY` reuses.
                let mut window_keys: Option<Vec<String>> = None;
                let mut window_names: std::collections::HashMap<String, String> =
                    std::collections::HashMap::new();
                if !calls.is_empty() {
                    let (layers, order) = window_layer_order(&calls, qualified);
                    let mut provided: Option<Vec<String>> = None;
                    for (number, index) in order.iter().enumerate() {
                        let layer = &layers[*index];
                        window_names.insert(layer.key.clone(), layer.display_name(number + 1));
                        let keys = layer.sort_keys(qualified);
                        // An index scan leaves its column in the order it
                        // reads: ascending forward, descending backward. A
                        // layer's ordering it only starts is an
                        // `Incremental Sort` over the prefix.
                        let presorted: Option<Vec<String>> =
                            index_ordered(&node).map(|(name, ascending)| {
                                let name = column(&name, qualified);
                                vec![if ascending {
                                    name
                                } else {
                                    format!("{name} DESC")
                                }]
                            });
                        let covered = provided.as_ref().is_some_and(|p| p.starts_with(&keys))
                            || presorted.as_ref().is_some_and(|p| p.starts_with(&keys));
                        let incremental = !covered
                            && !keys.is_empty()
                            && presorted
                                .as_ref()
                                .is_some_and(|p| !p.is_empty() && keys.starts_with(p));
                        if !keys.is_empty() && !covered {
                            let n = node.rows.max(2.0);
                            let mut sorted = if incremental {
                                Node::over("Incremental Sort", "Incremental Sort", vec![node])
                                    .keys("Sort Key", keys.clone())
                                    .keys(
                                        "Presorted Key",
                                        presorted_keys_text(presorted.as_ref().unwrap()),
                                    )
                            } else {
                                Node::over("Sort", "Sort", vec![node])
                                    .keys("Sort Key", keys.clone())
                            };
                            sorted.startup = sorted.total + n * n.log2() * 2.0 * CPU_OPERATOR;
                            sorted.total = sorted.startup + n * CPU_TUPLE;
                            node = sorted;
                        }
                        node = Node::over("WindowAgg", "WindowAgg", vec![node])
                            .detail("Window", layer.text(number + 1, qualified));
                        if !keys.is_empty() {
                            provided = Some(keys);
                        }
                    }
                    window_keys = provided;
                } else if projection
                    .iter()
                    .any(|p| matches!(p, ProjectionItem::WindowFunction { .. }))
                {
                    node = Node::over("WindowAgg", "WindowAgg", vec![node]);
                }
                if *distinct {
                    let keys: Vec<String> = projection
                        .iter()
                        .map(|p| match p {
                            ProjectionItem::Expr {
                                expr: ScalarExpr::Window(call),
                                ..
                            } => window_call_text(call, &window_names, qualified),
                            other => projection_text(other, qualified),
                        })
                        .collect();
                    node = Node::over("Aggregate", "HashAggregate", vec![node])
                        .prop("Strategy", json!("Hashed"))
                        .prop("Partial Mode", json!("Simple"))
                        .keys("Group Key", keys);
                }
                let keys: Vec<String> = sort
                    .iter()
                    .map(|k| sort_key_text(k, projection, qualified, &window_names))
                    .collect();
                // An index scan or a window leaves the rows in their
                // ordering; `ORDER BY` reuses it — exactly, or as an
                // Incremental Sort over the prefix it already has.
                let presorted: Option<Vec<String>> = index_ordered(&node)
                    .map(|(name, ascending)| {
                        let name = column(&name, qualified);
                        vec![if ascending {
                            name
                        } else {
                            format!("{name} DESC")
                        }]
                    })
                    .or_else(|| window_keys.clone());
                let covered = presorted
                    .as_ref()
                    .is_some_and(|presorted| presorted.starts_with(&keys));
                let incremental = !covered
                    && !keys.is_empty()
                    && presorted.as_ref().is_some_and(|presorted| {
                        !presorted.is_empty() && keys.starts_with(presorted)
                    });
                if !sort.is_empty() && !covered {
                    let n = node.rows.max(2.0);
                    let mut sorted = if incremental {
                        let mut sorted =
                            Node::over("Incremental Sort", "Incremental Sort", vec![node])
                                .keys("Sort Key", keys.clone())
                                .keys(
                                    "Presorted Key",
                                    presorted_keys_text(presorted.as_ref().unwrap()),
                                );
                        sorted.startup = sorted.total + n * n.log2() * 2.0 * CPU_OPERATOR;
                        sorted.total = sorted.startup + n * CPU_TUPLE;
                        sorted
                    } else {
                        let mut sorted =
                            Node::over("Sort", "Sort", vec![node]).keys("Sort Key", keys.clone());
                        sorted.startup = sorted.total + n * n.log2() * 2.0 * CPU_OPERATOR;
                        sorted.total = sorted.startup + n * CPU_TUPLE;
                        sorted
                    };
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
                // A materialized CTE's plan hangs off the query's top
                // node, before its other subplans.
                let mut cte_at = 0;
                for (name, plan) in materialized {
                    let _cte_body = CteBodyGuard::enter(&name);
                    let tree = self.explain_node(ctx, plan, &scope)?;
                    drop(_cte_body);
                    let mut block = Node::new(
                        &format!("CTE {name}"),
                        format!("CTE {name}"),
                        tree.rows,
                        tree.width,
                    );
                    block.startup = tree.startup;
                    block.total = tree.total;
                    block.children = vec![tree];
                    node.subplans.insert(cte_at, block);
                    cte_at += 1;
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
                    (SetOpKind::Union, true) => {
                        Node::over("Append", "Append", children).prop("Subplans Removed", json!(0))
                    }
                    (SetOpKind::Union, false) => {
                        let append = Node::over("Append", "Append", children)
                            .prop("Subplans Removed", json!(0));
                        Node::over("Aggregate", "HashAggregate", vec![append])
                            .prop("Strategy", json!("Hashed"))
                            .prop("Partial Mode", json!("Simple"))
                            .keys("Group Key", setop_group_keys(left))
                    }
                    (SetOpKind::Intersect, _) => {
                        Node::over("SetOp", "HashSetOp Intersect", children)
                            .prop("Command", json!("Intersect"))
                            .prop("Strategy", json!("Hashed"))
                    }
                    (SetOpKind::Except, _) => Node::over("SetOp", "HashSetOp Except", children)
                        .prop("Command", json!("Except"))
                        .prop("Strategy", json!("Hashed")),
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
            LogicalPlan::Renamed { input, .. }
            | LogicalPlan::ViewCheckOption { query: input, .. } => {
                self.explain_node(ctx, input, ctes)?
            }
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
                .prop("Relation Name", json!(relation_name(table_name)))
                .prop("Alias", json!(relation_name(table_name)));
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
                joins,
                ..
            } => self.explain_modify(
                ctx,
                "Update",
                (table_name, table_alias.as_deref()),
                from.as_deref(),
                joins,
                filter.as_ref(),
                ctes,
            )?,
            LogicalPlan::Delete {
                table_name,
                table_alias,
                filter,
                using,
                joins,
                ..
            } => self.explain_modify(
                ctx,
                "Delete",
                (table_name, table_alias.as_deref()),
                using.as_deref(),
                joins,
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
                let target = self.explain_relation(
                    ctx,
                    table_name,
                    table_alias.as_deref(),
                    None,
                    ctes,
                    None,
                    false,
                )?;
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
                .prop("Relation Name", json!(relation_name(table_name)))
                .prop(
                    "Alias",
                    json!(relation_name(table_alias.as_deref().unwrap_or(table_name))),
                );
                node.rows = 0.0;
                node.width = 0;
                node
            }
            LogicalPlan::With {
                ctes: own,
                cte_hints,
                body,
            } => {
                let mut scope: Vec<(String, &LogicalPlan)> = ctes.to_vec();
                scope.extend(own.iter().map(|(n, p)| (n.clone(), &**p)));
                let mut node = self.explain_node(ctx, body, &scope)?;
                // A `WITH` over a data-modifying statement materializes the
                // queries it reads: each one's plan hangs off the top node.
                for (name, plan) in own {
                    if plan_cte_references(body, name) == 0 {
                        continue;
                    }
                    let _cte_body = CteBodyGuard::enter(&name);
                    let tree = self.explain_node(ctx, &**plan, &scope)?;
                    drop(_cte_body);
                    let mut block = Node::new(
                        &format!("CTE {name}"),
                        format!("CTE {name}"),
                        tree.rows,
                        tree.width,
                    );
                    block.startup = tree.startup;
                    block.total = tree.total;
                    block.children = vec![tree];
                    node.subplans.push(block);
                }
                node
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
        joins: &[crate::Join],
        filter: Option<&FilterExpr>,
        ctes: &[(String, &LogicalPlan)],
    ) -> Result<Node> {
        // An equality between the target and the relations hashes, with
        // each side's own conditions on its scan, as PostgreSQL plans it.
        let target_prefix = relation_name(table_alias.unwrap_or(table_name));
        let source_plan = source;
        let source_prefixes = source_plan
            .map(crate::joins::relation_labels)
            .unwrap_or_default();
        let target_refs: Vec<&str> = std::iter::once(target_prefix.as_str()).collect();
        let source_refs: Vec<&str> = source_prefixes.iter().map(String::as_str).collect();
        let hash =
            source_plan.and_then(|_| crate::joins::hash_plan(filter, &target_refs, &source_refs));
        let target_filter = hash.as_ref().and_then(|plan| plan.left_filter());
        let source_filter = hash.as_ref().and_then(|plan| plan.right_filter());
        let pair_filter = hash.as_ref().and_then(|plan| plan.join_filter());
        // A target with descendants is scanned partition by partition, each
        // listed as a result relation; the scan itself is pruned the way a
        // query's is.
        let scans =
            match source {
                None => parse_object_name(table_name).ok().and_then(
                    |(db_name, schema_name, table_only)| {
                        let tbl = self
                            .catalog_reader
                            .get_table(db_name, schema_name, table_only)
                            .ok()?;
                        if tbl.view_query.is_some() {
                            return None;
                        }
                        let scans = self
                            .scan_tables(
                                db_name,
                                &tbl,
                                table_alias,
                                table_only,
                                target_filter.as_ref().or(filter),
                                false,
                            )
                            .ok()?;
                        (!scans.is_empty()).then_some((tbl, scans))
                    },
                ),
                Some(_) => None,
            };
        let (child, result_lines) = match (source, scans) {
            (None, Some((tbl, scans))) => {
                let mut lines = Vec::new();
                for (table, alias) in &scans {
                    lines.push(format!(
                        "{operation} on {} {alias}",
                        relation_name(&table.name)
                    ));
                }
                let children =
                    self.scan_children(ctx, &scans, target_filter.as_ref().or(filter))?;
                let scan = if children.len() == 1 {
                    children.into_iter().next().unwrap()
                } else {
                    append_scan(&tbl, children)
                };
                // `UPDATE` on an inherited (not partitioned) target computes
                // the new row in a `Result` over the scan.
                let scan = if operation == "Update" && tbl.partition_by.is_none() {
                    Node::over("Result", "Result", vec![scan])
                } else {
                    scan
                };
                (scan, lines)
            }
            (None, None) => (
                self.explain_relation(
                    ctx,
                    table_name,
                    table_alias,
                    target_filter.as_ref().or(filter),
                    ctes,
                    None,
                    false,
                )?,
                Vec::new(),
            ),
            (Some(source), _) => {
                let target = self.explain_relation(
                    ctx,
                    table_name,
                    table_alias,
                    target_filter.as_ref(),
                    ctes,
                    None,
                    false,
                )?;
                let mut source_node = self.explain_node(ctx, source, ctes)?;
                if let Some(f) = &source_filter {
                    source_node = source_node.detail("Filter", deparse_filter(f, false));
                }
                let mut node = match &hash {
                    Some(plan) => {
                        let text = plan
                            .hash_pairs
                            .iter()
                            .map(|(target, source)| {
                                format!(
                                    "({} = {})",
                                    hash_key_text(target, true),
                                    hash_key_text(source, true)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(" AND ");
                        let text = if text.matches(" = ").count() > 1 {
                            format!("({text})")
                        } else {
                            text
                        };
                        let mut hashed = Node::over("Hash", "Hash", vec![source_node]);
                        hashed.total = 0.0;
                        let join = Node::over("Hash Join", "Hash Join", vec![target, hashed])
                            .prop("Join Type", json!("Inner"))
                            .detail("Hash Cond", text);
                        if let Some(f) = &pair_filter {
                            join.detail("Join Filter", deparse_filter(f, true))
                        } else {
                            join
                        }
                    }
                    None => {
                        // A nested loop reads the relations' rows again for
                        // every target row, so they are materialized.
                        if crate::session_env::setting("enable_material").as_deref() != Some("off")
                            && !matches!(
                                source_node.kind.as_str(),
                                "Index Scan" | "Index Only Scan"
                            )
                        {
                            let mut materialize =
                                Node::over("Materialize", "Materialize", vec![source_node]);
                            materialize.total = 0.0;
                            source_node = materialize;
                        }
                        let mut join =
                            Node::over("Nested Loop", "Nested Loop", vec![target, source_node])
                                .prop("Join Type", json!("Inner"));
                        if let Some(f) = filter {
                            join = join.detail("Join Filter", deparse_filter(f, true));
                        }
                        join
                    }
                };
                (node, Vec::new())
            }
        };
        // The condition's `[NOT] EXISTS` / `IN` subqueries join as
        // semi/anti joins.
        let mut node = child;
        let mut outer_prefixes = vec![target_prefix.clone()];
        if source_plan.is_some() {
            outer_prefixes.extend(source_prefixes.iter().cloned());
        }
        for join in joins {
            let Some(plan) = &join.semi_subquery else {
                continue;
            };
            let inner = self.explain_node(ctx, plan, ctes)?;
            let inner_prefixes = crate::joins::relation_quals(plan);
            let left_refs: Vec<&str> = outer_prefixes.iter().map(String::as_str).collect();
            let right_refs: Vec<&str> = inner_prefixes.iter().map(String::as_str).collect();
            let sub_hash =
                crate::joins::hash_plan(join.condition.as_ref(), &left_refs, &right_refs);
            let anti = matches!(join.join_type, crate::JoinType::Anti);
            let (kind, label) = match (&sub_hash, anti) {
                (Some(_), false) => ("Semi", "Hash Semi Join"),
                (Some(_), true) => ("Anti", "Hash Anti Join"),
                (None, false) => ("Semi", "Nested Loop Semi Join"),
                (None, true) => ("Anti", "Nested Loop Anti Join"),
            };
            let (hashed_condition, condition) = match (&sub_hash, &join.condition) {
                (Some(plan), _) => {
                    let text = plan
                        .hash_pairs
                        .iter()
                        .map(|(left, right)| {
                            format!(
                                "({} = {})",
                                hash_key_text(left, true),
                                hash_key_text(right, true)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    let text = if plan.hash_pairs.len() > 1 {
                        format!("({text})")
                    } else {
                        text
                    };
                    (true, Some(text))
                }
                (None, Some(c)) => (false, Some(deparse_filter(c, true))),
                (None, None) => (false, None),
            };
            let width = node.width;
            let rows = node.rows.max(inner.rows);
            let mut joined = Node::over(
                if sub_hash.is_some() {
                    "Hash Join"
                } else {
                    "Nested Loop"
                },
                label,
                vec![node],
            )
            .prop("Join Type", json!(kind));
            joined.width = width;
            joined.rows = rows;
            if let Some(condition) = condition {
                joined = joined.detail(
                    if hashed_condition {
                        "Hash Cond"
                    } else {
                        "Join Filter"
                    },
                    condition,
                );
            }
            if let Some(plan) = &sub_hash
                && let Some(f) = plan.join_filter()
            {
                joined = joined.detail("Join Filter", deparse_filter(&f, true));
            }
            if sub_hash.is_some() {
                let mut hashed = Node::over("Hash", "Hash", vec![inner]);
                hashed.total = 0.0;
                joined.children.push(hashed);
            } else if crate::session_env::setting("enable_material").as_deref() != Some("off")
                && !matches!(inner.kind.as_str(), "Index Scan" | "Index Only Scan")
            {
                let mut materialize = Node::over("Materialize", "Materialize", vec![inner]);
                materialize.total = 0.0;
                joined.children.push(materialize);
            } else {
                joined.children.push(inner);
            }
            node = joined;
            outer_prefixes.extend(inner_prefixes);
        }
        let mut node = Node::over(
            "ModifyTable",
            format!(
                "{operation} on {}",
                scan_label_name(table_name, table_alias)
            ),
            vec![node],
        )
        .prop("Operation", json!(operation))
        .prop("Relation Name", json!(relation_name(table_name)))
        .prop(
            "Alias",
            json!(relation_name(table_alias.unwrap_or(table_name))),
        );
        for line in result_lines {
            node = node.note(line);
        }
        node.rows = 0.0;
        node.width = 0;
        Ok(node)
    }

    /// A scan's tables: a partitioned table's partitions after pruning, or
    /// an inherited table and its descendants; each with the alias the plan
    /// gives it (`t_1`, `t_2`, ...). Empty when nothing can match.
    fn scan_tables(
        &self,
        db_name: &str,
        tbl: &nodus_catalog::TableDescriptor,
        table_alias: Option<&str>,
        table_only: &str,
        filter: Option<&FilterExpr>,
        only: bool,
    ) -> Result<Vec<(nodus_catalog::TableDescriptor, String)>> {
        // A partitioned table holds no rows of its own: PostgreSQL's Append
        // lists only its partitions; an inherited table lists itself first.
        let partitioned = tbl.partition_by.is_some();
        let parent_alias = relation_name(table_alias.unwrap_or(table_only));
        let descendants = if only {
            Vec::new()
        } else {
            self.descendants(db_name, tbl.id)?
        };
        if descendants.is_empty() {
            return Ok(Vec::new());
        }
        let mut tables: Vec<nodus_catalog::TableDescriptor> = if partitioned {
            descendants
        } else {
            std::iter::once(tbl.clone()).chain(descendants).collect()
        };
        // Plan-time pruning: only the partitions a filter can reach are
        // scanned. A sub-partitioned child is kept whole (its own
        // partitions are not pruned here).
        if partitioned {
            let direct = self.partition_children(db_name, tbl.id)?;
            if direct.len() == tables.len() {
                let text = tbl.partition_by.as_deref().unwrap_or_default();
                let key = crate::partitioning::PartitionKey::parse(text)?;
                let kept = crate::partitioning::surviving_partitions(tbl, &key, &tables, filter);
                tables = kept.into_iter().map(|at| tables[at].clone()).collect();
            }
        }
        Ok(tables
            .into_iter()
            .enumerate()
            .map(|(at, table)| (table, format!("{parent_alias}_{}", at + 1)))
            .collect())
    }

    /// The scans of `scan_tables`' tables, each carrying the filter, as
    /// PostgreSQL plans them.
    fn scan_children(
        &self,
        ctx: &ExecutionContext,
        scans: &[(nodus_catalog::TableDescriptor, String)],
        filter: Option<&FilterExpr>,
    ) -> Result<Vec<Node>> {
        scans
            .iter()
            .map(|(table, alias)| self.scan_node(ctx, table, alias, filter))
            .collect()
    }

    /// One table's `Seq Scan`, filter included.
    fn scan_node(
        &self,
        ctx: &ExecutionContext,
        table: &nodus_catalog::TableDescriptor,
        alias: &str,
        filter: Option<&FilterExpr>,
    ) -> Result<Node> {
        let table_rows = self.scan_rows(table.id, &ctx.session_id)?.len() as f64;
        let table_width: u32 = table.columns.iter().map(|c| type_width(&c.data_type)).sum();
        let pages = (table_rows * table_width as f64 / 8192.0).ceil().max(1.0);
        let mut child = Node::new(
            "Seq Scan",
            format!("Seq Scan on {}", scan_label_name(&table.name, Some(alias))),
            table_rows,
            table_width,
        )
        .prop("Relation Name", json!(table.name))
        .prop("Alias", json!(alias));
        child.startup = 0.0;
        child.total = pages * PAGE + table_rows * CPU_TUPLE;
        if let Some(f) = filter {
            child = child.detail("Filter", deparse_filter(f, false));
            child.total += child.rows * CPU_OPERATOR;
            child.rows = (child.rows * selectivity(f)).max(1.0).round();
        }
        Ok(child)
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
        sample: Option<&crate::SampleSpec>,
        only: bool,
    ) -> Result<Node> {
        self.explain_relation_reading(
            ctx,
            table_name,
            table_alias,
            filter,
            None,
            ctes,
            sample,
            only,
            None,
            false,
            None,
        )
    }

    /// A scan with the columns the statement reads: when an index covers
    /// them all, it is an index-only scan.
    #[allow(clippy::too_many_arguments)]
    fn explain_relation_reading(
        &self,
        ctx: &ExecutionContext,
        table_name: &str,
        table_alias: Option<&str>,
        filter: Option<&FilterExpr>,
        // The column an `ORDER BY` reads the index in order by, and whether
        // it reads it backward (`DESC`).
        ordering: Option<(&str, bool)>,
        ctes: &[(String, &LogicalPlan)],
        sample: Option<&crate::SampleSpec>,
        only: bool,
        index_only: Option<&crate::index_keys::NeededColumns>,
        allow_index_only: bool,
        subplans: Option<&Subplans>,
    ) -> Result<Node> {
        let with_filter = |mut node: Node| {
            if let Some(f) = filter {
                node = merge_filter_detail(node, "Filter", deparse_filter_with(f, false, subplans));
                node.total += node.rows * CPU_OPERATOR;
                node.rows = (node.rows * selectivity(f)).max(1.0).round();
            }
            node
        };
        if let Some((name, plan)) = ctes.iter().rev().find(|(name, _)| name == table_name) {
            let _cte_body = CteBodyGuard::enter(name);
            // A recursive CTE reads itself: rendering its body again
            // would never end, so the scan stands alone.
            let recursive = matches!(&**plan, LogicalPlan::RecursiveCte { .. });
            // A derived table or single-use CTE is pulled up: the query's
            // ordering and its column needs reach the body's own scan, as
            // PostgreSQL's pull-up plans show.
            let _subordinate = (cte_inlined_here(table_name)
                && matches!(&**plan, LogicalPlan::Select { .. }))
            .then(|| {
                SubordinateGuard::enter(Subordinate {
                    ordering: ordering.map(|(name, backwards)| (name.to_string(), backwards)),
                    needed: index_only.cloned(),
                })
            });
            let child = if recursive {
                Node::new("CTE Scan", format!("CTE Scan on {name}"), 100.0, 64)
            } else {
                self.explain_node(ctx, plan, ctes)?
            };
            drop(_subordinate);
            drop(_cte_body);
            // The wrapper a query over a set operation or VALUES list gets.
            if name == "\u{0}result" {
                return Ok(with_filter(child));
            }
            if let LogicalPlan::TableFunction(spec) = plan {
                return Ok(with_filter(function_scan(spec)));
            }
            // The body of a CTE the query reads once stands in for the
            // reference, as PostgreSQL's inlining plans it.
            if cte_inlined_here(table_name) {
                return Ok(with_filter(child));
            }
            // Inside a recursive CTE's own body, its self-reference is
            // the working table.
            if recursive && rendering_body_of(name) {
                let node = Node::new(
                    "WorkTable Scan",
                    format!("WorkTable Scan on {name}"),
                    child.rows,
                    child.width,
                )
                .prop("CTE Name", json!(name))
                .prop("Alias", json!(relation_name(name)));
                return Ok(with_filter(node));
            }

            // A materialized CTE is read by a `CTE Scan`; its plan hangs
            // off the query's top node as a `CTE c` block.
            let alias = relation_name(table_alias.unwrap_or(name));
            let label = if alias == relation_name(name) {
                format!("CTE Scan on {name}")
            } else {
                format!("CTE Scan on {name} {alias}")
            };
            let node = Node::new("CTE Scan", label, child.rows, child.width)
                .prop("CTE Name", json!(name))
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
        // A scan reads the table's descendants too, appending each table's
        // rows; `ONLY` reads just the table.
        let descendant_tables = if only {
            Vec::new()
        } else {
            self.descendants(db_name, tbl.id)?
        };
        let scans = self.scan_tables(db_name, &tbl, table_alias, table_only, filter, only)?;
        if !scans.is_empty() {
            let children = self.scan_children(ctx, &scans, filter)?;
            // One partition alone needs no Append: its scan stands in for
            // the parent, under the query's own name.
            if tbl.partition_by.is_some() && children.len() == 1 {
                let parent_alias = relation_name(table_alias.unwrap_or(table_only));
                return Ok(single_scan(children, &scans[0].0, &parent_alias));
            }
            return Ok(append_scan(&tbl, children));
        }
        if !descendant_tables.is_empty() {
            // Every partition was pruned: no rows come back.
            return Ok(empty_scan());
        }
        let width: u32 = tbl.columns.iter().map(|c| type_width(&c.data_type)).sum();
        let pages = (rows * width as f64 / 8192.0).ceil().max(1.0);
        // The executor reads `col = value` on an indexed column by index,
        // and — when the session asks for index scans — a range over the
        // leading column from the index's key order (`SET enable_seqscan =
        // off`).
        // The index the scan reads: an equality on the leading column of
        // one (found in a conjunction), or — when the session asks for index
        // scans — a range over it (`SET enable_seqscan = off`).
        // A version-3 index folds conjuncts on trailing key columns into
        // the index condition; older ones read the leading column alone.
        let composite = crate::index_keys::composite_index(&tbl, &tbl.indexes, filter, index_only);
        let scan_index = tbl.indexes.iter().find_map(|idx| {
            let key = idx.key_columns.first()?;
            if crate::index_keys::is_expression_key(key) {
                return None;
            }
            let column = tbl.columns.iter().find(|c| c.id == key.column_id)?;
            let equality = filter
                .and_then(|filter| {
                    crate::index_keys::equality_operand(filter, &column.name).map(|_| ())
                })
                .is_some();
            let range = !equality
                && crate::session_env::setting("enable_seqscan").as_deref() == Some("off")
                && filter
                    .and_then(|filter| crate::index_keys::range_bounds(filter, &column.name))
                    .is_some()
                && Self::index_entry_encoding(&tbl, idx).1;
            (equality || range).then_some((column, idx))
        });
        let scan_index = match &composite {
            Some((idx, _)) => tbl
                .columns
                .iter()
                .find(|c| {
                    idx.key_columns
                        .first()
                        .is_some_and(|key| key.column_id == c.id)
                })
                .map(|column| (column, *idx)),
            None => scan_index,
        };
        // An `ORDER BY` on an index's leading column reads the index in
        // its own order, with no condition of its own.
        let ordering_index = ordering.and_then(|(ordering, _)| {
            if crate::session_env::setting("enable_seqscan").as_deref() != Some("off") {
                return None;
            }
            let bare = ordering.rsplit('.').next().unwrap_or(ordering);
            tbl.indexes.iter().find_map(|idx| {
                let key = idx.key_columns.first()?;
                if crate::index_keys::is_expression_key(key) {
                    return None;
                }
                let column = tbl.columns.iter().find(|c| c.id == key.column_id)?;
                (column.name.eq_ignore_ascii_case(bare) && Self::index_entry_encoding(&tbl, idx).1)
                    .then_some((column, idx))
            })
        });
        let scan_index = scan_index.or(ordering_index);
        // With index scans asked for, a covering index serves the whole
        // scan when nothing orders or filters it.
        let covering_index = if allow_index_only
            && crate::session_env::setting("enable_seqscan").as_deref() == Some("off")
        {
            index_only.and_then(|needed| {
                tbl.indexes
                    .iter()
                    .find(|idx| {
                        let Some(key) = idx.key_columns.first() else {
                            return false;
                        };
                        !crate::index_keys::is_expression_key(key)
                            && Self::index_entry_encoding(&tbl, idx).1
                            && crate::index_keys::needed_covered(&tbl, idx, needed)
                    })
                    .and_then(|idx| {
                        tbl.columns
                            .iter()
                            .find(|c| {
                                idx.key_columns
                                    .first()
                                    .is_some_and(|key| key.column_id == c.id)
                            })
                            .map(|column| (column, idx))
                    })
            })
        } else {
            None
        };
        let scan_index = scan_index.or(covering_index);
        // A descending `ORDER BY` reads the index backward, whichever way
        // the scan's own condition picked it.
        let backwards = ordering.is_some_and(|(name, backwards)| {
            let bare = name.rsplit('.').next().unwrap_or(name);
            backwards
                && scan_index.is_some_and(|(column, _)| column.name.eq_ignore_ascii_case(bare))
        });
        if sample.is_none()
            && let Some((col, idx)) = scan_index
            && let Some(idx) =
                crate::index_keys::equality_index(&tbl, &tbl.indexes, col.id, index_only)
        {
            // The scan's own condition is what the index is read by; the
            // rest of the filter is applied to each row (`Filter:`), as
            // PostgreSQL splits them.
            let (index_cond_text, residual_filter) = match filter {
                Some(filter) => {
                    let mut index_conds: Vec<&FilterExpr> = Vec::new();
                    let mut residuals: Vec<&FilterExpr> = Vec::new();
                    let consumed: Vec<String> = composite
                        .as_ref()
                        .map(|(_, conds)| conds.columns.clone())
                        .unwrap_or_else(|| vec![col.name.clone()]);
                    for conjunct in crate::index_keys::conjuncts(filter) {
                        let usable = match conjunct {
                            FilterExpr::Predicate(Predicate { left, op, right }) => {
                                let name = left.rsplit('.').next().unwrap_or(left);
                                consumed.iter().any(|c| c == name)
                                    && matches!(
                                        op,
                                        CompareOp::Eq
                                            | CompareOp::Lt
                                            | CompareOp::Le
                                            | CompareOp::Gt
                                            | CompareOp::Ge
                                    )
                                    && matches!(right, Operand::Literal(_))
                            }
                            _ => false,
                        };
                        if usable {
                            index_conds.push(conjunct);
                        } else {
                            residuals.push(conjunct);
                        }
                    }
                    let join = |parts: &[&FilterExpr]| -> Option<String> {
                        match parts {
                            [] => None,
                            [one] => Some(deparse_filter(one, false)),
                            many => Some(format!(
                                "({})",
                                many.iter()
                                    .map(|part| deparse_filter(part, false))
                                    .collect::<Vec<_>>()
                                    .join(" AND ")
                            )),
                        }
                    };
                    (
                        join(&index_conds).unwrap_or_else(|| deparse_filter(filter, false)),
                        join(&residuals),
                    )
                }
                None => (String::new(), None),
            };
            let only = allow_index_only
                && index_only
                    .is_some_and(|needed| crate::index_keys::needed_covered(&tbl, idx, needed));
            // A range across much of the table reads through a bitmap, as
            // PostgreSQL plans it: the matching entry keys first, then the
            // heap in its own order, rechecking the condition.
            let bitmap = if only || ordering.is_some() {
                false
            } else {
                let entries = match &composite {
                    Some((cidx, conds)) => {
                        let mut prefix = String::new();
                        let mut encodable = true;
                        for (name, operand) in &conds.equalities {
                            let Some(data_type) = tbl
                                .columns
                                .iter()
                                .find(|c| c.name == *name)
                                .map(|c| c.data_type.clone())
                            else {
                                encodable = false;
                                break;
                            };
                            let value = self.eval_operand(&[], &[], &[], operand, &data_type);
                            match crate::index_keys::key_component(&value, &data_type) {
                                Some(hex) => {
                                    if !prefix.is_empty() {
                                        prefix.push('!');
                                    }
                                    prefix.push_str(&hex);
                                }
                                None => {
                                    encodable = false;
                                    break;
                                }
                            }
                        }
                        if !encodable {
                            None
                        } else {
                            let bounds = conds.range.as_ref().and_then(|(name, lower, upper)| {
                                let data_type = tbl
                                    .columns
                                    .iter()
                                    .find(|c| c.name == *name)?
                                    .data_type
                                    .clone();
                                Some((data_type, lower.clone(), upper.clone()))
                            });
                            Some((prefix, bounds))
                        }
                        .map(|(prefix, bounds)| {
                            let bounds = bounds.as_ref().map(|(ty, lower, upper)| {
                                (ty.as_str(), lower.as_ref(), upper.as_ref())
                            });
                            self.index_prefix_count(cidx.id, &prefix, bounds, &ctx.session_id)
                        })
                    }
                    None => filter
                        .and_then(|filter| Self::leading_range(filter, &tbl))
                        .map(|(name, (lower, upper))| {
                            let data_type = tbl
                                .columns
                                .iter()
                                .find(|c| c.name == name)
                                .map(|c| c.data_type.clone())
                                .unwrap_or_default();
                            let bounds = (data_type.as_str(), lower.as_ref(), upper.as_ref());
                            self.index_prefix_count(idx.id, "", Some(bounds), &ctx.session_id)
                        }),
                };
                entries
                    .is_some_and(|entries| crate::index_keys::bitmap_scan_is_wider(rows, entries))
            };
            if bitmap {
                let inner = Node::new(
                    "Bitmap Index Scan",
                    format!("Bitmap Index Scan on {}", idx.name),
                    (rows * 0.005).max(1.0),
                    0,
                )
                .detail("Index Cond", index_cond_text.clone());
                let mut node = Node::over(
                    "Bitmap Heap Scan",
                    format!("Bitmap Heap Scan on {label}"),
                    vec![inner],
                )
                .prop("Recheck Cond", json!(&index_cond_text))
                .detail("Recheck Cond", index_cond_text.clone());
                if let Some(residual) = residual_filter {
                    node = node.detail("Filter", residual);
                }
                node.startup = 0.15;
                node.total = 8.17;
                return Ok(node);
            }
            let scan_kind = if only {
                "Index Only Scan"
            } else {
                "Index Scan"
            };
            let mut node = Node::new(
                scan_kind,
                format!(
                    "{scan_kind}{} using {} on {label}",
                    if backwards { " Backward" } else { "" },
                    idx.name
                ),
                if idx.unique {
                    1.0
                } else {
                    (rows * 0.005).max(1.0)
                },
                width,
            )
            .prop(
                "Scan Direction",
                json!(if backwards { "Backward" } else { "Forward" }),
            )
            .prop("Index Name", json!(idx.name))
            .prop("Relation Name", json!(table_only))
            .prop("Alias", json!(table_alias.unwrap_or(table_only)));
            // An ordering scan has no condition of its own.
            if !index_cond_text.is_empty() {
                node = node.detail("Index Cond", index_cond_text);
            }
            if let Some(residual) = residual_filter {
                node = node.detail("Filter", residual);
            }
            node.startup = 0.15;
            node.total = 8.17;
            return Ok(node);
        }
        let mut node = match sample {
            Some(spec) => {
                let (detail, rows) = sampling_detail(spec, rows);
                Node::new(
                    "Sample Scan",
                    format!("Sample Scan on {label}"),
                    rows,
                    width,
                )
                .prop("Relation Name", json!(table_only))
                .prop("Alias", json!(table_alias.unwrap_or(table_only)))
                .detail("Sampling", detail)
            }
            None => {
                let scan = Node::new("Seq Scan", format!("Seq Scan on {label}"), rows, width)
                    .prop("Relation Name", json!(table_only))
                    .prop("Alias", json!(table_alias.unwrap_or(table_only)));
                // A scan chosen while sequential scans are disabled says so.
                if crate::session_env::setting("enable_seqscan").as_deref() == Some("off") {
                    scan.detail("Disabled", "true".to_string())
                } else {
                    scan
                }
            }
        };
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
thread_local! {
    /// The plan of an inlined CTE's column references: a reference to the
    /// CTE's output prints the body's own name (or expression), as
    /// PostgreSQL's inlined plans do. Also the names being inlined.
    static CTE_SCOPE: std::cell::RefCell<CteScope> =
        std::cell::RefCell::new(CteScope::default());
}

#[derive(Default)]
struct CteScope {
    /// Stored reference -> the stored reference it prints as.
    labels: std::collections::HashMap<String, String>,
    /// The CTE names inlined at this level.
    inlined: std::collections::HashSet<String>,
}

/// Adds a `Filter`-style detail, ANDing into the existing one — a scan
/// shows one filter, as PostgreSQL plans it.
fn merge_filter_detail(mut node: Node, name: &'static str, text: String) -> Node {
    match node
        .details
        .iter_mut()
        .find(|(detail, _, _)| *detail == name)
    {
        Some((_, existing, json)) => {
            *existing = format!("({existing} AND {text})");
            *json = json!(existing.clone());
        }
        None => node = node.detail(name, text),
    }
    node
}

/// Restores the enclosing CTE scope when dropped.
struct CteScopeGuard(CteScope);

impl CteScopeGuard {
    fn extend(
        labels: std::collections::HashMap<String, String>,
        inlined: std::collections::HashSet<String>,
        shadowed: &[String],
    ) -> Self {
        let previous = CTE_SCOPE.with(|scope| {
            let mut scope = scope.borrow_mut();
            let previous = std::mem::take(&mut *scope);
            scope.labels = previous.labels.clone();
            // A CTE this level declares hides the enclosing one of the
            // same name.
            for name in shadowed {
                scope
                    .labels
                    .retain(|reference, _| !reference.starts_with(&format!("{name}.")));
            }
            scope.labels.extend(labels);
            scope.inlined = previous.inlined.clone();
            for name in shadowed {
                scope.inlined.remove(name);
            }
            scope.inlined.extend(inlined);
            previous
        });
        CteScopeGuard(previous)
    }
}

impl Drop for CteScopeGuard {
    fn drop(&mut self) {
        let previous = std::mem::take(&mut self.0);
        CTE_SCOPE.with(|scope| *scope.borrow_mut() = previous);
    }
}

/// Whether a CTE read at this level is inlined into it.
fn cte_inlined_here(name: &str) -> bool {
    CTE_SCOPE.with(|scope| scope.borrow().inlined.contains(name))
}

thread_local! {
    /// The CTE bodies being rendered: their columns print qualified, as
    /// PostgreSQL's plan texts show them, and a recursive body's
    /// self-reference is a `WorkTable Scan`.
    static CTE_BODIES: std::cell::RefCell<Vec<String>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

thread_local! {
    /// What the outer query needs of a pulled-up body's scan — its ordering
    /// and the columns it reads — reaching the base scan, as PostgreSQL's
    /// pull-up plans it. Consumed once by the body.
    static SUBORDINATE: std::cell::RefCell<Option<Subordinate>> =
        const { std::cell::RefCell::new(None) };
}

/// A pulled-up body's inherited requirements.
#[derive(Default)]
struct Subordinate {
    ordering: Option<(String, bool)>,
    needed: Option<crate::index_keys::NeededColumns>,
}

/// Takes what the outer query left for a pulled-up body, if anything.
fn take_subordinate() -> Subordinate {
    SUBORDINATE
        .with(|slot| slot.borrow_mut().take())
        .unwrap_or_default()
}

/// Leaves the outer query's requirements for a pulled-up `Select` body,
/// restoring the slot after the body renders.
struct SubordinateGuard(Option<Subordinate>);

impl SubordinateGuard {
    fn enter(subordinate: Subordinate) -> Self {
        SubordinateGuard(SUBORDINATE.with(|slot| slot.borrow_mut().replace(subordinate)))
    }
}

impl Drop for SubordinateGuard {
    fn drop(&mut self) {
        SUBORDINATE.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

fn rendering_cte_body() -> bool {
    CTE_BODIES.with(|bodies| !bodies.borrow().is_empty())
}

fn rendering_body_of(name: &str) -> bool {
    CTE_BODIES.with(|bodies| bodies.borrow().iter().any(|body| body == name))
}

/// Marks a CTE's body as being rendered, restoring the stack after.
struct CteBodyGuard(Option<String>);

impl CteBodyGuard {
    fn enter(name: &str) -> Self {
        let previous = CTE_BODIES.with(|bodies| {
            let mut bodies = bodies.borrow_mut();
            bodies.push(name.to_string());
            (bodies.len() > 1).then(|| bodies[bodies.len() - 2].clone())
        });
        CteBodyGuard(previous)
    }
}

impl Drop for CteBodyGuard {
    fn drop(&mut self) {
        CTE_BODIES.with(|bodies| {
            bodies.borrow_mut().pop();
        });
    }
}

fn column(name: &str, qualified: bool) -> String {
    let renamed = CTE_SCOPE.with(|scope| scope.borrow().labels.get(name).cloned());
    let name = renamed.as_deref().unwrap_or(name);
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

/// The window calls an expression reads, in order of appearance.
fn window_calls<'a>(expr: &'a ScalarExpr, out: &mut Vec<&'a WindowCall>) {
    match expr {
        ScalarExpr::Window(call) => out.push(call),
        ScalarExpr::Unary { expr, .. }
        | ScalarExpr::Cast { expr, .. }
        | ScalarExpr::Extract { expr, .. }
        | ScalarExpr::IsNull { expr, .. } => window_calls(expr, out),
        ScalarExpr::Binary { left, right, .. } => {
            window_calls(left, out);
            window_calls(right, out);
        }
        ScalarExpr::Function { args, .. } => {
            for arg in args {
                window_calls(arg, out);
            }
        }
        _ => {}
    }
}

/// Whether a window function ignores its frame: a clause with only such
/// calls prints `ROWS UNBOUNDED PRECEDING`, as PostgreSQL rewrites it.
/// `LAG`/`LEAD` read individual rows and keep the default frame's text.
fn frame_blind(func: &str) -> bool {
    matches!(
        func,
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" | "NTILE"
    )
}

/// A frame's bounds as written, in PostgreSQL's words: the short form
/// (`ROWS 3 PRECEDING`) when the clause wrote a start bound alone, the
/// `BETWEEN` form otherwise — even where the bounds are the default's.
fn frame_bounds_text(frame: &FrameSpec) -> String {
    let units = match frame.units {
        WindowFrameUnits::Rows => "ROWS",
        WindowFrameUnits::Range => "RANGE",
        WindowFrameUnits::Groups => "GROUPS",
    };
    let offset = |e: &ScalarExpr| match (&frame.units, e) {
        // PostgreSQL types a `ROWS`/`GROUPS` offset as a bigint constant; a
        // `RANGE` offset keeps the ordering key's own type.
        (WindowFrameUnits::Range, other) => deparse_scalar(other, false),
        (_, ScalarExpr::Literal(Value::Int(n))) => format!("'{n}'::bigint"),
        (_, other) => deparse_scalar(other, false),
    };
    let bound = |b: &FrameBound| -> String {
        match b {
            FrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
            FrameBound::Preceding(e) => format!("{} PRECEDING", offset(e)),
            FrameBound::CurrentRow => "CURRENT ROW".to_string(),
            FrameBound::Following(e) => format!("{} FOLLOWING", offset(e)),
            FrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
        }
    };
    let mut text = if frame.between {
        format!(
            "{units} BETWEEN {} AND {}",
            bound(&frame.start),
            bound(&frame.end)
        )
    } else {
        format!("{units} {}", bound(&frame.start))
    };
    text.push_str(match frame.exclude {
        FrameExclude::NoOthers => "",
        FrameExclude::CurrentRow => " EXCLUDE CURRENT ROW",
        FrameExclude::Group => " EXCLUDE GROUP",
        FrameExclude::Ties => " EXCLUDE TIES",
    });
    text
}

/// The columns a `UNION`'s dedup groups by: the left input's output
/// columns, named for the relation they read.
fn setop_group_keys(plan: &LogicalPlan) -> Vec<String> {
    let LogicalPlan::Select {
        table_name,
        table_alias,
        projection,
        ..
    } = plan
    else {
        return Vec::new();
    };
    let bare = |name: &str| name.rsplit('.').next().unwrap_or(name).to_string();
    let prefix = relation_name(table_alias.as_deref().unwrap_or(table_name));
    projection
        .iter()
        .map(|item| match item {
            ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
                format!("{prefix}.{}", bare(c))
            }
            ProjectionItem::Expr { expr, alias } => match (expr, alias) {
                (ScalarExpr::Column(c), _) => format!("{prefix}.{}", bare(c)),
                (_, Some(alias)) => alias.clone(),
                (expr, None) => deparse_scalar(expr, false),
            },
            other => projection_text(other, false),
        })
        .collect()
}

/// A window key expression: a bare column, or parenthesized, as
/// PostgreSQL's `Sort Key` and window texts print them.
fn window_expr_text(expr: &ScalarExpr, qualified: bool) -> String {
    match expr {
        ScalarExpr::Column(name) => column(name, qualified),
        other => format!("({})", deparse_scalar(other, qualified)),
    }
}

/// The columns a CTE's body yields, as `(output name, stored reference)`
/// pairs — what a reference to that column prints as when inlined.
fn cte_outputs(plan: &LogicalPlan) -> Vec<(String, String)> {
    match plan {
        LogicalPlan::Renamed { input, columns } => {
            let mut outputs = cte_outputs(input);
            for (output, column_name) in outputs.iter_mut().zip(columns) {
                *output = (column_name.clone(), column_name.clone());
            }
            outputs
        }
        LogicalPlan::ViewCheckOption { query, .. } => cte_outputs(query),
        LogicalPlan::Select {
            projection,
            table_name,
            table_alias,
            ..
        } => {
            // A bare column is the body's own relation's column.
            let prefix = table_alias.as_deref().unwrap_or(table_name);
            let qualified = |name: &str| {
                if name.contains('.') {
                    name.to_string()
                } else {
                    format!("{prefix}.{name}")
                }
            };
            projection
                .iter()
                .map(|item| match item {
                    ProjectionItem::Column(c) => {
                        (c.rsplit('.').next().unwrap_or(c).to_string(), qualified(c))
                    }
                    ProjectionItem::AliasedColumn(c, alias) => (alias.clone(), qualified(c)),
                    ProjectionItem::Expr {
                        expr: ScalarExpr::Column(c),
                        alias,
                    } => (
                        alias
                            .clone()
                            .unwrap_or_else(|| c.rsplit('.').next().unwrap_or(c).to_string()),
                        qualified(c),
                    ),
                    ProjectionItem::Expr { expr, alias } => (
                        alias.clone().unwrap_or_else(|| "?column?".to_string()),
                        format!("({})", deparse_scalar(expr, true)),
                    ),
                    other => (projection_text(other, false), projection_text(other, true)),
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Whether PostgreSQL can pull a CTE's body into the query that reads
/// it: a limit, offset or distinct on the body's own query is a barrier.
fn cte_pullable(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Select {
            distinct,
            distinct_on,
            ..
        } => !*distinct && distinct_on.is_empty(),
        LogicalPlan::Renamed { input, .. } | LogicalPlan::ViewCheckOption { query: input, .. } => {
            cte_pullable(input)
        }
        _ => true,
    }
}

/// Whether a CTE must be materialized: it reads itself (recursive), its
/// body modifies data, or the query reads it more than once — PostgreSQL
/// inlines only a single-use, side-effect-free, non-recursive CTE.
fn cte_materialized(
    plan: &LogicalPlan,
    name: &str,
    references: usize,
    hinted: Option<bool>,
) -> bool {
    plan_cte_references(plan, name) > 0
        || plan_contains_dml(plan)
        // An explicit `AS [NOT] MATERIALIZED` decides; otherwise a CTE
        // read more than once materializes.
        || hinted.unwrap_or(references != 1)
}

/// Whether a plan runs a data-modifying statement (which is never inlined).
fn plan_contains_dml(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Merge { .. } => true,
        LogicalPlan::Renamed { input, .. } | LogicalPlan::ViewCheckOption { query: input, .. } => {
            plan_contains_dml(input)
        }
        LogicalPlan::SetOp { left, right, .. } => {
            plan_contains_dml(left) || plan_contains_dml(right)
        }
        LogicalPlan::RecursiveCte {
            seed,
            recursive_term,
            ..
        } => plan_contains_dml(seed) || plan_contains_dml(recursive_term),
        LogicalPlan::With { ctes, body, .. } => {
            ctes.iter().any(|(_, plan)| plan_contains_dml(plan)) || plan_contains_dml(body)
        }
        LogicalPlan::Select { ctes, .. } => ctes.iter().any(|(_, plan)| plan_contains_dml(plan)),
        _ => false,
    }
}

/// The references a plan makes to a CTE: FROM and join members at every
/// level, plus the subqueries its conditions read.
fn plan_cte_references(plan: &LogicalPlan, name: &str) -> usize {
    match plan {
        LogicalPlan::Select {
            table_name,
            joins,
            ctes,
            projection,
            filter,
            having,
            ..
        } => {
            let mut count = (table_name == name) as usize;
            for join in joins {
                if join.table_name == name {
                    count += 1;
                }
                if let Some(lateral) = &join.lateral {
                    count += plan_cte_references(lateral, name);
                }
                if let Some(sub) = &join.semi_subquery {
                    count += plan_cte_references(sub, name);
                }
            }
            for (other, plan) in ctes {
                if other != name {
                    count += plan_cte_references(plan, name);
                }
            }
            let mut subplans = Subplans::default();
            collect_subplans(
                projection,
                &[],
                filter.as_ref(),
                having.as_ref(),
                &mut subplans,
            );
            for (plan, _) in subplans.collected() {
                count += plan_cte_references(plan, name);
            }
            count
        }
        LogicalPlan::Renamed { input, .. } | LogicalPlan::ViewCheckOption { query: input, .. } => {
            plan_cte_references(input, name)
        }
        LogicalPlan::SetOp { left, right, .. } => {
            plan_cte_references(left, name) + plan_cte_references(right, name)
        }
        LogicalPlan::RecursiveCte {
            seed,
            recursive_term,
            ..
        } => plan_cte_references(seed, name) + plan_cte_references(recursive_term, name),
        LogicalPlan::With { ctes, body, .. } => {
            ctes.iter()
                .filter(|(other, _)| other != name)
                .map(|(_, plan)| plan_cte_references(plan, name))
                .sum::<usize>()
                + plan_cte_references(body, name)
        }
        LogicalPlan::Update {
            table_name,
            joins,
            filter,
            ..
        }
        | LogicalPlan::Delete {
            table_name,
            joins,
            filter,
            ..
        } => {
            (table_name == name) as usize
                + joins.iter().filter(|join| join.table_name == name).count()
                + dml_filter_references(filter.as_ref(), name)
        }
        LogicalPlan::Insert {
            table_name, source, ..
        } => {
            (table_name == name) as usize
                + source.as_ref().map_or(0, |p| plan_cte_references(p, name))
        }
        LogicalPlan::Merge {
            table_name, source, ..
        } => (table_name == name) as usize + plan_cte_references(source, name),
        _ => 0,
    }
}

/// The references a statement's filter subqueries make to a CTE.
fn dml_filter_references(filter: Option<&FilterExpr>, name: &str) -> usize {
    let Some(filter) = filter else {
        return 0;
    };
    let mut subplans = Subplans::default();
    number_filter_subplans(filter, &mut subplans);
    subplans
        .collected()
        .iter()
        .map(|(plan, _)| plan_cte_references(plan, name))
        .sum()
}

/// The key two calls share when their `OVER` is the same window:
/// partition, order and frame as written.
/// The window layers of a query, innermost first: clauses sharing an
/// ordering stack in the written order (the first innermost); different
/// orderings stack with the last written innermost, and `OVER ()`
/// outermost — as PostgreSQL plans them.
fn window_layer_order<'a>(
    calls: &[&'a WindowCall],
    qualified: bool,
) -> (Vec<WindowLayer<'a>>, Vec<usize>) {
    let mut layers: Vec<WindowLayer> = Vec::new();
    for call in calls {
        let key = window_layer_key(call, qualified);
        match layers.iter_mut().find(|layer| layer.key == key) {
            Some(layer) => layer.calls.push(call),
            None => layers.push(WindowLayer {
                calls: vec![call],
                key,
            }),
        }
    }
    let mut clusters: Vec<Vec<usize>> = Vec::new();
    for (i, layer) in layers.iter().enumerate() {
        let same = clusters.iter_mut().find(|cluster| {
            let base = &layers[cluster[0]];
            base.window().partition_by == layer.window().partition_by
                && base.window().order_by == layer.window().order_by
        });
        match same {
            Some(cluster) => cluster.push(i),
            None => clusters.push(vec![i]),
        }
    }
    let (unordered, ordered): (Vec<_>, Vec<_>) = clusters
        .into_iter()
        .partition(|cluster| layers[cluster[0]].is_unordered());
    let order: Vec<usize> = ordered
        .into_iter()
        .rev()
        .chain(unordered)
        .flatten()
        .collect();
    (layers, order)
}

/// PostgreSQL names presorted columns without their direction
/// (`Presorted Key: v`).
fn presorted_keys_text(keys: &[String]) -> Vec<String> {
    keys.iter()
        .map(|key| key.strip_suffix(" DESC").unwrap_or(key).to_string())
        .collect()
}

/// The bare column a query's ordering wants its scan to read an index by,
/// and whether that read runs backward: the innermost window layer's
/// ordering first — its partition columns first, then its ordering keys, as
/// the `WindowAgg` nearest the scan needs them — else the query's own
/// `ORDER BY`. PostgreSQL serves the lower consumer and sorts above it.
/// `None` when neither names a column an order-preserving index read could
/// serve.
pub(crate) fn ordering_index_column(
    sort: &[crate::plan_types::SortKey],
    projection: &[crate::plan_types::ProjectionItem],
) -> Option<(String, bool)> {
    use crate::plan_types::{ProjectionItem, SortTarget};
    let mut calls: Vec<&WindowCall> = Vec::new();
    for item in projection {
        if let ProjectionItem::Expr { expr, .. } = item {
            window_calls(expr, &mut calls);
        }
    }
    for key in sort {
        if let SortTarget::Expr(expr) = &key.target {
            window_calls(expr, &mut calls);
        }
    }
    if !calls.is_empty() {
        let (layers, order) = window_layer_order(&calls, true);
        let window = layers[*order.first()?].window();
        let leading = match window.partition_by.first() {
            Some(expr) => Some((expr, true, None)),
            None => window.order_by.first().map(|(e, asc, nf)| (e, *asc, *nf)),
        };
        if let Some((ScalarExpr::Column(name), ascending, nulls_first)) = leading
            && crate::index_keys::index_serves_ordering(ascending, nulls_first)
        {
            return Some((name.clone(), !ascending));
        }
    }
    let key = sort.first()?;
    if !crate::index_keys::index_serves_ordering(key.ascending, key.nulls_first) {
        return None;
    }
    let name = match &key.target {
        SortTarget::Name(name) => Some(name.clone()),
        SortTarget::Output(i) => match projection.get(*i) {
            Some(ProjectionItem::Column(c)) | Some(ProjectionItem::AliasedColumn(c, _)) => {
                Some(c.clone())
            }
            _ => None,
        },
        _ => None,
    };
    name.map(|name| (name, !key.ascending))
}

fn window_layer_key(call: &WindowCall, qualified: bool) -> String {
    format!(
        "{}|{}|{}",
        call.partition_by
            .iter()
            .map(|e| window_expr_text(e, qualified))
            .collect::<Vec<_>>()
            .join(","),
        call.order_by
            .iter()
            .map(|k| format!("{}{}", window_expr_text(&k.0, qualified), k.1))
            .collect::<Vec<_>>()
            .join(","),
        call.frame
            .as_ref()
            .map(frame_bounds_text)
            .unwrap_or_default(),
    )
}

/// A window call as a plan's keys print it: `rank() OVER w1`.
fn window_call_text(
    call: &WindowCall,
    windows: &std::collections::HashMap<String, String>,
    qualified: bool,
) -> String {
    let args: Vec<String> = call
        .args
        .iter()
        .map(|arg| deparse_scalar(arg, qualified))
        .collect();
    let name = windows
        .get(&window_layer_key(call, qualified))
        .cloned()
        .unwrap_or_else(|| "(?)".to_string());
    format!(
        "{}({}) OVER {}",
        call.func.to_ascii_lowercase(),
        args.join(", "),
        name
    )
}

/// One `WindowAgg` layer: the calls whose `OVER` shares partition, order
/// and frame, in the order written.
struct WindowLayer<'a> {
    calls: Vec<&'a WindowCall>,
    key: String,
}

impl<'a> WindowLayer<'a> {
    fn window(&self) -> &'a WindowCall {
        self.calls[0]
    }

    /// The ordering the layer's input must be in: partition, then order.
    fn sort_keys(&self, qualified: bool) -> Vec<String> {
        let window = self.window();
        let mut keys: Vec<String> = window
            .partition_by
            .iter()
            .map(|e| window_expr_text(e, qualified))
            .collect();
        keys.extend(window.order_by.iter().map(|key| {
            let mut text = window_expr_text(&key.0, qualified);
            if !key.1 {
                text.push_str(" DESC");
            }
            match (key.2, key.1) {
                (Some(true), true) => text.push_str(" NULLS FIRST"),
                (Some(false), false) => text.push_str(" NULLS LAST"),
                _ => {}
            }
            text
        }));
        keys
    }

    /// Whether the layer's `OVER ()` names no ordering at all.
    fn is_unordered(&self) -> bool {
        let window = self.window();
        window.partition_by.is_empty() && window.order_by.is_empty()
    }

    /// The name the layer's `Window:` line prints: the named window's, or
    /// `wN` by position.
    fn display_name(&self, number: usize) -> String {
        self.calls
            .iter()
            .find_map(|call| call.name.clone())
            .unwrap_or_else(|| format!("w{number}"))
    }

    /// The `wN AS (...)` text of the layer's `Window:` line.
    fn text(&self, number: usize, qualified: bool) -> String {
        let window = self.window();
        let partition: Vec<String> = window
            .partition_by
            .iter()
            .map(|e| window_expr_text(e, qualified))
            .collect();
        // PostgreSQL's `Window:` line names the ordering's expressions
        // without their direction or NULLS placement.
        let order: Vec<String> = window
            .order_by
            .iter()
            .map(|key| window_expr_text(&key.0, qualified))
            .collect();
        let mut parts: Vec<String> = Vec::new();
        if !partition.is_empty() {
            parts.push(format!("PARTITION BY {}", partition.join(", ")));
        }
        if !order.is_empty() {
            parts.push(format!("ORDER BY {}", order.join(", ")));
        }
        let frame = if self.calls.iter().all(|c| frame_blind(&c.func)) {
            "ROWS UNBOUNDED PRECEDING".to_string()
        } else {
            window
                .frame
                .as_ref()
                .map(frame_bounds_text)
                .unwrap_or_default()
        };
        if !frame.is_empty() {
            parts.push(frame);
        }
        format!("{} AS ({})", self.display_name(number), parts.join(" "))
    }
}

fn sort_key_text(
    key: &SortKey,
    projection: &[ProjectionItem],
    qualified: bool,
    windows: &std::collections::HashMap<String, String>,
) -> String {
    let mut text = match &key.target {
        SortTarget::Output(i) => match projection.get(*i) {
            Some(item @ (ProjectionItem::Column(_) | ProjectionItem::AliasedColumn(..))) => {
                projection_text(item, qualified)
            }
            Some(ProjectionItem::Expr {
                expr: ScalarExpr::Window(call),
                ..
            }) => format!("({})", window_call_text(call, windows, qualified)),
            Some(item) => format!("({})", projection_text(item, qualified)),
            None => format!("{}", i + 1),
        },
        SortTarget::Name(n) => column(n, qualified),
        SortTarget::Expr(ScalarExpr::Window(call)) => {
            format!("({})", window_call_text(call, windows, qualified))
        }
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

/// A relation's rows' width.
fn width_of(table: &nodus_catalog::TableDescriptor) -> u32 {
    table.columns.iter().map(|c| type_width(&c.data_type)).sum()
}

/// PostgreSQL's `Sampling:` detail for a sampled scan — the method, the
/// percentage as the `real` it is read as, and any `REPEATABLE` seed —
/// together with the row estimate the percentage scales.
fn sampling_detail(spec: &crate::SampleSpec, rows: f64) -> (String, f64) {
    let argument = match &spec.percent {
        crate::ScalarExpr::Literal(v) => format!("'{}'::real", render(v)),
        other => deparse_scalar(other, false),
    };
    let mut detail = format!("{} ({argument})", spec.method);
    if let Some(seed) = &spec.seed {
        let seed_text = match seed {
            crate::ScalarExpr::Literal(v) => format!("'{}'::double precision", render(v)),
            other => deparse_scalar(other, false),
        };
        detail.push_str(&format!(" REPEATABLE ({seed_text})"));
    }
    let percent = match &spec.percent {
        crate::ScalarExpr::Literal(v) => crate::value::render(v).parse::<f64>().ok(),
        _ => None,
    };
    let sampled = match percent {
        Some(percent) if rows > 0.0 => (rows * percent / 100.0).max(1.0).round(),
        _ => rows,
    };
    (detail, sampled)
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

/// The SQL type of an array literal's elements, when their values show it.
fn array_element_type(value: &Value) -> Option<&'static str> {
    match value {
        Value::Int(_) => Some("integer"),
        Value::Float(_) => Some("double precision"),
        Value::Numeric(_) => Some("numeric"),
        Value::Bool(_) => Some("boolean"),
        Value::Text(_) => Some("text"),
        _ => None,
    }
}

/// One element of an array literal, as PostgreSQL prints it (quoted when it
/// holds what an array literal treats specially).
fn array_element(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Text(text) => {
            let quoted = text.is_empty()
                || text.contains([',', '{', '}', '"', '\\', ' '])
                || text.eq_ignore_ascii_case("null");
            let escaped = text.replace('\\', "\\\\").replace('"', "\\\"");
            if quoted {
                format!("\"{escaped}\"")
            } else {
                escaped
            }
        }
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
        B::JsonPathExists => "@?",
        B::JsonPathMatch => "@@",
        B::TsPhrase => "<->",
        B::Overlap => "&&",
    }
}

fn operand(op: &Operand, qualified: bool) -> String {
    match op {
        Operand::Literal(v) => literal(v),
        Operand::Ident(c) => column(c, qualified),
    }
}

/// A plan's subqueries, numbered as PostgreSQL numbers them: `InitPlan N`
/// for a subquery evaluated once, `SubPlan N` for one read per outer row —
/// `hashed` when a set of values is hashed and probed. The executor caches
/// both kinds per statement; a plan's filter references them by number.
/// Keyed by the subquery plan's JSON, the same fingerprint the cache uses.
#[derive(Default)]
pub(crate) struct Subplans {
    entries: Vec<(LogicalPlan, SubplanEntry)>,
}

#[derive(Clone)]
pub(crate) struct SubplanEntry {
    /// `InitPlan 1`, `SubPlan 2`.
    pub(crate) name: String,
    pub(crate) hashed: bool,
}

fn fingerprint(plan: &LogicalPlan) -> String {
    serde_json::to_string(plan).unwrap_or_default()
}

impl Subplans {
    fn number(&mut self, plan: &LogicalPlan, init: bool, hashed: bool) {
        let key = fingerprint(plan);
        if self.entries.iter().any(|(p, _)| fingerprint(p) == key) {
            return;
        }
        let name = format!(
            "{}{}",
            if init { "InitPlan " } else { "SubPlan " },
            self.entries.len() + 1
        );
        self.entries
            .push((plan.clone(), SubplanEntry { name, hashed }));
    }

    pub(crate) fn entry(&self, plan: &LogicalPlan) -> Option<&SubplanEntry> {
        let key = fingerprint(plan);
        self.entries
            .iter()
            .find(|(p, _)| fingerprint(p) == key)
            .map(|(_, entry)| entry)
    }

    /// How a filter names the subquery: `InitPlan 1`, `hashed SubPlan 2`.
    pub(crate) fn reference(&self, plan: &LogicalPlan) -> Option<String> {
        let entry = self.entry(plan)?;
        Some(if entry.hashed {
            format!("hashed {}", entry.name)
        } else {
            entry.name.clone()
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The collected subqueries in number order, with their plans.
    pub(crate) fn collected(&self) -> &[(LogicalPlan, SubplanEntry)] {
        &self.entries
    }
}

/// A select's subqueries, by the plan they read, in the order PostgreSQL
/// numbers them (the projection's first, then the filter's).
fn collect_subplans(
    projection: &[crate::plan_types::ProjectionItem],
    hoisted: &[(LogicalPlan, bool)],
    filter: Option<&FilterExpr>,
    having: Option<&FilterExpr>,
    subplans: &mut Subplans,
) {
    for plan in hoisted.iter().map(|(plan, _)| plan) {
        subplans.number(plan, true, false);
    }
    for item in projection {
        if let crate::plan_types::ProjectionItem::Subquery { plan, .. } = item {
            subplans.number(plan, true, false);
        }
    }
    if let Some(filter) = filter {
        number_filter_subplans(filter, subplans);
    }
    if let Some(having) = having {
        number_filter_subplans(having, subplans);
    }
}

/// Numbers the subqueries a condition reads: a scalar one is an `InitPlan`
/// when it stands alone (PostgreSQL evaluates it once), a set comparison a
/// `SubPlan` — hashed when the values can be hashed.
fn number_filter_subplans(filter: &FilterExpr, subplans: &mut Subplans) {
    match filter {
        FilterExpr::And(left, right) | FilterExpr::Or(left, right) => {
            number_filter_subplans(left, subplans);
            number_filter_subplans(right, subplans);
        }
        FilterExpr::Not(inner) => number_filter_subplans(inner, subplans),
        FilterExpr::CompareSubquery { subquery, .. } => {
            let init = !crate::filter_eval::subquery_is_correlated(subquery);
            subplans.number(subquery, init, false);
        }
        FilterExpr::Exists { subquery, .. } => {
            let init = !crate::filter_eval::subquery_is_correlated(subquery);
            subplans.number(subquery, init, false);
        }
        FilterExpr::InSubquery { subquery, .. } => {
            let uncorrelated = !crate::filter_eval::subquery_is_correlated(subquery);
            subplans.number(subquery, false, uncorrelated);
        }
        FilterExpr::QuantifiedSubquery {
            subquery, all, op, ..
        } => {
            let uncorrelated = !crate::filter_eval::subquery_is_correlated(subquery);
            let hashable = !*all && matches!(op, crate::plan_types::ScalarBinaryOp::Eq);
            subplans.number(subquery, false, uncorrelated && hashable);
        }
        _ => {}
    }
}

/// The `WHERE`'s uncorrelated `EXISTS` conjuncts — which PostgreSQL
/// evaluates once, as a `Result` node's one-time filter — and the rest of
/// the condition.
fn split_uncorrelated_exists(
    filter: Option<&FilterExpr>,
) -> (Vec<(LogicalPlan, bool)>, Option<FilterExpr>) {
    let Some(filter) = filter else {
        return (Vec::new(), None);
    };
    let mut hoisted = Vec::new();
    let mut remaining = Vec::new();
    for conjunct in crate::index_keys::conjuncts(filter) {
        let (subquery, negated) = match conjunct {
            FilterExpr::Exists { subquery, negated } => (&**subquery, *negated),
            FilterExpr::Not(inner) => match &**inner {
                FilterExpr::Exists {
                    subquery,
                    negated: false,
                } => (&**subquery, true),
                _ => {
                    remaining.push(conjunct.clone());
                    continue;
                }
            },
            _ => {
                remaining.push(conjunct.clone());
                continue;
            }
        };
        if crate::filter_eval::subquery_is_correlated(subquery) {
            remaining.push(conjunct.clone());
        } else {
            hoisted.push((subquery.clone(), negated));
        }
    }
    (hoisted, crate::joins::conjunction(&remaining))
}

/// A condition as PostgreSQL prints it in a plan: `((a > 1) AND (b = 'x'::text))`.
pub(crate) fn deparse_filter(filter: &FilterExpr, qualified: bool) -> String {
    deparse_filter_with(filter, qualified, None)
}

/// As [`deparse_filter`], naming subqueries by the numbers `subplans`
/// assigned them (without it, the inline `(SubPlan 1)` form).
pub(crate) fn deparse_filter_with(
    filter: &FilterExpr,
    qualified: bool,
    subplans: Option<&Subplans>,
) -> String {
    let f = |e: &FilterExpr| deparse_filter_with(e, qualified, subplans);
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
        } => {
            // A list of constants is one array test, as PostgreSQL plans it
            // (`a = ANY ('{1,2}'::integer[])`).
            let op = if *negated { "<> ALL" } else { "= ANY" };
            // A NULL element takes its siblings' type.
            if let Some(element_type) = list
                .iter()
                .map(|item| match item {
                    Operand::Literal(Value::Null) => Some(None),
                    Operand::Literal(value) => array_element_type(value).map(Some),
                    Operand::Ident(_) => None,
                })
                .collect::<Option<Vec<_>>>()
                .and_then(|types| {
                    let typed: Vec<&str> = types.into_iter().flatten().collect();
                    typed
                        .windows(2)
                        .all(|pair| pair[0] == pair[1])
                        .then(|| typed.first().copied())?
                })
            {
                let values = list
                    .iter()
                    .map(|item| match item {
                        Operand::Literal(value) => array_element(value),
                        Operand::Ident(_) => String::new(),
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                return format!(
                    "({} {} ('{{{}}}'::{}[]))",
                    column(left, qualified),
                    op,
                    values,
                    element_type
                );
            }
            format!(
                "({} {} ({}))",
                column(left, qualified),
                if *negated { "NOT IN" } else { "IN" },
                list.iter()
                    .map(|o| operand(o, qualified))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        FilterExpr::InSubquery {
            left,
            subquery,
            negated,
            ..
        } => {
            let reference = subplans
                .and_then(|s| s.reference(subquery))
                .unwrap_or_else(|| "SubPlan 1".to_string());
            if *negated {
                format!(
                    "(NOT (ANY ({} = ({reference}).col1)))",
                    column(left, qualified)
                )
            } else {
                format!("(ANY ({} = ({reference}).col1))", column(left, qualified))
            }
        }
        FilterExpr::CompareSubquery { left, op, subquery } => {
            // A one-time subplan names its single output column; one read
            // per row stands for its value.
            let reference = match subplans.and_then(|s| s.entry(subquery)) {
                Some(entry) if entry.name.starts_with("InitPlan") => {
                    format!("({}).col1", entry.name)
                }
                Some(entry) => format!("({})", entry.name),
                None => "(SubPlan 1)".to_string(),
            };
            format!(
                "({} {} {reference})",
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
        FilterExpr::Exists { subquery, negated } => {
            let reference = subplans
                .and_then(|s| s.reference(subquery))
                .unwrap_or_else(|| "SubPlan 1".to_string());
            if *negated {
                format!("(NOT EXISTS({reference}))")
            } else {
                format!("EXISTS({reference})")
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
        FilterExpr::QuantifiedSubquery {
            left,
            op,
            all,
            subquery,
        } => {
            let reference = subplans
                .and_then(|s| s.reference(subquery))
                .unwrap_or_else(|| "SubPlan 1".to_string());
            format!(
                "({} ({} {} ({reference}).col1))",
                if *all { "ALL" } else { "ANY" },
                deparse_scalar(left, qualified),
                binary_op(op),
            )
        }
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
        ScalarExpr::Function { name, args }
            if (name == "NEXTVAL" || name == crate::sequences::SERIAL) && args.len() == 1 =>
        {
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
        ScalarExpr::Window(call) => format!(
            "{}({}) OVER (?)",
            call.func.to_ascii_lowercase(),
            list(&call.args)
        ),
    }
}

/// A query as PostgreSQL pretty-prints a view's definition:
/// ` SELECT a,\n    b\n   FROM t\n  WHERE a > 0`. `None` for a plan it
/// cannot write back as SQL.
pub(crate) fn deparse_query(plan: &LogicalPlan) -> Option<String> {
    // A column alias list names the leading output columns.
    let plan = match plan {
        LogicalPlan::ViewCheckOption { query, .. } => query,
        other => other,
    };
    let (plan, renames): (&LogicalPlan, &[String]) = match plan {
        LogicalPlan::Renamed { input, columns } => (input, columns),
        other => (other, &[]),
    };
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
            .enumerate()
            .map(|(i, item)| {
                let (value, alias) = match item {
                    ProjectionItem::AliasedColumn(c, alias) => {
                        (column(c, qualified), Some(alias.clone()))
                    }
                    ProjectionItem::Expr { expr, alias } => {
                        (unwrap(deparse_scalar(expr, qualified)), alias.clone())
                    }
                    other => (unwrap(projection_text(other, qualified)), None),
                };
                match renames.get(i).cloned().or(alias) {
                    Some(alias) if alias != value => format!("{value} AS {alias}"),
                    _ => value,
                }
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
    let mut exists_clauses = Vec::new();
    for join in joins {
        // A semi/anti join came from `[NOT] EXISTS` in `WHERE`.
        if let (JoinType::Semi | JoinType::Anti, Some(sub)) = (&join.join_type, &join.semi_subquery)
            && let LogicalPlan::Select {
                table_name: inner_name,
                table_alias: inner_alias,
                joins: inner_joins,
                filter: inner_filter,
                ..
            } = &**sub
        {
            let mut inner = format!(
                "SELECT 1 FROM {}",
                scan_label_name_qualified(inner_name, inner_alias.as_deref())
            );
            for inner_join in inner_joins {
                inner.push_str(&format!(
                    "\n     JOIN {}",
                    scan_label_name_qualified(
                        &inner_join.table_name,
                        inner_join.table_alias.as_deref()
                    )
                ));
                if let Some(condition) = &inner_join.condition {
                    inner.push_str(&format!(" ON {}", deparse_filter(condition, true)));
                }
            }
            let mut conditions = Vec::new();
            if let Some(condition) = &join.condition {
                conditions.push(unwrap(deparse_filter(condition, true)));
            }
            if let Some(inner_filter) = inner_filter {
                conditions.push(unwrap(deparse_filter(inner_filter, true)));
            }
            if !conditions.is_empty() {
                inner.push_str(&format!("\n  WHERE {}", conditions.join(" AND ")));
            }
            exists_clauses.push(format!(
                "{}EXISTS ({inner})",
                if matches!(join.join_type, JoinType::Anti) {
                    "NOT "
                } else {
                    ""
                }
            ));
            continue;
        }
        let kind = match join.join_type {
            JoinType::Inner => "JOIN",
            JoinType::LeftOuter => "LEFT JOIN",
            JoinType::RightOuter => "RIGHT JOIN",
            JoinType::FullOuter => "FULL JOIN",
            JoinType::Cross => "CROSS JOIN",
            JoinType::Semi | JoinType::Anti => "JOIN",
        };
        sql.push_str(&format!(
            "\n     {kind} {}",
            scan_label_name_qualified(&join.table_name, join.table_alias.as_deref())
        ));
        if let Some(condition) = &join.condition {
            sql.push_str(&format!(" ON {}", deparse_filter(condition, true)));
        }
    }
    let mut conditions = exists_clauses;
    if let Some(filter) = filter {
        conditions.push(unwrap(deparse_filter(filter, qualified)));
    }
    if !conditions.is_empty() {
        sql.push_str(&format!("\n  WHERE {}", conditions.join(" AND ")));
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
            .map(|k| sort_key_text(k, projection, qualified, &Default::default()))
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
    let shown = shown_relation(table_name);
    match alias {
        Some(alias) if alias != relation_name(table_name) => format!("{shown} {alias}"),
        _ => shown,
    }
}

/// A relation's name as a view's definition shows it: without its schema
/// when that is on the search path.
fn shown_relation(table_name: &str) -> String {
    match table_name.rsplit_once('.') {
        Some((schema, name))
            if crate::search_path::existing_search_path()
                .iter()
                .any(|s| *s == schema.trim_matches('"')) =>
        {
            name.to_string()
        }
        _ => table_name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The JSON plan keeps PostgreSQL's shapes: subplans and CTE blocks as
    /// `Plans` entries, aggregate strategies, an `Append`'s members, a
    /// `ModifyTable`'s alias, and a recursive CTE's working table.
    #[test]
    fn plans_serialize_postgresqls_json_shape() {
        let (sql, _) = crate::constraint_tests::session();
        sql("create table t (a int, b int)").unwrap();
        sql("insert into t values (1, 2), (2, 3)").unwrap();
        let explain = |query: &str| {
            let out = sql(&format!("explain (format json, costs off) {query}")).unwrap();
            crate::render(&out.rows[0].values[0])
        };

        let plan = explain("select count(*) from t");
        assert!(plan.contains("\"Strategy\": \"Plain\""), "{plan}");
        assert!(plan.contains("\"Partial Mode\": \"Simple\""), "{plan}");

        let plan = explain("select (select max(a) from t) from t");
        assert!(
            plan.contains("\"Parent Relationship\": \"InitPlan\""),
            "{plan}"
        );
        assert!(plan.contains("\"Subplan Name\": \"InitPlan 1\""), "{plan}");

        let plan =
            explain("with c as (select a from t) select * from c c1 join c c2 on c1.a = c2.a");
        assert!(plan.contains("\"Subplan Name\": \"CTE c\""), "{plan}");
        assert!(plan.contains("\"Node Type\": \"CTE Scan\""), "{plan}");

        let plan = explain("update t set b = 0 where a = 1");
        assert!(plan.contains("\"Operation\": \"Update\""), "{plan}");
        assert!(plan.contains("\"Alias\": \"t\""), "{plan}");

        let plan = explain(
            "with recursive r(n) as (select 1 union all select n + 1 from r where n < 5) select * from r",
        );
        assert!(
            plan.contains("\"Node Type\": \"Recursive Union\""),
            "{plan}"
        );
        assert!(plan.contains("\"Node Type\": \"WorkTable Scan\""), "{plan}");

        sql("create table pt (id int) partition by range (id)").unwrap();
        sql("create table pt1 partition of pt for values from (0) to (10)").unwrap();
        sql("create table pt2 partition of pt for values from (10) to (20)").unwrap();
        let plan = explain("select * from pt");
        assert!(plan.contains("\"Subplans Removed\": 0"), "{plan}");
        assert!(
            plan.contains("\"Parent Relationship\": \"Member\""),
            "{plan}"
        );
    }

    /// The `WHERE` of a query, unconverted.
    fn where_filter(sql: &str) -> FilterExpr {
        let mut statements = nodus_sql::parse_sql(sql).unwrap();
        let sqlparser::ast::Statement::Query(query) = statements.remove(0) else {
            panic!("a query");
        };
        let sqlparser::ast::SetExpr::Select(select) = *query.body else {
            panic!("a select");
        };
        crate::planner::parse_predicates(&select.selection, &[])
            .unwrap()
            .unwrap()
    }

    #[test]
    fn uncorrelated_existentials_are_evaluated_once() {
        let (hoisted, rest) = split_uncorrelated_exists(Some(&where_filter(
            "select * from t1 where exists (select 1 from t2)",
        )));
        assert_eq!(hoisted.len(), 1);
        assert!(!hoisted[0].1 && rest.is_none());

        let (hoisted, _) = split_uncorrelated_exists(Some(&where_filter(
            "select * from t1 where not exists (select 1 from t2)",
        )));
        assert_eq!(hoisted.len(), 1);
        assert!(hoisted[0].1);

        // Other conditions stay on the scan.
        let (hoisted, rest) = split_uncorrelated_exists(Some(&where_filter(
            "select * from t1 where exists (select 1 from t2 where t2.a = 5) and b = 'x'",
        )));
        assert_eq!(hoisted.len(), 1);
        assert_eq!(
            rest.as_ref().map(|f| deparse_filter(f, true)).as_deref(),
            Some("(b = 'x'::text)")
        );

        // A correlated existential is not one-time.
        let (hoisted, rest) = split_uncorrelated_exists(Some(&where_filter(
            "select * from t1 where exists (select 1 from t2 where t2.a = t1.a)",
        )));
        assert!(hoisted.is_empty() && rest.is_some());
    }

    #[test]
    fn a_subplans_own_columns_print_bare() {
        let text = "(u2.a = u1.a)";
        assert_eq!(strip_qualified(text, &["u2".to_string()]), "(a = u1.a)");
        // Inside a literal, the name is not a column reference.
        let text = "(b = 'u2.a'::text)";
        assert_eq!(strip_qualified(text, &["u2".to_string()]), text);
    }
}
