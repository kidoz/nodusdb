//! Join strategies: the equalities a hash join hashes, and the conditions
//! each side applies alone. Shared by the executor and `EXPLAIN`, so a plan
//! shows the join the executor runs.

use crate::plan_types::{CompareOp, FilterExpr, JoinType, LogicalPlan, Operand, Predicate};

/// One side of a hashed equality: a bare column, or a value computed per
/// row (PostgreSQL hashes `e1.a + 1 = e2.a` on the expression).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HashKey {
    Column(String),
    Expr(crate::plan_types::ScalarExpr),
}

/// How a join's condition splits for a hash join: the equalities hashed
/// (`(left key, right key)` — bare columns or computed values), the
/// conditions only one side's rows must satisfy (which PostgreSQL pushes
/// into its scan), and the conditions over both sides, checked on each
/// matched pair.
#[derive(Debug, Default, Clone)]
pub(crate) struct JoinPlan {
    pub(crate) hash_pairs: Vec<(HashKey, HashKey)>,
    /// The original equalities, re-checked on each candidate pair so a
    /// hashed match means exactly what the condition says.
    pub(crate) pair_tests: Vec<FilterExpr>,
    pub(crate) left_only: Vec<FilterExpr>,
    pub(crate) right_only: Vec<FilterExpr>,
    pub(crate) join_filter: Vec<FilterExpr>,
}

/// Which side a qualified name belongs to: its prefix, as `a.x` names `a`.
fn prefix(name: &str) -> Option<&str> {
    name.rsplit_once('.').map(|(qualifier, _)| qualifier)
}

/// The side a column reference comes from, by the prefixes that name each
/// side (`None` when it names neither, or is unqualified).
fn side<'a>(name: &str, left: &[&'a str], right: &[&'a str]) -> Option<bool> {
    let qualifier = prefix(name)?;
    if right.contains(&qualifier) {
        return Some(false);
    }
    if left.contains(&qualifier) {
        return Some(true);
    }
    None
}

/// Splits `condition` for a hash join over the left and right sides named
/// by `left_prefixes` / `right_prefixes`. `None` when the condition is not
/// one (no equality between the sides).
pub(crate) fn hash_plan(
    condition: Option<&FilterExpr>,
    left_prefixes: &[&str],
    right_prefixes: &[&str],
) -> Option<JoinPlan> {
    let condition = condition?;
    let mut plan = JoinPlan::default();
    for conjunct in crate::index_keys::conjuncts(condition) {
        // An equality whose sides each come from one side of the join
        // hashes, whether a side is a bare column or a computed value.
        if let Some((left, right)) = equality_sides(conjunct, left_prefixes, right_prefixes) {
            plan.hash_pairs.push((left, right));
            plan.pair_tests.push(conjunct.clone());
            continue;
        }
        let mut refs = Vec::new();
        crate::filter_eval::filter_column_refs(conjunct, &mut refs);
        let mut on_left = false;
        let mut on_right = false;
        for reference in &refs {
            match side(reference, left_prefixes, right_prefixes) {
                Some(true) => on_left = true,
                Some(false) => on_right = true,
                None => {
                    on_left = true;
                    on_right = true;
                }
            }
        }
        if on_left && !on_right {
            plan.left_only.push(conjunct.clone());
        } else if on_right && !on_left {
            plan.right_only.push(conjunct.clone());
        } else {
            plan.join_filter.push(conjunct.clone());
        }
    }
    // `a = a` and the like are no hash pair.
    plan.hash_pairs.dedup();
    (!plan.hash_pairs.is_empty()).then_some(plan)
}

/// The `(left key, right key)` an equality conjunct hashes (left side
/// first, as PostgreSQL prints the condition), when each side's references
/// come from exactly one side of the join.
fn equality_sides(
    conjunct: &FilterExpr,
    left_prefixes: &[&str],
    right_prefixes: &[&str],
) -> Option<(HashKey, HashKey)> {
    let (left, right) = match conjunct {
        FilterExpr::Predicate(Predicate {
            left,
            op: CompareOp::Eq,
            right: Operand::Ident(right),
        }) => (
            crate::plan_types::ScalarExpr::Column(left.clone()),
            crate::plan_types::ScalarExpr::Column(right.clone()),
        ),
        FilterExpr::ExprCmp {
            left,
            op: CompareOp::Eq,
            right,
        } => (left.clone(), right.clone()),
        // A comparison of two computed sides parses as one scalar binary.
        FilterExpr::Scalar(crate::plan_types::ScalarExpr::Binary {
            op: crate::plan_types::ScalarBinaryOp::Eq,
            left,
            right,
        }) => ((**left).clone(), (**right).clone()),
        _ => return None,
    };
    let (left_side, right_side) = (
        scalar_side(&left, left_prefixes, right_prefixes),
        scalar_side(&right, left_prefixes, right_prefixes),
    );
    let key = |expr: &crate::plan_types::ScalarExpr| match expr {
        crate::plan_types::ScalarExpr::Column(name) => HashKey::Column(name.clone()),
        other => HashKey::Expr(other.clone()),
    };
    match (left_side, right_side) {
        (Some(true), Some(false)) => Some((key(&left), key(&right))),
        (Some(false), Some(true)) => Some((key(&right), key(&left))),
        _ => None,
    }
}

/// Which side a scalar expression's references belong to: `Some(true)` /
/// `Some(false)` when every reference is on one side (and at least one on
/// the side alone), `None` otherwise.
fn scalar_side(
    expr: &crate::plan_types::ScalarExpr,
    left_prefixes: &[&str],
    right_prefixes: &[&str],
) -> Option<bool> {
    let mut refs = Vec::new();
    crate::filter_eval::scalar_column_refs(expr, &mut refs);
    if refs.is_empty() {
        return None;
    }
    let mut on_left = false;
    let mut on_right = false;
    for reference in &refs {
        match side(reference, left_prefixes, right_prefixes) {
            Some(true) => on_left = true,
            Some(false) => on_right = true,
            None => return None,
        }
    }
    match (on_left, on_right) {
        (true, false) => Some(true),
        (false, true) => Some(false),
        _ => None,
    }
}

/// The equalities a `USING (...)` (or `NATURAL`) join matches on: one
/// `left.c = right.c` predicate per named column.
pub(crate) fn using_condition(
    columns: &[String],
    left_prefixes: &[String],
    right_prefixes: &[String],
) -> Option<FilterExpr> {
    let (left, right) = (left_prefixes.first()?, right_prefixes.first()?);
    let parts: Vec<FilterExpr> = columns
        .iter()
        .map(|column| {
            FilterExpr::Predicate(Predicate {
                left: format!("{left}.{column}"),
                op: CompareOp::Eq,
                right: Operand::Ident(format!("{right}.{column}")),
            })
        })
        .collect();
    conjunction(&parts)
}

/// The conditions of one side as a single filter, `None` when there are
/// none.
pub(crate) fn conjunction(parts: &[FilterExpr]) -> Option<FilterExpr> {
    let mut parts = parts.iter();
    let first = parts.next()?.clone();
    Some(parts.fold(first, |combined, next| {
        FilterExpr::And(Box::new(combined), Box::new(next.clone()))
    }))
}

/// The semi/anti joins a condition's `[NOT] EXISTS` / `IN` conjuncts
/// become, and the rest of the condition. Shared by `SELECT` and by
/// `UPDATE`/`DELETE`, whose plans carry the joins beside the target.
pub(crate) fn semi_joins(
    filter: Option<&FilterExpr>,
    outer_quals: &[String],
) -> (Vec<crate::Join>, Option<FilterExpr>) {
    let Some(filter) = filter else {
        return (Vec::new(), None);
    };
    let mut joins = Vec::new();
    let mut remaining = Vec::new();
    for conjunct in crate::index_keys::conjuncts(filter) {
        match semi_join(conjunct, outer_quals) {
            Some((join_type, subquery, condition)) => joins.push(crate::Join {
                only: false,
                table_name: String::new(),
                table_alias: None,
                condition: Some(condition),
                join_type,
                using_columns: Vec::new(),
                natural: false,
                table_fn: None,
                lateral: None,
                sample: None,
                semi_subquery: Some(subquery),
            }),
            None => remaining.push(conjunct.clone()),
        }
    }
    (joins, conjunction(&remaining))
}

/// The comparison an `ANY` operator stands for.
fn compare_of(op: &crate::plan_types::ScalarBinaryOp) -> Option<CompareOp> {
    Some(match op {
        crate::plan_types::ScalarBinaryOp::Eq => CompareOp::Eq,
        crate::plan_types::ScalarBinaryOp::NotEq => CompareOp::Ne,
        crate::plan_types::ScalarBinaryOp::Lt => CompareOp::Lt,
        crate::plan_types::ScalarBinaryOp::LtEq => CompareOp::Le,
        crate::plan_types::ScalarBinaryOp::Gt => CompareOp::Gt,
        crate::plan_types::ScalarBinaryOp::GtEq => CompareOp::Ge,
        _ => return None,
    })
}

/// The distinct qualifiers of a row's column names, in order.
pub(crate) fn qualifiers(names: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for name in names {
        if let Some((qualifier, _)) = name.rsplit_once('.')
            && !out.iter().any(|q| q == qualifier)
        {
            out.push(qualifier.to_string());
        }
    }
    out
}

/// The relations a plan's `FROM` reads, by the label each is scanned
/// under: its alias, else its name.
pub(crate) fn relation_labels(plan: &LogicalPlan) -> Vec<String> {
    let LogicalPlan::Select {
        table_name,
        table_alias,
        joins,
        ..
    } = plan
    else {
        return Vec::new();
    };
    std::iter::once(table_alias.clone().unwrap_or_else(|| table_name.clone()))
        .chain(joins.iter().map(|j| {
            j.table_alias
                .clone()
                .unwrap_or_else(|| j.table_name.clone())
        }))
        .collect()
}

/// The relations a plan's `FROM` reads, by the names that reach them: an
/// alias hides the table name.
pub(crate) fn relation_quals(plan: &LogicalPlan) -> Vec<String> {
    let LogicalPlan::Select {
        table_name,
        table_alias,
        joins,
        ..
    } = plan
    else {
        return Vec::new();
    };
    let mut quals = Vec::new();
    let mut add = |name: &str, alias: Option<&String>| match alias {
        Some(alias) => quals.push(alias.to_lowercase()),
        None => {
            quals.push(name.to_lowercase());
            if let Some(last) = name.rsplit('.').next() {
                quals.push(last.to_lowercase());
            }
        }
    };
    add(table_name, table_alias.as_ref());
    for join in joins {
        add(&join.table_name, join.table_alias.as_ref());
    }
    quals
}

/// The `[NOT] EXISTS (...)` / `IN (...)` conjuncts a semi/anti join can run:
/// the subquery's correlation predicates against the outer row become the
/// join's condition, and its inner side is the subquery with the projection
/// dropped — existence ignores it.
///
/// Returns the join type, the inner side, and the condition. `None` keeps
/// the subquery as a `SubPlan` predicate, as PostgreSQL does for shapes a
/// semi join cannot express (an uncorrelated subquery, an aggregate,
/// `NOT IN`'s NULL semantics, a LIMIT or OFFSET).
pub(crate) fn semi_join(
    conjunct: &FilterExpr,
    outer_quals: &[String],
) -> Option<(JoinType, Box<LogicalPlan>, FilterExpr)> {
    let (subquery, anti, in_key) = match conjunct {
        FilterExpr::Exists { subquery, negated } => (&**subquery, *negated, None),
        FilterExpr::InSubquery {
            left,
            subquery,
            negated: false,
            left_value: None,
        } => (
            &**subquery,
            false,
            Some((
                crate::plan_types::ScalarExpr::Column(left.clone()),
                crate::plan_types::ScalarBinaryOp::Eq,
            )),
        ),
        // `<expr> <op> ANY (<subquery>)` is the same comparison as a semi
        // join over the comparison written out; the left side may be
        // computed (`a + 1 in (...)`).
        FilterExpr::QuantifiedSubquery {
            left,
            op,
            subquery,
            all: false,
        } => (&**subquery, false, Some((left.clone(), op.clone()))),
        // `not (exists (...))` is the same predicate as `not exists (...)`.
        FilterExpr::Not(inner) => match &**inner {
            FilterExpr::Exists {
                subquery,
                negated: false,
            } => (&**subquery, true, None),
            _ => return None,
        },
        _ => return None,
    };
    let LogicalPlan::Select {
        joins,
        projection,
        group_by,
        filter,
        having,
        limit,
        offset,
        sort,
        group_exprs,
        grouping_sets,
        ..
    } = subquery
    else {
        return None;
    };
    // Shapes whose row count an existence check cannot ignore: an aggregate
    // or window function yields a row regardless of the rows below it, and a
    // set-returning function in the select list runs as a join of its own.
    if !group_by.is_empty()
        || having.is_some()
        || !group_exprs.is_empty()
        || grouping_sets.is_some()
        || offset.is_some()
        || limit.is_some_and(|rows| rows == 0)
        || joins.iter().any(|j| j.table_name == "\u{0}srf")
        || projection.iter().any(|item| {
            matches!(
                item,
                crate::plan_types::ProjectionItem::Aggregate(..)
                    | crate::plan_types::ProjectionItem::WindowFunction { .. }
            )
        })
    {
        return None;
    }
    let inner_quals = relation_quals(subquery);
    let is_outer = |name: &str| crate::filter_eval::is_outer_ref(name, &inner_quals);
    // The subquery's joins must be self-contained; only its `WHERE`
    // correlates with the outer row.
    if joins.iter().any(|j| {
        j.condition.as_ref().is_some_and(|condition| {
            let mut refs = Vec::new();
            crate::filter_eval::filter_column_refs(condition, &mut refs);
            refs.iter().any(|r| is_outer(r))
        })
    }) {
        return None;
    }
    let mut lifted = Vec::new();
    if let Some((comparison, any_op)) = &in_key {
        // `IN`/`ANY` compares against the subquery's single output column.
        let op = compare_of(any_op)?;
        let [
            crate::plan_types::ProjectionItem::Column(key)
            | crate::plan_types::ProjectionItem::AliasedColumn(key, _),
        ] = projection.as_slice()
        else {
            return None;
        };
        // The left side is written in the outer query, the key in the
        // subquery, so a bare name on either side belongs to its own
        // relations: qualify it with the only relation it can be, leaving
        // an ambiguous name to the `SubPlan`.
        let qualify = |name: &String, labels: Vec<String>| -> Option<String> {
            if name.contains('.') {
                return Some(name.clone());
            }
            let [label] = labels.as_slice() else {
                return None;
            };
            Some(format!("{label}.{name}"))
        };
        let key = qualify(key, relation_labels(subquery))?;
        let left = match comparison {
            crate::plan_types::ScalarExpr::Column(name) => {
                let name = qualify(name, outer_quals.to_vec())?;
                // A self-referencing subquery would shadow an outer name,
                // which the join cannot express.
                if !is_outer(&name) {
                    return None;
                }
                crate::plan_types::ScalarExpr::Column(name)
            }
            other => other.clone(),
        };
        // A bare column keeps the condition's usual shape; a computed side
        // becomes the comparison written out.
        lifted.push(match left {
            crate::plan_types::ScalarExpr::Column(name) => FilterExpr::Predicate(Predicate {
                left: name,
                op,
                right: Operand::Ident(key),
            }),
            other => FilterExpr::Scalar(crate::plan_types::ScalarExpr::Binary {
                op: any_op.clone(),
                left: Box::new(other),
                right: Box::new(crate::plan_types::ScalarExpr::Column(key)),
            }),
        });
    }
    let mut crossed = !lifted.is_empty();
    let mut kept = Vec::new();
    for conjunct in filter
        .as_ref()
        .map(crate::index_keys::conjuncts)
        .unwrap_or_default()
    {
        let mut refs = Vec::new();
        crate::filter_eval::filter_column_refs(conjunct, &mut refs);
        if refs.iter().any(|r| is_outer(r)) {
            crossed |= refs.iter().any(|r| !is_outer(r));
            lifted.push(conjunct.clone());
        } else {
            kept.push(conjunct.clone());
        }
    }
    // No correlation with the outer row: that is PostgreSQL's one-time
    // `InitPlan` shape, which is not a semi join.
    if !crossed {
        return None;
    }
    let condition = conjunction(&lifted)?;
    let mut inner = subquery.clone();
    let LogicalPlan::Select {
        projection: inner_projection,
        filter: inner_filter,
        sort: inner_sort,
        distinct: inner_distinct,
        distinct_on: inner_distinct_on,
        limit: inner_limit,
        ..
    } = &mut inner
    else {
        return None;
    };
    // Existence ignores the subquery's projection, ordering, and duplicate
    // removal.
    inner_projection.clear();
    *inner_filter = conjunction(&kept);
    inner_sort.clear();
    *inner_distinct = false;
    inner_distinct_on.clear();
    *inner_limit = None;
    Some((
        if anti { JoinType::Anti } else { JoinType::Semi },
        Box::new(inner),
        condition,
    ))
}

impl JoinPlan {
    /// The `Join Filter` PostgreSQL shows: the conditions checked on each
    /// matched pair.
    pub(crate) fn join_filter(&self) -> Option<FilterExpr> {
        conjunction(&self.join_filter)
    }

    pub(crate) fn left_filter(&self) -> Option<FilterExpr> {
        conjunction(&self.left_only)
    }

    pub(crate) fn right_filter(&self) -> Option<FilterExpr> {
        conjunction(&self.right_only)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn condition(sql: &str) -> FilterExpr {
        let mut statements = nodus_sql::parse_sql(sql).unwrap();
        let plan = crate::planner::plan_statement(&statements.remove(0), &[]).unwrap();
        let crate::plan_types::LogicalPlan::Select { joins, .. } = plan else {
            panic!("a select");
        };
        joins[0].condition.clone().unwrap()
    }

    #[test]
    fn conditions_split_into_pairs_sides_and_filters() {
        let plan = hash_plan(
            Some(&condition("select * from a join b on a.x = b.x")),
            &["a"],
            &["b"],
        )
        .unwrap();
        assert_eq!(
            plan.hash_pairs,
            [(
                HashKey::Column("a.x".to_string()),
                HashKey::Column("b.x".to_string())
            )]
        );
        assert!(plan.join_filter().is_none() && plan.left_filter().is_none());

        let plan = hash_plan(
            Some(&condition(
                "select * from a join b on a.x = b.x and a.y <> 1 and b.z = 2 and a.y < b.z",
            )),
            &["a"],
            &["b"],
        )
        .unwrap();
        assert_eq!(plan.hash_pairs.len(), 1);
        assert_eq!(
            plan.left_filter().map(|f| deparse(&f)).as_deref(),
            Some("(a.y <> 1)")
        );
        assert_eq!(
            plan.right_filter().map(|f| deparse(&f)).as_deref(),
            Some("(b.z = 2)")
        );
        assert_eq!(
            plan.join_filter().map(|f| deparse(&f)).as_deref(),
            Some("(a.y < b.z)")
        );

        // No equality between the sides: no hash join.
        assert!(
            hash_plan(
                Some(&condition("select * from a join b on a.x < b.x")),
                &["a"],
                &["b"]
            )
            .is_none()
        );
    }

    fn deparse(filter: &FilterExpr) -> String {
        crate::explain::deparse_filter(filter, true)
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

    fn outer_quals(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn correlated_subqueries_become_semi_and_anti_joins() {
        let (join_type, inner, condition) = semi_join(
            &where_filter("select * from t1 where exists (select 1 from t2 where t2.a = t1.a)"),
            &outer_quals(&["t1"]),
        )
        .unwrap();
        assert!(matches!(join_type, JoinType::Semi));
        assert_eq!(deparse(&condition), "(t2.a = t1.a)");
        // The inner side keeps the relation, without its projection or the
        // lifted correlation.
        let LogicalPlan::Select {
            table_name,
            filter,
            projection,
            ..
        } = &*inner
        else {
            panic!("a select");
        };
        assert_eq!(table_name, "t2");
        assert!(filter.is_none() && projection.is_empty());

        let (join_type, _, _) = semi_join(
            &where_filter("select * from t1 where not exists (select 1 from t2 where t2.a = t1.a)"),
            &outer_quals(&["t1"]),
        )
        .unwrap();
        assert!(matches!(join_type, JoinType::Anti));

        // `IN` compares the outer column with the subquery's output; a bare
        // name on either side takes its own relation's qualifier.
        let (join_type, _, condition) = semi_join(
            &where_filter("select * from t1 where a in (select b from t2)"),
            &outer_quals(&["t1"]),
        )
        .unwrap();
        assert!(matches!(join_type, JoinType::Semi));
        assert_eq!(deparse(&condition), "(t1.a = t2.b)");

        // The subquery's own conditions stay with it.
        let (_, inner, condition) = semi_join(
            &where_filter(
                "select * from t1 where exists (select 1 from t2 where t2.a = t1.a and t2.b = 'y')",
            ),
            &outer_quals(&["t1"]),
        )
        .unwrap();
        assert_eq!(deparse(&condition), "(t2.a = t1.a)");
        let LogicalPlan::Select { filter, .. } = &*inner else {
            panic!("a select");
        };
        assert_eq!(
            filter.as_ref().map(deparse).as_deref(),
            Some("(t2.b = 'y'::text)")
        );
    }

    #[test]
    fn computed_equalities_hash() {
        let plan = hash_plan(
            Some(&condition("select * from a join b on a.x + 1 = b.y")),
            &["a"],
            &["b"],
        )
        .unwrap();
        assert_eq!(plan.hash_pairs.len(), 1, "{:?}", plan.hash_pairs);
        let plan = hash_plan(
            Some(&condition("select * from a join b on (a.x)::text = b.y")),
            &["a"],
            &["b"],
        )
        .unwrap();
        assert_eq!(plan.hash_pairs.len(), 1, "{:?}", plan.hash_pairs);
    }

    #[test]
    fn any_subqueries_become_semi_joins() {
        let (join_type, _, condition) = semi_join(
            &where_filter("select * from t1 where a = any (select b from t2)"),
            &outer_quals(&["t1"]),
        )
        .unwrap();
        assert!(matches!(join_type, JoinType::Semi));
        assert_eq!(deparse(&condition), "(t1.a = t2.b)");

        let (_, _, condition) = semi_join(
            &where_filter("select * from t1 where a > any (select b from t2)"),
            &outer_quals(&["t1"]),
        )
        .unwrap();
        assert_eq!(deparse(&condition), "(t1.a > t2.b)");

        // `ALL` reads every row: not a semi join.
        assert!(
            semi_join(
                &where_filter("select * from t1 where a > all (select b from t2)"),
                &outer_quals(&["t1"]),
            )
            .is_none()
        );
    }

    #[test]
    fn a_condition_splits_into_semi_joins_and_the_rest() {
        let filter = where_filter(
            "select * from t1 where exists (select 1 from t2 where t2.a = t1.a) and b = 'x'",
        );
        let (joins, rest) = semi_joins(Some(&filter), &outer_quals(&["t1"]));
        assert_eq!(joins.len(), 1);
        assert!(matches!(joins[0].join_type, JoinType::Semi));
        assert!(joins[0].semi_subquery.is_some());
        assert_eq!(
            joins[0].condition.as_ref().map(deparse).as_deref(),
            Some("(t2.a = t1.a)")
        );
        assert_eq!(
            rest.as_ref().map(deparse).as_deref(),
            Some("(b = 'x'::text)")
        );
    }

    #[test]
    fn subquery_shapes_a_semi_join_cannot_take_stay_subplans() {
        for sql in [
            // Uncorrelated: PostgreSQL plans a one-time `InitPlan`.
            "select * from t1 where exists (select 1 from t2)",
            // `NOT IN` has NULL semantics an anti join does not reproduce.
            "select * from t1 where a not in (select b from t2)",
            // The row count is not the inner rows'.
            "select * from t1 where exists (select max(a) from t2 where t2.a = t1.a)",
            "select * from t1 where exists (select 1 from t2 group by a, t2.a having t2.a = t1.a)",
            // A zero limit matches nothing.
            "select * from t1 where exists (select 1 from t2 where t2.a = t1.a limit 0)",
        ] {
            assert!(
                semi_join(&where_filter(sql), &outer_quals(&["t1"])).is_none(),
                "{sql}"
            );
        }
        // An ambiguous bare name (two outer relations could own it).
        assert!(
            semi_join(
                &where_filter("select * from t1, t2 where b in (select b from t2)"),
                &outer_quals(&["t1", "t2"])
            )
            .is_none()
        );
        // A limit of one and an ordering do not change existence.
        let (_, inner, _) = semi_join(
            &where_filter(
                "select * from t1 where exists (select 1 from t2 where t2.a = t1.a order by t2.b limit 1)",
            ),
            &outer_quals(&["t1"]),
        )
        .unwrap();
        let LogicalPlan::Select { limit, sort, .. } = &*inner else {
            panic!("a select");
        };
        assert!(limit.is_none() && sort.is_empty());
    }
}
