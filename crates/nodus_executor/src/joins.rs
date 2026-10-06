//! Join strategies: the equalities a hash join hashes, and the conditions
//! each side applies alone. Shared by the executor and `EXPLAIN`, so a plan
//! shows the join the executor runs.

use crate::plan_types::{CompareOp, FilterExpr, Operand, Predicate};

/// How a join's condition splits for a hash join: the equalities hashed
/// (`(left column, right column)`), the conditions only one side's rows
/// must satisfy (which PostgreSQL pushes into its scan), and the conditions
/// over both sides, checked on each matched pair.
#[derive(Debug, Default, Clone)]
pub(crate) struct JoinPlan {
    pub(crate) hash_pairs: Vec<(String, String)>,
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
        match conjunct {
            FilterExpr::Predicate(Predicate {
                left,
                op: CompareOp::Eq,
                right: Operand::Ident(right),
            }) => match (
                side(left, left_prefixes, right_prefixes),
                side(right, left_prefixes, right_prefixes),
            ) {
                // One column from each side: hashed (left side first, as
                // PostgreSQL prints the condition).
                (Some(true), Some(false)) => plan.hash_pairs.push((left.clone(), right.clone())),
                (Some(false), Some(true)) => plan.hash_pairs.push((right.clone(), left.clone())),
                (Some(true), Some(true)) => plan.left_only.push(conjunct.clone()),
                (Some(false), Some(false)) => plan.right_only.push(conjunct.clone()),
                _ => plan.join_filter.push(conjunct.clone()),
            },
            other => {
                let mut refs = Vec::new();
                crate::filter_eval::filter_column_refs(other, &mut refs);
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
                    plan.left_only.push(other.clone());
                } else if on_right && !on_left {
                    plan.right_only.push(other.clone());
                } else {
                    plan.join_filter.push(other.clone());
                }
            }
        }
    }
    // `a = a` and the like are no hash pair.
    plan.hash_pairs.dedup();
    (!plan.hash_pairs.is_empty()).then_some(plan)
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
        assert_eq!(plan.hash_pairs, [("a.x".to_string(), "b.x".to_string())]);
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
}
