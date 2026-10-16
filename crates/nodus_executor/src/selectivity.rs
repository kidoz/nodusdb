//! Selectivity and row width from what `ANALYZE` recorded: PostgreSQL's
//! `selfuncs` model per column — the most-common values, the histogram and
//! the distinct estimate — and its extended statistics over several columns
//! (`extended_stats.c`: the multivariate MCV list and the functional
//! dependencies). A relation never analyzed falls back to the same defaults
//! PostgreSQL uses.

use crate::plan_types::{CompareOp, FilterExpr, Operand, Predicate};
use crate::statistics::{McvItem, StatisticsObject};
use crate::{MemExecutor, Value};
use nodus_catalog::{ColumnStatistics, TableDescriptor};

/// PostgreSQL's `DEFAULT_EQ_SEL`: an equality without statistics.
const DEFAULT_EQ_SEL: f64 = 0.005;
/// `DEFAULT_INEQ_SEL`: an inequality without statistics.
const DEFAULT_INEQ_SEL: f64 = 1.0 / 3.0;
/// `DEFAULT_NUM_DISTINCT`: the assumed distinct count of an unknown column.
const DEFAULT_NUM_DISTINCT: f64 = 200.0;
/// `DEFAULT_UNK_SEL`: an `IS NULL` test without statistics.
const DEFAULT_UNK_SEL: f64 = 0.005;
/// `DEFAULT_RANGE_INEQ_SEL`: a range whose bounds have no statistics.
const DEFAULT_RANGE_INEQ_SEL: f64 = 0.005;

/// A relation's estimates, from its descriptor's statistics.
pub(crate) struct Estimates<'a> {
    table: &'a TableDescriptor,
    objects: Vec<StatisticsObject>,
}

impl<'a> Estimates<'a> {
    pub(crate) fn of(executor: &MemExecutor, table: &'a TableDescriptor) -> Estimates<'a> {
        Estimates {
            table,
            objects: executor.statistics_objects(table.id),
        }
    }

    /// PostgreSQL's `set_rel_width`: each column's analyzed average width,
    /// and a type's default width for a column without statistics.
    pub(crate) fn width(&self) -> u32 {
        self.table
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                self.stats_for_attnum(index as i32 + 1).map_or_else(
                    || crate::explain::type_width(&column.data_type),
                    |stats| stats.avg_width.max(0) as u32,
                )
            })
            .sum()
    }

    /// The rows an estimate multiplies: `reltuples` once the table was
    /// analyzed, the scanned count otherwise.
    pub(crate) fn rows(&self, scanned: f64) -> f64 {
        self.table
            .reltuples
            .filter(|tuples| *tuples >= 0.0)
            .unwrap_or(scanned)
    }

    /// The selectivity of a filter over this relation.
    pub(crate) fn selectivity(&self, filter: &FilterExpr) -> f64 {
        self.filter_sel(filter)
    }

    /// The selectivity of a conjunction, for an index condition.
    pub(crate) fn conjunction(&self, conjuncts: &[&FilterExpr]) -> f64 {
        if let Some(sel) = self.extended(conjuncts) {
            return sel;
        }
        self.and_sel(conjuncts)
    }

    // --- filters ----------------------------------------------------------

    fn filter_sel(&self, filter: &FilterExpr) -> f64 {
        match filter {
            FilterExpr::And(..) => {
                let conjuncts = crate::index_keys::conjuncts(filter);
                if let Some(sel) = self.extended(&conjuncts) {
                    return sel;
                }
                self.and_sel(&conjuncts)
            }
            FilterExpr::Or(..) => {
                let mut disjuncts = Vec::new();
                collect_or(filter, &mut disjuncts);
                let none: f64 = disjuncts
                    .iter()
                    .map(|clause| 1.0 - self.filter_sel(clause))
                    .product();
                (1.0 - none).clamp(0.0, 1.0)
            }
            FilterExpr::Not(inner) => (1.0 - self.filter_sel(inner)).clamp(0.0, 1.0),
            FilterExpr::IsNull(name) => self
                .null_frac(name)
                .unwrap_or(DEFAULT_UNK_SEL)
                .clamp(0.0, 1.0),
            FilterExpr::IsNotNull(name) => {
                (1.0 - self.null_frac(name).unwrap_or(DEFAULT_UNK_SEL)).clamp(0.0, 1.0)
            }
            FilterExpr::InList {
                left,
                list,
                negated,
            } => {
                let mut selec = 0.0;
                for item in list {
                    if let Operand::Literal(value) = item {
                        selec += self.eq_sel(left, value);
                    }
                }
                // The listed values cannot be more common than the non-null
                // rows, as PostgreSQL caps them.
                let null_frac = self.null_frac(left).unwrap_or(0.0);
                selec = selec.clamp(0.0, (1.0 - null_frac).max(0.0));
                if *negated {
                    (1.0 - selec - null_frac).clamp(0.0, 1.0)
                } else {
                    selec
                }
            }
            FilterExpr::Predicate(_) => self.predicate_sel(filter),
            // Another shape (a subquery, a join clause): a default estimate.
            _ => DEFAULT_EQ_SEL,
        }
    }

    /// An AND chain: two bounds on one column are a range, whose
    /// selectivity is their overlap — not their product — as
    /// `clauselist_selectivity` merges the pair.
    fn and_sel(&self, conjuncts: &[&FilterExpr]) -> f64 {
        // Each column's tightest bound on each side.
        let mut bounds: Vec<(String, Option<f64>, Option<f64>)> = Vec::new();
        let mut product = 1.0f64;
        for clause in conjuncts {
            let bound = match clause {
                FilterExpr::Predicate(Predicate {
                    left,
                    op: op @ (CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge),
                    right: Operand::Literal(value),
                }) if !matches!(value, Value::Null) => {
                    let low = matches!(op, CompareOp::Gt | CompareOp::Ge);
                    Some((bare_name(left).to_string(), low, self.filter_sel(clause)))
                }
                _ => None,
            };
            let Some((name, low, selectivity)) = bound else {
                product *= self.filter_sel(clause);
                continue;
            };
            let entry = match bounds.iter_mut().find(|(column, _, _)| *column == name) {
                Some(entry) => entry,
                None => {
                    bounds.push((name, None, None));
                    bounds.last_mut().expect("just pushed")
                }
            };
            let side = if low { &mut entry.1 } else { &mut entry.2 };
            *side = Some(side.map_or(selectivity, |existing| existing.min(selectivity)));
        }
        for (name, low, high) in bounds {
            product *= match (low, high) {
                (Some(low), Some(high)) => {
                    let null_frac = self.null_frac(&name).unwrap_or(0.0);
                    let mut range = if low == DEFAULT_INEQ_SEL || high == DEFAULT_INEQ_SEL {
                        DEFAULT_RANGE_INEQ_SEL
                    } else {
                        // The two excluded tails overlap once: their
                        // intersection is what the pair keeps.
                        high + low - 1.0 + null_frac
                    };
                    if range <= 0.0 {
                        range = if range < -0.01 {
                            DEFAULT_RANGE_INEQ_SEL
                        } else {
                            1.0e-10
                        };
                    }
                    range
                }
                (Some(selectivity), None) | (None, Some(selectivity)) => selectivity,
                (None, None) => 1.0,
            };
        }
        product.clamp(0.0, 1.0)
    }

    fn predicate_sel(&self, filter: &FilterExpr) -> f64 {
        let FilterExpr::Predicate(Predicate { left, op, right }) = filter else {
            return DEFAULT_EQ_SEL;
        };
        let Operand::Literal(value) = right else {
            // A column on the right is a join clause, not a restriction.
            return match op {
                CompareOp::Eq | CompareOp::Ne => DEFAULT_EQ_SEL,
                _ => DEFAULT_INEQ_SEL,
            };
        };
        if matches!(value, Value::Null) {
            // A strict operator is never true of NULL.
            return 0.0;
        }
        let name = bare_name(left);
        match op {
            CompareOp::Eq => self.eq_sel(name, value),
            CompareOp::Ne => {
                let eq = self.eq_sel(name, value);
                let null_frac = self.null_frac(name).unwrap_or(0.0);
                (1.0 - eq - null_frac).clamp(0.0, 1.0)
            }
            CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
                self.ineq_sel(name, value, *op)
            }
            _ => DEFAULT_EQ_SEL,
        }
    }

    // --- one column -------------------------------------------------------

    /// `var_eq_const`: the equality's selectivity from the most-common
    /// values, or the remaining mass spread over the other distinct values.
    fn eq_sel(&self, name: &str, value: &Value) -> f64 {
        let Some(stats) = self.stats_for(name) else {
            return DEFAULT_EQ_SEL;
        };
        let null_frac = stats.null_frac as f64;
        // A unique index has exactly one match.
        if self.is_unique(name)
            && let Some(rows) = self.table.reltuples
            && rows >= 1.0
        {
            return 1.0 / rows;
        }
        let mcv = self.mcv(stats);
        if let Some((_, frequency)) = mcv
            .iter()
            .find(|(common, _)| crate::value::compare(common, value) == std::cmp::Ordering::Equal)
        {
            return *frequency as f64;
        }
        let sumcommon: f64 = mcv.iter().map(|(_, frequency)| *frequency as f64).sum();
        let mut selec = (1.0 - sumcommon - null_frac).clamp(0.0, 1.0);
        let otherdistinct = self.num_distinct(stats) - mcv.len() as f64;
        if otherdistinct > 1.0 {
            selec /= otherdistinct;
        }
        // No more than the least common of the most common values.
        if let Some((_, smallest)) = mcv.last()
            && selec > *smallest as f64
        {
            selec = *smallest as f64;
        }
        selec.clamp(0.0, 1.0)
    }

    /// `scalarineqsel`: the most-common values that satisfy the operator,
    /// plus the histogram's share of everything else.
    fn ineq_sel(&self, name: &str, value: &Value, op: CompareOp) -> f64 {
        let Some(stats) = self.stats_for(name) else {
            return DEFAULT_INEQ_SEL;
        };
        let isgt = matches!(op, CompareOp::Gt | CompareOp::Ge);
        let iseq = matches!(op, CompareOp::Le | CompareOp::Ge);
        let mcv = self.mcv(stats);
        let mut mcv_selec = 0.0;
        let mut sumcommon = 0.0;
        for (common, frequency) in &mcv {
            sumcommon += *frequency as f64;
            let cmp = crate::value::compare(common, value);
            let satisfies = match op {
                CompareOp::Lt => cmp == std::cmp::Ordering::Less,
                CompareOp::Le => cmp != std::cmp::Ordering::Greater,
                CompareOp::Gt => cmp == std::cmp::Ordering::Greater,
                CompareOp::Ge => cmp != std::cmp::Ordering::Less,
                _ => false,
            };
            if satisfies {
                mcv_selec += *frequency as f64;
            }
        }
        let histogram = self.histogram(stats);
        let hist_selec = if histogram.is_empty() {
            -1.0
        } else {
            self.histogram_fraction(&histogram, value, isgt, iseq, stats)
        };
        let mut selec = (1.0 - stats.null_frac as f64 - sumcommon).max(0.0);
        selec *= if hist_selec >= 0.0 { hist_selec } else { 0.5 };
        selec += mcv_selec;
        selec.clamp(0.0, 1.0)
    }

    /// `ineq_histogram_selectivity`: where the constant falls among the
    /// histogram's bounds, interpolated within its bin.
    fn histogram_fraction(
        &self,
        bounds: &[Value],
        value: &Value,
        isgt: bool,
        iseq: bool,
        stats: &ColumnStatistics,
    ) -> f64 {
        // The first bound that satisfies the operator (its direction
        // reversed for `isgt`), by binary search.
        let n = bounds.len();
        let mut low = 0usize;
        let mut high = n;
        while low < high {
            let probe = (low + high) / 2;
            let mut below =
                crate::value::compare(&bounds[probe], value) == std::cmp::Ordering::Less;
            if isgt {
                below = !below;
            }
            if below {
                low = probe + 1;
            } else {
                high = probe;
            }
        }
        if low == 0 {
            return if isgt { 1.0 } else { 0.0 };
        }
        if low >= n {
            return if isgt { 0.0 } else { 1.0 };
        }
        let i = low;
        let mut eq_selec = 0.0;
        if i == 1 || isgt == iseq {
            let otherdistinct = self.num_distinct(stats) - self.mcv(stats).len() as f64;
            if otherdistinct > 1.0 {
                eq_selec = 1.0 / otherdistinct;
            }
        }
        let data_type = &stats.data_type;
        let binfrac = match (
            numeric(bounds[i - 1].clone(), data_type),
            numeric(bounds[i].clone(), data_type),
            numeric(value.clone(), data_type),
        ) {
            (Some(low), Some(high), Some(value)) => {
                if high <= low {
                    0.5
                } else if value <= low {
                    0.0
                } else if value >= high {
                    1.0
                } else {
                    (value - low) / (high - low)
                }
            }
            // A type whose values do not convert to a number: the bin's
            // middle, as PostgreSQL assumes.
            _ => 0.5,
        };
        // The fraction of the population at or below the constant.
        let mut histfrac = (i - 1) as f64 + binfrac;
        histfrac /= (n - 1) as f64;
        // The first bin is narrower by one value's worth.
        if i == 1 {
            histfrac += eq_selec * (1.0 - binfrac);
        }
        // For `<` and `>=` the estimate must exclude the equal value.
        if isgt == iseq {
            histfrac -= eq_selec;
        }
        let hist_selec = if isgt { 1.0 - histfrac } else { histfrac };
        // The histogram's ends are not the table's: PostgreSQL keeps the
        // estimate off them (its `have_end` refinement is not reproduced).
        let cutoff = 0.01 / (n - 1) as f64;
        hist_selec.clamp(cutoff, 1.0 - cutoff)
    }

    // --- one relation's statistics ----------------------------------------

    fn stats_for(&self, name: &str) -> Option<&ColumnStatistics> {
        let bare = bare_name(name);
        let attnum = self
            .table
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(bare))? as i32
            + 1;
        self.stats_for_attnum(attnum)
    }

    fn stats_for_attnum(&self, attnum: i32) -> Option<&ColumnStatistics> {
        self.table
            .statistics
            .iter()
            .find(|stats| stats.attnum == attnum)
    }

    fn null_frac(&self, name: &str) -> Option<f64> {
        self.stats_for(name).map(|stats| stats.null_frac as f64)
    }

    fn is_unique(&self, name: &str) -> bool {
        let bare = bare_name(name);
        let Some(column) = self
            .table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(bare))
        else {
            return false;
        };
        self.table.indexes.iter().any(|index| {
            index.unique
                && index.key_columns.len() == 1
                && index.key_columns[0].column_id == column.id
        })
    }

    /// PostgreSQL's `get_variable_numdistinct`.
    fn num_distinct(&self, stats: &ColumnStatistics) -> f64 {
        let distinct = stats.n_distinct as f64;
        if distinct > 0.0 {
            distinct
        } else if distinct < 0.0 {
            (-distinct * self.table.reltuples.unwrap_or(0.0)).max(1.0)
        } else {
            DEFAULT_NUM_DISTINCT
        }
    }

    /// A column's most common values and their frequencies.
    fn mcv(&self, stats: &ColumnStatistics) -> Vec<(Value, f32)> {
        mcv_of(stats)
    }

    /// A column's histogram bounds.
    fn histogram(&self, stats: &ColumnStatistics) -> Vec<Value> {
        let Some(slot) = stats.slots.iter().find(|slot| slot.kind == 2) else {
            return Vec::new();
        };
        slot.values
            .as_ref()
            .map(|values| {
                values
                    .iter()
                    .map(|text| serde_json::from_str(text).unwrap_or(Value::Null))
                    .collect()
            })
            .unwrap_or_default()
    }

    // --- extended statistics ----------------------------------------------

    /// What the extended statistics say about an AND chain: the
    /// multivariate MCV list first, then the functional dependencies, as
    /// PostgreSQL applies them.
    fn extended(&self, conjuncts: &[&FilterExpr]) -> Option<f64> {
        if self.objects.is_empty() || conjuncts.len() < 2 {
            return None;
        }
        let mut remaining: Vec<&FilterExpr> = conjuncts.to_vec();
        let mut selectivity = 1.0;
        if let Some((sel, estimated)) = self.extended_mcv(&remaining) {
            selectivity *= sel;
            remaining = remaining
                .iter()
                .enumerate()
                .filter(|(index, _)| !estimated.contains(index))
                .map(|(_, clause)| *clause)
                .collect();
        }
        if let Some((sel, estimated)) = self.extended_dependencies(&remaining) {
            selectivity *= sel;
            remaining = remaining
                .iter()
                .enumerate()
                .filter(|(index, _)| !estimated.contains(index))
                .map(|(_, clause)| *clause)
                .collect();
        }
        selectivity *= remaining
            .iter()
            .map(|clause| self.filter_sel(clause))
            .product::<f64>();
        Some(selectivity.clamp(0.0, 1.0))
    }

    /// `statext_mcv_clauselist_selectivity`: the equality clauses a
    /// statistics object's keys cover become one combination, looked up in
    /// the object's MCV list.
    fn extended_mcv(&self, clauses: &[&FilterExpr]) -> Option<(f64, Vec<usize>)> {
        // Each clause's column, when it is an equality to a literal.
        let columns: Vec<Option<String>> = clauses
            .iter()
            .map(|clause| match clause {
                FilterExpr::Predicate(Predicate {
                    left,
                    op: CompareOp::Eq,
                    right: Operand::Literal(value),
                }) if !matches!(value, Value::Null) => Some(bare_name(left).to_string()),
                _ => None,
            })
            .collect();
        let constrained: Vec<&str> = columns.iter().flatten().map(String::as_str).collect();
        // The object covering the most of them, at least two.
        let object = self
            .objects
            .iter()
            .filter(|object| object.definition.kinds.contains(&'m'))
            .filter_map(|object| {
                let keys = object.definition.column_keys();
                let matched: Vec<usize> = (0..clauses.len())
                    .filter(|index| {
                        columns[*index].as_deref().is_some_and(|column| {
                            self.attnum_of(column)
                                .is_some_and(|attnum| keys.contains(&attnum))
                        })
                    })
                    .collect();
                (matched.len() >= 2).then_some((object, keys, matched))
            })
            .max_by_key(|(_, _, matched)| matched.len())?;
        let (object, keys, matched) = object;
        let data = object.definition.data(false)?;
        // The clauses' value per key position.
        let mut wanted: Vec<Option<Value>> = vec![None; keys.len()];
        for index in &matched {
            let FilterExpr::Predicate(Predicate {
                left,
                right: Operand::Literal(value),
                ..
            }) = clauses[*index]
            else {
                continue;
            };
            let attnum = self.attnum_of(bare_name(left))?;
            let position = keys.iter().position(|key| *key == attnum)?;
            wanted[position] = Some(value.clone());
        }
        let wanted: Vec<&Option<Value>> = wanted.iter().collect();
        // The items matching every constrained key: the first (the most
        // frequent, the list is ordered) is the combination the clauses
        // pin down, and their total is the mass the list covers.
        let mut mcv_sel = 0.0;
        let mut mcv_basesel = 0.0;
        let mut mcv_totalsel = 0.0;
        for item in &data.mcv {
            if !item_matches(item, &wanted) {
                continue;
            }
            if mcv_sel == 0.0 {
                mcv_sel = item.frequency;
                mcv_basesel = item.base_frequency;
            }
            mcv_totalsel += item.frequency;
        }
        let simple: f64 = matched
            .iter()
            .map(|index| self.filter_sel(clauses[*index]))
            .product();
        Some((combine(simple, mcv_sel, mcv_basesel, mcv_totalsel), matched))
    }

    /// `dependencies_clauselist_selectivity`: a functional dependency whose
    /// attributes the clauses all constrain scales the dependent column's
    /// estimate by the dependency's degree.
    fn extended_dependencies(&self, clauses: &[&FilterExpr]) -> Option<(f64, Vec<usize>)> {
        // Each clause's column, when it compares to a literal.
        let columns: Vec<Option<String>> =
            clauses
                .iter()
                .map(|clause| match clause {
                    FilterExpr::Predicate(Predicate {
                        left,
                        op:
                            CompareOp::Eq
                            | CompareOp::Lt
                            | CompareOp::Le
                            | CompareOp::Gt
                            | CompareOp::Ge,
                        right: Operand::Literal(value),
                    }) if !matches!(value, Value::Null) => Some(bare_name(left).to_string()),
                    _ => None,
                })
                .collect();
        let object = self
            .objects
            .iter()
            .filter(|object| object.definition.kinds.contains(&'f'))
            .find(|object| {
                let keys = object.definition.column_keys();
                columns
                    .iter()
                    .flatten()
                    .filter_map(|column| self.attnum_of(column))
                    .filter(|attnum| keys.contains(attnum))
                    .count()
                    >= 2
            })?;
        let data = object.definition.data(false)?;
        if data.dependencies.is_empty() {
            return None;
        }
        let keys = object.definition.column_keys();
        // Each clause's attribute number, when it constrains one of the
        // object's keys.
        let clause_attnums: Vec<Option<i32>> = columns
            .iter()
            .map(|column| {
                column
                    .as_deref()
                    .and_then(|column| self.attnum_of(column))
                    .filter(|attnum| keys.contains(attnum))
            })
            .collect();
        let mut constrained: Vec<i32> = clause_attnums.iter().flatten().copied().collect();
        constrained.sort_unstable();
        constrained.dedup();
        if constrained.len() < 2 {
            return None;
        }
        // The dependencies to apply: the strongest fully matched one is
        // taken and its implied attribute dropped from what later ones may
        // cover, until none matches; the ties go to the later entry, as
        // PostgreSQL's `find_strongest_dependency` picks them.
        let mut available = constrained.clone();
        let mut selected: Vec<&(Vec<i32>, f64)> = Vec::new();
        loop {
            let mut strongest: Option<&(Vec<i32>, f64)> = None;
            for dependency in &data.dependencies {
                let (attributes, degree) = dependency;
                if attributes.len() > available.len() {
                    continue;
                }
                if let Some(current) = strongest {
                    if attributes.len() < current.0.len() {
                        continue;
                    }
                    if attributes.len() == current.0.len() && current.1 > *degree {
                        continue;
                    }
                }
                if attributes.iter().all(|attnum| available.contains(attnum)) {
                    strongest = Some(dependency);
                }
            }
            let Some(dependency) = strongest else {
                break;
            };
            available.retain(|attnum| Some(*attnum) != dependency.0.last().copied());
            selected.push(dependency);
        }
        if selected.is_empty() {
            return None;
        }
        // The attributes they refer to, each with the selectivity of its own
        // clauses.
        let mut attnums: Vec<i32> = selected
            .iter()
            .flat_map(|(attributes, _)| attributes.iter().copied())
            .collect();
        attnums.sort_unstable();
        attnums.dedup();
        let mut estimated: Vec<usize> = Vec::new();
        let mut attribute_sel: Vec<(i32, f64)> = Vec::new();
        for attnum in attnums {
            let attr_clauses: Vec<&FilterExpr> = clause_attnums
                .iter()
                .enumerate()
                .filter(|(_, clause_attnum)| **clause_attnum == Some(attnum))
                .map(|(index, _)| {
                    estimated.push(index);
                    clauses[index]
                })
                .collect();
            attribute_sel.push((attnum, self.and_sel(&attr_clauses)));
        }
        // Applied in reverse: each implied attribute becomes the conditional
        // probability of its clauses given its determinants'.
        for (attributes, degree) in selected.iter().rev() {
            let (dependent, determinants) = attributes.split_last()?;
            let dependent_sel = attribute_sel
                .iter()
                .find(|(key, _)| key == dependent)
                .map(|(_, sel)| *sel)?;
            let determinants_sel: f64 = determinants
                .iter()
                .filter_map(|attnum| {
                    attribute_sel
                        .iter()
                        .find(|(key, _)| key == attnum)
                        .map(|(_, sel)| *sel)
                })
                .product();
            let adjusted = if determinants_sel <= dependent_sel {
                degree + (1.0 - degree) * dependent_sel
            } else {
                degree * dependent_sel / determinants_sel + (1.0 - degree) * dependent_sel
            };
            if let Some((_, sel)) = attribute_sel.iter_mut().find(|(key, _)| key == dependent) {
                *sel = adjusted;
            }
        }
        let selectivity: f64 = attribute_sel.iter().map(|(_, sel)| sel).product();
        Some((selectivity.clamp(0.0, 1.0), estimated))
    }

    fn attnum_of(&self, column: &str) -> Option<i32> {
        self.table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(column))
            .map(|index| index as i32 + 1)
    }
}

/// `mcv_combine_selectivities`: the matched combination's own frequency,
/// plus the unmatched part of the clauses' product.
fn combine(simple_sel: f64, mcv_sel: f64, mcv_basesel: f64, mcv_totalsel: f64) -> f64 {
    let mut other_sel = (simple_sel - mcv_basesel).clamp(0.0, 1.0);
    if other_sel > 1.0 - mcv_totalsel {
        other_sel = 1.0 - mcv_totalsel;
    }
    (mcv_sel + other_sel).clamp(0.0, 1.0)
}

/// Whether an MCV combination satisfies every wanted value.
fn item_matches(item: &McvItem, wanted: &[&Option<Value>]) -> bool {
    wanted.iter().enumerate().all(|(position, value)| {
        let Some(value) = value else {
            return true;
        };
        match item.values.get(position) {
            Some(Some(common)) => crate::value::compare(common, value) == std::cmp::Ordering::Equal,
            _ => false,
        }
    })
}

/// A column name without its qualifier.
fn bare_name(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

/// Every disjunct of an OR chain.
fn collect_or<'a>(filter: &'a FilterExpr, out: &mut Vec<&'a FilterExpr>) {
    match filter {
        FilterExpr::Or(a, b) => {
            collect_or(a, out);
            collect_or(b, out);
        }
        other => out.push(other),
    }
}

/// A value as a number, for the histogram's interpolation, where the
/// comparison's order matches its numbers' — as PostgreSQL's
/// `convert_to_scalar` does, and only where the type's order is numeric.
fn numeric(value: Value, data_type: &str) -> Option<f64> {
    match value {
        Value::Int(n) => Some(n as f64),
        Value::Float(f) => Some(f),
        Value::Numeric(d) => Some(crate::value::decimal_to_f64(&d)),
        Value::Bool(b) => Some(if b { 1.0 } else { 0.0 }),
        Value::Text(text) => match crate::datetime::Kind::of_type(data_type) {
            Some(crate::datetime::Kind::Date) => {
                chrono::NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                    .ok()
                    .map(|date| {
                        date.signed_duration_since(chrono::NaiveDate::default())
                            .num_days() as f64
                    })
            }
            Some(
                kind @ (crate::datetime::Kind::Timestamp | crate::datetime::Kind::TimestampTz),
            ) => match crate::datetime::Temporal::parse_as(&text, kind) {
                Some(crate::datetime::Temporal::Timestamp(ts)) => {
                    Some(ts.and_utc().timestamp_micros() as f64)
                }
                Some(crate::datetime::Temporal::TimestampTz(ts)) => {
                    Some(ts.timestamp_micros() as f64)
                }
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

// --- join selectivity (PostgreSQL's `eqjoinsel` and friends) ----------------

/// One side of a join key: the statistics recorded for the column, the
/// relation's estimated rows, its raw tuple count, and whether a unique
/// index proves it distinct — `eqjoinsel`'s inputs.
pub(crate) struct JoinSide<'a> {
    pub(crate) stats: Option<&'a ColumnStatistics>,
    /// The column's declared type, for the boolean special case.
    pub(crate) data_type: Option<&'a str>,
    /// The relation's estimated rows (filtered).
    pub(crate) rows: f64,
    /// The relation's raw `reltuples`.
    pub(crate) tuples: f64,
    /// A unique index (or key) covers the column alone.
    pub(crate) unique: bool,
}

impl JoinSide<'_> {
    fn null_frac(&self) -> f64 {
        self.stats.map_or(0.0, |stats| stats.null_frac as f64)
    }

    fn mcv(&self) -> Vec<(Value, f32)> {
        self.stats.map(mcv_of).unwrap_or_default()
    }

    /// `get_variable_numdistinct`: the column's distinct count and whether
    /// it is a default guess.
    fn num_distinct(&self) -> (f64, bool) {
        let null_frac = self.null_frac();
        let mut distinct = match self.stats {
            Some(stats) => stats.n_distinct as f64,
            // Two distinct values, presumably, for a boolean.
            None if self.data_type.is_some_and(|t| {
                t.trim().eq_ignore_ascii_case("boolean") || t.trim().eq_ignore_ascii_case("bool")
            }) =>
            {
                2.0
            }
            None => 0.0,
        };
        if self.unique {
            distinct = -1.0 * (1.0 - null_frac);
        }
        if distinct > 0.0 {
            return (clamp_rows(distinct), false);
        }
        if self.tuples <= 0.0 {
            return (DEFAULT_NUM_DISTINCT, true);
        }
        if distinct < 0.0 {
            return (clamp_rows(-distinct * self.tuples), false);
        }
        // No data: assume the table's own size for a small one.
        if self.tuples < DEFAULT_NUM_DISTINCT {
            (clamp_rows(self.tuples), false)
        } else {
            (DEFAULT_NUM_DISTINCT, true)
        }
    }
}

/// `clamp_row_est`: a positive whole number of rows, at least one.
fn clamp_rows(rows: f64) -> f64 {
    if rows > 1.0 { rows.round() } else { 1.0 }
}

/// A column's most common values and their frequencies.
fn mcv_of(stats: &ColumnStatistics) -> Vec<(Value, f32)> {
    let Some(slot) = stats.slots.iter().find(|slot| slot.kind == 1) else {
        return Vec::new();
    };
    let (Some(values), Some(numbers)) = (&slot.values, &slot.numbers) else {
        return Vec::new();
    };
    values
        .iter()
        .zip(numbers)
        .map(|(text, frequency)| {
            (
                serde_json::from_str(text).unwrap_or(Value::Null),
                *frequency,
            )
        })
        .collect()
}

/// `eqjoinsel_inner`: the equalities the two sides' most common values share,
/// and the rest of the distributions' — the smaller estimate of the two points
/// of view.
pub(crate) fn eq_join_selectivity(left: &JoinSide, right: &JoinSide) -> f64 {
    let (nd1, _) = left.num_distinct();
    let (nd2, _) = right.num_distinct();
    let null_frac1 = left.null_frac();
    let null_frac2 = right.null_frac();
    let mcv1 = left.mcv();
    let mcv2 = right.mcv();
    if mcv1.is_empty() || mcv2.is_empty() {
        let mut selec = (1.0 - null_frac1) * (1.0 - null_frac2);
        selec /= if nd1 > nd2 { nd1 } else { nd2 };
        return selec.clamp(0.0, 1.0);
    }
    // Which of each list's values find a partner in the other's.
    let mut matched1 = vec![false; mcv1.len()];
    let mut matched2 = vec![false; mcv2.len()];
    let mut match_freq = 0.0f64;
    let mut matches = 0usize;
    for (i, (value, frequency)) in mcv1.iter().enumerate() {
        for (j, (other, other_frequency)) in mcv2.iter().enumerate() {
            if matched2[j] {
                continue;
            }
            if crate::value::compare(value, other) == std::cmp::Ordering::Equal {
                matched1[i] = true;
                matched2[j] = true;
                match_freq += *frequency as f64 * *other_frequency as f64;
                matches += 1;
                break;
            }
        }
    }
    match_freq = match_freq.clamp(0.0, 1.0);
    let sum = |freqs: &[(Value, f32)], matched: &[bool], wanted: bool| -> f64 {
        freqs
            .iter()
            .zip(matched)
            .filter(|(_, flag)| **flag == wanted)
            .map(|((_, frequency), _)| *frequency as f64)
            .sum::<f64>()
            .clamp(0.0, 1.0)
    };
    let match_freq1 = sum(&mcv1, &matched1, true);
    let unmatch_freq1 = sum(&mcv1, &matched1, false);
    let match_freq2 = sum(&mcv2, &matched2, true);
    let unmatch_freq2 = sum(&mcv2, &matched2, false);
    // The non-null values outside the lists.
    let other_freq1 = (1.0 - null_frac1 - match_freq1 - unmatch_freq1).clamp(0.0, 1.0);
    let other_freq2 = (1.0 - null_frac2 - match_freq2 - unmatch_freq2).clamp(0.0, 1.0);
    // The matched pairs, the lists' unpartnered values against the others'
    // remaining population — from each point of view.
    let mut total1 = match_freq;
    if nd2 > mcv2.len() as f64 {
        total1 += unmatch_freq1 * other_freq2 / (nd2 - mcv2.len() as f64);
    }
    if nd2 > matches as f64 {
        total1 += other_freq1 * (other_freq2 + unmatch_freq2) / (nd2 - matches as f64);
    }
    let mut total2 = match_freq;
    if nd1 > mcv1.len() as f64 {
        total2 += unmatch_freq2 * other_freq1 / (nd1 - mcv1.len() as f64);
    }
    if nd1 > matches as f64 {
        total2 += other_freq2 * (other_freq1 + unmatch_freq1) / (nd1 - matches as f64);
    }
    if total1 < total2 { total1 } else { total2 }
}

/// `eqjoinsel` for a semi or anti join: the fraction of the outer rows that
/// have a partner.
pub(crate) fn semi_join_selectivity(outer: &JoinSide, inner: &JoinSide) -> f64 {
    let null_frac = outer.null_frac();
    let (mut nd1, isdefault1) = outer.num_distinct();
    let (mut nd2, mut isdefault2) = inner.num_distinct();
    // There can't be more distinct inner values than inner rows.
    if nd2 >= inner.rows {
        nd2 = inner.rows;
        isdefault2 = false;
    }
    let mcv1 = outer.mcv();
    let mcv2 = inner.mcv();
    if mcv1.is_empty() || mcv2.is_empty() {
        return if !isdefault1 && !isdefault2 {
            if nd1 <= nd2 || nd2 < 0.0 {
                1.0 - null_frac
            } else {
                (nd2 / nd1) * (1.0 - null_frac)
            }
        } else {
            0.5 * (1.0 - null_frac)
        }
        .clamp(0.0, 1.0);
    }
    // The outer values that certainly have a partner.
    let mut matched2 = vec![false; mcv2.len()];
    let mut match_freq1 = 0.0f64;
    let mut matches = 0usize;
    for (value, frequency) in &mcv1 {
        for (j, (other, _)) in mcv2.iter().enumerate() {
            if matched2[j] {
                continue;
            }
            if crate::value::compare(value, other) == std::cmp::Ordering::Equal {
                matched2[j] = true;
                match_freq1 += *frequency as f64;
                matches += 1;
                break;
            }
        }
    }
    match_freq1 = match_freq1.clamp(0.0, 1.0);
    // The rest: every outer value has a partner when the inner is the
    // narrower side, else only the fraction its distinct count allows.
    let uncertain_frac = if !isdefault1 && !isdefault2 {
        nd1 -= matches as f64;
        nd2 -= matches as f64;
        if nd1 <= nd2 || nd2 < 0.0 {
            1.0
        } else {
            nd2 / nd1
        }
    } else {
        0.5
    };
    let uncertain = (1.0 - match_freq1 - null_frac).clamp(0.0, 1.0);
    (match_freq1 + uncertain_frac * uncertain).clamp(0.0, 1.0)
}

/// `ineqjoinsel`: a bound between two columns keeps a third, without
/// statistics to say otherwise.
pub(crate) fn ineq_join_selectivity() -> f64 {
    DEFAULT_INEQ_SEL
}

/// `neqjoinsel` for a semi or anti join: every non-null outer row has a
/// partner to differ from.
pub(crate) fn neq_semi_join_selectivity(outer: &JoinSide) -> f64 {
    (1.0 - outer.null_frac()).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The estimates `EXPLAIN` prints come from the statistics `ANALYZE`
    /// recorded — PostgreSQL 18.4's own selectivity model, so these are its
    /// row counts for the same data (verified against it query by query).
    #[test]
    fn estimates_read_the_recorded_statistics() {
        let (sql, _) = crate::constraint_tests::session();
        let explain = |query: &str| {
            let out = sql(&format!("explain {query}")).unwrap();
            crate::render(&out.rows[0].values[0])
        };
        sql("create table es (a int, b int, c text, big bigint, d numeric(10,2))").unwrap();
        sql(
            "insert into es select g % 10, g % 7, 'x' || (g % 40), g * 1000000, \
             (g % 25)::numeric from generate_series(1, 2000) g",
        )
        .unwrap();
        sql("analyze es").unwrap();

        // An equality: a most common value's own frequency, an absent
        // value's the rest of the distribution's.
        assert!(explain("select * from es where a = 3").contains("rows=200 width=23"));
        assert!(explain("select * from es where a = 100").contains("rows=1 width=23"));
        assert!(explain("select * from es where c = 'x5'").contains("rows=50 width=23"));
        // A range in the histogram.
        assert!(explain("select * from es where c < 'x20'").contains("rows=650 width=23"));
        assert!(explain("select * from es where big < 500000000").contains("rows=499 width=23"));
        // Two bounds on one column merge as PostgreSQL's clausesel does.
        assert!(explain("select * from es where d >= 12 and d < 18").contains("rows=480 width=23"));
        assert!(explain("select * from es where a > 5 and a <= 8").contains("rows=600 width=23"));
        // `IS NOT NULL` and the inequalities take the null fraction and the
        // distinct estimate.
        assert!(explain("select * from es where a is not null").contains("rows=2000 width=23"));
        assert!(explain("select * from es where a <> 3 and b <> 2").contains("rows=1543 width=23"));

        // An unanalyzed table falls back to the shape defaults and the
        // declared types' widths.
        sql("create table na (a int, t text)").unwrap();
        sql("insert into na select g, 'v' || g from generate_series(1, 1000) g").unwrap();
        assert!(explain("select * from na where t = 'v5'").contains("rows=5 width=36"));
        sql("drop table na").unwrap();

        // Functional dependencies and the multivariate MCV list.
        sql("create table dep (zip int, city text, state text)").unwrap();
        sql(
            "insert into dep select g % 50, 'city' || (g % 50), 'st' || (g % 10) \
             from generate_series(1, 500) g",
        )
        .unwrap();
        sql("create statistics dep_st (dependencies, mcv) on zip, city from dep").unwrap();
        sql("analyze dep").unwrap();
        assert!(
            explain("select * from dep where zip = 3 and city = 'city3'")
                .contains("rows=10 width=14")
        );
        assert!(explain("select * from dep where zip = 3").contains("rows=10 width=14"));
        assert!(
            explain("select * from dep where zip = 3 and city = 'city4'")
                .contains("rows=1 width=14")
        );
        assert!(
            explain("select * from dep where zip > 40 and city = 'city45'")
                .contains("rows=10 width=14")
        );
        sql("drop table es").unwrap();
        sql("drop table dep").unwrap();

        // An index scan's rows: a unique equality is one, a non-unique
        // one's the column's own estimate, a range's the histogram's — and
        // a wide one reads through a bitmap, as PostgreSQL plans it.
        sql("create table ix (id int primary key, t text, n int)").unwrap();
        sql("insert into ix select g, 'v' || (g % 500), g % 3 from generate_series(1, 5000) g")
            .unwrap();
        sql("create index ix_n on ix (n)").unwrap();
        sql("analyze ix").unwrap();
        assert!(explain("select * from ix where n = 1").contains("Bitmap Heap Scan on ix"));
        let plan = explain("select * from ix where n = 1");
        assert!(plan.contains("rows=1667 width=12"), "{plan}");
        sql("set enable_seqscan = off").unwrap();
        assert!(explain("select * from ix where id = 42").contains("rows=1 width=12"));
        assert!(explain("select * from ix where id < 100").contains("rows=99 width=12"));
    }
}
