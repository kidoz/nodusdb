//! `TABLESAMPLE`: the rows a sampled scan keeps.
//!
//! The clause's semantics are `bernoulli.c`'s: the scan keeps each row when
//! its draw from the sampler's own generator (`random::State`, which no
//! `random()` call shares) times 100 is below the percentage; the percentage
//! and the `REPEATABLE` seed are PostgreSQL-typed (a `float4` percentage, a
//! `float8` seed) and checked before the scan. `system` sampling works at
//! block granularity in PostgreSQL; NodusDB's rows have no blocks, so it
//! samples them like `bernoulli` — a documented divergence.

use crate::{ExecutionContext, MemExecutor, SampleSpec, Value};
use anyhow::Result;

/// A scan's sampler: its generator, and the percentage it keeps rows by.
pub(crate) struct Sampler {
    state: crate::random::State,
    percent: f64,
}

impl Sampler {
    /// A sampler for one scan: from the `REPEATABLE` seed, or from entropy.
    pub(crate) fn new(seed: Option<f64>, percent: f64) -> Sampler {
        let seed = match seed {
            Some(seed) => seed as u64,
            None => u64::from_le_bytes(
                uuid::Uuid::new_v4().as_bytes()[..8]
                    .try_into()
                    .unwrap_or([0; 8]),
            ),
        };
        Sampler {
            state: crate::random::State::seeded(seed),
            percent,
        }
    }

    /// Whether the next row is kept, as `sampler_random_fract() * 100.0 <
    /// percent` decides it.
    pub(crate) fn keeps(&mut self) -> bool {
        self.state.double() * 100.0 < self.percent
    }
}

/// A parameter as the `float4`/`float8` PostgreSQL coerces it to.
fn to_float(value: &Value) -> Result<f64> {
    let number = match value {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Numeric(d) => Some(crate::value::decimal_to_f64(d)),
        Value::Text(s) => s.trim().parse().ok(),
        _ => None,
    };
    number.ok_or_else(|| {
        anyhow::anyhow!(
            crate::error_fields::DbError::new(format!(
                "invalid input syntax for type real: \"{}\"",
                crate::value::render(value)
            ))
            .code("22P02")
            .into_text()
        )
    })
}

/// The percentage a `TABLESAMPLE` clause samples with, checked as
/// `tablesample_init` checks it.
pub(crate) fn checked_percent(value: &Value) -> Result<f64> {
    if value == &Value::Null {
        anyhow::bail!(sample_error("TABLESAMPLE parameter cannot be null"));
    }
    let percent = to_float(value)? as f32 as f64;
    if percent.is_nan() || !(0.0..=100.0).contains(&percent) {
        anyhow::bail!(sample_error("sample percentage must be between 0 and 100"));
    }
    Ok(percent)
}

/// The `REPEATABLE` seed, checked as `tablesample_init` checks it.
pub(crate) fn checked_seed(value: &Value) -> Result<f64> {
    if value == &Value::Null {
        anyhow::bail!(repeat_error());
    }
    to_float(value)
}

/// The error a sampled scan raises about its arguments: `2202H`
/// (`invalid_tablesample_argument`), which PostgreSQL uses for both the
/// percentage and a null parameter.
fn sample_error(message: &str) -> String {
    crate::error_fields::DbError::new(message)
        .code("2202H")
        .into_text()
}

/// The error a `TABLESAMPLE` on something that is not a table — a view, a
/// CTE — raises, as `transformFromClauseItem` raises it.
pub(crate) fn relation_error() -> String {
    crate::error_fields::DbError::new(
        "TABLESAMPLE clause can only be applied to tables and materialized views",
    )
    .code("0A000")
    .into_text()
}

/// The null-seed error, `2202G` (`invalid_tablesample_repeat`).
fn repeat_error() -> String {
    crate::error_fields::DbError::new("TABLESAMPLE REPEATABLE parameter cannot be null")
        .code("2202G")
        .into_text()
}

impl MemExecutor {
    /// The rows of one scan under its `TABLESAMPLE` clause. The percentage
    /// and seed are evaluated once, here, and a failure fails the statement.
    pub(crate) fn sampled_rows(
        &self,
        ctx: &ExecutionContext,
        spec: &SampleSpec,
        rows: Vec<Vec<Value>>,
    ) -> Result<Vec<Vec<Value>>> {
        let percent = checked_percent(&self.eval_expr(ctx, &spec.percent, &[], &[]))?;
        let seed = match &spec.seed {
            Some(seed) => Some(checked_seed(&self.eval_expr(ctx, seed, &[], &[]))?),
            None => None,
        };
        let mut sampler = Sampler::new(seed, percent);
        Ok(rows.into_iter().filter(|_| sampler.keeps()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dml_join_tests::session;

    /// One statement's single value, rendered.
    fn value(sql: &impl Fn(&str) -> anyhow::Result<crate::QueryOutput>, statement: &str) -> String {
        let out = sql(statement).unwrap();
        assert_eq!(out.rows.len(), 1, "{statement}");
        crate::render(&out.rows[0].values[0])
    }

    /// Its message, without the field suffix a `DbError` carries.
    fn message(error: &anyhow::Error) -> String {
        crate::error_fields::error_message(&error.to_string()).to_string()
    }

    #[test]
    fn scans_sample_through_sql() {
        let sql = session();
        sql("create table st(a int)").unwrap();
        sql("insert into st select generate_series(1, 100)").unwrap();
        assert_eq!(
            value(&sql, "select count(*) from st tablesample bernoulli (0)"),
            "0"
        );
        assert_eq!(
            value(&sql, "select count(*) from st tablesample system (0)"),
            "0"
        );
        assert_eq!(
            value(&sql, "select count(*) from st tablesample bernoulli (100)"),
            "100"
        );
        assert_eq!(
            value(
                &sql,
                "select count(*) from st tablesample bernoulli (100) where a > 50"
            ),
            "50"
        );
        // The same seed draws the same sample; the percentage samples.
        let draw = |seed: i64| {
            value(
                &sql,
                &format!(
                    "select string_agg(a::text, ',' order by a) from st \
                     tablesample bernoulli (50) repeatable ({seed})"
                ),
            )
        };
        assert_eq!(draw(7), draw(7));
        assert_ne!(draw(7), draw(8), "a different seed draws differently");
        let kept = draw(7).split(',').count();
        assert!((40..60).contains(&kept), "about half: {kept}");
    }

    #[test]
    fn refusals_match_postgresql() {
        let sql = session();
        sql("create table st(a int)").unwrap();
        sql("create view v_st as select * from st").unwrap();
        for (statement, expected) in [
            (
                "select count(*) from st tablesample bernoulli (101)",
                "sample percentage must be between 0 and 100",
            ),
            (
                "select count(*) from st tablesample bernoulli (-1)",
                "sample percentage must be between 0 and 100",
            ),
            (
                "select count(*) from st tablesample bernoulli (NULL)",
                "TABLESAMPLE parameter cannot be null",
            ),
            (
                "select count(*) from st tablesample vacuum (10)",
                "tablesample method vacuum does not exist",
            ),
            (
                "select count(*) from st tablesample block (10)",
                "tablesample method block does not exist",
            ),
            (
                "select count(*) from st tablesample row (10)",
                "syntax error at or near \"(\"",
            ),
            (
                "select count(*) from st tablesample bernoulli (10 percent)",
                "syntax error at or near \"percent\"",
            ),
            (
                "select count(*) from (select * from st) s tablesample bernoulli (0)",
                "syntax error at or near \"tablesample\"",
            ),
            (
                "select count(*) from v_st tablesample bernoulli (0)",
                "TABLESAMPLE clause can only be applied to tables and materialized views",
            ),
            (
                "with c as (select * from st) select count(*) from c tablesample bernoulli (0)",
                "TABLESAMPLE clause can only be applied to tables and materialized views",
            ),
        ] {
            let err = sql(statement).unwrap_err();
            assert_eq!(message(&err), expected, "{statement}");
        }
    }

    #[test]
    fn bounds_are_deterministic() {
        // 0% keeps nothing and 100% keeps everything, whatever the seed.
        let mut none = Sampler::new(Some(1.0), 0.0);
        let mut all = Sampler::new(Some(1.0), 100.0);
        assert!((0..100).all(|_| !none.keeps()));
        assert!((0..100).all(|_| all.keeps()));
    }

    #[test]
    fn a_seed_repeats_and_a_percentage_samples() {
        let draw = |seed: Option<f64>| {
            let mut sampler = Sampler::new(seed, 50.0);
            (0..1000).filter(|_| sampler.keeps()).count()
        };
        assert_eq!(draw(Some(7.0)), draw(Some(7.0)), "the same seed repeats");
        let kept = draw(Some(7.0));
        assert!((400..600).contains(&kept), "about half: {kept}");
    }

    #[test]
    fn percentages_are_checked() {
        assert!(checked_percent(&Value::Float(10.0)).is_ok());
        assert!(checked_percent(&Value::Int(100)).is_ok());
        assert!(checked_percent(&Value::Text("10".into())).is_ok());
        for bad in [Value::Int(-1), Value::Float(101.0), Value::Float(f64::NAN)] {
            let err = checked_percent(&bad).unwrap_err().to_string();
            assert!(
                err.contains("sample percentage must be between 0 and 100"),
                "{err}"
            );
        }
        let err = checked_percent(&Value::Null).unwrap_err().to_string();
        assert!(
            err.contains("TABLESAMPLE parameter cannot be null"),
            "{err}"
        );
        let err = checked_seed(&Value::Null).unwrap_err().to_string();
        assert!(
            err.contains("TABLESAMPLE REPEATABLE parameter cannot be null"),
            "{err}"
        );
    }
}
