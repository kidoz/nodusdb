//! Cursors (`DECLARE`, `FETCH`, `MOVE`, `CLOSE`). A cursor holds its
//! query's result, read when it is declared (so from the transaction's
//! snapshot then, as PostgreSQL's cursor reads it), and a position in it
//! that each fetch moves. A cursor ends with its transaction unless it is
//! declared `WITH HOLD` and the transaction commits.

use std::collections::BTreeMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::error_fields::DbError;
use crate::{ExecutionContext, LogicalPlan, MemExecutor, QueryOutput, Row, Value};

/// Where a fetch goes, as PostgreSQL's `FETCH` directions reduce to: `NEXT`
/// is `Forward(1)`, `PRIOR` `Backward(1)`, `FIRST` `Absolute(1)`, `LAST`
/// `Absolute(-1)`, and `ALL` a count of [`i64::MAX`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FetchDirection {
    Forward(i64),
    Backward(i64),
    Absolute(i64),
    Relative(i64),
}

impl FetchDirection {
    /// A direction as written (`relative - 2`, `forward all`, empty for
    /// `NEXT`).
    pub(crate) fn parse(text: &str) -> Result<Self> {
        let lower = text.to_ascii_lowercase();
        let words: Vec<&str> = lower.split_whitespace().collect();
        // A count, signed; `- 2` is two words.
        let count = |words: &[&str]| -> Result<i64> {
            let text = words.concat();
            text.parse::<i64>()
                .map_err(|_| anyhow::anyhow!("syntax error at or near \"{text}\""))
        };
        let signed = |n: i64| {
            if n < 0 {
                Self::Backward(n.saturating_neg())
            } else {
                Self::Forward(n)
            }
        };
        Ok(match words.as_slice() {
            [] | ["next"] => Self::Forward(1),
            ["prior"] => Self::Backward(1),
            ["first"] => Self::Absolute(1),
            ["last"] => Self::Absolute(-1),
            ["all"] | ["forward", "all"] => Self::Forward(i64::MAX),
            ["backward", "all"] => Self::Backward(i64::MAX),
            ["forward"] => Self::Forward(1),
            ["backward"] => Self::Backward(1),
            ["absolute", rest @ ..] => Self::Absolute(count(rest)?),
            ["relative", rest @ ..] => Self::Relative(count(rest)?),
            ["forward", rest @ ..] => signed(count(rest)?),
            ["backward", rest @ ..] => match signed(count(rest)?) {
                Self::Forward(n) => Self::Backward(n),
                Self::Backward(n) => Self::Forward(n),
                other => other,
            },
            rest => signed(count(rest)?),
        })
    }
}

/// An open cursor.
pub(crate) struct Cursor {
    /// The `DECLARE` statement, as `pg_cursors` shows it.
    statement: String,
    columns: Vec<String>,
    types: Vec<String>,
    rows: Vec<Row>,
    /// 0 before the first row, `rows.len() + 1` after the last, else the
    /// row it is on (from 1).
    position: usize,
    /// Whether it may move backward.
    scroll: bool,
    hold: bool,
    binary: bool,
    /// The transaction that declared it, until it commits.
    txn_id: Option<nodus_storage_api::TxnId>,
    /// When it was declared, microseconds since the epoch.
    created: i64,
}

/// A session's cursors, by name.
pub(crate) type Cursors = BTreeMap<String, Cursor>;

impl Cursor {
    /// The rows a move in `direction` passes (their positions, in the order
    /// fetched), where it leaves the cursor, and whether it goes backward.
    fn plan_move(&self, direction: FetchDirection) -> (Vec<usize>, usize, bool) {
        let n = self.rows.len();
        let p = self.position;
        let on_row = (1..=n).contains(&p);
        match direction {
            // A count of 0 fetches the current row again, backing up to it.
            FetchDirection::Forward(0)
            | FetchDirection::Backward(0)
            | FetchDirection::Relative(0) => {
                if on_row {
                    (vec![p], p, true)
                } else {
                    (Vec::new(), p, false)
                }
            }
            FetchDirection::Forward(k) => {
                let k = k.max(0) as u64;
                let last = (p as u64).saturating_add(k);
                let rows = ((p + 1)..=n.min(last.min(n as u64) as usize)).collect();
                let end = if last > n as u64 {
                    n + 1
                } else {
                    last as usize
                };
                (rows, end, false)
            }
            FetchDirection::Backward(k) => {
                let k = k.max(0) as u64;
                let first = (p as u64).saturating_sub(k);
                let from = p.min(n + 1);
                let rows = (first.max(1) as usize..from).rev().collect();
                let end = if k >= p as u64 { 0 } else { first as usize };
                (rows, end, true)
            }
            FetchDirection::Absolute(k) => {
                let target = if k >= 0 {
                    k as u64
                } else {
                    (n as u64 + 1).saturating_sub(k.unsigned_abs())
                };
                // Reaching a row at or before the current one rewinds.
                let backward = k < 0 || (p > 0 && target <= p as u64);
                if target >= 1 && target <= n as u64 {
                    (vec![target as usize], target as usize, backward)
                } else if k > 0 {
                    (Vec::new(), n + 1, backward)
                } else {
                    (Vec::new(), 0, backward || p > 0)
                }
            }
            FetchDirection::Relative(k) => {
                let target = p as i128 + i128::from(k);
                let backward = k < 0;
                if target >= 1 && target <= n as i128 {
                    (vec![target as usize], target as usize, backward)
                } else if target < 1 {
                    (Vec::new(), 0, backward)
                } else {
                    (Vec::new(), n + 1, backward)
                }
            }
        }
    }
}

impl MemExecutor {
    /// `DECLARE`: runs the query and keeps its result under the name.
    pub(crate) fn exec_declare_cursor(
        &self,
        ctx: &ExecutionContext,
        name: String,
        query: LogicalPlan,
        scroll: Option<bool>,
        hold: bool,
        binary: bool,
        statement: String,
    ) -> Result<QueryOutput> {
        let txn = self
            .active_txns
            .read()
            .get(&ctx.session_id)
            .map(|t| (t.txn_id, t.explicit));
        if !hold && !txn.is_some_and(|(_, explicit)| explicit) {
            return Err(
                DbError::new("DECLARE CURSOR can only be used in transaction blocks")
                    .code("25P01")
                    .into(),
            );
        }
        if self
            .cursors
            .lock()
            .get(&ctx.session_id)
            .is_some_and(|c| c.contains_key(&name))
        {
            return Err(DbError::new(format!("cursor \"{name}\" already exists"))
                .code("42P03")
                .into());
        }
        // Without SCROLL or NO SCROLL, a query that reads a relation can
        // move backward (as PostgreSQL's plans for one can), a lone row not.
        let scroll = scroll.unwrap_or(!matches!(query, LogicalPlan::SelectLiteral { .. }));
        let out = self.execute_logical_inner(ctx, query)?;
        let cursor = Cursor {
            statement,
            columns: out.columns,
            types: out.types,
            rows: out.rows,
            position: 0,
            scroll,
            hold,
            binary,
            txn_id: txn.filter(|(_, explicit)| *explicit).map(|(id, _)| id),
            created: crate::session_env::wall_micros(),
        };
        self.cursors
            .lock()
            .entry(ctx.session_id.clone())
            .or_default()
            .insert(name, cursor);
        Ok(QueryOutput::tag("DECLARE CURSOR"))
    }

    /// `FETCH` (the rows passed) and `MOVE` (their count).
    pub(crate) fn exec_fetch_cursor(
        &self,
        ctx: &ExecutionContext,
        name: &str,
        direction: FetchDirection,
        move_only: bool,
    ) -> Result<QueryOutput> {
        let mut all = self.cursors.lock();
        let Some(cursor) = all.get_mut(&ctx.session_id).and_then(|c| c.get_mut(name)) else {
            return Err(DbError::new(format!("cursor \"{name}\" does not exist"))
                .code("34000")
                .into());
        };
        let (positions, end, backward) = cursor.plan_move(direction);
        // `MOVE 0` only tells whether the cursor is on a row.
        if move_only
            && matches!(
                direction,
                FetchDirection::Forward(0)
                    | FetchDirection::Backward(0)
                    | FetchDirection::Relative(0)
            )
        {
            return Ok(QueryOutput::tag(&format!("MOVE {}", positions.len())));
        }
        if backward && !cursor.scroll {
            return Err(DbError::new("cursor can only scan forward")
                .code("55000")
                .hint("Declare it with SCROLL option to enable backward scan.")
                .into());
        }
        cursor.position = end;
        if move_only {
            return Ok(QueryOutput::tag(&format!("MOVE {}", positions.len())));
        }
        let rows: Vec<Row> = positions
            .iter()
            .map(|&at| cursor.rows[at - 1].clone())
            .collect();
        Ok(QueryOutput {
            columns: cursor.columns.clone(),
            types: cursor.types.clone(),
            tag: format!("FETCH {}", rows.len()),
            rows,
        })
    }

    /// `CLOSE name` / `CLOSE ALL`.
    pub(crate) fn exec_close_cursor(
        &self,
        ctx: &ExecutionContext,
        name: Option<String>,
    ) -> Result<QueryOutput> {
        let mut all = self.cursors.lock();
        let Some(name) = name else {
            all.remove(&ctx.session_id);
            return Ok(QueryOutput::tag("CLOSE CURSOR ALL"));
        };
        if all
            .get_mut(&ctx.session_id)
            .and_then(|c| c.remove(&name))
            .is_none()
        {
            return Err(DbError::new(format!("cursor \"{name}\" does not exist"))
                .code("34000")
                .into());
        }
        Ok(QueryOutput::tag("CLOSE CURSOR"))
    }

    /// A transaction's end: its cursors close, but for the `WITH HOLD` ones
    /// of a commit, which stay open on their own.
    pub(crate) fn end_transaction_cursors(
        &self,
        session_id: &str,
        txn_id: nodus_storage_api::TxnId,
        committed: bool,
    ) {
        let mut all = self.cursors.lock();
        let Some(cursors) = all.get_mut(session_id) else {
            return;
        };
        cursors.retain(|_, c| {
            let own = c.txn_id == Some(txn_id);
            if committed && c.hold {
                if own {
                    c.txn_id = None;
                }
                true
            } else {
                !own && c.txn_id.is_none()
            }
        });
    }

    /// `pg_cursors`: the session's open cursors.
    pub(crate) fn pg_cursors_virtual_table(
        &self,
    ) -> (Vec<nodus_catalog::ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("name", "TEXT"),
            ("statement", "TEXT"),
            ("is_holdable", "BOOL"),
            ("is_binary", "BOOL"),
            ("is_scrollable", "BOOL"),
            ("creation_time", "TIMESTAMPTZ"),
        ]);
        let session = crate::session_env::with(|env| env.map(|e| e.session_id.clone()));
        let all = self.cursors.lock();
        let rows = session
            .and_then(|s| all.get(&s))
            .map(|cursors| {
                cursors
                    .iter()
                    .map(|(name, c)| {
                        vec![
                            Value::Text(name.clone()),
                            Value::Text(c.statement.clone()),
                            Value::Bool(c.hold),
                            Value::Bool(c.binary),
                            Value::Bool(c.scroll),
                            chrono::DateTime::from_timestamp_micros(c.created).map_or(
                                Value::Null,
                                |dt| {
                                    crate::datetime::Temporal::TimestampTz(dt.naive_utc())
                                        .to_value()
                                },
                            ),
                        ]
                    })
                    .collect()
            })
            .unwrap_or_default();
        (cols, rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(n: usize, position: usize) -> Cursor {
        Cursor {
            statement: String::new(),
            columns: vec!["id".into()],
            types: vec!["INT".into()],
            rows: (1..=n)
                .map(|i| Row {
                    values: vec![Value::Int(i as i64)],
                })
                .collect(),
            position,
            scroll: true,
            hold: false,
            binary: false,
            txn_id: None,
            created: 0,
        }
    }

    #[test]
    fn directions_parse_as_postgresql_reads_them() {
        use FetchDirection::*;
        assert_eq!(FetchDirection::parse("").unwrap(), Forward(1));
        assert_eq!(FetchDirection::parse("2").unwrap(), Forward(2));
        assert_eq!(FetchDirection::parse("- 2").unwrap(), Backward(2));
        assert_eq!(FetchDirection::parse("relative - 2").unwrap(), Relative(-2));
        assert_eq!(FetchDirection::parse("backward - 1").unwrap(), Forward(1));
        assert_eq!(FetchDirection::parse("LAST").unwrap(), Absolute(-1));
        assert_eq!(
            FetchDirection::parse("forward all").unwrap(),
            Forward(i64::MAX)
        );
        assert!(FetchDirection::parse("sideways").is_err());
    }

    #[test]
    fn moves_follow_postgresql_positions() {
        use FetchDirection::*;
        // From the start, two forward: rows 1 and 2, on row 2.
        assert_eq!(cursor(10, 0).plan_move(Forward(2)), (vec![1, 2], 2, false));
        // Past the end: the rest, after the last row.
        assert_eq!(
            cursor(10, 8).plan_move(Forward(i64::MAX)),
            (vec![9, 10], 11, false)
        );
        assert_eq!(cursor(10, 11).plan_move(Forward(1)), (vec![], 11, false));
        // LAST, then PRIOR.
        assert_eq!(cursor(10, 0).plan_move(Absolute(-1)), (vec![10], 10, true));
        assert_eq!(cursor(10, 10).plan_move(Backward(1)), (vec![9], 9, true));
        // RELATIVE -2 from row 5, then BACKWARD 2 from row 3.
        assert_eq!(cursor(10, 5).plan_move(Relative(-2)), (vec![3], 3, true));
        assert_eq!(cursor(10, 3).plan_move(Backward(2)), (vec![2, 1], 1, true));
        // Backward from after the end starts at the last row.
        assert_eq!(cursor(3, 4).plan_move(Backward(2)), (vec![3, 2], 2, true));
        assert_eq!(cursor(3, 2).plan_move(Backward(5)), (vec![1], 0, true));
        // A count of 0 is the current row, if the cursor is on one.
        assert_eq!(cursor(3, 0).plan_move(Forward(0)), (vec![], 0, false));
        assert_eq!(cursor(3, 2).plan_move(Relative(0)), (vec![2], 2, true));
        // ABSOLUTE beyond the rows leaves it after the last one.
        assert_eq!(cursor(3, 0).plan_move(Absolute(5)), (vec![], 4, false));
    }
}
