//! Sequences (`CREATE SEQUENCE`, `nextval`, `setval`, `serial` and identity
//! columns).
//!
//! As in PostgreSQL, a sequence is a relation holding one row: a table whose
//! columns are [`SEQUENCE_COLUMNS`] (PostgreSQL's `last_value`, `log_cnt`, and
//! `is_called`, then the sequence's options). It is stored and replicated like
//! any table, so it needs no catalog format of its own. `nextval` and `setval`
//! advance it in their own transaction, committed at once, so a value handed
//! out is never handed out again, even if the caller rolls back.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use bytes::Bytes;
use nodus_catalog::{CatalogReader, TableDescriptor, TableId};
use nodus_storage_api::{KeyRange, KvEngine};
use nodus_txn::TxnManager;
use serde::{Deserialize, Serialize};

use crate::Value;

/// A sequence relation's columns, in order.
pub(crate) const SEQUENCE_COLUMNS: [(&str, &str); 10] = [
    ("last_value", "BIGINT"),
    ("log_cnt", "BIGINT"),
    ("is_called", "BOOLEAN"),
    ("start_value", "BIGINT"),
    ("min_value", "BIGINT"),
    ("max_value", "BIGINT"),
    ("increment_by", "BIGINT"),
    ("cycle", "BOOLEAN"),
    ("cache_size", "BIGINT"),
    ("data_type", "TEXT"),
];

/// How many times `nextval`/`setval` retry after losing a write race.
const MAX_ATTEMPTS: usize = 64;

/// Whether a table is a sequence relation.
pub(crate) fn is_sequence(tbl: &TableDescriptor) -> bool {
    tbl.columns.len() == SEQUENCE_COLUMNS.len()
        && tbl
            .columns
            .iter()
            .zip(SEQUENCE_COLUMNS)
            .all(|(c, (name, _))| c.name == name)
}

/// The name of the sequence a `serial` or identity column draws from.
pub(crate) fn owned_sequence_name(table: &str, column: &str) -> String {
    format!("{table}_{column}_seq")
}

/// A sequence's definition, as `CREATE SEQUENCE` (or a `serial`/identity
/// column) specifies it. Unset bounds take the defaults for the data type and
/// direction.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SequenceSpec {
    /// `smallint`, `integer`, or `bigint` (the default).
    pub data_type: Option<String>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub start: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: bool,
}

/// A sequence's stored state and options.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SequenceState {
    pub(crate) last_value: i64,
    pub(crate) is_called: bool,
    pub(crate) start: i64,
    pub(crate) min: i64,
    pub(crate) max: i64,
    pub(crate) increment: i64,
    pub(crate) cycle: bool,
    pub(crate) cache: i64,
    pub(crate) data_type: String,
}

impl SequenceState {
    /// The initial state for a definition, with PostgreSQL's defaults and
    /// validation.
    pub(crate) fn new(spec: &SequenceSpec) -> Result<Self> {
        let data_type = match spec
            .data_type
            .as_deref()
            .map(|t| t.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("bigint" | "int8") => "bigint",
            Some("integer" | "int" | "int4") => "integer",
            Some("smallint" | "int2") => "smallint",
            Some(other) => bail!("sequence type must be smallint, integer, or bigint, not {other}"),
        };
        let (type_min, type_max) = match data_type {
            "smallint" => (i16::MIN as i64, i16::MAX as i64),
            "integer" => (i32::MIN as i64, i32::MAX as i64),
            _ => (i64::MIN, i64::MAX),
        };
        let increment = spec.increment.unwrap_or(1);
        if increment == 0 {
            bail!("INCREMENT must not be zero");
        }
        let min = spec
            .min_value
            .unwrap_or(if increment > 0 { 1 } else { type_min });
        let max = spec
            .max_value
            .unwrap_or(if increment > 0 { type_max } else { -1 });
        if min < type_min || max > type_max {
            bail!("MINVALUE and MAXVALUE must be within the range of type {data_type}");
        }
        if min >= max {
            bail!("MINVALUE ({min}) must be less than MAXVALUE ({max})");
        }
        let start = spec.start.unwrap_or(if increment > 0 { min } else { max });
        if start < min {
            bail!("START value ({start}) cannot be less than MINVALUE ({min})");
        }
        if start > max {
            bail!("START value ({start}) cannot be greater than MAXVALUE ({max})");
        }
        let cache = spec.cache.unwrap_or(1);
        if cache < 1 {
            bail!("CACHE ({cache}) must be greater than zero");
        }
        Ok(SequenceState {
            last_value: start,
            is_called: false,
            start,
            min,
            max,
            increment,
            cycle: spec.cycle,
            cache,
            data_type: data_type.to_string(),
        })
    }

    /// The stored row, in [`SEQUENCE_COLUMNS`] order.
    pub(crate) fn to_row(&self) -> Vec<Value> {
        vec![
            Value::Int(self.last_value),
            Value::Int(0),
            Value::Bool(self.is_called),
            Value::Int(self.start),
            Value::Int(self.min),
            Value::Int(self.max),
            Value::Int(self.increment),
            Value::Bool(self.cycle),
            Value::Int(self.cache),
            Value::Text(self.data_type.clone()),
        ]
    }

    fn from_row(row: &[Value]) -> Result<Self> {
        let int = |i: usize| match row.get(i) {
            Some(Value::Int(v)) => Ok(*v),
            _ => bail!("sequence row is malformed"),
        };
        let boolean = |i: usize| match row.get(i) {
            Some(Value::Bool(v)) => Ok(*v),
            _ => bail!("sequence row is malformed"),
        };
        Ok(SequenceState {
            last_value: int(0)?,
            is_called: boolean(2)?,
            start: int(3)?,
            min: int(4)?,
            max: int(5)?,
            increment: int(6)?,
            cycle: boolean(7)?,
            cache: int(8)?,
            data_type: match row.get(9) {
                Some(Value::Text(t)) => t.clone(),
                _ => "bigint".to_string(),
            },
        })
    }

    /// The value `nextval` hands out next.
    fn next(&self, name: &str) -> Result<i64> {
        if !self.is_called {
            return Ok(self.last_value);
        }
        match self.last_value.checked_add(self.increment) {
            Some(next) if (self.min..=self.max).contains(&next) => Ok(next),
            _ if self.cycle => Ok(if self.increment > 0 {
                self.min
            } else {
                self.max
            }),
            _ if self.increment > 0 => bail!(
                "nextval: reached maximum value of sequence \"{name}\" ({})",
                self.max
            ),
            _ => bail!(
                "nextval: reached minimum value of sequence \"{name}\" ({})",
                self.min
            ),
        }
    }
}

/// The sequence a column default draws from: `nextval('s')` (a `serial`
/// column) or an identity column's generator.
pub(crate) fn default_sequence(default: &crate::ScalarExpr) -> Option<String> {
    match default {
        crate::ScalarExpr::Function { name, args }
            if (name == "NEXTVAL" && args.len() == 1)
                || (name == "__IDENTITY__" && args.len() == 2) =>
        {
            match args.first() {
                Some(crate::ScalarExpr::Literal(Value::Text(sequence))) => Some(sequence.clone()),
                Some(crate::ScalarExpr::Cast { expr, .. }) => match &**expr {
                    crate::ScalarExpr::Literal(Value::Text(sequence)) => Some(sequence.clone()),
                    _ => None,
                },
                _ => None,
            }
        }
        _ => None,
    }
}

/// Whether a column default makes it an identity column, and if so whether
/// it is `GENERATED ALWAYS` (`Some(true)`) or `BY DEFAULT` (`Some(false)`).
pub(crate) fn identity_kind(default: &crate::ScalarExpr) -> Option<bool> {
    match default {
        crate::ScalarExpr::Function { name, args } if name == "__IDENTITY__" && args.len() == 2 => {
            Some(matches!(
                args[1],
                crate::ScalarExpr::Literal(Value::Bool(true))
            ))
        }
        _ => None,
    }
}

/// A session's values from `nextval`, for `currval` and `lastval`.
#[derive(Default)]
struct SessionValues {
    by_sequence: HashMap<TableId, i64>,
    last: Option<i64>,
}

/// Sequence operations that scalar functions reach during a statement.
pub(crate) struct SequenceStore {
    catalog: Arc<dyn CatalogReader>,
    kv: Arc<dyn KvEngine>,
    txn: Arc<dyn TxnManager>,
    sessions: parking_lot::RwLock<HashMap<String, SessionValues>>,
}

impl SequenceStore {
    pub(crate) fn new(
        catalog: Arc<dyn CatalogReader>,
        kv: Arc<dyn KvEngine>,
        txn: Arc<dyn TxnManager>,
    ) -> Self {
        SequenceStore {
            catalog,
            kv,
            txn,
            sessions: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Forgets a finished session's `currval` state.
    pub(crate) fn end_session(&self, session: &str) {
        self.sessions.write().remove(session);
    }

    /// Resolves a sequence named as `nextval` takes it: `'s'`, `'schema.s'`,
    /// or `'"MixedCase"'` (unquoted parts fold to lower case).
    pub(crate) fn resolve(&self, name: &str) -> Result<TableDescriptor> {
        let parts: Vec<String> = name
            .split('.')
            .map(|part| {
                let part = part.trim();
                match part.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
                    Some(quoted) => quoted.to_string(),
                    None => part.to_ascii_lowercase(),
                }
            })
            .collect();
        let (schema, table) = match parts.as_slice() {
            [table] => ("public", table.as_str()),
            [schema, table] => (schema.as_str(), table.as_str()),
            [_, schema, table] => (schema.as_str(), table.as_str()),
            _ => bail!("improper relation name (too many dotted names): {name}"),
        };
        let tbl = self
            .catalog
            .get_table("default", schema, table)
            .map_err(|_| anyhow::anyhow!("relation \"{name}\" does not exist"))?;
        if !is_sequence(&tbl) {
            bail!("\"{name}\" is not a sequence");
        }
        Ok(tbl)
    }

    /// `pg_get_serial_sequence(table, column)`: the sequence a `serial` or
    /// identity column draws from, schema-qualified; `None` for a column
    /// without one.
    pub(crate) fn owned_sequence(&self, table: &str, column: &str) -> Result<Option<String>> {
        let (schema, name) = match table.split_once('.') {
            Some((schema, name)) => (schema.to_string(), name.to_string()),
            None => ("public".to_string(), table.to_string()),
        };
        let fold = |s: &str| match s.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
            Some(quoted) => quoted.to_string(),
            None => s.to_ascii_lowercase(),
        };
        let (schema, name) = (fold(&schema), fold(&name));
        let tbl = self
            .catalog
            .get_table("default", &schema, &name)
            .map_err(|_| anyhow::anyhow!("relation \"{table}\" does not exist"))?;
        let col = tbl
            .columns
            .iter()
            .find(|c| c.name == column)
            .ok_or_else(|| {
                anyhow::anyhow!("column \"{column}\" of relation \"{name}\" does not exist")
            })?;
        Ok(col
            .default_expr
            .as_deref()
            .and_then(|json| serde_json::from_str::<crate::ScalarExpr>(json).ok())
            .and_then(|default| default_sequence(&default))
            .map(|sequence| format!("{schema}.{sequence}")))
    }

    /// Stores a new sequence's initial state, committed at once.
    pub(crate) fn initialize(&self, tbl: &TableDescriptor, state: &SequenceState) -> Result<()> {
        let record = self.txn.begin_txn()?;
        let key = format!("{}:0", tbl.id);
        let result = (|| -> Result<()> {
            self.txn
                .track_write(record.txn_id, key.as_bytes().to_vec())?;
            self.kv.write_intent(
                record.txn_id,
                Bytes::from(key.clone()),
                Bytes::from(serde_json::to_string(&state.to_row())?),
            )?;
            let commit_ts = self.txn.commit_txn(record.txn_id)?;
            self.kv.commit(record.txn_id, commit_ts)?;
            Ok(())
        })();
        if result.is_err() {
            self.release(record.txn_id);
        }
        result
    }

    /// `nextval(name)`.
    pub(crate) fn nextval(&self, session: &str, name: &str) -> Result<i64> {
        let tbl = self.resolve(name)?;
        let value = self.update(&tbl, |state| {
            let next = state.next(name)?;
            Ok(SequenceState {
                last_value: next,
                is_called: true,
                ..state.clone()
            })
        })?;
        let mut sessions = self.sessions.write();
        let values = sessions.entry(session.to_string()).or_default();
        values.by_sequence.insert(tbl.id, value.last_value);
        values.last = Some(value.last_value);
        Ok(value.last_value)
    }

    /// `setval(name, value [, is_called])`.
    pub(crate) fn setval(
        &self,
        session: &str,
        name: &str,
        value: i64,
        is_called: bool,
    ) -> Result<i64> {
        let tbl = self.resolve(name)?;
        self.update(&tbl, |state| {
            if !(state.min..=state.max).contains(&value) {
                bail!(
                    "setval: value {value} is out of bounds for sequence \"{name}\" ({}..{})",
                    state.min,
                    state.max
                );
            }
            Ok(SequenceState {
                last_value: value,
                is_called,
                ..state.clone()
            })
        })?;
        if is_called {
            let mut sessions = self.sessions.write();
            let values = sessions.entry(session.to_string()).or_default();
            values.by_sequence.insert(tbl.id, value);
            values.last = Some(value);
        }
        Ok(value)
    }

    /// Restarts a sequence at its start value (`TRUNCATE ... RESTART
    /// IDENTITY`).
    pub(crate) fn restart(&self, name: &str) -> Result<()> {
        let tbl = self.resolve(name)?;
        self.update(&tbl, |state| {
            Ok(SequenceState {
                last_value: state.start,
                is_called: false,
                ..state.clone()
            })
        })?;
        Ok(())
    }

    /// `currval(name)`: this session's last `nextval` of the sequence.
    pub(crate) fn currval(&self, session: &str, name: &str) -> Result<i64> {
        let tbl = self.resolve(name)?;
        self.sessions
            .read()
            .get(session)
            .and_then(|values| values.by_sequence.get(&tbl.id).copied())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "currval of sequence \"{}\" is not yet defined in this session",
                    tbl.name
                )
            })
    }

    /// `lastval()`: this session's last `nextval` of any sequence.
    pub(crate) fn lastval(&self, session: &str) -> Result<i64> {
        self.sessions
            .read()
            .get(session)
            .and_then(|values| values.last)
            .ok_or_else(|| anyhow::anyhow!("lastval is not yet defined in this session"))
    }

    /// Reads the sequence's row, applies `change`, and commits the new row in
    /// a transaction of its own, retrying if a concurrent caller won the race.
    fn update(
        &self,
        tbl: &TableDescriptor,
        change: impl Fn(&SequenceState) -> Result<SequenceState>,
    ) -> Result<SequenceState> {
        let range = || KeyRange {
            start: Bytes::from(format!("{}:", tbl.id)),
            end: Bytes::from(format!("{};", tbl.id)),
        };
        let mut last_error = None;
        for _ in 0..MAX_ATTEMPTS {
            let record = self.txn.begin_txn()?;
            let txn_id = record.txn_id;
            let attempt = (|| -> Result<Result<SequenceState>> {
                // The newest committed state, even when served by a replica.
                self.kv.read_range_barrier(range())?;
                let pair = self
                    .kv
                    .scan(range(), record.read_ts)?
                    .next()
                    .transpose()?
                    .ok_or_else(|| anyhow::anyhow!("sequence \"{}\" has no state", tbl.name))?;
                let row: Vec<Value> = serde_json::from_slice(&pair.value)?;
                let state = SequenceState::from_row(&row)?;
                let updated = match change(&state) {
                    Ok(updated) => updated,
                    // A domain error (bounds, limits) is final, not a race.
                    Err(e) => return Ok(Err(e)),
                };
                self.txn.track_write(txn_id, pair.key.to_vec())?;
                self.kv.write_intent(
                    txn_id,
                    pair.key.clone(),
                    Bytes::from(serde_json::to_string(&updated.to_row())?),
                )?;
                let commit_ts = self.txn.commit_txn(txn_id)?;
                self.kv.commit(txn_id, commit_ts)?;
                Ok(Ok(updated))
            })();
            match attempt {
                Ok(Ok(updated)) => return Ok(updated),
                Ok(Err(e)) => {
                    self.release(txn_id);
                    return Err(e);
                }
                Err(e) => {
                    // Lost a write race (or a transient storage error): retry
                    // from a fresh snapshot.
                    self.release(txn_id);
                    last_error = Some(e);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("could not advance sequence")))
    }

    fn release(&self, txn_id: nodus_storage_api::TxnId) {
        let _ = self.txn.abort_txn(txn_id);
        let _ = self.kv.abort(txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_follow_postgresql() {
        let s = SequenceState::new(&SequenceSpec::default()).unwrap();
        assert_eq!((s.start, s.min, s.max, s.increment), (1, 1, i64::MAX, 1));
        let down = SequenceState::new(&SequenceSpec {
            increment: Some(-1),
            data_type: Some("integer".into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!((down.start, down.min, down.max), (-1, i32::MIN as i64, -1));
        assert!(
            SequenceState::new(&SequenceSpec {
                increment: Some(0),
                ..Default::default()
            })
            .is_err()
        );
    }

    #[test]
    fn next_advances_cycles_and_stops_at_limits() {
        let mut s = SequenceState::new(&SequenceSpec {
            max_value: Some(3),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(s.next("s").unwrap(), 1);
        s.is_called = true;
        s.last_value = 3;
        assert!(
            s.next("s")
                .unwrap_err()
                .to_string()
                .contains("maximum value")
        );
        s.cycle = true;
        assert_eq!(s.next("s").unwrap(), 1);
    }
}
