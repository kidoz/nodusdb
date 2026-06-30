//! Transaction control and session statements: BEGIN/COMMIT/ROLLBACK,
//! savepoints, and SHOW/SET variable acknowledgements.

use crate::*;
use anyhow::Result;
use bytes::Bytes;
use nodus_storage_api::IntentReplacement;

impl MemExecutor {
    pub(crate) fn exec_begin(&self, ctx: &ExecutionContext) -> Result<QueryOutput> {
        let txn_record = self.txn.begin_txn()?;
        self.active_txns.write().insert(
            ctx.session_id.clone(),
            ActiveTxn::new(txn_record.txn_id, txn_record.read_ts, true),
        );
        Ok(QueryOutput::tag("BEGIN"))
    }

    pub(crate) fn exec_commit(&self, ctx: &ExecutionContext) -> Result<QueryOutput> {
        if let Some(txn) = self.active_txns.write().remove(&ctx.session_id) {
            let commit_ts = self.txn.commit_txn(txn.txn_id)?;
            self.kv.commit(txn.txn_id, commit_ts)?;
        }
        Ok(QueryOutput::tag("COMMIT"))
    }

    pub(crate) fn exec_rollback(&self, ctx: &ExecutionContext) -> Result<QueryOutput> {
        if let Some(txn) = self.active_txns.write().remove(&ctx.session_id) {
            self.txn.abort_txn(txn.txn_id)?;
            self.kv.abort(txn.txn_id)?;
        }
        Ok(QueryOutput::tag("SAVEPOINT"))
    }

    pub(crate) fn exec_savepoint(
        &self,
        ctx: &ExecutionContext,
        name: String,
    ) -> Result<QueryOutput> {
        let mut guard = self.active_txns.write();
        let txn = guard
            .get_mut(&ctx.session_id)
            .ok_or_else(|| anyhow::anyhow!("SAVEPOINT can only be used in transaction blocks"))?;
        txn.savepoints.push(SavepointState {
            name,
            write_log_len: txn.write_log.len(),
            overlay: txn.overlay.clone(),
        });
        Ok(QueryOutput::tag("SAVEPOINT"))
    }

    pub(crate) fn exec_rollback_to_savepoint(
        &self,
        ctx: &ExecutionContext,
        name: String,
    ) -> Result<QueryOutput> {
        let (txn_id, affected, snapshot, keep_len, keep_savepoints) = {
            let guard = self.active_txns.read();
            let txn = guard.get(&ctx.session_id).ok_or_else(|| {
                anyhow::anyhow!("ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
            })?;
            let savepoint_idx = txn
                .savepoints
                .iter()
                .rposition(|savepoint| savepoint.name.eq_ignore_ascii_case(&name))
                .ok_or_else(|| anyhow::anyhow!("savepoint \"{}\" does not exist", name))?;
            let savepoint = txn.savepoints[savepoint_idx].clone();
            let affected = txn.write_log[savepoint.write_log_len..].to_vec();
            (
                txn.txn_id,
                affected,
                savepoint.overlay,
                savepoint.write_log_len,
                savepoint_idx + 1,
            )
        };

        let mut unique_keys = affected;
        unique_keys.sort();
        unique_keys.dedup();
        for key in unique_keys {
            let replacement = match snapshot.get(&key) {
                Some(Some(value)) => IntentReplacement::Put(Bytes::from(value.clone())),
                Some(None) => IntentReplacement::Delete,
                None => IntentReplacement::Clear,
            };
            self.kv
                .replace_intent(txn_id, Bytes::from(key), replacement)?;
        }

        let mut guard = self.active_txns.write();
        if let Some(txn) = guard.get_mut(&ctx.session_id) {
            txn.overlay = snapshot;
            txn.write_log.truncate(keep_len);
            txn.savepoints.truncate(keep_savepoints);
        }
        Ok(QueryOutput::tag("ROLLBACK"))
    }

    pub(crate) fn exec_release_savepoint(
        &self,
        ctx: &ExecutionContext,
        name: String,
    ) -> Result<QueryOutput> {
        let mut guard = self.active_txns.write();
        let txn = guard.get_mut(&ctx.session_id).ok_or_else(|| {
            anyhow::anyhow!("RELEASE SAVEPOINT can only be used in transaction blocks")
        })?;
        let savepoint_idx = txn
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name.eq_ignore_ascii_case(&name))
            .ok_or_else(|| anyhow::anyhow!("savepoint \"{}\" does not exist", name))?;
        txn.savepoints.truncate(savepoint_idx);
        Ok(QueryOutput::tag("RELEASE"))
    }

    /// `SHOW <var>` — returns the session's set value, or the built-in default,
    /// or an empty string for variables NodusDB does not model.
    pub(crate) fn exec_show_variable(
        &self,
        ctx: &ExecutionContext,
        variable: String,
    ) -> Result<QueryOutput> {
        let key = variable.trim().to_ascii_lowercase();
        let set_value = self
            .session_vars
            .read()
            .get(&ctx.session_id)
            .and_then(|vars| vars.get(&key))
            .cloned();
        let value = set_value
            .or_else(|| crate::session_vars::default_session_var(&key).map(str::to_owned))
            .unwrap_or_default();
        Ok(QueryOutput {
            columns: vec![variable],
            types: vec!["VARCHAR".to_string()],
            rows: vec![Row {
                values: vec![Value::Text(value)],
            }],
            tag: "SHOW".into(),
        })
    }

    /// `SET <var> = <value>` — persists into the per-session overlay so a later
    /// `SHOW` reflects it. `SET <var> = DEFAULT` clears the override (reverting
    /// to the built-in default). The value is acknowledged for every variable,
    /// including ones NodusDB does not act on, to keep drivers like pgjdbc/Npgsql
    /// happy.
    pub(crate) fn exec_set_variable(
        &self,
        ctx: &ExecutionContext,
        variable: String,
        value: String,
    ) -> Result<QueryOutput> {
        let key = variable.trim().to_ascii_lowercase();
        let normalized = crate::session_vars::normalize_var_value(&value);
        let mut guard = self.session_vars.write();
        let vars = guard.entry(ctx.session_id.clone()).or_default();
        if normalized.eq_ignore_ascii_case("default") {
            vars.remove(&key);
        } else {
            vars.insert(key, normalized);
        }
        Ok(QueryOutput::tag("SET"))
    }
}
