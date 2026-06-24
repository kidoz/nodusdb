//! Transaction control and session statements: BEGIN/COMMIT/ROLLBACK,
//! savepoints, and SHOW/SET variable acknowledgements.

use crate::*;
use anyhow::Result;
use bytes::Bytes;
use nodus_storage_api::IntentReplacement;

impl MemExecutor {
    pub(crate) fn exec_begin(&self, ctx: &ExecutionContext) -> Result<QueryOutput> {
        let txn_record = self.txn.begin_txn()?;
        self.active_txns.write().unwrap().insert(
            ctx.session_id.clone(),
            ActiveTxn::new(txn_record.txn_id, txn_record.read_ts),
        );
        Ok(QueryOutput::tag("BEGIN"))
    }

    pub(crate) fn exec_commit(&self, ctx: &ExecutionContext) -> Result<QueryOutput> {
        if let Some(txn) = self.active_txns.write().unwrap().remove(&ctx.session_id) {
            let commit_ts = self.txn.commit_txn(txn.txn_id)?;
            self.kv.commit(txn.txn_id, commit_ts)?;
        }
        Ok(QueryOutput::tag("COMMIT"))
    }

    pub(crate) fn exec_rollback(&self, ctx: &ExecutionContext) -> Result<QueryOutput> {
        if let Some(txn) = self.active_txns.write().unwrap().remove(&ctx.session_id) {
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
        let mut guard = self.active_txns.write().unwrap();
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
            let guard = self.active_txns.read().unwrap();
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

        let mut guard = self.active_txns.write().unwrap();
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
        let mut guard = self.active_txns.write().unwrap();
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

    pub(crate) fn exec_show_variable(&self, variable: String) -> Result<QueryOutput> {
        let value = if variable.eq_ignore_ascii_case("search_path") {
            "public".to_string()
        } else {
            String::new()
        };
        Ok(QueryOutput {
            columns: vec![variable],
            types: vec!["VARCHAR".to_string()],
            rows: vec![Row {
                values: vec![Value::Text(value)],
            }],
            tag: "SHOW".into(),
        })
    }

    pub(crate) fn exec_set_variable(&self) -> Result<QueryOutput> {
        // Acknowledging SET requests to support clients like JDBC
        Ok(QueryOutput::tag("SET"))
    }
}
