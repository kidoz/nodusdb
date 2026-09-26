//! Transaction control and session statements: BEGIN/COMMIT/ROLLBACK,
//! savepoints, and SHOW/SET variable acknowledgements.

use crate::*;
use anyhow::Result;
use bytes::Bytes;
use nodus_storage_api::IntentReplacement;

/// A warning a transaction statement raises in the wrong place.
fn warning(message: &str, code: &str) -> crate::error_fields::DbError {
    crate::error_fields::DbError::new(message)
        .code(code)
        .severity("WARNING")
}

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
        let Some(txn) = self.active_txns.write().remove(&ctx.session_id) else {
            return Ok(QueryOutput::tag("COMMIT"));
        };
        // A `SET LOCAL` ends with the transaction.
        self.restore_settings(ctx, txn.local_settings.clone());
        let commit_ts = self.commit_or_release(txn.txn_id)?;
        self.kv.commit(txn.txn_id, commit_ts)?;
        Ok(QueryOutput::tag("COMMIT"))
    }

    /// Puts settings back to the values a transaction saved. A custom
    /// setting (`myapp.x`) once set stays defined, as empty.
    fn restore_settings(&self, ctx: &ExecutionContext, saved: HashMap<String, Option<String>>) {
        for (key, value) in saved {
            let value = match value {
                None if key.contains('.') => Some(String::new()),
                other => other,
            };
            self.store_setting(&ctx.session_id, &key, value);
        }
    }

    /// Stores a session's setting (`None` returns it to the built-in
    /// default) and notes a change the client is told of.
    fn store_setting(&self, session_id: &str, key: &str, value: Option<String>) {
        {
            let mut guard = self.session_vars.write();
            let vars = guard.entry(session_id.to_string()).or_default();
            match value {
                Some(value) => vars.insert(key.to_string(), value),
                None => vars.remove(key),
            };
        }
        if let Some(name) = crate::session_vars::reported_name(key) {
            let value = self.setting_value(session_id, key).unwrap_or_default();
            let mut changes = self.parameter_changes.lock();
            let changes = changes.entry(session_id.to_string()).or_default();
            changes.retain(|(n, _)| n != name);
            changes.push((name.to_string(), value));
        }
    }

    /// A setting's value in the session: set, or its default.
    pub(crate) fn setting_value(&self, session_id: &str, key: &str) -> Option<String> {
        self.session_vars
            .read()
            .get(session_id)
            .and_then(|vars| vars.get(key))
            .cloned()
            .or_else(|| crate::session_vars::default_session_var(key).map(str::to_owned))
    }

    /// Changes a setting, noting in an explicit transaction what to put back
    /// when it rolls back (and, for `SET LOCAL`, when it commits).
    pub(crate) fn change_setting(
        &self,
        ctx: &ExecutionContext,
        key: &str,
        value: Option<String>,
        local: bool,
    ) {
        let before = self
            .session_vars
            .read()
            .get(&ctx.session_id)
            .and_then(|vars| vars.get(key))
            .cloned();
        if let Some(txn) = self
            .active_txns
            .write()
            .get_mut(&ctx.session_id)
            .filter(|t| t.explicit)
        {
            txn.settings_before
                .entry(key.to_string())
                .or_insert_with(|| before.clone());
            if local {
                txn.local_settings.entry(key.to_string()).or_insert(before);
            } else {
                txn.local_settings.remove(key);
            }
        }
        self.store_setting(&ctx.session_id, key, value);
    }

    /// The value `RESET` returns a setting to: the session's at connection,
    /// else the built-in default (a custom setting's is empty).
    fn reset_value(&self, session_id: &str, key: &str) -> Option<String> {
        self.session_resets
            .read()
            .get(session_id)
            .and_then(|resets| resets.get(key))
            .cloned()
            .or_else(|| key.contains('.').then(String::new))
    }

    fn in_explicit_txn(&self, session_id: &str) -> bool {
        self.active_txns
            .read()
            .get(session_id)
            .is_some_and(|t| t.explicit)
    }

    /// Runs the commit-time conflict check. A losing transaction is dropped by
    /// the manager, so its storage intents must be released here: left behind,
    /// they would block every later write to those keys.
    pub(crate) fn commit_or_release(&self, txn_id: TxnId) -> Result<Timestamp> {
        self.txn.commit_txn(txn_id).inspect_err(|_| {
            let _ = self.kv.abort(txn_id);
        })
    }

    pub(crate) fn exec_rollback(&self, ctx: &ExecutionContext) -> Result<QueryOutput> {
        let Some(txn) = self.active_txns.write().remove(&ctx.session_id) else {
            return Ok(QueryOutput::tag("ROLLBACK"));
        };
        // Settings changed in the transaction go back.
        self.restore_settings(ctx, txn.settings_before.clone());
        self.txn.abort_txn(txn.txn_id)?;
        self.kv.abort(txn.txn_id)?;
        Ok(QueryOutput::tag("ROLLBACK"))
    }

    /// `RESET name` / `RESET ALL`: back to the session's values at connection.
    pub(crate) fn exec_reset_variable(
        &self,
        ctx: &ExecutionContext,
        variable: Option<String>,
    ) -> Result<QueryOutput> {
        let keys: Vec<String> = match variable {
            Some(name) => {
                let key = name.trim().to_ascii_lowercase();
                crate::session_vars::set_action(&key, "DEFAULT").map_err(|e| anyhow::anyhow!(e))?;
                vec![key]
            }
            None => {
                let mut keys: Vec<String> = self
                    .session_vars
                    .read()
                    .get(&ctx.session_id)
                    .map(|vars| vars.keys().cloned().collect())
                    .unwrap_or_default();
                keys.sort();
                keys
            }
        };
        for key in keys {
            let value = self.reset_value(&ctx.session_id, &key);
            self.change_setting(ctx, &key, value, false);
        }
        Ok(QueryOutput::tag("RESET"))
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

    /// `SHOW name` (and `SHOW ALL`): the session's value, else the default;
    /// the transaction's access mode and isolation level from it.
    pub(crate) fn exec_show_variable(
        &self,
        ctx: &ExecutionContext,
        variable: String,
    ) -> Result<QueryOutput> {
        let key = variable.trim().to_ascii_lowercase();
        if key == "all" {
            let rows = crate::session_vars::settings_table()
                .iter()
                .map(|info| Row {
                    values: vec![
                        Value::Text(info.name.to_string()),
                        Value::Text(
                            self.setting_value(&ctx.session_id, &info.name.to_ascii_lowercase())
                                .unwrap_or_default(),
                        ),
                        Value::Text(info.short_desc.to_string()),
                    ],
                })
                .collect();
            return Ok(QueryOutput {
                columns: vec!["name".into(), "setting".into(), "description".into()],
                types: vec!["TEXT".into(), "TEXT".into(), "TEXT".into()],
                rows,
                tag: "SHOW".into(),
            });
        }
        let value = self
            .setting_value(&ctx.session_id, &key)
            .ok_or_else(|| anyhow::anyhow!("unrecognized configuration parameter \"{key}\""))?;
        let column = crate::session_vars::setting_info(&key).map_or_else(
            || crate::session_vars::setting_display_name(&variable),
            |info| info.name.to_string(),
        );
        Ok(QueryOutput {
            columns: vec![column],
            types: vec!["TEXT".to_string()],
            rows: vec![Row {
                values: vec![Value::Text(value)],
            }],
            tag: "SHOW".into(),
        })
    }

    /// `SET name = value` / `SET LOCAL ...`: checked as PostgreSQL checks it,
    /// and undone when a transaction it ran in rolls back (a `SET LOCAL`
    /// also when it commits).
    pub(crate) fn exec_set_variable(
        &self,
        ctx: &ExecutionContext,
        variable: String,
        value: String,
        local: bool,
    ) -> Result<QueryOutput> {
        let key = variable.trim().to_ascii_lowercase();
        let action =
            crate::session_vars::set_action(&key, &value).map_err(|e| anyhow::anyhow!(e))?;
        if local && !self.in_explicit_txn(&ctx.session_id) {
            self.notice(
                ctx,
                warning("SET LOCAL can only be used in transaction blocks", "25P01"),
            );
            return Ok(QueryOutput::tag("SET"));
        }
        let value = match action {
            crate::session_vars::SetAction::Set(value) => Some(value),
            crate::session_vars::SetAction::Reset => self.reset_value(&ctx.session_id, &key),
        };
        self.change_setting(ctx, &key, value, local);
        Ok(QueryOutput::tag("SET"))
    }
}
