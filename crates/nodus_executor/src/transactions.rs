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
    /// A transaction for the session, with its default access mode and
    /// isolation level (`default_transaction_read_only`, ...).
    pub(crate) fn new_active_txn(
        &self,
        session_id: &str,
        txn_id: TxnId,
        read_ts: Timestamp,
        explicit: bool,
    ) -> ActiveTxn {
        let mut txn = ActiveTxn::new(txn_id, read_ts, explicit);
        let vars = self.session_vars.read();
        let var = |name: &str| vars.get(session_id).and_then(|v| v.get(name)).cloned();
        txn.read_only = var("default_transaction_read_only").as_deref() == Some("on");
        if let Some(isolation) = var("default_transaction_isolation") {
            txn.isolation = isolation;
        }
        txn
    }

    pub(crate) fn exec_begin(
        &self,
        ctx: &ExecutionContext,
        read_only: Option<bool>,
        isolation: Option<String>,
    ) -> Result<QueryOutput> {
        if self
            .active_txns
            .read()
            .get(&ctx.session_id)
            .is_some_and(|t| t.explicit)
        {
            self.notice(
                ctx,
                warning("there is already a transaction in progress", "25001"),
            );
            return Ok(QueryOutput::tag("BEGIN"));
        }
        let txn_record = self.txn.begin_txn()?;
        let mut txn =
            self.new_active_txn(&ctx.session_id, txn_record.txn_id, txn_record.read_ts, true);
        txn.read_only = read_only.unwrap_or(txn.read_only);
        if let Some(isolation) = isolation {
            txn.isolation = isolation;
        }
        self.active_txns.write().insert(ctx.session_id.clone(), txn);
        Ok(QueryOutput::tag("BEGIN"))
    }

    pub(crate) fn exec_commit(&self, ctx: &ExecutionContext) -> Result<QueryOutput> {
        let Some(txn) = self.active_txns.write().remove(&ctx.session_id) else {
            self.notice(ctx, warning("there is no transaction in progress", "25P01"));
            return Ok(QueryOutput::tag("COMMIT"));
        };
        // A `SET LOCAL` and transaction-level advisory locks end with the
        // transaction.
        self.restore_settings(ctx, txn.local_settings.clone());
        self.advisory.end_transaction(&ctx.session_id);
        let commit_ts = self.commit_or_release(txn.txn_id)?;
        self.kv.commit(txn.txn_id, commit_ts)?;
        self.deliver_notifications(&ctx.session_id, txn.pending_notifications.clone());
        self.after_commit(ctx);
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
            self.notice(ctx, warning("there is no transaction in progress", "25P01"));
            return Ok(QueryOutput::tag("ROLLBACK"));
        };
        // Settings changed in the transaction go back.
        self.restore_settings(ctx, txn.settings_before.clone());
        self.advisory.end_transaction(&ctx.session_id);
        self.txn.abort_txn(txn.txn_id)?;
        self.kv.abort(txn.txn_id)?;
        Ok(QueryOutput::tag("ROLLBACK"))
    }

    /// `COMMIT AND CHAIN` / `ROLLBACK AND CHAIN`: ends the transaction and
    /// starts another with its access mode and isolation level.
    pub(crate) fn exec_chain(&self, ctx: &ExecutionContext, rollback: bool) -> Result<QueryOutput> {
        let command = if rollback { "ROLLBACK" } else { "COMMIT" };
        let Some((read_only, isolation)) = self
            .active_txns
            .read()
            .get(&ctx.session_id)
            .filter(|t| t.explicit)
            .map(|t| (t.read_only, t.isolation.clone()))
        else {
            anyhow::bail!("{command} AND CHAIN can only be used in transaction blocks");
        };
        if rollback {
            self.exec_rollback(ctx)?;
        } else {
            self.exec_commit(ctx)?;
        }
        self.exec_begin(ctx, Some(read_only), Some(isolation))?;
        Ok(QueryOutput::tag(command))
    }

    /// `SET TRANSACTION` (of the current transaction, before its first
    /// query) and `SET SESSION CHARACTERISTICS AS TRANSACTION` (of later ones).
    pub(crate) fn exec_set_transaction(
        &self,
        ctx: &ExecutionContext,
        read_only: Option<bool>,
        isolation: Option<String>,
        session: bool,
    ) -> Result<QueryOutput> {
        if session {
            if let Some(read_only) = read_only {
                let value = if read_only { "on" } else { "off" };
                self.change_setting(
                    ctx,
                    "default_transaction_read_only",
                    Some(value.into()),
                    false,
                );
            }
            if let Some(isolation) = isolation {
                self.change_setting(ctx, "default_transaction_isolation", Some(isolation), false);
            }
            return Ok(QueryOutput::tag("SET"));
        }
        let mut guard = self.active_txns.write();
        let Some(txn) = guard.get_mut(&ctx.session_id).filter(|t| t.explicit) else {
            drop(guard);
            self.notice(
                ctx,
                warning(
                    "SET TRANSACTION can only be used in transaction blocks",
                    "25P01",
                ),
            );
            return Ok(QueryOutput::tag("SET"));
        };
        if txn.queried && isolation.as_ref().is_some_and(|i| *i != txn.isolation) {
            anyhow::bail!("SET TRANSACTION ISOLATION LEVEL must be called before any query");
        }
        if txn.queried && read_only == Some(false) && txn.read_only {
            anyhow::bail!("transaction read-write mode must be set before any query");
        }
        if let Some(read_only) = read_only {
            txn.read_only = read_only;
        }
        if let Some(isolation) = isolation {
            txn.isolation = isolation;
        }
        Ok(QueryOutput::tag("SET"))
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

    /// `LOCK TABLE`: NodusDB's transactions need no table locks, so it only
    /// checks that it runs in a transaction block and the tables exist.
    pub(crate) fn exec_lock_table(
        &self,
        ctx: &ExecutionContext,
        tables: Vec<String>,
    ) -> Result<QueryOutput> {
        if !self.in_explicit_txn(&ctx.session_id) {
            anyhow::bail!("LOCK TABLE can only be used in transaction blocks");
        }
        for table in &tables {
            let (db, schema, name) = crate::planner::parse_object_name(table)?;
            if self.catalog_reader.get_table(db, schema, name).is_err() {
                anyhow::bail!("relation \"{name}\" does not exist");
            }
        }
        Ok(QueryOutput::tag("LOCK TABLE"))
    }

    /// `LISTEN channel`.
    pub(crate) fn exec_listen(
        &self,
        ctx: &ExecutionContext,
        channel: String,
    ) -> Result<QueryOutput> {
        self.listeners
            .write()
            .entry(ctx.session_id.clone())
            .or_default()
            .insert(channel);
        Ok(QueryOutput::tag("LISTEN"))
    }

    /// `UNLISTEN channel` / `UNLISTEN *`.
    pub(crate) fn exec_unlisten(
        &self,
        ctx: &ExecutionContext,
        channel: Option<String>,
    ) -> Result<QueryOutput> {
        let mut listeners = self.listeners.write();
        match channel {
            Some(channel) => {
                if let Some(channels) = listeners.get_mut(&ctx.session_id) {
                    channels.remove(&channel);
                }
            }
            None => {
                listeners.remove(&ctx.session_id);
            }
        }
        Ok(QueryOutput::tag("UNLISTEN"))
    }

    /// `NOTIFY channel, payload`.
    pub(crate) fn exec_notify(
        &self,
        ctx: &ExecutionContext,
        channel: String,
        payload: String,
    ) -> Result<QueryOutput> {
        if payload.len() >= 8000 {
            anyhow::bail!("payload string too long");
        }
        self.queue_notifications(ctx, vec![(channel, payload)]);
        Ok(QueryOutput::tag("NOTIFY"))
    }

    /// `PREPARE`: keeps the statement for the session, by name.
    pub(crate) fn exec_prepare(
        &self,
        ctx: &ExecutionContext,
        name: String,
        param_types: Vec<String>,
        statement: String,
    ) -> Result<QueryOutput> {
        // Parameters without a declared type take the type their use shows.
        let inferred = self
            .infer_sql_parameters(ctx, &statement)
            .unwrap_or_default();
        let declared_types = param_types.clone();
        let param_types: Vec<String> = (0..param_types.len().max(inferred.len()))
            .map(|i| {
                param_types
                    .get(i)
                    .cloned()
                    .or_else(|| inferred.get(i).cloned().flatten())
                    .unwrap_or_else(|| "text".to_string())
            })
            .collect();
        let mut prepared = self.prepared.lock();
        let session = prepared.entry(ctx.session_id.clone()).or_default();
        if session.contains_key(&name) {
            anyhow::bail!("prepared statement \"{name}\" already exists");
        }
        session.insert(
            name,
            crate::PreparedStatement {
                statement,
                param_types,
                declared_types,
                prepared_at: session_env::wall_micros(),
                executions: 0,
            },
        );
        Ok(QueryOutput::tag("PREPARE"))
    }

    /// `EXECUTE`: plans a prepared statement with the parameters (of its
    /// declared types) and runs it.
    pub(crate) fn exec_execute(
        &self,
        ctx: &ExecutionContext,
        name: String,
        params: Vec<ScalarExpr>,
    ) -> Result<QueryOutput> {
        let (statement, param_types) = {
            let mut prepared = self.prepared.lock();
            let found = prepared
                .get_mut(&ctx.session_id)
                .and_then(|session| session.get_mut(&name))
                .ok_or_else(|| anyhow::anyhow!("prepared statement \"{name}\" does not exist"))?;
            found.executions += 1;
            (found.statement.clone(), found.param_types.clone())
        };
        if !param_types.is_empty() && params.len() != param_types.len() {
            anyhow::bail!("wrong number of parameters for prepared statement \"{name}\"");
        }
        let mut values = Vec::with_capacity(params.len());
        for (i, param) in params.iter().enumerate() {
            let value = self.eval_expr(ctx, param, &[], &[]);
            crate::eval_error::check()?;
            values.push(match param_types.get(i) {
                Some(ty) => crate::planner::try_cast(value, ty).map_err(|e| anyhow::anyhow!(e))?,
                None => value,
            });
        }
        let statements = nodus_sql::parse_sql(&statement)?;
        let stmt = statements
            .first()
            .ok_or_else(|| anyhow::anyhow!("prepared statement \"{name}\" is empty"))?;
        let plan = crate::planner::plan_statement(stmt, &values)?;
        self.execute_logical_inner(ctx, plan)
    }

    /// `DEALLOCATE name` / `DEALLOCATE ALL`.
    pub(crate) fn exec_deallocate(
        &self,
        ctx: &ExecutionContext,
        name: Option<String>,
    ) -> Result<QueryOutput> {
        let mut prepared = self.prepared.lock();
        match name {
            Some(name) => {
                let removed = prepared
                    .get_mut(&ctx.session_id)
                    .and_then(|session| session.remove(&name));
                if removed.is_none() {
                    anyhow::bail!("prepared statement \"{name}\" does not exist");
                }
                Ok(QueryOutput::tag("DEALLOCATE"))
            }
            None => {
                prepared.remove(&ctx.session_id);
                Ok(QueryOutput::tag("DEALLOCATE ALL"))
            }
        }
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
        let txn_value = self
            .active_txns
            .read()
            .get(&ctx.session_id)
            .and_then(|txn| match key.as_str() {
                "transaction_read_only" => {
                    Some(if txn.read_only { "on" } else { "off" }.to_string())
                }
                "transaction_isolation" => Some(txn.isolation.clone()),
                _ => None,
            });
        let value = txn_value
            .or_else(|| self.setting_value(&ctx.session_id, &key))
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
        // The transaction's own characteristics.
        match key.as_str() {
            "transaction_isolation" => {
                let level = crate::session_vars::normalize_var_value(&value).to_ascii_lowercase();
                return self.exec_set_transaction(ctx, None, Some(level), false);
            }
            "transaction_read_only" => {
                let value = crate::session_vars::normalize_var_value(&value).to_ascii_lowercase();
                let read_only = matches!(value.as_str(), "on" | "true" | "yes" | "1");
                return self.exec_set_transaction(ctx, Some(read_only), None, false);
            }
            _ => {}
        }
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
