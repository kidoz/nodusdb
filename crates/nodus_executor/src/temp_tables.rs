//! Temporary relations: each session's live in its own schema
//! (`pg_temp_<n>`), which unqualified names search first and which is
//! dropped with them when the session ends. A table's `ON COMMIT` action
//! empties or drops it whenever a transaction commits.

use crate::*;
use anyhow::Result;

/// A session's temporary relations.
#[derive(Default)]
pub(crate) struct TempRelations {
    /// The session's principal, which drops them when it ends.
    principal: Option<PrincipalId>,
    /// Tables with an `ON COMMIT` action (`DROP`, `DELETE ROWS`), by name.
    on_commit: Vec<(String, String)>,
}

/// A session's backend process id, as `pg_backend_pid()` reports it.
pub(crate) fn backend_pid(session_id: &str) -> i64 {
    session_id
        .bytes()
        .fold(17_i64, |h, b| (h * 31 + i64::from(b)) % 2_000_000_000)
        .abs()
        + 1
}

impl MemExecutor {
    /// Creates the session's temporary schema when it has none yet.
    pub(crate) fn ensure_temp_schema(&self, ctx: &ExecutionContext) -> Result<()> {
        let schema = crate::search_path::temp_schema_of(backend_pid(&ctx.session_id));
        self.temp_relations
            .lock()
            .entry(ctx.session_id.clone())
            .or_default()
            .principal = Some(ctx.principal_id);
        if self.catalog_reader.get_schema("default", &schema).is_ok() {
            return Ok(());
        }
        let db = self.catalog_reader.get_database("default")?;
        self.catalog_writer
            .create_schema(nodus_catalog::CreateSchemaRequest {
                id: nodus_catalog::SchemaId::new(),
                database_id: db.id,
                name: schema,
                owner_role_id: None,
                managed_access: false,
            })?;
        Ok(())
    }

    /// Notes a temporary table's `ON COMMIT` action.
    pub(crate) fn note_on_commit(&self, ctx: &ExecutionContext, table: String, action: String) {
        self.temp_relations
            .lock()
            .entry(ctx.session_id.clone())
            .or_default()
            .on_commit
            .push((table, action));
    }

    /// After a commit: empties the session's `ON COMMIT DELETE ROWS` tables
    /// and drops its `ON COMMIT DROP` ones.
    pub(crate) fn after_commit(&self, ctx: &ExecutionContext) {
        let actions = match self.temp_relations.lock().get_mut(&ctx.session_id) {
            Some(temp) if !temp.on_commit.is_empty() => std::mem::take(&mut temp.on_commit),
            _ => return,
        };
        let mut kept = Vec::new();
        for (table, action) in actions {
            let plan = if action == "DROP" {
                LogicalPlan::DropTable {
                    names: vec![table.clone()],
                    if_exists: true,
                    materialized: false,
                    cascade: true,
                }
            } else {
                kept.push((table.clone(), action));
                LogicalPlan::Truncate {
                    tables: vec![table],
                    restart_identity: false,
                    cascade: false,
                }
            };
            let _ = self.execute_logical(ctx, plan);
        }
        if let Some(temp) = self.temp_relations.lock().get_mut(&ctx.session_id) {
            temp.on_commit.extend(kept);
        }
    }

    /// Drops the session's temporary relations and schema (`DISCARD TEMP`,
    /// and when the session ends).
    pub(crate) fn drop_temp_relations(&self, session_id: &str) {
        let Some(temp) = self.temp_relations.lock().remove(session_id) else {
            return;
        };
        let Some(principal_id) = temp.principal else {
            return;
        };
        let ctx = ExecutionContext {
            session_id: session_id.to_string(),
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };
        let schema = crate::search_path::temp_schema_of(backend_pid(session_id));
        let names: Vec<String> = self
            .catalog_reader
            .list_tables("default", &schema)
            .unwrap_or_default()
            .into_iter()
            .map(|t| format!("{schema}.{}", t.name))
            .collect();
        if !names.is_empty() {
            let _ = self.execute_logical(
                &ctx,
                LogicalPlan::DropTable {
                    names,
                    if_exists: true,
                    materialized: false,
                    cascade: true,
                },
            );
        }
        if let Ok(sch) = self.catalog_reader.get_schema("default", &schema) {
            let _ = self.catalog_writer.drop_schema(sch.id);
        }
    }

    /// Whether a statement writes only the session's temporary relations,
    /// which a read-only transaction allows.
    pub(crate) fn writes_temp_relation(plan: &LogicalPlan) -> bool {
        let temp = |name: &str| {
            crate::planner::parse_object_name(name)
                .is_ok_and(|(_, schema, _)| crate::search_path::is_temp_schema(schema))
        };
        match plan {
            LogicalPlan::Insert { table_name, .. }
            | LogicalPlan::Update { table_name, .. }
            | LogicalPlan::Delete { table_name, .. } => temp(table_name),
            LogicalPlan::CreateTable { name, .. } => temp(name),
            LogicalPlan::Truncate { tables, .. } => tables.iter().all(|t| temp(t)),
            _ => false,
        }
    }

    /// `DISCARD ALL | PLANS | SEQUENCES | TEMP`.
    pub(crate) fn exec_discard(&self, ctx: &ExecutionContext, what: String) -> Result<QueryOutput> {
        let what = match what.as_str() {
            "TEMPORARY" => "TEMP".to_string(),
            _ => what,
        };
        match what.as_str() {
            "ALL" => {
                if self
                    .active_txns
                    .read()
                    .get(&ctx.session_id)
                    .is_some_and(|t| t.explicit)
                {
                    anyhow::bail!("DISCARD ALL cannot run inside a transaction block");
                }
                self.exec_reset_variable(ctx, None)?;
                self.drop_temp_relations(&ctx.session_id);
                self.sequences.end_session(&ctx.session_id);
            }
            "TEMP" => self.drop_temp_relations(&ctx.session_id),
            "SEQUENCES" => self.sequences.end_session(&ctx.session_id),
            _ => {}
        }
        Ok(QueryOutput::tag(&format!("DISCARD {what}")))
    }
}
