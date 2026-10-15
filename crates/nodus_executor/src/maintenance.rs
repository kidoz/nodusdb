//! `VACUUM` and `ANALYZE`: what they record. NodusDB keeps its rows in a
//! versioned store that reclaims itself, so there is nothing for a vacuum to
//! sweep and no sampled statistics to gather — but the counters
//! (`pg_stat_user_tables`) and the row counts (`pg_class.reltuples`) are
//! PostgreSQL's, and tools read them.

use crate::*;
use anyhow::Result;
use chrono::Utc;

impl MemExecutor {
    /// `VACUUM [FULL] [FREEZE] [VERBOSE] [ANALYZE] [table]`: stamps the
    /// tables it covers, sweeps their dead-tuple count, and analyzes them
    /// when the statement says `ANALYZE`.
    pub(crate) fn exec_vacuum(
        &self,
        ctx: &ExecutionContext,
        tables: Option<Vec<String>>,
        analyze: bool,
    ) -> Result<QueryOutput> {
        for tbl in self.maintained_tables(ctx, "vacuum", tables)? {
            self.authorize(ctx, Action::Delete, ResourceRef::Table(tbl.id))?;
            let now = Utc::now();
            self.stats.update_table(tbl.id, |stats| {
                stats.last_vacuum = Some(now);
                stats.vacuum_count += 1;
                // The dead versions and the inserts since the last vacuum go
                // with the vacuum.
                stats.n_dead_tup = 0;
                stats.n_ins_since_vacuum = 0;
            });
            if analyze {
                self.record_analysis(ctx, &tbl)?;
            }
        }
        Ok(QueryOutput::tag("VACUUM"))
    }

    /// `ANALYZE [VERBOSE] [table]`: records each table's row count and pages
    /// (`pg_class.reltuples`/`relpages`) and stamps the analysis.
    pub(crate) fn exec_analyze(
        &self,
        ctx: &ExecutionContext,
        tables: Option<Vec<String>>,
    ) -> Result<QueryOutput> {
        for tbl in self.maintained_tables(ctx, "analyze", tables)? {
            self.record_analysis(ctx, &tbl)?;
        }
        Ok(QueryOutput::tag("ANALYZE"))
    }

    /// The analysis itself: the table's live rows and the pages its rows and
    /// indexes occupy, written through the catalog as PostgreSQL writes
    /// `pg_class`, the per-column statistics (`pg_statistic`), plus the
    /// counters and times.
    fn record_analysis(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
    ) -> Result<()> {
        self.authorize(ctx, Action::Delete, ResourceRef::Table(tbl.id))?;
        let (rows, _) = self.stored_pages(ctx, tbl)?;
        let reltuples = self.stats.table(tbl.id).n_live_tup.max(0) as f64;
        let statistics = crate::column_stats::analyze_table(self, ctx, tbl)?;
        self.catalog_writer.update_table_descriptor(
            nodus_catalog::TableDescriptorChange::SetStatistics {
                table_id: tbl.id,
                reltuples,
                relpages: rows / 8192,
                statistics,
            },
        )?;
        let now = Utc::now();
        self.stats.update_table(tbl.id, |stats| {
            stats.last_analyze = Some(now);
            stats.analyze_count += 1;
            stats.n_mod_since_analyze = 0;
        });
        Ok(())
    }

    /// The tables a maintenance statement covers: each name it gives (with
    /// the schema and search path any relation name resolves through), or
    /// every table of the database, as PostgreSQL's bare `VACUUM`/`ANALYZE`
    /// cover. A named relation that is no table — a view, a sequence, an
    /// index — is skipped with PostgreSQL's warning; one that does not exist
    /// is an error.
    fn maintained_tables(
        &self,
        ctx: &ExecutionContext,
        verb: &str,
        tables: Option<Vec<String>>,
    ) -> Result<Vec<nodus_catalog::TableDescriptor>> {
        let Some(names) = tables else {
            return Ok(self
                .catalog_reader
                .list_all_tables("default")?
                .into_iter()
                .filter(|t| t.view_query.is_none() && !crate::sequences::is_sequence(t))
                .collect());
        };
        let mut found = Vec::new();
        for name in &names {
            let (db_name, schema_name, table_only) = crate::planner::parse_object_name(name)?;
            match self
                .catalog_reader
                .get_table(db_name, schema_name, table_only)
            {
                // A view has no rows of its own to sweep or count; a
                // sequence is no table at all.
                Ok(table)
                    if table.view_query.is_none() && !crate::sequences::is_sequence(&table) =>
                {
                    found.push(table);
                }
                named => {
                    let is_index = self
                        .catalog_reader
                        .list_all_tables("default")?
                        .iter()
                        .any(|t| t.indexes.iter().any(|i| i.name == table_only));
                    if named.is_err() && !is_index {
                        anyhow::bail!("relation \"{table_only}\" does not exist");
                    }
                    self.notice(
                        ctx,
                        crate::error_fields::DbError::new(format!(
                            "skipping \"{table_only}\" --- cannot {verb} non-tables or special system tables"
                        ))
                        .severity("WARNING"),
                    );
                }
            }
        }
        Ok(found)
    }

    /// The pages a table's rows and its indexes occupy in the store — what
    /// `pg_relation_size` and `pg_indexes_size` report of it.
    fn stored_pages(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
    ) -> Result<(i64, i64)> {
        let read_ts = self.read_ts(&ctx.session_id);
        Ok(crate::functions::stored_size(
            self.kv.as_ref(),
            read_ts,
            tbl,
        ))
    }
}
