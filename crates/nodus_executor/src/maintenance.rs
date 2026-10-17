//! `VACUUM` and `ANALYZE`: what they record. NodusDB keeps its rows in a
//! versioned store that reclaims itself, so there is nothing for a vacuum to
//! sweep and no sampled statistics to gather — but the counters
//! (`pg_stat_user_tables`) and the row counts (`pg_class.reltuples`) are
//! PostgreSQL's, and tools read them.

use crate::*;
use anyhow::Result;
use chrono::Utc;

/// An `INFO` report, as PostgreSQL's `VACUUM VERBOSE` and `ANALYZE VERBOSE`
/// raise them.
fn info(message: impl Into<String>) -> crate::error_fields::DbError {
    crate::error_fields::DbError::new(message).severity("INFO")
}

impl MemExecutor {
    /// `VACUUM [FULL] [FREEZE] [VERBOSE] [ANALYZE] [table]`: stamps the
    /// tables it covers, sweeps their dead-tuple count, and analyzes them
    /// when the statement says `ANALYZE`.
    pub(crate) fn exec_vacuum(
        &self,
        ctx: &ExecutionContext,
        tables: Option<Vec<String>>,
        analyze: bool,
        verbose: bool,
    ) -> Result<QueryOutput> {
        for tbl in self.maintained_tables(ctx, "vacuum", tables)? {
            self.authorize(ctx, Action::Delete, ResourceRef::Table(tbl.id))?;
            let before = self.stats.table(tbl.id);
            let now = Utc::now();
            self.stats.update_table(tbl.id, |stats| {
                stats.last_vacuum = Some(now);
                stats.vacuum_count += 1;
                // The dead versions and the inserts since the last vacuum go
                // with the vacuum.
                stats.n_dead_tup = 0;
                stats.n_ins_since_vacuum = 0;
            });
            if verbose {
                // The pages the table occupies, as `pg_class.relpages`
                // reports them.
                let (bytes, _) = self.stored_pages(ctx, &tbl).unwrap_or((0, 0));
                let pages = bytes / 8192;
                let name = self.qualified_name(&tbl);
                self.notice(ctx, info(&format!("vacuuming \"{name}\"")));
                self.notice(
                    ctx,
                    info(&format!(
                        "finished vacuuming \"{name}\": index scans: 0\n\
                         pages: 0 removed, {pages} remain, {pages} scanned (100.00% of total), 0 eagerly scanned\n\
                         tuples: 0 removed, {live} remain, 0 are dead but not yet removable",
                        live = self.scan_rows(tbl.id, &ctx.session_id)?.len(),
                    )),
                );
            }
            if analyze {
                self.record_analysis(ctx, &tbl, verbose)?;
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
        verbose: bool,
    ) -> Result<QueryOutput> {
        for tbl in self.maintained_tables(ctx, "analyze", tables)? {
            self.record_analysis(ctx, &tbl, verbose)?;
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
        verbose: bool,
    ) -> Result<()> {
        self.authorize(ctx, Action::Delete, ResourceRef::Table(tbl.id))?;
        let (bytes, _) = self.stored_pages(ctx, tbl)?;
        let pages = bytes / 8192;
        let stats = self.stats.table(tbl.id);
        // The rows themselves: the counters do not survive a restart, and
        // PostgreSQL's ANALYZE counts what it scanned.
        let reltuples = self.scan_rows(tbl.id, &ctx.session_id)?.len() as f64;
        if verbose {
            // PostgreSQL's INFO report: the pages scanned, the rows they
            // hold, and the sample ANALYZE drew.
            let sample = self.sample_rows(tbl, reltuples);
            self.notice(
                ctx,
                info(&format!(
                    "analyzing \"{}.{}\"",
                    self.schema_name(tbl),
                    tbl.name
                )),
            );
            self.notice(
                ctx,
                info(&format!(
                    "\"{}\": scanned {pages} of {pages} pages, containing {} live rows and {} dead rows; {sample} rows in sample, {} estimated total rows",
                    tbl.name, reltuples as i64, stats.n_dead_tup, reltuples as i64,
                )),
            );
        }
        let statistics = crate::column_stats::analyze_table(self, ctx, tbl)?;
        if verbose {
            self.notice(
                ctx,
                info(&format!(
                    "finished analyzing table \"{}\"",
                    self.qualified_name(tbl)
                )),
            );
        }
        self.catalog_writer.update_table_descriptor(
            nodus_catalog::TableDescriptorChange::SetStatistics {
                table_id: tbl.id,
                reltuples,
                relpages: pages,
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
                    // A named table's maintenance covers its inheritance tree,
                    // as PostgreSQL's does: the table and its descendants.
                    let children = self.descendants("default", table.id)?;
                    found.push(table);
                    for child in children {
                        if !found.iter().any(|t| t.id == child.id) {
                            found.push(child);
                        }
                    }
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
    /// The table's schema, from the catalog: `default`'s schemas hold every
    /// relation this engine resolves unqualified names into.
    fn schema_name(&self, tbl: &nodus_catalog::TableDescriptor) -> String {
        self.catalog_reader
            .list_schemas("default")
            .unwrap_or_default()
            .into_iter()
            .find(|schema| schema.id == tbl.schema_id)
            .map(|schema| schema.name)
            .unwrap_or_else(|| "public".to_string())
    }

    /// `database.schema.table`, as PostgreSQL's verbose reports name a
    /// relation.
    fn qualified_name(&self, tbl: &nodus_catalog::TableDescriptor) -> String {
        format!("default.{}.{}", self.schema_name(tbl), tbl.name)
    }

    /// The rows `ANALYZE` sampled: 300 per statistics target, capped by the
    /// table.
    fn sample_rows(&self, tbl: &nodus_catalog::TableDescriptor, rows: f64) -> i64 {
        let widest = tbl
            .columns
            .iter()
            .map(crate::column_stats::column_target)
            .max()
            .unwrap_or(0);
        (rows as i64).min((300 * widest) as i64)
    }

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
