//! Per-relation statistics: what the `pg_stat_*` views show and their
//! `pg_stat_get_*` functions read. PostgreSQL keeps these in its statistics
//! collector — shared by every session, reset when the server restarts — and
//! so do they live here: for the executor's lifetime, not the session's.

use chrono::{DateTime, Utc};
use nodus_catalog::{IndexId, TableId};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// A table's cumulative counters and last-run times.
#[derive(Debug, Clone, Default)]
pub(crate) struct TableStats {
    pub(crate) seq_scan: i64,
    pub(crate) seq_tup_read: i64,
    pub(crate) idx_scan: i64,
    pub(crate) idx_tup_fetch: i64,
    pub(crate) n_tup_ins: i64,
    pub(crate) n_tup_upd: i64,
    pub(crate) n_tup_del: i64,
    pub(crate) n_tup_hot_upd: i64,
    pub(crate) n_tup_newpage_upd: i64,
    /// Rows the table holds (`TRUNCATE` resets it, as PostgreSQL's report).
    pub(crate) n_live_tup: i64,
    /// Versions of rows an update or delete left behind; `VACUUM` sweeps it.
    pub(crate) n_dead_tup: i64,
    pub(crate) n_mod_since_analyze: i64,
    pub(crate) n_ins_since_vacuum: i64,
    pub(crate) last_vacuum: Option<DateTime<Utc>>,
    pub(crate) last_autovacuum: Option<DateTime<Utc>>,
    pub(crate) last_analyze: Option<DateTime<Utc>>,
    pub(crate) last_autoanalyze: Option<DateTime<Utc>>,
    pub(crate) vacuum_count: i64,
    pub(crate) autovacuum_count: i64,
    pub(crate) analyze_count: i64,
    pub(crate) autoanalyze_count: i64,
}

/// An index's cumulative counters.
#[derive(Debug, Clone, Default)]
pub(crate) struct IndexStats {
    pub(crate) idx_scan: i64,
    pub(crate) idx_tup_read: i64,
    pub(crate) idx_tup_fetch: i64,
}

/// Every relation's statistics (`pg_stat_user_tables`,
/// `pg_stat_user_indexes`).
#[derive(Debug, Default)]
pub(crate) struct Stats {
    tables: RwLock<HashMap<TableId, TableStats>>,
    indexes: RwLock<HashMap<IndexId, IndexStats>>,
}

impl Stats {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Reads the table's statistics, zero-backed before its first statement.
    pub(crate) fn table(&self, table: TableId) -> TableStats {
        self.tables.read().get(&table).cloned().unwrap_or_default()
    }

    /// Changes the table's statistics.
    pub(crate) fn update_table(&self, table: TableId, change: impl FnOnce(&mut TableStats)) {
        let mut guard = self.tables.write();
        change(guard.entry(table).or_default());
    }

    /// Reads the index's statistics.
    pub(crate) fn index(&self, index: IndexId) -> IndexStats {
        self.indexes.read().get(&index).cloned().unwrap_or_default()
    }

    /// Changes the index's statistics.
    pub(crate) fn update_index(&self, index: IndexId, change: impl FnOnce(&mut IndexStats)) {
        let mut guard = self.indexes.write();
        change(guard.entry(index).or_default());
    }

    /// A relation's statistics by its OID, whatever kind it is (the
    /// `pg_stat_get_*` functions take an OID). `None` when the OID is no
    /// relation of this server, as PostgreSQL reports it.
    pub(crate) fn by_oid(
        &self,
        catalog: &dyn nodus_catalog::CatalogReader,
        oid: i64,
    ) -> Option<RelationStats> {
        if let Some(table) = crate::MemExecutor::relation_by_oid(catalog, oid) {
            return Some(RelationStats::Table(self.table(table.id)));
        }
        if let Some((_, index)) = crate::MemExecutor::index_by_oid(catalog, oid) {
            return Some(RelationStats::Index(self.index(index.id)));
        }
        None
    }

    /// A scan of `table` that read `rows` rows: from an index when one
    /// supplied them (its own counters move too), else from the table
    /// itself.
    pub(crate) fn scanned(&self, table: TableId, index: Option<IndexId>, rows: usize) {
        let rows = rows as i64;
        self.update_table(table, |stats| match index {
            Some(_) => {
                stats.idx_scan += 1;
                stats.idx_tup_fetch += rows;
            }
            None => {
                stats.seq_scan += 1;
                stats.seq_tup_read += rows;
            }
        });
        if let Some(index) = index {
            self.update_index(index, |stats| {
                stats.idx_scan += 1;
                stats.idx_tup_read += rows;
                stats.idx_tup_fetch += rows;
            });
        }
    }

    /// A table's statistics on the paths that wrote it: an insert, update,
    /// or delete of `rows` rows.
    pub(crate) fn wrote(&self, table: TableId, change: Write) {
        self.update_table(table, |stats| match change {
            Write::Inserted(n) => {
                stats.n_tup_ins += n;
                stats.n_live_tup += n;
                stats.n_ins_since_vacuum += n;
                stats.n_mod_since_analyze += n;
            }
            Write::Updated(n) => {
                stats.n_tup_upd += n;
                stats.n_dead_tup += n;
                stats.n_mod_since_analyze += n;
            }
            Write::Deleted(n) => {
                stats.n_tup_del += n;
                stats.n_live_tup -= n;
                stats.n_dead_tup += n;
                stats.n_mod_since_analyze += n;
            }
            Write::Truncated => {
                stats.n_live_tup = 0;
                stats.n_dead_tup = 0;
                stats.n_mod_since_analyze = 0;
                stats.n_ins_since_vacuum = 0;
            }
        });
    }
}

/// What a statement did to a table's rows.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Write {
    Inserted(i64),
    Updated(i64),
    Deleted(i64),
    /// `TRUNCATE`: the live and dead counts go back to zero.
    Truncated,
}

/// One relation's statistics, as the `pg_stat_get_*` functions read them:
/// the OID could name a table or an index.
#[derive(Debug, Clone)]
pub(crate) enum RelationStats {
    Table(TableStats),
    Index(IndexStats),
}
