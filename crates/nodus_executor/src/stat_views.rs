//! The server-side statistics views PostgreSQL 17+ shows: the database's own
//! counters, the writer's and checkpointer's, WAL, SLRU, IO, the archiver,
//! replication, subscriptions, functions, the per-relation IO views, and the
//! progress reports of operations this engine runs to completion.
//!
//! The shapes are PostgreSQL 18's own. The counters are the ones the engine
//! actually keeps — transactions committed and rolled back, sessions, the
//! tuples relations returned and changed — and the rest read zero, because
//! this engine has no block IO, no checkpointer, and no background writer to
//! report on. `stats_reset` is NULL: these counters start with the server.

use crate::{MemExecutor, Value};
use nodus_catalog::ColumnDescriptor;

/// The `pg_stat_io` rows: one per backend type, object and context
/// PostgreSQL 18 reports, in its own order.
const IO_ROWS: &[(&str, &str, &str)] = &[
    ("autovacuum launcher", "relation", "bulkread"),
    ("autovacuum launcher", "relation", "init"),
    ("autovacuum launcher", "relation", "normal"),
    ("autovacuum launcher", "wal", "init"),
    ("autovacuum launcher", "wal", "normal"),
    ("autovacuum worker", "relation", "bulkread"),
    ("autovacuum worker", "relation", "init"),
    ("autovacuum worker", "relation", "normal"),
    ("autovacuum worker", "relation", "vacuum"),
    ("autovacuum worker", "wal", "init"),
    ("autovacuum worker", "wal", "normal"),
    ("background worker", "relation", "bulkread"),
    ("background worker", "relation", "bulkwrite"),
    ("background worker", "relation", "init"),
    ("background worker", "relation", "normal"),
    ("background worker", "relation", "vacuum"),
    ("background worker", "temp relation", "normal"),
    ("background worker", "wal", "init"),
    ("background worker", "wal", "normal"),
    ("background writer", "relation", "init"),
    ("background writer", "relation", "normal"),
    ("background writer", "wal", "init"),
    ("background writer", "wal", "normal"),
    ("checkpointer", "relation", "init"),
    ("checkpointer", "relation", "normal"),
    ("checkpointer", "wal", "init"),
    ("checkpointer", "wal", "normal"),
    ("client backend", "relation", "bulkread"),
    ("client backend", "relation", "bulkwrite"),
    ("client backend", "relation", "init"),
    ("client backend", "relation", "normal"),
    ("client backend", "relation", "vacuum"),
    ("client backend", "temp relation", "normal"),
    ("client backend", "wal", "init"),
    ("client backend", "wal", "normal"),
    ("io worker", "relation", "bulkread"),
    ("io worker", "relation", "bulkwrite"),
    ("io worker", "relation", "init"),
    ("io worker", "relation", "normal"),
    ("io worker", "relation", "vacuum"),
    ("io worker", "temp relation", "normal"),
    ("io worker", "wal", "init"),
    ("io worker", "wal", "normal"),
    ("slotsync worker", "relation", "bulkread"),
    ("slotsync worker", "relation", "bulkwrite"),
    ("slotsync worker", "relation", "init"),
    ("slotsync worker", "relation", "normal"),
    ("slotsync worker", "relation", "vacuum"),
    ("slotsync worker", "temp relation", "normal"),
    ("slotsync worker", "wal", "init"),
    ("slotsync worker", "wal", "normal"),
    ("standalone backend", "relation", "bulkread"),
    ("standalone backend", "relation", "bulkwrite"),
    ("standalone backend", "relation", "init"),
    ("standalone backend", "relation", "normal"),
    ("standalone backend", "relation", "vacuum"),
    ("standalone backend", "wal", "init"),
    ("standalone backend", "wal", "normal"),
    ("startup", "relation", "bulkread"),
    ("startup", "relation", "bulkwrite"),
    ("startup", "relation", "init"),
    ("startup", "relation", "normal"),
    ("startup", "relation", "vacuum"),
    ("startup", "wal", "init"),
    ("startup", "wal", "normal"),
    ("walreceiver", "wal", "init"),
    ("walreceiver", "wal", "normal"),
    ("walsender", "relation", "bulkread"),
    ("walsender", "relation", "bulkwrite"),
    ("walsender", "relation", "init"),
    ("walsender", "relation", "normal"),
    ("walsender", "relation", "vacuum"),
    ("walsender", "temp relation", "normal"),
    ("walsender", "wal", "init"),
    ("walsender", "wal", "normal"),
    ("walsummarizer", "wal", "init"),
    ("walsummarizer", "wal", "normal"),
    ("walwriter", "wal", "init"),
    ("walwriter", "wal", "normal"),
];

/// The SLRU names PostgreSQL 18 reports, in its own order.
const SLRU_NAMES: &[&str] = &[
    "commit_timestamp",
    "multixact_member",
    "multixact_offset",
    "notify",
    "other",
    "serializable",
    "subtransaction",
    "transaction",
];

/// A counter's row: the value, or zero when the engine does not keep it.
fn counter(value: i64) -> Value {
    Value::Int(value)
}

fn float_counter(value: f64) -> Value {
    Value::Float(value)
}

fn numeric_zero() -> Value {
    Value::Numeric(crate::numeric::Numeric::zero())
}

impl MemExecutor {
    /// One of the statistics views this engine shows without a statistics
    /// collector of its own. `None` for a name that is no such view.
    pub(crate) fn server_statistics_view(
        &self,
        db_name: &str,
        tables: &[nodus_catalog::TableDescriptor],
        schemas: &[nodus_catalog::SchemaDescriptor],
        view: &str,
    ) -> Option<(Vec<ColumnDescriptor>, Vec<Vec<Value>>)> {
        let (commits, rollbacks, sessions) = self.stats.server_counters();
        let backends = self.stats.live_sessions();
        match view {
            // The database's own counters. This engine resolves unqualified
            // names into one database, so its transaction and session
            // counters are the server's, reported on each database's row.
            "pg_stat_database" => {
                let columns = Self::virtual_columns(&[
                    ("datid", "OID"),
                    ("datname", "NAME"),
                    ("numbackends", "INT"),
                    ("xact_commit", "INT8"),
                    ("xact_rollback", "INT8"),
                    ("blks_read", "INT8"),
                    ("blks_hit", "INT8"),
                    ("tup_returned", "INT8"),
                    ("tup_fetched", "INT8"),
                    ("tup_inserted", "INT8"),
                    ("tup_updated", "INT8"),
                    ("tup_deleted", "INT8"),
                    ("conflicts", "INT8"),
                    ("temp_files", "INT8"),
                    ("temp_bytes", "INT8"),
                    ("deadlocks", "INT8"),
                    ("checksum_failures", "INT8"),
                    ("checksum_last_failure", "TIMESTAMPTZ"),
                    ("blk_read_time", "FLOAT8"),
                    ("blk_write_time", "FLOAT8"),
                    ("session_time", "FLOAT8"),
                    ("active_time", "FLOAT8"),
                    ("idle_in_transaction_time", "FLOAT8"),
                    ("sessions", "INT8"),
                    ("sessions_abandoned", "INT8"),
                    ("sessions_fatal", "INT8"),
                    ("sessions_killed", "INT8"),
                    ("parallel_workers_to_launch", "INT8"),
                    ("parallel_workers_launched", "INT8"),
                    ("stats_reset", "TIMESTAMPTZ"),
                ]);
                // What the database's relations returned and changed.
                let (mut returned, mut fetched) = (0i64, 0i64);
                let (mut inserted, mut updated, mut deleted) = (0i64, 0i64, 0i64);
                for table in tables {
                    let stats = self.stats.table(table.id);
                    returned += stats.seq_tup_read;
                    fetched += stats.idx_tup_fetch;
                    inserted += stats.n_tup_ins;
                    updated += stats.n_tup_upd;
                    deleted += stats.n_tup_del;
                    for index in &table.indexes {
                        returned += self.stats.index(index.id).idx_tup_read;
                    }
                }
                // The shared-objects row PostgreSQL puts first, then one per
                // database.
                let mut rows = vec![vec![
                    Value::Int(0),
                    Value::Null,
                    counter(backends),
                    counter(commits),
                    counter(rollbacks),
                    counter(0),
                    counter(0),
                    counter(returned),
                    counter(fetched),
                    counter(inserted),
                    counter(updated),
                    counter(deleted),
                    counter(0),
                    counter(0),
                    counter(0),
                    counter(0),
                    counter(0),
                    Value::Null,
                    float_counter(0.0),
                    float_counter(0.0),
                    float_counter(0.0),
                    float_counter(0.0),
                    float_counter(0.0),
                    counter(sessions),
                    counter(0),
                    counter(0),
                    counter(0),
                    counter(0),
                    counter(0),
                    Value::Null,
                ]];
                let mut row = rows[0].clone();
                row[0] = Value::Int(Self::database_oid(db_name));
                row[1] = Value::Text(db_name.to_string());
                rows.push(row);
                Some((columns, rows))
            }
            // The background writer's own counters: this engine writes
            // through its storage engine, and has none.
            "pg_stat_bgwriter" => Some((
                Self::virtual_columns(&[
                    ("buffers_clean", "INT8"),
                    ("maxwritten_clean", "INT8"),
                    ("buffers_alloc", "INT8"),
                    ("stats_reset", "TIMESTAMPTZ"),
                ]),
                vec![vec![counter(0), counter(0), counter(0), Value::Null]],
            )),
            "pg_stat_checkpointer" => Some((
                Self::virtual_columns(&[
                    ("num_timed", "INT8"),
                    ("num_requested", "INT8"),
                    ("num_done", "INT8"),
                    ("restartpoints_timed", "INT8"),
                    ("restartpoints_req", "INT8"),
                    ("restartpoints_done", "INT8"),
                    ("write_time", "FLOAT8"),
                    ("sync_time", "FLOAT8"),
                    ("buffers_written", "INT8"),
                    ("slru_written", "INT8"),
                    ("stats_reset", "TIMESTAMPTZ"),
                ]),
                vec![vec![
                    counter(0),
                    counter(0),
                    counter(0),
                    counter(0),
                    counter(0),
                    counter(0),
                    float_counter(0.0),
                    float_counter(0.0),
                    counter(0),
                    counter(0),
                    Value::Null,
                ]],
            )),
            "pg_stat_wal" => Some((
                Self::virtual_columns(&[
                    ("wal_records", "INT8"),
                    ("wal_fpi", "INT8"),
                    ("wal_bytes", "NUMERIC"),
                    ("wal_buffers_full", "INT8"),
                    ("stats_reset", "TIMESTAMPTZ"),
                ]),
                vec![vec![
                    counter(0),
                    counter(0),
                    numeric_zero(),
                    counter(0),
                    Value::Null,
                ]],
            )),
            "pg_stat_archiver" => Some((
                Self::virtual_columns(&[
                    ("archived_count", "INT8"),
                    ("last_archived_wal", "TEXT"),
                    ("last_archived_time", "TIMESTAMPTZ"),
                    ("failed_count", "INT8"),
                    ("last_failed_wal", "TEXT"),
                    ("last_failed_time", "TIMESTAMPTZ"),
                    ("stats_reset", "TIMESTAMPTZ"),
                ]),
                vec![vec![
                    counter(0),
                    Value::Null,
                    Value::Null,
                    counter(0),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]],
            )),
            "pg_stat_slru" => {
                let columns = Self::virtual_columns(&[
                    ("name", "TEXT"),
                    ("blks_zeroed", "INT8"),
                    ("blks_hit", "INT8"),
                    ("blks_read", "INT8"),
                    ("blks_written", "INT8"),
                    ("blks_exists", "INT8"),
                    ("flushes", "INT8"),
                    ("truncates", "INT8"),
                    ("stats_reset", "TIMESTAMPTZ"),
                ]);
                let rows = SLRU_NAMES
                    .iter()
                    .map(|name| {
                        vec![
                            Value::Text(name.to_string()),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                            Value::Null,
                        ]
                    })
                    .collect();
                Some((columns, rows))
            }
            "pg_stat_io" => {
                let columns = Self::virtual_columns(&[
                    ("backend_type", "TEXT"),
                    ("object", "TEXT"),
                    ("context", "TEXT"),
                    ("reads", "INT8"),
                    ("read_bytes", "NUMERIC"),
                    ("read_time", "FLOAT8"),
                    ("writes", "INT8"),
                    ("write_bytes", "NUMERIC"),
                    ("write_time", "FLOAT8"),
                    ("writebacks", "INT8"),
                    ("writeback_time", "FLOAT8"),
                    ("extends", "INT8"),
                    ("extend_bytes", "NUMERIC"),
                    ("extend_time", "FLOAT8"),
                    ("hits", "INT8"),
                    ("evictions", "INT8"),
                    ("reuses", "INT8"),
                    ("fsyncs", "INT8"),
                    ("fsync_time", "FLOAT8"),
                    ("stats_reset", "TIMESTAMPTZ"),
                ]);
                let rows = IO_ROWS
                    .iter()
                    .map(|(backend, object, context)| {
                        vec![
                            Value::Text((*backend).to_string()),
                            Value::Text((*object).to_string()),
                            Value::Text((*context).to_string()),
                            counter(0),
                            numeric_zero(),
                            float_counter(0.0),
                            counter(0),
                            numeric_zero(),
                            float_counter(0.0),
                            counter(0),
                            float_counter(0.0),
                            counter(0),
                            numeric_zero(),
                            float_counter(0.0),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                            float_counter(0.0),
                            Value::Null,
                        ]
                    })
                    .collect();
                Some((columns, rows))
            }
            // Replication standbys and logical subscriptions: this engine
            // replicates through Raft, not through PostgreSQL's protocol.
            "pg_stat_replication" => Some((
                Self::virtual_columns(&[
                    ("pid", "INT"),
                    ("usesysid", "OID"),
                    ("usename", "NAME"),
                    ("application_name", "TEXT"),
                    ("client_addr", "INET"),
                    ("client_hostname", "TEXT"),
                    ("client_port", "INT"),
                    ("backend_start", "TIMESTAMPTZ"),
                    ("backend_xmin", "XID"),
                    ("state", "TEXT"),
                    ("sent_lsn", "TEXT"),
                    ("write_lsn", "TEXT"),
                    ("flush_lsn", "TEXT"),
                    ("replay_lsn", "TEXT"),
                    ("write_lag", "INTERVAL"),
                    ("flush_lag", "INTERVAL"),
                    ("replay_lag", "INTERVAL"),
                    ("sync_priority", "INT"),
                    ("sync_state", "TEXT"),
                    ("reply_time", "TIMESTAMPTZ"),
                ]),
                Vec::new(),
            )),
            "pg_stat_subscription" => Some((
                Self::virtual_columns(&[
                    ("subid", "OID"),
                    ("subname", "NAME"),
                    ("worker_type", "TEXT"),
                    ("pid", "INT"),
                    ("leader_pid", "INT"),
                    ("relid", "OID"),
                    ("received_lsn", "TEXT"),
                    ("last_msg_send_time", "TIMESTAMPTZ"),
                    ("last_msg_receipt_time", "TIMESTAMPTZ"),
                    ("latest_end_lsn", "TEXT"),
                    ("latest_end_time", "TIMESTAMPTZ"),
                ]),
                Vec::new(),
            )),
            // The functions this engine runs are its own built-ins; there
            // are no user-defined ones to report on.
            "pg_stat_user_functions" | "pg_stat_sys_functions" | "pg_stat_all_functions" => Some((
                Self::virtual_columns(&[
                    ("funcid", "OID"),
                    ("schemaname", "NAME"),
                    ("funcname", "NAME"),
                    ("calls", "INT8"),
                    ("total_time", "FLOAT8"),
                    ("self_time", "FLOAT8"),
                ]),
                Vec::new(),
            )),
            // Progress reports: every operation this engine runs finishes
            // before it answers, so none is in progress.
            "pg_stat_progress_vacuum" => Some((
                Self::virtual_columns(&[
                    ("pid", "INT"),
                    ("datid", "OID"),
                    ("datname", "NAME"),
                    ("relid", "OID"),
                    ("phase", "TEXT"),
                    ("heap_blks_total", "INT8"),
                    ("heap_blks_scanned", "INT8"),
                    ("heap_blks_vacuumed", "INT8"),
                    ("index_vacuum_count", "INT8"),
                    ("max_dead_tuple_bytes", "INT8"),
                    ("dead_tuple_bytes", "INT8"),
                    ("num_dead_item_ids", "INT8"),
                    ("indexes_total", "INT8"),
                    ("indexes_processed", "INT8"),
                    ("delay_time", "FLOAT8"),
                ]),
                Vec::new(),
            )),
            "pg_stat_progress_analyze" => Some((
                Self::virtual_columns(&[
                    ("pid", "INT"),
                    ("datid", "OID"),
                    ("datname", "NAME"),
                    ("relid", "OID"),
                    ("phase", "TEXT"),
                    ("sample_blks_total", "INT8"),
                    ("sample_blks_scanned", "INT8"),
                    ("ext_stats_total", "INT8"),
                    ("ext_stats_computed", "INT8"),
                    ("child_tables_total", "INT8"),
                    ("child_tables_done", "INT8"),
                    ("current_child_table_relid", "OID"),
                    ("delay_time", "FLOAT8"),
                ]),
                Vec::new(),
            )),
            "pg_stat_progress_create_index" => Some((
                Self::virtual_columns(&[
                    ("pid", "INT"),
                    ("datid", "OID"),
                    ("datname", "NAME"),
                    ("relid", "OID"),
                    ("index_relid", "OID"),
                    ("command", "TEXT"),
                    ("phase", "TEXT"),
                    ("lockers_total", "INT8"),
                    ("lockers_done", "INT8"),
                    ("current_locker_pid", "INT8"),
                    ("blocks_total", "INT8"),
                    ("blocks_done", "INT8"),
                    ("tuples_total", "INT8"),
                    ("tuples_done", "INT8"),
                    ("partitions_total", "INT8"),
                    ("partitions_done", "INT8"),
                ]),
                Vec::new(),
            )),
            "pg_stat_progress_basebackup" => Some((
                Self::virtual_columns(&[
                    ("pid", "INT"),
                    ("phase", "TEXT"),
                    ("backup_total", "INT8"),
                    ("backup_streamed", "INT8"),
                    ("tablespaces_total", "INT8"),
                    ("tablespaces_streamed", "INT8"),
                ]),
                Vec::new(),
            )),
            "pg_stat_progress_cluster" => Some((
                Self::virtual_columns(&[
                    ("pid", "INT"),
                    ("datid", "OID"),
                    ("datname", "NAME"),
                    ("relid", "OID"),
                    ("command", "TEXT"),
                    ("phase", "TEXT"),
                    ("cluster_index_relid", "OID"),
                    ("heap_tuples_scanned", "INT8"),
                    ("heap_tuples_written", "INT8"),
                    ("heap_blks_total", "INT8"),
                    ("heap_blks_scanned", "INT8"),
                    ("index_rebuild_count", "INT8"),
                ]),
                Vec::new(),
            )),
            "pg_stat_progress_copy" => Some((
                Self::virtual_columns(&[
                    ("pid", "INT"),
                    ("datid", "OID"),
                    ("datname", "NAME"),
                    ("relid", "OID"),
                    ("command", "TEXT"),
                    ("type", "TEXT"),
                    ("bytes_processed", "INT8"),
                    ("bytes_total", "INT8"),
                    ("tuples_processed", "INT8"),
                    ("tuples_excluded", "INT8"),
                    ("tuples_skipped", "INT8"),
                ]),
                Vec::new(),
            )),
            // Relation IO: this engine reads and writes through its storage
            // engine's key-value interface, which the block counters do not
            // see.
            "pg_statio_user_tables" | "pg_statio_sys_tables" | "pg_statio_all_tables" => {
                let columns = Self::virtual_columns(&[
                    ("relid", "OID"),
                    ("schemaname", "NAME"),
                    ("relname", "NAME"),
                    ("heap_blks_read", "INT8"),
                    ("heap_blks_hit", "INT8"),
                    ("idx_blks_read", "INT8"),
                    ("idx_blks_hit", "INT8"),
                    ("toast_blks_read", "INT8"),
                    ("toast_blks_hit", "INT8"),
                    ("tidx_blks_read", "INT8"),
                    ("tidx_blks_hit", "INT8"),
                ]);
                let rows = tables
                    .iter()
                    .filter(|table| {
                        table.view_query.is_none() && !crate::sequences::is_sequence(table)
                    })
                    .map(|table| {
                        let schema = Self::schema_name_by_id(db_name, schemas, table.schema_id);
                        vec![
                            Value::Int(Self::table_oid(db_name, &schema, &table.name)),
                            Value::Text(schema),
                            Value::Text(table.name.clone()),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                            counter(0),
                        ]
                    })
                    .collect();
                Some((columns, rows))
            }
            "pg_statio_user_indexes" | "pg_statio_sys_indexes" | "pg_statio_all_indexes" => {
                let columns = Self::virtual_columns(&[
                    ("relid", "OID"),
                    ("indexrelid", "OID"),
                    ("schemaname", "NAME"),
                    ("relname", "NAME"),
                    ("indexrelname", "NAME"),
                    ("idx_blks_read", "INT8"),
                    ("idx_blks_hit", "INT8"),
                ]);
                let mut rows = Vec::new();
                for table in tables.iter().filter(|table| table.view_query.is_none()) {
                    let schema = Self::schema_name_by_id(db_name, schemas, table.schema_id);
                    let relid = Self::table_oid(db_name, &schema, &table.name);
                    for index in &table.indexes {
                        rows.push(vec![
                            Value::Int(relid),
                            Value::Int(Self::index_oid(db_name, &schema, &table.name, &index.name)),
                            Value::Text(schema.clone()),
                            Value::Text(table.name.clone()),
                            Value::Text(index.name.clone()),
                            counter(0),
                            counter(0),
                        ]);
                    }
                }
                Some((columns, rows))
            }
            "pg_statio_user_sequences" | "pg_statio_sys_sequences" | "pg_statio_all_sequences" => {
                let columns = Self::virtual_columns(&[
                    ("relid", "OID"),
                    ("schemaname", "NAME"),
                    ("relname", "NAME"),
                    ("blks_read", "INT8"),
                    ("blks_hit", "INT8"),
                ]);
                let rows = tables
                    .iter()
                    .filter(|table| crate::sequences::is_sequence(table))
                    .map(|table| {
                        let schema = Self::schema_name_by_id(db_name, schemas, table.schema_id);
                        vec![
                            Value::Int(Self::table_oid(db_name, &schema, &table.name)),
                            Value::Text(schema),
                            Value::Text(table.name.clone()),
                            counter(0),
                            counter(0),
                        ]
                    })
                    .collect();
                Some((columns, rows))
            }
            _ => None,
        }
    }
}
