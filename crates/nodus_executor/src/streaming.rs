//! Streaming execution: produce a query's rows one at a time into a [`RowSink`]
//! instead of materializing them all up front.
//!
//! Only a *plain single-table scan* streams — `SELECT [*|cols] FROM t [WHERE ...]
//! [LIMIT/OFFSET]` with column-only projection and no join, grouping, ordering,
//! DISTINCT, HAVING, CTE, view, or virtual table — because every other operator
//! needs the full input buffered anyway. For those, and for any case the
//! fast-path can't handle, we fall back to [`MemExecutor::execute_logical`] and
//! push the materialized rows, so behavior is unchanged.

use crate::*;
use anyhow::Result;
use bytes::Bytes;
use nodus_storage_api::{KeyRange, KvEngine};

impl MemExecutor {
    /// Streams `plan` to `sink` if it is a plain single-table scan; otherwise
    /// executes it fully and pushes the resulting rows. Returns the command tag.
    pub(crate) fn stream_or_fallback(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
        sink: &mut dyn RowSink,
    ) -> Result<String> {
        if let LogicalPlan::Select {
            ctes,
            table_name,
            table_alias,
            joins,
            projection,
            group_by,
            filter,
            having,
            order_by,
            limit,
            offset,
            distinct,
        } = &plan
            && ctes.is_empty()
            && joins.is_empty()
            && group_by.is_empty()
            && having.is_none()
            && order_by.is_empty()
            && !*distinct
            && projection.iter().all(|p| {
                matches!(
                    p,
                    ProjectionItem::Column(_) | ProjectionItem::AliasedColumn(_, _)
                )
            })
            && let Some(tag) = self.try_scan_streaming(
                ctx,
                table_name,
                table_alias.as_deref(),
                projection,
                filter.as_ref(),
                *limit,
                *offset,
                sink,
            )?
        {
            return Ok(tag);
        }

        // Fallback: full execution (which fences restores and manages the implicit
        // transaction itself), then push the materialized rows.
        let out = self.execute_logical(ctx, plan)?;
        sink.schema(out.columns, out.types);
        for row in out.rows {
            sink.row(row)?;
        }
        Ok(out.tag)
    }

    /// Attempts to stream a plain single-table scan. Returns `Ok(None)` — without
    /// emitting anything — when the table isn't a streamable base table (a view,
    /// a virtual/system table, an unresolved column, or a session with pending
    /// writes to it), so the caller can fall back. On success it emits the schema,
    /// then one row per matching tuple, and returns the command tag.
    #[allow(clippy::too_many_arguments)]
    fn try_scan_streaming(
        &self,
        ctx: &ExecutionContext,
        table_name: &str,
        table_alias: Option<&str>,
        projection: &[ProjectionItem],
        filter: Option<&FilterExpr>,
        limit: Option<usize>,
        offset: Option<usize>,
        sink: &mut dyn RowSink,
    ) -> Result<Option<String>> {
        // Fence query execution during a restore and hold a drain guard for this
        // call, exactly as `execute_logical` does, so no scan observes a partially
        // restored engine.
        if self.restoring.load(std::sync::atomic::Ordering::Acquire) {
            anyhow::bail!("restore in progress; retry shortly");
        }
        let _drain_guard = self.restore_gate.read().unwrap();

        let (db_name, schema_name, table_only) = parse_object_name(table_name)?;
        let schema_name = if schema_name.eq_ignore_ascii_case("public")
            && Self::is_pg_catalog_virtual_table_name(table_only)
        {
            "pg_catalog"
        } else {
            schema_name
        };
        // Virtual/system tables are computed and small; let the full path build them.
        if Self::is_virtual_schema(schema_name) {
            return Ok(None);
        }

        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Select, ResourceRef::Table(tbl.id))?;

        // Views recurse into a subquery; let the full path handle them.
        if tbl.view_query.is_some() {
            return Ok(None);
        }

        // A capped/streamed committed scan could skip a pending row that sorts
        // within the result, so fall back when the session has uncommitted writes
        // to this table. A read-only/autocommit statement has none.
        {
            let guard = self.active_txns.read().unwrap();
            if let Some(txn) = guard.get(&ctx.session_id) {
                let start = format!("{}:", tbl.id);
                let end = format!("{};", tbl.id);
                if txn.overlay.keys().any(|k| k >= &start && k < &end) {
                    return Ok(None);
                }
            }
        }

        let prefix = table_alias.unwrap_or(table_name);
        let col_names: Vec<String> = tbl
            .columns
            .iter()
            .map(|c| format!("{}.{}", prefix, c.name))
            .collect();
        let joined_columns = tbl.columns.clone();

        // Resolve output columns, types, and source indices up front. Types come
        // straight from the catalog descriptors, so the schema is known before any
        // row is read. An unresolvable column triggers the fallback.
        let (out_cols, out_types, indices): (Vec<String>, Vec<String>, Vec<usize>) = if projection
            .is_empty()
        {
            (
                tbl.columns.iter().map(|c| c.name.clone()).collect(),
                tbl.columns.iter().map(|c| c.data_type.clone()).collect(),
                (0..tbl.columns.len()).collect(),
            )
        } else {
            let mut cols = Vec::with_capacity(projection.len());
            let mut types = Vec::with_capacity(projection.len());
            let mut idx = Vec::with_capacity(projection.len());
            for item in projection {
                let (source, out_name) = match item {
                    ProjectionItem::Column(c) => (c, c.split('.').last().unwrap_or(c).to_string()),
                    ProjectionItem::AliasedColumn(c, a) => (c, a.clone()),
                    _ => return Ok(None),
                };
                let Some(i) = col_names
                    .iter()
                    .position(|tc| tc == source || tc.ends_with(&format!(".{}", source)))
                else {
                    return Ok(None);
                };
                cols.push(out_name);
                types.push(tbl.columns[i].data_type.clone());
                idx.push(i);
            }
            (cols, types, idx)
        };

        // Committed to streaming: emit the schema, then the rows.
        sink.schema(out_cols, out_types);

        let read_ts = self.read_ts(&ctx.session_id);
        let start = Bytes::from(format!("{}:", tbl.id));
        let end = Bytes::from(format!("{};", tbl.id));
        let mut produced = 0usize;
        let mut to_skip = offset.unwrap_or(0);

        for pair in self.kv.scan(KeyRange { start, end }, read_ts)? {
            if let Some(lim) = limit
                && produced >= lim
            {
                break;
            }
            let pair = pair?;
            let values: Vec<Value> = serde_json::from_slice(&pair.value)?;

            let keep = self
                .eval_filter(ctx, &values, &col_names, &joined_columns, filter)
                .unwrap_or(false);
            if !keep {
                continue;
            }
            if to_skip > 0 {
                to_skip -= 1;
                continue;
            }

            let out_row: Vec<Value> = indices
                .iter()
                .map(|&i| values.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            sink.row(Row { values: out_row })?;
            produced += 1;
        }

        Ok(Some(format!("SELECT {produced}")))
    }
}
