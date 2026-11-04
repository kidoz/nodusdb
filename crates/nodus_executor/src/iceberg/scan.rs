use super::*;

impl MemExecutor {
    pub(crate) fn stream_iceberg(
        &self,
        ctx: &ExecutionContext,
        plan: &LogicalPlan,
        sink: &mut dyn RowSink,
    ) -> Result<Option<String>> {
        let LogicalPlan::Select {
            table_name,
            table_alias,
            ctes,
            joins,
            projection,
            group_by,
            group_exprs,
            grouping_sets,
            filter,
            having,
            order_by,
            sort,
            distinct,
            distinct_on,
            sample,
            limit,
            offset,
            ..
        } = plan
        else {
            return Ok(None);
        };
        // The default-disabled feature must not add catalog cloning or stored
        // view-plan decoding to ordinary OLTP reads.
        if self.iceberg.read().is_none() {
            return Ok(None);
        }
        let (db, schema, relation) = parse_object_name(table_name)?;
        let Ok(table) = self.catalog_reader.get_table(db, schema, relation) else {
            return Ok(None);
        };
        let Some(descriptor) = stored_descriptor(&table)? else {
            return Ok(None);
        };
        self.authorize(ctx, Action::Select, ResourceRef::Table(table.id))?;
        if !ctes.is_empty()
            || !joins.is_empty()
            || !group_by.is_empty()
            || !group_exprs.is_empty()
            || grouping_sets.is_some()
            || having.is_some()
            || !order_by.is_empty()
            || !sort.is_empty()
            || *distinct
            || !distinct_on.is_empty()
            || sample.is_some()
            || offset.is_some()
            || !projection.iter().all(|p| {
                matches!(
                    p,
                    ProjectionItem::Column(_) | ProjectionItem::AliasedColumn(_, _)
                )
            })
        {
            return Err(unsupported(
                "Iceberg SQL supports projection, simple WHERE and LIMIT only",
            ));
        }
        let provider = self
            .iceberg
            .read()
            .clone()
            .ok_or_else(|| unsupported("Iceberg SQL is not enabled on this node"))?;
        let mut indices = Vec::new();
        let mut names = Vec::new();
        if projection.is_empty() {
            indices.extend(0..descriptor.fields.len());
            names.extend(descriptor.fields.iter().map(|f| f.name.clone()));
        }
        for item in projection {
            let (column, alias) = match item {
                ProjectionItem::Column(c) => (c, c),
                ProjectionItem::AliasedColumn(c, a) => (c, a),
                _ => unreachable!(),
            };
            let (qualifier, short) = column
                .rsplit_once('.')
                .map_or((None, column.as_str()), |(q, c)| (Some(q), c));
            if let Some(qualifier) = qualifier {
                anyhow::ensure!(
                    qualifier == table_alias.as_deref().unwrap_or(relation)
                        || qualifier == table_name,
                    "missing FROM-clause entry for table"
                );
            }
            let index = descriptor
                .fields
                .iter()
                .position(|f| f.name == short)
                .ok_or_else(|| anyhow::anyhow!("column \"{short}\" does not exist"))?;
            indices.push(index);
            names.push(if matches!(item, ProjectionItem::Column(_)) {
                short.into()
            } else {
                alias.clone()
            });
        }
        let types: Vec<_> = indices
            .iter()
            .map(|i| descriptor.fields[*i].sql_type.clone())
            .collect();
        anyhow::ensure!(
            limit.is_none_or(|l| l <= provider.settings.max_rows),
            "Iceberg LIMIT exceeds configured max_rows"
        );
        let bound = predicates::bind(
            filter.as_ref(),
            &descriptor,
            relation,
            table_alias.as_deref(),
        )?;
        sink.set_deadline(Instant::now() + query_timeout(&provider.settings));
        // LIMIT 0/Describe is local and uses registered types; no source access.
        if *limit == Some(0) {
            sink.schema(names, types);
            return Ok(Some("SELECT 0".into()));
        }
        let mut selected = Vec::new();
        for i in &indices {
            let name = &descriptor.fields[*i].name;
            if !selected.contains(name) {
                selected.push(name.clone());
            }
        }
        for index in &bound.columns {
            let name = &descriptor.fields[*index].name;
            if !selected.contains(name) {
                selected.push(name.clone());
            }
        }
        let scan_types: Vec<_> = selected
            .iter()
            .map(|name| {
                descriptor
                    .fields
                    .iter()
                    .find(|field| field.name == *name)
                    .map(|field| field.sql_type.clone())
                    .ok_or_else(|| anyhow::anyhow!("invalid Iceberg projection"))
            })
            .collect::<Result<_>>()?;
        let mut scan_columns: Vec<_> = selected
            .iter()
            .map(|name| {
                table
                    .columns
                    .iter()
                    .find(|column| column.name == *name)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("invalid Iceberg registered column"))
            })
            .collect::<Result<_>>()?;
        let date_positions: Vec<_> = selected
            .iter()
            .enumerate()
            .filter_map(|(position, name)| {
                bound
                    .dates
                    .iter()
                    .any(|index| descriptor.fields[*index].name == *name)
                    .then_some(position)
            })
            .collect();
        for position in &date_positions {
            scan_columns[*position].data_type = "BIGINT".into();
        }
        let positions: Vec<_> = indices
            .iter()
            .map(|i| {
                selected
                    .iter()
                    .position(|name| *name == descriptor.fields[*i].name)
                    .unwrap_or(0)
            })
            .collect();
        let mut request = provider.request(
            &descriptor,
            selected.clone(),
            provider.settings.max_scan_rows + 1,
            true,
        )?;
        request.predicates = bound.pushdown;
        let mut scan_rows = 0usize;
        let mut scan_bytes = 0usize;
        let mut stats_seen = false;
        let mut schema_sent = false;
        let mut count = 0;
        let mut bytes = 0usize;
        let result = provider.run(ctx, request, |frame| match frame {
            Response::Schema {
                version,
                table_uuid,
                fields,
                snapshot_id,
            } if !schema_sent => {
                anyhow::ensure!(
                    version == wire::WORKER_VERSION
                        && table_uuid == descriptor.table_uuid
                        && fields.len() == selected.len()
                        && fields
                            .iter()
                            .zip(&selected)
                            .all(|(f, n)| f.name == *n && descriptor.fields.contains(f)),
                    "Iceberg schema changed; register the view again"
                );
                tracing::debug!(%table_uuid, ?snapshot_id, "Iceberg SQL snapshot pinned");
                sink.schema(names.clone(), types.clone());
                schema_sent = true;
                Ok(None)
            }
            Response::Row(row) if schema_sent => {
                anyhow::ensure!(row.len() == selected.len(), "invalid Iceberg row width");
                scan_rows += 1;
                if scan_rows > provider.settings.max_scan_rows {
                    return Err(resource("Iceberg scan exceeds max_scan_rows"));
                }
                let mut scanned = Vec::with_capacity(row.len());
                let mut source_bytes =
                    2 * std::mem::size_of::<Row>() + row.len() * std::mem::size_of::<Value>();
                for (index, value) in row.iter().enumerate() {
                    let value = match value {
                        None => Value::Null,
                        Some(text) => {
                            source_bytes = source_bytes.saturating_add(text.len() + 64);
                            crate::value::coerce_for_column(
                                &Value::Text(text.clone()),
                                &scan_types[index],
                            )
                        }
                    };
                    scanned.push(value);
                }
                crate::eval_error::check()?;
                if source_bytes > 16 * 1024 {
                    return Err(resource("Iceberg row exceeds 16 KiB"));
                }
                scan_bytes = scan_bytes.saturating_add(source_bytes);
                if scan_bytes > provider.settings.max_scan_bytes {
                    return Err(resource("Iceberg scan exceeds max_scan_bytes"));
                }
                let mut date_values = (!date_positions.is_empty()).then(|| scanned.clone());
                if let Some(date_values) = &mut date_values {
                    for position in &date_positions {
                        if let Some(raw) = &row[*position] {
                            date_values[*position] =
                                Value::Int(i64::from(predicates::date_days(raw)?));
                        }
                    }
                }
                let filter_values = date_values.as_deref().unwrap_or(&scanned);
                let matches = self.eval_filter(
                    ctx,
                    filter_values,
                    &selected,
                    &scan_columns,
                    bound.residual.as_ref(),
                ) == Some(true);
                crate::eval_error::check()?;
                if !matches {
                    return Ok(None);
                }
                if count >= provider.settings.max_rows {
                    return Err(resource(
                        "Iceberg result exceeds max_rows; specify a smaller LIMIT",
                    ));
                }
                let mut row_bytes =
                    2 * std::mem::size_of::<Row>() + positions.len() * std::mem::size_of::<Value>();
                for position in &positions {
                    if let Some(text) = &row[*position] {
                        row_bytes = row_bytes.saturating_add(text.len() + 64);
                    }
                }
                if row_bytes > 16 * 1024 {
                    return Err(resource("Iceberg row exceeds 16 KiB"));
                }
                bytes = bytes.saturating_add(row_bytes);
                if bytes > provider.settings.max_result_bytes {
                    return Err(resource(
                        "Iceberg result exceeds max_result_bytes; specify a smaller LIMIT",
                    ));
                }
                let values = positions
                    .iter()
                    .map(|position| scanned[*position].clone())
                    .collect();
                sink.row(Row { values })?;
                count += 1;
                // LIMIT applies to exact SQL matches, never to input rows or
                // approximate file pruning. Drop kills/reaps the worker early.
                Ok(limit
                    .filter(|limit| count == *limit)
                    .map(|_| format!("SELECT {count}")))
            }
            Response::ScanStats(stats) if schema_sent && !stats_seen => {
                anyhow::ensure!(
                    stats.rows_read as usize == scan_rows,
                    "invalid Iceberg scan statistics"
                );
                tracing::debug!(
                    planned_files = stats.planned_files,
                    planned_file_bytes = stats.planned_file_bytes,
                    scan_rows,
                    "Iceberg file pruning completed"
                );
                stats_seen = true;
                Ok(None)
            }
            Response::Done { rows } if schema_sent && rows == scan_rows => {
                Ok(Some(format!("SELECT {count}")))
            }
            _ => anyhow::bail!("invalid Iceberg scan response"),
        });
        tracing::debug!(
            rows = count,
            scan_rows,
            scan_bytes,
            result_bytes = bytes,
            success = result.is_ok(),
            "Iceberg SQL scan finished"
        );
        result.map(Some)
    }
}
