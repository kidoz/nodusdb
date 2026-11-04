//! Read-only external views backed by an isolated optional Iceberg CLI worker.
//! The durable representation uses the existing TableFunction plan shape: older
//! executors decode it and reject the unknown function, rather than KV-scanning
//! a new table kind as an empty relation. DROP VIEW remains the rollback path.
use crate::*;
use nodus_common::iceberg::{
    self as wire, Descriptor, Profile, Profiles, Request, Response, Settings,
};
use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, sync_channel};
use std::time::{Duration, Instant};

/// Node-local protected profiles, prepared before server background tasks start.
/// This opaque handle deliberately has no Debug or serialization implementation.
pub struct IcebergRuntime(Option<Arc<Provider>>);
impl IcebergRuntime {
    /// Loads and validates private profiles; disabled settings require no files.
    pub fn load(settings: Settings) -> Result<Self> {
        settings.validate().map_err(anyhow::Error::msg)?;
        Ok(Self(if settings.enabled {
            Some(Arc::new(Provider::load(settings)?))
        } else {
            None
        }))
    }
}

pub(crate) struct Provider {
    settings: Settings,
    profiles: HashMap<String, Profile>,
    active: AtomicUsize,
}
impl Provider {
    fn load(settings: Settings) -> Result<Self> {
        settings.validate().map_err(anyhow::Error::msg)?;
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        anyhow::bail!("Iceberg SQL memory supervision requires Linux or macOS");
        let path = std::path::Path::new(&settings.profiles_file);
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|_| anyhow::anyhow!("cannot inspect Iceberg profiles file"))?;
        check_private(&metadata)?;
        let file = std::fs::File::open(path)
            .map_err(|_| anyhow::anyhow!("cannot open Iceberg profiles file"))?;
        let opened_metadata = file.metadata()?;
        check_private(&opened_metadata)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            anyhow::ensure!(
                metadata.dev() == opened_metadata.dev() && metadata.ino() == opened_metadata.ino(),
                "Iceberg profiles changed while opening; restart with a stable private file"
            );
        }
        let mut bytes = Vec::new();
        file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
        anyhow::ensure!(bytes.len() <= 1024 * 1024, "Iceberg profiles exceed 1 MiB");
        let profiles: Profiles = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid Iceberg profiles file"))?;
        anyhow::ensure!(
            profiles.version == wire::VERSION,
            "unsupported Iceberg profiles version"
        );
        anyhow::ensure!(profiles.profiles.len() <= 64, "too many Iceberg profiles");
        for (name, profile) in &profiles.profiles {
            anyhow::ensure!(
                valid_name(name) && !profile.uri.is_empty(),
                "invalid Iceberg profile"
            );
        }
        Ok(Self {
            settings,
            profiles: profiles.profiles,
            active: AtomicUsize::new(0),
        })
    }
    fn run<T>(
        &self,
        ctx: &ExecutionContext,
        mut request: Request,
        mut consume: impl FnMut(Response) -> Result<Option<T>>,
    ) -> Result<T> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.settings.max_concurrent_scans).then_some(count + 1)
            })
            .map_err(|_| resource("Iceberg concurrent scan limit reached"))?;
        struct Permit<'a>(&'a AtomicUsize);
        impl Drop for Permit<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let _permit = Permit(&self.active);
        let timeout = query_timeout(&self.settings);
        request.timeout_ms = timeout.as_millis() as u64;
        let worker = Worker::start(&self.settings, ctx, request, timeout)?;
        loop {
            worker.check()?;
            let frame = worker
                .rx
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Iceberg worker closed"))?
                .recv_timeout(Duration::from_millis(20));
            worker.check()?;
            match frame {
                Ok(Ok(Response::Error(message))) => {
                    // Worker messages are bounded, but still untrusted. Do not
                    // echo a replacement executable's response into logs/wire.
                    return Err(match message.as_str() {
                        "Iceberg table identity or schema changed; register the view again" =>
                            crate::error_fields::DbError::new(message).code("55000").into(),
                        "Iceberg SQL type is not supported" | "Iceberg snapshot feature is not supported" => unsupported(&message),
                        "canceling statement due to statement timeout" => anyhow::anyhow!(message),
                        "Iceberg row exceeds 16 KiB" => resource(&message),
                        _ => crate::error_fields::DbError::new("Iceberg worker read failed; check profile permissions and source metadata").code("HV000").into(),
                    });
                }
                Ok(Ok(frame)) => {
                    if let Some(result) = consume(frame)? {
                        return Ok(result);
                    }
                }
                Ok(Err(_)) | Err(RecvTimeoutError::Disconnected) => {
                    worker.check()?;
                    anyhow::bail!("Iceberg worker terminated or returned an invalid frame");
                }
                Err(RecvTimeoutError::Timeout) => {}
            }
        }
    }
    fn request(
        &self,
        descriptor: &Descriptor,
        columns: Vec<String>,
        limit: usize,
        scan: bool,
    ) -> Result<Request> {
        let profile = self
            .profiles
            .get(&descriptor.profile)
            .ok_or_else(|| anyhow::anyhow!("Iceberg profile is not configured on this node"))?
            .clone();
        Ok(Request {
            version: wire::VERSION,
            profile,
            namespace: descriptor.namespace.clone(),
            table: descriptor.table.clone(),
            expected: scan.then(|| descriptor.clone()),
            columns,
            limit,
            timeout_ms: 0,
            address_space_bytes: self.settings.worker_address_space_mib * 1024 * 1024,
        })
    }
}
fn check_private(metadata: &std::fs::Metadata) -> Result<()> {
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "Iceberg profiles must be a regular private file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "Iceberg profiles must have no group or other permissions (use chmod 600)"
        );
    }
    Ok(())
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn sql_column_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= 128
        && bytes
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}
fn resource(message: &str) -> anyhow::Error {
    crate::error_fields::DbError::new(message)
        .code("53200")
        .into()
}
fn unsupported(message: &str) -> anyhow::Error {
    crate::error_fields::DbError::new(message)
        .code("0A000")
        .into()
}
fn query_timeout(settings: &Settings) -> Duration {
    let configured = Duration::from_secs(settings.timeout_seconds);
    crate::session_env::setting("statement_timeout")
        .and_then(|s| crate::session_vars::duration_millis(&s))
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis)
        .map_or(configured, |d| d.min(configured))
}

/// Reader and process supervisor are independent: socket backpressure cannot
/// prevent a deadline, cancellation, or RSS excess from killing/reaping Arrow.
struct Worker {
    rx: Option<Receiver<std::io::Result<Response>>>,
    deadline: Instant,
    cancellation: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    stop: Arc<AtomicBool>,
    reason: Arc<AtomicUsize>,
    reader: Option<std::thread::JoinHandle<()>>,
    supervisor: Option<std::thread::JoinHandle<()>>,
}
impl Worker {
    fn start(
        settings: &Settings,
        ctx: &ExecutionContext,
        request: Request,
        timeout: Duration,
    ) -> Result<Self> {
        let deadline = Instant::now() + timeout;
        let mut child = Command::new(&settings.worker_executable)
            .args(["iceberg", "worker"])
            .env_clear()
            .env("TOKIO_WORKER_THREADS", "1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
            .env("AWS_CONFIG_FILE", "/dev/null")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| anyhow::anyhow!("cannot start configured Iceberg worker"))?;
        let pid = child.id();
        let input = child.stdin.take();
        let output = child.stdout.take();
        let (tx, rx) = sync_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let reason = Arc::new(AtomicUsize::new(0));
        let stop_child = stop.clone();
        let reason_child = reason.clone();
        let cancelled = ctx.cancellation.clone();
        let memory = settings.worker_memory_mib * 1024 * 1024;
        let supervisor = std::thread::spawn(move || {
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) => {}
                    Err(_) => break,
                }
                let code = if cancelled.as_ref().is_some_and(|f| f()) {
                    1
                } else if Instant::now() >= deadline {
                    2
                } else {
                    match resident_bytes(pid) {
                        Ok(bytes) if bytes > memory => 3,
                        Err(_) => {
                            // A worker can exit between try_wait and the RSS
                            // probe. Reap that normal exit; monitoring failures
                            // for a live process must fail closed.
                            if matches!(child.try_wait(), Ok(Some(_))) {
                                return;
                            }
                            4
                        }
                        _ => 0,
                    }
                };
                if code != 0 {
                    reason_child.store(code, Ordering::Release);
                }
                if code != 0 || stop_child.load(Ordering::Acquire) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = child.kill();
            let _ = child.wait();
        });
        let reader = std::thread::spawn(move || {
            let result = (|| -> std::io::Result<()> {
                let mut input = input.ok_or_else(|| std::io::Error::other("missing stdin"))?;
                let mut output = output.ok_or_else(|| std::io::Error::other("missing stdout"))?;
                wire::write_frame(&mut input, &request)?;
                drop(input);
                loop {
                    let frame: Response = wire::read_frame(&mut output)?;
                    let terminal = matches!(frame, Response::Done { .. } | Response::Error(_));
                    if tx.send(Ok(frame)).is_err() || terminal {
                        return Ok(());
                    }
                }
            })();
            if let Err(error) = result {
                let _ = tx.send(Err(error));
            }
        });
        Ok(Self {
            rx: Some(rx),
            deadline,
            cancellation: ctx.cancellation.clone(),
            stop,
            reason,
            reader: Some(reader),
            supervisor: Some(supervisor),
        })
    }
    fn check(&self) -> Result<()> {
        let reason = self.reason.load(Ordering::Acquire);
        if reason == 1 || self.cancellation.as_ref().is_some_and(|f| f()) {
            anyhow::bail!("canceling statement due to user request");
        }
        if reason == 3 {
            return Err(resource(
                "Iceberg decoder process exceeded its memory limit",
            ));
        }
        if reason == 4 {
            anyhow::bail!("cannot supervise Iceberg worker memory");
        }
        // Checking the clock here closes the race in which an upstream HTTP
        // timeout frame arrives between the supervisor's ten-millisecond polls.
        if reason == 2 || Instant::now() >= self.deadline {
            anyhow::bail!("canceling statement due to statement timeout");
        }
        Ok(())
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.rx.take();
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.supervisor.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
    }
}
fn resident_bytes(pid: u32) -> std::io::Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
        // An exited process can have no VmRSS, which is harmless.
        Ok(status
            .lines()
            .find_map(|line| {
                line.strip_prefix("VmRSS:")
                    .and_then(|s| s.split_whitespace().next())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0)
            * 1024)
    }
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("/bin/ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()?;
        if !output.status.success() {
            return Err(std::io::Error::other("RSS probe failed"));
        }
        let kib = std::str::from_utf8(&output.stdout)
            .map_err(std::io::Error::other)?
            .trim()
            .parse::<u64>()
            .map_err(std::io::Error::other)?;
        Ok(kib.saturating_mul(1024))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    Err(std::io::Error::other("memory supervision unavailable"))
}

fn stored_descriptor(table: &nodus_catalog::TableDescriptor) -> Result<Option<Descriptor>> {
    let Some(query) = &table.view_query else {
        return Ok(None);
    };
    let plan: LogicalPlan = serde_json::from_str(query)?;
    if let LogicalPlan::TableFunction(spec) = plan
        && spec.name == wire::STORED_FUNCTION
    {
        let [Operand::Literal(Value::Text(json))] = spec.args.as_slice() else {
            anyhow::bail!("invalid Iceberg descriptor");
        };
        let descriptor: Descriptor = serde_json::from_str(json)
            .map_err(|_| anyhow::anyhow!("invalid Iceberg descriptor"))?;
        anyhow::ensure!(
            descriptor.version == wire::VERSION,
            "unsupported Iceberg descriptor version"
        );
        anyhow::ensure!(
            !descriptor.fields.is_empty() && descriptor.fields.len() <= 256,
            "invalid Iceberg descriptor fields"
        );
        return Ok(Some(descriptor));
    }
    Ok(None)
}

/// Exact public registration form; other table-function shapes fail through the
/// existing unknown-function path. No reserved function is executable from SQL.
fn registration(plan: &LogicalPlan) -> Option<TableFnSpec> {
    let LogicalPlan::Select {
        ctes,
        table_name,
        projection,
        joins,
        filter,
        group_by,
        group_exprs,
        grouping_sets,
        having,
        order_by,
        sort,
        limit,
        offset,
        distinct,
        distinct_on,
        sample,
        ..
    } = plan
    else {
        return None;
    };
    if !projection.is_empty()
        || !joins.is_empty()
        || filter.is_some()
        || !group_by.is_empty()
        || !group_exprs.is_empty()
        || grouping_sets.is_some()
        || having.is_some()
        || !order_by.is_empty()
        || !sort.is_empty()
        || limit.is_some()
        || offset.is_some()
        || *distinct
        || !distinct_on.is_empty()
        || sample.is_some()
    {
        return None;
    }
    let [(alias, inner)] = ctes.as_slice() else {
        return None;
    };
    let LogicalPlan::TableFunction(spec) = inner.as_ref() else {
        return None;
    };
    (alias == table_name
        && spec.name == "nodus_iceberg"
        && !spec.with_ordinality
        && spec.column_aliases.is_empty()
        && spec.arg_exprs.is_empty())
    .then(|| spec.clone())
}
impl MemExecutor {
    pub(crate) fn reject_iceberg_mutation(table: &nodus_catalog::TableDescriptor) -> Result<()> {
        if stored_descriptor(table)?.is_some() {
            return Err(unsupported(
                "Iceberg views are read-only; drop and register the view to change its schema",
            ));
        }
        Ok(())
    }
    /// Installs node-local protected profiles. Disabled nodes fail closed when
    /// reading persisted external views; profiles are never catalog records.
    pub fn configure_iceberg(&self, settings: Settings) -> Result<()> {
        self.install_iceberg(IcebergRuntime::load(settings)?);
        Ok(())
    }
    /// Installs profiles already prepared during server startup.
    pub fn install_iceberg(&self, runtime: IcebergRuntime) {
        *self.iceberg.write() = runtime.0;
    }
    pub(crate) fn register_iceberg(
        &self,
        ctx: &ExecutionContext,
        name: &str,
        query: &LogicalPlan,
        or_replace: bool,
    ) -> Result<Option<QueryOutput>> {
        let Some(mut spec) = registration(query) else {
            return Ok(None);
        };
        self.authorize(ctx, Action::ManageCluster, ResourceRef::System)?;
        let (db_name, schema_name, view_name) = parse_object_name(name)?;
        let db = self.catalog_reader.get_database(db_name)?;
        let schema = self.catalog_reader.get_schema(db_name, schema_name)?;
        self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(schema.id))?;
        anyhow::ensure!(
            !or_replace && !crate::search_path::is_temp_schema(schema_name),
            "Iceberg registration does not support OR REPLACE or temporary views"
        );
        anyhow::ensure!(
            !self
                .active_txns
                .read()
                .get(&ctx.session_id)
                .is_some_and(ActiveTxn::in_block),
            "Iceberg registration cannot run inside a transaction block"
        );
        anyhow::ensure!(
            self.catalog_reader
                .get_table(db_name, schema_name, view_name)
                .is_err(),
            "relation already exists"
        );
        self.reject_type_name(schema_name, view_name)?;
        let args = spec
            .args
            .iter()
            .map(|a| match a {
                Operand::Literal(Value::Text(s)) => Ok(s.clone()),
                _ => Err(unsupported(
                    "Iceberg registration requires three literal strings",
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        let [profile, namespace, table] = args.as_slice() else {
            return Err(unsupported(
                "Iceberg registration requires profile, namespace and table",
            ));
        };
        anyhow::ensure!(
            valid_name(profile)
                && !namespace.is_empty()
                && namespace.len() <= 512
                && !table.is_empty()
                && table.len() <= 256,
            "invalid Iceberg registration names"
        );
        let mut descriptor = Descriptor {
            version: wire::VERSION,
            profile: profile.clone(),
            namespace: vec![namespace.clone()],
            table: table.clone(),
            table_uuid: String::new(),
            fields: vec![],
        };
        let provider = self
            .iceberg
            .read()
            .clone()
            .ok_or_else(|| unsupported("Iceberg SQL is not enabled on this node"))?;
        let request = provider.request(&descriptor, vec![], 0, false)?;
        let mut seen = false;
        provider.run(ctx, request, |frame| match frame {
            Response::Schema {
                version,
                table_uuid,
                fields,
                ..
            } if !seen && version == wire::VERSION => {
                anyhow::ensure!(
                    !fields.is_empty() && fields.len() <= 256,
                    "invalid Iceberg schema"
                );
                // Existing plans flatten quoted and qualified identifiers.
                // Reject ambiguous names rather than resolve "a.b" as field b.
                if fields.iter().any(|field| !sql_column_name(&field.name)) {
                    return Err(unsupported(
                        "Iceberg SQL column names must be ASCII identifiers of at most 128 bytes",
                    ));
                }
                descriptor.table_uuid = table_uuid;
                descriptor.fields = fields;
                seen = true;
                Ok(None)
            }
            Response::Done { rows: 0 } if seen => Ok(Some(())),
            _ => anyhow::bail!("invalid Iceberg inspection response"),
        })?;
        let now = Utc::now();
        let columns = descriptor
            .fields
            .iter()
            .map(|field| ColumnDescriptor {
                id: nodus_catalog::ColumnId::new(),
                name: field.name.clone(),
                version: 1,
                created_at: now,
                updated_at: now,
                state: DescriptorState::Public,
                data_type: field.sql_type.clone(),
                nullable: field.nullable,
                default_expr: None,
                comment: None,
                statistics_target: -1,
            })
            .collect();
        spec.name = wire::STORED_FUNCTION.into();
        spec.args = vec![Operand::Literal(Value::Text(serde_json::to_string(
            &descriptor,
        )?))];
        spec.alias = None;
        // One durable, replicated catalog publication, after complete inspection.
        self.catalog_writer.create_table(CreateTableRequest {
            id: TableId::new(),
            database_id: db.id,
            schema_id: schema.id,
            name: view_name.into(),
            columns,
            constraints: vec![],
            view_query: Some(serde_json::to_string(&LogicalPlan::TableFunction(spec))?),
            materialized_query: None,
            parents: vec![],
            partition_by: None,
            partition_bound: None,
        })?;
        tracing::info!(table_id = %descriptor.table_uuid, "Iceberg external view registered");
        Ok(Some(QueryOutput::tag("CREATE VIEW")))
    }

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
            || filter.is_some()
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
                "Iceberg SQL supports column projection and LIMIT only",
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
        let positions: Vec<_> = indices
            .iter()
            .map(|i| {
                selected
                    .iter()
                    .position(|name| *name == descriptor.fields[*i].name)
                    .unwrap_or(0)
            })
            .collect();
        let request = provider.request(
            &descriptor,
            selected.clone(),
            limit.unwrap_or(provider.settings.max_rows + 1),
            true,
        )?;
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
                    version == wire::VERSION
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
                if count >= limit.unwrap_or(provider.settings.max_rows) {
                    return Err(resource(
                        "Iceberg result exceeds max_rows; specify a smaller LIMIT",
                    ));
                }
                let mut values = Vec::with_capacity(indices.len());
                let mut row_bytes =
                    2 * std::mem::size_of::<Row>() + indices.len() * std::mem::size_of::<Value>();
                for (output, input) in positions.iter().enumerate() {
                    let value = match &row[*input] {
                        None => Value::Null,
                        Some(text) => {
                            row_bytes = row_bytes.saturating_add(text.len() + 64);
                            crate::value::coerce_for_column(
                                &Value::Text(text.clone()),
                                &types[output],
                            )
                        }
                    };
                    values.push(value);
                }
                crate::eval_error::check()?;
                if row_bytes > 16 * 1024 {
                    return Err(resource("Iceberg row exceeds 16 KiB"));
                }
                bytes = bytes.saturating_add(row_bytes);
                if bytes > provider.settings.max_result_bytes {
                    return Err(resource(
                        "Iceberg result exceeds max_result_bytes; specify a smaller LIMIT",
                    ));
                }
                sink.row(Row { values })?;
                count += 1;
                Ok(None)
            }
            Response::Done { rows } if schema_sent && rows == count => {
                Ok(Some(format!("SELECT {count}")))
            }
            _ => anyhow::bail!("invalid Iceberg scan response"),
        });
        tracing::debug!(
            rows = count,
            result_bytes = bytes,
            success = result.is_ok(),
            "Iceberg SQL scan finished"
        );
        result.map(Some)
    }
    pub(crate) fn collect_iceberg(
        &self,
        ctx: &ExecutionContext,
        plan: &LogicalPlan,
    ) -> Result<Option<QueryOutput>> {
        // A disabled node reaches this existing view-plan shape through the
        // normal view executor. Preserve a useful, fail-closed error there.
        if matches!(plan, LogicalPlan::TableFunction(spec) if spec.name == wire::STORED_FUNCTION)
            && self.iceberg.read().is_none()
        {
            return Err(unsupported("Iceberg SQL is not enabled on this node"));
        }
        struct Collect {
            columns: Vec<String>,
            types: Vec<String>,
            rows: Vec<Row>,
        }
        impl RowSink for Collect {
            fn schema(&mut self, columns: Vec<String>, types: Vec<String>) {
                self.columns = columns;
                self.types = types;
            }
            fn row(&mut self, row: Row) -> Result<()> {
                self.rows.push(row);
                Ok(())
            }
        }
        let mut sink = Collect {
            columns: vec![],
            types: vec![],
            rows: vec![],
        };
        Ok(self
            .stream_iceberg(ctx, plan, &mut sink)?
            .map(|tag| QueryOutput {
                columns: sink.columns,
                types: sink.types,
                rows: sink.rows,
                tag,
            }))
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;

    #[test]
    fn deadline_is_checked_before_watchdog_records_a_timeout() {
        // An HTTP error may be delivered between watchdog polls. An expired
        // clock must win even while the supervisor's reason is still zero.
        let worker = Worker {
            rx: None,
            deadline: Instant::now() - Duration::from_millis(1),
            cancellation: None,
            stop: Arc::new(AtomicBool::new(false)),
            reason: Arc::new(AtomicUsize::new(0)),
            reader: None,
            supervisor: None,
        };
        assert!(
            worker
                .check()
                .unwrap_err()
                .to_string()
                .contains("statement timeout")
        );
    }
}
