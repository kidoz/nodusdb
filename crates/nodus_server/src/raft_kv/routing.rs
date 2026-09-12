//! Read a table's validated routing snapshot once, preflight every assigned
//! group, then consume disjoint ranges lazily. No row collection at this layer.

use super::*;
use nodus_sharding::RoutingSnapshot;

type Scan<T> = Box<dyn Iterator<Item = Result<T>> + Send>;
type OpenScan<T> = Box<dyn Fn(Arc<dyn KvEngine>, KeyRange) -> Result<Scan<T>> + Send>;

struct RouteGuard {
    table: Option<TableId>,
    snapshot: Option<RoutingSnapshot>,
    router: Arc<dyn ShardRouter>,
    manager: Arc<MultiRaftManager>,
    groups: Vec<String>,
}

impl RouteGuard {
    fn check(&self) -> Result<()> {
        if let Some(table) = self.table {
            anyhow::ensure!(
                self.router.snapshot(table)? == self.snapshot,
                "shard routing changed for table {table}; retry the transaction"
            );
        }
        for group in &self.groups {
            require_host(&self.manager, group)?;
        }
        Ok(())
    }
}

fn require_host(manager: &MultiRaftManager, group: &str) -> Result<()> {
    // Unsharded/raw metadata reads also serve bootstrap before meta election.
    anyhow::ensure!(
        group == META_SHARD || manager.hosts(group),
        "shard unavailable: {group} is not hosted on this node; retry after replica reconciliation"
    );
    Ok(())
}

struct RoutedScan<T> {
    guard: RouteGuard,
    routes: std::vec::IntoIter<(Arc<dyn KvEngine>, KeyRange)>,
    current: Option<Scan<T>>,
    open: OpenScan<T>,
    done: bool,
}

impl<T> Iterator for RoutedScan<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = (|| -> Result<Option<T>> {
            self.guard.check()?;
            loop {
                if let Some(current) = &mut self.current {
                    if let Some(item) = current.next() {
                        let item = item?;
                        self.guard.check()?;
                        return Ok(Some(item));
                    }
                    self.current = None;
                }
                let Some((engine, range)) = self.routes.next() else {
                    self.guard.check()?;
                    return Ok(None);
                };
                self.current = Some((self.open)(engine, range)?);
            }
        })();
        match result {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

impl RaftKvEngine {
    pub(super) fn route(&self, key: &[u8]) -> Result<String> {
        let group = match parse_table_id(key) {
            Some(table) => match self.shard_router.snapshot(table)? {
                Some(snapshot) => MultiRaftManager::data_group_id(snapshot.locate_key(key)?),
                None => META_SHARD.to_string(),
            },
            None => META_SHARD.to_string(),
        };
        require_host(&self.manager, &group)?;
        Ok(group)
    }

    fn scan_routes(&self, range: KeyRange) -> Result<(RouteGuard, Vec<(String, KeyRange)>)> {
        let table = parse_table_id(&range.start);
        let snapshot = table
            .map(|t| self.shard_router.snapshot(t))
            .transpose()?
            .flatten();
        let routes = if range.start >= range.end {
            Vec::new()
        } else {
            if let Some(table) = table {
                anyhow::ensure!(
                    range.end.as_ref() <= format!("{table};").as_bytes(),
                    "unsupported row scan spanning multiple tables; scan each table separately"
                );
            }
            match &snapshot {
                Some(snapshot) => snapshot
                    .intersect(&range)
                    .into_iter()
                    .map(|(id, range)| (MultiRaftManager::data_group_id(id), range))
                    .collect(),
                None => vec![(META_SHARD.to_string(), range)],
            }
        };
        let guard = RouteGuard {
            table,
            snapshot,
            router: self.shard_router.clone(),
            manager: self.manager.clone(),
            groups: routes.iter().map(|(group, _)| group.clone()).collect(),
        };
        guard.check()?;
        Ok((guard, routes))
    }

    pub(super) fn routed_scan<T: Send + 'static>(
        &self,
        range: KeyRange,
        open: impl Fn(Arc<dyn KvEngine>, KeyRange) -> Result<Scan<T>> + Send + 'static,
    ) -> Result<Scan<T>> {
        let (guard, routes) = self.scan_routes(range)?;
        tracing::debug!(groups = routes.len(), "opening routed range scan");
        // At most one underlying iterator is opened at a time. In particular a
        // LIMIT on the first shard does not materialize later shard scans.
        let routes = routes
            .into_iter()
            .map(|(group, range)| (self.engine_for(&group), range))
            .collect::<Vec<_>>()
            .into_iter();
        Ok(Box::new(RoutedScan {
            guard,
            routes,
            current: None,
            open: Box::new(open),
            done: false,
        }))
    }

    pub(super) fn range_barrier(&self, range: KeyRange) -> Result<()> {
        let (guard, routes) = self.scan_routes(range)?;
        // Independent ReadIndex barriers do not provide one cluster-wide
        // snapshot or resolve partially applied 2PC decisions. Refuse this mode
        // explicitly until that shared snapshot/decision protocol exists.
        anyhow::ensure!(
            routes.len() <= 1,
            "unsupported linearizable cross-shard range read: shared snapshot coordination is not implemented"
        );
        for (group, _) in routes {
            self.router.read_barrier(&group)?;
            self.metrics.linearizable_reads_total.inc();
        }
        guard.check()
    }
}
