use super::*;
use nodus_catalog::{DescriptorState, ShardDescriptor, ShardId};
use nodus_meta::{MetaStore, ShardMap};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture {
    engine: Arc<RaftKvEngine>,
    meta: Arc<nodus_meta::MemMetaStore>,
    table: TableId,
    ids: Vec<ShardId>,
}

impl Fixture {
    fn key(&self, suffix: &str) -> Bytes {
        Bytes::from(format!("{}:{suffix}", self.table))
    }
    fn range(&self) -> KeyRange {
        KeyRange {
            start: self.key(""),
            end: Bytes::from(format!("{};", self.table)),
        }
    }
    fn write(&self, shard: usize, suffix: &str, value: Option<&'static [u8]>, ts: u64) {
        let store = self
            .engine
            .engine_for(&MultiRaftManager::data_group_id(self.ids[shard]));
        let txn = TxnId::new();
        match value {
            Some(v) => store
                .write_intent(txn, self.key(suffix), Bytes::from_static(v))
                .unwrap(),
            None => store.delete_intent(txn, self.key(suffix)).unwrap(),
        }
        store.commit(txn, ts).unwrap();
    }
}

async fn fixture() -> Fixture {
    let table = TableId::new();
    let meta = Arc::new(nodus_meta::MemMetaStore::new());
    let ids = vec![ShardId::new(), ShardId::new(), ShardId::new()];
    let boundaries = [
        vec![],
        format!("{table}:m").into_bytes(),
        format!("{table}:t").into_bytes(),
        vec![],
    ];
    let mut shards: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| ShardDescriptor {
            id: *id,
            name: format!("s{i}"),
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: DescriptorState::Public,
            table_id: table,
            start_key: boundaries[i].clone(),
            end_key: boundaries[i + 1].clone(),
        })
        .collect();
    shards.reverse(); // Persisted order is not assumed to be key order.
    meta.update_shard_map(ShardMap {
        table_id: table,
        shards,
    })
    .unwrap();
    let base: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
    let manager = Arc::new(MultiRaftManager::new(
        1,
        "127.0.0.1:0".into(),
        Arc::new(openraft::Config::default().validate().unwrap()),
        nodus_raftstore::server::RaftState::new(),
        base.clone(),
        None,
        Arc::new(nodus_txn::MemTxnManager::new()),
        None,
        nodus_raftstore::network::RaftTransport::default(),
    ));
    for id in &ids {
        manager
            .get_or_create_data(&MultiRaftManager::data_group_id(*id))
            .await
            .unwrap();
    }
    let engine = Arc::new(RaftKvEngine {
        local: base,
        router: RaftRouter::spawn(manager.clone()),
        shard_router: Arc::new(nodus_sharding::CatalogShardRouter::new(meta.clone())),
        manager,
        txn_groups: Mutex::new(HashMap::new()),
        metrics: nodus_monitoring::Metrics::default(),
    });
    let f = Fixture {
        engine,
        meta,
        table,
        ids,
    };
    for (shard, key) in [(0, "a"), (0, "b"), (1, "m"), (1, "n"), (2, "z")] {
        f.write(shard, key, Some(b"old"), 20);
    }
    f
}

fn keys(scan: Box<dyn Iterator<Item = Result<KvPair>> + Send>) -> Vec<Bytes> {
    scan.map(|p| p.unwrap().key).collect()
}

#[tokio::test]
async fn scans_clip_and_order_all_shards_at_one_timestamp() {
    let f = fixture().await;
    f.write(0, "z", Some(b"wrong namespace"), 20); // must be excluded by clipping
    f.write(0, "a", Some(b"new"), 35);
    f.write(0, "b", None, 30);
    assert_eq!(
        keys(f.engine.scan(f.range(), 25).unwrap()),
        ["a", "b", "m", "n", "z"].map(|s| f.key(s))
    );
    assert_eq!(
        keys(f.engine.scan(f.range(), 40).unwrap()),
        ["a", "m", "n", "z"].map(|s| f.key(s))
    );
    assert_eq!(
        keys(
            f.engine
                .scan(
                    KeyRange {
                        start: f.key("b"),
                        end: f.key("z")
                    },
                    25
                )
                .unwrap()
        ),
        ["b", "m", "n"].map(|s| f.key(s))
    );
    assert_eq!(
        keys(
            f.engine
                .scan(
                    KeyRange {
                        start: f.key("m"),
                        end: f.key("t")
                    },
                    25
                )
                .unwrap()
        ),
        ["m", "n"].map(|s| f.key(s))
    );
    assert!(
        f.engine
            .scan(
                KeyRange {
                    start: f.key("z"),
                    end: f.key("a")
                },
                40
            )
            .unwrap()
            .next()
            .is_none()
    );
    assert!(
        f.engine
            .scan(
                KeyRange {
                    start: f.key("a"),
                    end: f.key("a")
                },
                40
            )
            .unwrap()
            .next()
            .is_none()
    );
    let versions: Vec<_> = f
        .engine
        .scan_versions(f.range(), 25, 40)
        .unwrap()
        .map(|p| {
            let p = p.unwrap();
            (p.key, p.value, p.version)
        })
        .collect();
    assert_eq!(
        versions,
        vec![
            (f.key("a"), Some(Bytes::from_static(b"new")), 35),
            (f.key("b"), None, 30)
        ]
    );
    f.engine.manager.shutdown_all().await;
}

#[tokio::test]
async fn missing_assigned_group_never_falls_back_to_meta() {
    let f = fixture().await;
    f.engine
        .manager
        .remove_group(&MultiRaftManager::data_group_id(f.ids[2]))
        .await
        .unwrap();
    assert!(
        f.engine.scan(f.range(), 40).is_err(),
        "preflight includes later shards even for LIMIT"
    );
    assert!(f.engine.scan_versions(f.range(), 0, 40).is_err());
    assert!(f.engine.get(&f.key("z"), 40).is_err());
    let txn = TxnId::new();
    assert!(
        f.engine
            .write_intent(txn, f.key("z"), Bytes::from_static(b"bad"))
            .is_err()
    );
    assert!(f.engine.delete_intent(txn, f.key("z")).is_err());
    assert!(
        f.engine
            .replace_intent(txn, f.key("z"), IntentReplacement::Clear)
            .is_err()
    );
    assert!(f.engine.read_barrier(&f.key("z")).is_err());
    assert!(f.engine.local.get(&f.key("z"), u64::MAX).unwrap().is_none());
    assert!(f.engine.txn_groups.lock().unwrap().is_empty());
    f.engine.manager.shutdown_all().await;
}

#[tokio::test]
async fn routing_change_and_scan_failure_are_reported_and_fused() {
    let f = fixture().await;
    let mut scan = f.engine.scan(f.range(), 40).unwrap();
    assert!(scan.next().unwrap().is_ok());
    let mut map = f.meta.get_shard_map(f.table).unwrap();
    map.shards[0].version += 1;
    f.meta.update_shard_map(map).unwrap();
    assert!(
        scan.next()
            .unwrap()
            .err()
            .unwrap()
            .to_string()
            .starts_with("shard routing changed")
    );
    assert!(scan.next().is_none());
    let opens = Arc::new(AtomicUsize::new(0));
    let count = opens.clone();
    let mut scan = f
        .engine
        .routed_scan(f.range(), move |engine, range| {
            if count.fetch_add(1, Ordering::SeqCst) == 1 {
                anyhow::bail!("injected scan failure");
            }
            engine.scan(range, 40)
        })
        .unwrap();
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    assert_eq!(scan.next().unwrap().unwrap().key, f.key("a"));
    assert_eq!(
        opens.load(Ordering::SeqCst),
        1,
        "only the current shard is opened"
    );
    assert_eq!(scan.next().unwrap().unwrap().key, f.key("b"));
    assert!(
        scan.next()
            .unwrap()
            .err()
            .unwrap()
            .to_string()
            .contains("injected scan failure")
    );
    assert!(scan.next().is_none());
    f.engine.manager.shutdown_all().await;
}

#[tokio::test]
async fn invalid_metadata_and_unsupported_consistency_fail_explicitly() {
    let f = fixture().await;
    let error = f
        .engine
        .read_range_barrier(f.range())
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("unsupported linearizable cross-shard"));
    let mut map = f.meta.get_shard_map(f.table).unwrap();
    map.shards[0].start_key = f.key("u").to_vec(); // gap after the second shard
    f.meta.update_shard_map(map).unwrap();
    assert!(f.engine.scan(f.range(), 40).is_err());
    assert!(f.engine.get(&f.key("a"), 40).is_err());
    assert!(
        f.engine
            .write_intent(TxnId::new(), f.key("a"), Bytes::new())
            .is_err()
    );
    // A real storage decode failure is not the typed "no map" outcome.
    let meta = nodus_meta::PersistentMetaStore::new(f.engine.local.clone());
    let key = Bytes::from(format!("meta:shard_map:{}", f.table));
    let txn = TxnId::new();
    f.engine
        .local
        .write_intent(txn, key, Bytes::from_static(b"invalid json"))
        .unwrap();
    f.engine.local.commit(txn, 40).unwrap();
    let router = nodus_sharding::CatalogShardRouter::new(Arc::new(meta));
    assert!(router.snapshot(f.table).is_err());
    assert!(router.snapshot(TableId::new()).unwrap().is_none());
    f.engine.manager.shutdown_all().await;
}

#[tokio::test]
async fn malformed_coverage_is_never_treated_as_unsharded() {
    let f = fixture().await;
    let valid = f.meta.get_shard_map(f.table).unwrap();
    for case in 0..6 {
        let mut map = valid.clone();
        match case {
            0 => map.shards.clear(),
            1 => map.shards[1].id = map.shards[0].id,
            2 => map.shards[0].start_key = f.key("n").to_vec(), // overlap
            3 => map.shards[2].start_key = f.key("a").to_vec(), // missing lower bound
            4 => map.shards[0].end_key = f.key("z").to_vec(),   // missing upper bound
            5 => map.shards[1].table_id = TableId::new(),
            _ => unreachable!(),
        }
        f.meta.update_shard_map(map).unwrap();
        assert!(f.engine.scan(f.range(), 40).is_err(), "case {case}");
        assert!(f.engine.get(&f.key("a"), 40).is_err(), "case {case}");
    }
    f.meta.update_shard_map(valid).unwrap();
    // A binary primary-key suffix still belongs to the UUID's table.
    let mut binary_key = f.key("").to_vec();
    binary_key.push(0xff);
    assert_eq!(parse_table_id(&binary_key), Some(f.table));
    let unsharded_key = Bytes::from(format!("{}:a", TableId::new()));
    assert_eq!(f.engine.route(&unsharded_key).unwrap(), META_SHARD);
    assert_eq!(f.engine.route(b"i:index:val:pk").unwrap(), META_SHARD);
    f.engine.manager.shutdown_all().await;
}
