use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_meta_authority_keeps_tracked_intents_protected_until_restart() {
    let dir = tempfile::tempdir().unwrap();
    let c = Cluster::open(dir.path()).await;
    let txn = TxnId::new();
    c.write_pair(txn).await;
    // Stop the sole meta voter while both independent data-group leaders stay
    // alive. The learner cannot choose a decision without the meta quorum.
    let meta = c.engines[0].manager.get(META_SHARD).await.unwrap();
    meta.shutdown().await.unwrap();
    let e = c.engines[1].clone();
    let recovered = tokio::task::spawn_blocking(move || e.recover_tracked_transaction(txn));
    assert!(
        tokio::time::timeout(Duration::from_secs(15), recovered)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    for i in 0..2 {
        let store = c.engines[i].engine_for(&c.groups[i]);
        assert_eq!(store.pending_intent_keys(txn).len(), 1);
        assert_eq!(
            participant::read(store.as_ref(), txn.0)
                .unwrap()
                .unwrap()
                .phase,
            participant::PhaseV1::Active
        );
    }
    c.assert_rows(false);
    drop(meta);
    c.shutdown().await;
    let c = Cluster::open(dir.path()).await;
    let e = c.engines[1].clone();
    tokio::task::spawn_blocking(move || e.recover_tracked_transaction(txn))
        .await
        .unwrap()
        .unwrap();
    c.assert_rows(false);
    for i in 0..2 {
        assert!(
            c.engines[i]
                .engine_for(&c.groups[i])
                .pending_intent_keys(txn)
                .is_empty()
        );
    }
    c.shutdown().await;
}
