use super::*;

#[tokio::test]
async fn transaction_cluster_blocks_old_reader_before_consensus_or_resumed_snapshot_bytes() {
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    let dir = tempfile::tempdir().unwrap();
    let config = super::admission::tls_config(dir.path());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    let app = axum::Router::new()
        .route(
            "/raft/capabilities/v1",
            axum::routing::post(
                |axum::Json(request): axum::Json<upgrade::ProbeV1>| async move {
                    let mut report = upgrade::CapabilityV1::local(2, request.challenge);
                    report.txn_authority_version = None;
                    axum::Json(report)
                },
            ),
        )
        .fallback(move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                axum::http::StatusCode::OK
            }
        });
    let (tls, _) = crate::load_raft_tls_config(&config.tls).unwrap().unwrap();
    let server = tokio::spawn(async move {
        axum_server::from_tcp_rustls(
            listener.into_std().unwrap(),
            axum_server::tls_rustls::RustlsConfig::from_config(tls),
        )
        .unwrap()
        .serve(app.into_make_service())
        .await
        .unwrap();
    });
    let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
    let challenge = uuid::Uuid::new_v4();
    let mut report = upgrade::CapabilityV1::local(2, challenge);
    report.ready_for_finalize = true;
    let authority = upgrade::RecordV1 {
        version: 1,
        revision: 2,
        phase: Phase::Finalized,
        cluster_version: 5,
        session: challenge,
        membership: openraft::StoredMembership::new(
            None,
            openraft::Membership::new(
                vec![[2].into()],
                BTreeMap::from([(2, openraft::BasicNode::new(address.clone()))]),
            ),
        ),
        reports: BTreeMap::from([(2, report)]),
    };
    let txn = TxnId::new();
    kv.write_intent(
        txn,
        Bytes::from_static(upgrade::KEY),
        Bytes::from(serde_json::to_vec(&authority).unwrap()),
    )
    .unwrap();
    kv.commit(txn, 2).unwrap();
    let transport = crate::build_raft_transport(&config)
        .unwrap()
        .with_snapshot_checks()
        .with_admission_source(kv.clone());
    // Receiver-side checks must also reject an old process reusing an approved
    // ID. This guard runs before a missing group could return 404.
    let state = RaftState::new();
    state.set_capability_storage(kv);
    state.set_peer_transport(transport.clone());
    let receiver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let receiver_addr = receiver.local_addr().unwrap();
    let app = raft_routes(nodus_raftstore::server::RaftRouteAuth::TransportAuthenticated)
        .with_state(state);
    let (tls, _) = crate::load_raft_tls_config(&config.tls).unwrap().unwrap();
    let receiver_task = tokio::spawn(async move {
        axum_server::from_tcp_rustls(
            receiver.into_std().unwrap(),
            axum_server::tls_rustls::RustlsConfig::from_config(tls),
        )
        .unwrap()
        .serve(app.into_make_service())
        .await
        .unwrap();
    });
    let mut pem = std::fs::read(config.tls.cert_path.as_ref().unwrap()).unwrap();
    pem.extend(std::fs::read(config.tls.key_path.as_ref().unwrap()).unwrap());
    let peer = reqwest::Client::builder()
        .use_rustls_tls()
        .https_only(true)
        .tls_certs_only([reqwest::Certificate::from_pem(
            &std::fs::read(config.tls.ca_path.as_ref().unwrap()).unwrap(),
        )
        .unwrap()])
        .identity(reqwest::Identity::from_pem(&pem).unwrap())
        .build()
        .unwrap();
    let vote = openraft::Vote::new(3, 2);
    for (path, body) in [
        (
            "vote",
            serde_json::to_value(openraft::raft::VoteRequest {
                vote,
                last_log_id: None,
            })
            .unwrap(),
        ),
        (
            "append",
            serde_json::to_value(openraft::raft::AppendEntriesRequest::<
                nodus_raftstore::NodusTypeConfig,
            > {
                vote,
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            })
            .unwrap(),
        ),
        (
            "snapshot",
            serde_json::to_value(openraft::raft::InstallSnapshotRequest::<
                nodus_raftstore::NodusTypeConfig,
            > {
                vote,
                meta: Default::default(),
                offset: 0,
                data: b"NSNP\0\x02".to_vec(),
                done: false,
            })
            .unwrap(),
        ),
    ] {
        let response = peer
            .post(format!("https://{receiver_addr}/raft/shard-meta/{path}"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("authenticated transaction protocol")
        );
    }
    receiver_task.abort();
    let _ = receiver_task.await;
    let mut factory =
        nodus_raftstore::network::NodusNetworkFactory::new(META_SHARD.into(), transport);
    let mut client = factory
        .new_client(2, &openraft::BasicNode::new(address))
        .await;
    for (offset, data) in [
        (0, b"NSNP\0\x01".to_vec()),
        (0, b"NSNP\0\x02".to_vec()),
        (6, b"resume".to_vec()),
    ] {
        assert!(
            client
                .install_snapshot(
                    openraft::raft::InstallSnapshotRequest {
                        vote: openraft::Vote::new(1, 1),
                        meta: Default::default(),
                        offset,
                        data,
                        done: false
                    },
                    RPCOption::new(Duration::from_secs(3))
                )
                .await
                .is_err()
        );
    }
    assert!(
        client
            .append_entries(
                openraft::raft::AppendEntriesRequest {
                    vote: openraft::Vote::new(1, 1),
                    prev_log_id: None,
                    entries: vec![],
                    leader_commit: None
                },
                RPCOption::new(Duration::from_secs(3))
            )
            .await
            .is_err()
    );
    assert!(
        client
            .vote(
                openraft::raft::VoteRequest {
                    vote: openraft::Vote::new(2, 1),
                    last_log_id: None,
                },
                RPCOption::new(Duration::from_secs(3))
            )
            .await
            .is_err()
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    server.abort();
    let _ = server.await;
}
