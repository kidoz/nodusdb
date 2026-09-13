//! Process-only fixture: neither historical server is patched or linked here.
use reqwest::Client;
use serde_json::{Value, json};
use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};
use tokio::time::{sleep, timeout};

pub const TOKEN: &str = "mixed-binary-fixture";
pub struct Node {
    pub id: usize,
    pub http: String,
    pub raft: String,
    pub pg: u16,
    pub dir: PathBuf,
    pub child: Option<Child>,
    generation: usize,
}
impl Node {
    pub fn new(id: usize, root: &Path) -> Self {
        // Hold every reservation until all ports for this node are selected.
        let ports: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let port = |i: usize| ports[i].local_addr().unwrap().port();
        let node = Self {
            id,
            http: format!("127.0.0.1:{}", port(0)),
            raft: format!("127.0.0.1:{}", port(1)),
            pg: port(2),
            dir: root.join(format!("node-{id}")),
            child: None,
            generation: 0,
        };
        fs::create_dir_all(&node.dir).unwrap();
        node
    }
    pub fn start(&mut self, binary: &Path, root: &Path, peers: &[String]) {
        assert!(self.child.is_none());
        self.generation += 1;
        let config = format!(
            r#"
[server]
http_addr = {:?}
pgwire_addr = "127.0.0.1:{}"
[cluster]
node_id = {}
raft_advertise_addr = {:?}
raft_listen_addr = {:?}
join_peers = {}
raft_heartbeat_ms = 300
raft_election_timeout_min_ms = 1200
raft_election_timeout_max_ms = 2000
[cluster.tls]
enabled = true
cert_path = {:?}
key_path = {:?}
ca_path = {:?}
[storage]
data_dir = {:?}
allow_ephemeral = false
[admin]
token = {:?}
password = "nodus"
[observability]
log_level = "warn"
"#,
            self.http,
            self.pg,
            self.id,
            self.raft,
            self.raft,
            serde_json::to_string(peers).unwrap(),
            root.join("node.pem"),
            root.join("node.key"),
            root.join("ca.pem"),
            self.dir.join("data"),
            TOKEN
        );
        let path = self.dir.join("nodus.toml");
        fs::write(&path, config).unwrap();
        let log =
            fs::File::create(self.dir.join(format!("process-{}.log", self.generation))).unwrap();
        let mut command = Command::new(binary);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("NODUS_") {
                command.env_remove(key);
            }
        }
        self.child = Some(
            command
                .env("NODUS_CONFIG", path)
                .env("TOKIO_WORKER_THREADS", "8")
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
    }
    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }
    pub fn snapshot(&self) -> PathBuf {
        self.dir.join("data/snapshots/shard-meta/current.snap")
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub struct Matrix {
    pub root: PathBuf,
    pub old: PathBuf,
    pub new: PathBuf,
    pub nodes: Vec<Node>,
    pub http: Client,
    pub peer: Client,
    checks: Vec<Value>,
    blockers: Vec<Value>,
}
impl Matrix {
    pub fn new() -> Self {
        let root = PathBuf::from(
            std::env::var_os("NODUS_MIXED_OUTPUT").expect("use tools/testing/mixed_binary.py"),
        );
        let run = root.join(format!("run-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&run).unwrap();
        let openssl = |args: &[&str]| {
            let output = Command::new("openssl")
                .args(args)
                .current_dir(&run)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "openssl: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        openssl(&[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "2",
            "-subj",
            "/CN=Nodus mixed binary test CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]);
        openssl(&[
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "node.key",
            "-out",
            "node.csr",
            "-subj",
            "/CN=localhost",
        ]);
        fs::write(run.join("extensions"),"subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n").unwrap();
        openssl(&[
            "x509",
            "-req",
            "-in",
            "node.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "node.pem",
            "-days",
            "2",
            "-extfile",
            "extensions",
        ]);
        let mut pem = fs::read(run.join("node.pem")).unwrap();
        pem.extend(fs::read(run.join("node.key")).unwrap());
        let peer = Client::builder()
            .tls_backend_rustls()
            .tls_certs_only([reqwest::Certificate::from_pem(
                &fs::read(run.join("ca.pem")).unwrap(),
            )
            .unwrap()])
            .identity(reqwest::Identity::from_pem(&pem).unwrap())
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();
        let nodes = (1..=4).map(|id| Node::new(id, &run)).collect();
        eprintln!("process logs: {}", run.display());
        Self {
            root: run,
            old: PathBuf::from(std::env::var_os("NODUS_MIXED_OLD").unwrap()),
            new: PathBuf::from(std::env::var_os("NODUS_MIXED_NEW").unwrap()),
            nodes,
            http: Client::builder()
                .timeout(Duration::from_secs(35))
                .build()
                .unwrap(),
            peer,
            checks: vec![],
            blockers: vec![],
        }
    }
    pub fn start(&mut self, index: usize, new: bool, join: bool) {
        let peers = if join {
            self.nodes
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != index && *i < 3)
                .map(|(_, n)| n.http.clone())
                .collect()
        } else {
            vec![]
        };
        self.nodes[index].start(if new { &self.new } else { &self.old }, &self.root, &peers);
    }
    pub async fn alive(&self, index: usize) {
        timeout(Duration::from_secs(30), async {
            loop {
                if self
                    .http
                    .get(format!("http://{}/healthz", self.nodes[index].http))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("node listener failed to start");
    }
    pub async fn api(&self, index: usize, path: &str, body: Option<Value>) -> (u16, Value) {
        let url = format!("http://{}/api/v1/{path}", self.nodes[index].http);
        let request = match body {
            Some(body) => self.http.post(url).json(&body),
            None => self.http.get(url),
        };
        let response = request.bearer_auth(TOKEN).send().await.unwrap();
        let status = response.status().as_u16();
        let text = response.text().await.unwrap();
        (
            status,
            serde_json::from_str(&text).unwrap_or_else(|_| json!({"text":text})),
        )
    }
    pub async fn state(&self, index: usize) -> Value {
        self.api(index, "upgrade", None).await.1
    }
    pub async fn leader(&self) -> usize {
        timeout(Duration::from_secs(30), async {
            loop {
                for (i, n) in self.nodes.iter().enumerate() {
                    if n.child.is_some() && self.state(i).await.get("phase").is_some() {
                        return i;
                    }
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("no meta leader")
    }
    pub async fn elect(&self, index: usize) {
        self.elect_within(index, Duration::from_secs(30)).await;
    }
    async fn elect_within(&self, index: usize, deadline: Duration) {
        timeout(deadline, async {
            while self.state(index).await.get("phase").is_none() {
                assert_eq!(
                    self.api(index, "node/take-leadership/shard-meta", Some(json!({})))
                        .await
                        .0,
                    200
                );
                sleep(Duration::from_secs(1)).await;
            }
        })
        .await
        .expect("requested leader not elected");
    }
    pub async fn capability(&self, index: usize) -> Value {
        let challenge = uuid::Uuid::new_v4();
        let response: Value = self
            .peer
            .post(format!(
                "https://{}/raft/capabilities/v1",
                self.nodes[index].raft
            ))
            .json(&json!({"challenge":challenge,"preflight":false}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["node_id"], json!(index + 1));
        assert_eq!(response["challenge"], json!(challenge));
        response
    }
    pub async fn sql(&self, index: usize, sql: &str) -> Vec<String> {
        self.try_sql(index, sql)
            .await
            .unwrap_or_else(|e| panic!("node {} SQL {sql}: {e:?}", index + 1))
    }
    async fn try_sql(&self, index: usize, sql: &str) -> Result<Vec<String>, tokio_postgres::Error> {
        let (client, connection) = timeout(
            Duration::from_secs(15),
            tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user=nodus password=nodus dbname=nodus",
                    self.nodes[index].pg
                ),
                tokio_postgres::NoTls,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let task = tokio::spawn(connection);
        let messages = timeout(Duration::from_secs(30), client.simple_query(sql))
            .await
            .unwrap();
        drop(client);
        task.abort();
        let messages = messages?;
        let rows = messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                    (0..row.len())
                        .map(|c| row.get(c).unwrap_or("NULL"))
                        .collect::<Vec<_>>()
                        .join("|"),
                ),
                _ => None,
            })
            .collect();
        Ok(rows)
    }
    /// Diagnose the pinned pair's stale bootstrap credential after catalog
    /// replacement. A workaround never changes the overall gate to passed.
    pub async fn snapshot_login(&mut self, index: usize, new: bool) {
        let query = "SELECT id, value FROM mixed_probe ORDER BY id";
        if let Err(error) = self.try_sql(index, query).await {
            assert_eq!(
                error.as_db_error().map(|e| e.message()),
                Some("permission denied"),
                "unexpected snapshot SQL failure: {error:?}"
            );
            self.blockers.push(json!({
                "name":"snapshot_replaces_bootstrap_principal",
                "node_id":index+1,"reader":if new {"new"} else {"old"},
                "error":"permission denied","workaround":"restart recipient after snapshot install"
            }));
            self.save(false, false);
            assert_eq!(
                std::env::var("NODUS_MIXED_DIAGNOSE").as_deref(),
                Ok("1"),
                "snapshot login failed; use --diagnose to record the blocker and continue with an explicit restart"
            );
            eprintln!(
                "BLOCKER node {}: snapshot login denied; diagnosing with recipient restart",
                index + 1
            );
            self.nodes[index].kill();
            self.start(index, new, true);
            self.alive(index).await;
            self.ready(index).await;
            self.sql(index, query).await;
        }
    }
    pub async fn ready(&self, index: usize) {
        timeout(Duration::from_secs(45), async {
            loop {
                if self
                    .http
                    .get(format!("http://{}/readyz", self.nodes[index].http))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("node did not become ready");
    }
    pub async fn voters(&self, index: usize, total: u64) {
        timeout(Duration::from_secs(30), async {
            loop {
                let v = self.api(index, "cluster/overview", None).await.1;
                if v["nodes_total"] == total {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("membership did not converge");
    }
    pub async fn phase(&self, index: usize, operation: &str, phase: &str) {
        let (status, value) = self
            .api(index, &format!("upgrade/{operation}"), Some(json!({})))
            .await;
        assert_eq!(status, 200);
        assert_eq!(value["phase"], phase, "upgrade response: {value}");
    }
    pub async fn reject_join(&self, leader: usize, candidate: usize) {
        let before = self.state(leader).await;
        let (code, value) = self
            .api(
                leader,
                "cluster/join",
                Some(
                    json!({"node_id":candidate+1,"raft_advertise_addr":self.nodes[candidate].raft}),
                ),
            )
            .await;
        assert_eq!(code, 409, "{value}");
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .contains("admission protocol"),
            "{value}"
        );
        assert_eq!(
            self.state(leader).await,
            before,
            "rejected join mutated authority"
        );
        self.voters(leader, 3).await;
    }
    pub async fn snapshot(&self, leader: usize, version: u8) {
        let path = self.nodes[leader].snapshot();
        let old = fs::read(&path).ok();
        let published = || {
            fs::read(&path).is_ok_and(|bytes| {
                bytes.len() > 6
                    && bytes[..6] == [b'N', b'S', b'N', b'P', 0, version]
                    && old.as_ref() != Some(&bytes)
            })
        };
        // 5,000 is the unmodified production threshold. Only filler aborts may
        // have uncertain responses while synchronous log purging pauses Raft.
        for batch in 0..1320 {
            if published() {
                break;
            }
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..4 {
                let peer = self.peer.clone();
                let address = self.nodes[leader].raft.clone();
                tasks.spawn(async move {
                    peer.post(format!("https://{address}/raft/shard-meta/write"))
                        .json(&json!({"AbortTxn":{"txn_id":uuid::Uuid::new_v4().to_string(),"shard_id":null}}))
                        .send().await?.error_for_status()?.json::<Value>().await
                });
            }
            while let Some(result) = tasks.join_next().await {
                match result.unwrap() {
                    Ok(response) => assert_eq!(response["success"], true, "{response}"),
                    Err(error) => assert!(
                        published(),
                        "filler failed before snapshot publication: {error}"
                    ),
                }
            }
            if batch % 250 == 0 {
                eprintln!("snapshot workload: {} commands", (batch + 1) * 4);
            }
        }
        assert!(published(), "production snapshot was not built");
        eprintln!("NSNP v{version} published; waiting for synchronous log purge");
        timeout(Duration::from_secs(240), async {
            loop {
                let mut ready = true;
                for node in self.nodes.iter().filter(|n| n.child.is_some()) {
                    let response = self
                        .peer
                        .post(format!("https://{}/raft/shard-meta/read_index", node.raft))
                        .timeout(Duration::from_secs(2))
                        .send()
                        .await;
                    ready &= response.is_ok_and(|r| matches!(r.status().as_u16(), 200 | 503));
                }
                if ready {
                    break;
                }
                sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("Raft did not recover after snapshot log purge");
        // Followers can answer read_index with 503 while their Raft core is
        // still purging. Require an actual leader ReadIndex after recovery;
        // retry elections that raced a blocked peer or another candidate.
        self.elect_within(leader, Duration::from_secs(240)).await;
    }
    pub async fn received_snapshot(&self, index: usize, version: u8) {
        timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(bytes) = fs::read(self.nodes[index].snapshot())
                    && bytes.len() > 6
                    && bytes[..6] == [b'N', b'S', b'N', b'P', 0, version]
                {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("follower did not install expected snapshot version");
    }
    pub fn record(&mut self, name: &str, evidence: Value) {
        eprintln!("PASS {name}: {evidence}");
        self.checks.push(json!({"name":name,"evidence":evidence}));
        self.save(false, false);
    }
    fn save(&self, passed: bool, matrix_completed: bool) {
        fs::write(self.root.parent().unwrap().join("results.json"),serde_json::to_vec_pretty(&json!({"schema_version":1,"passed":passed,"matrix_completed":matrix_completed,"process_logs":self.root,"checks":self.checks,"blockers":self.blockers})).unwrap()).unwrap();
    }
    pub fn finish(&self) {
        self.save(self.blockers.is_empty(), true);
        assert!(
            self.blockers.is_empty(),
            "matrix completed with compatibility blockers; see results.json"
        );
    }
}
