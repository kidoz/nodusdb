use clap::{Parser, Subcommand};
use nodus_monitoring::ClusterOverview;
use reqwest::Client;

#[derive(Parser)]
#[command(author, version, about = "CLI for NodusDB", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

const DEFAULT_ADDR: &str = "http://127.0.0.1:8088";

#[derive(Subcommand)]
enum Commands {
    /// Show client version
    Version,
    /// Check liveness of the server
    Health {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Cluster administration
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },
    /// Print raw Prometheus metrics from the server
    Metrics {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Inspect and manage active sessions
    Session {
        #[command(subcommand)]
        cmd: SessionCmd,
    },
    /// Query the audit trail
    Audit {
        #[command(subcommand)]
        cmd: AuditCmd,
    },
    /// Manage backups
    Backup {
        #[command(subcommand)]
        cmd: BackupCmd,
    },
    /// Control rolling upgrades
    Upgrade {
        #[command(subcommand)]
        cmd: UpgradeCmd,
    },
    /// Manage shards
    Shard {
        #[command(subcommand)]
        cmd: ShardCmd,
    },
    /// Show recent slow queries
    Queries {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Node lifecycle operations
    Node {
        #[command(subcommand)]
        cmd: NodeCmd,
    },
    /// Manage roles and users
    Role {
        #[command(subcommand)]
        cmd: RoleCmd,
    },
    /// Manage privilege grants
    Grant {
        #[command(subcommand)]
        cmd: GrantCmd,
    },
}

#[derive(Subcommand)]
enum RoleCmd {
    /// List roles and users
    List {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Create a role
    Create {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[derive(Subcommand)]
enum GrantCmd {
    /// List privilege grants
    List {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Grant a privilege on a table to a principal
    Add {
        #[arg(long)]
        principal: String,
        #[arg(long)]
        privilege: String,
        #[arg(long, default_value = "default")]
        database: String,
        #[arg(long, default_value = "public")]
        schema: String,
        #[arg(long)]
        table: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Revoke a privilege on a table from a principal
    Revoke {
        #[arg(long)]
        principal: String,
        #[arg(long)]
        privilege: String,
        #[arg(long, default_value = "default")]
        database: String,
        #[arg(long, default_value = "public")]
        schema: String,
        #[arg(long)]
        table: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Drain the node: stop accepting traffic and cancel active sessions
    Drain {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[derive(Subcommand)]
enum ShardCmd {
    /// Create an initial single shard for a table
    Init {
        #[arg(long)]
        table: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Show the shard map for a table
    Map {
        #[arg(long)]
        table: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Split a shard at a key byte (0-255)
    Split {
        #[arg(long)]
        table: String,
        #[arg(long)]
        shard: String,
        #[arg(long)]
        key: u8,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Merge two adjacent shards into one
    Merge {
        #[arg(long)]
        table: String,
        #[arg(long)]
        left: String,
        #[arg(long)]
        right: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Rebalance a table's shards across nodes (comma-separated)
    Rebalance {
        #[arg(long)]
        table: String,
        #[arg(long)]
        nodes: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[derive(Subcommand)]
enum UpgradeCmd {
    /// Show upgrade state
    Status {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Start an upgrade to a target version
    Start {
        #[arg(long)]
        target: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Report a node as upgraded
    NodeUpgraded {
        #[arg(long)]
        node: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Finalize the upgrade
    Finalize {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Roll back an in-progress upgrade
    Rollback {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[derive(Subcommand)]
enum BackupCmd {
    /// Create a full backup
    Create {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// List backup ids
    List {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Verify a backup's integrity
    Verify {
        #[arg(long)]
        id: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Restore a backup (returns object count)
    Restore {
        #[arg(long)]
        id: String,
        #[arg(long)]
        target_ts: Option<u64>,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[derive(Subcommand)]
enum AuditCmd {
    /// Query audit events with optional filters
    Query {
        #[arg(long)]
        actor: Option<String>,
        #[arg(long)]
        action: Option<String>,
        #[arg(long)]
        result: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[derive(Subcommand)]
enum ClusterCmd {
    /// Show a cluster overview (nodes, shards, QPS, alerts)
    Info {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[derive(Subcommand)]
enum SessionCmd {
    /// List active client sessions
    List {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Terminate a session by id
    Kill {
        #[arg(long)]
        id: String,
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = Client::new();

    match &cli.command {
        Commands::Version => {
            println!("nodusctl {}", env!("CARGO_PKG_VERSION"));
        }
        Commands::Health { addr } => {
            let resp = client.get(format!("{addr}/healthz")).send().await?;
            if resp.status().is_success() {
                println!("Server at {addr} is healthy.");
            } else {
                anyhow::bail!("Server responded with status: {}", resp.status());
            }
        }
        Commands::Cluster {
            cmd: ClusterCmd::Info { addr },
        } => {
            let resp = client
                .get(format!("{addr}/api/v1/cluster/overview"))
                .send()
                .await?
                .error_for_status()?;
            let o: ClusterOverview = resp.json().await?;
            println!("Cluster status : {}", o.cluster_status);
            println!("Nodes          : {}/{} live", o.nodes_live, o.nodes_total);
            println!(
                "Shards         : {} total, {} unavailable",
                o.shards_total, o.shards_unavailable
            );
            println!("QPS            : {:.1}", o.qps);
            println!("Active alerts  : {}", o.active_alerts);
        }
        Commands::Metrics { addr } => {
            let body = client
                .get(format!("{addr}/metrics"))
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;
            print!("{body}");
        }
        Commands::Session {
            cmd: SessionCmd::List { addr },
        } => {
            let sessions: serde_json::Value = client
                .get(format!("{addr}/api/v1/sessions"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let empty = vec![];
            let arr = sessions.as_array().unwrap_or(&empty);
            if arr.is_empty() {
                println!("No active sessions.");
            }
            for s in arr {
                println!(
                    "{}  user={}  principal={}  query={}",
                    s["session_id"].as_str().unwrap_or("?"),
                    s["user_name"].as_str().unwrap_or("(anonymous)"),
                    s["principal_id"].as_str().unwrap_or("?"),
                    s["current_query"].as_str().unwrap_or("-")
                );
            }
        }
        Commands::Session {
            cmd: SessionCmd::Kill { id, addr },
        } => {
            let killed: bool = client
                .post(format!("{addr}/api/v1/sessions/{id}/kill"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            if killed {
                println!("Session {id} terminated.");
            } else {
                anyhow::bail!("session {id} not found");
            }
        }
        Commands::Audit {
            cmd:
                AuditCmd::Query {
                    actor,
                    action,
                    result,
                    limit,
                    addr,
                },
        } => {
            let mut req = client.get(format!("{addr}/api/v1/audit"));
            let mut q: Vec<(&str, String)> = Vec::new();
            if let Some(a) = actor {
                q.push(("actor", a.clone()));
            }
            if let Some(a) = action {
                q.push(("action", a.clone()));
            }
            if let Some(r) = result {
                q.push(("result", r.clone()));
            }
            if let Some(l) = limit {
                q.push(("limit", l.to_string()));
            }
            if !q.is_empty() {
                req = req.query(&q);
            }
            let events: serde_json::Value = req.send().await?.error_for_status()?.json().await?;
            let empty = vec![];
            let arr = events.as_array().unwrap_or(&empty);
            if arr.is_empty() {
                println!("No audit events.");
            }
            for e in arr {
                println!(
                    "{}  actor={}  action={}  result={}  reason={}",
                    e["time"].as_str().unwrap_or("?"),
                    e["actor"].as_str().unwrap_or("?"),
                    e["action"].as_str().unwrap_or("?"),
                    e["result"].as_str().unwrap_or("?"),
                    e["reason"].as_str().unwrap_or("-")
                );
            }
        }
        Commands::Backup {
            cmd: BackupCmd::Create { addr },
        } => {
            let v: serde_json::Value = client
                .post(format!("{addr}/api/v1/backups"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Commands::Backup {
            cmd: BackupCmd::List { addr },
        } => {
            let ids: serde_json::Value = client
                .get(format!("{addr}/api/v1/backups"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let empty = vec![];
            let arr = ids.as_array().unwrap_or(&empty);
            if arr.is_empty() {
                println!("No backups.");
            }
            for id in arr {
                println!("{}", id.as_str().unwrap_or("?"));
            }
        }
        Commands::Backup {
            cmd: BackupCmd::Verify { id, addr },
        } => {
            let v: serde_json::Value = client
                .post(format!("{addr}/api/v1/backups/{id}/verify"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Commands::Backup {
            cmd:
                BackupCmd::Restore {
                    id,
                    target_ts,
                    addr,
                },
        } => {
            let url = if let Some(ts) = target_ts {
                format!("{addr}/api/v1/backups/{id}/restore?target_ts={ts}")
            } else {
                format!("{addr}/api/v1/backups/{id}/restore")
            };
            let v: serde_json::Value = client
                .post(&url)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Commands::Upgrade { cmd } => {
            let (method_post, path, query): (bool, String, Vec<(&str, String)>) = match &cmd {
                UpgradeCmd::Status { addr } => (false, format!("{addr}/api/v1/upgrade"), vec![]),
                UpgradeCmd::Start { target, addr } => (
                    true,
                    format!("{addr}/api/v1/upgrade/start"),
                    vec![("target", target.clone())],
                ),
                UpgradeCmd::NodeUpgraded { node, addr } => (
                    true,
                    format!("{addr}/api/v1/upgrade/node-upgraded"),
                    vec![("node", node.clone())],
                ),
                UpgradeCmd::Finalize { addr } => {
                    (true, format!("{addr}/api/v1/upgrade/finalize"), vec![])
                }
                UpgradeCmd::Rollback { addr } => {
                    (true, format!("{addr}/api/v1/upgrade/rollback"), vec![])
                }
            };
            let mut req = if method_post {
                client.post(&path)
            } else {
                client.get(&path)
            };
            if !query.is_empty() {
                req = req.query(&query);
            }
            let v: serde_json::Value = req.send().await?.error_for_status()?.json().await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Commands::Shard { cmd } => {
            let (method_post, path, query): (bool, String, Vec<(&str, String)>) = match &cmd {
                ShardCmd::Init { table, addr } => {
                    (true, format!("{addr}/api/v1/shards/{table}/init"), vec![])
                }
                ShardCmd::Map { table, addr } => {
                    (false, format!("{addr}/api/v1/shards/{table}"), vec![])
                }
                ShardCmd::Split {
                    table,
                    shard,
                    key,
                    addr,
                } => (
                    true,
                    format!("{addr}/api/v1/shards/{table}/split"),
                    vec![("shard", shard.clone()), ("key", key.to_string())],
                ),
                ShardCmd::Merge {
                    table,
                    left,
                    right,
                    addr,
                } => (
                    true,
                    format!("{addr}/api/v1/shards/{table}/merge"),
                    vec![("left", left.clone()), ("right", right.clone())],
                ),
                ShardCmd::Rebalance { table, nodes, addr } => (
                    true,
                    format!("{addr}/api/v1/shards/{table}/rebalance"),
                    vec![("nodes", nodes.clone())],
                ),
            };
            let mut req = if method_post {
                client.post(&path)
            } else {
                client.get(&path)
            };
            if !query.is_empty() {
                req = req.query(&query);
            }
            let v: serde_json::Value = req.send().await?.error_for_status()?.json().await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Commands::Queries { addr } => {
            let rows: serde_json::Value = client
                .get(format!("{addr}/api/v1/queries"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let empty = vec![];
            let arr = rows.as_array().unwrap_or(&empty);
            if arr.is_empty() {
                println!("No slow queries recorded.");
            }
            for q in arr {
                println!(
                    "{} ms  session={}  {}",
                    q["duration_ms"].as_u64().unwrap_or(0),
                    q["session_id"].as_str().unwrap_or("?"),
                    q["sql"].as_str().unwrap_or("?")
                );
            }
        }
        Commands::Node {
            cmd: NodeCmd::Drain { addr },
        } => {
            let v: serde_json::Value = client
                .post(format!("{addr}/api/v1/node/drain"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Commands::Role {
            cmd: RoleCmd::List { addr },
        } => {
            let roles: serde_json::Value = client
                .get(format!("{addr}/api/v1/roles"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let empty = vec![];
            let arr = roles.as_array().unwrap_or(&empty);
            if arr.is_empty() {
                println!("No roles.");
            }
            for r in arr {
                println!(
                    "{}  type={}  id={}",
                    r["name"].as_str().unwrap_or("?"),
                    r["type"].as_str().unwrap_or("?"),
                    r["id"].as_str().unwrap_or("?"),
                );
            }
        }
        Commands::Role {
            cmd: RoleCmd::Create { name, addr },
        } => {
            let v: serde_json::Value = client
                .post(format!("{addr}/api/v1/roles"))
                .json(&serde_json::json!({ "name": name }))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Commands::Grant {
            cmd: GrantCmd::List { addr },
        } => {
            let grants: serde_json::Value = client
                .get(format!("{addr}/api/v1/grants"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let empty = vec![];
            let arr = grants.as_array().unwrap_or(&empty);
            if arr.is_empty() {
                println!("No grants.");
            }
            for g in arr {
                println!(
                    "{} ON {} TO {}",
                    g["privilege"].as_str().unwrap_or("?"),
                    g["resource"].as_str().unwrap_or("?"),
                    g["principal"].as_str().unwrap_or("?"),
                );
            }
        }
        Commands::Grant {
            cmd:
                GrantCmd::Add {
                    principal,
                    privilege,
                    database,
                    schema,
                    table,
                    addr,
                },
        } => {
            let v: serde_json::Value = client
                .post(format!("{addr}/api/v1/grants"))
                .json(&serde_json::json!({
                    "principal": principal,
                    "privilege": privilege,
                    "database": database,
                    "schema": schema,
                    "table": table,
                }))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Commands::Grant {
            cmd:
                GrantCmd::Revoke {
                    principal,
                    privilege,
                    database,
                    schema,
                    table,
                    addr,
                },
        } => {
            let v: serde_json::Value = client
                .delete(format!("{addr}/api/v1/grants"))
                .json(&serde_json::json!({
                    "principal": principal,
                    "privilege": privilege,
                    "database": database,
                    "schema": schema,
                    "table": table,
                }))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
    }

    Ok(())
}
