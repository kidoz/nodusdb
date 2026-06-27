//! NodusDB node configuration.
//!
//! Configuration is layered: built-in defaults, then an optional TOML file,
//! then environment variables prefixed with `NODUS_` (double underscore selects
//! a nested key, e.g. `NODUS_SERVER__PGWIRE_ADDR`). Later layers win.

#![allow(clippy::result_large_err, clippy::derivable_impls)]

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to load configuration: {0}")]
    Load(String),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct NodusConfig {
    pub cluster: ClusterConfig,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub tls: TlsConfig,
    pub backup: BackupConfig,
    pub observability: ObservabilityConfig,
    pub admin: AdminConfig,
    pub audit: AuditConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ClusterConfig {
    /// Unique integer identifier for the node.
    pub node_id: u64,
    /// The address this node advertises to peers for Raft communication. When a
    /// dedicated `raft_listen_addr` is configured, this should point at it (with
    /// a hostname/IP that matches the peer certificate when `tls` is enabled).
    pub raft_advertise_addr: String,
    /// A list of addresses of existing nodes to contact for joining.
    pub join_peers: Vec<String>,
    /// Optional dedicated listen address for inter-node Raft RPCs. When set, Raft
    /// traffic is served here instead of on the public HTTP listener, isolating
    /// peer authentication (mTLS) from client/admin access. Required when
    /// `tls.enabled`.
    pub raft_listen_addr: Option<String>,
    /// Mutual-TLS settings for the inter-node Raft channel.
    pub tls: ClusterTlsConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            raft_advertise_addr: "127.0.0.1:8088".into(),
            join_peers: Vec::new(),
            raft_listen_addr: None,
            tls: ClusterTlsConfig::default(),
        }
    }
}

/// Mutual-TLS configuration for the dedicated Raft peer listener. When enabled,
/// the listener serves Raft RPCs over TLS and requires every peer to present a
/// client certificate signed by `ca_path`; the outbound Raft client presents
/// `cert_path`/`key_path` and verifies peers against the same `ca_path`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct ClusterTlsConfig {
    pub enabled: bool,
    /// This node's certificate, presented to peers (as a server) and to the
    /// peer it dials (as a client).
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// CA bundle that signs every node's certificate; used to verify both
    /// inbound peer certificates and the certificate of a dialed peer.
    pub ca_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: Option<String>,
    /// Optional 256-bit AES-GCM key (32 bytes as a hex string) for Transparent Data Encryption (TDE).
    pub encryption_key: Option<String>,
    /// Whether to permit running with **in-memory** storage when `data_dir` is
    /// unset. In-memory storage loses ALL data (including the durable Raft log)
    /// on restart, so production deployments should set this to `false` to refuse
    /// to start without a `data_dir`. Defaults to `true` for dev/test ergonomics.
    pub allow_ephemeral: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            encryption_key: None,
            allow_ephemeral: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct AdminConfig {
    /// Bearer token required on `/api/v1/*` admin endpoints. When unset (default),
    /// the admin API is unauthenticated — only safe bound to localhost.
    pub token: Option<String>,
    /// Password for the default 'nodus' superuser. If unset, a random password is generated on startup.
    pub password: Option<String>,
    /// Explicitly permit an unauthenticated admin API on a non-loopback bind
    /// (e.g. when fronted by an authenticating proxy). Off by default: the
    /// server refuses to start with a non-loopback `http_addr` and no `token`.
    pub allow_insecure: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AuditConfig {
    /// Path to a durable JSONL audit log. When set, audit events are persisted
    /// there; when unset (default), an in-memory sink is used.
    pub file_path: Option<String>,
    /// Rotate the durable log once it would exceed this many bytes. `None` (the
    /// default) keeps the historical unbounded append-only behavior.
    pub max_size_bytes: Option<u64>,
    /// Number of rotated segments (`<file>.1` ..= `<file>.N`) to retain when
    /// rotation is enabled.
    pub max_files: usize,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            file_path: None,
            max_size_bytes: None,
            // Sensible retention once a size cap is configured; ignored while
            // `max_size_bytes` is `None`.
            max_files: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ServerConfig {
    /// PostgreSQL wire-protocol listen address.
    pub pgwire_addr: String,
    /// HTTP admin/metrics listen address. Binds localhost by default.
    pub http_addr: String,
    pub max_connections: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// Path to a CA certificate bundle used to verify client certificates (mTLS).
    pub client_ca_path: Option<String>,
    /// If true, clients must present a valid certificate signed by `client_ca_path`.
    pub require_client_auth: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BackupConfig {
    /// Backup repository URI (e.g. `file:///var/lib/nodus/backups`). Required in
    /// production; empty disables backups.
    pub repository_uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ObservabilityConfig {
    pub metrics_enabled: bool,
    /// Log level: trace|debug|info|warn|error.
    pub log_level: String,
    /// Redact potentially sensitive values (query literals, secrets) from logs.
    pub log_redaction: bool,
    /// OTLP HTTP endpoint for trace export (e.g. `http://127.0.0.1:4318`). When
    /// unset, tracing spans are no-ops.
    pub otlp_endpoint: Option<String>,
}

impl Default for NodusConfig {
    fn default() -> Self {
        Self {
            cluster: ClusterConfig::default(),
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            tls: TlsConfig::default(),
            backup: BackupConfig::default(),
            observability: ObservabilityConfig::default(),
            admin: AdminConfig::default(),
            audit: AuditConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        // Safe defaults: bind localhost, modest connection cap.
        Self {
            pgwire_addr: "127.0.0.1:5432".into(),
            http_addr: "127.0.0.1:8088".into(),
            max_connections: 100,
        }
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: None,
            key_path: None,
            client_ca_path: None,
            require_client_auth: false,
        }
    }
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            repository_uri: String::new(),
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            log_level: "info".into(),
            log_redaction: true,
            otlp_endpoint: None,
        }
    }
}

impl NodusConfig {
    /// Loads defaults, overlays the TOML file at `path` if it exists, then env
    /// overrides. A missing file is not an error.
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let config: NodusConfig = Figment::from(Serialized::defaults(NodusConfig::default()))
            .merge(Toml::file(path))
            .merge(Env::prefixed("NODUS_").split("__"))
            .extract()
            .map_err(|e| ConfigError::Load(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Loads from defaults and env only (no file). Useful for containers.
    pub fn from_env() -> Result<Self, ConfigError> {
        let config: NodusConfig = Figment::from(Serialized::defaults(NodusConfig::default()))
            .merge(Env::prefixed("NODUS_").split("__"))
            .extract()
            .map_err(|e| ConfigError::Load(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.server.pgwire_addr.is_empty() {
            return Err(ConfigError::Invalid("server.pgwire_addr is empty".into()));
        }
        if self.server.http_addr.is_empty() {
            return Err(ConfigError::Invalid("server.http_addr is empty".into()));
        }
        if self.server.max_connections == 0 {
            return Err(ConfigError::Invalid(
                "server.max_connections must be > 0".into(),
            ));
        }
        let lvl = self.observability.log_level.to_lowercase();
        if !["trace", "debug", "info", "warn", "error"].contains(&lvl.as_str()) {
            return Err(ConfigError::Invalid(format!(
                "observability.log_level '{}' is not a valid level",
                self.observability.log_level
            )));
        }
        if self.tls.enabled && (self.tls.cert_path.is_none() || self.tls.key_path.is_none()) {
            return Err(ConfigError::Invalid(
                "tls.enabled requires both cert_path and key_path".into(),
            ));
        }
        // Inter-node mTLS must run on its own listener: requiring client
        // certificates on the shared HTTP port would also lock out admin/web
        // clients, so a dedicated `raft_listen_addr` is mandatory.
        if self.cluster.tls.enabled {
            if self.cluster.tls.cert_path.is_none()
                || self.cluster.tls.key_path.is_none()
                || self.cluster.tls.ca_path.is_none()
            {
                return Err(ConfigError::Invalid(
                    "cluster.tls.enabled requires cluster.tls cert_path, key_path, and ca_path"
                        .into(),
                ));
            }
            if self.cluster.raft_listen_addr.is_none() {
                return Err(ConfigError::Invalid(
                    "cluster.tls.enabled requires a dedicated cluster.raft_listen_addr so peer \
                     mTLS does not gate the public HTTP listener"
                        .into(),
                ));
            }
        }
        // An unauthenticated admin API falls back to the `nodus` superuser, so a
        // non-loopback bind without a token exposes unauthenticated cluster
        // control. Refuse it unless explicitly opted into.
        if self.admin.token.is_none()
            && !self.admin.allow_insecure
            && !is_loopback_addr(&self.server.http_addr)
        {
            return Err(ConfigError::Invalid(format!(
                "admin API is unauthenticated (no admin.token) but server.http_addr '{}' is not \
                 loopback; set admin.token, bind to localhost, or set admin.allow_insecure=true",
                self.server.http_addr
            )));
        }
        Ok(())
    }
}

/// Returns `true` if `addr` (a `host:port`) binds only the loopback interface.
/// Unparsable or non-loopback hosts are treated as non-loopback (fail closed).
fn is_loopback_addr(addr: &str) -> bool {
    let host = match addr.rfind(':') {
        // Strip the port; keep IPv6 literals like `[::1]` intact otherwise.
        Some(idx) if !addr[idx + 1..].contains(']') => &addr[..idx],
        _ => addr,
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_localhost() {
        let cfg = NodusConfig::default();
        assert!(cfg.validate().is_ok());
        assert!(cfg.server.http_addr.starts_with("127.0.0.1"));
        assert!(cfg.observability.metrics_enabled);
    }

    #[test]
    fn loopback_detection() {
        assert!(is_loopback_addr("127.0.0.1:8088"));
        assert!(is_loopback_addr("127.5.0.1:8088"));
        assert!(is_loopback_addr("localhost:8088"));
        assert!(is_loopback_addr("[::1]:8088"));
        assert!(!is_loopback_addr("0.0.0.0:8088"));
        assert!(!is_loopback_addr("192.168.1.5:8088"));
        assert!(!is_loopback_addr("example.com:8088"));
    }

    #[test]
    fn unauthenticated_admin_on_non_loopback_is_rejected() {
        let mut cfg = NodusConfig::default();
        cfg.server.http_addr = "0.0.0.0:8088".into();
        // No token + non-loopback bind => refused.
        assert!(cfg.validate().is_err());

        // A token makes it acceptable.
        cfg.admin.token = Some("secret".into());
        assert!(cfg.validate().is_ok());

        // Or an explicit opt-out.
        cfg.admin.token = None;
        cfg.admin.allow_insecure = true;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn env_overrides_apply() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("NODUS_SERVER__PGWIRE_ADDR", "0.0.0.0:6000");
            jail.set_env("NODUS_OBSERVABILITY__LOG_LEVEL", "debug");
            let cfg = NodusConfig::from_env().unwrap();
            assert_eq!(cfg.server.pgwire_addr, "0.0.0.0:6000");
            assert_eq!(cfg.observability.log_level, "debug");
            Ok(())
        });
    }

    #[test]
    fn toml_file_overlays_defaults() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "nodus.toml",
                r#"
                [server]
                max_connections = 250
                [backup]
                repository_uri = "file:///var/lib/nodus/backups"
                "#,
            )?;
            let cfg = NodusConfig::load("nodus.toml").unwrap();
            assert_eq!(cfg.server.max_connections, 250);
            assert_eq!(cfg.backup.repository_uri, "file:///var/lib/nodus/backups");
            // Untouched fields keep their defaults.
            assert_eq!(cfg.server.pgwire_addr, "127.0.0.1:5432");
            Ok(())
        });
    }

    #[test]
    fn allow_ephemeral_defaults_true_and_is_env_overridable() {
        // Default keeps dev/test ergonomics (in-memory allowed).
        assert!(NodusConfig::default().storage.allow_ephemeral);

        // Operators can require a data dir by turning it off.
        figment::Jail::expect_with(|jail| {
            jail.set_env("NODUS_STORAGE__ALLOW_EPHEMERAL", "false");
            let cfg = NodusConfig::from_env().unwrap();
            assert!(!cfg.storage.allow_ephemeral);
            Ok(())
        });
    }

    #[test]
    fn cluster_mtls_requires_certs_and_dedicated_listener() {
        let mut cfg = NodusConfig::default();
        cfg.cluster.tls.enabled = true;
        // Missing cert/key/ca => refused.
        assert!(cfg.validate().is_err());

        cfg.cluster.tls.cert_path = Some("/tmp/node.crt".into());
        cfg.cluster.tls.key_path = Some("/tmp/node.key".into());
        cfg.cluster.tls.ca_path = Some("/tmp/ca.crt".into());
        // Certs present but no dedicated peer listener => still refused.
        assert!(cfg.validate().is_err());

        cfg.cluster.raft_listen_addr = Some("0.0.0.0:8090".into());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn invalid_log_level_rejected() {
        let mut cfg = NodusConfig::default();
        cfg.observability.log_level = "loud".into();
        assert!(cfg.validate().is_err());
    }
}
