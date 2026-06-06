use chrono::{DateTime, Utc};
use nodus_catalog::{CatalogReader, DatabaseId, PrincipalId, RoleId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub principal_id: PrincipalId,
    pub active_roles: Vec<RoleId>,
    pub database_id: Option<DatabaseId>,
}

/// A point-in-time snapshot of an active session for the admin inspector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub principal_id: PrincipalId,
    pub current_query: Option<String>,
    pub started_at: DateTime<Utc>,
    pub cancelled: bool,
}

struct SessionEntry {
    principal_id: PrincipalId,
    current_query: Option<String>,
    started_at: DateTime<Utc>,
    cancel: Arc<AtomicBool>,
}

/// Tracks live client sessions so operators can inspect and cancel them.
///
/// `register` hands back a cancellation token the session's work loop should
/// poll between statements; `kill` flips that token. This is the mechanism
/// behind the admin "active queries" view and `KILL`/`pg_terminate_backend`.
#[derive(Default)]
pub struct SessionRegistry {
    sessions: RwLock<HashMap<String, SessionEntry>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a session and returns its cancellation token.
    pub fn register(&self, session: &Session) -> Arc<AtomicBool> {
        let cancel = Arc::new(AtomicBool::new(false));
        self.sessions.write().unwrap().insert(
            session.session_id.clone(),
            SessionEntry {
                principal_id: session.principal_id,
                current_query: None,
                started_at: Utc::now(),
                cancel: cancel.clone(),
            },
        );
        cancel
    }

    pub fn deregister(&self, session_id: &str) {
        self.sessions.write().unwrap().remove(session_id);
    }

    pub fn set_current_query(&self, session_id: &str, sql: impl Into<String>) {
        if let Some(e) = self.sessions.write().unwrap().get_mut(session_id) {
            e.current_query = Some(sql.into());
        }
    }

    pub fn clear_current_query(&self, session_id: &str) {
        if let Some(e) = self.sessions.write().unwrap().get_mut(session_id) {
            e.current_query = None;
        }
    }

    /// Requests cancellation of a session. Returns false if it is not known.
    pub fn kill(&self, session_id: &str) -> bool {
        match self.sessions.read().unwrap().get(session_id) {
            Some(e) => {
                e.cancel.store(true, Ordering::SeqCst);
                true
            }
            None => false,
        }
    }

    pub fn is_cancelled(&self, session_id: &str) -> bool {
        self.sessions
            .read()
            .unwrap()
            .get(session_id)
            .map(|e| e.cancel.load(Ordering::SeqCst))
            .unwrap_or(false)
    }

    /// Lists active sessions for the inspector.
    pub fn list(&self) -> Vec<SessionInfo> {
        self.sessions
            .read()
            .unwrap()
            .iter()
            .map(|(id, e)| SessionInfo {
                session_id: id.clone(),
                principal_id: e.principal_id,
                current_query: e.current_query.clone(),
                started_at: e.started_at,
                cancelled: e.cancel.load(Ordering::SeqCst),
            })
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("authentication failed for user '{0}'")]
    InvalidCredentials(String),
    #[error("unknown user '{0}'")]
    UnknownUser(String),
}

/// A stored password credential: a random per-user salt plus the SHA-256 hash
/// of `salt || password`. Plaintext passwords are never retained.
#[derive(Debug, Clone)]
struct PasswordCredential {
    principal_id: PrincipalId,
    salt: String,
    hash: String,
}

fn hash_password(salt: &str, password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt.as_bytes());
    hasher.update(password.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Authenticates a `(username, password)` pair and issues a [`Session`].
pub trait Authenticator: Send + Sync {
    fn authenticate(&self, username: &str, password: &str) -> Result<Session, AuthError>;
}

/// In-memory password authenticator. Role membership for the issued session is
/// resolved from the catalog so the session reflects current grants.
pub struct PasswordAuthenticator {
    credentials: RwLock<HashMap<String, PasswordCredential>>,
    catalog: Arc<dyn CatalogReader>,
}

impl PasswordAuthenticator {
    pub fn new(catalog: Arc<dyn CatalogReader>) -> Self {
        Self {
            credentials: RwLock::new(HashMap::new()),
            catalog,
        }
    }

    /// Registers (or replaces) a user's password. The salt is generated here so
    /// callers never deal with hashing.
    pub fn set_password(&self, username: &str, principal_id: PrincipalId, password: &str) {
        let salt = Uuid::new_v4().to_string();
        let hash = hash_password(&salt, password);
        self.credentials.write().unwrap().insert(
            username.to_string(),
            PasswordCredential {
                principal_id,
                salt,
                hash,
            },
        );
    }
}

impl Authenticator for PasswordAuthenticator {
    fn authenticate(&self, username: &str, password: &str) -> Result<Session, AuthError> {
        let cred = {
            let guard = self.credentials.read().unwrap();
            guard
                .get(username)
                .cloned()
                .ok_or_else(|| AuthError::UnknownUser(username.to_string()))?
        };

        if hash_password(&cred.salt, password) != cred.hash {
            return Err(AuthError::InvalidCredentials(username.to_string()));
        }

        let active_roles = self
            .catalog
            .get_effective_roles(cred.principal_id)
            .unwrap_or_default();

        Ok(Session {
            session_id: Uuid::new_v4().to_string(),
            principal_id: cred.principal_id,
            active_roles,
            database_id: None,
        })
    }
}

/// Connection-level TLS configuration. This is the scaffold consumed by the
/// config and deployment layers; terminating TLS on the pgwire/admin listeners
/// is wired up separately.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl TlsConfig {
    /// Validates that, when TLS is enabled, both a certificate and key path are
    /// present and exist on disk. Returns an error describing the first problem.
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let cert = self
            .cert_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("tls.enabled but cert_path is not set"))?;
        let key = self
            .key_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("tls.enabled but key_path is not set"))?;
        if !std::path::Path::new(cert).exists() {
            anyhow::bail!("tls cert_path does not exist: {cert}");
        }
        if !std::path::Path::new(key).exists() {
            anyhow::bail!("tls key_path does not exist: {key}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_catalog::{CatalogWriter, CreateRoleRequest, MemoryCatalog, PrincipalType};

    #[test]
    fn authenticate_success_and_failure() {
        let catalog = Arc::new(MemoryCatalog::new());
        let user = catalog
            .create_role(CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "alice".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        let auth = PasswordAuthenticator::new(catalog);
        auth.set_password("alice", user.id, "s3cret");

        let session = auth.authenticate("alice", "s3cret").unwrap();
        assert_eq!(session.principal_id, user.id);

        assert!(matches!(
            auth.authenticate("alice", "wrong"),
            Err(AuthError::InvalidCredentials(_))
        ));
        assert!(matches!(
            auth.authenticate("nobody", "x"),
            Err(AuthError::UnknownUser(_))
        ));
    }

    #[test]
    fn session_registry_inspect_and_kill() {
        let reg = SessionRegistry::new();
        let session = Session {
            session_id: "s1".into(),
            principal_id: PrincipalId::new(),
            active_roles: vec![],
            database_id: None,
        };
        let token = reg.register(&session);
        reg.set_current_query("s1", "SELECT 1");

        let listed = reg.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].current_query.as_deref(), Some("SELECT 1"));
        assert!(!token.load(Ordering::SeqCst));

        assert!(reg.kill("s1"));
        assert!(reg.is_cancelled("s1"));
        assert!(token.load(Ordering::SeqCst)); // the running session observes it
        assert!(!reg.kill("missing"));

        reg.deregister("s1");
        assert!(reg.list().is_empty());
    }

    #[test]
    fn tls_validation() {
        let off = TlsConfig::default();
        assert!(off.validate().is_ok());
        let on = TlsConfig {
            enabled: true,
            cert_path: None,
            key_path: None,
        };
        assert!(on.validate().is_err());
    }
}
