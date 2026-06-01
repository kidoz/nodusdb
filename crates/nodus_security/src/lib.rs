use nodus_catalog::{CatalogReader, DatabaseId, PrincipalId, RoleId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub principal_id: PrincipalId,
    pub active_roles: Vec<RoleId>,
    pub database_id: Option<DatabaseId>,
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
