use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use nodus_catalog::{CatalogReader, DatabaseId, PrincipalId, RoleId};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use subtle::ConstantTimeEq;
use uuid::Uuid;

pub mod scram;
pub use scram::{ClientFirst, ScramError, ScramKeys, ScramVerifier};

/// Compares two byte strings in constant time (independent of where they first
/// differ), so secret comparisons — passwords, bearer tokens — don't leak their
/// contents through timing. Returns `false` for differing lengths.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

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
    active_query: bool,
    started_at: DateTime<Utc>,
    terminate: Arc<AtomicBool>,
    query_cancel: Arc<AtomicBool>,
    backend_key: Option<(i32, i32)>,
}

/// Tracks live client sessions so operators can inspect and cancel them.
///
/// `register` hands back a termination token the session's work loop should
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

    /// Registers a session and returns its termination token.
    pub fn register(&self, session: &Session) -> Arc<AtomicBool> {
        let terminate = Arc::new(AtomicBool::new(false));
        self.sessions.write().unwrap().insert(
            session.session_id.clone(),
            SessionEntry {
                principal_id: session.principal_id,
                current_query: None,
                active_query: false,
                started_at: Utc::now(),
                terminate: terminate.clone(),
                query_cancel: Arc::new(AtomicBool::new(false)),
                backend_key: None,
            },
        );
        terminate
    }

    pub fn deregister(&self, session_id: &str) {
        self.sessions.write().unwrap().remove(session_id);
    }

    pub fn set_current_query(&self, session_id: &str, sql: impl Into<String>) {
        if let Some(e) = self.sessions.write().unwrap().get_mut(session_id) {
            e.current_query = Some(sql.into());
            e.active_query = true;
        }
    }

    pub fn finish_current_query(&self, session_id: &str) {
        if let Some(e) = self.sessions.write().unwrap().get_mut(session_id) {
            e.active_query = false;
            e.query_cancel.store(false, Ordering::SeqCst);
        }
    }

    pub fn clear_current_query(&self, session_id: &str) {
        if let Some(e) = self.sessions.write().unwrap().get_mut(session_id) {
            e.current_query = None;
            e.active_query = false;
            e.query_cancel.store(false, Ordering::SeqCst);
        }
    }

    pub fn update_principal(&self, session_id: &str, principal_id: PrincipalId) {
        if let Some(e) = self.sessions.write().unwrap().get_mut(session_id) {
            e.principal_id = principal_id;
        }
    }

    pub fn register_backend_key(&self, session_id: &str, pid: i32, secret_key: i32) {
        if let Some(e) = self.sessions.write().unwrap().get_mut(session_id) {
            e.backend_key = Some((pid, secret_key));
        }
    }

    /// Requests cancellation of a session. Returns false if it is not known.
    pub fn kill(&self, session_id: &str) -> bool {
        match self.sessions.read().unwrap().get(session_id) {
            Some(e) => {
                e.terminate.store(true, Ordering::SeqCst);
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
            .map(|e| e.terminate.load(Ordering::SeqCst))
            .unwrap_or(false)
    }

    /// Requests cancellation of the currently running query for a backend key.
    ///
    /// PostgreSQL cancel requests are one-shot and should not kill the session.
    /// If the session is idle, the request is accepted but has no later effect.
    pub fn cancel_backend_query(&self, pid: i32, secret_key: i32) -> bool {
        let guard = self.sessions.read().unwrap();
        if let Some(entry) = guard
            .values()
            .find(|e| e.backend_key == Some((pid, secret_key)))
        {
            if entry.active_query {
                entry.query_cancel.store(true, Ordering::SeqCst);
            }
            true
        } else {
            false
        }
    }

    pub fn is_query_cancelled(&self, session_id: &str) -> bool {
        self.sessions
            .read()
            .unwrap()
            .get(session_id)
            .map(|e| e.query_cancel.load(Ordering::SeqCst))
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
                cancelled: e.terminate.load(Ordering::SeqCst)
                    || e.query_cancel.load(Ordering::SeqCst),
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

/// A stored credential: the principal it authenticates plus the SCRAM-SHA-256
/// verifier material (a random per-user salt, the iteration count, and the
/// derived `StoredKey`/`ServerKey`). The same material backs both the SASL/SCRAM
/// wire flow and direct password checks (admin Basic auth); the iteration count
/// is stored alongside so the work factor can be raised later without
/// invalidating existing credentials. Plaintext passwords are never retained.
#[derive(Debug, Clone)]
struct StoredCredential {
    principal_id: PrincipalId,
    keys: ScramKeys,
}

/// PBKDF2-HMAC-SHA256 work factor. A deliberate cost (vs. a single SHA-256) so an
/// attacker who exfiltrates the credential store can't brute-force at hash speed.
const PBKDF2_ITERATIONS: u32 = 100_000;

type HmacSha256 = Hmac<Sha256>;

/// PBKDF2-HMAC-SHA256 with a single 32-byte output block (`dkLen == hLen`), which
/// is exactly the SCRAM `SaltedPassword` for SHA-256. Implemented over `hmac`
/// directly since no `pbkdf2` crate is vendored.
pub(crate) fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let prf = |parts: &[&[u8]]| -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(password).expect("HMAC accepts any key length");
        for part in parts {
            mac.update(part);
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&mac.finalize().into_bytes());
        out
    };
    // U1 = PRF(password, salt || INT_32_BE(1)); DK = U1 ^ U2 ^ … ^ Uc.
    let mut u = prf(&[salt, &1u32.to_be_bytes()]);
    let mut dk = u;
    for _ in 1..iterations.max(1) {
        u = prf(&[&u]);
        for (acc, x) in dk.iter_mut().zip(u.iter()) {
            *acc ^= *x;
        }
    }
    dk
}

/// Authenticates a `(username, password)` pair and issues a [`Session`].
pub trait Authenticator: Send + Sync {
    fn authenticate(&self, username: &str, password: &str) -> Result<Session, AuthError>;
}

/// In-memory credential store. Each password is kept as SCRAM-SHA-256 verifier
/// material, which serves both the SASL/SCRAM wire handshake (passwords never
/// cross the wire) and direct password checks for the admin HTTP API. Role
/// membership for an issued session is resolved from the catalog so the session
/// reflects current grants.
pub struct PasswordAuthenticator {
    credentials: RwLock<HashMap<String, StoredCredential>>,
    catalog: Arc<dyn CatalogReader>,
}

impl PasswordAuthenticator {
    pub fn new(catalog: Arc<dyn CatalogReader>) -> Self {
        Self {
            credentials: RwLock::new(HashMap::new()),
            catalog,
        }
    }

    /// Registers (or replaces) a user's password. A fresh 128-bit salt is
    /// generated here and the SCRAM keys are derived, so callers never deal with
    /// hashing or the password beyond this call.
    pub fn set_password(&self, username: &str, principal_id: PrincipalId, password: &str) {
        let salt = Uuid::new_v4().as_bytes().to_vec();
        let keys = ScramKeys::derive(password, salt, PBKDF2_ITERATIONS);
        self.credentials.write().unwrap().insert(
            username.to_string(),
            StoredCredential { principal_id, keys },
        );
    }

    /// Returns the stored SCRAM verifier material for `username`, if known. Used
    /// by the SASL/SCRAM startup handler to run a challenge/response without ever
    /// seeing the plaintext password.
    pub fn scram_keys(&self, username: &str) -> Option<ScramKeys> {
        self.credentials
            .read()
            .unwrap()
            .get(username)
            .map(|c| c.keys.clone())
    }

    /// Issues a session for an already-authenticated user (e.g. after a SCRAM
    /// exchange has verified the client's proof). Fails only if the user is
    /// unknown.
    pub fn issue_session(&self, username: &str) -> Result<Session, AuthError> {
        let principal_id = self
            .credentials
            .read()
            .unwrap()
            .get(username)
            .map(|c| c.principal_id)
            .ok_or_else(|| AuthError::UnknownUser(username.to_string()))?;
        Ok(self.build_session(principal_id))
    }

    fn build_session(&self, principal_id: PrincipalId) -> Session {
        let active_roles = self
            .catalog
            .get_effective_roles(principal_id)
            .unwrap_or_default();
        Session {
            session_id: Uuid::new_v4().to_string(),
            principal_id,
            active_roles,
            database_id: None,
        }
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

        // Re-derive the SCRAM StoredKey from the supplied password and compare it
        // to the stored one in constant time. This keeps the admin Basic-auth
        // path working off the same material the SCRAM handshake uses.
        let candidate = ScramKeys::derive(password, cred.keys.salt.clone(), cred.keys.iterations);
        if !constant_time_eq(&candidate.stored_key, &cred.keys.stored_key) {
            return Err(AuthError::InvalidCredentials(username.to_string()));
        }

        Ok(self.build_session(cred.principal_id))
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
    fn pbkdf2_is_deterministic_salted_and_not_plain_sha256() {
        // Deterministic for the same salt+password+iterations.
        let a = pbkdf2_sha256(b"pw", b"salt", 1000);
        let b = pbkdf2_sha256(b"pw", b"salt", 1000);
        assert_eq!(a, b);
        // Salt and iteration count both change the output.
        assert_ne!(a, pbkdf2_sha256(b"pw", b"other-salt", 1000));
        assert_ne!(a, pbkdf2_sha256(b"pw", b"salt", 1001));
        // Not a single SHA-256 of salt||password (that would be the old scheme).
        let single = {
            use sha2::Digest;
            let mut h = sha2::Sha256::new();
            h.update(b"salt");
            h.update(b"pw");
            let mut out = [0u8; 32];
            out.copy_from_slice(&h.finalize());
            out
        };
        assert_ne!(pbkdf2_sha256(b"pw", b"salt", PBKDF2_ITERATIONS), single);
    }

    #[test]
    fn constant_time_eq_matches_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd")); // length mismatch
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
        reg.register_backend_key("s1", 123, 456);

        let listed = reg.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].current_query.as_deref(), Some("SELECT 1"));
        assert!(!token.load(Ordering::SeqCst));

        assert!(reg.cancel_backend_query(123, 456));
        assert!(reg.is_query_cancelled("s1"));
        reg.clear_current_query("s1");
        assert!(!reg.is_query_cancelled("s1"));

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
