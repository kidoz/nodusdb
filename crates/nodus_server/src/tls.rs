//! Hot-reloadable TLS for the server's listeners.
//!
//! Each listener's `rustls::ServerConfig` resolves its certificate through a
//! [`ReloadableCertResolver`] instead of a fixed certificate, so a `SIGHUP` can
//! swap in a renewed certificate without dropping the listener or any live
//! connection: new handshakes pick up the new certificate while in-flight
//! sessions keep the one they negotiated. A reload that fails to parse (a
//! half-written file, a mismatched key) leaves the previous certificate in
//! place.

use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert};
use tokio_rustls::rustls::sign::CertifiedKey;

/// A rustls certificate resolver whose certificate can be reloaded from disk at
/// runtime. Built from the original cert/key paths so a reload re-reads the same
/// files (the operator replaces them in place, then signals the process).
pub struct ReloadableCertResolver {
    cert_path: PathBuf,
    key_path: PathBuf,
    current: RwLock<Arc<CertifiedKey>>,
}

impl fmt::Debug for ReloadableCertResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReloadableCertResolver")
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .finish()
    }
}

impl ReloadableCertResolver {
    /// Loads the initial certificate and key.
    pub fn load(
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> anyhow::Result<Arc<Self>> {
        let cert_path = cert_path.into();
        let key_path = key_path.into();
        let key = Self::build(&cert_path, &key_path)?;
        Ok(Arc::new(Self {
            cert_path,
            key_path,
            current: RwLock::new(key),
        }))
    }

    fn build(cert_path: &PathBuf, key_path: &PathBuf) -> anyhow::Result<Arc<CertifiedKey>> {
        let cert_bytes = std::fs::read(cert_path)?;
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut cert_bytes.as_slice()).collect::<Result<_, _>>()?;
        if certs.is_empty() {
            anyhow::bail!("no certificates found in {}", cert_path.display());
        }
        let key_bytes = std::fs::read(key_path)?;
        let key: PrivateKeyDer<'static> =
            rustls_pemfile::private_key(&mut key_bytes.as_slice())?
                .ok_or_else(|| anyhow::anyhow!("no private key found in {}", key_path.display()))?;
        let provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
        let certified = CertifiedKey::from_der(certs, key, &provider)?;
        Ok(Arc::new(certified))
    }

    /// Re-reads the certificate and key from their original paths and swaps them
    /// in for subsequent handshakes. The previous certificate is retained if the
    /// new material cannot be read or parsed.
    pub fn reload(&self) -> anyhow::Result<()> {
        let fresh = Self::build(&self.cert_path, &self.key_path)?;
        *self.current.write().unwrap() = fresh;
        Ok(())
    }

    /// The DER bytes of the currently-served leaf certificate (test helper for
    /// asserting a reload took effect).
    #[cfg(test)]
    pub(crate) fn current_leaf_der(&self) -> Vec<u8> {
        self.current.read().unwrap().cert[0].as_ref().to_vec()
    }
}

impl ResolvesServerCert for ReloadableCertResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current.read().unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_self_signed(cert_path: &std::path::Path, key_path: &std::path::Path) {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        std::fs::write(cert_path, cert.pem()).unwrap();
        std::fs::write(key_path, key.serialize_pem()).unwrap();
    }

    #[test]
    fn reload_swaps_the_served_certificate() {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");

        write_self_signed(&cert_path, &key_path);
        let resolver = ReloadableCertResolver::load(&cert_path, &key_path).unwrap();
        let before = resolver.current_leaf_der();

        // Replace the on-disk material with a freshly generated certificate, then
        // reload: the served certificate must change.
        write_self_signed(&cert_path, &key_path);
        resolver.reload().unwrap();
        let after = resolver.current_leaf_der();
        assert_ne!(before, after, "reload should serve the new certificate");
    }

    #[test]
    fn reload_keeps_previous_certificate_on_failure() {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");

        write_self_signed(&cert_path, &key_path);
        let resolver = ReloadableCertResolver::load(&cert_path, &key_path).unwrap();
        let before = resolver.current_leaf_der();

        // A truncated/half-written certificate file must not clobber the live one.
        std::fs::write(&cert_path, b"not a pem").unwrap();
        assert!(resolver.reload().is_err());
        assert_eq!(before, resolver.current_leaf_der());
    }
}
