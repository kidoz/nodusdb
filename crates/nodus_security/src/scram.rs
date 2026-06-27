//! Server-side SCRAM-SHA-256 (RFC 5802 / RFC 7677) for the PostgreSQL SASL
//! authentication flow.
//!
//! Only the `SCRAM-SHA-256` mechanism is offered (not `-PLUS`), so channel
//! binding is not negotiated and clients send the `n,,` GS2 header. The plaintext
//! password never crosses the wire: the server stores only a salt, an iteration
//! count, and the derived `StoredKey`/`ServerKey`, and proves knowledge of the
//! password by checking the client's proof against `StoredKey`.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use crate::{constant_time_eq, pbkdf2_sha256};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, thiserror::Error)]
pub enum ScramError {
    #[error("malformed SCRAM message: {0}")]
    Protocol(&'static str),
    #[error("SCRAM channel binding is not supported")]
    UnsupportedChannelBinding,
    #[error("SCRAM authentication failed")]
    AuthFailed,
}

/// The SCRAM verifier material stored per user. Derived from the password once at
/// `set_password` time; the plaintext password is never retained. `StoredKey` and
/// `ServerKey` are not password-equivalent — an attacker who exfiltrates them
/// still cannot answer a SCRAM challenge without the (PBKDF2-stretched) password.
#[derive(Debug, Clone)]
pub struct ScramKeys {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl ScramKeys {
    /// Derives SCRAM keys from a plaintext password, salt, and iteration count.
    pub fn derive(password: &str, salt: Vec<u8>, iterations: u32) -> Self {
        let salted = pbkdf2_sha256(password.as_bytes(), &salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        let server_key = hmac(&salted, b"Server Key");
        let stored_key = sha256(&client_key);
        Self {
            salt,
            iterations,
            stored_key,
            server_key,
        }
    }
}

fn hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    let mut out = [0u8; 32];
    out.copy_from_slice(&mac.finalize().into_bytes());
    out
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&h.finalize());
    out
}

/// Decodes a SCRAM `saslname`, undoing the `=2C`/`=3D` escaping of `,`/`=`.
fn decode_saslname(s: &str) -> String {
    s.replace("=2C", ",").replace("=3D", "=")
}

/// The parsed `client-first-message`: the GS2 header plus the bare body that the
/// `AuthMessage` is computed over.
#[derive(Debug, Clone)]
pub struct ClientFirst {
    pub username: String,
    /// The full GS2 header including its trailing comma (e.g. `n,,`).
    pub gs2_header: String,
    /// `client-first-message-bare`: everything after the GS2 header.
    pub client_first_bare: String,
    pub client_nonce: String,
}

impl ClientFirst {
    pub fn parse(data: &[u8]) -> Result<Self, ScramError> {
        let s = std::str::from_utf8(data).map_err(|_| ScramError::Protocol("not utf-8"))?;
        // gs2-header = gs2-cbind-flag "," [ authzid ] ","
        let first = s
            .find(',')
            .ok_or(ScramError::Protocol("missing gs2 cbind flag"))?;
        let cbind_flag = &s[..first];
        // We never advertise SCRAM-SHA-256-PLUS, so a client requiring channel
        // binding ('p=...') is a protocol violation here.
        if cbind_flag.starts_with('p') {
            return Err(ScramError::UnsupportedChannelBinding);
        }
        if cbind_flag != "n" && cbind_flag != "y" {
            return Err(ScramError::Protocol("invalid gs2 cbind flag"));
        }
        let rest = &s[first + 1..];
        let second = rest
            .find(',')
            .ok_or(ScramError::Protocol("missing authzid separator"))?;
        let gs2_header = s[..first + 1 + second + 1].to_string();
        let bare = rest[second + 1..].to_string();

        let mut username = None;
        let mut nonce = None;
        for field in bare.split(',') {
            if let Some(v) = field.strip_prefix("n=") {
                username = Some(decode_saslname(v));
            } else if let Some(v) = field.strip_prefix("r=") {
                nonce = Some(v.to_string());
            }
        }
        Ok(ClientFirst {
            username: username.ok_or(ScramError::Protocol("missing username"))?,
            gs2_header,
            client_first_bare: bare,
            client_nonce: nonce.ok_or(ScramError::Protocol("missing client nonce"))?,
        })
    }
}

/// In-progress server-side SCRAM exchange, carrying everything needed to verify
/// the client's final message after the server-first message has been sent.
#[derive(Debug, Clone)]
pub struct ScramVerifier {
    client_first_bare: String,
    server_first: String,
    combined_nonce: String,
    gs2_header: String,
    stored_key: [u8; 32],
    server_key: [u8; 32],
}

impl ScramVerifier {
    /// Builds the `server-first-message` and the verifier that checks the
    /// client's final proof. `server_nonce` is supplied by the caller (so this
    /// module stays free of a randomness dependency and is deterministic in
    /// tests); it must be a fresh, unpredictable, comma-free token per exchange.
    pub fn start(
        cf: &ClientFirst,
        keys: &ScramKeys,
        server_nonce: &str,
    ) -> (String, ScramVerifier) {
        let combined_nonce = format!("{}{}", cf.client_nonce, server_nonce);
        let server_first = format!(
            "r={},s={},i={}",
            combined_nonce,
            B64.encode(&keys.salt),
            keys.iterations
        );
        let verifier = ScramVerifier {
            client_first_bare: cf.client_first_bare.clone(),
            server_first: server_first.clone(),
            combined_nonce,
            gs2_header: cf.gs2_header.clone(),
            stored_key: keys.stored_key,
            server_key: keys.server_key,
        };
        (server_first, verifier)
    }

    /// Verifies the `client-final-message` and, on success, returns the
    /// `server-final-message` (`v=<ServerSignature>`) proving the server also
    /// holds the key. Fails closed on any malformed field or proof mismatch.
    pub fn finish(&self, client_final: &[u8]) -> Result<String, ScramError> {
        let s = std::str::from_utf8(client_final).map_err(|_| ScramError::Protocol("not utf-8"))?;
        let mut cbind = None;
        let mut rcvd_nonce = None;
        let mut proof_b64 = None;
        for field in s.split(',') {
            if let Some(v) = field.strip_prefix("c=") {
                cbind = Some(v);
            } else if let Some(v) = field.strip_prefix("r=") {
                rcvd_nonce = Some(v);
            } else if let Some(v) = field.strip_prefix("p=") {
                proof_b64 = Some(v);
            }
        }
        let cbind = cbind.ok_or(ScramError::Protocol("missing channel binding"))?;
        let rcvd_nonce = rcvd_nonce.ok_or(ScramError::Protocol("missing nonce"))?;
        let proof_b64 = proof_b64.ok_or(ScramError::Protocol("missing client proof"))?;

        // The channel-binding data must echo our GS2 header verbatim (base64),
        // and the nonce must match the one we issued — both guard against an
        // attacker splicing a different handshake's messages together.
        let expected_cbind = B64.encode(self.gs2_header.as_bytes());
        if !constant_time_eq(cbind.as_bytes(), expected_cbind.as_bytes()) {
            return Err(ScramError::Protocol("channel binding mismatch"));
        }
        if !constant_time_eq(rcvd_nonce.as_bytes(), self.combined_nonce.as_bytes()) {
            return Err(ScramError::Protocol("nonce mismatch"));
        }

        let without_proof = format!("c={cbind},r={rcvd_nonce}");
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );

        let client_signature = hmac(&self.stored_key, auth_message.as_bytes());
        let proof = B64
            .decode(proof_b64)
            .map_err(|_| ScramError::Protocol("client proof not base64"))?;
        if proof.len() != 32 {
            return Err(ScramError::Protocol("client proof wrong length"));
        }
        // ClientKey = ClientProof XOR ClientSignature; StoredKey == H(ClientKey).
        let mut client_key = [0u8; 32];
        for i in 0..32 {
            client_key[i] = proof[i] ^ client_signature[i];
        }
        if !constant_time_eq(&sha256(&client_key), &self.stored_key) {
            return Err(ScramError::AuthFailed);
        }

        let server_signature = hmac(&self.server_key, auth_message.as_bytes());
        Ok(format!("v={}", B64.encode(server_signature)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirrors a conforming client: derives ClientKey, signs the AuthMessage, and
    /// produces the client-final proof so we can drive a full round trip.
    fn client_final_for(
        password: &str,
        keys: &ScramKeys,
        client_first_bare: &str,
        server_first: &str,
        gs2_header: &str,
        combined_nonce: &str,
    ) -> String {
        let salted = pbkdf2_sha256(password.as_bytes(), &keys.salt, keys.iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let cbind = B64.encode(gs2_header.as_bytes());
        let without_proof = format!("c={cbind},r={combined_nonce}");
        let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = client_key[i] ^ client_signature[i];
        }
        format!("{without_proof},p={}", B64.encode(proof))
    }

    #[test]
    fn full_round_trip_succeeds_and_returns_server_signature() {
        let keys = ScramKeys::derive("s3cret", b"0123456789abcdef".to_vec(), 4096);
        let cf = ClientFirst::parse(b"n,,n=alice,r=clientnonce123").unwrap();
        assert_eq!(cf.username, "alice");
        let (server_first, verifier) = ScramVerifier::start(&cf, &keys, "servernonceXYZ");
        let combined = format!("{}{}", cf.client_nonce, "servernonceXYZ");
        assert!(server_first.contains(&combined));

        let client_final = client_final_for(
            "s3cret",
            &keys,
            &cf.client_first_bare,
            &server_first,
            &cf.gs2_header,
            &combined,
        );
        let server_final = verifier.finish(client_final.as_bytes()).unwrap();
        assert!(server_final.starts_with("v="));
    }

    #[test]
    fn wrong_password_is_rejected() {
        let keys = ScramKeys::derive("s3cret", b"0123456789abcdef".to_vec(), 4096);
        let cf = ClientFirst::parse(b"n,,n=alice,r=clientnonce123").unwrap();
        let (server_first, verifier) = ScramVerifier::start(&cf, &keys, "servernonceXYZ");
        let combined = format!("{}{}", cf.client_nonce, "servernonceXYZ");
        // Client computes its proof from the wrong password.
        let client_final = client_final_for(
            "wrong",
            &keys,
            &cf.client_first_bare,
            &server_first,
            &cf.gs2_header,
            &combined,
        );
        assert!(matches!(
            verifier.finish(client_final.as_bytes()),
            Err(ScramError::AuthFailed)
        ));
    }

    #[test]
    fn tampered_nonce_is_rejected() {
        let keys = ScramKeys::derive("s3cret", b"0123456789abcdef".to_vec(), 4096);
        let cf = ClientFirst::parse(b"n,,n=alice,r=clientnonce123").unwrap();
        let (_server_first, verifier) = ScramVerifier::start(&cf, &keys, "servernonceXYZ");
        let forged = format!(
            "c={},r=someothernonce,p={}",
            B64.encode(b"n,,"),
            B64.encode([0u8; 32])
        );
        assert!(matches!(
            verifier.finish(forged.as_bytes()),
            Err(ScramError::Protocol(_))
        ));
    }

    #[test]
    fn channel_binding_request_is_refused() {
        // 'p=tls-server-end-point' would require SCRAM-SHA-256-PLUS, which we
        // never advertise.
        assert!(matches!(
            ClientFirst::parse(b"p=tls-server-end-point,,n=alice,r=nonce"),
            Err(ScramError::UnsupportedChannelBinding)
        ));
    }

    #[test]
    fn gs2_header_and_bare_are_split_correctly() {
        let cf = ClientFirst::parse(b"n,,n=bob,r=abc").unwrap();
        assert_eq!(cf.gs2_header, "n,,");
        assert_eq!(cf.client_first_bare, "n=bob,r=abc");
        assert_eq!(cf.client_nonce, "abc");
    }
}
