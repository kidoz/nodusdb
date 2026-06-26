//! A self-describing envelope for durable and replicated records.
//!
//! NodusDB requires that new binaries read old formats and that an unrecognized
//! format fails loudly rather than being silently misparsed (or, worse, treated
//! as empty — which would erase a catalog or meta store). This envelope prefixes
//! a serialized payload with a 4-byte magic and a `u16` version, so a reader can
//! dispatch on the version and reject an unknown one, while still recognizing
//! legacy, pre-envelope bytes for backward compatibility.

/// Magic marker identifying a versioned NodusDB record envelope. Chosen so it
/// can never collide with a legacy JSON payload (which begins with `{`, `[`, or
/// whitespace).
pub const MAGIC: [u8; 4] = *b"NDBv";

/// The fixed-size header: 4-byte magic + 2-byte little-endian version.
const HEADER_LEN: usize = MAGIC.len() + 2;

/// Result of inspecting a stored record.
#[derive(Debug, PartialEq, Eq)]
pub enum Envelope<'a> {
    /// A versioned payload: the magic was present.
    Versioned { version: u16, payload: &'a [u8] },
    /// Bytes with no envelope — written before versioning was introduced.
    Legacy(&'a [u8]),
}

/// Wraps `payload` in a versioned envelope (`MAGIC ++ version_le ++ payload`).
pub fn encode(version: u16, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Classifies stored `bytes` as a versioned envelope or legacy bytes. Never
/// fails: legacy data (which cannot begin with `MAGIC`) is returned verbatim for
/// the caller's legacy parse path.
pub fn decode(bytes: &[u8]) -> Envelope<'_> {
    if bytes.len() >= HEADER_LEN && bytes[..MAGIC.len()] == MAGIC {
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        Envelope::Versioned {
            version,
            payload: &bytes[HEADER_LEN..],
        }
    } else {
        Envelope::Legacy(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_versioned_payload() {
        let encoded = encode(7, b"hello");
        assert_eq!(
            decode(&encoded),
            Envelope::Versioned {
                version: 7,
                payload: b"hello"
            }
        );
    }

    #[test]
    fn classifies_legacy_json_as_legacy() {
        // A pre-envelope JSON blob has no magic.
        assert_eq!(decode(b"{\"a\":1}"), Envelope::Legacy(b"{\"a\":1}"));
        // Too-short buffers are legacy, not a truncated envelope.
        assert_eq!(decode(b"ND"), Envelope::Legacy(b"ND"));
        assert_eq!(decode(b""), Envelope::Legacy(b""));
    }

    #[test]
    fn empty_payload_round_trips() {
        let encoded = encode(1, b"");
        assert_eq!(
            decode(&encoded),
            Envelope::Versioned {
                version: 1,
                payload: b""
            }
        );
    }
}
