//! Read-only Iceberg REST interoperability, isolated from NodusDB's OLTP engine.
//!
//! A read pins the loaded table's snapshot and read schema and returns Arrow
//! batches. Current reads use the current table schema; explicit time travel
//! uses the snapshot's historical schema. This external client does not access
//! NodusDB SQL/catalog state or use admin
//! credentials. The external catalog and storage authorize requests.

mod catalog;
mod config;
mod pruning;
mod reader;
mod storage;

pub use config::{CatalogOptions, ReadOptions, StorageBackend};
pub use nodus_common::iceberg::{ScanLiteral, ScanOp, ScanPredicate, ScanStats};
pub use reader::{BatchReader, ReadSummary, SnapshotReader};

/// Errors deliberately omit upstream response bodies, URLs and credentials.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid local arguments; no remote operation is required to detect this.
    #[error("invalid Iceberg options: {0}")]
    InvalidOptions(String),
    /// The requested feature is outside the verified read-only subset.
    #[error("unsupported Iceberg feature: {0}")]
    Unsupported(&'static str),
    /// A safe operation label and error class, without remote contents.
    #[error(
        "Iceberg {operation} failed ({kind}); check catalog permissions, table metadata and storage access"
    )]
    Operation {
        /// Operation that failed.
        operation: &'static str,
        /// Error classification, not a potentially sensitive upstream message.
        kind: iceberg::ErrorKind,
    },
    /// The deadline includes metadata loading, planning and batch consumption.
    #[error("Iceberg read deadline exceeded; increase --timeout-seconds or reduce the scan")]
    Timeout,
}

pub(crate) fn operation(operation: &'static str, error: iceberg::Error) -> Error {
    Error::Operation {
        operation,
        kind: error.kind(),
    }
}
