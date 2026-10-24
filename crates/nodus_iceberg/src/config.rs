use std::collections::HashMap;
use std::time::Duration;

use crate::Error;

/// Storage access is explicit: a remote catalog cannot switch an S3 reader to
/// reading local files. Use local files only with a trusted development catalog.
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize)]
pub enum StorageBackend {
    /// S3 or an S3-compatible object store.
    #[default]
    S3,
    /// Local filesystem, for a catalog whose file paths are visible to this host.
    Local,
}

/// Connection settings. Intentionally not `Debug`: properties may hold secrets.
pub struct CatalogOptions {
    /// REST base URL, before `/v1`, e.g. Lakekeeper's `/catalog` endpoint.
    pub uri: String,
    /// Catalog-specific warehouse identifier, supplied to config discovery.
    pub warehouse: Option<String>,
    /// External catalog bearer token; never a NodusDB admin token.
    pub bearer_token: Option<String>,
    /// Optional S3 properties, e.g. `s3.endpoint`, `s3.region` and credentials.
    pub storage_properties: HashMap<String, String>,
    /// Allowed storage backend.
    pub storage: StorageBackend,
    /// Total metadata/scan deadline, including time between batch requests.
    pub timeout: Duration,
}

impl CatalogOptions {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let url = iceberg_http::Url::parse(&self.uri)
            .map_err(|_| Error::InvalidOptions("catalog URI must be an HTTP(S) URL".into()))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::InvalidOptions(
                "catalog URI must be HTTP(S), without user info, query or fragment".into(),
            ));
        }
        if self.timeout.is_zero() || self.timeout > Duration::from_secs(86_400) {
            return Err(Error::InvalidOptions(
                "timeout must be within 1ns..=24h".into(),
            ));
        }
        if let Some(token) = &self.bearer_token
            && (token.is_empty()
                || iceberg_http::header::HeaderValue::from_str(&format!("Bearer {token}")).is_err())
        {
            return Err(Error::InvalidOptions("invalid catalog bearer token".into()));
        }
        if self
            .storage_properties
            .keys()
            .any(|key| !key.starts_with("s3."))
        {
            return Err(Error::InvalidOptions(
                "storage properties must use s3.* keys; catalog authentication is configured separately".into(),
            ));
        }
        Ok(())
    }
}

/// A single table/snapshot selection. Column names are exact and case sensitive.
#[derive(Clone, Debug)]
pub struct ReadOptions {
    /// Namespace components, not a dot-split SQL identifier.
    pub namespace: Vec<String>,
    /// Table name.
    pub table: String,
    /// Retained snapshot ID; omission pins the current snapshot at load time.
    pub snapshot_id: Option<i64>,
    /// Top-level columns in output order; empty means all columns.
    pub columns: Vec<String>,
    /// Maximum rows requested per Arrow batch (1..=65536).
    pub batch_size: usize,
    /// Maximum output rows, or no row limit. This is not an ordered SQL LIMIT.
    pub limit: Option<usize>,
}

impl ReadOptions {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        // The upstream REST client interpolates the table name into its URL.
        // Constrain the initial identifier subset rather than allow path/query
        // separators to change the requested resource.
        let simple = |s: &str| {
            !s.is_empty()
                && s != "."
                && s != ".."
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
        };
        if self.namespace.is_empty()
            || !self.namespace.iter().all(|s| simple(s))
            || !simple(&self.table)
        {
            return Err(Error::InvalidOptions(
                "namespace components and table must contain only ASCII letters, digits, _, - or . (not . or .. alone)".into(),
            ));
        }
        if !(1..=65_536).contains(&self.batch_size) {
            return Err(Error::InvalidOptions(
                "batch size must be within 1..=65536".into(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        if self.columns.iter().any(|s| s.is_empty() || !seen.insert(s)) {
            return Err(Error::InvalidOptions(
                "columns must be nonempty and unique".into(),
            ));
        }
        Ok(())
    }
}
