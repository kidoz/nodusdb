//! The two read-only REST operations, with origin checks before authentication.
//!
//! The upstream catalog applies server-supplied URI overrides internally. Keep
//! discovery and table loading here so no request can escape the configured
//! origin, including through a changing discovery response.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::FileIOBuilder;
use iceberg::table::Table;
use iceberg::{ErrorKind, NamespaceIdent, Runtime, TableIdent};
use iceberg_catalog_rest::LoadTableResult;
use iceberg_http::{Client, StatusCode, Url};
use serde::Deserialize;

use crate::{CatalogOptions, Error, ReadOptions, operation};

#[derive(Deserialize)]
struct CatalogConfig {
    defaults: HashMap<String, String>,
    overrides: HashMap<String, String>,
}

fn checked_uri(uri: &str, origin: &Url) -> Result<Url, Error> {
    let url = Url::parse(uri).map_err(|_| Error::Unsupported("invalid catalog URI override"))?;
    if url.origin() != origin.origin()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Unsupported(
            "catalog URI overrides must preserve the configured origin (scheme, host and port) and omit user info, query and fragment",
        ));
    }
    Ok(url)
}

async fn get<T: serde::de::DeserializeOwned>(
    client: &Client,
    url: Url,
    token: Option<&str>,
    operation: &'static str,
) -> Result<T, Error> {
    let mut request = client
        .get(url)
        .header("X-Iceberg-Access-Delegation", "vended-credentials")
        .header("Accept", "application/json");
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.map_err(|_| Error::Operation {
        operation,
        kind: ErrorKind::Unexpected,
    })?;
    if response.status() != StatusCode::OK {
        return Err(Error::Operation {
            operation,
            kind: if response.status() == StatusCode::NOT_FOUND && operation == "load table" {
                ErrorKind::TableNotFound
            } else {
                ErrorKind::Unexpected
            },
        });
    }
    // Neither HTTP error bodies nor JSON decode errors may disclose credentials.
    response.json().await.map_err(|_| Error::Operation {
        operation,
        kind: ErrorKind::DataInvalid,
    })
}

pub(crate) async fn load_table(
    config: CatalogOptions,
    options: &ReadOptions,
) -> Result<Table, Error> {
    let origin =
        Url::parse(&config.uri).map_err(|_| Error::InvalidOptions("invalid catalog URI".into()))?;
    let client = Client::builder()
        .redirect(iceberg_http::redirect::Policy::none())
        .timeout(config.timeout)
        .build()
        .map_err(|_| Error::InvalidOptions("could not construct catalog HTTP client".into()))?;
    let mut discovery_url = origin.clone();
    discovery_url.set_path(&format!(
        "{}/v1/config",
        origin.path().trim_end_matches('/')
    ));
    if let Some(warehouse) = &config.warehouse {
        discovery_url
            .query_pairs_mut()
            .append_pair("warehouse", warehouse);
    }
    let discovery: CatalogConfig = get(
        &client,
        discovery_url,
        config.bearer_token.as_deref(),
        "catalog configuration",
    )
    .await?;
    let base = match discovery.overrides.get("uri") {
        Some(uri) => checked_uri(uri, &origin)?,
        None => origin,
    };
    let mut properties = discovery.defaults;
    properties.extend(discovery.overrides);
    let mut table_url = base.clone();
    {
        let mut path = table_url
            .path_segments_mut()
            .map_err(|_| Error::Unsupported("catalog URI must support hierarchical paths"))?;
        path.pop_if_empty().push("v1");
        if let Some(prefix) = properties.get("prefix") {
            path.extend(prefix.split('/').filter(|part| !part.is_empty()));
        }
        path.extend([
            "namespaces",
            &options.namespace.join("\u{1f}"),
            "tables",
            &options.table,
        ]);
    }
    // Config properties cannot replace the caller's bearer token or add headers.
    let response: LoadTableResult = get(
        &client,
        table_url,
        config.bearer_token.as_deref(),
        "load table",
    )
    .await?;
    properties.extend(response.config);
    properties.extend(config.storage_properties);
    let file_io = FileIOBuilder::new(Arc::new(crate::storage::ReadStorageFactory(config.storage)))
        .with_props(properties)
        .build();
    let namespace = NamespaceIdent::from_vec(options.namespace.clone())
        .map_err(|e| operation("namespace validation", e))?;
    let mut table = Table::builder()
        .identifier(TableIdent::new(namespace, options.table.clone()))
        .metadata(response.metadata)
        .file_io(file_io)
        .runtime(Runtime::try_current().map_err(|e| operation("catalog runtime", e))?)
        .readonly(true);
    if let Some(location) = response.metadata_location {
        table = table.metadata_location(location);
    }
    table.build().map_err(|e| operation("load table", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_overrides_preserve_origin_and_cannot_downgrade_tls() {
        let origin = Url::parse("https://catalog.example/catalog").unwrap();
        assert!(checked_uri("https://catalog.example:443/other", &origin).is_ok());
        for uri in [
            "http://catalog.example/catalog",
            "https://other.example/catalog",
            "https://catalog.example:444/catalog",
            "https://user@catalog.example/catalog",
            "https://catalog.example/catalog?token=secret",
            "https://catalog.example/catalog#fragment",
            "file:///etc/passwd",
            "invalid",
        ] {
            assert!(checked_uri(uri, &origin).is_err(), "{uri}");
        }
    }
}
