//! Version-one external-table descriptor and private worker protocol.
//! Credentials travel only over the worker's stdin; these types deliberately
//! do not implement Debug. Frames are bounded before allocation.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Read, Write};

pub const VERSION: u8 = 1;
/// Private worker protocol. Durable descriptors/profiles remain version one.
pub const WORKER_VERSION: u8 = 2;
pub const MAX_FRAME: usize = 64 * 1024;
pub const STORED_FUNCTION: &str = "__nodus_iceberg_v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub enabled: bool,
    pub worker_executable: String,
    pub profiles_file: String,
    pub timeout_seconds: u64,
    pub worker_memory_mib: u64,
    pub worker_address_space_mib: u64,
    pub max_result_bytes: usize,
    pub max_rows: usize,
    pub max_scan_rows: usize,
    pub max_scan_bytes: usize,
    pub max_concurrent_scans: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            worker_executable: String::new(),
            profiles_file: String::new(),
            timeout_seconds: 60,
            worker_memory_mib: 512,
            worker_address_space_mib: 2048,
            max_result_bytes: 16 * 1024 * 1024,
            max_rows: 100_000,
            max_scan_rows: 1_000_000,
            max_scan_bytes: 256 * 1024 * 1024,
            max_concurrent_scans: 2,
        }
    }
}
impl Settings {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !self.enabled {
            return Ok(());
        }
        if !std::path::Path::new(&self.worker_executable).is_absolute()
            || !std::path::Path::new(&self.profiles_file).is_absolute()
        {
            return Err("iceberg worker_executable and profiles_file must be absolute paths");
        }
        if !(1..=86400).contains(&self.timeout_seconds)
            || !(64..=65536).contains(&self.worker_memory_mib)
            || self.worker_address_space_mib < self.worker_memory_mib
            || self.worker_address_space_mib > 131072
            || !(1..=64 * 1024 * 1024).contains(&self.max_result_bytes)
            || !(1..=1_000_000).contains(&self.max_rows)
            || !(1..=10_000_000).contains(&self.max_scan_rows)
            || !(1..=1024 * 1024 * 1024).contains(&self.max_scan_bytes)
            || !(1..=8).contains(&self.max_concurrent_scans)
        {
            return Err("invalid iceberg resource limits");
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub uri: String,
    pub warehouse: Option<String>,
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub storage_properties: HashMap<String, String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profiles {
    pub version: u8,
    pub profiles: HashMap<String, Profile>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub id: i32,
    pub name: String,
    pub sql_type: String,
    pub source_type: String,
    pub nullable: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    pub version: u8,
    pub profile: String,
    pub namespace: Vec<String>,
    pub table: String,
    pub table_uuid: String,
    pub fields: Vec<Field>,
}
/// An AND-connected scan predicate, bound to a registered field identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanPredicate {
    pub field_id: i32,
    pub column: String,
    pub op: ScanOp,
    pub value: ScanLiteral,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ScanOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    IsNull,
    IsNotNull,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScanLiteral {
    Null,
    Int(i64),
    Bool(bool),
    String(String),
    Date(i32),
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScanStats {
    pub planned_files: u64,
    pub planned_file_bytes: u64,
    pub rows_read: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u8,
    pub profile: Profile,
    pub namespace: Vec<String>,
    pub table: String,
    pub expected: Option<Descriptor>,
    /// Empty means inspect only; scan projection names are unique.
    pub columns: Vec<String>,
    pub limit: usize,
    pub timeout_ms: u64,
    pub address_space_bytes: u64,
    /// Absent only in a version-one, unfiltered request.
    #[serde(default)]
    pub predicates: Vec<ScanPredicate>,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Schema {
        version: u8,
        table_uuid: String,
        fields: Vec<Field>,
        snapshot_id: Option<i64>,
    },
    Row(Vec<Option<String>>),
    /// Version two only; planned file sizes are not network-byte measurements.
    ScanStats(ScanStats),
    Done {
        rows: usize,
    },
    /// Safe fixed error classification, never an upstream error body.
    Error(String),
}

pub fn write_frame<T: Serialize>(writer: &mut impl Write, value: &T) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    if bytes.len() > MAX_FRAME {
        return Err(io::Error::other("Iceberg frame exceeds 64 KiB"));
    }
    writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()
}
pub fn read_frame<T: serde::de::DeserializeOwned>(reader: &mut impl Read) -> io::Result<T> {
    let mut length = [0; 4];
    reader.read_exact(&mut length)?;
    let length = u32::from_le_bytes(length) as usize;
    if length > MAX_FRAME {
        return Err(io::Error::other("Iceberg frame exceeds 64 KiB"));
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(|_| io::Error::other("invalid Iceberg worker frame"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_oversized_length_before_reading_body() {
        let bytes = ((MAX_FRAME + 1) as u32).to_le_bytes();
        assert!(
            read_frame::<Response>(&mut bytes.as_slice())
                .unwrap_err()
                .to_string()
                .contains("64 KiB")
        );
    }
    #[test]
    fn protocol_roundtrip_and_configuration_validation() {
        let mut bytes = vec![];
        write_frame(&mut bytes, &Response::Done { rows: 4 }).unwrap();
        assert!(matches!(
            read_frame::<Response>(&mut bytes.as_slice()).unwrap(),
            Response::Done { rows: 4 }
        ));
        let mut settings = Settings {
            enabled: true,
            ..Settings::default()
        };
        assert!(settings.validate().is_err());
        settings.worker_executable = "/bin/worker".into();
        settings.profiles_file = "/etc/profiles.json".into();
        assert!(settings.validate().is_ok());
    }
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;
    #[test]
    fn v1_requests_remain_unfiltered_and_durable_versions_do_not_change() {
        let request = serde_json::json!({"version":1, "profile":{"uri":"http://localhost", "warehouse":null, "bearer_token":null, "storage_properties":{}}, "namespace":["n"], "table":"t", "expected":null, "columns":[], "limit":10, "timeout_ms":1000, "address_space_bytes":1000000});
        let parsed: Request = serde_json::from_value(request).unwrap();
        assert!(parsed.predicates.is_empty());
        assert_eq!(parsed.version, 1);
        assert_eq!(VERSION, 1);
        assert_eq!(WORKER_VERSION, 2);
    }
    #[test]
    fn scan_limits_validate_separately_and_old_settings_get_safe_defaults() {
        let old: Settings = serde_json::from_str(r#"{"enabled":false,"max_rows":10}"#).unwrap();
        assert_eq!(old.max_scan_rows, 1000000);
        assert_eq!(old.max_scan_bytes, 256 * 1024 * 1024);
        let mut configured = Settings {
            enabled: true,
            worker_executable: "/bin/w".into(),
            profiles_file: "/etc/p".into(),
            ..old
        };
        configured.max_scan_rows = 0;
        assert!(configured.validate().is_err());
        configured.max_scan_rows = 1;
        configured.max_scan_bytes = 0;
        assert!(configured.validate().is_err());
    }
}
