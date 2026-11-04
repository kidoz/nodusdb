//! Version-one external-table descriptor and private worker protocol.
//! Credentials travel only over the worker's stdin; these types deliberately
//! do not implement Debug. Frames are bounded before allocation.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Read, Write};

pub const VERSION: u8 = 1;
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
