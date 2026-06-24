//! Schema descriptors and the typed-id newtypes shared across the catalog.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum DescriptorState {
    Public,
    Adding,
    Dropping,
    Dropped,
}

macro_rules! typed_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

typed_id!(DatabaseId);
typed_id!(SchemaId);
typed_id!(TableId);
typed_id!(ColumnId);
typed_id!(IndexId);
typed_id!(ShardId);
typed_id!(PrincipalId);
typed_id!(RoleId);
typed_id!(GrantId);
typed_id!(DefaultGrantId);
typed_id!(PolicyId);
typed_id!(MaskId);
typed_id!(RoleMembershipId);
typed_id!(AuditEventId);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseDescriptor {
    pub id: DatabaseId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub owner_role_id: Option<RoleId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDescriptor {
    pub id: SchemaId,
    pub database_id: DatabaseId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub owner_role_id: Option<RoleId>,
    pub managed_access: bool,
    pub system_schema: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDescriptor {
    pub id: TableId,
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub columns: Vec<ColumnDescriptor>,
    pub indexes: Vec<IndexDescriptor>,
    #[serde(default)]
    pub constraints: Vec<TableConstraint>,
    #[serde(default)]
    pub view_query: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableConstraint {
    Check {
        name: Option<String>,
        expr: String, // AST expr as string for evaluation
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        foreign_table: String,
        referred_columns: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub id: ColumnId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub data_type: String, // Simplified for MVP
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexType {
    Primary,
    LocalSecondary,
    Composite,
    Unique,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexState {
    Creating,
    Backfilling,
    Validating,
    Ready,
    Dropping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexColumn {
    pub column_id: ColumnId,
    pub descending: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Expression {
    pub sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDescriptor {
    pub id: IndexId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub index_type: IndexType,
    pub index_state: IndexState,
    pub key_columns: Vec<IndexColumn>,
    pub include_columns: Vec<ColumnId>,
    pub unique: bool,
    pub global: bool,
    pub predicate: Option<Expression>,
    pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexBackfillDescriptor {
    pub id: Uuid,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub index_id: IndexId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardDescriptor {
    pub id: ShardId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub table_id: TableId,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneConfig {
    pub id: Uuid,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterDescriptor {
    pub id: Uuid,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterVersion {
    pub id: Uuid,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub active_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureGate {
    pub id: Uuid,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub enabled: bool,
}
