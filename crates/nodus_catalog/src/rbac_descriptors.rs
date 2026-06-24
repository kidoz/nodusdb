//! RBAC descriptors: principals, roles, grants, row policies, column masks.
use crate::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PrincipalType {
    User,
    ServiceAccount,
    Role,
    DatabaseRole,
    Public,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalDescriptor {
    pub id: PrincipalId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub principal_type: PrincipalType,
    pub database_id: Option<DatabaseId>, // for DatabaseRole
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleMembershipDescriptor {
    pub id: RoleMembershipId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub role_id: RoleId,
    pub member_id: PrincipalId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceRef {
    Database(DatabaseId),
    Schema(SchemaId),
    Table(TableId),
    Column(ColumnId),
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantDescriptor {
    pub id: GrantId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String, // CONNECT, USAGE, SELECT, INSERT, etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultGrantDescriptor {
    pub id: DefaultGrantId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub schema_id: SchemaId,
    pub principal_id: PrincipalId,
    pub privilege: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowPolicyDescriptor {
    pub id: PolicyId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub table_id: TableId,
    pub expression: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMaskDescriptor {
    pub id: MaskId,
    pub name: String,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: DescriptorState,
    pub column_id: ColumnId,
    pub expression: String,
}
