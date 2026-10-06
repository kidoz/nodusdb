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
    /// The role's `CREATE` / `ALTER ROLE` attributes. Defaulted so
    /// descriptors persisted before it decode (to PostgreSQL's defaults).
    #[serde(default)]
    pub attributes: RoleAttributes,
}

/// A role's attributes, as `CREATE ROLE` and `ALTER ROLE` set them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoleAttributes {
    pub can_login: bool,
    pub superuser: bool,
    pub create_db: bool,
    pub create_role: bool,
    pub inherit: bool,
    pub bypass_rls: bool,
    pub replication: bool,
    pub connection_limit: i32,
    /// `VALID UNTIL`, as written.
    pub valid_until: Option<String>,
    /// The role's password as a SCRAM-SHA-256 verifier (never plaintext).
    pub password: Option<String>,
    /// `ALTER ROLE ... SET name = value`, as `pg_db_role_setting` lists them.
    pub settings: Vec<(String, String)>,
}

impl Default for RoleAttributes {
    fn default() -> Self {
        RoleAttributes {
            can_login: false,
            superuser: false,
            create_db: false,
            create_role: false,
            inherit: true,
            bypass_rls: false,
            replication: false,
            connection_limit: -1,
            valid_until: None,
            password: None,
            settings: Vec::new(),
        }
    }
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

/// One role-membership edge: the role, its member, the admin option
/// (`GRANT ... WITH ADMIN OPTION`), and the role that granted it.
pub type RoleMembershipEdge = (PrincipalId, PrincipalId, bool, Option<PrincipalId>);

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
    /// `WITH GRANT OPTION`: the grantee may grant the privilege onward.
    #[serde(default)]
    pub grantable: bool,
    /// Who made the grant (as `information_schema` shows it).
    #[serde(default)]
    pub grantor: Option<PrincipalId>,
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
