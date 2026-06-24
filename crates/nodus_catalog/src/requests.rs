//! Catalog operation requests and object/snapshot helper types.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum ObjectDescriptor {
    Database(DatabaseDescriptor),
    Schema(SchemaDescriptor),
    Table(TableDescriptor),
}

// API Traits

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveObjectRequest {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseRequest {
    pub id: DatabaseId,
    pub name: String,
    pub owner_role_id: Option<RoleId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSchemaRequest {
    pub id: SchemaId,
    pub database_id: DatabaseId,
    pub name: String,
    pub owner_role_id: Option<RoleId>,
    pub managed_access: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableRequest {
    pub id: TableId,
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub name: String,
    pub columns: Vec<ColumnDescriptor>,
    pub constraints: Vec<TableConstraint>,
    #[serde(default)]
    pub view_query: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantPrivilegesRequest {
    pub id: GrantId,
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokePrivilegesRequest {
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableDescriptorChange {
    AddColumn {
        table_id: TableId,
        column: ColumnDescriptor,
    },
    RenameTable {
        table_id: TableId,
        new_name: String,
    },
    RenameColumn {
        table_id: TableId,
        old_name: String,
        new_name: String,
    },
    DropColumn {
        table_id: TableId,
        column_name: String,
    },
    AddIndex {
        table_id: TableId,
        index: IndexDescriptor,
    },
    DropIndex {
        table_id: TableId,
        index_name: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRoleRequest {
    pub id: PrincipalId,
    pub name: String,
    pub principal_type: PrincipalType,
    pub database_id: Option<DatabaseId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantPrivilegeRequest {
    pub id: GrantId,
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokePrivilegeRequest {
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddRoleMemberRequest {
    /// The role (itself a principal) that the member is being added to.
    pub role_principal_id: PrincipalId,
    /// The principal (user, service account, or nested role) being granted membership.
    pub member_id: PrincipalId,
}

/// A serializable point-in-time snapshot of catalog state, used for backups.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub databases: Vec<DatabaseDescriptor>,
    pub schemas: Vec<SchemaDescriptor>,
    pub tables: Vec<TableDescriptor>,
    pub principals: Vec<PrincipalDescriptor>,
    pub grants: Vec<GrantDescriptor>,
}
