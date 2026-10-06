//! Catalog operation requests and object/snapshot helper types.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum ObjectDescriptor {
    Database(Box<DatabaseDescriptor>),
    Schema(Box<SchemaDescriptor>),
    Table(Box<TableDescriptor>),
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
    /// For a materialized view, the query its rows are computed from.
    #[serde(default)]
    pub materialized_query: Option<String>,
    /// The tables the new one inherits from (`INHERITS`).
    #[serde(default)]
    pub parents: Vec<TableId>,
    /// A partitioned table's `PARTITION BY`, as canonical text.
    #[serde(default)]
    pub partition_by: Option<String>,
    /// A partition's bound, as canonical text.
    #[serde(default)]
    pub partition_bound: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantPrivilegesRequest {
    pub id: GrantId,
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
    /// `WITH GRANT OPTION`; defaulted so older requests decode.
    #[serde(default)]
    pub grantable: bool,
    #[serde(default)]
    pub grantor: Option<PrincipalId>,
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
    /// Changes a column's declared type. Only catalog metadata is updated;
    /// existing stored values are left as-is (a best-effort `ALTER ... SET DATA
    /// TYPE`), with the type system coercing on subsequent reads/writes.
    AlterColumnType {
        table_id: TableId,
        column_name: String,
        data_type: String,
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
    /// `COMMENT ON`: sets the comment on the table, or on its column
    /// `column`; `None` removes it.
    SetComment {
        table_id: TableId,
        column: Option<String>,
        comment: Option<String>,
    },
    /// Replaces the column with the same id (its default, nullability).
    ReplaceColumn {
        table_id: TableId,
        column: ColumnDescriptor,
    },
    /// Adds a CHECK or FOREIGN KEY constraint.
    AddConstraint {
        table_id: TableId,
        constraint: TableConstraint,
    },
    /// Removes the CHECK or FOREIGN KEY constraint named `name` (see
    /// [`crate::TableConstraint::effective_name`]).
    DropConstraint {
        table_id: TableId,
        name: String,
    },
    /// Moves the relation to another schema of its database (`SET SCHEMA`).
    SetSchema {
        table_id: TableId,
        schema_id: SchemaId,
    },
    /// Replaces a view's (or materialized view's) stored query, as when a
    /// relation it reads moves or is renamed.
    SetViewQuery {
        table_id: TableId,
        query: String,
    },
    /// Replaces the table's inheritance (`ALTER TABLE ... INHERIT` /
    /// `NO INHERIT`).
    SetParents {
        table_id: TableId,
        parents: Vec<TableId>,
    },
    /// Replaces the table's partition bound (`ATTACH` / `DETACH
    /// PARTITION`).
    SetPartitionBound {
        table_id: TableId,
        bound: Option<String>,
    },
    /// Sets the relation's owner (`OWNER TO`).
    SetOwner {
        table_id: TableId,
        owner_role_id: Option<RoleId>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRoleRequest {
    pub id: PrincipalId,
    pub name: String,
    pub principal_type: PrincipalType,
    pub database_id: Option<DatabaseId>,
    /// The role's attributes; defaulted so older requests decode.
    #[serde(default)]
    pub attributes: crate::RoleAttributes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantPrivilegeRequest {
    pub id: GrantId,
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
    /// `WITH GRANT OPTION`; defaulted so older requests decode.
    #[serde(default)]
    pub grantable: bool,
    #[serde(default)]
    pub grantor: Option<PrincipalId>,
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
    /// `WITH ADMIN OPTION`; defaulted so older requests decode.
    #[serde(default)]
    pub admin_option: bool,
    /// The role that granted the membership (`pg_auth_members.grantor`);
    /// defaulted so older requests decode.
    #[serde(default)]
    pub grantor: Option<PrincipalId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveRoleMemberRequest {
    /// The role the member is being removed from.
    pub role_principal_id: PrincipalId,
    /// The principal being removed.
    pub member_id: PrincipalId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatePrincipalRequest {
    pub principal_id: PrincipalId,
    /// The attributes that replace the principal's.
    pub attributes: PrincipalDescriptorAttributes,
    /// A new name for `ALTER ROLE ... RENAME TO`.
    #[serde(default)]
    pub new_name: Option<String>,
}

/// The attributes of an [`UpdatePrincipalRequest`].
pub type PrincipalDescriptorAttributes = crate::RoleAttributes;

/// A serializable point-in-time snapshot of catalog state, used for backups.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub databases: Vec<DatabaseDescriptor>,
    pub schemas: Vec<SchemaDescriptor>,
    pub tables: Vec<TableDescriptor>,
    pub principals: Vec<PrincipalDescriptor>,
    pub grants: Vec<GrantDescriptor>,
}
