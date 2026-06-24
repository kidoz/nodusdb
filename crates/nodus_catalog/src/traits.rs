//! Catalog reader / writer / store trait interfaces.
use crate::*;
use anyhow::Result;

pub trait CatalogReader: Send + Sync {
    fn get_database(&self, name: &str) -> Result<DatabaseDescriptor>;
    fn get_database_by_id(&self, id: DatabaseId) -> Result<DatabaseDescriptor>;
    fn get_schema(&self, database: &str, schema: &str) -> Result<SchemaDescriptor>;
    fn get_schema_by_id(&self, id: SchemaId) -> Result<SchemaDescriptor>;
    fn list_schemas(&self, database: &str) -> Result<Vec<SchemaDescriptor>>;
    fn resolve_object(&self, request: ResolveObjectRequest) -> Result<ObjectDescriptor>;
    fn get_table(&self, database: &str, schema: &str, table: &str) -> Result<TableDescriptor>;
    fn get_table_by_id(&self, id: TableId) -> Result<TableDescriptor>;
    fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableDescriptor>>;
    fn list_all_tables(&self, database: &str) -> Result<Vec<TableDescriptor>>;
    fn get_principal_by_name(&self, name: &str) -> Result<PrincipalDescriptor>;
    fn get_principal_by_id(&self, id: PrincipalId) -> Result<PrincipalDescriptor>;
    fn get_cluster_version(&self) -> Result<ClusterVersion>;
    fn get_grants_for_resource(&self, resource: ResourceRef) -> Result<Vec<GrantDescriptor>>;
    fn get_grant_by_id(&self, id: GrantId) -> Result<GrantDescriptor>;
    fn get_effective_roles(&self, principal: PrincipalId) -> Result<Vec<RoleId>>;
    /// Returns the principal itself plus the transitive closure of role
    /// principals it is a member of. Used by the authorization engine to match
    /// grants made either directly to a principal or to any of its roles.
    fn get_effective_principals(&self, principal: PrincipalId) -> Result<Vec<PrincipalId>>;

    /// Exports a serializable snapshot of catalog state for backups. Default is
    /// an empty snapshot.
    fn export_snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot::default()
    }
}

pub trait CatalogWriter: Send + Sync {
    fn create_database(&self, request: CreateDatabaseRequest) -> Result<DatabaseDescriptor>;
    fn create_schema(&self, request: CreateSchemaRequest) -> Result<SchemaDescriptor>;
    fn create_table(&self, request: CreateTableRequest) -> Result<TableDescriptor>;
    fn drop_table(&self, id: TableId) -> Result<()>;
    fn drop_schema(&self, id: SchemaId) -> Result<()>;
    fn grant_privileges(&self, request: GrantPrivilegesRequest) -> Result<GrantDescriptor>;
    fn revoke_privileges(&self, request: RevokePrivilegesRequest) -> Result<()>;
    fn update_table_descriptor(&self, change: TableDescriptorChange) -> Result<TableDescriptor>;
    fn create_role(&self, request: CreateRoleRequest) -> Result<PrincipalDescriptor>;
    fn grant_privilege(&self, request: GrantPrivilegeRequest) -> Result<GrantDescriptor>;
    fn revoke_privilege(&self, request: RevokePrivilegeRequest) -> Result<()>;
    fn add_role_member(&self, request: AddRoleMemberRequest) -> Result<()>;
    fn update_index_state(
        &self,
        table_id: TableId,
        index_id: IndexId,
        state: IndexState,
    ) -> Result<()>;
    fn import_snapshot(&self, snapshot: CatalogSnapshot) -> Result<()>;
}

// In-Memory MVP implementation

/// Durable backing for the catalog's serialized state. Defined here (rather than
/// using a `KvEngine` directly) so `nodus_catalog` stays free of a storage
/// dependency — `nodus_storage_api` already depends on this crate. The server
/// implements it over the same LSM store the KV data lives in, so the catalog
/// and user data share one durable mechanism and one recovery path.
pub trait CatalogStore: Send + Sync {
    /// Returns the most recently saved catalog state, if any.
    fn load(&self) -> Option<Vec<u8>>;
    /// Durably persists the catalog state.
    fn save(&self, bytes: &[u8]) -> Result<()>;
}
