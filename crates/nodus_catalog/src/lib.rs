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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone)]
pub struct ObjectDescriptor {
    // A resolved object that can be DB, Schema, or Table.
}

// API Traits
use anyhow::Result;

pub struct ResolveObjectRequest {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub name: String,
}

pub struct CreateDatabaseRequest {
    pub name: String,
    pub owner_role_id: Option<RoleId>,
}

pub struct CreateSchemaRequest {
    pub database_id: DatabaseId,
    pub name: String,
    pub owner_role_id: Option<RoleId>,
    pub managed_access: bool,
}

pub struct CreateTableRequest {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub name: String,
    pub columns: Vec<ColumnDescriptor>,
}

pub struct GrantPrivilegesRequest {
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
}

pub struct RevokePrivilegesRequest {
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
}

pub struct TableDescriptorChange {
    pub table_id: TableId,
    // delta
}

pub struct CreateRoleRequest {
    pub name: String,
    pub principal_type: PrincipalType,
    pub database_id: Option<DatabaseId>,
}

pub struct GrantPrivilegeRequest {
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
}

pub struct RevokePrivilegeRequest {
    pub principal_id: PrincipalId,
    pub resource: ResourceRef,
    pub privilege: String,
}

pub trait CatalogReader: Send + Sync {
    fn get_database(&self, name: &str) -> Result<DatabaseDescriptor>;
    fn get_schema(&self, database: &str, schema: &str) -> Result<SchemaDescriptor>;
    fn resolve_object(&self, request: ResolveObjectRequest) -> Result<ObjectDescriptor>;
    fn get_table(&self, database: &str, schema: &str, table: &str) -> Result<TableDescriptor>;
    fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableDescriptor>>;
    fn get_cluster_version(&self) -> Result<ClusterVersion>;
    fn get_grants_for_resource(&self, resource: ResourceRef) -> Result<Vec<GrantDescriptor>>;
    fn get_effective_roles(&self, principal: PrincipalId) -> Result<Vec<RoleId>>;
}

pub trait CatalogWriter: Send + Sync {
    fn create_database(&self, request: CreateDatabaseRequest) -> Result<DatabaseDescriptor>;
    fn create_schema(&self, request: CreateSchemaRequest) -> Result<SchemaDescriptor>;
    fn create_table(&self, request: CreateTableRequest) -> Result<TableDescriptor>;
    fn grant_privileges(&self, request: GrantPrivilegesRequest) -> Result<GrantDescriptor>;
    fn revoke_privileges(&self, request: RevokePrivilegesRequest) -> Result<()>;
    fn update_table_descriptor(&self, change: TableDescriptorChange) -> Result<TableDescriptor>;
    fn create_role(&self, request: CreateRoleRequest) -> Result<PrincipalDescriptor>;
    fn grant_privilege(&self, request: GrantPrivilegeRequest) -> Result<GrantDescriptor>;
    fn revoke_privilege(&self, request: RevokePrivilegeRequest) -> Result<()>;
}

// In-Memory MVP implementation
use std::collections::HashMap;
use std::sync::RwLock;

#[allow(dead_code)]
pub struct MemoryCatalog {
    databases: RwLock<HashMap<String, DatabaseDescriptor>>,
    schemas: RwLock<HashMap<(DatabaseId, String), SchemaDescriptor>>,
    tables: RwLock<HashMap<(DatabaseId, SchemaId, String), TableDescriptor>>,
    principals: RwLock<HashMap<String, PrincipalDescriptor>>,
    grants: RwLock<Vec<GrantDescriptor>>,
    roles: RwLock<Vec<RoleMembershipDescriptor>>,
    catalog_version: RwLock<u64>,
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            databases: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            tables: RwLock::new(HashMap::new()),
            principals: RwLock::new(HashMap::new()),
            grants: RwLock::new(Vec::new()),
            roles: RwLock::new(Vec::new()),
            catalog_version: RwLock::new(1),
        }
    }

    fn increment_version(&self) -> u64 {
        let mut v = self.catalog_version.write().unwrap();
        *v += 1;
        *v
    }
}

impl CatalogReader for MemoryCatalog {
    fn get_database(&self, name: &str) -> Result<DatabaseDescriptor> {
        let guard = self.databases.read().unwrap();
        guard
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Database {} not found", name))
    }

    fn get_schema(&self, database: &str, schema: &str) -> Result<SchemaDescriptor> {
        let db = self.get_database(database)?;
        let guard = self.schemas.read().unwrap();
        guard
            .get(&(db.id, schema.to_string()))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Schema {} not found", schema))
    }

    fn resolve_object(&self, _request: ResolveObjectRequest) -> Result<ObjectDescriptor> {
        anyhow::bail!("resolve_object not implemented")
    }

    fn get_table(&self, database: &str, schema: &str, table: &str) -> Result<TableDescriptor> {
        let db = self.get_database(database)?;
        let sch = self.get_schema(database, schema)?;
        let guard = self.tables.read().unwrap();
        guard
            .get(&(db.id, sch.id, table.to_string()))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Table {} not found", table))
    }

    fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableDescriptor>> {
        let db = self.get_database(database)?;
        let sch = self.get_schema(database, schema)?;
        let guard = self.tables.read().unwrap();
        let mut res = Vec::new();
        for ((d_id, s_id, _), t) in guard.iter() {
            if *d_id == db.id && *s_id == sch.id {
                res.push(t.clone());
            }
        }
        Ok(res)
    }

    fn get_cluster_version(&self) -> Result<ClusterVersion> {
        Ok(ClusterVersion {
            id: Uuid::new_v4(),
            name: "default".into(),
            version: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            active_version: 1,
        })
    }

    fn get_grants_for_resource(&self, _resource: ResourceRef) -> Result<Vec<GrantDescriptor>> {
        Ok(vec![])
    }

    fn get_effective_roles(&self, _principal: PrincipalId) -> Result<Vec<RoleId>> {
        Ok(vec![])
    }
}

impl CatalogWriter for MemoryCatalog {
    fn create_database(&self, request: CreateDatabaseRequest) -> Result<DatabaseDescriptor> {
        let mut guard = self.databases.write().unwrap();
        if guard.contains_key(&request.name) {
            anyhow::bail!("Database {} already exists", request.name);
        }
        let desc = DatabaseDescriptor {
            id: DatabaseId::new(),
            name: request.name.clone(),
            version: self.increment_version(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            owner_role_id: request.owner_role_id,
        };
        guard.insert(request.name.clone(), desc.clone());
        Ok(desc)
    }

    fn create_schema(&self, request: CreateSchemaRequest) -> Result<SchemaDescriptor> {
        let mut guard = self.schemas.write().unwrap();
        let key = (request.database_id, request.name.clone());
        if guard.contains_key(&key) {
            anyhow::bail!("Schema {} already exists", request.name);
        }
        let desc = SchemaDescriptor {
            id: SchemaId::new(),
            database_id: request.database_id,
            name: request.name.clone(),
            version: self.increment_version(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            owner_role_id: request.owner_role_id,
            managed_access: request.managed_access,
            system_schema: false,
        };
        guard.insert(key, desc.clone());
        Ok(desc)
    }

    fn create_table(&self, request: CreateTableRequest) -> Result<TableDescriptor> {
        let mut guard = self.tables.write().unwrap();
        let key = (request.database_id, request.schema_id, request.name.clone());
        if guard.contains_key(&key) {
            anyhow::bail!("Table {} already exists", request.name);
        }
        let desc = TableDescriptor {
            id: TableId::new(),
            database_id: request.database_id,
            schema_id: request.schema_id,
            name: request.name.clone(),
            version: self.increment_version(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            columns: request.columns,
            indexes: vec![],
        };
        guard.insert(key, desc.clone());
        Ok(desc)
    }

    fn grant_privileges(&self, _request: GrantPrivilegesRequest) -> Result<GrantDescriptor> {
        anyhow::bail!("Not implemented")
    }

    fn revoke_privileges(&self, _request: RevokePrivilegesRequest) -> Result<()> {
        anyhow::bail!("Not implemented")
    }

    fn update_table_descriptor(&self, _change: TableDescriptorChange) -> Result<TableDescriptor> {
        anyhow::bail!("Not implemented")
    }

    fn create_role(&self, request: CreateRoleRequest) -> Result<PrincipalDescriptor> {
        let mut guard = self.principals.write().unwrap();
        if guard.contains_key(&request.name) {
            anyhow::bail!("Principal {} already exists", request.name);
        }
        let desc = PrincipalDescriptor {
            id: PrincipalId::new(),
            name: request.name.clone(),
            version: self.increment_version(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            principal_type: request.principal_type,
            database_id: request.database_id,
        };
        guard.insert(request.name.clone(), desc.clone());
        Ok(desc)
    }

    fn grant_privilege(&self, request: GrantPrivilegeRequest) -> Result<GrantDescriptor> {
        let mut guard = self.grants.write().unwrap();
        let desc = GrantDescriptor {
            id: GrantId::new(),
            name: "grant".into(),
            version: self.increment_version(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            principal_id: request.principal_id,
            resource: request.resource,
            privilege: request.privilege,
        };
        guard.push(desc.clone());
        Ok(desc)
    }

    fn revoke_privilege(&self, _request: RevokePrivilegeRequest) -> Result<()> {
        let _v = self.increment_version();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_database_schema_table() {
        let catalog = MemoryCatalog::new();

        let db = catalog
            .create_database(CreateDatabaseRequest {
                name: "testdb".into(),
                owner_role_id: None,
            })
            .unwrap();
        assert_eq!(db.name, "testdb");
        assert_eq!(db.version, 2);

        let sch = catalog
            .create_schema(CreateSchemaRequest {
                database_id: db.id,
                name: "public".into(),
                owner_role_id: None,
                managed_access: false,
            })
            .unwrap();
        assert_eq!(sch.name, "public");
        assert_eq!(sch.version, 3);

        let tbl = catalog
            .create_table(CreateTableRequest {
                database_id: db.id,
                schema_id: sch.id,
                name: "users".into(),
                columns: vec![],
            })
            .unwrap();
        assert_eq!(tbl.name, "users");
        assert_eq!(tbl.version, 4);

        let fetched_tbl = catalog.get_table("testdb", "public", "users").unwrap();
        assert_eq!(fetched_tbl.id, tbl.id);
    }
}
