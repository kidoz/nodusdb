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

#[derive(Debug, Clone)]
pub enum ObjectDescriptor {
    Database(DatabaseDescriptor),
    Schema(SchemaDescriptor),
    Table(TableDescriptor),
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

#[derive(Debug, Clone)]
pub enum TableDescriptorChange {
    AddColumn { table_id: TableId, column: ColumnDescriptor },
    RenameTable { table_id: TableId, new_name: String },
    DropColumn { table_id: TableId, column_name: String },
    AddIndex { table_id: TableId, index: IndexDescriptor },
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

pub trait CatalogReader: Send + Sync {
    fn get_database(&self, name: &str) -> Result<DatabaseDescriptor>;
    fn get_schema(&self, database: &str, schema: &str) -> Result<SchemaDescriptor>;
    fn resolve_object(&self, request: ResolveObjectRequest) -> Result<ObjectDescriptor>;
    fn get_table(&self, database: &str, schema: &str, table: &str) -> Result<TableDescriptor>;
    fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableDescriptor>>;
    fn get_cluster_version(&self) -> Result<ClusterVersion>;
    fn get_grants_for_resource(&self, resource: ResourceRef) -> Result<Vec<GrantDescriptor>>;
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
    /// (role_principal_id, member_id) edges of the role-membership graph.
    memberships: RwLock<Vec<(PrincipalId, PrincipalId)>>,
    catalog_version: RwLock<u64>,
    path: Option<std::path::PathBuf>,
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Deserialize)]
struct MemoryCatalogState {
    databases: HashMap<String, DatabaseDescriptor>,
    schemas: Vec<((DatabaseId, String), SchemaDescriptor)>,
    tables: Vec<((DatabaseId, SchemaId, String), TableDescriptor)>,
    principals: HashMap<String, PrincipalDescriptor>,
    grants: Vec<GrantDescriptor>,
    roles: Vec<RoleMembershipDescriptor>,
    memberships: Vec<(PrincipalId, PrincipalId)>,
    catalog_version: u64,
}

impl MemoryCatalog {
    pub fn load_from_disk(path: std::path::PathBuf) -> Result<Self> {
        if path.exists() {
            let data = std::fs::read_to_string(&path)?;
            let state: MemoryCatalogState = serde_json::from_str(&data)?;
            let schemas_map = state.schemas.into_iter().collect();
            let tables_map = state.tables.into_iter().collect();
            Ok(Self {
                databases: RwLock::new(state.databases),
                schemas: RwLock::new(schemas_map),
                tables: RwLock::new(tables_map),
                principals: RwLock::new(state.principals),
                grants: RwLock::new(state.grants),
                roles: RwLock::new(state.roles),
                memberships: RwLock::new(state.memberships),
                catalog_version: RwLock::new(state.catalog_version),
                path: Some(path),
            })
        } else {
            let mut cat = Self::new();
            cat.path = Some(path);
            Ok(cat)
        }
    }

    pub fn save_to_disk(&self) -> Result<()> {
        if let Some(path) = &self.path {
            let schemas_vec: Vec<_> = self
                .schemas
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let tables_vec: Vec<_> = self
                .tables
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            let state = MemoryCatalogState {
                databases: self.databases.read().unwrap().clone(),
                schemas: schemas_vec,
                tables: tables_vec,
                principals: self.principals.read().unwrap().clone(),
                grants: self.grants.read().unwrap().clone(),
                roles: self.roles.read().unwrap().clone(),
                memberships: self.memberships.read().unwrap().clone(),
                catalog_version: *self.catalog_version.read().unwrap(),
            };

            let data = serde_json::to_string_pretty(&state)?;
            std::fs::write(path, data)?;
        }
        Ok(())
    }

    pub fn new() -> Self {
        Self {
            databases: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            tables: RwLock::new(HashMap::new()),
            principals: RwLock::new(HashMap::new()),
            grants: RwLock::new(Vec::new()),
            roles: RwLock::new(Vec::new()),
            memberships: RwLock::new(Vec::new()),
            catalog_version: RwLock::new(1),
            path: None,
        }
    }

    fn increment_version(&self) -> u64 {
        let mut v = self.catalog_version.write().unwrap();
        *v += 1;
        let val = *v;
        drop(v);
        let _ = self.save_to_disk();
        val
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

    fn resolve_object(&self, request: ResolveObjectRequest) -> Result<ObjectDescriptor> {
        // Table lookup: DB, Schema, and Name provided
        if let (Some(db_name), Some(schema_name)) = (&request.database, &request.schema) {
            if let Ok(table) = self.get_table(db_name, schema_name, &request.name) {
                return Ok(ObjectDescriptor::Table(table));
            }
        }
        
        // Schema lookup: DB and Name provided
        if let Some(db_name) = &request.database {
            if let Ok(schema) = self.get_schema(db_name, &request.name) {
                return Ok(ObjectDescriptor::Schema(schema));
            }
        }

        // Database lookup
        if let Ok(db) = self.get_database(&request.name) {
            return Ok(ObjectDescriptor::Database(db));
        }

        anyhow::bail!("Object not found: {:?}", request.name)
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

    fn get_grants_for_resource(&self, resource: ResourceRef) -> Result<Vec<GrantDescriptor>> {
        let guard = self.grants.read().unwrap();
        Ok(guard
            .iter()
            .filter(|g| g.state == DescriptorState::Public && g.resource == resource)
            .cloned()
            .collect())
    }

    fn get_effective_roles(&self, principal: PrincipalId) -> Result<Vec<RoleId>> {
        let guard = self.roles.read().unwrap();
        Ok(guard
            .iter()
            .filter(|m| m.member_id == principal && m.state == DescriptorState::Public)
            .map(|m| m.role_id)
            .collect())
    }

    fn get_effective_principals(&self, principal: PrincipalId) -> Result<Vec<PrincipalId>> {
        let edges = self.memberships.read().unwrap();
        // Breadth-first transitive closure over the role-membership graph.
        let mut result = vec![principal];
        let mut frontier = vec![principal];
        while let Some(current) = frontier.pop() {
            for (role_principal_id, member_id) in edges.iter() {
                if *member_id == current && !result.contains(role_principal_id) {
                    result.push(*role_principal_id);
                    frontier.push(*role_principal_id);
                }
            }
        }
        Ok(result)
    }

    fn export_snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot {
            databases: self.databases.read().unwrap().values().cloned().collect(),
            schemas: self.schemas.read().unwrap().values().cloned().collect(),
            tables: self.tables.read().unwrap().values().cloned().collect(),
            principals: self.principals.read().unwrap().values().cloned().collect(),
            grants: self.grants.read().unwrap().clone(),
        }
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

    fn grant_privileges(&self, request: GrantPrivilegesRequest) -> Result<GrantDescriptor> {
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

    fn revoke_privileges(&self, _request: RevokePrivilegesRequest) -> Result<()> {
        anyhow::bail!("Not implemented")
    }

    fn update_table_descriptor(&self, change: TableDescriptorChange) -> Result<TableDescriptor> {
        let mut guard = self.tables.write().unwrap();
        let table_id = match &change {
            TableDescriptorChange::AddColumn { table_id, .. } => *table_id,
            TableDescriptorChange::RenameTable { table_id, .. } => *table_id,
            TableDescriptorChange::DropColumn { table_id, .. } => *table_id,
            TableDescriptorChange::AddIndex { table_id, .. } => *table_id,
        };
        
        let mut target_key = None;
        for (k, v) in guard.iter() {
            if v.id == table_id {
                target_key = Some(k.clone());
                break;
            }
        }
        
        let key = target_key.ok_or_else(|| anyhow::anyhow!("Table not found"))?;
        let mut table = guard.remove(&key).unwrap();
        
        table.version += 1;
        table.updated_at = Utc::now();
        
        let mut new_key = key.clone();
        
        match change {
            TableDescriptorChange::AddColumn { column, .. } => {
                table.columns.push(column);
            }
            TableDescriptorChange::RenameTable { new_name, .. } => {
                table.name = new_name.clone();
                new_key.2 = new_name;
            }
            TableDescriptorChange::DropColumn { column_name, .. } => {
                table.columns.retain(|c| c.name != column_name);
            }
            TableDescriptorChange::AddIndex { index, .. } => {
                table.indexes.push(index);
            }
        }
        
        let out = table.clone();
        guard.insert(new_key, table);
        Ok(out)
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

    fn revoke_privilege(&self, request: RevokePrivilegeRequest) -> Result<()> {
        let mut guard = self.grants.write().unwrap();
        guard.retain(|g| {
            !(g.principal_id == request.principal_id
                && g.resource == request.resource
                && g.privilege.eq_ignore_ascii_case(&request.privilege))
        });
        self.increment_version();
        Ok(())
    }

    fn add_role_member(&self, request: AddRoleMemberRequest) -> Result<()> {
        let edge = (request.role_principal_id, request.member_id);
        let mut guard = self.memberships.write().unwrap();
        if !guard.contains(&edge) {
            guard.push(edge);
        }
        self.increment_version();
        Ok(())
    }

    fn update_index_state(
        &self,
        _table_id: TableId,
        index_id: IndexId,
        state: IndexState,
    ) -> Result<()> {
        let mut tables = self.tables.write().unwrap();
        for (_, tbl) in tables.iter_mut() {
            for idx in tbl.indexes.iter_mut() {
                if idx.id == index_id {
                    idx.index_state = state;
                    self.increment_version();
                    return Ok(());
                }
            }
        }
        anyhow::bail!("Index {} not found", index_id);
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
