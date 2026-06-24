//! In-memory `CatalogReader`/`CatalogWriter` implementation.
use crate::{
    AddRoleMemberRequest, CatalogReader, CatalogSnapshot, CatalogStore, CatalogWriter,
    ClusterVersion, CreateDatabaseRequest, CreateRoleRequest, CreateSchemaRequest,
    CreateTableRequest, DatabaseDescriptor, DatabaseId, DescriptorState, GrantDescriptor, GrantId,
    GrantPrivilegeRequest, GrantPrivilegesRequest, IndexId, IndexState, ObjectDescriptor,
    PrincipalDescriptor, PrincipalId, ResolveObjectRequest, ResourceRef, RevokePrivilegeRequest,
    RevokePrivilegesRequest, RoleId, RoleMembershipDescriptor, SchemaDescriptor, SchemaId,
    TableDescriptor, TableDescriptorChange, TableId,
};
use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use uuid::Uuid;

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
    /// Durable backing store; `None` for a purely in-memory catalog.
    store: Option<std::sync::Arc<dyn CatalogStore>>,
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
    /// Builds a catalog backed by `store`, loading any previously persisted
    /// state from it. All subsequent mutations are persisted through the same
    /// store, so the catalog shares the KV layer's durability and recovery.
    pub fn with_store(store: std::sync::Arc<dyn CatalogStore>) -> Self {
        if let Some(bytes) = store.load()
            && let Ok(state) = serde_json::from_slice::<MemoryCatalogState>(&bytes)
        {
            return Self {
                databases: RwLock::new(state.databases),
                schemas: RwLock::new(state.schemas.into_iter().collect()),
                tables: RwLock::new(state.tables.into_iter().collect()),
                principals: RwLock::new(state.principals),
                grants: RwLock::new(state.grants),
                roles: RwLock::new(state.roles),
                memberships: RwLock::new(state.memberships),
                catalog_version: RwLock::new(state.catalog_version),
                store: Some(store),
            };
        }
        let mut cat = Self::new();
        cat.store = Some(store);
        cat
    }

    /// Persists the full catalog state through the backing [`CatalogStore`]
    /// (no-op for an in-memory catalog). Durability is the store's concern — the
    /// server backs it with the crash-safe LSM, so this no longer maintains a
    /// separate on-disk file.
    pub fn save_to_disk(&self) -> Result<()> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let state = MemoryCatalogState {
            databases: self.databases.read().unwrap().clone(),
            schemas: self
                .schemas
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            tables: self
                .tables
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            principals: self.principals.read().unwrap().clone(),
            grants: self.grants.read().unwrap().clone(),
            roles: self.roles.read().unwrap().clone(),
            memberships: self.memberships.read().unwrap().clone(),
            catalog_version: *self.catalog_version.read().unwrap(),
        };
        store.save(&serde_json::to_vec(&state)?)
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
            store: None,
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

    fn get_database_by_id(&self, id: DatabaseId) -> Result<DatabaseDescriptor> {
        let guard = self.databases.read().unwrap();
        guard
            .values()
            .find(|d| d.id == id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Database ID {} not found", id))
    }

    fn get_schema(&self, database: &str, schema: &str) -> Result<SchemaDescriptor> {
        let db = self.get_database(database)?;
        let guard = self.schemas.read().unwrap();
        guard
            .get(&(db.id, schema.to_string()))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Schema {} not found", schema))
    }

    fn get_schema_by_id(&self, id: SchemaId) -> Result<SchemaDescriptor> {
        let guard = self.schemas.read().unwrap();
        guard
            .values()
            .find(|s| s.id == id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Schema ID {} not found", id))
    }

    fn list_schemas(&self, database: &str) -> Result<Vec<SchemaDescriptor>> {
        let db = self.get_database(database)?;
        let guard = self.schemas.read().unwrap();
        Ok(guard
            .values()
            .filter(|s| s.database_id == db.id && s.state == DescriptorState::Public)
            .cloned()
            .collect())
    }

    fn list_all_tables(&self, database: &str) -> Result<Vec<TableDescriptor>> {
        let db = self.get_database(database)?;
        let guard = self.tables.read().unwrap();
        Ok(guard
            .values()
            .filter(|t| t.database_id == db.id && t.state == DescriptorState::Public)
            .cloned()
            .collect())
    }

    #[allow(clippy::collapsible_if)]
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
        tracing::info!(
            "get_table CALLED for database={}, schema={}, table={}",
            database,
            schema,
            table
        );
        let db = self.get_database(database)?;
        let sch = self.get_schema(database, schema)?;
        let guard = self.tables.read().unwrap();
        if let Some(t) = guard.get(&(db.id, sch.id, table.to_string())) {
            Ok(t.clone())
        } else {
            tracing::error!(
                "get_table failed. Looking for: db={}, sch={}, table={}",
                db.id,
                sch.id,
                table
            );
            tracing::error!("Available tables:");
            for (k, _) in guard.iter() {
                tracing::error!("  {:?}", k);
            }
            anyhow::bail!("Table {} not found", table)
        }
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

    fn get_table_by_id(&self, id: TableId) -> Result<TableDescriptor> {
        let guard = self.tables.read().unwrap();
        guard
            .values()
            .find(|t| t.id == id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Table ID {} not found", id))
    }

    fn get_principal_by_name(&self, name: &str) -> Result<PrincipalDescriptor> {
        let guard = self.principals.read().unwrap();
        guard
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Principal {} not found", name))
    }

    fn get_principal_by_id(&self, id: PrincipalId) -> Result<PrincipalDescriptor> {
        let guard = self.principals.read().unwrap();
        guard
            .values()
            .find(|p| p.id == id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Principal ID {} not found", id))
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

    fn get_grant_by_id(&self, id: GrantId) -> Result<GrantDescriptor> {
        let guard = self.grants.read().unwrap();
        guard
            .iter()
            .find(|g| g.id == id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Grant ID {} not found", id))
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
            id: request.id,
            name: request.name.clone(),
            version: self.increment_version(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            owner_role_id: request.owner_role_id,
        };
        guard.insert(request.name.clone(), desc.clone());
        drop(guard);
        let _ = self.save_to_disk();
        Ok(desc)
    }

    fn create_schema(&self, request: CreateSchemaRequest) -> Result<SchemaDescriptor> {
        let mut guard = self.schemas.write().unwrap();
        let key = (request.database_id, request.name.clone());
        if guard.contains_key(&key) {
            anyhow::bail!("Schema {} already exists", request.name);
        }
        let desc = SchemaDescriptor {
            id: request.id,
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
        drop(guard);
        let _ = self.save_to_disk();
        Ok(desc)
    }

    fn create_table(&self, request: CreateTableRequest) -> Result<TableDescriptor> {
        let mut guard = self.tables.write().unwrap();
        let key = (request.database_id, request.schema_id, request.name.clone());
        if guard.contains_key(&key) {
            anyhow::bail!("Table {} already exists", request.name);
        }
        let desc = TableDescriptor {
            id: request.id,
            database_id: request.database_id,
            schema_id: request.schema_id,
            name: request.name.clone(),
            version: self.increment_version(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            columns: request.columns,
            indexes: vec![],
            constraints: request.constraints,
            view_query: request.view_query,
        };
        guard.insert(key, desc.clone());
        drop(guard);
        let _ = self.save_to_disk();
        Ok(desc)
    }

    fn drop_table(&self, id: TableId) -> Result<()> {
        let mut guard = self.tables.write().unwrap();
        let key = guard
            .iter()
            .find(|(_, t)| t.id == id)
            .map(|(k, _)| k.clone());
        if let Some(key) = key {
            guard.remove(&key);
            drop(guard);
            let _ = self.save_to_disk();
            Ok(())
        } else {
            anyhow::bail!("Table not found")
        }
    }

    fn drop_schema(&self, id: SchemaId) -> Result<()> {
        let mut guard = self.schemas.write().unwrap();
        let key = guard
            .iter()
            .find(|(_, s)| s.id == id)
            .map(|(k, _)| k.clone());
        if let Some(key) = key {
            guard.remove(&key);
            drop(guard);
            let _ = self.save_to_disk();
            Ok(())
        } else {
            anyhow::bail!("Schema not found")
        }
    }

    fn grant_privileges(&self, request: GrantPrivilegesRequest) -> Result<GrantDescriptor> {
        let mut guard = self.grants.write().unwrap();
        let desc = GrantDescriptor {
            id: request.id,
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
        drop(guard);
        let _ = self.save_to_disk();
        Ok(desc)
    }

    fn revoke_privileges(&self, request: RevokePrivilegesRequest) -> Result<()> {
        let mut guard = self.grants.write().unwrap();
        guard.retain(|g| {
            !(g.principal_id == request.principal_id
                && g.resource == request.resource
                && g.privilege.eq_ignore_ascii_case(&request.privilege))
        });
        self.increment_version();
        drop(guard);
        let _ = self.save_to_disk();
        Ok(())
    }

    fn update_table_descriptor(&self, change: TableDescriptorChange) -> Result<TableDescriptor> {
        let mut guard = self.tables.write().unwrap();
        let table_id = match &change {
            TableDescriptorChange::AddColumn { table_id, .. } => *table_id,
            TableDescriptorChange::RenameTable { table_id, .. } => *table_id,
            TableDescriptorChange::RenameColumn { table_id, .. } => *table_id,
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
            TableDescriptorChange::RenameColumn {
                old_name, new_name, ..
            } => {
                if let Some(col) = table.columns.iter_mut().find(|c| c.name == old_name) {
                    col.name = new_name;
                } else {
                    anyhow::bail!("Column {} not found", old_name);
                }
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
            id: request.id,
            name: request.name.clone(),
            version: self.increment_version(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            principal_type: request.principal_type,
            database_id: request.database_id,
        };
        guard.insert(request.name.clone(), desc.clone());
        drop(guard);
        let _ = self.save_to_disk();
        Ok(desc)
    }

    fn grant_privilege(&self, request: GrantPrivilegeRequest) -> Result<GrantDescriptor> {
        let mut guard = self.grants.write().unwrap();
        let desc = GrantDescriptor {
            id: request.id,
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
        drop(guard);
        let _ = self.save_to_disk();
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
        drop(guard);
        let _ = self.save_to_disk();
        Ok(())
    }

    fn add_role_member(&self, request: AddRoleMemberRequest) -> Result<()> {
        let edge = (request.role_principal_id, request.member_id);
        let mut guard = self.memberships.write().unwrap();
        if !guard.contains(&edge) {
            guard.push(edge);
        }
        self.increment_version();
        drop(guard);
        let _ = self.save_to_disk();
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
                    drop(tables);
                    let _ = self.save_to_disk();
                    return Ok(());
                }
            }
        }
        anyhow::bail!("Index {} not found", index_id);
    }

    fn import_snapshot(&self, snapshot: CatalogSnapshot) -> Result<()> {
        *self.databases.write().unwrap() = snapshot
            .databases
            .into_iter()
            .map(|d| (d.name.clone(), d))
            .collect();
        *self.schemas.write().unwrap() = snapshot
            .schemas
            .into_iter()
            .map(|s| ((s.database_id, s.name.clone()), s))
            .collect();
        *self.tables.write().unwrap() = snapshot
            .tables
            .into_iter()
            .map(|t| ((t.database_id, t.schema_id, t.name.clone()), t))
            .collect();
        // Do not overwrite principals, grants, or roles to preserve server-level auth state
        self.increment_version();
        let _ = self.save_to_disk();
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
                id: DatabaseId::new(),
                name: "testdb".into(),
                owner_role_id: None,
            })
            .unwrap();
        assert_eq!(db.name, "testdb");
        assert_eq!(db.version, 2);

        let sch = catalog
            .create_schema(CreateSchemaRequest {
                id: SchemaId::new(),
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
                id: TableId::new(),
                database_id: db.id,
                schema_id: sch.id,
                name: "users".into(),
                columns: vec![],
                constraints: vec![],
                view_query: None,
            })
            .unwrap();
        assert_eq!(tbl.name, "users");
        assert_eq!(tbl.version, 4);

        let fetched_tbl = catalog.get_table("testdb", "public", "users").unwrap();
        assert_eq!(fetched_tbl.id, tbl.id);
    }
}
