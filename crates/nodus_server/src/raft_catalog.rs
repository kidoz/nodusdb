use anyhow::Result;
use nodus_catalog::*;
use nodus_raftstore::{NodusTypeConfig, ShardCommand};
use openraft::Raft;
use std::sync::Arc;

pub struct RaftCatalogWriter {
    pub local: Arc<dyn CatalogWriter>,
    pub raft: Raft<NodusTypeConfig>,
}

impl CatalogWriter for RaftCatalogWriter {
    fn create_database(&self, request: CreateDatabaseRequest) -> Result<DatabaseDescriptor> {
        let name = request.name.clone();
        let id = request.id;
        let cmd = ShardCommand::CreateDatabase(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("create_database client_write failed: {}", e);
            anyhow::bail!("create_database raft error: {}", e);
        }
        Ok(DatabaseDescriptor {
            id,
            name,
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: DescriptorState::Public,
            owner_role_id: None,
        })
    }

    fn create_schema(&self, request: CreateSchemaRequest) -> Result<SchemaDescriptor> {
        let name = request.name.clone();
        let id = request.id;
        let db_id = request.database_id;
        let cmd = ShardCommand::CreateSchema(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("create_schema client_write failed: {}", e);
            anyhow::bail!("create_schema raft error: {}", e);
        }
        Ok(SchemaDescriptor {
            id,
            database_id: db_id,
            name,
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: DescriptorState::Public,
            owner_role_id: None,
            managed_access: false,
            system_schema: false,
        })
    }

    fn create_table(&self, request: CreateTableRequest) -> Result<TableDescriptor> {
        let name = request.name.clone();
        let db_id = request.database_id;
        let sch_id = request.schema_id;
        let id = request.id;
        
        let cmd = ShardCommand::CreateTable(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("create_table client_write failed: {}", e);
            anyhow::bail!("create_table raft error: {}", e);
        }
        Ok(TableDescriptor {
            id,
            database_id: db_id,
            schema_id: sch_id,
            name,
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: DescriptorState::Public,
            columns: vec![],
            indexes: vec![],
        })
    }

    fn grant_privileges(&self, _request: GrantPrivilegesRequest) -> Result<GrantDescriptor> { Err(anyhow::anyhow!("Not implemented")) }
    fn revoke_privileges(&self, _request: RevokePrivilegesRequest) -> Result<()> { Ok(()) }
    fn update_table_descriptor(&self, _change: TableDescriptorChange) -> Result<TableDescriptor> { Err(anyhow::anyhow!("Not implemented")) }
    fn create_role(&self, _request: CreateRoleRequest) -> Result<PrincipalDescriptor> { Err(anyhow::anyhow!("Not implemented")) }
    fn grant_privilege(&self, _request: GrantPrivilegeRequest) -> Result<GrantDescriptor> { Err(anyhow::anyhow!("Not implemented")) }
    fn revoke_privilege(&self, _request: RevokePrivilegeRequest) -> Result<()> { Ok(()) }
    fn add_role_member(&self, _request: AddRoleMemberRequest) -> Result<()> { Ok(()) }
    fn update_index_state(&self, _table_id: TableId, _index_id: IndexId, _state: IndexState) -> Result<()> { Ok(()) }
    fn import_snapshot(&self, _snapshot: CatalogSnapshot) -> Result<()> { Ok(()) }
}
