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

    fn grant_privileges(&self, request: GrantPrivilegesRequest) -> Result<GrantDescriptor> {
        let cmd = ShardCommand::GrantPrivileges(request.clone());
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("grant_privileges client_write failed: {}", e);
            anyhow::bail!("grant_privileges raft error: {}", e);
        }
        Ok(GrantDescriptor {
            id: GrantId::new(),
            name: "grant".into(),
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            principal_id: request.principal_id,
            resource: request.resource,
            privilege: request.privilege,
            state: DescriptorState::Public,
        })
    }
    
    fn revoke_privileges(&self, request: RevokePrivilegesRequest) -> Result<()> {
        let cmd = ShardCommand::RevokePrivileges(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("revoke_privileges client_write failed: {}", e);
            anyhow::bail!("revoke_privileges raft error: {}", e);
        }
        Ok(())
    }
    fn update_table_descriptor(&self, change: TableDescriptorChange) -> Result<TableDescriptor> {
        let cmd = ShardCommand::UpdateTableDescriptor(change);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("update_table_descriptor client_write failed: {}", e);
            anyhow::bail!("update_table_descriptor raft error: {}", e);
        }
        
        // This is a dummy descriptor since we can't easily fetch it right now without CatalogReader.
        // The MemExecutor currently just drops the returned value anyway.
        Ok(TableDescriptor {
            id: TableId::new(),
            database_id: DatabaseId::new(),
            schema_id: SchemaId::new(),
            name: "dummy".into(),
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: DescriptorState::Public,
            columns: vec![],
            indexes: vec![],
        })
    }
    fn create_role(&self, request: CreateRoleRequest) -> Result<PrincipalDescriptor> {
        let name = request.name.clone();
        let cmd = ShardCommand::CreateRole(request.clone());
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("create_role client_write failed: {}", e);
            anyhow::bail!("create_role raft error: {}", e);
        }
        Ok(PrincipalDescriptor {
            id: PrincipalId::new(),
            name,
            principal_type: request.principal_type,
            database_id: request.database_id,
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: DescriptorState::Public,
        })
    }
    
    fn grant_privilege(&self, request: GrantPrivilegeRequest) -> Result<GrantDescriptor> {
        let cmd = ShardCommand::GrantPrivilege(request.clone());
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("grant_privilege client_write failed: {}", e);
            anyhow::bail!("grant_privilege raft error: {}", e);
        }
        Ok(GrantDescriptor {
            id: GrantId::new(),
            name: "grant".into(),
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            principal_id: request.principal_id,
            resource: request.resource,
            privilege: request.privilege,
            state: DescriptorState::Public,
        })
    }
    
    fn revoke_privilege(&self, request: RevokePrivilegeRequest) -> Result<()> {
        let cmd = ShardCommand::RevokePrivilege(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("revoke_privilege client_write failed: {}", e);
            anyhow::bail!("revoke_privilege raft error: {}", e);
        }
        Ok(())
    }
    
    fn add_role_member(&self, request: AddRoleMemberRequest) -> Result<()> {
        let cmd = ShardCommand::AddRoleMember(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("add_role_member client_write failed: {}", e);
            anyhow::bail!("add_role_member raft error: {}", e);
        }
        Ok(())
    }
    fn update_index_state(&self, table_id: TableId, index_id: IndexId, state: IndexState) -> Result<()> {
        let cmd = ShardCommand::UpdateIndexState { table_id, index_id, state };
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.raft.client_write(cmd).await
            })
        });
        if let Err(e) = res {
            tracing::error!("update_index_state client_write failed: {}", e);
            anyhow::bail!("update_index_state raft error: {}", e);
        }
        Ok(())
    }
    fn import_snapshot(&self, snapshot: CatalogSnapshot) -> Result<()> {
        // In a fully replicated setup, this should be sent via Raft.
        // For MVP, since restore operations are usually orchestrated manually or on a single node before clustering,
        // we write it directly to the local catalog.
        self.local.import_snapshot(snapshot)
    }
}
