use anyhow::Result;
use nodus_catalog::*;
use nodus_raftstore::{NodusTypeConfig, ShardCommand};
use openraft::Raft;
use std::sync::Arc;

pub struct RaftCatalogWriter {
    pub local: Arc<dyn CatalogWriter>,
    pub reader: Arc<dyn CatalogReader>,
    pub raft_state: nodus_raftstore::server::RaftState,
}

impl RaftCatalogWriter {
    async fn get_raft(&self) -> Result<Raft<NodusTypeConfig>> {
        let rafts = self.raft_state.rafts.read().await;
        rafts
            .get("shard-meta")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Meta shard raft not found"))
    }
}

impl CatalogWriter for RaftCatalogWriter {
    fn create_database(&self, request: CreateDatabaseRequest) -> Result<DatabaseDescriptor> {
        let _name = request.name.clone();
        let id = request.id;
        let cmd = ShardCommand::CreateDatabase(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("create_database client_write failed: {}", e);
            anyhow::bail!("create_database raft error: {}", e);
        }
        self.reader.get_database_by_id(id)
    }

    fn create_schema(&self, request: CreateSchemaRequest) -> Result<SchemaDescriptor> {
        let _name = request.name.clone();
        let id = request.id;
        let _db_id = request.database_id;
        let cmd = ShardCommand::CreateSchema(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("create_schema client_write failed: {}", e);
            anyhow::bail!("create_schema raft error: {}", e);
        }
        self.reader.get_schema_by_id(id)
    }

    fn create_table(&self, request: CreateTableRequest) -> Result<TableDescriptor> {
        let _name = request.name.clone();
        let _db_id = request.database_id;
        let _sch_id = request.schema_id;
        let id = request.id;

        let cmd = ShardCommand::CreateTable(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("create_table client_write failed: {}", e);
            anyhow::bail!("create_table raft error: {}", e);
        }

        // `client_write` (openraft 0.9) resolves only after the entry is applied to
        // the leader's state machine, so the table is already visible here. Reading
        // the local catalog is correct because a non-leader would have failed the
        // `client_write` above with ForwardToLeader.
        self.reader.get_table_by_id(id)
    }

    fn drop_table(&self, id: TableId) -> Result<()> {
        let cmd = ShardCommand::DropTable(id);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("drop_table client_write failed: {}", e);
            anyhow::bail!("drop_table raft error: {}", e);
        }
        Ok(())
    }

    fn drop_schema(&self, id: SchemaId) -> Result<()> {
        let cmd = ShardCommand::DropSchema(id);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("drop_schema client_write failed: {}", e);
            anyhow::bail!("drop_schema raft error: {}", e);
        }
        Ok(())
    }

    fn grant_privileges(&self, request: GrantPrivilegesRequest) -> Result<GrantDescriptor> {
        let id = request.id;
        let cmd = ShardCommand::GrantPrivileges(request.clone());
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("grant_privileges client_write failed: {}", e);
            anyhow::bail!("grant_privileges raft error: {}", e);
        }
        self.reader.get_grant_by_id(id)
    }

    fn revoke_privileges(&self, request: RevokePrivilegesRequest) -> Result<()> {
        let cmd = ShardCommand::RevokePrivileges(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("revoke_privileges client_write failed: {}", e);
            anyhow::bail!("revoke_privileges raft error: {}", e);
        }
        Ok(())
    }
    fn update_table_descriptor(&self, change: TableDescriptorChange) -> Result<TableDescriptor> {
        let table_id = match &change {
            TableDescriptorChange::AddColumn { table_id, .. } => *table_id,
            TableDescriptorChange::RenameTable { table_id, .. } => *table_id,
            TableDescriptorChange::RenameColumn { table_id, .. } => *table_id,
            TableDescriptorChange::DropColumn { table_id, .. } => *table_id,
            TableDescriptorChange::AddIndex { table_id, .. } => *table_id,
        };
        let cmd = ShardCommand::UpdateTableDescriptor(change);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("update_table_descriptor client_write failed: {}", e);
            anyhow::bail!("update_table_descriptor raft error: {}", e);
        }
        self.reader.get_table_by_id(table_id)
    }
    fn create_role(&self, request: CreateRoleRequest) -> Result<PrincipalDescriptor> {
        let _name = request.name.clone();
        let id = request.id;
        let cmd = ShardCommand::CreateRole(request.clone());
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("create_role client_write failed: {}", e);
            anyhow::bail!("create_role raft error: {}", e);
        }
        self.reader.get_principal_by_id(id)
    }

    fn grant_privilege(&self, request: GrantPrivilegeRequest) -> Result<GrantDescriptor> {
        let id = request.id;
        let cmd = ShardCommand::GrantPrivilege(request.clone());
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("grant_privilege client_write failed: {}", e);
            anyhow::bail!("grant_privilege raft error: {}", e);
        }
        self.reader.get_grant_by_id(id)
    }

    fn revoke_privilege(&self, request: RevokePrivilegeRequest) -> Result<()> {
        let cmd = ShardCommand::RevokePrivilege(request);
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
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
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("add_role_member client_write failed: {}", e);
            anyhow::bail!("add_role_member raft error: {}", e);
        }
        Ok(())
    }
    fn update_index_state(
        &self,
        table_id: TableId,
        index_id: IndexId,
        state: IndexState,
    ) -> Result<()> {
        let cmd = ShardCommand::UpdateIndexState {
            table_id,
            index_id,
            state,
        };
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
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
