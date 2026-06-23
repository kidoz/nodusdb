use anyhow::Result;
use nodus_catalog::*;
use nodus_raftstore::ShardCommand;
use std::sync::Arc;

use crate::raft_router::RaftRouter;

/// `CatalogWriter` that replicates catalog DDL/RBAC mutations through Raft (the
/// `shard-meta` group) before reading the applied descriptor back locally.
///
/// Write methods route through the async [`RaftRouter`], whose `submit` waits via
/// `blocking_recv` — so they MUST be invoked from a blocking context (e.g. inside
/// `tokio::task::spawn_blocking`), never directly on a runtime worker thread.
pub struct RaftCatalogWriter {
    pub local: Arc<dyn CatalogWriter>,
    pub reader: Arc<dyn CatalogReader>,
    pub router: RaftRouter,
    pub shard_id: String,
}

impl RaftCatalogWriter {
    /// Replicate a catalog command and surface a labelled error on failure.
    fn replicate(&self, op: &str, cmd: ShardCommand) -> Result<()> {
        self.router.submit(&self.shard_id, cmd).map_err(|e| {
            tracing::error!("{op} client_write failed: {e}");
            anyhow::anyhow!("{op} raft error: {e}")
        })
    }
}

impl CatalogWriter for RaftCatalogWriter {
    fn create_database(&self, request: CreateDatabaseRequest) -> Result<DatabaseDescriptor> {
        let id = request.id;
        self.replicate("create_database", ShardCommand::CreateDatabase(request))?;
        self.reader.get_database_by_id(id)
    }

    fn create_schema(&self, request: CreateSchemaRequest) -> Result<SchemaDescriptor> {
        let id = request.id;
        self.replicate("create_schema", ShardCommand::CreateSchema(request))?;
        self.reader.get_schema_by_id(id)
    }

    fn create_table(&self, request: CreateTableRequest) -> Result<TableDescriptor> {
        let id = request.id;
        self.replicate("create_table", ShardCommand::CreateTable(request))?;
        // `client_write` (openraft 0.9) resolves only after the entry is applied to
        // the leader's state machine, so the table is already visible here. Reading
        // the local catalog is correct because a non-leader would have failed the
        // replication above with ForwardToLeader.
        self.reader.get_table_by_id(id)
    }

    fn drop_table(&self, id: TableId) -> Result<()> {
        self.replicate("drop_table", ShardCommand::DropTable(id))
    }

    fn drop_schema(&self, id: SchemaId) -> Result<()> {
        self.replicate("drop_schema", ShardCommand::DropSchema(id))
    }

    fn grant_privileges(&self, request: GrantPrivilegesRequest) -> Result<GrantDescriptor> {
        let id = request.id;
        self.replicate("grant_privileges", ShardCommand::GrantPrivileges(request))?;
        self.reader.get_grant_by_id(id)
    }

    fn revoke_privileges(&self, request: RevokePrivilegesRequest) -> Result<()> {
        self.replicate("revoke_privileges", ShardCommand::RevokePrivileges(request))
    }

    fn update_table_descriptor(&self, change: TableDescriptorChange) -> Result<TableDescriptor> {
        let table_id = match &change {
            TableDescriptorChange::AddColumn { table_id, .. } => *table_id,
            TableDescriptorChange::RenameTable { table_id, .. } => *table_id,
            TableDescriptorChange::RenameColumn { table_id, .. } => *table_id,
            TableDescriptorChange::DropColumn { table_id, .. } => *table_id,
            TableDescriptorChange::AddIndex { table_id, .. } => *table_id,
        };
        self.replicate(
            "update_table_descriptor",
            ShardCommand::UpdateTableDescriptor(change),
        )?;
        self.reader.get_table_by_id(table_id)
    }

    fn create_role(&self, request: CreateRoleRequest) -> Result<PrincipalDescriptor> {
        let id = request.id;
        self.replicate("create_role", ShardCommand::CreateRole(request))?;
        self.reader.get_principal_by_id(id)
    }

    fn grant_privilege(&self, request: GrantPrivilegeRequest) -> Result<GrantDescriptor> {
        let id = request.id;
        self.replicate("grant_privilege", ShardCommand::GrantPrivilege(request))?;
        self.reader.get_grant_by_id(id)
    }

    fn revoke_privilege(&self, request: RevokePrivilegeRequest) -> Result<()> {
        self.replicate("revoke_privilege", ShardCommand::RevokePrivilege(request))
    }

    fn add_role_member(&self, request: AddRoleMemberRequest) -> Result<()> {
        self.replicate("add_role_member", ShardCommand::AddRoleMember(request))
    }

    fn update_index_state(
        &self,
        table_id: TableId,
        index_id: IndexId,
        state: IndexState,
    ) -> Result<()> {
        self.replicate(
            "update_index_state",
            ShardCommand::UpdateIndexState {
                table_id,
                index_id,
                state,
            },
        )
    }

    fn import_snapshot(&self, snapshot: CatalogSnapshot) -> Result<()> {
        // In a fully replicated setup, this should be sent via Raft.
        // For MVP, since restore operations are usually orchestrated manually or on a single node before clustering,
        // we write it directly to the local catalog.
        self.local.import_snapshot(snapshot)
    }
}
