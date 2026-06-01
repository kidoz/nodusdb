use anyhow::Result;
use nodus_catalog::{IndexId, TableId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    CreateTable { name: String }, // Simplified for MVP
    Insert { table_name: String },
    Project,
    Filter,
    Update { table_name: String },
    Delete { table_name: String },
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalPlan {
    LocalInsert {
        table_id: TableId,
    },
    LocalPointGet {
        table_id: TableId,
    },
    LocalIndexScan {
        table_id: TableId,
        index_id: IndexId,
    },
    LocalUpdate {
        table_id: TableId,
    },
    LocalDelete {
        table_id: TableId,
    },
    DistributedRoute {
        plan: Box<PhysicalPlan>,
    },
}

pub struct ExecutionContext {
    pub session_id: String,
    pub authz_catalog_version: u64,
}

pub trait Executor: Send + Sync {
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<()>;
    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<()>;
}

// In-Memory MVP implementation
pub struct MemExecutor;

impl Default for MemExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl MemExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Executor for MemExecutor {
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<()> {
        // MVP: Just print what would be done
        println!(
            "Executing LogicalPlan: {:?} for session {}",
            plan, ctx.session_id
        );
        Ok(())
    }

    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<()> {
        println!(
            "Executing PhysicalPlan: {:?} for session {}",
            plan, ctx.session_id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_scaffold() {
        let exec = MemExecutor::new();
        let ctx = ExecutionContext {
            session_id: "test".to_string(),
            authz_catalog_version: 1,
        };
        exec.execute_logical(&ctx, LogicalPlan::Begin).unwrap();
        exec.execute_physical(
            &ctx,
            PhysicalPlan::LocalPointGet {
                table_id: TableId::new(),
            },
        )
        .unwrap();
    }
}
