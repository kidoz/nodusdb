use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use nodus_authz::AuthzEngine;
use nodus_catalog::{
    CatalogReader, CatalogWriter, ColumnDescriptor, CreateTableRequest, DescriptorState, IndexId,
    TableId,
};
use nodus_storage_api::{KvEngine, Timestamp, TxnId};
use nodus_txn::TxnManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    CreateTable {
        name: String,
    }, // Simplified for MVP
    Insert {
        table_name: String,
        id: String,
        name_val: String,
    },
    Project,
    Filter,
    Update {
        table_name: String,
    },
    Delete {
        table_name: String,
    },
    Begin,
    Commit,
    Rollback,
    SelectById {
        table_name: String,
        id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalPlan {
    LocalInsert {
        table_id: TableId,
        id: String,
        name_val: String,
    },
    LocalPointGet {
        table_id: TableId,
        id: String,
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

#[derive(Debug)]
pub struct Row {
    pub columns: Vec<String>,
}

pub trait Executor: Send + Sync {
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<Vec<Row>>;
    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>>;
}

// MVP implementation mapping to required interfaces
#[allow(dead_code)]
pub struct MemExecutor {
    catalog_reader: Arc<dyn CatalogReader>,
    catalog_writer: Arc<dyn CatalogWriter>,
    authz: Arc<dyn AuthzEngine>,
    kv: Arc<dyn KvEngine>,
    txn: Arc<dyn TxnManager>,
    // Hack for MVP: track active transaction in memory for the single session
    active_txn: std::sync::RwLock<Option<(TxnId, Timestamp)>>,
}

impl MemExecutor {
    pub fn new(
        catalog_reader: Arc<dyn CatalogReader>,
        catalog_writer: Arc<dyn CatalogWriter>,
        authz: Arc<dyn AuthzEngine>,
        kv: Arc<dyn KvEngine>,
        txn: Arc<dyn TxnManager>,
    ) -> Self {
        // Bootstrapping the default database and schema
        let _db = catalog_writer.create_database(nodus_catalog::CreateDatabaseRequest {
            name: "default".into(),
            owner_role_id: None,
        });
        if let Ok(db) = catalog_reader.get_database("default") {
            let _sch = catalog_writer.create_schema(nodus_catalog::CreateSchemaRequest {
                database_id: db.id,
                name: "public".into(),
                owner_role_id: None,
                managed_access: false,
            });
        }

        Self {
            catalog_reader,
            catalog_writer,
            authz,
            kv,
            txn,
            active_txn: std::sync::RwLock::new(None),
        }
    }
}

// Temporary default constructor so we don't break existing setups
impl Default for MemExecutor {
    fn default() -> Self {
        let cat = Arc::new(nodus_catalog::MemoryCatalog::new());
        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let txn = Arc::new(nodus_txn::MemTxnManager::new());
        let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(cat.clone()));

        Self::new(cat.clone(), cat, authz, kv, txn)
    }
}

impl Executor for MemExecutor {
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<Vec<Row>> {
        println!(
            "Executing LogicalPlan: {:?} for session {}",
            plan, ctx.session_id
        );
        match plan {
            LogicalPlan::CreateTable { name } => {
                let db = self.catalog_reader.get_database("default")?;
                let sch = self.catalog_reader.get_schema("default", "public")?;
                self.catalog_writer.create_table(CreateTableRequest {
                    database_id: db.id,
                    schema_id: sch.id,
                    name: name.clone(),
                    columns: vec![
                        ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name: "id".into(),
                            version: 1,
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            state: DescriptorState::Public,
                            data_type: "UUID".into(),
                            nullable: false,
                        },
                        ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name: "name".into(),
                            version: 1,
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            state: DescriptorState::Public,
                            data_type: "TEXT".into(),
                            nullable: false,
                        },
                    ],
                })?;
                Ok(vec![Row {
                    columns: vec!["CREATE TABLE".into()],
                }])
            }
            LogicalPlan::Insert {
                table_name,
                id,
                name_val,
            } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
                self.execute_physical(
                    ctx,
                    PhysicalPlan::LocalInsert {
                        table_id: tbl.id,
                        id,
                        name_val,
                    },
                )
            }
            LogicalPlan::SelectById { table_name, id } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
                self.execute_physical(
                    ctx,
                    PhysicalPlan::LocalPointGet {
                        table_id: tbl.id,
                        id,
                    },
                )
            }
            LogicalPlan::Begin => {
                let txn_record = self.txn.begin_txn()?;
                *self.active_txn.write().unwrap() = Some((txn_record.txn_id, txn_record.read_ts));
                Ok(vec![Row {
                    columns: vec!["BEGIN".into()],
                }])
            }
            LogicalPlan::Commit => {
                if let Some((txn_id, _)) = *self.active_txn.read().unwrap() {
                    let commit_ts = self.txn.commit_txn(txn_id)?;
                    self.kv.commit(txn_id, commit_ts)?;
                }
                *self.active_txn.write().unwrap() = None;
                Ok(vec![Row {
                    columns: vec!["COMMIT".into()],
                }])
            }
            LogicalPlan::Rollback => {
                if let Some((txn_id, _)) = *self.active_txn.read().unwrap() {
                    self.txn.abort_txn(txn_id)?;
                    self.kv.abort(txn_id)?;
                }
                *self.active_txn.write().unwrap() = None;
                Ok(vec![Row {
                    columns: vec!["ROLLBACK".into()],
                }])
            }
            _ => Ok(vec![]),
        }
    }

    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>> {
        println!(
            "Executing PhysicalPlan: {:?} for session {}",
            plan, ctx.session_id
        );
        match plan {
            PhysicalPlan::LocalInsert {
                table_id,
                id,
                name_val,
            } => {
                // Determine active txn or auto-commit
                let mut is_auto = false;
                let active = *self.active_txn.read().unwrap();
                let txn_id = if let Some((tid, _)) = active {
                    tid
                } else {
                    is_auto = true;
                    self.txn.begin_txn()?.txn_id
                };

                let key_str = format!("{}:{}", table_id, id);
                let val_str = name_val.clone();
                self.kv
                    .write_intent(txn_id, Bytes::from(key_str), Bytes::from(val_str))?;

                if is_auto {
                    self.txn.commit_txn(txn_id)?;
                }

                Ok(vec![Row {
                    columns: vec!["INSERT 0 1".into()],
                }])
            }
            PhysicalPlan::LocalPointGet { table_id, id } => {
                let active = *self.active_txn.read().unwrap();
                let read_ts = if let Some((_, ts)) = active {
                    ts
                } else {
                    u64::MAX // Simplification: read latest if no active txn
                };

                let key_str = format!("{}:{}", table_id, id);
                if let Some(val) = self.kv.get(key_str.as_bytes(), read_ts)? {
                    let val_str = String::from_utf8(val.to_vec())?;
                    Ok(vec![Row {
                        columns: vec![id, val_str],
                    }])
                } else {
                    Ok(vec![])
                }
            }
            _ => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_scaffold() {
        let exec = MemExecutor::default();
        let ctx = ExecutionContext {
            session_id: "test".to_string(),
            authz_catalog_version: 1,
        };
        exec.execute_logical(&ctx, LogicalPlan::Begin).unwrap();
        exec.execute_physical(
            &ctx,
            PhysicalPlan::LocalPointGet {
                table_id: TableId::new(),
                id: "1".into(),
            },
        )
        .unwrap();
    }
}
