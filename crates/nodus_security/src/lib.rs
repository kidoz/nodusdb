use nodus_catalog::{DatabaseId, PrincipalId, RoleId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub principal_id: PrincipalId,
    pub active_roles: Vec<RoleId>,
    pub database_id: Option<DatabaseId>,
}
