use anyhow::Result;
use nodus_catalog::{
    CatalogReader, DatabaseId, GrantId, PolicyId, PrincipalId, ResourceRef, RoleId,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Action {
    Connect,
    Usage,
    Select,
    Insert,
    Update,
    Delete,
    CreateDatabase,
    CreateSchema,
    CreateTable,
    ManageGrants,
    // Add other actions...
}

pub struct AuthzContext {
    pub database_id: Option<DatabaseId>,
}

pub struct AuthzRequest {
    pub principal_id: PrincipalId,
    pub active_roles: Vec<RoleId>,
    pub action: Action,
    pub resource: ResourceRef,
    pub context: AuthzContext,
}

#[derive(Debug, Clone)]
pub enum AuthzReason {
    ExplicitDeny,
    ImplicitDeny,
    GrantedByRole(RoleId, GrantId),
    GrantedToPrincipal(GrantId),
    Superuser,
    Owner,
}

pub struct AuthzDecision {
    pub allowed: bool,
    pub reason: AuthzReason,
    pub matched_grants: Vec<GrantId>,
    pub matched_policies: Vec<PolicyId>,
    pub catalog_version: u64,
}

pub struct AuthzExplanation {
    pub is_allowed: bool,
    pub steps: Vec<String>,
}

pub trait AuthzEngine: Send + Sync {
    fn authorize(&self, request: AuthzRequest) -> Result<AuthzDecision>;
    fn explain(&self, request: AuthzRequest) -> Result<AuthzExplanation>;
}

#[allow(dead_code)]
pub struct DefaultAuthzEngine {
    catalog: Arc<dyn CatalogReader>,
}

impl DefaultAuthzEngine {
    pub fn new(catalog: Arc<dyn CatalogReader>) -> Self {
        Self { catalog }
    }
}

impl AuthzEngine for DefaultAuthzEngine {
    fn authorize(&self, _request: AuthzRequest) -> Result<AuthzDecision> {
        // MVP: Deny by default
        Ok(AuthzDecision {
            allowed: false,
            reason: AuthzReason::ImplicitDeny,
            matched_grants: vec![],
            matched_policies: vec![],
            catalog_version: 1, // Mock
        })
    }

    fn explain(&self, _request: AuthzRequest) -> Result<AuthzExplanation> {
        Ok(AuthzExplanation {
            is_allowed: false,
            steps: vec!["Implicit deny: No matching grants found.".to_string()],
        })
    }
}
