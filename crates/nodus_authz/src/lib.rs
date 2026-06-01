use anyhow::Result;
use nodus_catalog::{
    CatalogReader, DatabaseId, GrantDescriptor, GrantId, PolicyId, PrincipalId, ResourceRef, RoleId,
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

impl Action {
    /// Maps an action to the privilege string stored on grants.
    pub fn as_privilege(&self) -> &'static str {
        match self {
            Action::Connect => "CONNECT",
            Action::Usage => "USAGE",
            Action::Select => "SELECT",
            Action::Insert => "INSERT",
            Action::Update => "UPDATE",
            Action::Delete => "DELETE",
            Action::CreateDatabase => "CREATE_DATABASE",
            Action::CreateSchema => "CREATE_SCHEMA",
            Action::CreateTable => "CREATE_TABLE",
            Action::ManageGrants => "MANAGE_GRANTS",
        }
    }
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

impl DefaultAuthzEngine {
    /// Returns the matching grant (if any) that authorizes the request, along
    /// with the set of principal ids that were considered (self + roles).
    fn find_matching_grant(
        &self,
        request: &AuthzRequest,
    ) -> Result<(Option<GrantDescriptor>, Vec<PrincipalId>, u64)> {
        let catalog_version = self.catalog.get_cluster_version()?.active_version;
        let effective = self
            .catalog
            .get_effective_principals(request.principal_id)?;
        let wanted = request.action.as_privilege();
        let grants = self
            .catalog
            .get_grants_for_resource(request.resource.clone())?;

        let matched = grants.into_iter().find(|g| {
            effective.contains(&g.principal_id)
                && (g.privilege.eq_ignore_ascii_case(wanted)
                    || g.privilege.eq_ignore_ascii_case("ALL"))
        });
        Ok((matched, effective, catalog_version))
    }
}

impl AuthzEngine for DefaultAuthzEngine {
    fn authorize(&self, request: AuthzRequest) -> Result<AuthzDecision> {
        // Deny-by-default: a request is allowed only when a grant on the
        // resource matches the principal (directly or via one of its roles).
        let (matched, _effective, catalog_version) = self.find_matching_grant(&request)?;

        match matched {
            Some(grant) => {
                let reason = if grant.principal_id == request.principal_id {
                    AuthzReason::GrantedToPrincipal(grant.id)
                } else {
                    // Granted via a role the principal is a member of. We do not
                    // have the originating RoleId here, so report the first
                    // active role when present.
                    match request.active_roles.first() {
                        Some(role_id) => AuthzReason::GrantedByRole(*role_id, grant.id),
                        None => AuthzReason::GrantedToPrincipal(grant.id),
                    }
                };
                Ok(AuthzDecision {
                    allowed: true,
                    reason,
                    matched_grants: vec![grant.id],
                    matched_policies: vec![],
                    catalog_version,
                })
            }
            None => Ok(AuthzDecision {
                allowed: false,
                reason: AuthzReason::ImplicitDeny,
                matched_grants: vec![],
                matched_policies: vec![],
                catalog_version,
            }),
        }
    }

    fn explain(&self, request: AuthzRequest) -> Result<AuthzExplanation> {
        let wanted = request.action.as_privilege().to_string();
        let (matched, effective, _version) = self.find_matching_grant(&request)?;

        let mut steps = vec![
            format!("Requested privilege: {}", wanted),
            format!(
                "Effective principals considered: {} (self + {} role(s))",
                effective.len(),
                effective.len().saturating_sub(1)
            ),
        ];

        match matched {
            Some(grant) => {
                steps.push(format!(
                    "Matched grant {} ({}) on the resource.",
                    grant.id, grant.privilege
                ));
                steps.push("Decision: ALLOW.".to_string());
                Ok(AuthzExplanation {
                    is_allowed: true,
                    steps,
                })
            }
            None => {
                steps.push("No matching grant found for any effective principal.".to_string());
                steps.push("Decision: implicit DENY (deny-by-default).".to_string());
                Ok(AuthzExplanation {
                    is_allowed: false,
                    steps,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_catalog::{
        AddRoleMemberRequest, CatalogWriter, CreateRoleRequest, GrantPrivilegeRequest,
        MemoryCatalog, PrincipalType, ResourceRef, TableId,
    };

    fn request(
        principal: PrincipalId,
        roles: Vec<RoleId>,
        action: Action,
        resource: ResourceRef,
    ) -> AuthzRequest {
        AuthzRequest {
            principal_id: principal,
            active_roles: roles,
            action,
            resource,
            context: AuthzContext { database_id: None },
        }
    }

    #[test]
    fn deny_by_default_without_grant() {
        let catalog = Arc::new(MemoryCatalog::new());
        let engine = DefaultAuthzEngine::new(catalog);
        let table = ResourceRef::Table(TableId::new());
        let decision = engine
            .authorize(request(PrincipalId::new(), vec![], Action::Select, table))
            .unwrap();
        assert!(!decision.allowed);
    }

    #[test]
    fn direct_grant_allows() {
        let catalog = Arc::new(MemoryCatalog::new());
        let user = catalog
            .create_role(CreateRoleRequest {
                name: "alice".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        let table = ResourceRef::Table(TableId::new());
        catalog
            .grant_privilege(GrantPrivilegeRequest {
                principal_id: user.id,
                resource: table.clone(),
                privilege: "SELECT".into(),
            })
            .unwrap();
        let engine = DefaultAuthzEngine::new(catalog);
        let decision = engine
            .authorize(request(user.id, vec![], Action::Select, table))
            .unwrap();
        assert!(decision.allowed);
    }

    #[test]
    fn role_grant_allows_member() {
        let catalog = Arc::new(MemoryCatalog::new());
        let role = catalog
            .create_role(CreateRoleRequest {
                name: "readers".into(),
                principal_type: PrincipalType::Role,
                database_id: None,
            })
            .unwrap();
        let user = catalog
            .create_role(CreateRoleRequest {
                name: "bob".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        catalog
            .add_role_member(AddRoleMemberRequest {
                role_principal_id: role.id,
                member_id: user.id,
            })
            .unwrap();
        let table = ResourceRef::Table(TableId::new());
        catalog
            .grant_privilege(GrantPrivilegeRequest {
                principal_id: role.id,
                resource: table.clone(),
                privilege: "SELECT".into(),
            })
            .unwrap();
        let engine = DefaultAuthzEngine::new(catalog);
        // Member inherits the role's privilege; a non-member would be denied.
        let decision = engine
            .authorize(request(user.id, vec![], Action::Select, table.clone()))
            .unwrap();
        assert!(decision.allowed);
        let stranger = engine
            .authorize(request(PrincipalId::new(), vec![], Action::Select, table))
            .unwrap();
        assert!(!stranger.allowed);
    }
}
