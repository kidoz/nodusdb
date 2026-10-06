//! Roles and privileges: `CREATE` / `ALTER` / `DROP ROLE`, `GRANT` /
//! `REVOKE` over privileges and role memberships, and the session's
//! `SET ROLE` / `SET SESSION AUTHORIZATION`.

use crate::error_fields::DbError;
use crate::plan_types::{AlterRoleAction, GrantObjectsPlan};
use crate::{ExecutionContext, MemExecutor, parse_object_name};
use anyhow::Result;
use nodus_authz::Action;
use nodus_catalog::{PrincipalDescriptor, PrincipalId, PrincipalType, ResourceRef, RoleAttributes};

/// The session's effective role after `SET ROLE` / `SET SESSION
/// AUTHORIZATION`.
#[derive(Debug, Clone)]
pub(crate) struct SessionRole {
    pub(crate) name: String,
    pub(crate) id: PrincipalId,
    /// `SET SESSION AUTHORIZATION` also changes `session_user`.
    pub(crate) session_authorization: bool,
}

/// What kind of object a `GRANT` / `REVOKE` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GrantKind {
    Relation,
    Sequence,
    Schema,
    Database,
}

impl GrantKind {
    /// How PostgreSQL words the kind in errors and dependency lists.
    fn noun(self) -> &'static str {
        match self {
            GrantKind::Relation => "table",
            GrantKind::Sequence => "sequence",
            GrantKind::Schema => "schema",
            GrantKind::Database => "database",
        }
    }

    /// How it words the kind in `invalid privilege type X for ...`.
    fn error_noun(self) -> &'static str {
        match self {
            GrantKind::Relation => "relation",
            GrantKind::Sequence => "sequence",
            GrantKind::Schema => "schema",
            GrantKind::Database => "database",
        }
    }

    /// The privileges this kind takes.
    fn privileges(self) -> &'static [&'static str] {
        match self {
            GrantKind::Relation => &[
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
            ],
            GrantKind::Sequence => &["USAGE", "SELECT", "UPDATE"],
            GrantKind::Schema => &["CREATE", "USAGE"],
            GrantKind::Database => &["CREATE", "CONNECT", "TEMPORARY", "TEMP"],
        }
    }
}

/// A resolved `GRANT` / `REVOKE` target.
struct GrantTarget {
    resource: ResourceRef,
    kind: GrantKind,
    /// As a dependency list names it: `table rt`.
    description: String,
}

impl MemExecutor {
    /// The principal a statement runs as: the session's `SET ROLE`, else
    /// the authenticated principal.
    pub(crate) fn effective_principal(&self, ctx: &ExecutionContext) -> PrincipalId {
        self.session_roles
            .read()
            .get(&ctx.session_id)
            .map_or(ctx.principal_id, |role| role.id)
    }

    /// The attributes of the session's effective principal.
    fn effective_attributes(&self, ctx: &ExecutionContext) -> RoleAttributes {
        self.catalog_reader
            .get_principal_by_id(self.effective_principal(ctx))
            .map(|principal| principal.attributes)
            .unwrap_or_default()
    }

    /// Whether the effective principal may administer roles: the superuser,
    /// or a role with the `CREATEROLE` attribute.
    fn may_create_role(&self, ctx: &ExecutionContext) -> bool {
        self.is_superuser(ctx) || self.effective_attributes(ctx).create_role
    }

    /// Whether the effective principal holds `role` membership with the
    /// admin option (or is the role itself), which lets it grant it onward.
    fn may_administer_role(&self, ctx: &ExecutionContext, role: PrincipalId) -> bool {
        if self.is_superuser(ctx) {
            return true;
        }
        let effective = self
            .catalog_reader
            .get_effective_principals(self.effective_principal(ctx))
            .unwrap_or_default();
        if effective.contains(&role) {
            // Holding the role itself is enough only with the admin option
            // — checked through the membership edges below.
        }
        self.catalog_reader
            .list_role_memberships()
            .unwrap_or_default()
            .iter()
            .any(|(r, member, admin, _)| *r == role && *admin && effective.contains(member))
    }

    /// The session's role override, if any.
    pub(crate) fn session_role(&self, session_id: &str) -> Option<SessionRole> {
        self.session_roles.read().get(session_id).cloned()
    }

    /// Whether the effective principal of `ctx` is a superuser: it holds
    /// `ALL` on the system, or its role says so.
    pub(crate) fn is_superuser(&self, ctx: &ExecutionContext) -> bool {
        if self
            .catalog_reader
            .get_principal_by_id(self.effective_principal(ctx))
            .is_ok_and(|principal| principal.attributes.superuser)
        {
            return true;
        }
        self.authz
            .authorize(nodus_authz::AuthzRequest {
                principal_id: self.effective_principal(ctx),
                active_roles: Vec::new(),
                action: Action::ManageGrants,
                resource: ResourceRef::System,
                context: nodus_authz::AuthzContext { database_id: None },
            })
            .is_ok_and(|decision| decision.allowed)
    }

    /// Whether the effective principal holds `privilege` with the grant
    /// option on `resource` (so it may grant it onward).
    fn holds_with_grant_option(
        &self,
        ctx: &ExecutionContext,
        privilege: &str,
        resource: &ResourceRef,
    ) -> bool {
        let Ok(grants) = self
            .catalog_reader
            .get_grants_for_resource(resource.clone())
        else {
            return false;
        };
        let effective = self
            .catalog_reader
            .get_effective_principals(self.effective_principal(ctx))
            .unwrap_or_default();
        grants.iter().any(|grant| {
            effective.contains(&grant.principal_id)
                && grant.grantable
                && (grant.privilege.eq_ignore_ascii_case(privilege)
                    || grant.privilege.eq_ignore_ascii_case("ALL"))
        })
    }

    /// `CREATE ROLE name [WITH options]`.
    pub(crate) fn exec_create_role(
        &self,
        ctx: &ExecutionContext,
        name: String,
        attributes: RoleAttributes,
    ) -> Result<crate::QueryOutput> {
        if !self.may_create_role(ctx) {
            return Err(DbError::new("permission denied to create role")
                .detail("Only roles with the CREATEROLE attribute may create roles.")
                .code("42501")
                .into());
        }
        if name.eq_ignore_ascii_case(nodus_catalog::PUBLIC_ROLE) {
            anyhow::bail!("role name \"{name}\" is reserved");
        }
        if name.starts_with("pg_") {
            anyhow::bail!("role name \"{name}\" is reserved");
        }
        if self.catalog_reader.get_principal_by_name(&name).is_ok() {
            anyhow::bail!("role \"{name}\" already exists");
        }
        let created = self
            .catalog_writer
            .create_role(nodus_catalog::CreateRoleRequest {
                id: PrincipalId::new(),
                name: name.clone(),
                principal_type: PrincipalType::Role,
                database_id: None,
                attributes,
            })?;
        // `CREATEROLE` without `SUPERUSER` leaves the creator with the
        // admin option on the new role, as PostgreSQL records it.
        if !self.is_superuser(ctx) {
            self.catalog_writer
                .add_role_member(nodus_catalog::AddRoleMemberRequest {
                    role_principal_id: created.id,
                    member_id: self.effective_principal(ctx),
                    admin_option: true,
                    grantor: Some(self.effective_principal(ctx)),
                })?;
        }
        Ok(crate::QueryOutput::tag("CREATE ROLE"))
    }

    /// `ALTER ROLE name ...`.
    pub(crate) fn exec_alter_role(
        &self,
        ctx: &ExecutionContext,
        name: String,
        action: AlterRoleAction,
    ) -> Result<crate::QueryOutput> {
        let principal = self.role_named(&name)?;
        if let AlterRoleAction::Attributes { patch } = &action {
            // `SUPERUSER`, `REPLICATION`, and `BYPASSRLS` are the
            // superuser's to change.
            for (changed, attribute) in [
                (patch.superuser.is_some(), "SUPERUSER"),
                (patch.replication.is_some(), "REPLICATION"),
                (patch.bypass_rls.is_some(), "BYPASSRLS"),
            ] {
                if changed && !self.is_superuser(ctx) {
                    return Err(DbError::new("permission denied to alter role")
                        .detail(format!(
                            "Only roles with the SUPERUSER attribute may change the {attribute} attribute."
                        ))
                        .code("42501")
                        .into());
                }
            }
        }
        if !self.may_create_role(ctx) || !self.may_administer_role(ctx, principal.id) {
            return Err(DbError::new("permission denied to alter role")
                .detail(format!(
                    "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{}\" may alter this role.",
                    principal.name
                ))
                .code("42501")
                .into());
        }
        let new_name = match &action {
            AlterRoleAction::Rename { name: new_name } => {
                if new_name.eq_ignore_ascii_case(nodus_catalog::PUBLIC_ROLE)
                    || new_name.starts_with("pg_")
                {
                    anyhow::bail!("role name \"{new_name}\" is reserved");
                }
                if self.catalog_reader.get_principal_by_name(new_name).is_ok() {
                    anyhow::bail!("role \"{new_name}\" already exists");
                }
                Some(new_name.clone())
            }
            _ => None,
        };
        let mut attributes = principal.attributes.clone();
        match &action {
            AlterRoleAction::Attributes { patch } => {
                apply_patch(&mut attributes, patch);
            }
            AlterRoleAction::Rename { .. } => {}
            AlterRoleAction::SetSetting { name, value } => {
                match attributes
                    .settings
                    .iter_mut()
                    .find(|(setting, _)| setting == name)
                {
                    Some(setting) => setting.1 = value.clone(),
                    None => attributes.settings.push((name.clone(), value.clone())),
                }
            }
            AlterRoleAction::ResetSetting { name } => {
                if name.is_empty() {
                    attributes.settings.clear();
                } else {
                    attributes.settings.retain(|(setting, _)| setting != name);
                }
            }
        }
        self.catalog_writer
            .update_principal(nodus_catalog::UpdatePrincipalRequest {
                principal_id: principal.id,
                attributes,
                new_name,
            })?;
        Ok(crate::QueryOutput::tag("ALTER ROLE"))
    }

    /// `DROP ROLE [IF EXISTS] name [, ...]`.
    pub(crate) fn exec_drop_role(
        &self,
        ctx: &ExecutionContext,
        names: Vec<String>,
        if_exists: bool,
    ) -> Result<crate::QueryOutput> {
        if !self.may_create_role(ctx) {
            return Err(DbError::new("permission denied to drop role")
                .detail(
                    "Only roles with the CREATEROLE attribute and the ADMIN option on the target roles may drop roles.",
                )
                .code("42501")
                .into());
        }
        for name in &names {
            let principal = match self.role_named(name) {
                Ok(principal) => principal,
                Err(_) if if_exists => {
                    self.notice(
                        ctx,
                        DbError::new(format!("role \"{name}\" does not exist, skipping")),
                    );
                    continue;
                }
                Err(error) => return Err(error),
            };
            if principal.name == "nodus" {
                anyhow::bail!("role \"{}\" cannot be dropped", principal.name);
            }
            if !self.may_administer_role(ctx, principal.id) {
                return Err(DbError::new("permission denied to drop role")
                    .detail(format!(
                        "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{}\" may drop this role.",
                        principal.name
                    ))
                    .code("42501")
                    .into());
            }
            // What depends on it: the objects it owns, and the privileges
            // it holds (memberships do not block a drop). PostgreSQL lists
            // the shared database first, then by the object's creation.
            let owner = nodus_catalog::RoleId(principal.id.0);
            let schemas = self
                .catalog_reader
                .list_schemas("default")
                .unwrap_or_default();
            let tables = self
                .catalog_reader
                .list_all_tables("default")
                .unwrap_or_default();
            let mut dependencies = Vec::new();
            for schema in &schemas {
                if schema.owner_role_id == Some(owner) {
                    dependencies.push((
                        schema.created_at,
                        format!("owner of schema {}", schema.name),
                    ));
                }
            }
            for table in &tables {
                if table.owner_role_id != Some(owner) {
                    continue;
                }
                let schema = schemas
                    .iter()
                    .find(|schema| schema.id == table.schema_id)
                    .map_or_else(|| "public".to_string(), |schema| schema.name.clone());
                let noun = if crate::sequences::is_sequence(table) {
                    "sequence"
                } else if table.materialized_query.is_some() {
                    "materialized view"
                } else if table.view_query.is_some() {
                    "view"
                } else {
                    "table"
                };
                let name = if schema == "public" {
                    table.name.clone()
                } else {
                    format!("{schema}.{}", table.name)
                };
                dependencies.push((table.created_at, format!("owner of {noun} {name}")));
            }
            for grant in self.catalog_reader.get_grants_for_principal(principal.id)? {
                let Some(description) = self.resource_description(&grant.resource) else {
                    continue;
                };
                let created = match &grant.resource {
                    ResourceRef::Table(id) => tables
                        .iter()
                        .find(|table| table.id == *id)
                        .map(|t| t.created_at),
                    ResourceRef::Schema(id) => schemas
                        .iter()
                        .find(|schema| schema.id == *id)
                        .map(|s| s.created_at),
                    ResourceRef::Database(_) => self
                        .catalog_reader
                        .get_database("default")
                        .ok()
                        .map(|database| database.created_at),
                    _ => None,
                };
                dependencies.push((
                    created.unwrap_or(grant.created_at),
                    format!("privileges for {description}"),
                ));
            }
            dependencies.sort_by_key(|(created, _)| *created);
            let mut details: Vec<String> = Vec::new();
            for (_, description) in dependencies {
                if !details.contains(&description) {
                    details.push(description);
                }
            }
            if !details.is_empty() {
                return Err(DbError::new(format!(
                    "role \"{}\" cannot be dropped because some objects depend on it",
                    principal.name
                ))
                .detail(details.join("\n"))
                .into());
            }
            self.catalog_writer.drop_principal(principal.id)?;
        }
        Ok(crate::QueryOutput::tag("DROP ROLE"))
    }

    /// `GRANT privileges ON objects TO grantees [WITH GRANT OPTION]`.
    pub(crate) fn exec_grant(
        &self,
        ctx: &ExecutionContext,
        privileges: Vec<String>,
        objects: GrantObjectsPlan,
        grantees: Vec<String>,
        with_grant_option: bool,
    ) -> Result<crate::QueryOutput> {
        let roles = self.resolve_grantees(ctx, &grantees)?;
        let targets = self.grant_targets(&objects)?;
        for target in &targets {
            if !self.is_superuser(ctx)
                && !self
                    .authorize(ctx, Action::ManageGrants, target.resource.clone())
                    .is_ok()
                && !privileges
                    .iter()
                    .any(|privilege| self.holds_with_grant_option(ctx, privilege, &target.resource))
            {
                self.authorize(ctx, Action::ManageGrants, target.resource.clone())?;
            }
            for privilege in &privileges {
                self.check_privilege(privilege, target.kind)?;
                for role in &roles {
                    self.grant_one(role, &target.resource, privilege, with_grant_option, ctx)?;
                }
            }
        }
        Ok(crate::QueryOutput::tag("GRANT"))
    }

    /// `REVOKE [GRANT OPTION FOR] privileges ON objects FROM grantees`.
    pub(crate) fn exec_revoke(
        &self,
        ctx: &ExecutionContext,
        privileges: Vec<String>,
        objects: GrantObjectsPlan,
        grantees: Vec<String>,
        grant_option_for: bool,
    ) -> Result<crate::QueryOutput> {
        let roles = self.resolve_grantees(ctx, &grantees)?;
        let targets = self.grant_targets(&objects)?;
        for target in &targets {
            self.authorize(ctx, Action::ManageGrants, target.resource.clone())?;
            for privilege in &privileges {
                self.check_privilege(privilege, target.kind)?;
                for role in &roles {
                    self.revoke_one(role, &target.resource, privilege, grant_option_for)?;
                }
            }
        }
        Ok(crate::QueryOutput::tag("REVOKE"))
    }

    /// `GRANT role TO member [WITH ADMIN OPTION]` / `REVOKE role FROM member`
    /// / `REVOKE ADMIN OPTION FOR`.
    pub(crate) fn exec_grant_role(
        &self,
        ctx: &ExecutionContext,
        roles: Vec<String>,
        members: Vec<String>,
        admin_option: bool,
        admin_only: bool,
        grant: bool,
    ) -> Result<crate::QueryOutput> {
        let mut resolved = Vec::new();
        for name in &roles {
            resolved.push(self.resolve_grantee_name(ctx, name)?);
        }
        for role in &resolved {
            if !self.may_administer_role(ctx, role.id) {
                let verb = if grant { "grant" } else { "revoke" };
                return Err(DbError::new(format!(
                    "permission denied to {verb} role \"{}\"",
                    role.name
                ))
                .detail(format!(
                    "Only roles with the ADMIN option on role \"{}\" may {verb} this role.",
                    role.name
                ))
                .code("42501")
                .into());
            }
        }
        let mut resolved_members = Vec::new();
        for name in &members {
            resolved_members.push(self.resolve_grantee_name(ctx, name)?);
        }
        for role in &resolved {
            for member in &resolved_members {
                if grant {
                    // A membership that would close a cycle: PostgreSQL
                    // reports the pair as written. A membership that is
                    // already there is no change, and no cycle.
                    let existing = self
                        .catalog_reader
                        .list_role_memberships()?
                        .into_iter()
                        .find(|(r, m, ..)| *r == role.id && *m == member.id);
                    let held = existing.is_some();
                    if let Some((_, _, admin, grantor)) = existing
                        && admin == admin_option
                    {
                        let by = grantor
                            .and_then(|id| self.principal_name(id))
                            .unwrap_or_else(|| "nodus".to_string());
                        self.notice(
                            ctx,
                            DbError::new(format!(
                                "role \"{}\" has already been granted membership in role \"{}\" by role \"{by}\"",
                                member.name, role.name
                            )),
                        );
                    }
                    // The role's own closure: the member belongs to it
                    // already when the role is a member of the member.
                    let effective = self
                        .catalog_reader
                        .get_effective_principals(role.id)
                        .unwrap_or_default();
                    if !held && effective.contains(&member.id) {
                        anyhow::bail!(
                            "role \"{}\" is a member of role \"{}\"",
                            role.name,
                            member.name
                        );
                    }
                    self.catalog_writer
                        .add_role_member(nodus_catalog::AddRoleMemberRequest {
                            role_principal_id: role.id,
                            member_id: member.id,
                            admin_option,
                            grantor: Some(self.effective_principal(ctx)),
                        })?;
                } else {
                    let memberships = self.catalog_reader.list_role_memberships()?;
                    let existing = memberships
                        .iter()
                        .find(|(r, m, ..)| *r == role.id && *m == member.id)
                        .copied();
                    if admin_only && let Some((_, _, true, grantor)) = existing {
                        self.catalog_writer.add_role_member(
                            nodus_catalog::AddRoleMemberRequest {
                                role_principal_id: role.id,
                                member_id: member.id,
                                admin_option: false,
                                grantor,
                            },
                        )?;
                        continue;
                    }
                    if existing.is_none() {
                        let grantor = self
                            .catalog_reader
                            .get_principal_by_id(self.effective_principal(ctx))
                            .map_or_else(|_| "nodus".to_string(), |p| p.name);
                        self.notice(
                            ctx,
                            crate::transactions::warning(
                                format!(
                                    "role \"{}\" has not been granted membership in role \"{}\" by role \"{grantor}\"",
                                    member.name, role.name
                                ),
                                "01007",
                            ),
                        );
                        continue;
                    }
                    if !admin_only {
                        self.catalog_writer.remove_role_member(
                            nodus_catalog::RemoveRoleMemberRequest {
                                role_principal_id: role.id,
                                member_id: member.id,
                            },
                        )?;
                    }
                }
            }
        }
        let tag = if grant { "GRANT ROLE" } else { "REVOKE ROLE" };
        Ok(crate::QueryOutput::tag(tag))
    }

    /// `SET ROLE` / `RESET ROLE` / `SET SESSION AUTHORIZATION`.
    pub(crate) fn exec_set_role(
        &self,
        ctx: &ExecutionContext,
        role: Option<String>,
        session_authorization: bool,
    ) -> Result<crate::QueryOutput> {
        let tag = if role.is_none() { "RESET" } else { "SET" };
        match role {
            None => {
                let mut guard = self.session_roles.write();
                match guard.get_mut(&ctx.session_id) {
                    Some(current) if session_authorization => {
                        if current.session_authorization {
                            guard.remove(&ctx.session_id);
                        }
                    }
                    _ => {
                        guard.remove(&ctx.session_id);
                    }
                }
            }
            Some(name) => {
                let target = self.role_named(&name)?;
                if session_authorization {
                    if !self.is_superuser(ctx) {
                        anyhow::bail!("permission denied to set session authorization \"{name}\"");
                    }
                } else if target.id != self.effective_principal(ctx) && !self.is_superuser(ctx) {
                    let member = self
                        .catalog_reader
                        .get_effective_principals(self.effective_principal(ctx))
                        .unwrap_or_default();
                    if !member.contains(&target.id) {
                        anyhow::bail!("permission denied to set role \"{name}\"");
                    }
                }
                self.session_roles.write().insert(
                    ctx.session_id.clone(),
                    SessionRole {
                        name: target.name.clone(),
                        id: target.id,
                        session_authorization,
                    },
                );
            }
        }
        Ok(crate::QueryOutput::tag(tag))
    }

    /// The role named `name`, with PostgreSQL's error when there is none.
    fn role_named(&self, name: &str) -> Result<PrincipalDescriptor> {
        self.catalog_reader
            .get_principal_by_name(name)
            .map_err(|_| anyhow::anyhow!("role \"{name}\" does not exist"))
    }

    /// One role name as a principal, with `current_user` (and its
    /// synonyms) the session's role.
    pub(crate) fn resolve_grantee_name(
        &self,
        ctx: &ExecutionContext,
        name: &str,
    ) -> Result<PrincipalDescriptor> {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "current_user" | "current_role" | "session_user" | "user"
        ) {
            return Ok(self
                .catalog_reader
                .get_principal_by_id(self.effective_principal(ctx))?);
        }
        self.role_named(name)
    }

    /// The grantees of a `GRANT` / `REVOKE`, with `PUBLIC` made on first
    /// use.
    fn resolve_grantees(
        &self,
        ctx: &ExecutionContext,
        grantees: &[String],
    ) -> Result<Vec<PrincipalDescriptor>> {
        let mut roles = Vec::new();
        for grantee in grantees {
            if grantee.eq_ignore_ascii_case(nodus_catalog::PUBLIC_ROLE) {
                match self
                    .catalog_reader
                    .get_principal_by_name(nodus_catalog::PUBLIC_ROLE)
                {
                    Ok(role) => roles.push(role),
                    Err(_) => roles.push(self.catalog_writer.create_role(
                        nodus_catalog::CreateRoleRequest {
                            id: PrincipalId::new(),
                            name: nodus_catalog::PUBLIC_ROLE.to_string(),
                            principal_type: PrincipalType::Public,
                            database_id: None,
                            attributes: Default::default(),
                        },
                    )?),
                }
                continue;
            }
            roles.push(self.resolve_grantee_name(ctx, grantee)?);
        }
        Ok(roles)
    }

    /// The objects of a `GRANT` / `REVOKE`.
    fn grant_targets(&self, objects: &GrantObjectsPlan) -> Result<Vec<GrantTarget>> {
        let mut targets = Vec::new();
        match objects {
            GrantObjectsPlan::ByName { kind, names } => {
                let kind = kind_kind(kind)?;
                for name in names {
                    targets.push(self.grant_target(kind, name)?);
                }
            }
            GrantObjectsPlan::AllInSchema { kind, schemas } => {
                let kind = kind_kind(kind)?;
                for schema_name in schemas {
                    let schema = self
                        .catalog_reader
                        .list_schemas("default")?
                        .into_iter()
                        .find(|s| &s.name == schema_name)
                        .ok_or_else(|| {
                            anyhow::anyhow!("schema \"{schema_name}\" does not exist")
                        })?;
                    for table in self.catalog_reader.list_all_tables("default")? {
                        if table.schema_id != schema.id || table.view_query.is_some() {
                            continue;
                        }
                        let is_sequence = crate::sequences::is_sequence(&table);
                        let wanted = match kind {
                            GrantKind::Sequence => is_sequence,
                            GrantKind::Relation => !is_sequence,
                            _ => false,
                        };
                        if wanted {
                            targets.push(GrantTarget {
                                resource: ResourceRef::Table(table.id),
                                kind,
                                description: format!("{} {}", kind.noun(), table.name),
                            });
                        }
                    }
                }
            }
        }
        Ok(targets)
    }

    /// One named object of a `GRANT` / `REVOKE`.
    fn grant_target(&self, kind: GrantKind, name: &str) -> Result<GrantTarget> {
        match kind {
            GrantKind::Relation | GrantKind::Sequence => {
                let (db, schema, table_only) = parse_object_name(name)?;
                let table = self.catalog_reader.get_table(db, schema, table_only)?;
                if kind == GrantKind::Sequence && !crate::sequences::is_sequence(&table) {
                    anyhow::bail!("\"{}\" is not a sequence", table.name);
                }
                if kind == GrantKind::Relation && crate::sequences::is_sequence(&table) {
                    anyhow::bail!("cannot grant on sequence \"{}\"", table.name);
                }
                Ok(GrantTarget {
                    resource: ResourceRef::Table(table.id),
                    kind,
                    description: format!("{} {}", kind.noun(), table.name),
                })
            }
            GrantKind::Schema => {
                let schema = self
                    .catalog_reader
                    .list_schemas("default")?
                    .into_iter()
                    .find(|s| s.name == name)
                    .ok_or_else(|| anyhow::anyhow!("schema \"{name}\" does not exist"))?;
                Ok(GrantTarget {
                    resource: ResourceRef::Schema(schema.id),
                    kind,
                    description: format!("schema {}", schema.name),
                })
            }
            GrantKind::Database => {
                let database = self
                    .catalog_reader
                    .get_database(name)
                    .map_err(|_| anyhow::anyhow!("database \"{name}\" does not exist"))?;
                Ok(GrantTarget {
                    resource: ResourceRef::Database(database.id),
                    kind,
                    description: format!("database {}", database.name),
                })
            }
        }
    }

    /// PostgreSQL's privilege-kind check.
    fn check_privilege(&self, privilege: &str, kind: GrantKind) -> Result<()> {
        if privilege.eq_ignore_ascii_case("ALL") {
            return Ok(());
        }
        if kind
            .privileges()
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(privilege))
        {
            return Ok(());
        }
        anyhow::bail!(
            "invalid privilege type {privilege} for {}",
            kind.error_noun()
        )
    }

    /// Applies one grant, replacing an existing one (the grant option may
    /// change).
    fn grant_one(
        &self,
        role: &PrincipalDescriptor,
        resource: &ResourceRef,
        privilege: &str,
        grantable: bool,
        ctx: &ExecutionContext,
    ) -> Result<()> {
        let existing = self
            .catalog_reader
            .get_grants_for_resource(resource.clone())?
            .into_iter()
            .find(|grant| {
                grant.principal_id == role.id && grant.privilege.eq_ignore_ascii_case(privilege)
            });
        match existing {
            Some(grant) if grant.grantable == grantable => Ok(()),
            Some(_) => {
                self.catalog_writer
                    .revoke_privileges(nodus_catalog::RevokePrivilegesRequest {
                        principal_id: role.id,
                        resource: resource.clone(),
                        privilege: privilege.to_string(),
                    })?;
                self.catalog_writer
                    .grant_privileges(nodus_catalog::GrantPrivilegesRequest {
                        id: nodus_catalog::GrantId::new(),
                        principal_id: role.id,
                        resource: resource.clone(),
                        privilege: privilege.to_string(),
                        grantable,
                        grantor: Some(self.effective_principal(ctx)),
                    })?;
                Ok(())
            }
            None => {
                self.catalog_writer
                    .grant_privileges(nodus_catalog::GrantPrivilegesRequest {
                        id: nodus_catalog::GrantId::new(),
                        principal_id: role.id,
                        resource: resource.clone(),
                        privilege: privilege.to_string(),
                        grantable,
                        grantor: Some(self.effective_principal(ctx)),
                    })?;
                Ok(())
            }
        }
    }

    /// Applies one revoke; `GRANT OPTION FOR` withdraws only the option.
    fn revoke_one(
        &self,
        role: &PrincipalDescriptor,
        resource: &ResourceRef,
        privilege: &str,
        grant_option_for: bool,
    ) -> Result<()> {
        // `ALL` takes back every privilege the role holds on the resource;
        // a named one just itself.
        let all = privilege.eq_ignore_ascii_case("ALL");
        let held: Vec<nodus_catalog::GrantDescriptor> = self
            .catalog_reader
            .get_grants_for_resource(resource.clone())?
            .into_iter()
            .filter(|grant| {
                grant.principal_id == role.id
                    && (all || grant.privilege.eq_ignore_ascii_case(privilege))
            })
            .collect();
        if all {
            for grant in held {
                self.revoke_one(role, resource, &grant.privilege, grant_option_for)?;
            }
            return Ok(());
        }
        let Some(existing) = held.into_iter().next() else {
            return Ok(());
        };
        if grant_option_for {
            if !existing.grantable {
                return Ok(());
            }
            self.catalog_writer
                .revoke_privileges(nodus_catalog::RevokePrivilegesRequest {
                    principal_id: role.id,
                    resource: resource.clone(),
                    privilege: privilege.to_string(),
                })?;
            self.catalog_writer
                .grant_privileges(nodus_catalog::GrantPrivilegesRequest {
                    id: nodus_catalog::GrantId::new(),
                    principal_id: role.id,
                    resource: resource.clone(),
                    privilege: privilege.to_string(),
                    grantable: false,
                    grantor: existing.grantor,
                })?;
            return Ok(());
        }
        self.catalog_writer
            .revoke_privileges(nodus_catalog::RevokePrivilegesRequest {
                principal_id: role.id,
                resource: resource.clone(),
                privilege: privilege.to_string(),
            })
    }

    /// A resource as a dependency list names it.
    fn resource_description(&self, resource: &ResourceRef) -> Option<String> {
        match resource {
            ResourceRef::Table(id) => {
                let table = self.catalog_reader.get_table_by_id(*id).ok()?;
                let noun = if crate::sequences::is_sequence(&table) {
                    "sequence"
                } else {
                    "table"
                };
                Some(format!("{noun} {}", table.name))
            }
            ResourceRef::Schema(id) => {
                let schema = self.catalog_reader.get_schema_by_id(*id).ok()?;
                Some(format!("schema {}", schema.name))
            }
            ResourceRef::Database(_) => {
                // One database exists; its name is the session's.
                Some("database default".to_string())
            }
            _ => None,
        }
    }

    /// The name of a relation's owner; a relation that never had `OWNER TO`
    /// belongs to the bootstrap superuser.
    pub(crate) fn owner_name(&self, table: &nodus_catalog::TableDescriptor) -> String {
        table
            .owner_role_id
            .and_then(|owner| {
                self.catalog_reader
                    .get_principal_by_id(PrincipalId(owner.0))
                    .ok()
            })
            .map_or_else(|| "nodus".to_string(), |principal| principal.name)
    }

    /// The name of a principal id, when the catalog still has it.
    pub(crate) fn principal_name(&self, id: PrincipalId) -> Option<String> {
        self.catalog_reader
            .get_principal_by_id(id)
            .ok()
            .map(|principal| principal.name)
    }
}

/// The kind of a `GRANT` object list.
fn kind_kind(kind: &str) -> Result<GrantKind> {
    match kind.to_ascii_uppercase().as_str() {
        "TABLE" | "VIEW" => Ok(GrantKind::Relation),
        "SEQUENCE" => Ok(GrantKind::Sequence),
        "SCHEMA" => Ok(GrantKind::Schema),
        "DATABASE" => Ok(GrantKind::Database),
        other => anyhow::bail!("GRANT on {other} is not supported"),
    }
}

/// Applies an `ALTER ROLE` option patch over a role's attributes.
fn apply_patch(attributes: &mut RoleAttributes, patch: &crate::plan_types::RoleAttrsPatch) {
    if let Some(value) = patch.can_login {
        attributes.can_login = value;
    }
    if let Some(value) = patch.superuser {
        attributes.superuser = value;
    }
    if let Some(value) = patch.create_db {
        attributes.create_db = value;
    }
    if let Some(value) = patch.create_role {
        attributes.create_role = value;
    }
    if let Some(value) = patch.inherit {
        attributes.inherit = value;
    }
    if let Some(value) = patch.bypass_rls {
        attributes.bypass_rls = value;
    }
    if let Some(value) = patch.replication {
        attributes.replication = value;
    }
    if let Some(value) = patch.connection_limit {
        attributes.connection_limit = value;
    }
    if let Some(value) = &patch.valid_until {
        attributes.valid_until = value.clone();
    }
    if let Some(value) = &patch.password {
        attributes.password = Some(value.clone());
    }
}
