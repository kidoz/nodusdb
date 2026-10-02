//! Schemas: creating one (with the objects created in it), dropping one
//! with what it holds (`CASCADE`) or refusing to (`RESTRICT`), renaming it,
//! its comment, and moving relations and types between schemas (`SET
//! SCHEMA`).
//!
//! The catalog keeps a relation's references to others by name — a view's
//! plan, a foreign key, a column's type, the sequence a column's default
//! draws from — so moving or renaming what they name retargets them, as
//! PostgreSQL's references (by OID) follow the object.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use nodus_authz::Action;
use nodus_catalog::{
    ColumnDescriptor, CreateTableRequest, DescriptorState, ResourceRef, SchemaDescriptor,
    TableConstraint, TableDescriptor, TableDescriptorChange, TableId,
};

use crate::ddl::{Dependent, RelationKind};
use crate::error_fields::DbError;
use crate::user_types::{self, UserType};
use crate::{ExecutionContext, LogicalPlan, MemExecutor, QueryOutput, Value};

/// The column of the hidden relation that keeps a schema's own facts (its
/// comment, as the relation's).
pub(crate) const SCHEMA_COLUMN: &str = "__nodus_schema__";

/// Whether a relation keeps a schema's own facts.
pub(crate) fn is_schema_relation(table: &TableDescriptor) -> bool {
    table.view_query.is_none() && table.columns.len() == 1 && table.columns[0].name == SCHEMA_COLUMN
}

/// Refuses a schema name PostgreSQL reserves for itself.
fn check_schema_name(name: &str) -> Result<()> {
    if name.starts_with("pg_") {
        return Err(DbError::new(format!("unacceptable schema name \"{name}\""))
            .code("42939")
            .detail("The prefix \"pg_\" is reserved for system schemas.")
            .into());
    }
    Ok(())
}

fn missing_schema(name: &str) -> anyhow::Error {
    DbError::new(format!("schema \"{name}\" does not exist"))
        .code("3F000")
        .into()
}

fn unquote(name: &str) -> String {
    let name = name.trim();
    match name.strip_prefix('"').and_then(|n| n.strip_suffix('"')) {
        Some(inner) => inner.replace("\"\"", "\""),
        None => name.to_string(),
    }
}

/// A name as PostgreSQL shows an object: qualified by its schema when that
/// is not on the search path.
fn shown_name(schema: &str, name: &str) -> String {
    if crate::search_path::existing_search_path()
        .iter()
        .any(|s| s == schema)
    {
        name.to_string()
    } else {
        format!("{schema}.{name}")
    }
}

/// The sequences a relation's columns own (`serial` and identity columns),
/// by name: they belong to it rather than to its schema.
fn owned_sequences(table: &TableDescriptor) -> Vec<String> {
    table
        .columns
        .iter()
        .filter_map(|column| crate::sequences::owned_by_column(&table.name, column))
        .map(|sequence| unquote(sequence.rsplit('.').next().unwrap_or(&sequence)))
        .collect()
}

/// An object a schema holds.
enum Member {
    Relation(TableDescriptor),
    Type(Arc<UserType>, TableDescriptor),
}

impl Member {
    fn relation(&self) -> &TableDescriptor {
        match self {
            Member::Relation(table) | Member::Type(_, table) => table,
        }
    }

    /// As PostgreSQL describes it: `table s1.t`.
    fn description(&self, schema: &str) -> String {
        let table = self.relation();
        let noun = match self {
            Member::Type(..) => "type",
            Member::Relation(table) if crate::sequences::is_sequence(table) => "sequence",
            Member::Relation(table) => RelationKind::of(table)
                .unwrap_or(RelationKind::Table)
                .noun(),
        };
        format!("{noun} {}", shown_name(schema, &table.name))
    }
}

/// A relation or type moving to another schema or name.
struct Move {
    old_schema: String,
    old_name: String,
    new_schema: String,
    new_name: String,
    /// Whether an unqualified reference to the old name reached it (it was
    /// the first of that name on the search path).
    visible: bool,
    /// Whether references are qualified by the schema from now on (it moves
    /// schema), or keep the form they had (it is renamed).
    qualify: bool,
    /// A type's move (else a relation's).
    is_type: bool,
}

impl Move {
    /// A reference as it reads after the move, if it names the object.
    fn retarget(&self, reference: &str) -> Option<String> {
        let (schema, name) = user_types::split_name(reference);
        if name != self.old_name {
            return None;
        }
        let schema = schema.map(|s| unquote(s.rsplit('.').next().unwrap_or(&s)));
        match &schema {
            Some(s) if *s == self.old_schema => {}
            None if self.visible => {}
            _ => return None,
        }
        let name = user_types::quote(&self.new_name);
        Some(if self.qualify || schema.is_some() {
            format!("{}.{name}", user_types::quote(&self.new_schema))
        } else {
            name
        })
    }
}

/// A view's stored plan with its references to moved relations retargeted;
/// `None` when it names none of them. A relation read without an alias keeps
/// its old name as one, so the columns the plan qualifies by it still read.
fn retarget_plan(query: &str, moves: &[&Move]) -> Option<String> {
    fn walk(value: &mut serde_json::Value, moves: &[&Move]) -> bool {
        match value {
            serde_json::Value::Object(map) => {
                let mut changed = false;
                let target = map
                    .get("table_name")
                    .and_then(|v| v.as_str())
                    .and_then(|r| {
                        moves
                            .iter()
                            .find_map(|m| m.retarget(r).map(|new| (new, m.old_name.clone())))
                    });
                if let Some((new, old)) = target {
                    map.insert("table_name".into(), serde_json::Value::String(new));
                    if map.get("table_alias").is_some_and(|a| a.is_null()) {
                        map.insert("table_alias".into(), serde_json::Value::String(old));
                    }
                    changed = true;
                }
                for (_, v) in map.iter_mut() {
                    changed |= walk(v, moves);
                }
                changed
            }
            serde_json::Value::Array(items) => {
                let mut changed = false;
                for item in items {
                    changed |= walk(item, moves);
                }
                changed
            }
            _ => false,
        }
    }
    let mut plan: serde_json::Value = serde_json::from_str(query).ok()?;
    walk(&mut plan, moves).then(|| plan.to_string())
}

impl MemExecutor {
    /// Whether an unqualified `name` reaches the object of that name in
    /// `schema`: the first of the search path's schemas holding the name.
    fn reached_unqualified(&self, schema: &str, name: &str) -> bool {
        crate::search_path::temp_schema()
            .map(str::to_string)
            .into_iter()
            .chain(crate::search_path::existing_search_path())
            .find(|s| self.types_catalog.get_table("default", s, name).is_ok())
            .is_some_and(|s| s == schema)
    }

    /// `CREATE SCHEMA [IF NOT EXISTS] name [AUTHORIZATION role]`, then the
    /// statements creating objects in it, with it first on the search path.
    pub(crate) fn exec_create_schema(
        &self,
        ctx: &ExecutionContext,
        schema_name: String,
        if_not_exists: bool,
        authorization: Option<String>,
        elements: Vec<LogicalPlan>,
    ) -> Result<QueryOutput> {
        let db = self.catalog_reader.get_database("default")?;
        self.authorize(ctx, Action::CreateSchema, ResourceRef::Database(db.id))?;
        check_schema_name(&schema_name)?;
        if let Some(role) = &authorization
            && self.catalog_reader.get_principal_by_name(role).is_err()
        {
            return Err(DbError::new(format!("role \"{role}\" does not exist"))
                .code("42704")
                .into());
        }
        // Checked first: a replicated catalog reports a lost race only as a
        // missing schema.
        if self
            .catalog_reader
            .get_schema("default", &schema_name)
            .is_ok()
        {
            if if_not_exists {
                self.notice(
                    ctx,
                    DbError::new(format!("schema \"{schema_name}\" already exists, skipping"))
                        .code("42P06"),
                );
                return Ok(QueryOutput::tag("CREATE SCHEMA"));
            }
            return Err(
                DbError::new(format!("schema \"{schema_name}\" already exists"))
                    .code("42P06")
                    .into(),
            );
        }
        self.catalog_writer
            .create_schema(nodus_catalog::CreateSchemaRequest {
                id: nodus_catalog::SchemaId::new(),
                database_id: db.id,
                name: schema_name.clone(),
                owner_role_id: None,
                managed_access: false,
            })?;
        if !elements.is_empty() {
            let path = format!(
                "{}, {}",
                user_types::quote(&schema_name),
                crate::session_env::setting("search_path").unwrap_or_default()
            );
            crate::session_env::with_setting("search_path", path, || {
                elements
                    .into_iter()
                    .try_for_each(|plan| self.execute_logical_inner(ctx, plan).map(|_| ()))
            })?;
        }
        Ok(QueryOutput::tag("CREATE SCHEMA"))
    }

    /// The objects a schema holds, in the order they were made (the
    /// sequences its tables' columns own go with those tables), and the
    /// relation keeping its own facts, if it has one.
    fn schema_members(&self, schema: &str) -> Result<(Vec<Member>, Option<TableDescriptor>)> {
        let mut tables = self.types_catalog.list_tables("default", schema)?;
        tables.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.name.cmp(&b.name)));
        let owned: HashSet<String> = tables.iter().flat_map(owned_sequences).collect();
        let mut members = Vec::new();
        let mut meta = None;
        for table in tables {
            if is_schema_relation(&table) {
                meta = Some(table);
            } else if let Some(t) = UserType::of(&table, schema.to_string()) {
                members.push(Member::Type(Arc::new(t), table));
            } else if !(crate::sequences::is_sequence(&table) && owned.contains(&table.name)) {
                members.push(Member::Relation(table));
            }
        }
        Ok((members, meta))
    }

    /// `DROP SCHEMA [IF EXISTS] names [CASCADE | RESTRICT]`: a schema that
    /// holds anything is dropped only with `CASCADE`, and then with what it
    /// holds and what depends on that elsewhere.
    pub(crate) fn exec_drop_schemas(
        &self,
        ctx: &ExecutionContext,
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    ) -> Result<QueryOutput> {
        let mut targets: Vec<SchemaDescriptor> = Vec::new();
        for name in &names {
            let name = unquote(name);
            if name == "pg_catalog" {
                return Err(DbError::new(
                    "cannot drop schema pg_catalog because it is required by the database system",
                )
                .code("2BP01")
                .into());
            }
            if name == "information_schema" {
                return Err(DbError::new(
                    "cannot drop schema information_schema because other objects depend on it",
                )
                .code("2BP01")
                .hint("Use DROP ... CASCADE to drop the dependent objects too.")
                .into());
            }
            match self.catalog_reader.get_schema("default", &name) {
                Ok(schema) => {
                    self.authorize(ctx, Action::CreateSchema, ResourceRef::Schema(schema.id))?;
                    if !targets.iter().any(|t| t.id == schema.id) {
                        targets.push(schema);
                    }
                }
                Err(_) if if_exists => self.notice(
                    ctx,
                    DbError::new(format!("schema \"{name}\" does not exist, skipping")),
                ),
                Err(_) => return Err(missing_schema(&name)),
            }
        }
        let mut held: Vec<(String, Vec<Member>, Option<TableDescriptor>)> = Vec::new();
        for schema in &targets {
            let (members, meta) = self.schema_members(&schema.name)?;
            held.push((schema.name.clone(), members, meta));
        }
        let any_member = held.iter().any(|(_, members, _)| !members.is_empty());
        if any_member && !cascade {
            let message = match targets.as_slice() {
                [only] => format!(
                    "cannot drop schema {} because other objects depend on it",
                    only.name
                ),
                _ => "cannot drop desired object(s) because other objects depend on them".into(),
            };
            let detail: Vec<String> = held
                .iter()
                .flat_map(|(schema, members, _)| {
                    members.iter().map(move |m| {
                        format!("{} depends on schema {schema}", m.description(schema))
                    })
                })
                .collect();
            return Err(DbError::new(message)
                .code("2BP01")
                .detail(detail.join("\n"))
                .hint("Use DROP ... CASCADE to drop the dependent objects too.")
                .into());
        }

        // What goes: each member, followed by what depends on it elsewhere.
        let member_ids: HashSet<TableId> = held
            .iter()
            .flat_map(|(_, members, _)| members.iter().map(|m| m.relation().id))
            .collect();
        let tables = self.catalog_reader.list_all_tables("default")?;
        let mut described = Vec::new();
        let mut external_relations: Vec<Dependent> = Vec::new();
        let mut external_types: Vec<user_types::Dependent> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        // Across the schemas, in the order they were made.
        let mut ordered: Vec<(&str, &Member)> = held
            .iter()
            .flat_map(|(schema, members, _)| members.iter().map(move |m| (schema.as_str(), m)))
            .collect();
        ordered.sort_by(|a, b| {
            a.1.relation()
                .created_at
                .cmp(&b.1.relation().created_at)
                .then(a.1.relation().name.cmp(&b.1.relation().name))
        });
        {
            for (schema, member) in ordered {
                described.push(member.description(schema));
                match member {
                    Member::Relation(table) => {
                        for dependent in self.dependents(std::slice::from_ref(table))? {
                            let key = match &dependent.object {
                                Dependent::View(view) if member_ids.contains(&view.id) => continue,
                                Dependent::Constraint { table, .. }
                                    if member_ids.contains(table) =>
                                {
                                    continue;
                                }
                                Dependent::View(view) => format!("view {}", view.id),
                                Dependent::Constraint { table, name } => {
                                    format!("constraint {table} {name}")
                                }
                            };
                            if seen.insert(key) {
                                described.push(dependent.object_description());
                                external_relations.push(dependent.object);
                            }
                        }
                    }
                    Member::Type(t, _) => {
                        let mut out = Vec::new();
                        user_types::dependents(&tables, t, &mut out);
                        for (dependent, _) in out {
                            let key = match &dependent {
                                user_types::Dependent::Column(table, _)
                                    if member_ids.contains(&table.id) =>
                                {
                                    continue;
                                }
                                user_types::Dependent::Type(d) if member_ids.contains(&d.id) => {
                                    continue;
                                }
                                user_types::Dependent::Column(table, column) => {
                                    format!("column {} {column}", table.id)
                                }
                                user_types::Dependent::Type(d) => format!("type {}", d.id),
                            };
                            if seen.insert(key) {
                                described.push(dependent.description());
                                external_types.push(dependent);
                            }
                        }
                    }
                }
            }
        }
        match described.as_slice() {
            [] => {}
            [only] => self.notice(ctx, DbError::new(format!("drop cascades to {only}"))),
            many => self.notice(
                ctx,
                DbError::new(format!("drop cascades to {} other objects", many.len())).detail(
                    many.iter()
                        .map(|d| format!("drop cascades to {d}"))
                        .collect::<Vec<_>>()
                        .join("\n"),
                ),
            ),
        }
        for dependent in external_relations {
            match dependent {
                Dependent::Constraint { table, name } => {
                    self.catalog_writer.update_table_descriptor(
                        TableDescriptorChange::DropConstraint {
                            table_id: table,
                            name,
                        },
                    )?;
                }
                Dependent::View(view) => self.drop_relation(&view)?,
            }
        }
        for dependent in external_types {
            match dependent {
                user_types::Dependent::Column(table, column) => {
                    if let Ok(current) = self.catalog_reader.get_table_by_id(table.id) {
                        self.drop_column(ctx, &current, &column, true, true)?;
                    }
                }
                user_types::Dependent::Type(d) => self.catalog_writer.drop_table(d.id)?,
            }
        }
        for (_, members, meta) in &held {
            for member in members {
                match member {
                    Member::Relation(table) => self.drop_relation(table)?,
                    Member::Type(_, table) => self.catalog_writer.drop_table(table.id)?,
                }
            }
            if let Some(meta) = meta {
                self.catalog_writer.drop_table(meta.id)?;
            }
        }
        for schema in &targets {
            self.catalog_writer.drop_schema(schema.id)?;
        }
        user_types::forget_found();
        Ok(QueryOutput::tag("DROP SCHEMA"))
    }

    /// `ALTER SCHEMA name RENAME TO new_name` (what it holds moves to a
    /// schema of the new name) or `OWNER TO role`.
    pub(crate) fn exec_alter_schema(
        &self,
        ctx: &ExecutionContext,
        name: String,
        new_name: Option<String>,
        owner: Option<String>,
    ) -> Result<QueryOutput> {
        let name = unquote(&name);
        let schema = self
            .catalog_reader
            .get_schema("default", &name)
            .map_err(|_| missing_schema(&name))?;
        self.authorize(ctx, Action::CreateSchema, ResourceRef::Schema(schema.id))?;
        if let Some(owner) = owner {
            if self.catalog_reader.get_principal_by_name(&owner).is_err() {
                return Err(DbError::new(format!("role \"{owner}\" does not exist"))
                    .code("42704")
                    .into());
            }
            return Ok(QueryOutput::tag("ALTER SCHEMA"));
        }
        let Some(new_name) = new_name.map(|n| unquote(&n)) else {
            return Ok(QueryOutput::tag("ALTER SCHEMA"));
        };
        check_schema_name(&new_name)?;
        if self.catalog_reader.get_schema("default", &new_name).is_ok() {
            return Err(
                DbError::new(format!("schema \"{new_name}\" already exists"))
                    .code("42P06")
                    .into(),
            );
        }
        let renamed = self
            .catalog_writer
            .create_schema(nodus_catalog::CreateSchemaRequest {
                id: nodus_catalog::SchemaId::new(),
                database_id: schema.database_id,
                name: new_name.clone(),
                owner_role_id: schema.owner_role_id,
                managed_access: schema.managed_access,
            })?;
        let tables = self.types_catalog.list_tables("default", &name)?;
        let moves: Vec<Move> = tables
            .iter()
            .map(|t| Move {
                old_schema: name.clone(),
                old_name: t.name.clone(),
                new_schema: new_name.clone(),
                new_name: t.name.clone(),
                visible: self.reached_unqualified(&name, &t.name),
                qualify: true,
                is_type: user_types::is_type_relation(t),
            })
            .collect();
        for table in &tables {
            self.catalog_writer
                .update_table_descriptor(TableDescriptorChange::SetSchema {
                    table_id: table.id,
                    schema_id: renamed.id,
                })?;
        }
        self.retarget(&moves)?;
        self.catalog_writer.drop_schema(schema.id)?;
        Ok(QueryOutput::tag("ALTER SCHEMA"))
    }

    /// `ALTER <kind> [IF EXISTS] name SET SCHEMA schema`: a relation moves
    /// with its indexes and the sequences its columns own, a type alone.
    pub(crate) fn exec_set_schema(
        &self,
        ctx: &ExecutionContext,
        kind: &str,
        name: String,
        schema: String,
        if_exists: bool,
    ) -> Result<QueryOutput> {
        let tag = match kind {
            "VIEW" => "ALTER VIEW",
            "MATERIALIZED VIEW" => "ALTER MATERIALIZED VIEW",
            "SEQUENCE" => "ALTER SEQUENCE",
            "TYPE" => "ALTER TYPE",
            "DOMAIN" => "ALTER DOMAIN",
            _ => "ALTER TABLE",
        };
        let target_name = crate::search_path::schema_named(&unquote(&schema)).to_string();
        let temporary = |schema: &str| -> Result<()> {
            if crate::search_path::is_temp_schema(schema) {
                return Err(
                    DbError::new("cannot move objects into or out of temporary schemas")
                        .code("0A000")
                        .into(),
                );
            }
            Ok(())
        };
        if matches!(kind, "TYPE" | "DOMAIN") {
            let Some(t) = user_types::lookup(&name) else {
                let bare = user_types::split_name(&name).1;
                if if_exists {
                    self.notice(
                        ctx,
                        DbError::new(format!("type \"{bare}\" does not exist, skipping")),
                    );
                    return Ok(QueryOutput::tag(tag));
                }
                return Err(DbError::new(format!("type \"{bare}\" does not exist"))
                    .code("42704")
                    .into());
            };
            if kind == "DOMAIN" && t.domain().is_none() {
                anyhow::bail!("\"{}\" is not a domain", t.name);
            }
            self.authorize(ctx, Action::CreateTable, ResourceRef::Table(t.id))?;
            let target = self
                .catalog_reader
                .get_schema("default", &target_name)
                .map_err(|_| missing_schema(&target_name))?;
            temporary(&target.name)?;
            if target.name == t.schema {
                return Ok(QueryOutput::tag(tag));
            }
            if self
                .types_catalog
                .get_table("default", &target.name, &t.name)
                .is_ok()
            {
                return Err(DbError::new(format!(
                    "type \"{}\" already exists in schema \"{}\"",
                    t.name, target.name
                ))
                .code("42710")
                .into());
            }
            let moved = Move {
                old_schema: t.schema.clone(),
                old_name: t.name.clone(),
                new_schema: target.name.clone(),
                new_name: t.name.clone(),
                visible: self.reached_unqualified(&t.schema, &t.name),
                qualify: true,
                is_type: true,
            };
            self.catalog_writer
                .update_table_descriptor(TableDescriptorChange::SetSchema {
                    table_id: t.id,
                    schema_id: target.id,
                })?;
            self.retarget(&[moved])?;
            return Ok(QueryOutput::tag(tag));
        }

        let (_, schema_name, relation) = crate::parse_object_name(&name)?;
        let Ok(table) = self
            .catalog_reader
            .get_table("default", schema_name, relation)
        else {
            if if_exists {
                self.notice(
                    ctx,
                    DbError::new(format!("relation \"{relation}\" does not exist, skipping")),
                );
                return Ok(QueryOutput::tag(tag));
            }
            return Err(
                DbError::new(format!("relation \"{relation}\" does not exist"))
                    .code("42P01")
                    .into(),
            );
        };
        let (fits, noun) = match kind {
            "VIEW" => (table.view_query.is_some(), "a view"),
            "MATERIALIZED VIEW" => (table.materialized_query.is_some(), "a materialized view"),
            "SEQUENCE" => (crate::sequences::is_sequence(&table), "a sequence"),
            _ => (true, "a table"),
        };
        if !fits {
            return Err(DbError::new(format!("\"{relation}\" is not {noun}"))
                .code("42809")
                .into());
        }
        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(table.id))?;
        let target = self
            .catalog_reader
            .get_schema("default", &target_name)
            .map_err(|_| missing_schema(&target_name))?;
        temporary(schema_name)?;
        temporary(&target.name)?;
        if target.name == schema_name {
            return Ok(QueryOutput::tag(tag));
        }
        if self
            .types_catalog
            .get_table("default", &target.name, &table.name)
            .is_ok()
        {
            return Err(DbError::new(format!(
                "relation \"{}\" already exists in schema \"{}\"",
                table.name, target.name
            ))
            .code("42P07")
            .into());
        }
        let mut moving = vec![table.clone()];
        for sequence in owned_sequences(&table) {
            if let Ok(owned) = self
                .types_catalog
                .get_table("default", schema_name, &sequence)
            {
                moving.push(owned);
            }
        }
        let moves: Vec<Move> = moving
            .iter()
            .map(|t| Move {
                old_schema: schema_name.to_string(),
                old_name: t.name.clone(),
                new_schema: target.name.clone(),
                new_name: t.name.clone(),
                visible: self.reached_unqualified(schema_name, &t.name),
                qualify: true,
                is_type: false,
            })
            .collect();
        for t in &moving {
            self.catalog_writer
                .update_table_descriptor(TableDescriptorChange::SetSchema {
                    table_id: t.id,
                    schema_id: target.id,
                })?;
        }
        self.retarget(&moves)?;
        Ok(QueryOutput::tag(tag))
    }

    /// A view's plan with the relations it reads named with their schemas,
    /// as they resolve when it is made: PostgreSQL binds a view to them
    /// then, so a later search path does not change what it reads.
    pub(crate) fn bind_view_plan(&self, query: &str) -> String {
        fn cte_names(value: &serde_json::Value, out: &mut HashSet<String>) {
            match value {
                serde_json::Value::Object(map) => {
                    if let Some(serde_json::Value::Array(ctes)) = map.get("ctes") {
                        for cte in ctes {
                            if let Some(name) = cte.get(0).and_then(|n| n.as_str()) {
                                out.insert(name.to_string());
                            }
                        }
                    }
                    map.values().for_each(|v| cte_names(v, out));
                }
                serde_json::Value::Array(items) => items.iter().for_each(|v| cte_names(v, out)),
                _ => {}
            }
        }
        fn walk(
            executor: &MemExecutor,
            value: &mut serde_json::Value,
            ctes: &HashSet<String>,
        ) -> bool {
            match value {
                serde_json::Value::Object(map) => {
                    let mut changed = false;
                    if let Some(name) = map.get("table_name").and_then(|v| v.as_str())
                        && !name.contains('.')
                        && !name.starts_with('\0')
                        && !ctes.contains(name)
                        && let Ok((db, schema, relation)) = crate::parse_object_name(name)
                        && !MemExecutor::is_virtual_schema(schema)
                        && executor
                            .catalog_reader
                            .get_table(db, schema, relation)
                            .is_ok()
                    {
                        let bound = format!(
                            "{}.{}",
                            user_types::quote(schema),
                            user_types::quote(relation)
                        );
                        map.insert("table_name".into(), serde_json::Value::String(bound));
                        changed = true;
                    }
                    for v in map.values_mut() {
                        changed |= walk(executor, v, ctes);
                    }
                    changed
                }
                serde_json::Value::Array(items) => {
                    let mut changed = false;
                    for item in items {
                        changed |= walk(executor, item, ctes);
                    }
                    changed
                }
                _ => false,
            }
        }
        let Ok(mut plan) = serde_json::from_str::<serde_json::Value>(query) else {
            return query.to_string();
        };
        let mut ctes = HashSet::new();
        cte_names(&plan, &mut ctes);
        if walk(self, &mut plan, &ctes) {
            plan.to_string()
        } else {
            query.to_string()
        }
    }

    /// A relation renamed: the views reading it follow it.
    pub(crate) fn retarget_renamed(
        &self,
        schema: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let moved = Move {
            old_schema: schema.to_string(),
            old_name: old_name.to_string(),
            new_schema: schema.to_string(),
            new_name: new_name.to_string(),
            visible: true,
            qualify: false,
            is_type: false,
        };
        self.retarget(&[moved])
    }

    /// Points what names moved objects by name at where they are now:
    /// views' plans and foreign keys (relations), columns' and domains'
    /// types (types), and columns' defaults (sequences).
    fn retarget(&self, moves: &[Move]) -> Result<()> {
        let relations: Vec<&Move> = moves.iter().filter(|m| !m.is_type).collect();
        let types: Vec<&Move> = moves.iter().filter(|m| m.is_type).collect();
        let retarget_type = |data_type: &str| -> Option<String> {
            match crate::value::array_element_type(data_type) {
                Some(element) => types
                    .iter()
                    .find_map(|m| m.retarget(element))
                    .map(|t| format!("{t}[]")),
                None => types.iter().find_map(|m| m.retarget(data_type)),
            }
        };
        for table in self.types_catalog.list_all_tables("default")? {
            if let Some(query) = table
                .view_query
                .as_ref()
                .or(table.materialized_query.as_ref())
                && let Some(query) = retarget_plan(query, &relations)
            {
                self.catalog_writer.update_table_descriptor(
                    TableDescriptorChange::SetViewQuery {
                        table_id: table.id,
                        query,
                    },
                )?;
            }
            for constraint in &table.constraints {
                let TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                } = constraint
                else {
                    continue;
                };
                let Some(retargeted) = relations.iter().find_map(|m| m.retarget(foreign_table))
                else {
                    continue;
                };
                let effective = constraint.effective_name(&table.name);
                self.catalog_writer.update_table_descriptor(
                    TableDescriptorChange::DropConstraint {
                        table_id: table.id,
                        name: effective.clone(),
                    },
                )?;
                self.catalog_writer.update_table_descriptor(
                    TableDescriptorChange::AddConstraint {
                        table_id: table.id,
                        constraint: TableConstraint::ForeignKey {
                            name: Some(name.clone().unwrap_or(effective)),
                            columns: columns.clone(),
                            foreign_table: retargeted,
                            referred_columns: referred_columns.clone(),
                            on_delete: *on_delete,
                            on_update: *on_update,
                        },
                    },
                )?;
            }
            for column in &table.columns {
                // A domain over a moved type.
                if column.name == user_types::TYPE_COLUMN {
                    if let Some(user_types::TypeDefinition::Domain(mut domain)) =
                        user_types::definition_of(&table)
                        && let Some(base) = retarget_type(&domain.base)
                    {
                        domain.base = base;
                        self.catalog_writer.update_table_descriptor(
                            TableDescriptorChange::AlterColumnType {
                                table_id: table.id,
                                column_name: column.name.clone(),
                                data_type: serde_json::to_string(
                                    &user_types::TypeDefinition::Domain(domain),
                                )?,
                            },
                        )?;
                    }
                    continue;
                }
                if let Some(data_type) = retarget_type(&column.data_type) {
                    self.catalog_writer.update_table_descriptor(
                        TableDescriptorChange::AlterColumnType {
                            table_id: table.id,
                            column_name: column.name.clone(),
                            data_type,
                        },
                    )?;
                }
                if let Some(default) = Self::column_default(column)
                    && let Some(sequence) = crate::sequences::default_sequence(&default)
                    && let Some(retargeted) = relations.iter().find_map(|m| m.retarget(&sequence))
                {
                    let current = self.types_catalog.get_table_by_id(table.id)?;
                    if let Some(mut replaced) =
                        current.columns.iter().find(|c| c.id == column.id).cloned()
                    {
                        replaced.default_expr = Some(serde_json::to_string(
                            &crate::sequences::with_default_sequence(&default, &retargeted),
                        )?);
                        self.catalog_writer.update_table_descriptor(
                            TableDescriptorChange::ReplaceColumn {
                                table_id: table.id,
                                column: replaced,
                            },
                        )?;
                    }
                }
            }
        }
        user_types::forget_found();
        Ok(())
    }

    /// `COMMENT ON SCHEMA`, kept as the comment of the schema's own hidden
    /// relation.
    pub(crate) fn exec_comment_schema(
        &self,
        ctx: &ExecutionContext,
        name: &str,
        comment: Option<String>,
    ) -> Result<QueryOutput> {
        let name = unquote(name);
        let schema = self
            .catalog_reader
            .get_schema("default", &name)
            .map_err(|_| missing_schema(&name))?;
        self.authorize(ctx, Action::CreateSchema, ResourceRef::Schema(schema.id))?;
        let (_, meta) = self.schema_members(&name)?;
        let meta = match meta {
            Some(meta) => meta,
            None => {
                let now = Utc::now();
                self.catalog_writer.create_table(CreateTableRequest {
                    id: TableId::new(),
                    database_id: schema.database_id,
                    schema_id: schema.id,
                    name: SCHEMA_COLUMN.to_string(),
                    columns: vec![ColumnDescriptor {
                        id: nodus_catalog::ColumnId::new(),
                        name: SCHEMA_COLUMN.to_string(),
                        version: 1,
                        created_at: now,
                        updated_at: now,
                        state: DescriptorState::Public,
                        data_type: "TEXT".to_string(),
                        nullable: true,
                        default_expr: None,
                        comment: None,
                    }],
                    constraints: vec![],
                    view_query: None,
                    materialized_query: None,
                })?
            }
        };
        self.catalog_writer
            .update_table_descriptor(TableDescriptorChange::SetComment {
                table_id: meta.id,
                column: None,
                comment: comment.filter(|c| !c.is_empty()),
            })?;
        Ok(QueryOutput::tag("COMMENT"))
    }

    /// `COMMENT ON TYPE` / `COMMENT ON DOMAIN`, kept as the comment of the
    /// type's relation.
    pub(crate) fn exec_comment_type(
        &self,
        ctx: &ExecutionContext,
        kind: &str,
        name: &str,
        comment: Option<String>,
    ) -> Result<QueryOutput> {
        let Some(t) = user_types::lookup(name) else {
            return Err(DbError::new(format!(
                "type \"{}\" does not exist",
                user_types::split_name(name).1
            ))
            .code("42704")
            .into());
        };
        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(t.id))?;
        if kind == "DOMAIN" && t.domain().is_none() {
            anyhow::bail!("\"{}\" is not a domain", t.name);
        }
        self.catalog_writer
            .update_table_descriptor(TableDescriptorChange::SetComment {
                table_id: t.id,
                column: None,
                comment: comment.filter(|c| !c.is_empty()),
            })?;
        Ok(QueryOutput::tag("COMMENT"))
    }
}

/// The comments on schemas and types, as `pg_description` rows (`objoid`,
/// `classoid`, `objsubid`, `description`), from the whole catalog.
pub(crate) fn schema_and_type_descriptions(
    catalog: &dyn nodus_catalog::CatalogReader,
    db_name: &str,
) -> Vec<Vec<Value>> {
    let Ok(tables) = catalog.list_all_tables(db_name) else {
        return Vec::new();
    };
    let schemas = catalog.list_schemas(db_name).unwrap_or_default();
    let mut rows = Vec::new();
    for table in tables {
        let Some(comment) = &table.comment else {
            continue;
        };
        let Some(schema) = schemas.iter().find(|s| s.id == table.schema_id) else {
            continue;
        };
        let (oid, class) = if is_schema_relation(&table) {
            (MemExecutor::schema_oid(db_name, &schema.name), 2615)
        } else if let Some(t) = UserType::of(&table, schema.name.clone()) {
            (t.oid(), 1247)
        } else {
            continue;
        };
        rows.push(vec![
            Value::Int(oid),
            Value::Int(class),
            Value::Int(0),
            Value::Text(comment.clone()),
        ]);
    }
    rows
}

/// `obj_description(oid, 'pg_namespace' | 'pg_type')`, in the statement's
/// catalog.
pub(crate) fn schema_or_type_description(oid: i64, class: &str) -> Option<String> {
    let class = match class {
        "pg_namespace" => 2615,
        "pg_type" => 1247,
        _ => return None,
    };
    let catalog = crate::session_env::with(|env| env.and_then(|e| e.types.clone()))?;
    schema_and_type_descriptions(catalog.as_ref(), "default")
        .into_iter()
        .find(|row| row[0] == Value::Int(oid) && row[1] == Value::Int(class))
        .and_then(|row| match &row[3] {
            Value::Text(text) => Some(text.clone()),
            _ => None,
        })
}
