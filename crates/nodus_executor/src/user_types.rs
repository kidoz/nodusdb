//! User-defined types: enums (`CREATE TYPE ... AS ENUM`) and domains
//! (`CREATE DOMAIN`).
//!
//! A type is kept as a relation of its schema that no query sees: its one
//! column, [`TYPE_COLUMN`], has the type's definition as its type. So a
//! type is stored and replicated as relations are, with no catalog format
//! of its own. [`RelationCatalog`] hides these relations from all but this
//! module, which reads them through the catalog it wraps.
//!
//! A type's values are those of its base: an enum's are its labels (text),
//! a domain's its base type's. An enum's labels compare in their sort order
//! ([`ENUM_SORT`]), and a domain's value must meet its constraints.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use nodus_catalog::{
    CatalogReader, ColumnDescriptor, CreateTableRequest, DescriptorState, TableDescriptor,
    TableDescriptorChange, TableId,
};
use serde::{Deserialize, Serialize};

use crate::error_fields::DbError;
use crate::{ExecutionContext, MemExecutor, QueryOutput, Value};

/// The column of a type's relation, whose type is the type's definition.
pub(crate) const TYPE_COLUMN: &str = "__nodus_type__";

/// `__ENUM_SORT__(value, type)`: an enum value's place in its type's order.
pub(crate) const ENUM_SORT: &str = "__ENUM_SORT__";

/// `__ENUM_LABEL__(place, type)`: the label at a place in an enum's order.
pub(crate) const ENUM_LABEL: &str = "__ENUM_LABEL__";

/// A type's definition, as its relation keeps it (externally tagged: a
/// buffered, internally tagged form would not read its numbers back).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypeDefinition {
    Enum { labels: Vec<EnumLabel> },
    Domain(DomainDefinition),
}

/// An enum's label and its place in the order (`pg_enum.enumsortorder`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumLabel {
    pub label: String,
    pub sort: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DomainDefinition {
    /// The base type, as declared.
    pub base: String,
    #[serde(default)]
    pub not_null: bool,
    /// The default's SQL.
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub checks: Vec<DomainCheck>,
}

/// A domain's `CHECK` constraint: its name and its SQL, over `value`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DomainCheck {
    pub name: String,
    pub sql: String,
}

/// What `ALTER TYPE` or `ALTER DOMAIN` changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypeChange {
    AddValue {
        label: String,
        if_not_exists: bool,
        /// The neighbor it goes before (`true`) or after.
        position: Option<(bool, String)>,
    },
    RenameValue {
        from: String,
        to: String,
    },
    Rename {
        new_name: String,
    },
    /// `SET DEFAULT` (its SQL) or `DROP DEFAULT`.
    SetDefault(Option<String>),
    SetNotNull(bool),
    AddConstraint {
        name: Option<String>,
        sql: String,
    },
    DropConstraint {
        name: String,
        if_exists: bool,
    },
    RenameConstraint {
        from: String,
        to: String,
    },
    /// What changes nothing NodusDB keeps (`OWNER TO`, `VALIDATE
    /// CONSTRAINT`).
    Nothing,
}

/// A user-defined type, found.
#[derive(Debug)]
pub(crate) struct UserType {
    pub(crate) id: TableId,
    pub(crate) schema: String,
    pub(crate) name: String,
    pub(crate) definition: TypeDefinition,
}

impl UserType {
    fn of(table: &TableDescriptor, schema: String) -> Option<Self> {
        Some(UserType {
            id: table.id,
            schema,
            name: table.name.clone(),
            definition: definition_of(table)?,
        })
    }

    pub(crate) fn oid(&self) -> i64 {
        MemExecutor::stable_oid(&format!("type:{}", self.id.0), 100_000)
    }

    pub(crate) fn array_oid(&self) -> i64 {
        MemExecutor::stable_oid(&format!("arraytype:{}", self.id.0), 100_000)
    }

    pub(crate) fn is_enum(&self) -> bool {
        matches!(self.definition, TypeDefinition::Enum { .. })
    }

    pub(crate) fn domain(&self) -> Option<&DomainDefinition> {
        match &self.definition {
            TypeDefinition::Domain(d) => Some(d),
            TypeDefinition::Enum { .. } => None,
        }
    }

    /// An enum's labels in their order.
    pub(crate) fn labels(&self) -> Vec<&EnumLabel> {
        let TypeDefinition::Enum { labels } = &self.definition else {
            return Vec::new();
        };
        let mut sorted: Vec<&EnumLabel> = labels.iter().collect();
        sorted.sort_by(|a, b| a.sort.total_cmp(&b.sort));
        sorted
    }

    /// The name as `format_type` prints it: qualified by its schema when
    /// that is not on the search path.
    pub(crate) fn display_name(&self) -> String {
        let name = quote(&self.name);
        if crate::search_path::existing_search_path().contains(&self.schema) {
            name
        } else {
            format!("{}.{name}", quote(&self.schema))
        }
    }
}

fn quote(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Whether a relation is a type's.
pub(crate) fn is_type_relation(table: &TableDescriptor) -> bool {
    table.view_query.is_none()
        && table.materialized_query.is_none()
        && table.columns.len() == 1
        && table.columns[0].name == TYPE_COLUMN
}

/// A type relation's definition.
pub(crate) fn definition_of(table: &TableDescriptor) -> Option<TypeDefinition> {
    if !is_type_relation(table) {
        return None;
    }
    serde_json::from_str(&table.columns[0].data_type).ok()
}

/// The catalog as everything but this module reads it: without the
/// relations that hold types.
pub(crate) struct RelationCatalog(pub(crate) Arc<dyn CatalogReader>);

impl RelationCatalog {
    fn visible(table: TableDescriptor) -> Result<TableDescriptor> {
        if is_type_relation(&table) {
            anyhow::bail!("relation \"{}\" does not exist", table.name);
        }
        Ok(table)
    }
}

impl CatalogReader for RelationCatalog {
    fn export_raft_catalog(&self) -> Result<Vec<u8>> {
        self.0.export_raft_catalog()
    }
    fn get_database(&self, name: &str) -> Result<nodus_catalog::DatabaseDescriptor> {
        self.0.get_database(name)
    }
    fn get_database_by_id(
        &self,
        id: nodus_catalog::DatabaseId,
    ) -> Result<nodus_catalog::DatabaseDescriptor> {
        self.0.get_database_by_id(id)
    }
    fn get_schema(&self, database: &str, schema: &str) -> Result<nodus_catalog::SchemaDescriptor> {
        self.0.get_schema(database, schema)
    }
    fn get_schema_by_id(
        &self,
        id: nodus_catalog::SchemaId,
    ) -> Result<nodus_catalog::SchemaDescriptor> {
        self.0.get_schema_by_id(id)
    }
    fn list_schemas(&self, database: &str) -> Result<Vec<nodus_catalog::SchemaDescriptor>> {
        self.0.list_schemas(database)
    }
    fn resolve_object(
        &self,
        request: nodus_catalog::ResolveObjectRequest,
    ) -> Result<nodus_catalog::ObjectDescriptor> {
        self.0.resolve_object(request)
    }
    fn get_table(&self, database: &str, schema: &str, table: &str) -> Result<TableDescriptor> {
        self.0
            .get_table(database, schema, table)
            .and_then(Self::visible)
    }
    fn get_table_by_id(&self, id: TableId) -> Result<TableDescriptor> {
        self.0.get_table_by_id(id).and_then(Self::visible)
    }
    fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableDescriptor>> {
        let mut tables = self.0.list_tables(database, schema)?;
        tables.retain(|t| !is_type_relation(t));
        Ok(tables)
    }
    fn list_all_tables(&self, database: &str) -> Result<Vec<TableDescriptor>> {
        let mut tables = self.0.list_all_tables(database)?;
        tables.retain(|t| !is_type_relation(t));
        Ok(tables)
    }
    fn get_principal_by_name(&self, name: &str) -> Result<nodus_catalog::PrincipalDescriptor> {
        self.0.get_principal_by_name(name)
    }
    fn get_principal_by_id(
        &self,
        id: nodus_catalog::PrincipalId,
    ) -> Result<nodus_catalog::PrincipalDescriptor> {
        self.0.get_principal_by_id(id)
    }
    fn get_cluster_version(&self) -> Result<nodus_catalog::ClusterVersion> {
        self.0.get_cluster_version()
    }
    fn get_grants_for_resource(
        &self,
        resource: nodus_catalog::ResourceRef,
    ) -> Result<Vec<nodus_catalog::GrantDescriptor>> {
        self.0.get_grants_for_resource(resource)
    }
    fn get_grant_by_id(
        &self,
        id: nodus_catalog::GrantId,
    ) -> Result<nodus_catalog::GrantDescriptor> {
        self.0.get_grant_by_id(id)
    }
    fn get_effective_roles(
        &self,
        principal: nodus_catalog::PrincipalId,
    ) -> Result<Vec<nodus_catalog::RoleId>> {
        self.0.get_effective_roles(principal)
    }
    fn get_effective_principals(
        &self,
        principal: nodus_catalog::PrincipalId,
    ) -> Result<Vec<nodus_catalog::PrincipalId>> {
        self.0.get_effective_principals(principal)
    }
    fn list_principals(&self) -> Result<Vec<nodus_catalog::PrincipalDescriptor>> {
        self.0.list_principals()
    }
    fn list_grants(&self) -> Result<Vec<nodus_catalog::GrantDescriptor>> {
        self.0.list_grants()
    }
    fn export_snapshot(&self) -> nodus_catalog::CatalogSnapshot {
        self.0.export_snapshot()
    }
}

thread_local! {
    /// Types looked up in the current statement, by the name used.
    static FOUND: RefCell<HashMap<String, Option<Arc<UserType>>>> = RefCell::new(HashMap::new());
    /// Every type, once the statement has asked for them all.
    static ALL: RefCell<Option<Arc<Vec<Arc<UserType>>>>> = const { RefCell::new(None) };
}

/// Forgets the types looked up: a statement starts with the catalog as it
/// is then.
pub(crate) fn forget_found() {
    FOUND.with(|f| f.borrow_mut().clear());
    ALL.with(|a| *a.borrow_mut() = None);
}

/// Built-in type names (upper case, without modifiers), never a user
/// type's.
fn is_builtin(upper: &str) -> bool {
    matches!(
        upper.trim_start_matches("PG_CATALOG."),
        "INT"
            | "INTEGER"
            | "INT2"
            | "INT4"
            | "INT8"
            | "SMALLINT"
            | "BIGINT"
            | "SERIAL"
            | "SERIAL2"
            | "SERIAL4"
            | "SERIAL8"
            | "SMALLSERIAL"
            | "BIGSERIAL"
            | "TEXT"
            | "VARCHAR"
            | "CHAR"
            | "CHARACTER"
            | "CHARACTER VARYING"
            | "CHAR VARYING"
            | "BPCHAR"
            | "NAME"
            | "BOOL"
            | "BOOLEAN"
            | "REAL"
            | "FLOAT"
            | "FLOAT4"
            | "FLOAT8"
            | "DOUBLE"
            | "DOUBLE PRECISION"
            | "NUMERIC"
            | "DECIMAL"
            | "DEC"
            | "DATE"
            | "TIME"
            | "TIMETZ"
            | "TIMESTAMP"
            | "TIMESTAMPTZ"
            | "TIME WITH TIME ZONE"
            | "TIME WITHOUT TIME ZONE"
            | "TIMESTAMP WITH TIME ZONE"
            | "TIMESTAMP WITHOUT TIME ZONE"
            | "INTERVAL"
            | "UUID"
            | "JSON"
            | "JSONB"
            | "BYTEA"
            | "OID"
            | "XID"
            | "BIT"
            | "VARBIT"
            | "BIT VARYING"
            | "REGCLASS"
            | "REGTYPE"
            | "REGPROC"
            | "REGPROCEDURE"
            | "REGNAMESPACE"
            | "REGROLE"
            | "REGOPER"
            | "REGOPERATOR"
            | "REGCONFIG"
            | "REGDICTIONARY"
            | "INET"
            | "CIDR"
            | "MACADDR"
            | "MACADDR8"
            | "MONEY"
            | "XML"
            | "TSVECTOR"
            | "TSQUERY"
            | "POINT"
            | "LINE"
            | "LSEG"
            | "BOX"
            | "PATH"
            | "POLYGON"
            | "CIRCLE"
            | "VOID"
            | "RECORD"
            | "UNKNOWN"
            | "ANYELEMENT"
            | "ANYARRAY"
            | "INT4RANGE"
            | "INT8RANGE"
            | "NUMRANGE"
            | "TSRANGE"
            | "TSTZRANGE"
            | "DATERANGE"
            | "PG_CHAR"
            | "STRING"
            | "PG_LSN"
            | "TXID_SNAPSHOT"
            | "PG_SNAPSHOT"
            | "JSONPATH"
    )
}

/// Whether a declared type could name a user type (or an array of one):
/// what planning, which cannot look, leaves for the statement to find out.
pub(crate) fn may_be_user_type(data_type: &str) -> bool {
    let text = data_type.trim().trim_end_matches("[]").trim_end();
    !text.is_empty() && !text.contains('(') && !is_builtin(&text.to_ascii_uppercase())
}

/// The user type a declared type names, if it names one (not an array of
/// one). Found along the search path, once per statement.
pub(crate) fn lookup(data_type: &str) -> Option<Arc<UserType>> {
    // Planning, outside a statement, cannot look (nor remember not finding).
    if !crate::session_env::with(|env| env.is_some()) {
        return None;
    }
    if let Some(hit) = FOUND.with(|f| f.borrow().get(data_type).cloned()) {
        return hit;
    }
    let found = find(data_type);
    FOUND.with(|f| {
        f.borrow_mut().insert(data_type.to_string(), found.clone());
    });
    found
}

fn find(data_type: &str) -> Option<Arc<UserType>> {
    let text = data_type.trim();
    if text.is_empty()
        || text.ends_with(']')
        || text.contains('(')
        || is_builtin(&text.to_ascii_uppercase())
    {
        return None;
    }
    let catalog = crate::session_env::with(|env| env.and_then(|e| e.types.clone()))?;
    let (schema, name) = split_name(text);
    let schemas: Vec<String> = match schema {
        Some(schema) => vec![crate::search_path::schema_named(&schema).to_string()],
        None => crate::search_path::temp_schema()
            .map(str::to_string)
            .into_iter()
            .chain(crate::search_path::existing_search_path())
            .collect(),
    };
    schemas.into_iter().find_map(|schema| {
        let table = catalog.get_table("default", &schema, &name).ok()?;
        UserType::of(&table, schema).map(Arc::new)
    })
}

/// A possibly schema-qualified name's parts, unquoted.
fn split_name(text: &str) -> (Option<String>, String) {
    let unquote = |s: &str| s.trim().trim_matches('"').replace("\"\"", "\"");
    match text.rsplit_once('.') {
        Some((schema, name)) if !name.contains('"') || name.starts_with('"') => {
            (Some(unquote(schema)), unquote(name))
        }
        _ => (None, unquote(text)),
    }
}

/// Every type of the database.
pub(crate) fn all_types() -> Arc<Vec<Arc<UserType>>> {
    if let Some(all) = ALL.with(|a| a.borrow().clone()) {
        return all;
    }
    let Some(catalog) = crate::session_env::with(|env| env.and_then(|e| e.types.clone())) else {
        return Arc::new(Vec::new());
    };
    let schemas = catalog.list_schemas("default").unwrap_or_default();
    let all: Vec<Arc<UserType>> = catalog
        .list_all_tables("default")
        .unwrap_or_default()
        .iter()
        .filter_map(|t| {
            let schema = schemas.iter().find(|s| s.id == t.schema_id)?.name.clone();
            UserType::of(t, schema).map(Arc::new)
        })
        .collect();
    let all = Arc::new(all);
    ALL.with(|a| *a.borrow_mut() = Some(all.clone()));
    all
}

/// The OID of a declared type that is a user type or an array of one.
pub(crate) fn type_oid(data_type: &str) -> Option<i64> {
    match crate::value::array_element_type(data_type) {
        Some(element) => lookup(element).map(|t| t.array_oid()),
        None => lookup(data_type).map(|t| t.oid()),
    }
}

/// The user type (or array of one, `true`) with an OID.
pub(crate) fn type_of_oid(oid: i64) -> Option<(Arc<UserType>, bool)> {
    if oid < 100_000 {
        return None;
    }
    all_types().iter().find_map(|t| {
        if t.oid() == oid {
            Some((t.clone(), false))
        } else if t.array_oid() == oid {
            Some((t.clone(), true))
        } else {
            None
        }
    })
}

/// A declared type with its domains resolved to their base types, as
/// operators and the wire read it.
pub(crate) fn base_type(data_type: &str) -> String {
    let mut current = data_type.to_string();
    for _ in 0..16 {
        let next = match crate::value::array_element_type(&current) {
            Some(element) => match lookup(element).and_then(|t| t.domain().map(|d| d.base.clone()))
            {
                Some(base) => format!("{base}[]"),
                None => return current,
            },
            None => match lookup(&current).and_then(|t| t.domain().map(|d| d.base.clone())) {
                Some(base) => base,
                None => return current,
            },
        };
        current = next;
    }
    current
}

/// The enum a declared type is, if it is one.
pub(crate) fn enum_type(data_type: &str) -> Option<Arc<UserType>> {
    lookup(&base_type(data_type)).filter(|t| t.is_enum())
}

/// A domain's default, as an expression, for a column of it without one.
pub(crate) fn domain_default(data_type: &str) -> Option<crate::ScalarExpr> {
    let t = lookup(data_type)?;
    let domain = t.domain()?;
    match &domain.default {
        Some(sql) => crate::index_keys::parse_expression(sql),
        None => domain_default(&domain.base),
    }
}

/// A value as a user type takes it: stored in a column of the type
/// (`explicit` false), or cast to it. An enum takes one of its labels; a
/// domain its base type's value, which must meet its constraints.
pub(crate) fn coerce(t: &UserType, value: &Value, explicit: bool) -> Result<Value, String> {
    match &t.definition {
        TypeDefinition::Enum { labels } => {
            let text = match value {
                Value::Null => return Ok(Value::Null),
                Value::Text(s) => s.clone(),
                other => crate::value::render(other),
            };
            if labels.iter().any(|l| l.label == text) {
                Ok(Value::Text(text))
            } else {
                Err(DbError::new(format!(
                    "invalid input value for enum {}: \"{text}\"",
                    t.name
                ))
                .code("22P02")
                .into_text())
            }
        }
        TypeDefinition::Domain(domain) => {
            let value = match value {
                Value::Null => Value::Null,
                _ if explicit => crate::planner::try_cast(value.clone(), &domain.base)?,
                // An error storing the base value fails the statement.
                _ => crate::value::coerce_for_column(value, &domain.base),
            };
            check_domain(t, domain, &value)?;
            Ok(value)
        }
    }
}

/// Rejects a value a domain's constraints refuse.
fn check_domain(t: &UserType, domain: &DomainDefinition, value: &Value) -> Result<(), String> {
    if matches!(value, Value::Null) && domain.not_null {
        return Err(
            DbError::new(format!("domain {} does not allow null values", t.name))
                .code("23502")
                .schema(&t.schema)
                .datatype(&t.name)
                .into_text(),
        );
    }
    let names = ["value".to_string()];
    for check in &domain.checks {
        let Some(expr) = crate::index_keys::parse_expression(&check.sql) else {
            continue;
        };
        if crate::eval_scalar_expr(&expr, std::slice::from_ref(value), &names) == Value::Bool(false)
        {
            return Err(DbError::new(format!(
                "value for domain {} violates check constraint \"{}\"",
                t.name, check.name
            ))
            .code("23514")
            .schema(&t.schema)
            .datatype(&t.name)
            .constraint(&check.name)
            .into_text());
        }
    }
    Ok(())
}

/// The functions of enums, called by name: `None` for any other.
pub(crate) fn call(name: &str, args: &[Value]) -> Option<Value> {
    let type_name = |v: &Value| crate::value::render(v);
    Some(match (name, args) {
        (ENUM_SORT, [value, t]) => enum_sort(value, &type_name(t)),
        (ENUM_LABEL, [value, t]) => enum_label(value, &type_name(t)),
        ("ENUM_RANGE" | "ENUM_FIRST" | "ENUM_LAST", _) => enum_function(name, args),
        _ => return None,
    })
}

/// `__ENUM_SORT__(value, type)`: a label's place in its enum's order.
pub(crate) fn enum_sort(value: &Value, type_name: &str) -> Value {
    let Some(t) = enum_type(type_name) else {
        return value.clone();
    };
    let text = match value {
        Value::Null => return Value::Null,
        Value::Text(s) => s.clone(),
        other => crate::value::render(other),
    };
    match t.labels().iter().find(|l| l.label == text) {
        Some(label) => Value::Float(label.sort),
        None => crate::eval_error::raise(format!(
            "invalid input value for enum {}: \"{text}\"",
            t.name
        )),
    }
}

/// `__ENUM_LABEL__(place, type)`: the label at a place in an enum's order.
pub(crate) fn enum_label(value: &Value, type_name: &str) -> Value {
    let (Some(t), Value::Float(sort)) = (enum_type(type_name), value) else {
        return value.clone();
    };
    t.labels()
        .iter()
        .find(|l| l.sort == *sort)
        .map_or(Value::Null, |l| Value::Text(l.label.clone()))
}

/// `enum_range(a [, b])`, `enum_first(a)`, and `enum_last(a)`, the enum
/// given as the last argument.
pub(crate) fn enum_function(name: &str, args: &[Value]) -> Value {
    let Some((Value::Text(type_name), values)) = args.split_last() else {
        return Value::Null;
    };
    let Some(t) = enum_type(type_name) else {
        return crate::eval_error::raise(format!(
            "function {}({}) does not exist",
            name.to_ascii_lowercase(),
            type_name
        ));
    };
    let labels = t.labels();
    let text = |l: &&EnumLabel| Value::Text(l.label.clone());
    let place = |v: &Value| -> Option<usize> {
        match v {
            Value::Text(s) => labels.iter().position(|l| l.label == *s),
            _ => None,
        }
    };
    match (name, values) {
        ("ENUM_FIRST", [_]) => labels.first().map_or(Value::Null, text),
        ("ENUM_LAST", [_]) => labels.last().map_or(Value::Null, text),
        ("ENUM_RANGE", [_]) => Value::Array(labels.iter().map(text).collect()),
        ("ENUM_RANGE", [low, high]) => {
            let from = place(low).unwrap_or(0);
            let to = place(high).map_or(labels.len(), |p| p + 1);
            Value::Array(
                labels
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i >= from && *i < to)
                    .map(|(_, l)| text(&l))
                    .collect(),
            )
        }
        _ => Value::Null,
    }
}

/// The column types that are a type or arrays of it (`true`), by table:
/// what depends on the type.
fn dependent_columns(
    tables: &[TableDescriptor],
    t: &UserType,
) -> Vec<(TableDescriptor, String, bool)> {
    let names = |data_type: &str| names_type(data_type, t);
    let mut arrays = Vec::new();
    let mut direct = Vec::new();
    for table in tables {
        for column in &table.columns {
            match crate::value::array_element_type(&column.data_type) {
                Some(element) if names(element) => {
                    arrays.push((table.clone(), column.name.clone(), true));
                }
                None if names(&column.data_type) => {
                    direct.push((table.clone(), column.name.clone(), false));
                }
                _ => {}
            }
        }
    }
    // As PostgreSQL reports them: the array columns, then the others, each
    // latest first.
    arrays.reverse();
    direct.reverse();
    arrays.extend(direct);
    arrays
}

/// Whether a declared type names a type (not an array of it).
fn names_type(data_type: &str, t: &UserType) -> bool {
    let (schema, name) = split_name(data_type.trim());
    name == t.name && schema.is_none_or(|s| s == t.schema)
}

/// What depends on a type, as PostgreSQL reports it: the columns of arrays
/// of it, the domains over it (each followed by what depends on it), then
/// its columns, each group latest first. Each object with the type it
/// depends on (`posint[]`).
enum Dependent {
    Column(TableDescriptor, String),
    Type(Arc<UserType>),
}

fn dependents(tables: &[TableDescriptor], t: &UserType, out: &mut Vec<(Dependent, String)>) {
    let columns = dependent_columns(tables, t);
    for (table, column, array) in columns.iter().filter(|c| c.2) {
        let _ = array;
        out.push((
            Dependent::Column(table.clone(), column.clone()),
            format!("{}[]", t.name),
        ));
    }
    let mut domains: Vec<Arc<UserType>> = all_types()
        .iter()
        .filter(|d| d.domain().is_some_and(|dd| names_type(&dd.base, t)))
        .cloned()
        .collect();
    domains.reverse();
    for domain in domains {
        out.push((Dependent::Type(domain.clone()), t.name.clone()));
        dependents(tables, &domain, out);
    }
    for (table, column, _) in columns.iter().filter(|c| !c.2) {
        out.push((
            Dependent::Column(table.clone(), column.clone()),
            t.name.clone(),
        ));
    }
}

impl Dependent {
    fn description(&self) -> String {
        match self {
            Dependent::Column(table, column) => format!("column {column} of table {}", table.name),
            Dependent::Type(t) => format!("type {}", t.name),
        }
    }
}

impl MemExecutor {
    /// The type a statement names, found along the search path, or
    /// PostgreSQL's error when there is none.
    fn named_type(&self, name: &str) -> Result<Arc<UserType>> {
        lookup(name).ok_or_else(|| {
            DbError::new(format!("type \"{}\" does not exist", split_name(name).1))
                .code("42704")
                .into()
        })
    }

    /// Refuses a relation named as a type of its schema is: a relation
    /// has a type of its own name, as in PostgreSQL.
    pub(crate) fn reject_type_name(&self, schema: &str, name: &str) -> Result<()> {
        if self
            .types_catalog
            .get_table("default", schema, name)
            .is_ok_and(|t| is_type_relation(&t))
        {
            return Err(DbError::new(format!("type \"{name}\" already exists"))
                .code("42710")
                .hint("A relation has an associated type of the same name, so you must use a name that doesn't conflict with any existing type.")
                .into());
        }
        Ok(())
    }

    /// Writes a type's changed definition.
    fn store_definition(&self, t: &UserType, definition: &TypeDefinition) -> Result<()> {
        self.catalog_writer
            .update_table_descriptor(TableDescriptorChange::AlterColumnType {
                table_id: t.id,
                column_name: TYPE_COLUMN.to_string(),
                data_type: serde_json::to_string(definition)?,
            })?;
        forget_found();
        Ok(())
    }

    /// `CREATE TYPE name AS ENUM (...)` or `CREATE DOMAIN`.
    pub(crate) fn exec_create_type(
        &self,
        ctx: &ExecutionContext,
        name: String,
        mut definition: TypeDefinition,
    ) -> Result<QueryOutput> {
        let domain = matches!(definition, TypeDefinition::Domain(_));
        let (qualifier, type_name) = split_name(&name);
        let schema_name = match &qualifier {
            Some(schema) => crate::search_path::schema_named(schema).to_string(),
            None => crate::search_path::schema_for(&type_name).to_string(),
        };
        let db = self.catalog_reader.get_database("default")?;
        let schema = self.catalog_reader.get_schema("default", &schema_name)?;
        self.authorize(
            ctx,
            nodus_authz::Action::CreateTable,
            nodus_catalog::ResourceRef::Schema(schema.id),
        )?;
        if self
            .types_catalog
            .get_table("default", &schema_name, &type_name)
            .is_ok()
        {
            return Err(DbError::new(format!("type \"{type_name}\" already exists"))
                .code("42710")
                .into());
        }
        let id = TableId::new();
        match &mut definition {
            TypeDefinition::Enum { labels } => {
                let oid = UserType {
                    id,
                    schema: schema_name.clone(),
                    name: type_name.clone(),
                    definition: TypeDefinition::Enum { labels: Vec::new() },
                }
                .oid();
                for (i, label) in labels.iter().enumerate() {
                    check_label(&label.label)?;
                    if labels[..i].iter().any(|l| l.label == label.label) {
                        return Err(DbError::new(
                            "duplicate key value violates unique constraint \"pg_enum_typid_label_index\"",
                        )
                        .code("23505")
                        .detail(format!(
                            "Key (enumtypid, enumlabel)=({oid}, {}) already exists.",
                            label.label
                        ))
                        .schema("pg_catalog")
                        .table("pg_enum")
                        .constraint("pg_enum_typid_label_index")
                        .into());
                    }
                }
            }
            TypeDefinition::Domain(d) => {
                // Unnamed checks are `<domain>_check`, then numbered.
                let mut taken: Vec<String> = d
                    .checks
                    .iter()
                    .filter(|c| !c.name.is_empty())
                    .map(|c| c.name.clone())
                    .collect();
                for check in d.checks.iter_mut().filter(|c| c.name.is_empty()) {
                    check.name = unused_name(&format!("{type_name}_check"), &taken);
                    taken.push(check.name.clone());
                }
            }
        }
        let now = Utc::now();
        self.catalog_writer.create_table(CreateTableRequest {
            id,
            database_id: db.id,
            schema_id: schema.id,
            name: type_name,
            columns: vec![ColumnDescriptor {
                id: nodus_catalog::ColumnId::new(),
                name: TYPE_COLUMN.to_string(),
                version: 1,
                created_at: now,
                updated_at: now,
                state: DescriptorState::Public,
                data_type: serde_json::to_string(&definition)?,
                nullable: true,
                default_expr: None,
                comment: None,
            }],
            constraints: vec![],
            view_query: None,
            materialized_query: None,
        })?;
        forget_found();
        Ok(QueryOutput::tag(if domain {
            "CREATE DOMAIN"
        } else {
            "CREATE TYPE"
        }))
    }

    /// `ALTER TYPE` (of an enum, or its name) or `ALTER DOMAIN`.
    pub(crate) fn exec_alter_type(
        &self,
        ctx: &ExecutionContext,
        name: String,
        change: TypeChange,
        domain: bool,
    ) -> Result<QueryOutput> {
        let t = self.named_type(&name)?;
        let tag = if domain { "ALTER DOMAIN" } else { "ALTER TYPE" };
        if domain && t.domain().is_none() {
            anyhow::bail!("\"{}\" is not a domain", t.name);
        }
        let not_enum = || -> anyhow::Error {
            DbError::new(format!("\"{}\" is not an enum", t.name))
                .code("42809")
                .into()
        };
        let mut definition = t.definition.clone();
        match change {
            TypeChange::Nothing => return Ok(QueryOutput::tag(tag)),
            TypeChange::Rename { new_name } => {
                self.rename_type(&t, &new_name)?;
                return Ok(QueryOutput::tag(tag));
            }
            TypeChange::AddValue {
                label,
                if_not_exists,
                position,
            } => {
                let TypeDefinition::Enum { labels } = &mut definition else {
                    return Err(not_enum());
                };
                check_label(&label)?;
                if labels.iter().any(|l| l.label == label) {
                    if if_not_exists {
                        self.notice(
                            ctx,
                            DbError::new(format!(
                                "enum label \"{label}\" already exists, skipping"
                            ))
                            .code("42710"),
                        );
                        return Ok(QueryOutput::tag(tag));
                    }
                    return Err(
                        DbError::new(format!("enum label \"{label}\" already exists"))
                            .code("42710")
                            .into(),
                    );
                }
                let mut order: Vec<f64> = labels.iter().map(|l| l.sort).collect();
                order.sort_by(f64::total_cmp);
                let sort = match position {
                    None => order.last().map_or(1.0, |last| last + 1.0),
                    Some((before, neighbor)) => {
                        let Some(at) = labels.iter().find(|l| l.label == neighbor).map(|l| l.sort)
                        else {
                            return Err(DbError::new(format!(
                                "\"{neighbor}\" is not an existing enum label"
                            ))
                            .code("22023")
                            .into());
                        };
                        let place = order.iter().position(|s| *s == at).unwrap_or(0);
                        if before {
                            match place.checked_sub(1).map(|p| order[p]) {
                                Some(previous) => (previous + at) / 2.0,
                                None => at - 1.0,
                            }
                        } else {
                            match order.get(place + 1) {
                                Some(next) => (at + next) / 2.0,
                                None => at + 1.0,
                            }
                        }
                    }
                };
                labels.push(EnumLabel { label, sort });
            }
            TypeChange::RenameValue { from, to } => {
                let TypeDefinition::Enum { labels } = &mut definition else {
                    return Err(not_enum());
                };
                if labels.iter().any(|l| l.label == to) {
                    return Err(DbError::new(format!("enum label \"{to}\" already exists"))
                        .code("42710")
                        .into());
                }
                let Some(label) = labels.iter_mut().find(|l| l.label == from) else {
                    return Err(
                        DbError::new(format!("\"{from}\" is not an existing enum label"))
                            .code("22023")
                            .into(),
                    );
                };
                check_label(&to)?;
                label.label = to.clone();
                self.store_definition(&t, &definition)?;
                self.relabel_rows(ctx, &t, &from, &to)?;
                return Ok(QueryOutput::tag(tag));
            }
            TypeChange::SetDefault(default) => {
                let TypeDefinition::Domain(d) = &mut definition else {
                    anyhow::bail!("\"{}\" is not a domain", t.name);
                };
                d.default = default;
            }
            TypeChange::SetNotNull(not_null) => {
                let TypeDefinition::Domain(d) = &mut definition else {
                    anyhow::bail!("\"{}\" is not a domain", t.name);
                };
                if not_null && !d.not_null {
                    for (table, column, values) in self.domain_values(ctx, &t)? {
                        if values.iter().any(|v| matches!(v, Value::Null)) {
                            return Err(DbError::new(format!(
                                "column \"{column}\" of table \"{}\" contains null values",
                                table.name
                            ))
                            .code("23502")
                            .schema(self.schema_name_of(&table))
                            .table(&table.name)
                            .column(&column)
                            .into());
                        }
                    }
                }
                d.not_null = not_null;
            }
            TypeChange::AddConstraint { name, sql } => {
                let TypeDefinition::Domain(d) = &mut definition else {
                    anyhow::bail!("\"{}\" is not a domain", t.name);
                };
                let taken: Vec<String> = d.checks.iter().map(|c| c.name.clone()).collect();
                let name = match name {
                    Some(name) if taken.contains(&name) => {
                        return Err(DbError::new(format!(
                            "constraint \"{name}\" for domain \"{}\" already exists",
                            t.name
                        ))
                        .code("42710")
                        .into());
                    }
                    Some(name) => name,
                    None => unused_name(&format!("{}_check", t.name), &taken),
                };
                let check = DomainCheck { name, sql };
                // The values the domain's columns hold must meet it.
                let probe = UserType {
                    id: t.id,
                    schema: t.schema.clone(),
                    name: t.name.clone(),
                    definition: TypeDefinition::Domain(DomainDefinition {
                        base: d.base.clone(),
                        not_null: false,
                        default: None,
                        checks: vec![check.clone()],
                    }),
                };
                for (table, column, values) in self.domain_values(ctx, &t)? {
                    if values.iter().any(|v| {
                        check_domain(&probe, probe.domain().expect("a domain"), v).is_err()
                    }) {
                        return Err(DbError::new(format!(
                            "column \"{column}\" of table \"{}\" contains values that violate the new constraint",
                            table.name
                        ))
                        .code("23514")
                        .schema(self.schema_name_of(&table))
                        .table(&table.name)
                        .column(&column)
                        .into());
                    }
                }
                d.checks.push(check);
            }
            TypeChange::DropConstraint { name, if_exists } => {
                let TypeDefinition::Domain(d) = &mut definition else {
                    anyhow::bail!("\"{}\" is not a domain", t.name);
                };
                let before = d.checks.len();
                d.checks.retain(|c| c.name != name);
                if d.checks.len() == before {
                    let missing = format!(
                        "constraint \"{name}\" of domain \"{}\" does not exist",
                        t.name
                    );
                    if if_exists {
                        self.notice(ctx, DbError::new(format!("{missing}, skipping")));
                        return Ok(QueryOutput::tag(tag));
                    }
                    return Err(DbError::new(missing).code("42704").into());
                }
            }
            TypeChange::RenameConstraint { from, to } => {
                let TypeDefinition::Domain(d) = &mut definition else {
                    anyhow::bail!("\"{}\" is not a domain", t.name);
                };
                let Some(check) = d.checks.iter_mut().find(|c| c.name == from) else {
                    return Err(DbError::new(format!(
                        "constraint \"{from}\" for domain {} does not exist",
                        t.name
                    ))
                    .code("42704")
                    .into());
                };
                check.name = to;
            }
        }
        self.store_definition(&t, &definition)?;
        Ok(QueryOutput::tag(tag))
    }

    /// The values the columns of a domain hold: each column's table, name,
    /// and values. A column of an array of it cannot be checked, which
    /// refuses the change.
    fn domain_values(
        &self,
        ctx: &ExecutionContext,
        t: &UserType,
    ) -> Result<Vec<(TableDescriptor, String, Vec<Value>)>> {
        let tables = self.catalog_reader.list_all_tables("default")?;
        let mut out = Vec::new();
        let columns = dependent_columns(&tables, t);
        if let Some((table, column, _)) = columns.iter().find(|c| c.2) {
            return Err(DbError::new(format!(
                "cannot alter type \"{}\" because column \"{}.{column}\" uses it",
                t.name, table.name
            ))
            .code("0A000")
            .into());
        }
        for (table, column, _) in columns {
            if table.view_query.is_some() {
                continue;
            }
            let Some(at) = table.columns.iter().position(|c| c.name == column) else {
                continue;
            };
            let values = self
                .scan_rows(table.id, &ctx.session_id)?
                .into_iter()
                .map(|row| row.get(at).cloned().unwrap_or(Value::Null))
                .collect();
            out.push((table, column, values));
        }
        Ok(out)
    }

    /// Renames an enum's label in the rows holding it.
    fn relabel_rows(
        &self,
        ctx: &ExecutionContext,
        t: &UserType,
        from: &str,
        to: &str,
    ) -> Result<()> {
        let tables = self.catalog_reader.list_all_tables("default")?;
        let relabel = |v: &Value| match v {
            Value::Text(s) if s == from => Value::Text(to.to_string()),
            other => other.clone(),
        };
        for (table, column, array) in dependent_columns(&tables, t) {
            if table.view_query.is_some() || table.materialized_query.is_some() {
                continue;
            }
            let Some(at) = table.columns.iter().position(|c| c.name == column) else {
                continue;
            };
            let current = self.catalog_reader.get_table_by_id(table.id)?;
            for (key, row) in self.scan_rows_keyed(table.id, &ctx.session_id)? {
                let old = row.get(at).cloned().unwrap_or(Value::Null);
                let new = match &old {
                    Value::Array(items) if array => {
                        Value::Array(items.iter().map(relabel).collect())
                    }
                    other if !array => relabel(other),
                    other => other.clone(),
                };
                if new != old {
                    let mut updated = row.clone();
                    updated[at] = new;
                    self.replace_row(ctx, &current, &key, &row, &updated)?;
                }
            }
        }
        Ok(())
    }

    /// `ALTER TYPE name RENAME TO new_name`: the columns of the type follow.
    fn rename_type(&self, t: &UserType, new_name: &str) -> Result<()> {
        if self
            .types_catalog
            .get_table("default", &t.schema, new_name)
            .is_ok()
        {
            return Err(DbError::new(format!("type \"{new_name}\" already exists"))
                .code("42710")
                .into());
        }
        let tables = self.catalog_reader.list_all_tables("default")?;
        for (table, column, array) in dependent_columns(&tables, t) {
            let data_type = if array {
                format!("{}[]", quote(new_name))
            } else {
                quote(new_name)
            };
            self.catalog_writer.update_table_descriptor(
                TableDescriptorChange::AlterColumnType {
                    table_id: table.id,
                    column_name: column,
                    data_type,
                },
            )?;
        }
        self.catalog_writer
            .update_table_descriptor(TableDescriptorChange::RenameTable {
                table_id: t.id,
                new_name: new_name.to_string(),
            })?;
        forget_found();
        Ok(())
    }

    /// `DROP TYPE` or `DROP DOMAIN`: with `cascade`, the columns of the
    /// types go with them.
    pub(crate) fn exec_drop_type(
        &self,
        ctx: &ExecutionContext,
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
        domain: bool,
    ) -> Result<QueryOutput> {
        let tag = if domain { "DROP DOMAIN" } else { "DROP TYPE" };
        let mut found = Vec::new();
        for name in &names {
            match lookup(name) {
                Some(t) => {
                    if domain && t.domain().is_none() {
                        anyhow::bail!("\"{}\" is not a domain", t.name);
                    }
                    found.push(t);
                }
                None if if_exists => self.notice(
                    ctx,
                    DbError::new(format!(
                        "type \"{}\" does not exist, skipping",
                        split_name(name).1
                    )),
                ),
                None => {
                    return Err(DbError::new(format!(
                        "type \"{}\" does not exist",
                        split_name(name).1
                    ))
                    .code("42704")
                    .into());
                }
            }
        }
        let tables = self.catalog_reader.list_all_tables("default")?;
        let mut found_dependents = Vec::new();
        for t in found.iter().rev() {
            dependents(&tables, t, &mut found_dependents);
        }
        // What is dropped anyway is no dependent.
        found_dependents.retain(|(d, _)| match d {
            Dependent::Type(dt) => !found.iter().any(|f| f.id == dt.id),
            Dependent::Column(..) => true,
        });
        if !found_dependents.is_empty() {
            let objects: Vec<String> = found_dependents
                .iter()
                .map(|(d, _)| d.description())
                .collect();
            if !cascade {
                let detail: Vec<String> = found_dependents
                    .iter()
                    .zip(&objects)
                    .map(|((_, kind), object)| format!("{object} depends on type {kind}"))
                    .collect();
                let name = &found[0].name;
                return Err(DbError::new(format!(
                    "cannot drop type {name} because other objects depend on it"
                ))
                .code("2BP01")
                .detail(detail.join("\n"))
                .hint("Use DROP ... CASCADE to drop the dependent objects too.")
                .into());
            }
            match objects.as_slice() {
                [only] => self.notice(ctx, DbError::new(format!("drop cascades to {only}"))),
                many => self.notice(
                    ctx,
                    DbError::new(format!("drop cascades to {} other objects", many.len())).detail(
                        many.iter()
                            .map(|o| format!("drop cascades to {o}"))
                            .collect::<Vec<_>>()
                            .join("\n"),
                    ),
                ),
            }
            for (dependent, _) in &found_dependents {
                match dependent {
                    Dependent::Column(table, column) => {
                        let current = self.catalog_reader.get_table_by_id(table.id)?;
                        self.drop_column(ctx, &current, column, true, true)?;
                    }
                    Dependent::Type(t) => self.catalog_writer.drop_table(t.id)?,
                }
            }
        }
        for t in &found {
            self.catalog_writer.drop_table(t.id)?;
        }
        forget_found();
        Ok(QueryOutput::tag(tag))
    }

    /// `pg_type` rows for the user types, in `pg_type`'s columns: each
    /// enum with its array type, and each domain.
    pub(crate) fn user_type_rows(&self, db_name: &str) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        for t in all_types().iter() {
            let namespace = Self::schema_oid(db_name, &t.schema);
            let (typtype, category, len, byval, notnull, basetype, typmod, align) = match t.domain()
            {
                Some(d) => {
                    let base_oid = Self::pg_type_oid(&d.base);
                    let (category, len) = type_category(Self::pg_type_oid(&base_type(&d.base)));
                    (
                        "d",
                        category,
                        len,
                        matches!(len, 1 | 2 | 4 | 8),
                        d.not_null,
                        base_oid,
                        Self::pg_type_modifier(&d.base),
                        if len == 8 { "d" } else { "i" },
                    )
                }
                None => ("e", "E", 4, true, false, 0, -1, "i"),
            };
            rows.push(type_row(TypeRow {
                oid: t.oid(),
                name: t.name.clone(),
                namespace,
                len,
                byval,
                typtype,
                category,
                elem: 0,
                array: if t.is_enum() { t.array_oid() } else { 0 },
                align,
                notnull,
                basetype,
                typmod,
                default: t
                    .domain()
                    .and_then(|d| d.default.as_ref().map(|sql| default_text(sql, &d.base))),
            }));
            if t.is_enum() {
                rows.push(type_row(TypeRow {
                    oid: t.array_oid(),
                    name: format!("_{}", t.name),
                    namespace,
                    len: -1,
                    byval: false,
                    typtype: "b",
                    category: "A",
                    elem: t.oid(),
                    array: 0,
                    align: "i",
                    notnull: false,
                    basetype: 0,
                    typmod: -1,
                    default: None,
                }));
            }
        }
        rows
    }

    /// `pg_enum` rows: each enum's labels.
    pub(crate) fn pg_enum_rows() -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        for t in all_types().iter().filter(|t| t.is_enum()) {
            for label in t.labels() {
                rows.push(vec![
                    Value::Int(Self::stable_oid(
                        &format!("enum:{}:{}", t.id.0, label.label),
                        100_000,
                    )),
                    Value::Int(t.oid()),
                    Value::Float(label.sort),
                    Value::Text(label.label.clone()),
                ]);
            }
        }
        rows
    }

    /// A domain constraint's OID.
    pub(crate) fn domain_constraint_oid(t: &UserType, name: &str) -> i64 {
        Self::stable_oid(
            &format!("domainconstraint:{}:{name}", t.id.0),
            1_000_000_000,
        )
    }

    /// `pg_constraint` rows for the domains' checks and `NOT NULL`, in
    /// `pg_constraint`'s columns.
    pub(crate) fn domain_constraint_rows(db_name: &str) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        for t in all_types().iter() {
            let Some(domain) = t.domain() else {
                continue;
            };
            let not_null = format!("{}_not_null", t.name);
            let constraints = domain
                .checks
                .iter()
                .map(|c| (c.name.as_str(), "c"))
                .chain(domain.not_null.then_some((not_null.as_str(), "n")));
            for (name, kind) in constraints {
                rows.push(vec![
                    Value::Int(Self::domain_constraint_oid(t, name)),
                    Value::Text(name.to_string()),
                    Value::Int(Self::schema_oid(db_name, &t.schema)),
                    Value::Text(kind.into()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Int(0),
                    Value::Int(t.oid()),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Text(" ".into()),
                    Value::Text(" ".into()),
                    Value::Text(" ".into()),
                    Value::Bool(true),
                    Value::Int(0),
                    Value::Bool(false),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]);
            }
        }
        rows
    }

    /// `pg_get_constraintdef` of a domain's check or `NOT NULL`.
    pub(crate) fn domain_constraint_definition(oid: i64, pretty: bool) -> Option<String> {
        all_types().iter().find_map(|t| {
            let domain = t.domain()?;
            if domain.not_null
                && Self::domain_constraint_oid(t, &format!("{}_not_null", t.name)) == oid
            {
                return Some("NOT NULL".to_string());
            }
            let check = domain
                .checks
                .iter()
                .find(|c| Self::domain_constraint_oid(t, &c.name) == oid)?;
            let text = crate::index_keys::deparse_domain_sql(&check.sql);
            Some(if pretty {
                format!("CHECK {text}")
            } else {
                format!("CHECK ({text})")
            })
        })
    }
}

/// A domain's default as `pg_type.typdefault` shows it: a string literal
/// typed as the domain's base type.
fn default_text(sql: &str, base: &str) -> String {
    let parsed = sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr());
    match parsed {
        Ok(sqlparser::ast::Expr::Value(v)) => match &v.value {
            sqlparser::ast::Value::SingleQuotedString(s) => {
                let base = base_type(base);
                let base = base.split('(').next().unwrap_or(&base);
                format!(
                    "'{}'::{}",
                    s.replace('\'', "''"),
                    crate::functions::format_type_name(MemExecutor::pg_type_oid(base))
                )
            }
            _ => sql.to_string(),
        },
        _ => sql.to_string(),
    }
}

/// A `pg_type` row's values that vary.
struct TypeRow {
    oid: i64,
    name: String,
    namespace: i64,
    len: i64,
    byval: bool,
    typtype: &'static str,
    category: &'static str,
    elem: i64,
    array: i64,
    align: &'static str,
    notnull: bool,
    basetype: i64,
    typmod: i64,
    default: Option<String>,
}

fn type_row(r: TypeRow) -> Vec<Value> {
    vec![
        Value::Int(r.oid),
        Value::Text(r.name),
        Value::Int(r.namespace),
        Value::Int(10),
        Value::Int(r.len),
        Value::Bool(r.byval),
        Value::Text(r.typtype.into()),
        Value::Text(r.category.into()),
        Value::Bool(false),
        Value::Bool(true),
        Value::Text(",".into()),
        Value::Int(0),
        Value::Int(r.elem),
        Value::Int(r.array),
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Text(r.align.into()),
        Value::Text(if r.len == -1 { "x" } else { "p" }.into()),
        Value::Bool(r.notnull),
        Value::Int(r.basetype),
        Value::Int(r.typmod),
        Value::Int(0),
        // The collation columns have (NodusDB's columns all have one).
        Value::Int(100),
        Value::Null,
        r.default.map_or(Value::Null, Value::Text),
        Value::Null,
    ]
}

/// A built-in type's category and length, as a domain over it has them.
fn type_category(oid: i64) -> (&'static str, i64) {
    match oid {
        16 => ("B", 1),
        20 => ("N", 8),
        21 => ("N", 2),
        23 | 26 => ("N", 4),
        700 => ("N", 4),
        701 => ("N", 8),
        1700 => ("N", -1),
        1082 => ("D", 4),
        1083 | 1114 | 1184 => ("D", 8),
        1186 => ("T", 16),
        17 => ("U", -1),
        2950 => ("U", 16),
        _ => ("S", -1),
    }
}

/// Rejects an enum label PostgreSQL refuses.
fn check_label(label: &str) -> Result<()> {
    if label.is_empty() || label.len() > 63 {
        return Err(DbError::new(format!("invalid enum label \"{label}\""))
            .code("42602")
            .detail("Labels must be 63 bytes or less.")
            .into());
    }
    Ok(())
}

/// `base`, or with the first number after it that no name in `taken` has.
fn unused_name(base: &str, taken: &[String]) -> String {
    let mut name = base.to_string();
    let mut n = 0;
    while taken.contains(&name) {
        n += 1;
        name = format!("{base}{n}");
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_split_and_builtins_stay_builtin() {
        assert_eq!(split_name("mood"), (None, "mood".to_string()));
        assert_eq!(
            split_name("public.\"Mood\""),
            (Some("public".to_string()), "Mood".to_string())
        );
        assert!(is_builtin("INTEGER"));
        assert!(is_builtin("PG_CATALOG.TEXT"));
        assert!(!is_builtin("MOOD"));
        assert_eq!(unused_name("d_check", &["d_check".to_string()]), "d_check1");
    }

    #[test]
    fn enum_functions_follow_the_order() {
        let t = UserType {
            id: TableId::new(),
            schema: "public".into(),
            name: "mood".into(),
            definition: TypeDefinition::Enum {
                labels: vec![
                    EnumLabel {
                        label: "ok".into(),
                        sort: 2.0,
                    },
                    EnumLabel {
                        label: "sad".into(),
                        sort: 1.0,
                    },
                    EnumLabel {
                        label: "meh".into(),
                        sort: 1.5,
                    },
                ],
            },
        };
        let labels: Vec<&str> = t.labels().iter().map(|l| l.label.as_str()).collect();
        assert_eq!(labels, ["sad", "meh", "ok"]);
        assert_eq!(
            coerce(&t, &Value::Text("ok".into()), true),
            Ok(Value::Text("ok".into()))
        );
        assert_eq!(
            coerce(&t, &Value::Text("bad".into()), false)
                .map_err(|e| crate::error_fields::error_message(&e).to_string()),
            Err("invalid input value for enum mood: \"bad\"".to_string())
        );
        let d = UserType {
            id: TableId::new(),
            schema: "public".into(),
            name: "pos".into(),
            definition: TypeDefinition::Domain(DomainDefinition {
                base: "INT".into(),
                not_null: true,
                default: None,
                checks: vec![DomainCheck {
                    name: "pos_check".into(),
                    sql: "value > 0".into(),
                }],
            }),
        };
        assert_eq!(
            coerce(&d, &Value::Text("5".into()), true),
            Ok(Value::Int(5))
        );
        assert_eq!(
            coerce(&d, &Value::Int(0), false)
                .map_err(|e| crate::error_fields::error_message(&e).to_string()),
            Err("value for domain pos violates check constraint \"pos_check\"".to_string())
        );
        assert_eq!(
            coerce(&d, &Value::Null, false)
                .map_err(|e| crate::error_fields::error_message(&e).to_string()),
            Err("domain pos does not allow null values".to_string())
        );
    }
}
