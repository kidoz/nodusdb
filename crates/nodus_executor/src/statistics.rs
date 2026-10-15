//! Extended statistics: `CREATE STATISTICS` objects. Like a user type, an
//! object is a hidden relation whose single column carries its definition —
//! the covered table, the keys, the kinds, and what the last `ANALYZE`
//! found. The numbers are PostgreSQL 18's `mvdistinct.c`, `dependencies.c`
//! and `mcv.c`, transcribed.

use crate::error_fields::DbError;
use crate::*;
use anyhow::Result;
use nodus_catalog::{
    ColumnDescriptor, CreateTableRequest, DescriptorState, TableDescriptor, TableDescriptorChange,
    TableId,
};
use serde::{Deserialize, Serialize};

/// The one column a statistics object's relation carries.
pub(crate) const STATISTICS_COLUMN: &str = "__nodus_statistics__";

/// The serialization magics and version of PostgreSQL's `statistics.h`.
const NDISTINCT_MAGIC: u32 = 0xA352BFA4;
const DEPENDENCIES_MAGIC: u32 = 0xB4549A2C;
const MCV_MAGIC: u32 = 0xE1A651C2;
const TYPE_BASIC: u32 = 1;

/// A statistics object, as its relation stores it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct StatisticsDefinition {
    /// The table the object covers.
    pub table_id: TableId,
    /// The keys, in the declared order (columns by attribute number,
    /// expressions by their SQL text).
    pub keys: Vec<StatisticsKey>,
    /// The kinds requested: `d` (ndistinct), `f` (dependencies), `m` (MCV),
    /// `e` (expressions), as PostgreSQL sorts them.
    pub kinds: Vec<char>,
    /// `ALTER STATISTICS ... SET STATISTICS`: the object's own target.
    #[serde(default)]
    pub target: Option<i32>,
    /// `COMMENT ON STATISTICS`.
    #[serde(default)]
    pub comment: Option<String>,
    /// What the last `ANALYZE` found, one entry per inheritance flag.
    #[serde(default)]
    pub data: Vec<StatisticsData>,
}

/// One key of a statistics object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum StatisticsKey {
    Column { attnum: i32 },
    Expression { text: String },
}

/// What `ANALYZE` found for one statistics object (a `pg_statistic_ext_data`
/// row): the distinct counts of every key combination, the functional
/// dependencies, the most common combinations, and the per-expression
/// statistics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub(crate) struct StatisticsData {
    /// `stxdinherit`: built over the table's whole inheritance tree.
    pub inherited: bool,
    /// `stxdndistinct`: each attribute set and its estimated distinct count.
    pub n_distinct: Vec<(Vec<i32>, f64)>,
    /// `stxddependencies`: each dependent attribute set (its last member
    /// determined by the others) and its degree.
    pub dependencies: Vec<(Vec<i32>, f64)>,
    /// `stxdmcv`: the most common key combinations.
    pub mcv: Vec<McvItem>,
    /// `stxdexpr`: the expressions' own statistics, in key order.
    pub expressions: Vec<nodus_catalog::ColumnStatistics>,
}

/// One most-common combination: its value per key (`None` for SQL NULL), its
/// frequency, and the frequency independent columns would give.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct McvItem {
    pub values: Vec<Option<Value>>,
    pub frequency: f64,
    pub base_frequency: f64,
}

/// A statistics object, found.
#[derive(Debug, Clone)]
pub(crate) struct StatisticsObject {
    pub id: TableId,
    pub schema: String,
    pub name: String,
    pub definition: StatisticsDefinition,
}

impl StatisticsObject {
    pub(crate) fn of(table: &TableDescriptor, schema: String) -> Option<Self> {
        if !is_statistics_relation(table) {
            return None;
        }
        let definition: StatisticsDefinition =
            serde_json::from_str(&table.columns[0].data_type).ok()?;
        Some(StatisticsObject {
            id: table.id,
            schema,
            name: table.name.clone(),
            definition,
        })
    }

    /// The object's OID (`pg_statistic_ext.oid`).
    pub(crate) fn oid(&self) -> i64 {
        MemExecutor::stable_oid(&format!("statistics:{}", self.id.0), 2_400_000_000)
    }

    /// The object's serialized definition, as the relation stores it.
    pub(crate) fn serialized(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.definition)?)
    }
}

/// Whether a relation is a statistics object's.
pub(crate) fn is_statistics_relation(table: &TableDescriptor) -> bool {
    table.view_query.is_none()
        && table.materialized_query.is_none()
        && table.columns.len() == 1
        && table.columns[0].name == STATISTICS_COLUMN
}

/// Every statistics object of a database, in creation order.
pub(crate) fn all(executor: &MemExecutor) -> Vec<StatisticsObject> {
    let schemas = executor
        .types_catalog
        .list_schemas("default")
        .unwrap_or_default();
    let mut found: Vec<StatisticsObject> = executor
        .types_catalog
        .list_all_tables("default")
        .unwrap_or_default()
        .iter()
        .filter_map(|table| {
            let schema = schemas.iter().find(|s| s.id == table.schema_id)?;
            StatisticsObject::of(table, schema.name.clone())
        })
        .collect();
    found.sort_by_key(|object| object.id.0);
    found
}

/// The statistics objects a name resolves to, along the search path.
pub(crate) fn find(name: &str) -> Option<StatisticsObject> {
    let catalog = crate::session_env::with(|env| env.and_then(|e| e.types.clone()))?;
    let objects = statistics_of(catalog.as_ref());
    lookup_in(&objects, name)
}

/// The objects a session's catalog carries (used where no executor is at
/// hand, as `user_types::lookup` is).
fn statistics_of(executor: &dyn nodus_catalog::CatalogReader) -> Vec<StatisticsObject> {
    let schemas = executor.list_schemas("default").unwrap_or_default();
    let mut found: Vec<StatisticsObject> = executor
        .list_all_tables("default")
        .unwrap_or_default()
        .iter()
        .filter_map(|table| {
            let schema = schemas.iter().find(|s| s.id == table.schema_id)?;
            StatisticsObject::of(table, schema.name.clone())
        })
        .collect();
    found.sort_by_key(|object| object.id.0);
    found
}

/// A name — possibly schema-qualified — among the objects.
fn lookup_in(objects: &[StatisticsObject], name: &str) -> Option<StatisticsObject> {
    let (schema, object) = crate::user_types::split_name(name);
    let schemas: Vec<String> = match schema {
        Some(schema) => vec![crate::search_path::schema_named(&schema).to_string()],
        None => crate::search_path::existing_search_path(),
    };
    schemas.iter().find_map(|schema| {
        objects
            .iter()
            .find(|o| &o.schema == schema && o.name == object)
            .cloned()
    })
}

// --- the definition's own rendering ---------------------------------------

impl StatisticsDefinition {
    /// The keys as PostgreSQL's `stxkeys` prints them: the attribute numbers,
    /// space-separated (expressions are not in it).
    pub(crate) fn stxkeys_text(&self) -> String {
        self.keys
            .iter()
            .filter_map(|key| match key {
                StatisticsKey::Column { attnum } => Some(attnum.to_string()),
                StatisticsKey::Expression { .. } => None,
            })
            .collect::<Vec<String>>()
            .join(" ")
    }

    /// The kinds as PostgreSQL's `stxkind` prints them: `{d,f,m}`.
    pub(crate) fn stxkind_text(&self) -> String {
        let kinds: Vec<String> = self.kinds.iter().map(char::to_string).collect();
        format!("{{{}}}", kinds.join(","))
    }

    /// The expression keys' text, in order.
    pub(crate) fn expressions(&self) -> Vec<&str> {
        self.keys
            .iter()
            .filter_map(|key| match key {
                StatisticsKey::Expression { text } => Some(text.as_str()),
                StatisticsKey::Column { .. } => None,
            })
            .collect()
    }

    /// The attribute number a key's statistics carry, in PostgreSQL's order:
    /// the columns ascending by attribute number, then the expressions as
    /// `-1`, `-2`, ... in their declared order.
    pub(crate) fn attnums(&self) -> Vec<i32> {
        let mut columns: Vec<i32> = self
            .keys
            .iter()
            .filter_map(|key| match key {
                StatisticsKey::Column { attnum } => Some(*attnum),
                StatisticsKey::Expression { .. } => None,
            })
            .collect();
        columns.sort_unstable();
        let mut expression = -1i32;
        columns.extend(self.expressions().iter().map(|_| {
            let attnum = expression;
            expression -= 1;
            attnum
        }));
        columns
    }

    /// The column keys' attribute numbers, ascending.
    pub(crate) fn column_keys(&self) -> Vec<i32> {
        let mut columns: Vec<i32> = self
            .keys
            .iter()
            .filter_map(|key| match key {
                StatisticsKey::Column { attnum } => Some(*attnum),
                StatisticsKey::Expression { .. } => None,
            })
            .collect();
        columns.sort_unstable();
        columns
    }

    /// What the last analysis found for an inheritance flag.
    pub(crate) fn data(&self, inherited: bool) -> Option<&StatisticsData> {
        self.data.iter().find(|data| data.inherited == inherited)
    }
}

// --- DDL ------------------------------------------------------------------

impl MemExecutor {
    /// `CREATE STATISTICS [IF NOT EXISTS] name [(kinds)] ON keys FROM table`:
    /// the object is stored, as a hidden relation, for `ANALYZE` to fill.
    pub(crate) fn exec_create_statistics_full(
        &self,
        ctx: &ExecutionContext,
        name: String,
        keys: Vec<StatisticsKey>,
        kinds: Vec<char>,
        table: &TableDescriptor,
    ) -> Result<QueryOutput> {
        let (qualifier, object_name) = crate::user_types::split_name(&name);
        let schema_name = match &qualifier {
            Some(schema) => crate::search_path::schema_named(schema).to_string(),
            None => crate::search_path::schema_for(&object_name).to_string(),
        };
        self.authorize(
            ctx,
            nodus_authz::Action::CreateTable,
            nodus_catalog::ResourceRef::Table(table.id),
        )?;
        let db = self.catalog_reader.get_database("default")?;
        let schema = self.catalog_reader.get_schema("default", &schema_name)?;
        if self
            .types_catalog
            .get_table("default", &schema_name, &object_name)
            .is_ok()
        {
            return Err(DbError::new(format!(
                "statistics object \"{object_name}\" already exists"
            ))
            .code("42710")
            .into());
        }
        // The kinds as declared, expression statistics appended last, as
        // PostgreSQL stores them.
        let mut kinds = kinds;
        kinds.dedup();
        // The keys in PostgreSQL's own order: the columns ascending by
        // attribute number, then the expressions as declared.
        let mut keys = keys;
        keys.sort_by_key(|key| match key {
            StatisticsKey::Column { attnum } => (0, *attnum, 0),
            StatisticsKey::Expression { .. } => (1, 0, 0),
        });
        let definition = StatisticsDefinition {
            table_id: table.id,
            keys,
            kinds,
            target: None,
            comment: None,
            data: Vec::new(),
        };
        let now = chrono::Utc::now();
        self.catalog_writer.create_table(CreateTableRequest {
            id: TableId::new(),
            database_id: db.id,
            schema_id: schema.id,
            name: object_name,
            columns: vec![ColumnDescriptor {
                id: nodus_catalog::ColumnId::new(),
                name: STATISTICS_COLUMN.to_string(),
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
            parents: Vec::new(),
            partition_by: None,
            partition_bound: None,
        })?;
        Ok(QueryOutput::tag("CREATE STATISTICS"))
    }

    /// `DROP STATISTICS [IF EXISTS] name [, ...] [CASCADE | RESTRICT]`.
    pub(crate) fn exec_drop_statistics(
        &self,
        ctx: &ExecutionContext,
        target: &str,
    ) -> Result<QueryOutput> {
        let (names, if_exists, _cascade) = parse_drop_statistics(target)?;
        let mut found = Vec::new();
        for name in &names {
            match find(name) {
                Some(object) => {
                    self.authorize(
                        ctx,
                        nodus_authz::Action::CreateTable,
                        nodus_catalog::ResourceRef::Table(object.id),
                    )?;
                    found.push(object);
                }
                None if if_exists => self.notice(
                    ctx,
                    DbError::new(format!(
                        "statistics object \"{}\" does not exist, skipping",
                        crate::user_types::split_name(name).1
                    )),
                ),
                None => {
                    return Err(DbError::new(format!(
                        "statistics object \"{}\" does not exist",
                        crate::user_types::split_name(name).1
                    ))
                    .code("42704")
                    .into());
                }
            }
        }
        for object in found {
            self.catalog_writer.drop_table(object.id)?;
        }
        Ok(QueryOutput::tag("DROP STATISTICS"))
    }

    /// `ALTER STATISTICS name { SET STATISTICS n | RENAME TO x | OWNER TO r
    /// | SET SCHEMA s }`.
    pub(crate) fn exec_alter_statistics(
        &self,
        ctx: &ExecutionContext,
        target: &str,
    ) -> Result<QueryOutput> {
        let (name, action, value) = parse_alter_statistics(target)?;
        let object = find(&name).ok_or_else(|| {
            DbError::new(format!(
                "statistics object \"{}\" does not exist",
                crate::user_types::split_name(&name).1
            ))
            .code("42704")
        })?;
        let table = self
            .types_catalog
            .get_table_by_id(object.definition.table_id)?;
        self.authorize(
            ctx,
            nodus_authz::Action::CreateTable,
            nodus_catalog::ResourceRef::Table(table.id),
        )?;
        let mut definition = object.definition.clone();
        match action.as_str() {
            "set statistics" => {
                let target: i32 = value.parse().map_err(|_| {
                    anyhow::anyhow!("statistics target must be an integer between 0 and 10000")
                })?;
                definition.target = Some(target);
                self.write_statistics(&object, &definition)?;
            }
            "rename to" => {
                self.catalog_writer.update_table_descriptor(
                    TableDescriptorChange::RenameTable {
                        table_id: object.id,
                        new_name: value.clone(),
                    },
                )?;
            }
            // `OWNER TO` changes nothing this engine enforces; `SET SCHEMA`
            // moves the object's own relation.
            "set schema" => {
                let schema = self.catalog_reader.get_schema("default", &value)?;
                self.catalog_writer
                    .update_table_descriptor(TableDescriptorChange::SetSchema {
                        table_id: object.id,
                        schema_id: schema.id,
                    })?;
            }
            _ => {}
        }
        Ok(QueryOutput::tag("ALTER STATISTICS"))
    }

    /// `COMMENT ON STATISTICS name IS 'text'` (or `IS NULL`).
    pub(crate) fn exec_comment_statistics(&self, target: &str) -> Result<QueryOutput> {
        let (name, comment) = parse_comment_statistics(target)?;
        let object = find(&name).ok_or_else(|| {
            DbError::new(format!(
                "statistics object \"{}\" does not exist",
                crate::user_types::split_name(&name).1
            ))
            .code("42704")
        })?;
        let mut definition = object.definition.clone();
        definition.comment = comment;
        self.write_statistics(&object, &definition)?;
        Ok(QueryOutput::tag("COMMENT"))
    }

    /// Writes a statistics object's definition back to its relation.
    fn write_statistics(
        &self,
        object: &StatisticsObject,
        definition: &StatisticsDefinition,
    ) -> Result<()> {
        self.catalog_writer
            .update_table_descriptor(TableDescriptorChange::AlterColumnType {
                table_id: object.id,
                column_name: STATISTICS_COLUMN.to_string(),
                data_type: serde_json::to_string(definition)?,
            })?;
        Ok(())
    }

    /// The statistics objects a table covers, in creation order.
    pub(crate) fn statistics_objects(&self, table_id: TableId) -> Vec<StatisticsObject> {
        all(self)
            .into_iter()
            .filter(|object| object.definition.table_id == table_id)
            .collect()
    }

    /// `pg_get_statisticsobjdef(oid)`: `CREATE STATISTICS public.s ON c,
    /// (a + b) FROM t`.
    pub(crate) fn statistics_definition(
        catalog: &dyn nodus_catalog::CatalogReader,
        oid: i64,
    ) -> Option<String> {
        let object = object_of_oid(catalog, oid)?;
        let table = catalog.get_table_by_id(object.definition.table_id).ok()?;
        Some(format!(
            "CREATE STATISTICS {}.{} ON {} FROM {}",
            object.schema,
            object.name,
            object.definition.key_list_named(&table),
            table.name,
        ))
    }

    /// `pg_get_statisticsobjdef_columns(oid)`: `c, (a + b)`.
    pub(crate) fn statistics_columns(
        catalog: &dyn nodus_catalog::CatalogReader,
        oid: i64,
    ) -> Option<String> {
        let object = object_of_oid(catalog, oid)?;
        let table = catalog.get_table_by_id(object.definition.table_id).ok()?;
        Some(object.definition.key_list_named(&table))
    }

    /// `pg_get_statisticsobjdef_expressions(oid)`: the expressions, each in
    /// parentheses, or NULL when the object has none.
    pub(crate) fn statistics_expressions(
        catalog: &dyn nodus_catalog::CatalogReader,
        oid: i64,
    ) -> Option<Option<Vec<String>>> {
        let object = object_of_oid(catalog, oid)?;
        let expressions = object.definition.expressions();
        if expressions.is_empty() {
            return Some(None);
        }
        Some(Some(
            expressions.iter().map(|text| format!("({text})")).collect(),
        ))
    }
}

/// A statistics object's comment, for `obj_description`.
pub(crate) fn description(oid: i64) -> Option<String> {
    let catalog = crate::session_env::with(|env| env.and_then(|e| e.types.clone()))?;
    object_of_oid(catalog.as_ref(), oid)?.definition.comment
}

/// The statistics object a catalog holds for an OID.
fn object_of_oid(catalog: &dyn nodus_catalog::CatalogReader, oid: i64) -> Option<StatisticsObject> {
    statistics_of(catalog)
        .into_iter()
        .find(|object| object.oid() == oid)
}

impl StatisticsDefinition {
    /// The keys as a definition prints them: the columns by name, then the
    /// expressions in parentheses.
    fn key_list_named(&self, table: &TableDescriptor) -> String {
        self.keys
            .iter()
            .filter_map(|key| match key {
                StatisticsKey::Column { attnum } => table
                    .columns
                    .get(*attnum as usize - 1)
                    .map(|column| column.name.clone()),
                StatisticsKey::Expression { text } => Some(format!("({text})")),
            })
            .collect::<Vec<String>>()
            .join(", ")
    }
}

// --- what ANALYZE computes ------------------------------------------------

/// Fills every statistics object covering `tbl` with what the sample shows,
/// for PostgreSQL's two passes (the table's own rows, and its inheritance
/// tree's).
pub(crate) fn analyze_objects(
    executor: &MemExecutor,
    tbl: &TableDescriptor,
    rows: &[Vec<Value>],
    inherited: bool,
) -> Result<()> {
    // A pass with no sampled rows stores nothing, as PostgreSQL's does.
    if rows.is_empty() {
        return Ok(());
    }
    for object in executor.statistics_objects(tbl.id) {
        let mut definition = object.definition.clone();
        // The object's own target; the columns' targets are all the default,
        // so the system default (100) stands in for the maximum.
        let stattarget = definition.target.unwrap_or(DEFAULT_TARGET);
        // A target of zero leaves the existing statistics alone.
        if stattarget == 0 {
            continue;
        }
        let data = BuildData::build(tbl, &definition, rows);
        let attnums = definition.attnums();
        let mut computed = StatisticsData {
            inherited,
            ..Default::default()
        };
        if definition.kinds.contains(&'d') {
            computed.n_distinct = build_ndistinct(&data, &attnums);
        }
        if definition.kinds.contains(&'f') {
            computed.dependencies = build_dependencies(&data, &attnums);
        }
        if definition.kinds.contains(&'m') {
            computed.mcv = build_mcv(&data, stattarget as usize);
        }
        if definition.kinds.contains(&'e') {
            computed.expressions = expression_statistics(tbl, &definition, rows)?;
        }
        definition.data.retain(|data| data.inherited != inherited);
        definition.data.push(computed);
        executor.write_statistics(&object, &definition)?;
    }
    Ok(())
}

/// PostgreSQL's `default_statistics_target`.
const DEFAULT_TARGET: i32 = 100;

/// `compute_expr_stats`: each expression's own statistics, as a column's
/// would be (the same machinery `ANALYZE` uses).
fn expression_statistics(
    tbl: &TableDescriptor,
    definition: &StatisticsDefinition,
    rows: &[Vec<Value>],
) -> Result<Vec<nodus_catalog::ColumnStatistics>> {
    let names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
    let column_type = |name: &str| -> Option<String> {
        tbl.columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.data_type.clone())
    };
    let mut all = Vec::new();
    for (index, text) in definition.expressions().iter().enumerate() {
        let expr =
            crate::index_keys::parse_expression(text).unwrap_or(ScalarExpr::Literal(Value::Null));
        let data_type =
            crate::result_types::expr_type(&expr, &column_type).unwrap_or_else(|| "text".into());
        let values: Vec<Value> = rows
            .iter()
            .map(|row| crate::eval_scalar_expr(&expr, row, &names))
            .collect();
        let cells: Vec<&Value> = values.iter().collect();
        let attnum = index as i32 + 1;
        if let Some(statistics) =
            crate::column_stats::analyze_values(attnum, &data_type, &cells, rows.len() as f64)
        {
            all.push(statistics);
        }
    }
    Ok(all)
}

// --- serializations (PostgreSQL's binary forms) ---------------------------

/// `statext_ndistinct_serialize`: magic, version, item count, then each
/// item's distinct count, member count and attribute numbers.
pub(crate) fn serialize_ndistinct(items: &[(Vec<i32>, f64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + items.len() * 14);
    out.extend_from_slice(&NDISTINCT_MAGIC.to_le_bytes());
    out.extend_from_slice(&TYPE_BASIC.to_le_bytes());
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for (attributes, ndistinct) in items {
        out.extend_from_slice(&ndistinct.to_le_bytes());
        out.extend_from_slice(&(attributes.len() as i32).to_le_bytes());
        for attnum in attributes {
            out.extend_from_slice(&(*attnum as i16).to_le_bytes());
        }
    }
    out
}

/// `pg_ndistinct_out`: `{"1, 2": 70}` with the count as an integer.
pub(crate) fn ndistinct_text(items: &[(Vec<i32>, f64)]) -> String {
    let rendered: Vec<String> = items
        .iter()
        .map(|(attributes, ndistinct)| {
            let names: Vec<String> = attributes.iter().map(|a| a.to_string()).collect();
            format!("\"{}\": {}", names.join(", "), *ndistinct as i64)
        })
        .collect();
    format!("{{{}}}", rendered.join(", "))
}

/// `statext_dependencies_serialize`: magic, version, dependency count, then
/// each dependency's degree, member count and attribute numbers.
pub(crate) fn serialize_dependencies(items: &[(Vec<i32>, f64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + items.len() * 14);
    out.extend_from_slice(&DEPENDENCIES_MAGIC.to_le_bytes());
    out.extend_from_slice(&TYPE_BASIC.to_le_bytes());
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for (attributes, degree) in items {
        out.extend_from_slice(&degree.to_le_bytes());
        out.extend_from_slice(&(attributes.len() as i16).to_le_bytes());
        for attnum in attributes {
            out.extend_from_slice(&(*attnum as i16).to_le_bytes());
        }
    }
    out
}

/// `pg_dependencies_out`: `{"1, 2 => 3": 0.918367}` with six decimals.
pub(crate) fn dependencies_text(items: &[(Vec<i32>, f64)]) -> String {
    let rendered: Vec<String> = items
        .iter()
        .map(|(attributes, degree)| {
            let names: Vec<String> = attributes.iter().map(|a| a.to_string()).collect();
            let (dependent, determinants) = names.split_last().expect("a dependency has members");
            format!(
                "\"{determinants} => {dependent}\": {degree:.6}",
                determinants = determinants.join(", ")
            )
        })
        .collect();
    format!("{{{}}}", rendered.join(", "))
}

/// `stxdmcv`'s value: a bytea framed by NodusDB's own header (PostgreSQL's
/// binary MCV layout is not reproduced) with the items as JSON, so the
/// statistics views can decode them.
pub(crate) fn serialize_mcv(items: &[McvItem]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&MCV_MAGIC.to_le_bytes());
    out.extend_from_slice(&TYPE_BASIC.to_le_bytes());
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    let json = serde_json::to_vec(items).unwrap_or_default();
    out.extend_from_slice(&json);
    out
}

// --- the computations (PostgreSQL's algorithms) ---------------------------

/// One key's values over the sample rows, and whether each is NULL.
struct KeyValues {
    values: Vec<Value>,
    nulls: Vec<bool>,
}

/// What an analysis works on: the keys' values per row.
struct BuildData {
    keys: Vec<KeyValues>,
    numrows: usize,
    totalrows: f64,
}

impl BuildData {
    /// The sample's values for one key: a column by position, an expression
    /// evaluated over the row.
    fn build(
        tbl: &TableDescriptor,
        definition: &StatisticsDefinition,
        rows: &[Vec<Value>],
    ) -> BuildData {
        let names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        let mut keys = Vec::new();
        for key in &definition.keys {
            let values: Vec<Value> = match key {
                StatisticsKey::Column { attnum } => rows
                    .iter()
                    .map(|row| {
                        row.get(*attnum as usize - 1)
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
                StatisticsKey::Expression { text } => {
                    let expr = crate::index_keys::parse_expression(text)
                        .unwrap_or(ScalarExpr::Literal(Value::Null));
                    rows.iter()
                        .map(|row| crate::eval_scalar_expr(&expr, row, &names))
                        .collect()
                }
            };
            let nulls = values
                .iter()
                .map(|value| matches!(value, Value::Null))
                .collect();
            keys.push(KeyValues { values, nulls });
        }
        BuildData {
            keys,
            numrows: rows.len(),
            totalrows: rows.len() as f64,
        }
    }
}

/// A row's key values over the subset an ordering uses, with its count.
#[derive(Clone)]
struct SortItem {
    values: Vec<Value>,
    nulls: Vec<bool>,
    count: usize,
}

/// The order of two rows over the given key positions: by each key's value,
/// NULLs first, as PostgreSQL orders them.
fn cmp_items(a: &SortItem, b: &SortItem, positions: &[usize]) -> std::cmp::Ordering {
    for &position in positions {
        let order = match (a.nulls[position], b.nulls[position]) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => crate::value::compare(&a.values[position], &b.values[position]),
        };
        if order != std::cmp::Ordering::Equal {
            return order;
        }
    }
    std::cmp::Ordering::Equal
}

/// The rows as sort items over a subset of keys, in order.
fn sorted_items(data: &BuildData, positions: &[usize]) -> Vec<SortItem> {
    let mut items: Vec<SortItem> = (0..data.numrows)
        .map(|row| SortItem {
            values: positions
                .iter()
                .map(|&key| {
                    if data.keys[key].nulls[row] {
                        Value::Null
                    } else {
                        data.keys[key].values[row].clone()
                    }
                })
                .collect(),
            nulls: positions
                .iter()
                .map(|&key| data.keys[key].nulls[row])
                .collect(),
            count: 1,
        })
        .collect();
    let all: Vec<usize> = (0..positions.len()).collect();
    items.sort_by(|a, b| cmp_items(a, b, &all));
    items
}

/// All combinations of `k` of `n` indexes, lexicographic ascending, as
/// PostgreSQL's combination generator walks them.
fn combinations(n: usize, k: usize) -> Vec<Vec<usize>> {
    let mut out = Vec::new();
    let mut pick: Vec<usize> = (0..k).collect();
    if k > n {
        return out;
    }
    loop {
        out.push(pick.clone());
        let mut i = k;
        while i > 0 {
            i -= 1;
            if pick[i] != i + n - k {
                break;
            }
            if i == 0 {
                return out;
            }
        }
        pick[i] += 1;
        for j in i + 1..k {
            pick[j] = pick[j - 1] + 1;
        }
    }
}

/// The dependencies of `k` keys PostgreSQL's generator produces: each
/// ascending set of `k - 1` determinants, and each remaining key as the
/// dependent, in that order.
fn dependent_sets(n: usize, k: usize) -> Vec<Vec<usize>> {
    let mut out = Vec::new();
    for determinants in combinations(n, k - 1) {
        for dependent in 0..n {
            if determinants.contains(&dependent) {
                continue;
            }
            let mut set = determinants.clone();
            set.push(dependent);
            out.push(set);
        }
    }
    out
}

/// The Duj1 estimator's result, as `estimate_ndistinct` computes it.
fn estimate_ndistinct(totalrows: f64, numrows: usize, d: usize, f1: usize) -> f64 {
    let numer = numrows as f64 * d as f64;
    let denom = (numrows - f1) as f64 + f1 as f64 * numrows as f64 / totalrows;
    let mut ndistinct = numer / denom;
    if ndistinct < d as f64 {
        ndistinct = d as f64;
    }
    if ndistinct > totalrows {
        ndistinct = totalrows;
    }
    (ndistinct + 0.5).floor()
}

/// `statext_ndistinct_build`: every combination of two or more keys, with
/// the Duj1 estimate of its distinct count, keyed by the attribute numbers.
fn build_ndistinct(data: &BuildData, attnums: &[i32]) -> Vec<(Vec<i32>, f64)> {
    let n = data.keys.len();
    let mut out = Vec::new();
    for k in 2..=n {
        for combination in combinations(n, k) {
            // The sorted rows split into runs of equal key values: the runs
            // are the distinct combinations, and `f1` counts the singletons.
            let items = sorted_items(data, &combination);
            let all: Vec<usize> = (0..k).collect();
            let mut f1 = 0usize;
            let mut d = 1usize;
            let mut count = 1usize;
            for i in 1..items.len() {
                if cmp_items(&items[i], &items[i - 1], &all) != std::cmp::Ordering::Equal {
                    if count == 1 {
                        f1 += 1;
                    }
                    d += 1;
                    count = 0;
                }
                count += 1;
            }
            if count == 1 {
                f1 += 1;
            }
            let attributes: Vec<i32> = combination.iter().map(|&key| attnums[key]).collect();
            out.push((
                attributes,
                estimate_ndistinct(data.totalrows, data.numrows, d, f1),
            ));
        }
    }
    out
}

/// `dependency_degree`: the rows sorted by all the keys and grouped by the
/// determinants; a group whose dependent value never changes supports the
/// dependency, and the degree is the supporting rows' share.
fn dependency_degree(data: &BuildData, dependency: &[usize]) -> f64 {
    let items = sorted_items(data, dependency);
    let determinants: Vec<usize> = (0..dependency.len() - 1).collect();
    let dependent = dependency.len() - 1;
    let mut supporting = 0usize;
    let mut violations = 0usize;
    let mut group_size = 1usize;
    for i in 1..=items.len() {
        let group_ended = i == items.len()
            || cmp_items(&items[i - 1], &items[i], &determinants) != std::cmp::Ordering::Equal;
        if group_ended {
            if violations == 0 {
                supporting += group_size;
            }
            violations = 0;
            group_size = 1;
            continue;
        }
        if cmp_items(&items[i - 1], &items[i], &[dependent]) != std::cmp::Ordering::Equal {
            violations += 1;
        }
        group_size += 1;
    }
    supporting as f64 / data.numrows as f64
}

/// `statext_dependencies_build`: every ordered key set of two or more whose
/// last member the others determine with a non-zero degree.
fn build_dependencies(data: &BuildData, attnums: &[i32]) -> Vec<(Vec<i32>, f64)> {
    let n = data.keys.len();
    let mut out = Vec::new();
    for k in 2..=n {
        for dependency in dependent_sets(n, k) {
            let degree = dependency_degree(data, &dependency);
            if degree == 0.0 {
                continue;
            }
            let attributes: Vec<i32> = dependency.iter().map(|&key| attnums[key]).collect();
            out.push((attributes, degree));
        }
    }
    out
}

/// `get_mincount_for_mcv_list`: the cutoff a combination's count must reach
/// to be kept.
fn mincount_for_mcv_list(numrows: usize, totalrows: f64) -> f64 {
    let n = numrows as f64;
    let big_n = totalrows;
    let denom = big_n - n + 0.04 * n * (big_n - 1.0);
    if denom == 0.0 {
        return 0.0;
    }
    n * (big_n - n) / denom
}

/// `statext_mcv_build`: the most common key combinations, kept while their
/// counts reach the cutoff, each with its frequency and the frequency
/// independent columns would give.
fn build_mcv(data: &BuildData, stattarget: usize) -> Vec<McvItem> {
    let n = data.keys.len();
    if data.numrows == 0 {
        return Vec::new();
    }
    let positions: Vec<usize> = (0..n).collect();
    let items = sorted_items(data, &positions);
    // The distinct combinations, in value order, with their counts.
    let mut groups: Vec<SortItem> = Vec::new();
    for item in items {
        match groups.last_mut() {
            Some(last) if cmp_items(last, &item, &positions) == std::cmp::Ordering::Equal => {
                last.count += 1;
            }
            _ => {
                let mut group = item;
                group.count = 1;
                groups.push(group);
            }
        }
    }
    // By frequency, descending; the sort keeps the value order for ties.
    groups.sort_by(|a, b| b.count.cmp(&a.count));
    let mut nitems = stattarget.min(groups.len());
    let mincount = mincount_for_mcv_list(data.numrows, data.totalrows);
    for i in 0..nitems {
        if (groups[i].count as f64) < mincount {
            nitems = i;
            break;
        }
    }
    // Each key's value frequencies over all the groups.
    let mut frequencies: Vec<Vec<(Value, bool, usize)>> = Vec::with_capacity(n);
    for key in 0..n {
        let mut values: Vec<(Value, bool, usize)> = Vec::new();
        for group in &groups {
            let value = group.values[key].clone();
            let null = group.nulls[key];
            match values.iter_mut().find(|(existing, existing_null, _)| {
                *existing_null == null
                    && (null
                        || crate::value::compare(existing, &value) == std::cmp::Ordering::Equal)
            }) {
                Some((_, _, count)) => *count += group.count,
                None => values.push((value, null, group.count)),
            }
        }
        frequencies.push(values);
    }
    let mut out = Vec::with_capacity(nitems);
    for group in groups.iter().take(nitems) {
        let mut base_frequency = 1.0f64;
        for key in 0..n {
            let count = frequencies[key]
                .iter()
                .find(|(value, null, _)| {
                    *null == group.nulls[key]
                        && (*null
                            || crate::value::compare(value, &group.values[key])
                                == std::cmp::Ordering::Equal)
                })
                .map_or(0, |(_, _, count)| *count);
            base_frequency *= count as f64 / data.numrows as f64;
        }
        out.push(McvItem {
            values: group
                .values
                .iter()
                .zip(&group.nulls)
                .map(|(value, null)| if *null { None } else { Some(value.clone()) })
                .collect(),
            frequency: group.count as f64 / data.numrows as f64,
            base_frequency,
        });
    }
    out
}

/// `ALTER STATISTICS`'s target text: the object's name, the action, and its
/// value.
fn parse_alter_statistics(target: &str) -> Result<(String, String, String)> {
    use sqlparser::tokenizer::{Token, Tokenizer};
    let tokens: Vec<Token> = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, target)
        .tokenize()
        .unwrap_or_default()
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let syntax = |at: usize| crate::ddl::statement_syntax_error(&tokens, at);
    // The object's name, up to the action's first word.
    let mut at = 0usize;
    let mut name = String::new();
    while let Some(token) = tokens.get(at) {
        match token {
            Token::Word(word)
                if !matches!(
                    word.value.to_ascii_lowercase().as_str(),
                    "set" | "rename" | "owner"
                ) =>
            {
                if !name.is_empty() {
                    name.push('.');
                }
                name.push_str(&word.value);
                at += 1;
            }
            Token::Period => {
                at += 1;
            }
            _ => break,
        }
    }
    if name.is_empty() {
        return Err(syntax(at));
    }
    // The action and its value.
    let word = |at: usize| match tokens.get(at) {
        Some(Token::Word(word)) => Some(word.value.to_ascii_lowercase()),
        _ => None,
    };
    let value_word = |at: usize| match tokens.get(at) {
        Some(Token::Word(word)) => Some(word.value.clone()),
        Some(Token::Number(number, _)) => Some(number.clone()),
        _ => None,
    };
    match (word(at), word(at + 1)) {
        (Some(first), Some(second)) if first == "set" && second == "statistics" => {
            let value = value_word(at + 2).ok_or_else(|| syntax(at + 2))?;
            Ok((name, "set statistics".into(), value))
        }
        (Some(first), Some(second)) if first == "set" && second == "schema" => {
            let value = value_word(at + 2).ok_or_else(|| syntax(at + 2))?;
            Ok((name, "set schema".into(), value))
        }
        (Some(first), Some(second)) if first == "rename" && second == "to" => {
            let value = value_word(at + 2).ok_or_else(|| syntax(at + 2))?;
            Ok((name, "rename to".into(), value))
        }
        (Some(first), Some(second)) if first == "owner" && second == "to" => {
            let value = value_word(at + 2).ok_or_else(|| syntax(at + 2))?;
            Ok((name, "owner to".into(), value))
        }
        _ => Err(syntax(at)),
    }
}

/// `DROP STATISTICS`'s target text: the names, whether `IF EXISTS` was
/// written, and whether the drop cascades.
fn parse_drop_statistics(target: &str) -> Result<(Vec<String>, bool, bool)> {
    use sqlparser::tokenizer::{Token, Tokenizer};
    let tokens: Vec<Token> = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, target)
        .tokenize()
        .unwrap_or_default()
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let words: Vec<String> = tokens
        .iter()
        .filter_map(|token| match token {
            Token::Word(word) => Some(word.value.to_ascii_lowercase()),
            _ => None,
        })
        .collect();
    let if_exists = words.starts_with(&["if".into(), "exists".into()]);
    let cascade = words.last().is_some_and(|word| word == "cascade");
    // The names: words joined by periods, up to the comma or the end.
    let mut names = Vec::new();
    let mut current = String::new();
    let mut at = if if_exists { 2 } else { 0 };
    while at < tokens.len() {
        match &tokens[at] {
            Token::Word(word) => {
                let lower = word.value.to_ascii_lowercase();
                if matches!(lower.as_str(), "cascade" | "restrict") && !current.is_empty() {
                    break;
                }
                if !current.is_empty() {
                    current.push('.');
                }
                current.push_str(&word.value);
                at += 1;
            }
            Token::Period => at += 1,
            Token::Comma => {
                names.push(std::mem::take(&mut current));
                at += 1;
            }
            _ => at += 1,
        }
    }
    if !current.is_empty() {
        names.push(current);
    }
    if names.iter().any(String::is_empty) {
        return Err(crate::ddl::statement_syntax_error(&tokens, at));
    }
    Ok((names, if_exists, cascade))
}

/// `COMMENT ON STATISTICS`'s target text: the name, and the comment (`None`
/// for `IS NULL`).
fn parse_comment_statistics(target: &str) -> Result<(String, Option<String>)> {
    use sqlparser::tokenizer::{Token, Tokenizer};
    let tokens: Vec<Token> = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, target)
        .tokenize()
        .unwrap_or_default()
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let mut at = 0usize;
    let mut name = String::new();
    while let Some(token) = tokens.get(at) {
        match token {
            Token::Word(word) if word.value.to_ascii_lowercase() != "is" => {
                if !name.is_empty() {
                    name.push('.');
                }
                name.push_str(&word.value);
                at += 1;
            }
            Token::Period => at += 1,
            _ => break,
        }
    }
    let is = match tokens.get(at) {
        Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("is") => at,
        _ => return Err(crate::ddl::statement_syntax_error(&tokens, at)),
    };
    let comment = match tokens.get(is + 1) {
        Some(Token::SingleQuotedString(text)) => Some(text.clone()),
        Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("null") => None,
        _ => return Err(crate::ddl::statement_syntax_error(&tokens, is + 1)),
    };
    if name.is_empty() {
        return Err(crate::ddl::statement_syntax_error(&tokens, 0));
    }
    Ok((name, comment))
}
