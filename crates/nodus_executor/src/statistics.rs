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
