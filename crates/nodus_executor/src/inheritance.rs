//! Table inheritance: the columns a child takes from its parents, the scans
//! that read a parent's descendants, and the operations that reach them.
//!
//! PostgreSQL's rules, as probed: a child's columns are the parents' first
//! (in `INHERITS` order), then its own not already inherited; a same-named
//! column merges (the types must match, `NOT NULL` wins, the child's default
//! wins) with a NOTICE; `CHECK` constraints are copied down by name, while
//! indexes, unique and primary-key constraints, and foreign keys are not
//! inherited. A `SELECT` on a table reads its descendants too (breadth-first
//! by depth, siblings in creation order), each row carrying its source table
//! as the `tableoid` value; `ONLY` reads just the table.

use crate::{ColumnDef, ExecutionContext, MemExecutor, ScalarExpr, Value};
use anyhow::Result;
use nodus_catalog::{ColumnDescriptor, TableConstraint, TableDescriptor, TableId};

impl MemExecutor {
    /// A table's descendants, breadth-first by depth with siblings in
    /// creation order — the order PostgreSQL's scans append them in.
    pub(crate) fn descendants(&self, db_name: &str, id: TableId) -> Result<Vec<TableDescriptor>> {
        let all = self.catalog_reader.list_all_tables(db_name)?;
        let mut out = Vec::new();
        let mut frontier = vec![id];
        while !frontier.is_empty() {
            let mut next = Vec::new();
            for parent in &frontier {
                let mut children: Vec<TableDescriptor> = all
                    .iter()
                    .filter(|t| t.parents.contains(parent))
                    .cloned()
                    .collect();
                children.sort_by(|a, b| {
                    (a.created_at, a.name.as_str()).cmp(&(b.created_at, b.name.as_str()))
                });
                next.extend(children.iter().map(|c| c.id));
                out.extend(children);
            }
            frontier = next;
        }
        Ok(out)
    }

    /// A table's ancestors, nearest first.
    pub(crate) fn ancestors(&self, db_name: &str, id: TableId) -> Result<Vec<TableDescriptor>> {
        let all = self.catalog_reader.list_all_tables(db_name)?;
        let mut out: Vec<TableDescriptor> = Vec::new();
        let mut frontier = vec![id];
        let mut seen = vec![id];
        while let Some(current) = frontier.pop() {
            let Some(descriptor) = all.iter().find(|t| t.id == current) else {
                continue;
            };
            for parent in &descriptor.parents {
                if seen.contains(parent) {
                    continue;
                }
                seen.push(*parent);
                if let Some(found) = all.iter().find(|t| t.id == *parent) {
                    out.push(found.clone());
                    frontier.push(found.id);
                }
            }
        }
        Ok(out)
    }

    /// Whether `column` is inherited by `table` from any ancestor.
    pub(crate) fn inherited_column(
        &self,
        db_name: &str,
        table: &TableDescriptor,
        column: &str,
    ) -> Result<bool> {
        Ok(self
            .ancestors(db_name, table.id)?
            .iter()
            .any(|a| a.columns.iter().any(|c| c.name == column)))
    }

    /// Whether `constraint` (a CHECK by name) is inherited by `table`.
    pub(crate) fn inherited_constraint(
        &self,
        db_name: &str,
        table: &TableDescriptor,
        name: &str,
    ) -> Result<bool> {
        Ok(self.ancestors(db_name, table.id)?.iter().any(|a| {
            a.constraints
                .iter()
                .any(|c| c.effective_name(&a.name) == name)
        }))
    }

    /// The parents `inherits` names, resolved like any relation reference and
    /// checked as PostgreSQL checks them: each must be a table (not a view)
    /// and none may be temporary.
    pub(crate) fn resolve_parents(&self, inherits: &[String]) -> Result<Vec<TableDescriptor>> {
        let mut parents = Vec::new();
        for name in inherits {
            let (db_name, schema_name, table_only) = crate::parse_object_name(name)?;
            if crate::search_path::is_temp_schema(schema_name) {
                anyhow::bail!("cannot inherit from temporary relation \"{table_only}\"");
            }
            let parent = self
                .catalog_reader
                .get_table(db_name, schema_name, table_only)?;
            if parent.view_query.is_some() {
                anyhow::bail!(
                    "inherited relation \"{table_only}\" is not a table or foreign table"
                );
            }
            parents.push(parent);
        }
        Ok(parents)
    }

    /// The checks PostgreSQL makes before a child takes parents on
    /// (`ALTER TABLE ... INHERIT`).
    pub(crate) fn alter_inherit_parents(
        &self,
        child: &TableDescriptor,
        parents: &[TableDescriptor],
    ) -> Result<()> {
        for parent in parents {
            // The child must already carry every parent column.
            for column in &parent.columns {
                match child.columns.iter().find(|c| c.name == column.name) {
                    Some(own) if own.data_type != column.data_type => {
                        anyhow::bail!(
                            crate::error_fields::DbError::new(format!(
                                "column \"{}\" has a type conflict",
                                column.name
                            ))
                            .detail(format!("{} versus {}", own.data_type, column.data_type))
                            .into_text()
                        );
                    }
                    Some(own) if !column.nullable && own.nullable => {
                        anyhow::bail!(
                            "column \"{}\" in child table \"{}\" must be marked NOT NULL",
                            column.name,
                            child.name
                        );
                    }
                    Some(_) => {}
                    None => {
                        anyhow::bail!("child table is missing column \"{}\"", column.name);
                    }
                }
            }
            // The child must already carry the parent's CHECK constraints,
            // by name and definition.
            for constraint in &parent.constraints {
                let TableConstraint::Check { expr, .. } = constraint else {
                    continue;
                };
                let name = constraint.effective_name(&parent.name);
                let found = child
                    .constraints
                    .iter()
                    .find(|c| c.effective_name(&child.name) == name);
                match found {
                    Some(TableConstraint::Check { expr: own, .. }) if own == expr => {}
                    _ => {
                        anyhow::bail!("child table is missing constraint \"{name}\"");
                    }
                }
            }
            // An inheritance loop may not form.
            if self.is_descendant(parent.id, child.id, "default")? || parent.id == child.id {
                anyhow::bail!(
                    crate::error_fields::DbError::new("circular inheritance not allowed")
                        .detail(format!(
                            "\"{}\" is already a child of \"{}\".",
                            parent.name, child.name
                        ))
                        .into_text()
                );
            }
        }
        Ok(())
    }

    /// Whether `table` is `ancestor` or one of its descendants (walking the
    /// parents up from `table`).
    pub(crate) fn is_descendant(
        &self,
        table: TableId,
        ancestor: TableId,
        db_name: &str,
    ) -> Result<bool> {
        let all = self.catalog_reader.list_all_tables(db_name)?;
        let mut frontier = vec![table];
        let mut seen: Vec<TableId> = Vec::new();
        while let Some(id) = frontier.pop() {
            if seen.contains(&id) {
                continue;
            }
            seen.push(id);
            if id == ancestor {
                return Ok(true);
            }
            if let Some(descriptor) = all.iter().find(|t| t.id == id) {
                frontier.extend(descriptor.parents.iter().copied());
            }
        }
        Ok(false)
    }

    /// A child's columns and constraints: its parents' first, then its own,
    /// same-named columns merged (types must match, `NOT NULL` wins, the
    /// child's default wins) and the parents' `CHECK` constraints copied
    /// down by name.
    pub(crate) fn merge_inherited(
        &self,
        ctx: &ExecutionContext,
        parents: &[TableDescriptor],
        own: Vec<ColumnDef>,
    ) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>)> {
        let mut out: Vec<ColumnDef> = Vec::new();
        let mut checks: Vec<TableConstraint> = Vec::new();
        let mut seen_checks: Vec<String> = Vec::new();
        for parent in parents {
            for column in &parent.columns {
                let inherited = inherited_column(column);
                match out.iter_mut().find(|c| c.name == inherited.name) {
                    Some(existing) => {
                        self.notice(
                            ctx,
                            crate::error_fields::DbError::new(format!(
                                "merging multiple inherited definitions of column \"{}\"",
                                inherited.name
                            )),
                        );
                        type_conflict(&inherited.name, &existing.data_type, &inherited.data_type)?;
                        merge_column(existing, &inherited);
                    }
                    None => out.push(inherited),
                }
            }
            for constraint in &parent.constraints {
                let TableConstraint::Check { name, .. } = constraint else {
                    continue;
                };
                let effective = constraint.effective_name(&parent.name);
                if seen_checks.contains(&effective) {
                    continue;
                }
                seen_checks.push(effective.clone());
                checks.push(TableConstraint::Check {
                    name: name.clone().or(Some(effective)),
                    expr: match constraint {
                        TableConstraint::Check { expr, .. } => expr.clone(),
                        _ => unreachable!(),
                    },
                });
            }
        }
        for column in own {
            match out.iter_mut().find(|c| c.name == column.name) {
                Some(existing) => {
                    self.notice(
                        ctx,
                        crate::error_fields::DbError::new(format!(
                            "merging column \"{}\" with inherited definition",
                            column.name
                        )),
                    );
                    type_conflict(&column.name, &existing.data_type, &column.data_type)?;
                    existing.nullable = existing.nullable && column.nullable;
                    if column.default.is_some() {
                        existing.default = column.default.clone();
                    }
                    // The child's own key and sequence attributes are its own.
                    existing.primary = column.primary;
                    existing.unique = column.unique;
                    existing.sequence = column.sequence.clone();
                }
                None => out.push(column),
            }
        }
        Ok((out, checks))
    }

    /// The rows of `tbl` and its descendants, each row carrying the hidden
    /// `tableoid` value of the table it came from. A descendant's own extra
    /// columns (after `ALTER TABLE ONLY parent DROP COLUMN`) are dropped by
    /// name.
    pub(crate) fn inherited_rows(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
    ) -> Result<Vec<Vec<Value>>> {
        let db_name = "default";
        let mut tables = vec![tbl.clone()];
        tables.extend(self.descendants(db_name, tbl.id)?);
        let mut rows = Vec::new();
        for table in &tables {
            let source = Self::table_oid(db_name, &self.schema_name_of(table), &table.name);
            let projection: Vec<Option<usize>> = tbl
                .columns
                .iter()
                .map(|c| table.columns.iter().position(|t| t.name == c.name))
                .collect();
            for row in self.scan_rows(table.id, &ctx.session_id)? {
                let mut projected = Vec::with_capacity(tbl.columns.len() + 1);
                for at in &projection {
                    projected.push(at.and_then(|i| row.get(i)).cloned().unwrap_or(Value::Null));
                }
                projected.push(Value::Int(source));
                rows.push(projected);
            }
        }
        Ok(rows)
    }
}

/// The error a merging column with mismatched types raises.
fn type_conflict(name: &str, existing: &str, new: &str) -> Result<()> {
    if existing == new {
        return Ok(());
    }
    anyhow::bail!(
        crate::error_fields::DbError::new(format!("column \"{name}\" has a type conflict"))
            .detail(format!("{existing} versus {new}"))
            .into_text()
    )
}

/// A parent's column as the child inherits it: its default comes too, and
/// its key attributes do not.
fn inherited_column(column: &ColumnDescriptor) -> ColumnDef {
    ColumnDef {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
        unique: false,
        primary: false,
        default: column
            .default_expr
            .as_ref()
            .and_then(|text| serde_json::from_str::<ScalarExpr>(text).ok()),
        sequence: None,
    }
}

/// Folds an inherited definition into a merging column: `NOT NULL` wins, and
/// the inherited default applies when the local side has none.
fn merge_column(into: &mut ColumnDef, inherited: &ColumnDef) {
    into.nullable = into.nullable && inherited.nullable;
    if into.default.is_none() {
        into.default = inherited.default.clone();
    }
}

#[cfg(test)]
mod tests {
    use crate::dml_join_tests::{rows, session};

    /// The rows of a select, rendered.
    fn values(
        sql: &impl Fn(&str) -> anyhow::Result<crate::QueryOutput>,
        statement: &str,
    ) -> Vec<String> {
        rows(&sql(statement).unwrap())
    }

    #[test]
    fn children_inherit_columns_constraints_and_scans() {
        let sql = session();
        sql("create table ip (a int not null, b text default 'x', ck int check (ck > 0))").unwrap();
        sql("create table ic (z int) inherits (ip)").unwrap();
        // The parents' columns come first.
        assert_eq!(
            values(
                &sql,
                "select string_agg(column_name, ',' order by ordinal_position) from information_schema.columns \
                 where table_name = 'ic' and table_schema = 'public'"
            ),
            ["a,b,ck,z"]
        );
        // CHECK and NOT NULL come down.
        assert_eq!(
            values(
                &sql,
                "select conname from pg_constraint where conrelid = 'ic'::regclass order by conname"
            ),
            ["ip_a_not_null", "ip_ck_check"]
        );
        // Scanning the parent reads the child; ONLY and tableoid follow.
        sql("insert into ip values (1, 'p', 1)").unwrap();
        sql("insert into ic values (2, 'c', 2, 9)").unwrap();
        assert_eq!(values(&sql, "select a from ip order by a"), ["1", "2"]);
        assert_eq!(values(&sql, "select a from only ip"), ["1"]);
        assert_eq!(
            values(&sql, "select tableoid::regclass, a from ip order by a"),
            ["ic|2", "ip|1"]
        );
        // Writes through the parent reach the child's rows.
        sql("update ip set b = 'u' where a = 2").unwrap();
        assert_eq!(values(&sql, "select b from ic"), ["u"]);
        sql("delete from ip where a = 1").unwrap();
        assert_eq!(values(&sql, "select count(*) from ip"), ["1"]);
        // TRUNCATE takes descendants unless ONLY.
        sql("truncate only ic").unwrap();
        assert_eq!(values(&sql, "select count(*) from ic"), ["0"]);
        // DROP without CASCADE is refused while the child exists.
        let err = sql("drop table ip").unwrap_err();
        assert!(
            crate::error_fields::error_message(&err.to_string())
                .contains("cannot drop table ip because other objects depend on it"),
            "{err}"
        );
        sql("drop table ip cascade").unwrap();
        assert!(
            sql("select count(*) from ic").is_err(),
            "cascade dropped ic"
        );
    }

    #[test]
    fn only_reads_just_the_table() {
        let sql = session();
        sql("create table op (a int)").unwrap();
        sql("create table oc () inherits (op)").unwrap();
        sql("create table og () inherits (oc)").unwrap();
        sql("insert into op values (1)").unwrap();
        sql("insert into oc values (2)").unwrap();
        sql("insert into og values (3)").unwrap();
        assert_eq!(values(&sql, "select a from op order by a"), ["1", "2", "3"]);
        assert_eq!(values(&sql, "select a from only op"), ["1"]);
        assert_eq!(values(&sql, "select a from only oc"), ["2"]);
        assert_eq!(values(&sql, "select a from only op x"), ["1"]);
        assert_eq!(values(&sql, "select a from only op as x"), ["1"]);
        assert_eq!(
            values(
                &sql,
                "select p1.a from op p1 join only oc c on true order by p1.a"
            ),
            ["1", "2", "3"]
        );
    }

    /// One message, without the field suffix a `DbError` carries.
    fn message(error: &anyhow::Error) -> String {
        crate::error_fields::error_message(&error.to_string()).to_string()
    }

    #[test]
    fn alters_and_drops_reach_descendants() {
        let sql = session();
        sql("create table hp (a int not null, ck int check (ck > 0))").unwrap();
        sql("create table hc () inherits (hp)").unwrap();
        sql("create table hg () inherits (hc)").unwrap();
        let columns = |table: &str| {
            values(
                &sql,
                &format!(
                    "select string_agg(column_name, ',' order by ordinal_position) \
                     from information_schema.columns where table_name = '{table}' \
                     and table_schema = 'public'"
                ),
            )
        };
        sql("alter table hp add column n int default 7").unwrap();
        assert_eq!(columns("hc"), ["a,ck,n"]);
        assert_eq!(columns("hg"), ["a,ck,n"]);
        sql("alter table hp add constraint ck2 check (n > 0)").unwrap();
        assert_eq!(
            values(
                &sql,
                "select conrelid::regclass from pg_constraint where conname = 'ck2' order by 1::text"
            ),
            ["hc", "hg", "hp"]
        );
        sql("alter table hp rename column n to nn").unwrap();
        assert_eq!(columns("hc"), ["a,ck,nn"]);
        sql("alter table hp drop column nn").unwrap();
        assert_eq!(columns("hc"), ["a,ck"]);
        // A separate constraint, since dropping a column takes the checks
        // that name it with it.
        sql("alter table hp add constraint ck3 check (ck > 0)").unwrap();
        for (statement, expected) in [
            (
                "alter table only hp add column o int",
                "column must be added to child tables too",
            ),
            (
                "alter table only hp rename column a to b",
                "inherited column \"a\" must be renamed in child tables too",
            ),
            (
                "alter table only hp add constraint cx check (a > 0)",
                "constraint must be added to child tables too",
            ),
            (
                "alter table hc rename column a to b",
                "cannot rename inherited column \"a\"",
            ),
            (
                "alter table hc drop column a",
                "cannot drop inherited column \"a\"",
            ),
            (
                "alter table hc alter column a type bigint",
                "cannot alter inherited column \"a\"",
            ),
            (
                "alter table hc drop constraint ck3",
                "cannot drop inherited constraint \"ck3\" of relation \"hc\"",
            ),
        ] {
            let err = sql(statement).unwrap_err();
            assert_eq!(message(&err), expected, "{statement}");
        }
        sql("insert into hp values (1, 1)").unwrap();
        sql("insert into hc values (2, 2)").unwrap();
        sql("insert into hg values (3, 3)").unwrap();
        // TRUNCATE takes the descendants; ONLY does not.
        sql("truncate only hc").unwrap();
        assert_eq!(values(&sql, "select count(*) from only hc"), ["0"]);
        assert_eq!(values(&sql, "select count(*) from hg"), ["1"]);
        sql("truncate hp").unwrap();
        assert_eq!(values(&sql, "select count(*) from hg"), ["0"]);
        // DROP takes the children only with CASCADE.
        let err = sql("drop table hp").unwrap_err();
        assert_eq!(
            message(&err),
            "cannot drop table hp because other objects depend on it"
        );
        sql("drop table hp cascade").unwrap();
        assert!(sql("select count(*) from hc").is_err());
    }

    #[test]
    fn inherit_and_no_inherit_attach_and_detach() {
        let sql = session();
        sql("create table ip (a int not null default 5, ck int check (ck > 0))").unwrap();
        sql("create table ic (a int, ck int)").unwrap();
        let err = sql("alter table ic inherit ip").unwrap_err();
        assert_eq!(
            message(&err),
            "column \"a\" in child table \"ic\" must be marked NOT NULL"
        );
        sql("alter table ic alter column a set not null").unwrap();
        let err = sql("alter table ic inherit ip").unwrap_err();
        assert_eq!(
            message(&err),
            "child table is missing constraint \"ip_ck_check\""
        );
        sql("alter table ic add constraint ip_ck_check check (ck > 0)").unwrap();
        sql("alter table ic inherit ip").unwrap();
        assert_eq!(
            values(
                &sql,
                "select count(*) from pg_inherits where inhrelid = 'ic'::regclass"
            ),
            ["1"]
        );
        sql("insert into ip values (1, 1)").unwrap();
        sql("insert into ic values (2, 2)").unwrap();
        assert_eq!(values(&sql, "select a from ip order by a"), ["1", "2"]);
        let err = sql("alter table ic inherit ip").unwrap_err();
        assert_eq!(
            message(&err),
            "relation \"ip\" would be inherited from more than once"
        );
        sql("alter table ic no inherit ip").unwrap();
        assert_eq!(values(&sql, "select a from ip"), ["1"]);
        let err = sql("alter table ic no inherit ip").unwrap_err();
        assert_eq!(
            message(&err),
            "relation \"ip\" is not a parent of relation \"ic\""
        );
    }

    #[test]
    fn inheritance_errors_match_postgresql() {
        let sql = session();
        sql("create table ep (a int)").unwrap();
        sql("create table ec () inherits (ep)").unwrap();
        sql("create view ev as select 1 as x").unwrap();
        for (statement, expected) in [
            (
                "create table x1 () inherits (nosuch)",
                "relation \"nosuch\" does not exist",
            ),
            (
                "create table x2 () inherits (ev)",
                "inherited relation \"ev\" is not a table or foreign table",
            ),
            (
                "create table x3 (a text) inherits (ep)",
                "column \"a\" has a type conflict",
            ),
            (
                "alter table only ep add column o int",
                "column must be added to child tables too",
            ),
        ] {
            let err = sql(statement).unwrap_err();
            assert_eq!(
                crate::error_fields::error_message(&err.to_string()),
                expected,
                "{statement}"
            );
        }
        sql("create table x4 (b int)").unwrap();
        let err = sql("alter table x4 inherit ep").unwrap_err();
        assert_eq!(
            crate::error_fields::error_message(&err.to_string()),
            "child table is missing column \"a\""
        );
    }
}
