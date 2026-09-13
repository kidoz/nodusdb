//! Lower COPY TO STDOUT to a normal read plan so catalog resolution,
//! authorization, and transaction visibility use the regular executor.

use crate::{LogicalPlan, ProjectionItem};
use anyhow::{Result, bail};
use sqlparser::ast::{CopyOption, CopySource, CopyTarget, Statement};

/// Supported COPY output representations. Non-default delimiters and other
/// options are rejected explicitly until their semantics are implemented.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyOutputFormat {
    Text,
    Csv,
    Binary,
}

/// Parses a single COPY TO STDOUT statement and returns its read plan, format,
/// and CSV header flag. Files and programs are never opened by this path.
pub fn plan_copy_out(sql: &str) -> Result<(LogicalPlan, CopyOutputFormat, bool)> {
    let mut statements = nodus_sql::parse_sql(sql)?;
    if statements.len() != 1 {
        bail!("COPY requires a single statement");
    }
    let Some(Statement::Copy {
        source,
        to: true,
        target: CopyTarget::Stdout,
        options,
        legacy_options,
        ..
    }) = statements.pop()
    else {
        bail!("only COPY TO STDOUT is supported");
    };
    if !legacy_options.is_empty() {
        bail!("legacy COPY options are not supported; use WITH (FORMAT ...)");
    }
    let mut format = CopyOutputFormat::Text;
    let mut header = false;
    for option in options {
        match option {
            CopyOption::Format(name) => {
                format = match name.value.to_ascii_lowercase().as_str() {
                    "text" => CopyOutputFormat::Text,
                    "csv" => CopyOutputFormat::Csv,
                    "binary" => CopyOutputFormat::Binary,
                    _ => bail!("unsupported COPY format: {name}"),
                }
            }
            CopyOption::Header(value) => header = value,
            other => bail!("unsupported COPY option: {other}"),
        }
    }
    if header && format != CopyOutputFormat::Csv {
        bail!("COPY HEADER requires CSV format");
    }
    let plan = match source {
        CopySource::Query(query) => super::plan_statement(&Statement::Query(query), &[])?,
        CopySource::Table {
            table_name,
            columns,
        } => LogicalPlan::Select {
            ctes: vec![],
            table_name: table_name.to_string(),
            table_alias: None,
            joins: vec![],
            projection: columns
                .into_iter()
                .map(|c| ProjectionItem::Column(c.value))
                .collect(),
            group_by: vec![],
            filter: None,
            having: None,
            grouping_sets: None,
            order_by: vec![],
            limit: None,
            offset: None,
            distinct: false,
        },
    };
    Ok((plan, format, header))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn copy_is_a_read_plan_and_rejects_unsupported_options() {
        let (plan, format, header) =
            plan_copy_out("COPY \"odd table\" (id) TO STDOUT WITH (FORMAT CSV, HEADER)").unwrap();
        assert!(matches!(plan, LogicalPlan::Select { .. }));
        assert_eq!(format, CopyOutputFormat::Csv);
        assert!(header);
        assert!(plan_copy_out("COPY t TO PROGRAM 'cat'").is_err());
        assert!(plan_copy_out("COPY t TO STDOUT (DELIMITER '|')").is_err());
        assert!(plan_copy_out("COPY t TO STDOUT; DELETE FROM t").is_err());
    }
}
