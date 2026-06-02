use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlLogicRecordKind {
    Statement,
    Query,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlLogicRecord {
    pub kind: SqlLogicRecordKind,
    pub text: String,
}

pub fn parse_case(input: &str) -> Vec<SqlLogicRecord> {
    let mut records = Vec::new();
    let mut current_kind = None;
    let mut current_sql = Vec::new();

    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("statement ") || trimmed.starts_with("query ") {
            if let Some(kind) = current_kind.take() {
                records.push(SqlLogicRecord {
                    kind,
                    text: current_sql.join("\n").trim().to_string(),
                });
                current_sql.clear();
            }

            current_kind = if trimmed.starts_with("statement ") {
                Some(SqlLogicRecordKind::Statement)
            } else {
                Some(SqlLogicRecordKind::Query)
            };
            continue;
        }

        if trimmed == "----" {
            continue;
        }

        if current_kind.is_some() && !trimmed.is_empty() {
            current_sql.push(line.to_string());
        }
    }

    if let Some(kind) = current_kind {
        records.push(SqlLogicRecord {
            kind,
            text: current_sql.join("\n").trim().to_string(),
        });
    }

    records
}

pub fn case_paths(root: impl AsRef<Path>) -> std::io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "slt") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::{SqlLogicRecordKind, parse_case};

    #[test]
    fn parses_statement_and_query_records() {
        let records = parse_case(
            "statement ok\nCREATE TABLE users (id INT PRIMARY KEY);\n\nquery I\nSELECT id FROM users;\n----\n1\n",
        );

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].kind, SqlLogicRecordKind::Statement);
        assert_eq!(records[0].text, "CREATE TABLE users (id INT PRIMARY KEY);");
        assert_eq!(records[1].kind, SqlLogicRecordKind::Query);
        assert!(records[1].text.contains("SELECT id FROM users;"));
    }
}
