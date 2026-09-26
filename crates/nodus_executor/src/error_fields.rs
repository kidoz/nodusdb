//! Errors with the fields PostgreSQL reports beside the message: `DETAIL`,
//! `HINT`, and the schema, table, column, and constraint an error concerns.
//! They travel in the error's text after the message, each behind a unit
//! separator, so they survive every layer that passes errors on as text;
//! the wire layer splits them back out.

/// Separates the message and each `name=value` field.
const SEPARATOR: char = '\u{1f}';

/// An error message with fields, built up and then turned into an error.
pub(crate) struct DbError {
    text: String,
}

impl DbError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        DbError {
            text: message.into(),
        }
    }

    fn field(mut self, name: &str, value: impl AsRef<str>) -> Self {
        self.text.push(SEPARATOR);
        self.text.push_str(name);
        self.text.push('=');
        self.text.push_str(value.as_ref());
        self
    }

    /// The SQLSTATE of a notice, when it is not `00000`.
    pub(crate) fn code(self, code: &str) -> Self {
        self.field("code", code)
    }

    /// The message and its fields, as they travel.
    pub(crate) fn into_text(self) -> String {
        self.text
    }

    pub(crate) fn detail(self, detail: impl AsRef<str>) -> Self {
        self.field("detail", detail)
    }

    pub(crate) fn hint(self, hint: impl AsRef<str>) -> Self {
        self.field("hint", hint)
    }

    pub(crate) fn schema(self, schema: impl AsRef<str>) -> Self {
        self.field("schema", schema)
    }

    pub(crate) fn table(self, table: impl AsRef<str>) -> Self {
        self.field("table", table)
    }

    pub(crate) fn column(self, column: impl AsRef<str>) -> Self {
        self.field("column", column)
    }

    pub(crate) fn constraint(self, constraint: impl AsRef<str>) -> Self {
        self.field("constraint", constraint)
    }
}

impl From<DbError> for anyhow::Error {
    fn from(error: DbError) -> Self {
        anyhow::anyhow!(error.text)
    }
}

/// An error's message, without its fields.
pub fn error_message(error: &str) -> &str {
    error.split(SEPARATOR).next().unwrap_or(error)
}

/// An error's fields, as `(name, value)` pairs: `detail`, `hint`, `schema`,
/// `table`, `column`, `constraint`, and for a notice `code`.
pub fn error_fields(error: &str) -> Vec<(&str, &str)> {
    error
        .split(SEPARATOR)
        .skip(1)
        .filter_map(|field| field.split_once('='))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_travel_after_the_message() {
        let error: anyhow::Error = DbError::new("duplicate key")
            .detail("Key (id)=(1) already exists.")
            .table("t")
            .constraint("t_pkey")
            .into();
        let text = error.to_string();
        assert_eq!(error_message(&text), "duplicate key");
        assert_eq!(
            error_fields(&text),
            [
                ("detail", "Key (id)=(1) already exists."),
                ("table", "t"),
                ("constraint", "t_pkey")
            ]
        );
        assert_eq!(error_message("plain"), "plain");
        assert!(error_fields("plain").is_empty());
    }
}
