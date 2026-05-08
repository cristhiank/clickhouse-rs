use std::fmt;

use crate::errors::{DriverError, Error, Result};

/// A value bound to a named query parameter.
///
/// Values are serialized as single-quoted, ClickHouse-compatible escaped strings on the wire.
/// The `From` implementations for numeric types store their `Display` representation; string
/// types store the raw text which is then quoted during serialization.
#[derive(Clone)]
pub struct QueryParameterValue(String);

impl QueryParameterValue {
    /// Returns the wire representation of this value: a single-quoted, ClickHouse-escaped string.
    pub(crate) fn write_quoted(&self) -> String {
        let s = &self.0;
        let mut out = String::with_capacity(s.len() + 2);
        out.push('\'');
        for ch in s.chars() {
            match ch {
                '\'' => {
                    out.push('\\');
                    out.push('\'');
                }
                '\\' => {
                    out.push('\\');
                    out.push('\\');
                }
                '\n' => {
                    out.push('\\');
                    out.push('n');
                }
                '\r' => {
                    out.push('\\');
                    out.push('r');
                }
                '\t' => {
                    out.push('\\');
                    out.push('t');
                }
                '\0' => {
                    out.push('\\');
                    out.push('0');
                }
                c => out.push(c),
            }
        }
        out.push('\'');
        out
    }
}

impl fmt::Debug for QueryParameterValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted>")
    }
}

macro_rules! impl_from_display {
    ($($t:ty),*) => {
        $(impl From<$t> for QueryParameterValue {
            fn from(v: $t) -> Self {
                Self(v.to_string())
            }
        })*
    };
}

impl_from_display!(
    bool, i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, f32, f64
);

impl From<&str> for QueryParameterValue {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for QueryParameterValue {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Returns `true` when the name satisfies `[A-Za-z_][A-Za-z0-9_]*` and does not
/// start with `param_` (reserved by ClickHouse).
fn validate_parameter_name(name: &str) -> bool {
    if name.is_empty() || name.starts_with("param_") {
        return false;
    }
    let mut chars = name.chars();
    let first = match chars.next() {
        Some(c) => c,
        None => return false,
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[derive(Clone)]
pub struct Query {
    sql: String,
    id: String,
    parameters: Vec<(String, QueryParameterValue)>,
}

impl fmt::Debug for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.parameters.iter().map(|(k, _)| k.as_str()).collect();
        f.debug_struct("Query")
            .field("sql", &self.sql)
            .field("id", &self.id)
            .field("parameter_names", &names)
            .field("parameter_count", &self.parameters.len())
            .finish()
    }
}

impl Query {
    pub fn new(sql: impl AsRef<str>) -> Self {
        Self {
            sql: sql.as_ref().to_string(),
            id: "".to_string(),
            parameters: Vec::new(),
        }
    }

    pub fn id(self, id: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().to_string(),
            ..self
        }
    }

    /// Bind a named query parameter. The name must match `[A-Za-z_][A-Za-z0-9_]*` and must not
    /// start with `param_`. Values are serialized as single-quoted, escaped strings on the wire.
    pub fn with_parameter(
        mut self,
        name: impl AsRef<str>,
        value: impl Into<QueryParameterValue>,
    ) -> Result<Self> {
        let name = name.as_ref().to_owned();
        if !validate_parameter_name(&name) {
            return Err(Error::Driver(DriverError::InvalidParameterName {
                name,
            }));
        }
        self.parameters.push((name, value.into()));
        Ok(self)
    }

    pub(crate) fn get_sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn get_id(&self) -> &str {
        &self.id
    }

    pub(crate) fn get_parameters(&self) -> &[(String, QueryParameterValue)] {
        &self.parameters
    }

    pub(crate) fn has_parameters(&self) -> bool {
        !self.parameters.is_empty()
    }

    pub(crate) fn map_sql<F>(self, f: F) -> Self
    where
        F: Fn(&str) -> String,
    {
        Self {
            sql: f(&self.sql),
            ..self
        }
    }
}

impl<T> From<T> for Query
where
    T: AsRef<str>,
{
    fn from(source: T) -> Self {
        Self::new(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_parameter_name_valid() {
        assert!(validate_parameter_name("foo"));
        assert!(validate_parameter_name("foo_bar"));
        assert!(validate_parameter_name("_foo"));
        assert!(validate_parameter_name("Foo123"));
        assert!(validate_parameter_name("A"));
    }

    #[test]
    fn test_validate_parameter_name_invalid() {
        assert!(!validate_parameter_name(""));
        assert!(!validate_parameter_name("param_foo")); // reserved prefix
        assert!(!validate_parameter_name("1foo"));      // digit start
        assert!(!validate_parameter_name("foo.bar"));   // dot
        assert!(!validate_parameter_name("foo bar"));   // space
        assert!(!validate_parameter_name("foo{bar}"));  // braces
        assert!(!validate_parameter_name("foo:bar"));   // colon
        assert!(!validate_parameter_name("fooé"));      // non-ASCII
    }

    #[test]
    fn test_query_parameter_value_write_quoted_plain() {
        let v = QueryParameterValue::from("hello");
        assert_eq!(v.write_quoted(), "'hello'");
    }

    #[test]
    fn test_query_parameter_value_write_quoted_escape_backslash() {
        let v = QueryParameterValue::from("a\\b");
        assert_eq!(v.write_quoted(), "'a\\\\b'");
    }

    #[test]
    fn test_query_parameter_value_write_quoted_escape_quote() {
        let v = QueryParameterValue::from("O'Reilly");
        assert_eq!(v.write_quoted(), "'O\\'Reilly'");
    }

    #[test]
    fn test_query_parameter_value_write_quoted_newline() {
        let v = QueryParameterValue::from("a\nb");
        assert_eq!(v.write_quoted(), "'a\\nb'");
    }

    #[test]
    fn test_query_parameter_value_write_quoted_null() {
        let v = QueryParameterValue::from("a\0b");
        assert_eq!(v.write_quoted(), "'a\\0b'");
    }

    #[test]
    fn test_query_parameter_value_negative_int() {
        let v = QueryParameterValue::from(-1_i64);
        assert_eq!(v.write_quoted(), "'-1'");
    }

    #[test]
    fn test_query_parameter_value_bool() {
        assert_eq!(QueryParameterValue::from(true).write_quoted(), "'true'");
        assert_eq!(QueryParameterValue::from(false).write_quoted(), "'false'");
    }

    #[test]
    fn test_query_parameter_value_float() {
        let v = QueryParameterValue::from(3.14_f64);
        assert!(v.write_quoted().starts_with("'3.14"));
    }

    #[test]
    fn test_query_parameter_value_non_ascii_string() {
        // Non-ASCII characters in string values pass through unescaped.
        let v = QueryParameterValue::from("héllo");
        assert_eq!(v.write_quoted(), "'héllo'");
    }

    #[test]
    fn test_debug_redacts_values() {
        let q = Query::new("SELECT 1")
            .with_parameter("secret", "top_secret_value")
            .unwrap();
        let dbg = format!("{:?}", q);
        assert!(dbg.contains("secret"));
        assert!(!dbg.contains("top_secret_value"));
        assert!(dbg.contains("parameter_count: 1"));
    }

    #[test]
    fn test_with_parameter_invalid_name_returns_error() {
        let result = Query::new("SELECT 1").with_parameter("param_bad", "v");
        assert!(result.is_err());
    }

    #[test]
    fn test_with_parameter_chaining() {
        let q = Query::new("SELECT {a:Int32} + {b:Int32}")
            .with_parameter("a", 1_i32)
            .unwrap()
            .with_parameter("b", 2_i32)
            .unwrap();
        assert_eq!(q.get_parameters().len(), 2);
        assert_eq!(q.get_parameters()[0].0, "a");
        assert_eq!(q.get_parameters()[1].0, "b");
    }
}
