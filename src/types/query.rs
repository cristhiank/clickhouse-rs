use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Query {
    sql: String,
    id: String,
    /// Server-side query parameters (referenced from SQL as `{name:Type}`).
    /// `None` value encodes as the SQL NULL representation on the wire.
    params: HashMap<String, Option<String>>,
}

impl Query {
    pub fn new(sql: impl AsRef<str>) -> Self {
        Self {
            sql: sql.as_ref().to_string(),
            id: "".to_string(),
            params: HashMap::new(),
        }
    }

    pub fn id(self, id: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().to_string(),
            ..self
        }
    }

    /// Bind a server-side parameter referenced as `{name:Type}` in the SQL.
    /// The value is sent over the native protocol — no client-side string
    /// substitution happens, so this is safe against SQL injection.
    pub fn with_param(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.params.insert(name.into(), Some(value.into()));
        self
    }

    /// Bind a server-side parameter to SQL NULL.
    pub fn with_null_param(mut self, name: impl Into<String>) -> Self {
        self.params.insert(name.into(), None);
        self
    }

    pub(crate) fn get_sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn get_id(&self) -> &str {
        &self.id
    }

    pub(crate) fn get_params(&self) -> &HashMap<String, Option<String>> {
        &self.params
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
