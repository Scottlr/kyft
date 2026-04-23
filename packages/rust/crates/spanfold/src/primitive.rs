use serde::{Deserialize, Serialize};

/// JSON-compatible primitive value used by tags, segments, annotations, and
/// fixture/import boundaries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PrimitiveValue {
    /// String value.
    String(String),
    /// Signed integer value.
    Integer(i64),
    /// Floating point value.
    Float(f64),
    /// Boolean value.
    Bool(bool),
    /// Null value.
    Null,
}

impl From<&str> for PrimitiveValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for PrimitiveValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<i64> for PrimitiveValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<i32> for PrimitiveValue {
    fn from(value: i32) -> Self {
        Self::Integer(i64::from(value))
    }
}

impl From<f64> for PrimitiveValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<bool> for PrimitiveValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}
