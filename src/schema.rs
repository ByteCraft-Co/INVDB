//! Minimal schema representation with validation.

use crate::error::{InvError, InvResult};

/// Column data types supported by the row codec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColType {
    U32,
    U64,
    I64,
    Bool,
    Bytes,
    String,
}

/// Column definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColType,
    pub nullable: bool,
}

/// Simple schema holding an ordered set of columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    /// Construct a validated schema.
    pub fn new(columns: Vec<Column>) -> InvResult<Self> {
        if columns.is_empty() {
            return Err(InvError::InvalidArgument {
                name: "columns",
                details: "schema must have at least one column".to_string(),
            });
        }

        let mut seen = std::collections::HashSet::new();
        for col in &columns {
            if col.name.is_empty() {
                return Err(InvError::InvalidArgument {
                    name: "column.name",
                    details: "name must not be empty".to_string(),
                });
            }
            if !col
                .name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(InvError::InvalidArgument {
                    name: "column.name",
                    details: format!("invalid characters in name '{}'", col.name),
                });
            }
            if !seen.insert(col.name.clone()) {
                return Err(InvError::InvalidArgument {
                    name: "column.name",
                    details: format!("duplicate column name '{}'", col.name),
                });
            }
        }

        // TODO: constraints, indexes, defaults.

        Ok(Self { columns })
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns true if there are no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}
