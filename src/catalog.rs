//! Persistent catalog structures and encoding/decoding.

use crate::config::PAGE_SIZE;
use crate::encoding;
use crate::error::{InvError, InvResult};
use crate::schema::{ColType, Column, Schema};

/// Strongly typed table identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TableId(pub u32);

/// Table definition stored in the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableDef {
    pub id: TableId,
    pub name: String,
    pub schema: Schema,
    pub next_pk: u32,
    pub last_row_page: u32,
}

#[derive(Clone, Debug)]
pub struct Catalog {
    pub(crate) next_table_id: u32,
    pub(crate) tables: Vec<TableDef>,
}

impl Catalog {
    pub fn empty() -> Self {
        Self {
            next_table_id: 1,
            tables: Vec::new(),
        }
    }

    pub fn get_by_name(&self, name: &str) -> Option<&TableDef> {
        self.tables.iter().find(|t| t.name == name)
    }

    pub fn list(&self) -> Vec<TableDef> {
        self.tables.clone()
    }

    pub fn create_table(&mut self, name: &str, schema: &Schema) -> InvResult<TableId> {
        validate_table_name(name)?;
        if self.get_by_name(name).is_some() {
            return Err(InvError::InvalidArgument {
                name: "table.name",
                details: "duplicate table name".to_string(),
            });
        }
        let id = self.next_table_id;
        self.next_table_id = self
            .next_table_id
            .checked_add(1)
            .ok_or(InvError::Overflow {
                context: "catalog.next_table_id",
            })?;

        self.tables.push(TableDef {
            id: TableId(id),
            name: name.to_string(),
            schema: schema.clone(),
            next_pk: 1,
            last_row_page: 0,
        });
        Ok(TableId(id))
    }
}

fn validate_table_name(name: &str) -> InvResult<()> {
    if name.is_empty() || name.len() > 64 {
        return Err(InvError::InvalidArgument {
            name: "table.name",
            details: "name must be 1..=64 chars".to_string(),
        });
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(InvError::InvalidArgument {
            name: "table.name",
            details: "invalid characters in name".to_string(),
        });
    }
    Ok(())
}

/// Encode a schema to deterministic bytes for catalog storage.
pub fn encode_schema(schema: &Schema) -> InvResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"SCH1");
    encoding::write_var_u64(&mut out, schema.len() as u64);
    for col in &schema.columns {
        if col.name.len() > 64 {
            return Err(InvError::InvalidArgument {
                name: "column.name",
                details: "name too long".to_string(),
            });
        }
        encoding::write_bytes(&mut out, col.name.as_bytes());
        out.push(col_type_tag(&col.ty)?);
        out.push(if col.nullable { 1 } else { 0 });
    }
    Ok(out)
}

/// Decode schema bytes into a Schema instance.
pub fn decode_schema(bytes: &[u8]) -> InvResult<Schema> {
    if bytes.len() < 4 || &bytes[0..4] != b"SCH1" {
        return Err(InvError::Corruption {
            context: "schema.magic",
            details: "bad schema magic".to_string(),
        });
    }
    let mut pos = 4;
    let col_count = encoding::read_var_u64(bytes, &mut pos)? as usize;
    let mut cols = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let name_bytes = encoding::read_bytes(bytes, &mut pos, 64)?;
        let name = String::from_utf8(name_bytes).map_err(|e| InvError::Corruption {
            context: "schema.name.utf8",
            details: e.to_string(),
        })?;
        let ty_tag = *bytes.get(pos).ok_or(InvError::Corruption {
            context: "schema.col_type",
            details: "missing tag".to_string(),
        })?;
        pos += 1;
        let ty = tag_to_col_type(ty_tag)?;
        let nullable_byte = *bytes.get(pos).ok_or(InvError::Corruption {
            context: "schema.nullable",
            details: "missing nullable byte".to_string(),
        })?;
        pos += 1;
        let nullable = match nullable_byte {
            0 => false,
            1 => true,
            _ => {
                return Err(InvError::Corruption {
                    context: "schema.nullable",
                    details: format!("invalid nullable byte {}", nullable_byte),
                })
            }
        };
        cols.push(Column {
            name,
            ty,
            nullable,
        });
    }
    Schema::new(cols).map_err(|e| match e {
        InvError::InvalidArgument { .. } => InvError::Corruption {
            context: "schema.invalid",
            details: e.to_string(),
        },
        other => other,
    })
}

fn col_type_tag(ty: &ColType) -> InvResult<u8> {
    Ok(match ty {
        ColType::U32 => 1,
        ColType::U64 => 2,
        ColType::I64 => 3,
        ColType::Bool => 4,
        ColType::Bytes => 5,
        ColType::String => 6,
    })
}

fn tag_to_col_type(tag: u8) -> InvResult<ColType> {
    match tag {
        1 => Ok(ColType::U32),
        2 => Ok(ColType::U64),
        3 => Ok(ColType::I64),
        4 => Ok(ColType::Bool),
        5 => Ok(ColType::Bytes),
        6 => Ok(ColType::String),
        _ => Err(InvError::Corruption {
            context: "schema.col_type",
            details: format!("unknown tag {}", tag),
        }),
    }
}

/// Encode a catalog into payload bytes (starting at page payload).
pub fn encode_catalog(cat: &Catalog) -> InvResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"CAT1");
    out.extend_from_slice(&1u16.to_le_bytes()); // version
    let entry_count: u16 = cat
        .tables
        .len()
        .try_into()
        .map_err(|_| InvError::Unsupported {
            feature: "catalog.page_overflow",
        })?;
    out.extend_from_slice(&entry_count.to_le_bytes());
    out.extend_from_slice(&cat.next_table_id.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // reserved

    for table in &cat.tables {
        out.extend_from_slice(&table.id.0.to_le_bytes());
        encoding::write_bytes(&mut out, table.name.as_bytes());
        let schema_bytes = encode_schema(&table.schema)?;
        if schema_bytes.len() > 64 * 1024 {
            return Err(InvError::Corruption {
                context: "catalog.schema.too_large",
                details: format!("schema bytes {}", schema_bytes.len()),
            });
        }
        encoding::write_bytes(&mut out, &schema_bytes);
        out.extend_from_slice(&table.next_pk.to_le_bytes());
        out.extend_from_slice(&table.last_row_page.to_le_bytes());
    }

    if out.len() > PAGE_SIZE - 16 {
        return Err(InvError::Unsupported {
            feature: "catalog.page_overflow",
        });
    }

    Ok(out)
}

/// Decode catalog payload bytes into Catalog struct.
pub fn decode_catalog(payload: &[u8]) -> InvResult<Catalog> {
    if payload.len() < 16 {
        return Err(InvError::Corruption {
            context: "catalog.eof",
            details: "payload too small".to_string(),
        });
    }
    if &payload[0..4] != b"CAT1" {
        return Err(InvError::Corruption {
            context: "catalog.magic",
            details: "invalid catalog magic".to_string(),
        });
    }
    let version = u16::from_le_bytes([payload[4], payload[5]]);
    if version != 1 {
        return Err(InvError::Unsupported {
            feature: "catalog.version",
        });
    }
    let entry_count = u16::from_le_bytes([payload[6], payload[7]]) as usize;
    let next_table_id = u32::from_le_bytes([payload[8], payload[9], payload[10], payload[11]]);
    let reserved = u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]);
    if reserved != 0 {
        return Err(InvError::Unsupported {
            feature: "catalog.reserved",
        });
    }

    let mut pos = 16usize;
    let mut tables = Vec::with_capacity(entry_count);
    let mut name_set = std::collections::HashSet::new();
    let mut id_set = std::collections::HashSet::new();
    for _ in 0..entry_count {
        if pos + 4 > payload.len() {
            return Err(InvError::Corruption {
                context: "catalog.eof",
                details: "truncated table_id".to_string(),
            });
        }
        let table_id = u32::from_le_bytes([
            payload[pos],
            payload[pos + 1],
            payload[pos + 2],
            payload[pos + 3],
        ]);
        pos += 4;
        let name_bytes = encoding::read_bytes(payload, &mut pos, 256)?;
        let name = String::from_utf8(name_bytes).map_err(|e| InvError::Corruption {
            context: "catalog.name",
            details: e.to_string(),
        })?;
        if name.is_empty() || name.len() > 64 || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(InvError::Corruption {
                context: "catalog.name",
                details: "invalid table name on disk".to_string(),
            });
        }
        let schema_bytes = encoding::read_bytes(payload, &mut pos, 64 * 1024)?;
        let schema = decode_schema(&schema_bytes)?;

        if pos + 8 > payload.len() {
            return Err(InvError::Corruption {
                context: "catalog.eof",
                details: "truncated table pk metadata".to_string(),
            });
        }
        let next_pk = u32::from_le_bytes([
            payload[pos],
            payload[pos + 1],
            payload[pos + 2],
            payload[pos + 3],
        ]);
        pos += 4;
        let last_row_page = u32::from_le_bytes([
            payload[pos],
            payload[pos + 1],
            payload[pos + 2],
            payload[pos + 3],
        ]);
        pos += 4;

        if next_pk < 1 {
            return Err(InvError::Corruption {
                context: "catalog.next_pk",
                details: format!("invalid next_pk {}", next_pk),
            });
        }

        if !id_set.insert(table_id) || !name_set.insert(name.clone()) {
            return Err(InvError::Corruption {
                context: "catalog.duplicate",
                details: "duplicate table id or name".to_string(),
            });
        }

        tables.push(TableDef {
            id: TableId(table_id),
            name,
            schema,
            next_pk,
            last_row_page,
        });
    }

    Ok(Catalog {
        next_table_id,
        tables,
    })
}
