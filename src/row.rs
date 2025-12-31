//! Row value representation and deterministic encoding/decoding.

use crate::encoding;
use crate::error::{InvError, InvResult};
use crate::schema::{ColType, Schema};

/// Logical value types supported by the row codec.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    U32(u32),
    U64(u64),
    I64(i64),
    Bool(bool),
    Bytes(Vec<u8>),
    String(String),
}

/// A row is a sequence of values matching a schema.
pub type Row = Vec<Value>;

const ROW_MAGIC: &[u8; 4] = b"ROW1";
const MAX_VAR_LEN: usize = 1_048_576; // 1 MiB guard

/// Encode a row according to the provided schema.
pub fn encode_row(schema: &Schema, row: &Row) -> InvResult<Vec<u8>> {
    if schema.len() != row.len() {
        return Err(InvError::InvalidArgument {
            name: "row",
            details: format!(
                "schema columns {} != row values {}",
                schema.len(),
                row.len()
            ),
        });
    }

    let mut out = Vec::new();
    out.extend_from_slice(ROW_MAGIC);
    encoding::write_var_u64(&mut out, schema.len() as u64);

    for (idx, (col, val)) in schema.columns.iter().zip(row.iter()).enumerate() {
        match (&col.ty, val) {
            (_, Value::Null) if !col.nullable => {
                return Err(InvError::InvalidArgument {
                    name: "row.null",
                    details: format!("column '{}' is not nullable", col.name),
                });
            }
            (ColType::U32, Value::U32(v)) => {
                out.push(0x01);
                encoding::write_u32_le(&mut out, *v);
            }
            (ColType::U64, Value::U64(v)) => {
                out.push(0x02);
                encoding::write_u64_le(&mut out, *v);
            }
            (ColType::I64, Value::I64(v)) => {
                out.push(0x03);
                out.extend_from_slice(&v.to_le_bytes());
            }
            (ColType::Bool, Value::Bool(b)) => {
                out.push(0x04);
                out.push(if *b { 1 } else { 0 });
            }
            (ColType::Bytes, Value::Bytes(bytes)) => {
                out.push(0x05);
                encoding::write_bytes(&mut out, bytes);
            }
            (ColType::String, Value::String(s)) => {
                out.push(0x06);
                encoding::write_string(&mut out, s);
            }
            (_, Value::Null) => {
                out.push(0x00);
            }
            _ => {
                return Err(InvError::InvalidArgument {
                    name: "row.type",
                    details: format!("column {} type mismatch for '{}'", idx, col.name),
                });
            }
        }
    }

    Ok(out)
}

/// Decode bytes into a row according to the schema.
pub fn decode_row(schema: &Schema, bytes: &[u8]) -> InvResult<Row> {
    if bytes.len() < ROW_MAGIC.len() {
        return Err(InvError::Corruption {
            context: "row.magic",
            details: "input too short".to_string(),
        });
    }
    if &bytes[0..4] != ROW_MAGIC {
        return Err(InvError::Corruption {
            context: "row.magic",
            details: "magic mismatch".to_string(),
        });
    }
    let mut pos = 4;
    let col_count = encoding::read_var_u64(bytes, &mut pos)? as usize;
    if col_count != schema.len() {
        return Err(InvError::Corruption {
            context: "row.column_count",
            details: format!("expected {} got {}", schema.len(), col_count),
        });
    }

    let mut row = Vec::with_capacity(col_count);
    for col in &schema.columns {
        if pos >= bytes.len() {
            return Err(InvError::Corruption {
                context: "row.tag",
                details: "unexpected eof reading tag".to_string(),
            });
        }
        let tag = bytes[pos];
        pos += 1;
        let value = match tag {
            0x00 => {
                if !col.nullable {
                    return Err(InvError::InvalidArgument {
                        name: "row.null",
                        details: format!("column '{}' is not nullable", col.name),
                    });
                }
                Value::Null
            }
            0x01 => {
                let v = encoding::read_u32_le(bytes, &mut pos)?;
                Value::U32(v)
            }
            0x02 => {
                let v = encoding::read_u64_le(bytes, &mut pos)?;
                Value::U64(v)
            }
            0x03 => {
                let v = encoding::read_u64_le(bytes, &mut pos)?;
                Value::I64(i64::from_le_bytes(v.to_le_bytes()))
            }
            0x04 => {
                if pos >= bytes.len() {
                    return Err(InvError::Corruption {
                        context: "row.bool",
                        details: "missing bool payload".to_string(),
                    });
                }
                let b = bytes[pos];
                pos += 1;
                match b {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    _ => {
                        return Err(InvError::Corruption {
                            context: "row.bool",
                            details: format!("invalid bool byte {}", b),
                        })
                    }
                }
            }
            0x05 => {
                let data = encoding::read_bytes(bytes, &mut pos, MAX_VAR_LEN)?;
                Value::Bytes(data)
            }
            0x06 => {
                let s = encoding::read_string(bytes, &mut pos, MAX_VAR_LEN)?;
                Value::String(s)
            }
            _ => {
                return Err(InvError::Corruption {
                    context: "row.tag",
                    details: format!("unknown tag {}", tag),
                })
            }
        };

        // Schema type validation during decode.
        match (&col.ty, &value) {
            (ColType::U32, Value::U32(_))
            | (ColType::U64, Value::U64(_))
            | (ColType::I64, Value::I64(_))
            | (ColType::Bool, Value::Bool(_))
            | (ColType::Bytes, Value::Bytes(_))
            | (ColType::String, Value::String(_))
            | (_, Value::Null) => {}
            _ => {
                return Err(InvError::Corruption {
                    context: "row.type",
                    details: format!("decoded value does not match schema for '{}'", col.name),
                });
            }
        }

        row.push(value);
    }

    if pos != bytes.len() {
        return Err(InvError::Corruption {
            context: "row.trailing",
            details: "extra trailing bytes".to_string(),
        });
    }

    Ok(row)
}
