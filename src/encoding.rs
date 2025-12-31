//! Deterministic binary encoding helpers (varints, primitives, bytes, strings).

use crate::error::{InvError, InvResult};

/// Write an unsigned LEB128-style varint.
pub fn write_var_u64(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
}

/// Read an unsigned LEB128-style varint.
pub fn read_var_u64(input: &[u8], pos: &mut usize) -> InvResult<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;
    while *pos < input.len() {
        let b = input[*pos];
        *pos += 1;
        bytes_read += 1;
        if bytes_read > 10 {
            return Err(InvError::Corruption {
                context: "encoding.varint.too_long",
                details: "varint exceeded 10 bytes".to_string(),
            });
        }
        let value = (b & 0x7F) as u64;
        result |= value << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(InvError::Corruption {
        context: "encoding.varint.eof",
        details: "unexpected end of input while reading varint".to_string(),
    })
}

/// Write a u32 little-endian.
pub fn write_u32_le(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// Write a u64 little-endian.
pub fn write_u64_le(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// Read u32 little-endian.
pub fn read_u32_le(input: &[u8], pos: &mut usize) -> InvResult<u32> {
    if *pos + 4 > input.len() {
        return Err(InvError::Corruption {
            context: "encoding.fixed.eof",
            details: "not enough bytes for u32".to_string(),
        });
    }
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&input[*pos..*pos + 4]);
    *pos += 4;
    Ok(u32::from_le_bytes(buf))
}

/// Read u64 little-endian.
pub fn read_u64_le(input: &[u8], pos: &mut usize) -> InvResult<u64> {
    if *pos + 8 > input.len() {
        return Err(InvError::Corruption {
            context: "encoding.fixed.eof",
            details: "not enough bytes for u64".to_string(),
        });
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&input[*pos..*pos + 8]);
    *pos += 8;
    Ok(u64::from_le_bytes(buf))
}

/// Write a length-prefixed byte slice.
pub fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    write_var_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// Read a length-prefixed byte slice with a maximum length guard.
pub fn read_bytes(input: &[u8], pos: &mut usize, max_len: usize) -> InvResult<Vec<u8>> {
    let len = read_var_u64(input, pos)? as usize;
    if len > max_len {
        return Err(InvError::Corruption {
            context: "encoding.bytes.too_large",
            details: format!("len {} exceeds max {}", len, max_len),
        });
    }
    if *pos + len > input.len() {
        return Err(InvError::Corruption {
            context: "encoding.bytes.eof",
            details: "not enough bytes for payload".to_string(),
        });
    }
    let slice = &input[*pos..*pos + len];
    *pos += len;
    Ok(slice.to_vec())
}

/// Write a UTF-8 string with length prefix.
pub fn write_string(out: &mut Vec<u8>, s: &str) {
    write_bytes(out, s.as_bytes());
}

/// Read a UTF-8 string with length prefix and max bound.
pub fn read_string(input: &[u8], pos: &mut usize, max_len: usize) -> InvResult<String> {
    let bytes = read_bytes(input, pos, max_len)?;
    String::from_utf8(bytes).map_err(|e| InvError::Corruption {
        context: "encoding.string.utf8",
        details: e.to_string(),
    })
}
