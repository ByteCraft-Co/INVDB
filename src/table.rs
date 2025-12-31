//! Table-level operations using catalog, row store, and global btree.

use crate::btree;
use crate::catalog::{Catalog, TableDef};
use crate::error::{InvError, InvResult};
use crate::pager::Pager;
use crate::row::{decode_row, encode_row, Row};
use crate::rowstore::{RowPtr, RowStore};

/// Mix table_id and pk into a composite u32 key.
pub fn composite_key(table_id: u32, pk: u32) -> u32 {
    let mut x = table_id ^ 0x9E3779B9;
    x = x.wrapping_mul(0x85EBCA6B);
    x ^= pk.wrapping_add(0xC2B2AE35);
    x = x.wrapping_mul(0x27D4EB2F);
    x ^ (x >> 16)
}

fn find_table_mut<'a>(cat: &'a mut Catalog, name: &str) -> InvResult<&'a mut TableDef> {
    cat.tables
        .iter_mut()
        .find(|t| t.name == name)
        .ok_or(InvError::InvalidArgument {
            name: "table",
            details: "not found".to_string(),
        })
}

fn find_table<'a>(cat: &'a Catalog, name: &str) -> InvResult<&'a TableDef> {
    cat.tables
        .iter()
        .find(|t| t.name == name)
        .ok_or(InvError::InvalidArgument {
            name: "table",
            details: "not found".to_string(),
        })
}

/// Insert a row and return its primary key.
pub fn insert_row(
    pager: &mut Pager,
    catalog: &mut Catalog,
    table_name: &str,
    row: &Row,
) -> InvResult<u32> {
    let table = find_table_mut(catalog, table_name)?;

    let pk = table
        .next_pk
        .checked_add(0)
        .ok_or(InvError::Overflow {
            context: "table.next_pk",
        })?;
    table.next_pk = table
        .next_pk
        .checked_add(1)
        .ok_or(InvError::Overflow {
            context: "table.next_pk",
        })?;

    let encoded_row = encode_row(&table.schema, row)?;
    let mut stored = Vec::with_capacity(4 + encoded_row.len());
    stored.extend_from_slice(&pk.to_le_bytes());
    stored.extend_from_slice(&encoded_row);

    let (ptr, new_last_page) = RowStore::append_row(pager, table.last_row_page, &stored)?;
    table.last_row_page = new_last_page;

    let composite = composite_key(table.id.0, pk);
    let packed = ptr.pack();
    let root = pager.root_page_id();
    let new_root = btree::insert::insert_u64(pager, root, composite, packed)?;
    if new_root != root {
        pager.set_root_page_id(new_root)?;
    }

    Ok(pk)
}

/// Fetch a row by primary key.
pub fn get_row_by_pk(
    pager: &mut Pager,
    catalog: &Catalog,
    table_name: &str,
    pk: u32,
) -> InvResult<Option<Row>> {
    let table = find_table(catalog, table_name)?;
    let composite = composite_key(table.id.0, pk);
    let root = pager.root_page_id();
    let ptr_val = btree::search::search_u64(pager, root, composite)?;
    let Some(raw_ptr) = ptr_val else { return Ok(None); };
    let ptr = RowPtr::unpack(raw_ptr);
    ptr.validate()?;

    let stored = RowStore::read_row(pager, ptr)?;
    if stored.len() < 4 {
        return Err(InvError::Corruption {
            context: "table.pk_mismatch",
            details: "stored row too small".to_string(),
        });
    }
    let stored_pk = u32::from_le_bytes([stored[0], stored[1], stored[2], stored[3]]);
    if stored_pk != pk {
        return Err(InvError::Corruption {
            context: "table.pk_mismatch",
            details: format!("expected {} got {}", pk, stored_pk),
        });
    }
    let row_bytes = &stored[4..];
    let row = decode_row(&table.schema, row_bytes)?;
    Ok(Some(row))
}

/// Naive full scan by iterating pk range.
pub fn scan_table(
    pager: &mut Pager,
    catalog: &Catalog,
    table_name: &str,
) -> InvResult<Vec<(u32, Row)>> {
    let table = find_table(catalog, table_name)?;
    let mut rows = Vec::new();
    for pk in 1..table.next_pk {
        if let Some(row) = get_row_by_pk(pager, catalog, table_name, pk)? {
            rows.push((pk, row));
        }
    }
    Ok(rows)
}

#[cfg(test)]
pub(crate) fn composite_for_tests(table_id: u32, pk: u32) -> u32 {
    composite_key(table_id, pk)
}
