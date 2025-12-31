//! Row storage primitives for appending and reading variable-length rows.

use crate::config::{PAGE_SIZE, ROW_PAGE_KIND};
use crate::error::{InvError, InvResult};
use crate::pager::Pager;
use crate::types::PageId;

/// Pointer to a stored row (page, offset, length).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowPtr {
    pub page_id: u32,
    pub offset: u16,
    pub len: u16,
}

impl RowPtr {
    /// Pack into a u64 for btree storage.
    pub fn pack(self) -> u64 {
        ((self.page_id as u64) << 32) | ((self.offset as u64) << 16) | (self.len as u64)
    }

    /// Unpack from a u64.
    pub fn unpack(v: u64) -> Self {
        RowPtr {
            page_id: (v >> 32) as u32,
            offset: ((v >> 16) & 0xFFFF) as u16,
            len: (v & 0xFFFF) as u16,
        }
    }

    /// Validate pointer fields against invariants.
    pub fn validate(self) -> InvResult<()> {
        if self.page_id == 0 {
            return Err(InvError::Corruption {
                context: "rowptr.invalid",
                details: "page_id is 0".to_string(),
            });
        }
        if self.offset < 32 {
            return Err(InvError::Corruption {
                context: "rowptr.invalid",
                details: format!("offset {} too small", self.offset),
            });
        }
        if self.len == 0 {
            return Err(InvError::Corruption {
                context: "rowptr.invalid",
                details: "len is 0".to_string(),
            });
        }
        let end = self.offset as u32 + self.len as u32;
        if end > PAGE_SIZE as u32 {
            return Err(InvError::Corruption {
                context: "rowptr.invalid",
                details: format!("end {} exceeds page size", end),
            });
        }
        Ok(())
    }
}

/// Row storage operations.
pub struct RowStore;

impl RowStore {
    /// Append a row and return its pointer and updated last_row_page value.
    pub fn append_row(
        pager: &mut Pager,
        table_last_row_page: u32,
        row_bytes: &[u8],
    ) -> InvResult<(RowPtr, u32)> {
        if row_bytes.len() > 3500 {
            return Err(InvError::Unsupported {
                feature: "row.too_large",
            });
        }

        let mut target_page_id = if table_last_row_page == 0 {
            pager.allocate_row_page()?.0
        } else {
            table_last_row_page
        };

        // Try appending to current page; if not enough space, allocate new.
        {
            let free_offset = Self::read_free_offset(pager, PageId(target_page_id))?;
            let needed = 2 + row_bytes.len();
            if (free_offset as usize + needed) > PAGE_SIZE {
                target_page_id = pager.allocate_row_page()?.0;
            }
        }

        let page_id = PageId(target_page_id);
        let free_offset = Self::read_free_offset(pager, page_id)?;
        let needed = 2 + row_bytes.len();
        if (free_offset as usize + needed) > PAGE_SIZE {
            return Err(InvError::Corruption {
                context: "rowpage.free_offset",
                details: "insufficient space after allocation".to_string(),
            });
        }

        let page = pager.get_page_mut(page_id)?;
        let buf = page.as_bytes_mut();
        // Write length
        let len_u16: u16 = row_bytes
            .len()
            .try_into()
            .map_err(|_| InvError::Unsupported {
                feature: "row.too_large",
            })?;
        buf[free_offset as usize..free_offset as usize + 2]
            .copy_from_slice(&len_u16.to_le_bytes());
        // Write row bytes
        let row_start = free_offset as usize + 2;
        buf[row_start..row_start + row_bytes.len()].copy_from_slice(row_bytes);

        let new_free = free_offset as usize + needed;
        Self::write_free_offset(page, new_free as u16)?;

        let ptr = RowPtr {
            page_id: page_id.0,
            offset: (free_offset + 2) as u16,
            len: len_u16,
        };
        Ok((ptr, page_id.0))
    }

    /// Read row bytes from a pointer.
    pub fn read_row(pager: &mut Pager, ptr: RowPtr) -> InvResult<Vec<u8>> {
        ptr.validate()?;
        let page = pager.get_page(PageId(ptr.page_id))?;
        let buf = page.as_bytes();
        if buf.get(0) != Some(&ROW_PAGE_KIND) {
            return Err(InvError::Corruption {
                context: "rowpage.kind",
                details: format!("expected {} got {}", ROW_PAGE_KIND, buf.get(0).copied().unwrap_or(255)),
            });
        }
        page.validate_header()?;
        validate_row_page_header(buf)?;

        let len_offset = (ptr.offset as usize).checked_sub(2).ok_or(InvError::Corruption {
            context: "rowptr.invalid",
            details: "offset underflow".to_string(),
        })?;
        if len_offset + 2 > buf.len() {
            return Err(InvError::Corruption {
                context: "rowpage.len_mismatch",
                details: "length field out of bounds".to_string(),
            });
        }
        let stored_len = u16::from_le_bytes([buf[len_offset], buf[len_offset + 1]]);
        if stored_len != ptr.len {
            return Err(InvError::Corruption {
                context: "rowpage.len_mismatch",
                details: format!("stored {} != ptr {}", stored_len, ptr.len),
            });
        }
        let start = ptr.offset as usize;
        let end = start + ptr.len as usize;
        if end > buf.len() {
            return Err(InvError::Corruption {
                context: "rowptr.invalid",
                details: "row extends beyond page".to_string(),
            });
        }
        Ok(buf[start..end].to_vec())
    }

    fn read_free_offset(pager: &mut Pager, page_id: PageId) -> InvResult<u16> {
        let page = pager.get_page(page_id)?;
        let buf = page.as_bytes();
        if buf.get(0) != Some(&ROW_PAGE_KIND) {
            return Err(InvError::Corruption {
                context: "rowpage.kind",
                details: format!("expected {} got {}", ROW_PAGE_KIND, buf.get(0).copied().unwrap_or(255)),
            });
        }
        page.validate_header()?;
        validate_row_page_header(buf)?;
        let free = u16::from_le_bytes([buf[22], buf[23]]);
        if free < 32 || free as usize > PAGE_SIZE {
            return Err(InvError::Corruption {
                context: "rowpage.free_offset",
                details: format!("invalid free_offset {}", free),
            });
        }
        Ok(free)
    }

    fn write_free_offset(page: &mut crate::page::Page, free: u16) -> InvResult<()> {
        if free as usize > PAGE_SIZE {
            return Err(InvError::Corruption {
                context: "rowpage.free_offset",
                details: "free offset beyond page".to_string(),
            });
        }
        let buf = page.as_bytes_mut();
        buf[22..24].copy_from_slice(&free.to_le_bytes());
        Ok(())
    }
}

pub(crate) fn validate_row_page_header(buf: &[u8]) -> InvResult<()> {
    let base = 16;
    if &buf[base..base + 4] != b"ROWP" {
        return Err(InvError::Corruption {
            context: "rowpage.magic",
            details: "invalid row page magic".to_string(),
        });
    }
    let version = u16::from_le_bytes([buf[base + 4], buf[base + 5]]);
    if version != 1 {
        return Err(InvError::Unsupported {
            feature: "rowpage.version",
        });
    }
    let reserved = u32::from_le_bytes([
        buf[base + 8],
        buf[base + 9],
        buf[base + 10],
        buf[base + 11],
    ]);
    if reserved != 0 {
        return Err(InvError::Unsupported {
            feature: "rowpage.reserved",
        });
    }
    let reserved2 = u32::from_le_bytes([
        buf[base + 12],
        buf[base + 13],
        buf[base + 14],
        buf[base + 15],
    ]);
    if reserved2 != 0 {
        return Err(InvError::Unsupported {
            feature: "rowpage.reserved2",
        });
    }
    let free_offset = u16::from_le_bytes([buf[base + 6], buf[base + 7]]);
    if free_offset < 32 || free_offset as usize > PAGE_SIZE {
        return Err(InvError::Corruption {
            context: "rowpage.free_offset",
            details: format!("invalid free_offset {}", free_offset),
        });
    }
    Ok(())
}
