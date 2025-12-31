//! In-memory page buffer with fixed layout invariants.

use crate::config::PAGE_SIZE;
use crate::error::{InvError, InvResult};
use crate::types::PageId;

/// Page buffer storing exactly `PAGE_SIZE` bytes.
#[derive(Debug)]
pub struct Page {
    id: PageId,
    buf: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    /// Create a zeroed page with the given identifier.
    pub fn new_zeroed(id: PageId) -> Self {
        Self {
            id,
            buf: Box::new([0u8; PAGE_SIZE]),
        }
    }

    /// Return the page identifier.
    pub fn id(&self) -> PageId {
        self.id
    }

    /// Borrow the raw bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..]
    }

    /// Borrow the raw bytes mutably.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.buf[..]
    }

    /// Initialize the per-page header to a known kind with clean flags.
    pub fn init_header(&mut self, kind: u8) -> InvResult<()> {
        self.write_u8(0, kind);
        self.write_u8(1, 0);
        self.write_u16(2, 0);
        self.write_u32(4, 0);
        self.write_u32(8, self.id.0);
        self.write_u32(12, 0);
        Ok(())
    }

    /// Validate the per-page header invariants for non-header pages.
    pub fn validate_header(&self) -> InvResult<()> {
        let flags = self.read_u8(1);
        if flags != 0 {
            return Err(InvError::Unsupported {
                feature: "page.flags",
            });
        }

        let reserved = self.read_u16(2);
        if reserved != 0 {
            return Err(InvError::Corruption {
                context: "page.reserved",
                details: format!("expected 0 got {}", reserved),
            });
        }

        let crc32 = self.read_u32(4);
        if crc32 != 0 {
            return Err(InvError::Unsupported {
                feature: "page.crc32",
            });
        }

        let stored_page_id = self.read_u32(8);
        if stored_page_id != self.id.0 {
            return Err(InvError::Corruption {
                context: "page.page_id",
                details: format!("expected {} got {}", self.id.0, stored_page_id),
            });
        }

        let reserved2 = self.read_u32(12);
        if reserved2 != 0 {
            return Err(InvError::Corruption {
                context: "page.reserved2",
                details: format!("expected 0 got {}", reserved2),
            });
        }

        Ok(())
    }

    fn read_u8(&self, offset: usize) -> u8 {
        self.buf[offset]
    }

    fn write_u8(&mut self, offset: usize, val: u8) {
        self.buf[offset] = val;
    }

    fn read_u16(&self, offset: usize) -> u16 {
        let bytes = [self.buf[offset], self.buf[offset + 1]];
        u16::from_le_bytes(bytes)
    }

    fn write_u16(&mut self, offset: usize, val: u16) {
        let bytes = val.to_le_bytes();
        self.buf[offset] = bytes[0];
        self.buf[offset + 1] = bytes[1];
    }

    fn read_u32(&self, offset: usize) -> u32 {
        let bytes = [
            self.buf[offset],
            self.buf[offset + 1],
            self.buf[offset + 2],
            self.buf[offset + 3],
        ];
        u32::from_le_bytes(bytes)
    }

    fn write_u32(&mut self, offset: usize, val: u32) {
        let bytes = val.to_le_bytes();
        self.buf[offset] = bytes[0];
        self.buf[offset + 1] = bytes[1];
        self.buf[offset + 2] = bytes[2];
        self.buf[offset + 3] = bytes[3];
    }
}
