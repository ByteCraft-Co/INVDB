//! Shared strongly-typed identifiers used throughout the engine.

use crate::error::{InvError, InvResult};

/// Logical page identifier (INV-2, INV-8).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct PageId(pub u32);

impl PageId {
    /// Create a new page identifier from a u32.
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Access the raw numeric value.
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Returns true when this identifier points at the header page.
    pub const fn is_header(self) -> bool {
        self.0 == 0
    }
}

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PageId({})", self.0)
    }
}

/// File format version wrapper (INV-10).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct DbVersion(pub u16);

impl std::fmt::Display for DbVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DbVersion({})", self.0)
    }
}

/// Placeholder transaction identifier.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct TxId(pub u64);

impl std::fmt::Display for TxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TxId({})", self.0)
    }
}

/// Log sequence number placeholder for WAL integration.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Lsn(pub u64);

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lsn({})", self.0)
    }
}

/// Convert a `u64` into a `PageId`, reporting overflow explicitly (INV-9).
pub fn checked_page_index(i: u64) -> InvResult<PageId> {
    if i <= u64::from(u32::MAX) {
        Ok(PageId(i as u32))
    } else {
        Err(InvError::Overflow {
            context: "page index exceeds u32::MAX",
        })
    }
}
