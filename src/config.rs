//! Configuration constants for INVDB.
//! These constants define the stable on-disk format and global identifiers.

use crate::error::{InvError, InvResult};
use crate::types::PageId;

/// Logical page size in bytes for all database files (INV-1).
pub const PAGE_SIZE: usize = 4096;

/// File magic header used to identify INVDB files (INV-4).
pub const FILE_MAGIC: [u8; 8] = *b"INVDB\0\0\0";

/// Current on-disk file format version (INV-10).
pub const FILE_FORMAT_VERSION: u16 = 1;

/// Minimum supported file format version.
pub const MIN_SUPPORTED_VERSION: u16 = 1;

/// Maximum supported file format version.
pub const MAX_SUPPORTED_VERSION: u16 = 1;

/// Page identifier for the header page.
pub const HEADER_PAGE_ID: PageId = PageId(0);

/// Placeholder page identifier for the root btree node (allocated later).
pub const ROOT_PAGE_ID: PageId = PageId(1);
/// Fixed page id for the catalog metadata page.
pub const CATALOG_PAGE_ID: PageId = PageId(2);

/// Page kind for catalog/meta pages.
pub const META_PAGE_KIND: u8 = 3;

/// Page kind for row storage pages.
pub const ROW_PAGE_KIND: u8 = 4;

/// Validate a file format version against supported bounds.
///
/// Returns [`InvError::InvalidVersion`] if the version is outside the
/// supported inclusive range `[MIN_SUPPORTED_VERSION, MAX_SUPPORTED_VERSION]`.
pub fn validate_version(v: u16) -> InvResult<()> {
    if (MIN_SUPPORTED_VERSION..=MAX_SUPPORTED_VERSION).contains(&v) {
        Ok(())
    } else {
        Err(InvError::InvalidVersion {
            found: v,
            min: MIN_SUPPORTED_VERSION,
            max: MAX_SUPPORTED_VERSION,
        })
    }
}
