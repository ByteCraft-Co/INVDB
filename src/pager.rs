//! Simple pager that caches fixed-size pages and handles header validation.

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::path::Path;

use crate::btree::node::{encode_into_page, InternalNode, LeafNode, Node};

use crate::config::{
    CATALOG_PAGE_ID, FILE_FORMAT_VERSION, FILE_MAGIC, HEADER_PAGE_ID, META_PAGE_KIND, PAGE_SIZE,
    ROOT_PAGE_ID, ROW_PAGE_KIND,
};
use crate::error::{InvError, InvResult};
use crate::file::DbFile;
use crate::page::Page;
use crate::types::{DbVersion, PageId};

/// Pager with in-memory cache and dirty tracking.
#[derive(Debug)]
pub struct Pager {
    file: DbFile,
    cache: HashMap<PageId, Page>,
    dirty: HashSet<PageId>,
    root_page_id: PageId,
    page_count: u32,
    version: DbVersion,
}

impl Pager {
    /// Create a new database file with initialized header and root pages.
    pub fn create(path: &Path) -> InvResult<Self> {
        let mut file = DbFile::create_new(path)?;

        let mut header_buf = [0u8; PAGE_SIZE];
        encode_header_page(
            &mut header_buf,
            FILE_FORMAT_VERSION,
            ROOT_PAGE_ID,
            3, // header + root + catalog
        )?;
        file.write_page(HEADER_PAGE_ID, &header_buf)?;

        let mut root_page = Page::new_zeroed(ROOT_PAGE_ID);
        root_page.init_header(2)?;
        initialize_empty_leaf_payload(root_page.as_bytes_mut());
        let root_arr: &[u8; PAGE_SIZE] = root_page
            .as_bytes()
            .try_into()
            .expect("page buffer length must equal PAGE_SIZE");
        file.write_page(ROOT_PAGE_ID, root_arr)?;

        // Catalog page
        let mut cat_page = Page::new_zeroed(CATALOG_PAGE_ID);
        cat_page.init_header(META_PAGE_KIND)?;
        initialize_empty_catalog_payload(cat_page.as_bytes_mut());
        let cat_arr: &[u8; PAGE_SIZE] = cat_page
            .as_bytes()
            .try_into()
            .expect("page buffer length must equal PAGE_SIZE");
        file.write_page(CATALOG_PAGE_ID, cat_arr)?;

        Ok(Self {
            file,
            cache: HashMap::new(),
            dirty: HashSet::new(),
            root_page_id: ROOT_PAGE_ID,
            page_count: 3,
            version: DbVersion(FILE_FORMAT_VERSION),
        })
    }

    /// Open an existing database file, validating the header.
    pub fn open(path: &Path) -> InvResult<Self> {
        let mut file = DbFile::open_existing(path)?;

        let mut header_buf = [0u8; PAGE_SIZE];
        file.read_page(HEADER_PAGE_ID, &mut header_buf)?;
        let (version, root_page_id, page_count) = decode_and_validate_header_page(&header_buf)?;

        let actual_count = file.page_count()?;
        if actual_count != page_count {
            return Err(InvError::Corruption {
                context: "header.page_count",
                details: format!(
                    "header page_count {} != file page_count {}",
                    page_count, actual_count
                ),
            });
        }

        if page_count < 3 {
            return Err(InvError::Corruption {
                context: "catalog.missing",
                details: "catalog page missing".to_string(),
            });
        }

        Ok(Self {
            file,
            cache: HashMap::new(),
            dirty: HashSet::new(),
            root_page_id,
            page_count,
            version,
        })
    }

    /// Fetch a page by id, validating the header for non-header pages.
    pub fn get_page(&mut self, id: PageId) -> InvResult<&Page> {
        if id.0 >= self.page_count {
            return Err(InvError::InvalidArgument {
                name: "page_id",
                details: format!("{} out of bounds (page_count={})", id.0, self.page_count),
            });
        }

        if !self.cache.contains_key(&id) {
            let mut page = Page::new_zeroed(id);
            let buf: &mut [u8; PAGE_SIZE] = page
                .as_bytes_mut()
                .try_into()
                .expect("page buffer length must equal PAGE_SIZE");
            self.file.read_page(id, buf)?;

            if id != HEADER_PAGE_ID {
                page.validate_header()?;
            }

            self.cache.insert(id, page);
        }

        // SAFETY: entry now exists.
        Ok(self.cache.get(&id).expect("page must exist in cache"))
    }

    /// Fetch a mutable page, marking it dirty.
    pub fn get_page_mut(&mut self, id: PageId) -> InvResult<&mut Page> {
        // Ensure cached and validated.
        if !self.cache.contains_key(&id) {
            self.get_page(id)?;
        }
        self.dirty.insert(id);
        Ok(self.cache.get_mut(&id).expect("page must exist in cache"))
    }

    /// Flush all dirty pages and header metadata to disk.
    pub fn flush(&mut self) -> InvResult<()> {
        // Always write header to ensure counts are persisted.
        self.rewrite_header()?;

        let mut dirty_ids: Vec<PageId> = self.dirty.iter().copied().collect();
        dirty_ids.sort();
        for id in dirty_ids {
            if let Some(page) = self.cache.get(&id) {
                let data: &[u8; PAGE_SIZE] = page
                    .as_bytes()
                    .try_into()
                    .expect("page buffer length must equal PAGE_SIZE");
                self.file.write_page(id, data)?;
            }
        }
        self.dirty.clear();
        Ok(())
    }

    /// Return the root page identifier.
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Return the file format version.
    pub fn version(&self) -> DbVersion {
        self.version
    }

    /// Return the number of pages currently in the file.
    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Return the database path.
    pub fn path(&self) -> &Path {
        self.file.path()
    }

    /// Allocate a new btree page by appending to the file.
    pub fn allocate_btree_page(&mut self) -> InvResult<PageId> {
        if self.page_count == u32::MAX {
            return Err(InvError::Overflow {
                context: "pager.allocate.page_count",
            });
        }
        let new_id = PageId(self.page_count);
        let mut page = Page::new_zeroed(new_id);
        page.init_header(2)?;
        initialize_empty_leaf_payload(page.as_bytes_mut());
        let data: &[u8; PAGE_SIZE] = page
            .as_bytes()
            .try_into()
            .expect("page buffer length must equal PAGE_SIZE");
        self.file.write_page(new_id, data)?;
        self.page_count += 1;
        self.rewrite_header()?;
        Ok(new_id)
    }

    /// Allocate a new row page by appending to the file.
    pub fn allocate_row_page(&mut self) -> InvResult<PageId> {
        if self.page_count == u32::MAX {
            return Err(InvError::Overflow {
                context: "pager.allocate.page_count",
            });
        }
        let new_id = PageId(self.page_count);
        let mut page = Page::new_zeroed(new_id);
        page.init_header(ROW_PAGE_KIND)?;
        initialize_empty_row_page_payload(page.as_bytes_mut());
        let data: &[u8; PAGE_SIZE] = page
            .as_bytes()
            .try_into()
            .expect("page buffer length must equal PAGE_SIZE");
        self.file.write_page(new_id, data)?;
        self.page_count += 1;
        self.rewrite_header()?;
        Ok(new_id)
    }

    /// Update root page id and persist header.
    pub fn set_root_page_id(&mut self, new_root: PageId) -> InvResult<()> {
        if new_root.0 == 0 || new_root.0 >= self.page_count {
            return Err(InvError::Corruption {
                context: "header.root_page_id",
                details: format!(
                    "root {} invalid for page_count {}",
                    new_root.0, self.page_count
                ),
            });
        }
        self.root_page_id = new_root;
        self.rewrite_header()
    }

    /// Read catalog from disk.
    pub fn read_catalog(&mut self) -> InvResult<crate::catalog::Catalog> {
        let page = self.get_page(CATALOG_PAGE_ID)?;
        let buf = page.as_bytes();
        if buf.get(0) != Some(&META_PAGE_KIND) {
            return Err(InvError::Corruption {
                context: "catalog.page_kind",
                details: format!("expected {} got {}", META_PAGE_KIND, buf.get(0).copied().unwrap_or(255)),
            });
        }
        // validate header invariants
        page.validate_header()?;
        let payload = &buf[16..];
        crate::catalog::decode_catalog(payload)
    }

    /// Write catalog to disk (marks page dirty; flush persists).
    pub fn write_catalog(&mut self, cat: &crate::catalog::Catalog) -> InvResult<()> {
        let encoded = crate::catalog::encode_catalog(cat)?;
        if encoded.len() > PAGE_SIZE - 16 {
            return Err(InvError::Unsupported {
                feature: "catalog.page_overflow",
            });
        }
        let page = self.get_page_mut(CATALOG_PAGE_ID)?;
        let buf = page.as_bytes_mut();
        if buf.get(0) != Some(&META_PAGE_KIND) {
            return Err(InvError::Corruption {
                context: "catalog.page_kind",
                details: "wrong page kind for catalog".to_string(),
            });
        }
        for b in &mut buf[16..] {
            *b = 0;
        }
        buf[16..16 + encoded.len()].copy_from_slice(&encoded);
        Ok(())
    }

    fn rewrite_header(&mut self) -> InvResult<()> {
        let mut header_buf = [0u8; PAGE_SIZE];
        encode_header_page(
            &mut header_buf,
            self.version.0,
            self.root_page_id,
            self.page_count,
        )?;
        self.file.write_page(HEADER_PAGE_ID, &header_buf)
    }

    pub(crate) fn encode_leaf_into_page(
        &mut self,
        page_id: PageId,
        node: &LeafNode,
    ) -> InvResult<()> {
        let page = self.get_page_mut(page_id)?;
        encode_into_page(&Node::Leaf(LeafNode {
            num_keys: node.num_keys,
            next_leaf: node.next_leaf,
            keys: node.keys.clone(),
            values: node.values.clone(),
        }), page)
    }

    pub(crate) fn encode_internal_into_page(
        &mut self,
        page_id: PageId,
        node: &InternalNode,
    ) -> InvResult<()> {
        let page = self.get_page_mut(page_id)?;
        encode_into_page(&Node::Internal(InternalNode {
            num_keys: node.num_keys,
            children: node.children.clone(),
            keys: node.keys.clone(),
        }), page)
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            #[cfg(debug_assertions)]
            eprintln!("pager flush on drop failed: {}", e);
        }
    }
}

fn encode_header_page(
    buf: &mut [u8; PAGE_SIZE],
    version: u16,
    root: PageId,
    page_count: u32,
) -> InvResult<()> {
    // zero-fill entire buffer first
    buf.fill(0);

    buf[0..8].copy_from_slice(&FILE_MAGIC);
    buf[8..10].copy_from_slice(&version.to_le_bytes());

    let ps: u16 = PAGE_SIZE
        .try_into()
        .map_err(|_| InvError::Overflow {
            context: "PAGE_SIZE exceeds u16::MAX",
        })?;
    buf[10..12].copy_from_slice(&ps.to_le_bytes());
    buf[12..16].copy_from_slice(&root.0.to_le_bytes());
    buf[16..20].copy_from_slice(&page_count.to_le_bytes());
    // reserved [20..24) stays zero; non-zero indicates forward-compat
    Ok(())
}

fn initialize_empty_leaf_payload(buf: &mut [u8]) {
    let base = 16;
    buf[base] = 1; // node_kind leaf
    buf[base + 1] = 0; // node_flags
    buf[base + 2..base + 4].copy_from_slice(&0u16.to_le_bytes()); // num_keys
    buf[base + 4..base + 8].copy_from_slice(&0u32.to_le_bytes()); // reserved
    buf[base + 8..base + 12].copy_from_slice(&0u32.to_le_bytes()); // next_leaf
    buf[base + 12..base + 16].copy_from_slice(&0u32.to_le_bytes()); // reserved2
}

fn initialize_empty_catalog_payload(buf: &mut [u8]) {
    let base = 16;
    buf[base..base + 4].copy_from_slice(b"CAT1");
    buf[base + 4..base + 6].copy_from_slice(&1u16.to_le_bytes());
    buf[base + 6..base + 8].copy_from_slice(&0u16.to_le_bytes()); // entry_count
    buf[base + 8..base + 12].copy_from_slice(&1u32.to_le_bytes()); // next_table_id
    buf[base + 12..base + 16].copy_from_slice(&0u32.to_le_bytes()); // reserved
}

fn initialize_empty_row_page_payload(buf: &mut [u8]) {
    let base = 16;
    buf[base..base + 4].copy_from_slice(b"ROWP");
    buf[base + 4..base + 6].copy_from_slice(&1u16.to_le_bytes());
    buf[base + 6..base + 8].copy_from_slice(&32u16.to_le_bytes()); // free_offset absolute
    buf[base + 8..base + 12].copy_from_slice(&0u32.to_le_bytes()); // reserved
    buf[base + 12..base + 16].copy_from_slice(&0u32.to_le_bytes()); // reserved2
}

fn decode_and_validate_header_page(buf: &[u8; PAGE_SIZE]) -> InvResult<(DbVersion, PageId, u32)> {
    let mut found_magic = [0u8; 8];
    found_magic.copy_from_slice(&buf[0..8]);
    if found_magic != FILE_MAGIC {
        return Err(InvError::InvalidMagic {
            expected: FILE_MAGIC,
            found: found_magic,
        });
    }

    let version = u16::from_le_bytes([buf[8], buf[9]]);
    crate::config::validate_version(version)?;

    let page_size = u16::from_le_bytes([buf[10], buf[11]]);
    if page_size as usize != PAGE_SIZE {
        return Err(InvError::Corruption {
            context: "header.page_size",
            details: format!("expected {} got {}", PAGE_SIZE, page_size),
        });
    }

    let root_page_id_raw = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let page_count = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);

    let reserved = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]);
    if reserved != 0 {
        return Err(InvError::Unsupported {
            feature: "header.reserved_nonzero",
        });
    }

    if page_count < 2 {
        return Err(InvError::Corruption {
            context: "header.page_count",
            details: format!("expected >=2 got {}", page_count),
        });
    }

    if root_page_id_raw == 0 || root_page_id_raw >= page_count {
        return Err(InvError::Corruption {
            context: "header.root_page_id",
            details: format!(
                "root_page_id {} invalid for page_count {}",
                root_page_id_raw, page_count
            ),
        });
    }

    Ok((DbVersion(version), PageId(root_page_id_raw), page_count))
}
