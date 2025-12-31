pub mod config;
pub mod error;
pub mod types;
pub mod file;
pub mod page;
pub mod pager;
pub mod btree;
pub mod encoding;
pub mod schema;
pub mod row;
pub mod catalog;
pub mod rowstore;
pub mod table;

pub use error::{InvError, InvResult};
pub use types::{DbVersion, Lsn, PageId, TxId};
pub use schema::{Schema, Column, ColType};
pub use row::{Row, Value, encode_row, decode_row};
pub use catalog::{TableDef, TableId};

use std::path::Path;
use std::collections::HashSet;

use crate::pager::Pager;
use crate::btree::node::Node;

/// High-level database handle.
///
/// The handle encapsulates the pager and exposes high-level entry points.
#[derive(Debug)]
pub struct Db {
    pager: Pager,
}

impl Db {
    /// Create a new database file at the given path.
    ///
    /// Stable API: part of the supported surface.
    /// # Errors
    /// - [`InvError::InvalidArgument`] if the path is empty.
    /// - [`InvError::Unsupported`] if a WAL path is provided.
    pub fn create(path: impl AsRef<Path>) -> InvResult<Self> {
        let path_buf = path.as_ref().to_path_buf();
        validate_path(&path_buf)?;
        let pager = Pager::create(&path_buf)?;
        Ok(Self { pager })
    }

    /// Open an existing database file.
    ///
    /// Stable API: part of the supported surface.
    /// # Errors
    /// - [`InvError::InvalidArgument`] if the path is empty.
    /// - [`InvError::Unsupported`] if a WAL path is provided.
    pub fn open(path: impl AsRef<Path>) -> InvResult<Self> {
        let path_buf = path.as_ref().to_path_buf();
        validate_path(&path_buf)?;
        let mut pager = Pager::open(&path_buf)?;
        validate_database(&mut pager)?;
        Ok(Self { pager })
    }

    /// Return the stored file format version.
    pub fn version(&self) -> DbVersion {
        self.pager.version()
    }

    /// Return the database path.
    pub fn path(&self) -> &Path {
        self.pager.path()
    }

    /// Flush cached pages to disk.
    ///
    /// Stable API: part of the supported surface.
    pub fn flush(&mut self) -> InvResult<()> {
        self.pager.flush()
    }

    /// Read-only lookup of a u32 key returning an associated u64 value if present.
    ///
    /// Stable API: part of the supported surface.
    pub fn get_u64(&mut self, key: u32) -> InvResult<Option<u64>> {
        let root = self.pager.root_page_id();
        crate::btree::search::search_u64(&mut self.pager, root, key)
    }

    /// Insert or overwrite a u32->u64 mapping.
    ///
    /// Stable API: part of the supported surface.
    pub fn put_u64(&mut self, key: u32, value: u64) -> InvResult<()> {
        let root = self.pager.root_page_id();
        let new_root = crate::btree::insert::insert_u64(&mut self.pager, root, key, value)?;
        if new_root != root {
            self.pager.set_root_page_id(new_root)?;
        }
        Ok(())
    }

    /// Create a new table and persist catalog.
    ///
    /// Stable API: part of the supported surface.
    pub fn create_table(&mut self, name: &str, schema: &Schema) -> InvResult<TableId> {
        let mut cat = self.pager.read_catalog()?;
        let id = cat.create_table(name, schema)?;
        self.pager.write_catalog(&cat)?;
        Ok(id)
    }

    /// Fetch a table definition by name.
    ///
    /// Stable API: part of the supported surface.
    pub fn get_table(&mut self, name: &str) -> InvResult<Option<TableDef>> {
        let cat = self.pager.read_catalog()?;
        Ok(cat.get_by_name(name).cloned())
    }

    /// List all table definitions.
    ///
    /// Stable API: part of the supported surface.
    pub fn list_tables(&mut self) -> InvResult<Vec<TableDef>> {
        let cat = self.pager.read_catalog()?;
        Ok(cat.list())
    }

    /// Insert a row into a table, returning the allocated primary key.
    ///
    /// Stable API: part of the supported surface.
    pub fn insert_row(&mut self, table_name: &str, row: &Row) -> InvResult<u32> {
        let mut cat = self.pager.read_catalog()?;
        let pk = crate::table::insert_row(&mut self.pager, &mut cat, table_name, row)?;
        self.pager.write_catalog(&cat)?;
        Ok(pk)
    }

    /// Fetch a row by primary key.
    ///
    /// Stable API: part of the supported surface.
    pub fn get_row_by_pk(&mut self, table_name: &str, pk: u32) -> InvResult<Option<Row>> {
        let cat = self.pager.read_catalog()?;
        crate::table::get_row_by_pk(&mut self.pager, &cat, table_name, pk)
    }

    /// Scan rows in primary key order (naive implementation).
    ///
    /// Stable API: part of the supported surface.
    pub fn scan_table(&mut self, table_name: &str) -> InvResult<Vec<(u32, Row)>> {
        let cat = self.pager.read_catalog()?;
        crate::table::scan_table(&mut self.pager, &cat, table_name)
    }
}

/// Validate caller-provided path arguments for Db operations.
fn validate_path(path: &Path) -> InvResult<()> {
    if path.as_os_str().is_empty() {
        return Err(InvError::InvalidArgument {
            name: "path",
            details: "path must not be empty".to_string(),
        });
    }

    if path.extension().map_or(false, |ext| ext == "wal") {
        return Err(InvError::Unsupported { feature: "wal" });
    }

    Ok(())
}

fn validate_database(pager: &mut Pager) -> InvResult<()> {
    let page_count = pager.page_count();
    if page_count < 3 {
        return Err(InvError::Corruption {
            context: "catalog.missing",
            details: format!("page_count {} too small", page_count),
        });
    }
    let root = pager.root_page_id();
    if root.0 == 0 || root.0 >= page_count {
        return Err(InvError::Corruption {
            context: "header.root_page_id",
            details: format!("root {} invalid for page_count {}", root.0, page_count),
        });
    }

    // Root btree validation
    {
        let root_page = pager.get_page(root)?;
        let buf = root_page.as_bytes();
        if buf.get(0) != Some(&2) {
            return Err(InvError::Corruption {
                context: "btree.page_kind",
                details: format!("expected 2 got {}", buf.get(0).copied().unwrap_or(255)),
            });
        }
        root_page.validate_header()?;
        Node::decode(root_page, page_count)?;
    }

    let cat = pager.read_catalog()?;
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    for table in &cat.tables {
        if table.id.0 == 0 {
            return Err(InvError::Corruption {
                context: "catalog.table_id",
                details: "table id is 0".to_string(),
            });
        }
        if !ids.insert(table.id.0) || !names.insert(table.name.clone()) {
            return Err(InvError::Corruption {
                context: "catalog.duplicate",
                details: "duplicate table id or name".to_string(),
            });
        }
        if table.next_pk < 1 {
            return Err(InvError::Corruption {
                context: "catalog.next_pk",
                details: format!("invalid next_pk {}", table.next_pk),
            });
        }
        if table.last_row_page != 0 && table.last_row_page >= page_count {
            return Err(InvError::Corruption {
                context: "catalog.last_row_page",
                details: format!(
                    "last_row_page {} >= page_count {}",
                    table.last_row_page, page_count
                ),
            });
        }
        if table.schema.is_empty() {
            return Err(InvError::Corruption {
                context: "catalog.schema",
                details: "schema empty".to_string(),
            });
        }
    }

    // Row page reachability (best-effort)
    for table in &cat.tables {
        if table.last_row_page != 0 {
            let page = pager.get_page(PageId(table.last_row_page))?;
            let buf = page.as_bytes();
            if buf.get(0) != Some(&config::ROW_PAGE_KIND) {
                return Err(InvError::Corruption {
                    context: "rowpage.kind",
                    details: format!(
                        "expected {} got {}",
                        config::ROW_PAGE_KIND,
                        buf.get(0).copied().unwrap_or(255)
                    ),
                });
            }
            page.validate_header()?;
            crate::rowstore::validate_row_page_header(buf)?;
        }
    }

    validate_leaf_chain(pager, root, page_count)?;

    Ok(())
}

fn validate_leaf_chain(pager: &mut Pager, root: PageId, page_count: u32) -> InvResult<()> {
    let start_leaf = find_leftmost_leaf(pager, root, page_count)?;
    let mut current = start_leaf;
    let mut steps = 0usize;
    let mut visited = HashSet::new();
    while current.0 != 0 {
        if steps > 10_000 {
            return Err(InvError::Corruption {
                context: "btree.leaf_cycle",
                details: "leaf traversal exceeded limit".to_string(),
            });
        }
        if !visited.insert(current.0) {
            return Err(InvError::Corruption {
                context: "btree.leaf_cycle",
                details: format!("cycle detected at {}", current.0),
            });
        }
        let page = pager.get_page(current)?;
        let node = Node::decode(page, page_count)?;
        let leaf = match node {
            Node::Leaf(l) => l,
            _ => {
                return Err(InvError::Corruption {
                    context: "btree.leaf_cycle",
                    details: "expected leaf during traversal".to_string(),
                })
            }
        };
        let next = leaf.next_leaf.0;
        if next == 0 {
            break;
        }
        if next >= page_count {
            return Err(InvError::Corruption {
                context: "btree.leaf_cycle",
                details: format!("invalid next_leaf {} from {}", next, current.0),
            });
        }
        current = PageId(next);
        steps += 1;
    }
    Ok(())
}

fn find_leftmost_leaf(pager: &mut Pager, mut current: PageId, page_count: u32) -> InvResult<PageId> {
    let mut depth = 0usize;
    loop {
        if depth > 64 {
            return Err(InvError::Corruption {
                context: "btree.depth",
                details: "exceeded max depth while finding leaf".to_string(),
            });
        }
        let page = pager.get_page(current)?;
        let node = Node::decode(page, page_count)?;
        match node {
            Node::Leaf(_) => return Ok(current),
            Node::Internal(internal) => {
                if internal.children.is_empty() {
                    return Err(InvError::Corruption {
                        context: "btree.internal.child",
                        details: "internal node has no children".to_string(),
                    });
                }
                current = internal.children[0];
            }
        }
        depth += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::node::{self, Node};
    use crate::btree::node::max_leaf_keys;
    use crate::config::{MAX_SUPPORTED_VERSION, MIN_SUPPORTED_VERSION, PAGE_SIZE, ROOT_PAGE_ID};
    use crate::rowstore::RowPtr;
    use crate::table::composite_for_tests;
    use crate::types::checked_page_index;
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn validate_version_accepts_current() {
        assert!(config::validate_version(config::FILE_FORMAT_VERSION).is_ok());
    }

    #[test]
    fn validate_version_rejects_out_of_range() {
        assert!(config::validate_version(0).is_err());
        assert!(config::validate_version(MAX_SUPPORTED_VERSION.saturating_add(1)).is_err());
        if MIN_SUPPORTED_VERSION > 0 {
            assert!(config::validate_version(MIN_SUPPORTED_VERSION - 1).is_err());
        }
    }

    #[test]
    fn checked_page_index_overflow() {
        let result = checked_page_index(u64::MAX);
        assert!(matches!(
            result,
            Err(InvError::Overflow {
                context: "page index exceeds u32::MAX"
            })
        ));
    }

    #[test]
    fn create_rejects_empty_path() {
        let err = Db::create("").unwrap_err();
        assert!(matches!(
            err,
            InvError::InvalidArgument { name: "path", .. }
        ));
    }

    #[test]
    fn open_rejects_wal_path() {
        let err = Db::open("test.wal").unwrap_err();
        assert!(matches!(
            err,
            InvError::Unsupported {
                feature: "wal", ..
            }
        ));
    }

    fn unique_temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("invdb_{}_{}", name, nanos))
    }

    #[test]
    fn create_creates_file_and_valid_header() {
        let path = unique_temp_path("create_header");
        {
            let mut db = Db::create(&path).unwrap();
            db.flush().unwrap();
        }
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), (3 * PAGE_SIZE) as u64);

        let db_open = Db::open(&path);
        assert!(db_open.is_ok());
    }

    #[test]
    fn open_rejects_bad_magic() {
        let path = unique_temp_path("bad_magic");
        {
            let mut db = Db::create(&path).unwrap();
            db.flush().unwrap();
        }

        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(err, InvError::InvalidMagic { .. }));
    }

    #[test]
    fn open_rejects_bad_page_size() {
        let path = unique_temp_path("bad_page_size");
        {
            let mut db = Db::create(&path).unwrap();
            db.flush().unwrap();
        }

        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.seek(SeekFrom::Start(10)).unwrap();
            f.write_all(&[0x01, 0x00]).unwrap(); // set page_size to 1
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "header.page_size",
                ..
            }
        ));
    }

    #[test]
    fn pager_get_page_validates_page_header() {
        let path = unique_temp_path("page_header");
        {
            let mut db = Db::create(&path).unwrap();
            {
                let pager = db.pager_mut_for_tests();
                let root_page = pager.get_page(ROOT_PAGE_ID).unwrap();
                assert_eq!(root_page.as_bytes()[0], 2);

                let root_page_mut = pager.get_page_mut(ROOT_PAGE_ID).unwrap();
                root_page_mut.as_bytes_mut()[1] = 1; // set flags non-zero
            }
            db.flush().unwrap();
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Unsupported {
                feature: "page.flags"
            }
        ));
    }

    #[test]
    fn search_empty_leaf_returns_none() {
        let path = unique_temp_path("search_empty");
        let mut db = Db::create(&path).unwrap();
        let result = db.get_u64(10).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn search_leaf_finds_value() {
        let path = unique_temp_path("search_leaf");
        {
            let mut db = Db::create(&path).unwrap();
            {
                let pager = db.pager_mut_for_tests();
                let root_id = pager.root_page_id();
                let page = pager.get_page_mut(root_id).unwrap();
                let buf = page.as_bytes_mut();
                let base = 16;
                buf[base] = 1; // leaf
                buf[base + 1] = 0;
                buf[base + 2..base + 4].copy_from_slice(&(3u16).to_le_bytes());
                buf[base + 4..base + 8].copy_from_slice(&0u32.to_le_bytes());
                buf[base + 8..base + 12].copy_from_slice(&0u32.to_le_bytes());
                buf[base + 12..base + 16].copy_from_slice(&0u32.to_le_bytes());

                let keys = [10u32, 20, 30];
                let values = [1000u64, 2000, 3000];
                let keys_offset = base + 16;
                for (i, k) in keys.iter().enumerate() {
                    let offset = keys_offset + 4 * i;
                    buf[offset..offset + 4].copy_from_slice(&k.to_le_bytes());
                }
                let values_offset = keys_offset + 4 * keys.len();
                for (i, v) in values.iter().enumerate() {
                    let offset = values_offset + 8 * i;
                    buf[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
                }
            }
            db.flush().unwrap();
        }

        let mut db = Db::open(&path).unwrap();
        assert_eq!(db.get_u64(20).unwrap(), Some(2000));
        assert_eq!(db.get_u64(25).unwrap(), None);
        assert_eq!(db.get_u64(10).unwrap(), Some(1000));
    }

    #[test]
    fn search_internal_traversal_basic() {
        let path = unique_temp_path("search_internal");
        {
            let mut db = Db::create(&path).unwrap();
            {
                let pager = db.pager_mut_for_tests();
                let child_a = pager.allocate_btree_page().unwrap();
                let child_b = pager.allocate_btree_page().unwrap();

                // root internal node with key 50 separating child_a and child_b
                let root_id = pager.root_page_id();
                let root_page = pager.get_page_mut(root_id).unwrap();
                let buf = root_page.as_bytes_mut();
                let base = 16;
                buf[base] = 2; // internal
                buf[base + 1] = 0;
                buf[base + 2..base + 4].copy_from_slice(&(1u16).to_le_bytes());
                buf[base + 4..base + 8].copy_from_slice(&0u32.to_le_bytes());
                buf[base + 8..base + 12].copy_from_slice(&0u32.to_le_bytes()); // reserved2
                buf[base + 12..base + 16].copy_from_slice(&0u32.to_le_bytes()); // reserved3
                let children_offset = base + 16;
                buf[children_offset..children_offset + 4]
                    .copy_from_slice(&child_a.0.to_le_bytes());
                buf[children_offset + 4..children_offset + 8]
                    .copy_from_slice(&child_b.0.to_le_bytes());
                let keys_offset = children_offset + 8;
                buf[keys_offset..keys_offset + 4].copy_from_slice(&50u32.to_le_bytes());

                // child_a leaf with key 10 -> 111
                let child_a_page = pager.get_page_mut(child_a).unwrap();
                let buf_a = child_a_page.as_bytes_mut();
                buf_a[base] = 1;
                buf_a[base + 1] = 0;
                buf_a[base + 2..base + 4].copy_from_slice(&(1u16).to_le_bytes());
                buf_a[base + 4..base + 8].copy_from_slice(&0u32.to_le_bytes());
                buf_a[base + 8..base + 12].copy_from_slice(&0u32.to_le_bytes());
                buf_a[base + 12..base + 16].copy_from_slice(&0u32.to_le_bytes());
                let keys_offset_a = base + 16;
                buf_a[keys_offset_a..keys_offset_a + 4].copy_from_slice(&10u32.to_le_bytes());
                let values_offset_a = keys_offset_a + 4;
                buf_a[values_offset_a..values_offset_a + 8]
                    .copy_from_slice(&111u64.to_le_bytes());

                // child_b leaf with key 60 -> 222
                let child_b_page = pager.get_page_mut(child_b).unwrap();
                let buf_b = child_b_page.as_bytes_mut();
                buf_b[base] = 1;
                buf_b[base + 1] = 0;
                buf_b[base + 2..base + 4].copy_from_slice(&(1u16).to_le_bytes());
                buf_b[base + 4..base + 8].copy_from_slice(&0u32.to_le_bytes());
                buf_b[base + 8..base + 12].copy_from_slice(&0u32.to_le_bytes());
                buf_b[base + 12..base + 16].copy_from_slice(&0u32.to_le_bytes());
                let keys_offset_b = base + 16;
                buf_b[keys_offset_b..keys_offset_b + 4].copy_from_slice(&60u32.to_le_bytes());
                let values_offset_b = keys_offset_b + 4;
                buf_b[values_offset_b..values_offset_b + 8]
                    .copy_from_slice(&222u64.to_le_bytes());
            }
            db.flush().unwrap();
        }

        let mut db = Db::open(&path).unwrap();
        assert_eq!(db.get_u64(10).unwrap(), Some(111));
        assert_eq!(db.get_u64(60).unwrap(), Some(222));
        assert_eq!(db.get_u64(55).unwrap(), None);
    }

    #[test]
    fn node_validation_rejects_unsorted_keys() {
        let path = unique_temp_path("unsorted_keys");
        {
            let mut db = Db::create(&path).unwrap();
            {
                let pager = db.pager_mut_for_tests();
                let root_id = pager.root_page_id();
                let page = pager.get_page_mut(root_id).unwrap();
                let buf = page.as_bytes_mut();
                let base = 16;
                buf[base] = 1; // leaf
                buf[base + 1] = 0;
                buf[base + 2..base + 4].copy_from_slice(&(2u16).to_le_bytes());
                buf[base + 4..base + 8].copy_from_slice(&0u32.to_le_bytes());
                buf[base + 8..base + 12].copy_from_slice(&0u32.to_le_bytes());
                buf[base + 12..base + 16].copy_from_slice(&0u32.to_le_bytes());
                let keys_offset = base + 16;
                let keys = [20u32, 10u32];
                for (i, k) in keys.iter().enumerate() {
                    let offset = keys_offset + 4 * i;
                    buf[offset..offset + 4].copy_from_slice(&k.to_le_bytes());
                }
                let values_offset = keys_offset + 4 * keys.len();
                let values = [1u64, 2u64];
                for (i, v) in values.iter().enumerate() {
                    let offset = values_offset + 8 * i;
                    buf[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
                }
            }
            db.flush().unwrap();
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "btree.leaf.keys_order",
                ..
            }
        ));
    }

    #[test]
    fn put_then_get_single() {
        let path = unique_temp_path("put_single");
        {
            let mut db = Db::create(&path).unwrap();
            db.put_u64(10, 111).unwrap();
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        assert_eq!(db.get_u64(10).unwrap(), Some(111));
    }

    #[test]
    fn overwrite_value_same_key() {
        let path = unique_temp_path("overwrite_key");
        {
            let mut db = Db::create(&path).unwrap();
            db.put_u64(10, 111).unwrap();
            db.put_u64(10, 222).unwrap();
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        assert_eq!(db.get_u64(10).unwrap(), Some(222));
    }

    #[test]
    fn leaf_split_creates_multiple_pages() {
        let path = unique_temp_path("leaf_split");
        let max = max_leaf_keys();
        {
            let mut db = Db::create(&path).unwrap();
            for k in 1..=(max as u32 + 5) {
                db.put_u64(k, (k as u64) * 10).unwrap();
            }
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        assert!(db.pager_mut_for_tests().page_count() > 2);
        assert_eq!(db.get_u64(1).unwrap(), Some(10));
        assert_eq!(db.get_u64((max as u32) / 2).unwrap(), Some(((max as u32) / 2) as u64 * 10));
        assert_eq!(db.get_u64(max as u32 + 5).unwrap(), Some((max as u32 + 5) as u64 * 10));
    }

    #[test]
    fn root_split_creates_internal_root() {
        let path = unique_temp_path("root_split");
        let max = max_leaf_keys();
        {
            let mut db = Db::create(&path).unwrap();
            let upper = (max * 3) as u32;
            for k in 1..=upper {
                db.put_u64(k, (k as u64) * 2).unwrap();
            }
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        let pager = db.pager_mut_for_tests();
        let root = pager.root_page_id();
        let page_count = pager.page_count();
        let node = node::Node::decode(pager.get_page(root).unwrap(), page_count).unwrap();
        match node {
            Node::Internal(_) => {}
            _ => panic!("expected internal root after splits"),
        }
        assert_eq!(db.get_u64(1).unwrap(), Some(2));
        assert_eq!(db.get_u64((max * 2) as u32).unwrap(), Some((max * 2) as u64 * 2));
        assert_eq!(db.get_u64((max * 3) as u32).unwrap(), Some((max * 3) as u64 * 2));
    }

    #[test]
    fn persistence_of_root_page_id() {
        let path = unique_temp_path("root_persist");
        let max = max_leaf_keys();
        {
            let mut db = Db::create(&path).unwrap();
            for k in 1..=((max * 3) as u32) {
                db.put_u64(k, (k as u64) + 1).unwrap();
            }
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        assert_ne!(db.pager_mut_for_tests().root_page_id(), ROOT_PAGE_ID);
        assert_eq!(db.get_u64(5).unwrap(), Some(6));
        assert_eq!(db.get_u64((max * 3) as u32).unwrap(), Some((max * 3 + 1) as u64));
    }

    #[test]
    fn catalog_persists_table_definition() {
        let path = unique_temp_path("catalog_persist");
        let schema = Schema::new(vec![
            Column {
                name: "id".to_string(),
                ty: ColType::U32,
                nullable: false,
            },
            Column {
                name: "name".to_string(),
                ty: ColType::String,
                nullable: true,
            },
        ])
        .unwrap();
        {
            let mut db = Db::create(&path).unwrap();
            let id = db.create_table("users", &schema).unwrap();
            assert_eq!(id, TableId(1));
            db.flush().unwrap();
        }

        let mut db = Db::open(&path).unwrap();
        let tbl = db.get_table("users").unwrap().expect("table exists");
        assert_eq!(tbl.id, TableId(1));
        assert_eq!(tbl.schema, schema);
    }

    #[test]
    fn catalog_rejects_duplicate_table_name() {
        let path = unique_temp_path("catalog_dupe");
        let schema = Schema::new(vec![Column {
            name: "id".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        let mut db = Db::create(&path).unwrap();
        db.create_table("users", &schema).unwrap();
        let err = db.create_table("users", &schema).unwrap_err();
        assert!(matches!(
            err,
            InvError::InvalidArgument {
                name: "table.name",
                ..
            }
        ));
    }

    #[test]
    fn catalog_list_tables() {
        let path = unique_temp_path("catalog_list");
        let schema = Schema::new(vec![Column {
            name: "id".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        let mut db = Db::create(&path).unwrap();
        db.create_table("a", &schema).unwrap();
        db.create_table("b", &schema).unwrap();
        let list = db.list_tables().unwrap();
        let names: HashSet<_> = list.into_iter().map(|t| t.name).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains("a"));
        assert!(names.contains("b"));
    }

    #[test]
    fn open_rejects_corrupt_catalog_magic_on_read() {
        let path = unique_temp_path("catalog_corrupt_magic");
        {
            let mut db = Db::create(&path).unwrap();
            db.flush().unwrap();
        }

        // Corrupt catalog magic byte on disk at payload offset.
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let offset = (2 * PAGE_SIZE + 16) as u64;
            f.seek(SeekFrom::Start(offset)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            b[0] ^= 0xFF;
            f.seek(SeekFrom::Start(offset)).unwrap();
            f.write_all(&b).unwrap();
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "catalog.magic",
                ..
            }
        ));
    }

    #[test]
    fn create_requires_catalog_page_kind() {
        let path = unique_temp_path("catalog_kind");
        {
            let mut db = Db::create(&path).unwrap();
            db.flush().unwrap();
        }
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let offset = (2 * PAGE_SIZE) as u64; // start of catalog page
            f.seek(SeekFrom::Start(offset)).unwrap();
            // Set page_kind byte 0 to 2 (btree kind)
            f.write_all(&[2]).unwrap();
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "catalog.page_kind",
                ..
            }
        ));
    }

    #[test]
    fn insert_and_get_row_roundtrip() {
        let path = unique_temp_path("row_roundtrip");
        let schema = Schema::new(vec![
            Column {
                name: "age".to_string(),
                ty: ColType::U32,
                nullable: false,
            },
            Column {
                name: "name".to_string(),
                ty: ColType::String,
                nullable: true,
            },
        ])
        .unwrap();
        let pk;
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("users", &schema).unwrap();
            pk = db
                .insert_row("users", &vec![Value::U32(20), Value::String("kazuha".to_string())])
                .unwrap();
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        let row = db.get_row_by_pk("users", pk).unwrap().unwrap();
        assert_eq!(row, vec![Value::U32(20), Value::String("kazuha".to_string())]);
    }

    #[test]
    fn multiple_inserts_increment_pk() {
        let path = unique_temp_path("row_pk_inc");
        let schema = Schema::new(vec![Column {
            name: "v".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        let mut db = Db::create(&path).unwrap();
        db.create_table("t", &schema).unwrap();
        let pk1 = db.insert_row("t", &vec![Value::U32(1)]).unwrap();
        let pk2 = db.insert_row("t", &vec![Value::U32(2)]).unwrap();
        let pk3 = db.insert_row("t", &vec![Value::U32(3)]).unwrap();
        assert_eq!(pk1, 1);
        assert_eq!(pk2, 2);
        assert_eq!(pk3, 3);
        db.flush().unwrap();

        let mut db = Db::open(&path).unwrap();
        let rows = db.scan_table("t").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (1, vec![Value::U32(1)]));
        assert_eq!(rows[1], (2, vec![Value::U32(2)]));
        assert_eq!(rows[2], (3, vec![Value::U32(3)]));
    }

    #[test]
    fn persistence_of_catalog_pk_state_rows() {
        let path = unique_temp_path("row_pk_state");
        let schema = Schema::new(vec![Column {
            name: "v".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("t", &schema).unwrap();
            db.insert_row("t", &vec![Value::U32(1)]).unwrap();
            db.insert_row("t", &vec![Value::U32(2)]).unwrap();
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        let pk3 = db.insert_row("t", &vec![Value::U32(3)]).unwrap();
        assert_eq!(pk3, 3);
        let row = db.get_row_by_pk("t", 3).unwrap().unwrap();
        assert_eq!(row, vec![Value::U32(3)]);
    }

    #[test]
    fn corruption_detect_pk_mismatch() {
        let path = unique_temp_path("pk_mismatch");
        let schema = Schema::new(vec![Column {
            name: "v".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("t", &schema).unwrap();
            db.insert_row("t", &vec![Value::U32(10)]).unwrap();
            db.flush().unwrap();
        }

        // Corrupt pk prefix in stored row.
        {
            let mut db = Db::open(&path).unwrap();
            let cat = db.pager.read_catalog().unwrap();
            let table = cat.get_by_name("t").unwrap();
            let composite = composite_for_tests(table.id.0, 1);
            let root = db.pager.root_page_id();
            let packed = crate::btree::search::search_u64(&mut db.pager, root, composite)
                .unwrap()
                .unwrap();
            let ptr = RowPtr::unpack(packed);
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let byte_offset =
                (ptr.page_id as u64) * (PAGE_SIZE as u64) + (ptr.offset as u64);
            f.seek(SeekFrom::Start(byte_offset)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            b[0] ^= 0xFF;
            f.seek(SeekFrom::Start(byte_offset)).unwrap();
            f.write_all(&b).unwrap();
        }

        let mut db = Db::open(&path).unwrap();
        let err = db.get_row_by_pk("t", 1).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "table.pk_mismatch",
                ..
            }
        ));
    }

    #[test]
    fn rowpage_header_validation() {
        let path = unique_temp_path("rowpage_magic");
        let schema = Schema::new(vec![Column {
            name: "v".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        let row_ptr_page;
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("t", &schema).unwrap();
            db.insert_row("t", &vec![Value::U32(1)]).unwrap();

            let cat = db.pager.read_catalog().unwrap();
            let table = cat.get_by_name("t").unwrap();
            let composite = composite_for_tests(table.id.0, 1);
            let root = db.pager.root_page_id();
            let packed = crate::btree::search::search_u64(
                &mut db.pager,
                root,
                composite,
            )
            .unwrap()
            .unwrap();
            let ptr = RowPtr::unpack(packed);
            row_ptr_page = ptr.page_id;
            db.flush().unwrap();
        }

        // Corrupt row page magic
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let offset = (row_ptr_page as u64) * (PAGE_SIZE as u64) + 16;
            f.seek(SeekFrom::Start(offset)).unwrap();
            f.write_all(b"XOWP").unwrap();
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "rowpage.magic",
                ..
            }
        ));
    }

    fn pseudo_shuffle(n: u32) -> Vec<u32> {
        let mut v: Vec<u32> = (1..=n).collect();
        let mut seed: u64 = 0x1234_5678_9ABC_DEF0;
        for i in (1..v.len()).rev() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let j = (seed >> 33) as usize % (i + 1);
            v.swap(i, j);
        }
        v
    }

    #[test]
    fn btree_stress_insert_and_lookup() {
        let path = unique_temp_path("btree_stress");
        let keys = pseudo_shuffle(5_000);
        {
            let mut db = Db::create(&path).unwrap();
            for &k in &keys {
                db.put_u64(k, (k as u64) * 10).unwrap();
            }
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        for k in 1..=5_000u32 {
            assert_eq!(db.get_u64(k).unwrap(), Some((k as u64) * 10));
        }
    }

    #[test]
    fn table_stress_insert_and_scan() {
        let path = unique_temp_path("table_stress");
        let schema = Schema::new(vec![
            Column {
                name: "x".to_string(),
                ty: ColType::U32,
                nullable: false,
            },
            Column {
                name: "name".to_string(),
                ty: ColType::String,
                nullable: true,
            },
        ])
        .unwrap();
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("items", &schema).unwrap();
            for i in 0..1000u32 {
                db.insert_row(
                    "items",
                    &vec![Value::U32(i), Value::String(format!("item{}", i))],
                )
                .unwrap();
            }
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        let rows = db.scan_table("items").unwrap();
        assert_eq!(rows.len(), 1000);
        for (idx, (pk, row)) in rows.iter().enumerate() {
            assert_eq!(*pk, (idx as u32) + 1);
            assert_eq!(
                *row,
                vec![
                    Value::U32(idx as u32),
                    Value::String(format!("item{}", idx))
                ]
            );
        }
    }

    #[test]
    fn random_access_after_reopen() {
        let path = unique_temp_path("random_access");
        let schema = Schema::new(vec![Column {
            name: "v".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("t", &schema).unwrap();
            for i in 0..200u32 {
                db.insert_row("t", &vec![Value::U32(i)]).unwrap();
            }
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        let keys = pseudo_shuffle(200);
        for pk in keys.into_iter().take(50) {
            let row = db.get_row_by_pk("t", pk).unwrap().unwrap();
            assert_eq!(row, vec![Value::U32(pk - 1)]);
        }
    }

    #[test]
    fn corruption_does_not_panic() {
        let path = unique_temp_path("corruption_no_panic");
        let schema = Schema::new(vec![Column {
            name: "v".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        let leaf_page_id;
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("t", &schema).unwrap();
            for i in 0..10u32 {
                db.insert_row("t", &vec![Value::U32(i)]).unwrap();
            }
            let leaves = collect_leaf_chain(db.pager_mut_for_tests());
            leaf_page_id = leaves[0].0;
            db.flush().unwrap();
        }
        // Corrupt a byte in leaf payload.
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let leaf_payload_offset = (leaf_page_id as u64) * (PAGE_SIZE as u64) + 32;
            f.seek(SeekFrom::Start(leaf_payload_offset)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }

        let res = Db::open(&path);
        if let Ok(mut db) = res {
            let _ = db.get_row_by_pk("t", 1);
        }
    }

    #[test]
    fn btree_depth_growth_is_reasonable() {
        let path = unique_temp_path("btree_depth");
        {
            let mut db = Db::create(&path).unwrap();
            for k in 1..=5_000u32 {
                db.put_u64(k, k as u64).unwrap();
            }
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        let pager = db.pager_mut_for_tests();
        let root_id = pager.root_page_id();
        let page_count = pager.page_count();
        let root_node = node::Node::decode(pager.get_page(root_id).unwrap(), page_count).unwrap();
        match root_node {
            Node::Internal(ref internal) => {
                assert_eq!(internal.children.len(), (internal.num_keys as usize) + 1);
                for child in &internal.children {
                    assert!(child.0 > 0 && child.0 < page_count);
                }
            }
            _ => panic!("expected internal root for deep tree"),
        }
    }

    #[test]
    fn catalog_and_row_invariants_hold() {
        let path = unique_temp_path("catalog_invariants");
        let schema = Schema::new(vec![Column {
            name: "v".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("a", &schema).unwrap();
            db.create_table("b", &schema).unwrap();
            db.insert_row("a", &vec![Value::U32(1)]).unwrap();
            db.insert_row("a", &vec![Value::U32(2)]).unwrap();
            db.insert_row("b", &vec![Value::U32(3)]).unwrap();
            db.flush().unwrap();
        }
        let mut db = Db::open(&path).unwrap();
        let cat = db.pager.read_catalog().unwrap();
        for table in &cat.tables {
            let rows = db.scan_table(&table.name).unwrap();
            let max_pk = rows.iter().map(|(pk, _)| *pk).max().unwrap_or(0);
            assert!(table.next_pk > max_pk);
            assert_ne!(table.last_row_page, 0);
            let page = db
                .pager
                .get_page(PageId(table.last_row_page))
                .expect("row page readable");
            crate::rowstore::validate_row_page_header(page.as_bytes()).unwrap();
        }
    }

    #[test]
    fn open_detects_corrupt_root_page() {
        let path = unique_temp_path("corrupt_root_kind");
        {
            let mut db = Db::create(&path).unwrap();
            db.flush().unwrap();
        }
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let offset = PAGE_SIZE as u64; // root page start
            f.seek(SeekFrom::Start(offset)).unwrap();
            f.write_all(&[config::META_PAGE_KIND]).unwrap();
        }
        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "btree.page_kind",
                ..
            }
        ));
    }

    #[test]
    fn open_detects_catalog_duplicate_ids() {
        let path = unique_temp_path("catalog_dup_ids");
        {
            let mut db = Db::create(&path).unwrap();
            let schema = Schema::new(vec![Column {
                name: "id".to_string(),
                ty: ColType::U32,
                nullable: false,
            }])
            .unwrap();
            db.create_table("a", &schema).unwrap();
            db.create_table("b", &schema).unwrap();
            // Corrupt catalog to duplicate ids
            let mut cat = db.pager.read_catalog().unwrap();
            let first_id = cat.tables[0].id;
            cat.tables[1].id = first_id;
            db.pager.write_catalog(&cat).unwrap();
            db.flush().unwrap();
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "catalog.duplicate",
                ..
            }
        ));
    }

    #[test]
    fn open_detects_row_page_magic_corruption() {
        let path = unique_temp_path("row_magic_open");
        let schema = Schema::new(vec![Column {
            name: "v".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        let row_page_id;
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("t", &schema).unwrap();
            db.insert_row("t", &vec![Value::U32(7)]).unwrap();

            let cat = db.pager.read_catalog().unwrap();
            let table = cat.get_by_name("t").unwrap();
            let composite = composite_for_tests(table.id.0, 1);
            let root = db.pager.root_page_id();
            let packed =
                crate::btree::search::search_u64(&mut db.pager, root, composite)
                    .unwrap()
                    .unwrap();
            let ptr = RowPtr::unpack(packed);
            row_page_id = ptr.page_id;
            db.flush().unwrap();
        }

        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let offset = (row_page_id as u64) * (PAGE_SIZE as u64) + 16;
            f.seek(SeekFrom::Start(offset)).unwrap();
            f.write_all(b"BAD!").unwrap();
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "rowpage.magic",
                ..
            }
        ));
    }

    fn collect_leaf_chain(pager: &mut Pager) -> Vec<PageId> {
        let page_count = pager.page_count();
        let mut ids = Vec::new();
        let mut current = {
            let root = pager.root_page_id();
            // leftmost leaf
            let mut cur = root;
            loop {
                let node = node::Node::decode(
                    pager.get_page(cur).unwrap(),
                    page_count,
                )
                .unwrap();
                match node {
                    Node::Leaf(_) => break cur,
                    Node::Internal(int) => {
                        cur = int.children[0];
                    }
                }
            }
        };
        loop {
            ids.push(current);
            let node = node::Node::decode(
                pager.get_page(current).unwrap(),
                page_count,
            )
            .unwrap();
            match node {
                Node::Leaf(l) => {
                    if l.next_leaf.0 == 0 {
                        break;
                    }
                    current = PageId(l.next_leaf.0);
                }
                _ => break,
            }
        }
        ids
    }

    #[test]
    fn open_detects_leaf_cycle() {
        let path = unique_temp_path("leaf_cycle");
        {
            let mut db = Db::create(&path).unwrap();
            let schema = Schema::new(vec![Column {
                name: "v".to_string(),
                ty: ColType::U32,
                nullable: false,
            }])
            .unwrap();
            db.create_table("t", &schema).unwrap();
            let inserts = (max_leaf_keys() as u32) + 10;
            for i in 0..inserts {
                db.insert_row("t", &vec![Value::U32(i)]).unwrap();
            }
            let leaves = collect_leaf_chain(db.pager_mut_for_tests());
            assert!(leaves.len() >= 2, "expected multiple leaves");
            let first = leaves[0];
            let second = leaves[1];
            db.flush().unwrap();

            // Corrupt second leaf next_leaf to point back to first.
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let offset = (second.0 as u64) * (PAGE_SIZE as u64) + 16 + 8;
            f.seek(SeekFrom::Start(offset)).unwrap();
            f.write_all(&first.0.to_le_bytes()).unwrap();
        }

        let err = Db::open(&path).unwrap_err();
        assert!(matches!(
            err,
            InvError::Corruption {
                context: "btree.leaf_cycle",
                ..
            }
        ));
    }

    #[test]
    fn open_valid_database_passes() {
        let path = unique_temp_path("open_valid");
        let schema = Schema::new(vec![Column {
            name: "v".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        {
            let mut db = Db::create(&path).unwrap();
            db.create_table("t", &schema).unwrap();
            db.insert_row("t", &vec![Value::U32(42)]).unwrap();
            db.flush().unwrap();
        }

        let mut db = Db::open(&path).unwrap();
        let row = db.get_row_by_pk("t", 1).unwrap().unwrap();
        assert_eq!(row, vec![Value::U32(42)]);
    }

    #[test]
    fn schema_validation_rejects_duplicates() {
        let cols = vec![
            Column {
                name: "id".to_string(),
                ty: ColType::U32,
                nullable: false,
            },
            Column {
                name: "id".to_string(),
                ty: ColType::U64,
                nullable: false,
            },
        ];
        let err = Schema::new(cols).unwrap_err();
        assert!(matches!(err, InvError::InvalidArgument { name: "column.name", .. }));
    }

    #[test]
    fn row_roundtrip_basic() {
        let schema = Schema::new(vec![
            Column {
                name: "id".to_string(),
                ty: ColType::U32,
                nullable: false,
            },
            Column {
                name: "score".to_string(),
                ty: ColType::U64,
                nullable: false,
            },
            Column {
                name: "ok".to_string(),
                ty: ColType::Bool,
                nullable: false,
            },
        ])
        .unwrap();
        let row = vec![Value::U32(7), Value::U64(9001), Value::Bool(true)];
        let bytes = encode_row(&schema, &row).unwrap();
        let decoded = decode_row(&schema, &bytes).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn row_roundtrip_bytes_string() {
        let schema = Schema::new(vec![
            Column {
                name: "payload".to_string(),
                ty: ColType::Bytes,
                nullable: false,
            },
            Column {
                name: "name".to_string(),
                ty: ColType::String,
                nullable: true,
            },
        ])
        .unwrap();
        let row = vec![Value::Bytes(vec![1, 2, 3]), Value::String("abc".to_string())];
        let bytes = encode_row(&schema, &row).unwrap();
        let decoded = decode_row(&schema, &bytes).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn row_rejects_null_for_nonnullable() {
        let schema = Schema::new(vec![Column {
            name: "x".to_string(),
            ty: ColType::U64,
            nullable: false,
        }])
        .unwrap();
        let row = vec![Value::Null];
        let err = encode_row(&schema, &row).unwrap_err();
        assert!(matches!(err, InvError::InvalidArgument { name: "row.null", .. }));
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let schema = Schema::new(vec![Column {
            name: "x".to_string(),
            ty: ColType::U64,
            nullable: false,
        }])
        .unwrap();
        let row = vec![Value::U64(1)];
        let mut bytes = encode_row(&schema, &row).unwrap();
        bytes[0] ^= 0xFF;
        let err = decode_row(&schema, &bytes).unwrap_err();
        assert!(matches!(err, InvError::Corruption { context: "row.magic", .. }));
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let schema = Schema::new(vec![Column {
            name: "x".to_string(),
            ty: ColType::U32,
            nullable: false,
        }])
        .unwrap();
        let row = vec![Value::U32(5)];
        let mut bytes = encode_row(&schema, &row).unwrap();
        bytes.push(0xAA);
        let err = decode_row(&schema, &bytes).unwrap_err();
        assert!(matches!(err, InvError::Corruption { context: "row.trailing", .. }));
    }

    #[test]
    fn decode_rejects_bad_bool() {
        let schema = Schema::new(vec![Column {
            name: "b".to_string(),
            ty: ColType::Bool,
            nullable: false,
        }])
        .unwrap();
        // Manually craft bytes: magic + count + tag + invalid bool byte
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"ROW1");
        crate::encoding::write_var_u64(&mut bytes, 1);
        bytes.push(0x04);
        bytes.push(2);
        let err = decode_row(&schema, &bytes).unwrap_err();
        assert!(matches!(err, InvError::Corruption { context: "row.bool", .. }));
    }

    #[test]
    fn display_formats_without_panic() {
        let err = InvError::Overflow {
            context: "test overflow",
        };
        let _ = format!("{}", err);
    }
}

#[cfg(test)]
impl Db {
    fn pager_mut_for_tests(&mut self) -> &mut Pager {
        &mut self.pager
    }
}
