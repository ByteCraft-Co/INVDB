//! Low-level file primitives for page-aligned IO.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::config::PAGE_SIZE;
use crate::error::{InvError, InvResult};
use crate::types::PageId;

/// Wrapper around the database file handle.
#[derive(Debug)]
pub struct DbFile {
    file: File,
    path: PathBuf,
}

impl DbFile {
    /// Create a new database file, truncating any existing file.
    pub fn create_new(path: &Path) -> InvResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| InvError::io("create_new", e))?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    /// Open an existing database file for read/write access.
    pub fn open_existing(path: &Path) -> InvResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| InvError::io("open_existing", e))?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    /// Return the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read a full page into the provided buffer.
    pub fn read_page(&mut self, id: PageId, out: &mut [u8; PAGE_SIZE]) -> InvResult<()> {
        let offset = (id.0 as u64)
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(InvError::Overflow {
                context: "page offset overflow",
            })?;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| InvError::io("seek_read", e))?;
        match self.file.read_exact(out) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Err(InvError::Corruption {
                context: "file.short_read",
                details: "file shorter than expected for page".to_string(),
            }),
            Err(e) => Err(InvError::io("read_page", e)),
        }
    }

    /// Write a full page from the provided buffer.
    pub fn write_page(&mut self, id: PageId, data: &[u8; PAGE_SIZE]) -> InvResult<()> {
        let offset = (id.0 as u64)
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(InvError::Overflow {
                context: "page offset overflow",
            })?;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| InvError::io("seek_write", e))?;
        self.file
            .write_all(data)
            .map_err(|e| InvError::io("write_page", e))
    }

    /// Return the current file length in bytes.
    pub fn file_len(&mut self) -> InvResult<u64> {
        self.file
            .metadata()
            .map(|m| m.len())
            .map_err(|e| InvError::io("file_len", e))
    }

    /// Return the number of pages in the file, ensuring alignment.
    pub fn page_count(&mut self) -> InvResult<u32> {
        let len = self.file_len()?;
        if len % PAGE_SIZE as u64 != 0 {
            return Err(InvError::Corruption {
                context: "file.len_alignment",
                details: format!("len={} not aligned to PAGE_SIZE", len),
            });
        }
        let pages = len / (PAGE_SIZE as u64);
        if pages > u32::MAX as u64 {
            return Err(InvError::Overflow {
                context: "page count exceeds u32::MAX",
            });
        }
        Ok(pages as u32)
    }
}
