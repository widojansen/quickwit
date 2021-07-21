/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use async_trait::async_trait;
use bytes::Bytes;
use quickwit_storage::SliceCache;
use stable_deref_trait::StableDeref;
use std::fmt;
use std::io;
use std::ops::Deref;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, OwnedBytes, WatchHandle, WritePtr};
use tantivy::{AsyncIoResult, Directory, HasLen};

/// The caching directory is a simple cache that wraps another directory.
#[derive(Clone)]
pub struct CachingDirectory {
    underlying: Arc<dyn Directory>,
    // TODO fixme: that's a pretty ugly cache we have here.
    cache: Arc<SliceCache>,
}

impl CachingDirectory {
    /// Creates a new CachingDirectory.
    /// `capacity_in_bytes` acts as a memory budget for the directory.
    ///
    /// The implementation is voluntarily very naive as it was design solely to
    /// address Quickwit's requirements.
    /// Most notably, if two reads targetting the same read on the same path
    /// happen concurrently, the read will be executed twice.
    ///
    /// The overall number of bytes held in memory may exceed the capacity at one point
    /// if a read request is large than `capacity_in_bytes`.
    /// In that case, the read payload will not be saved in the cache.
    pub fn new_with_capacity_in_bytes(
        underlying: Arc<dyn Directory>,
        capacity_in_bytes: usize,
    ) -> CachingDirectory {
        CachingDirectory {
            underlying,
            cache: Arc::new(SliceCache::with_capacity_in_bytes(capacity_in_bytes)),
        }
    }

    /// Creates a new CachingDirectory.
    ///
    /// Warming: The resulting CacheDirectory will cache all information without ever
    /// removing any item from the cache.
    /// Prefer using `.new_with_capacity_in_bytes(...)` in most case.
    pub fn new_with_unlimited_capacity(underlying: Arc<dyn Directory>) -> CachingDirectory {
        CachingDirectory {
            underlying,
            cache: Arc::new(SliceCache::with_infinite_capacity()),
        }
    }
}

impl fmt::Debug for CachingDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CachingDirectory({:?})", self.underlying)
    }
}

struct CachingFileHandle {
    path: PathBuf,
    cache: Arc<SliceCache>,
    underlying_filehandle: Box<dyn FileHandle>,
}

impl fmt::Debug for CachingFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CachingFileHandle(path={:?}, underlying={:?})",
            &self.path,
            self.underlying_filehandle.as_ref()
        )
    }
}

pub struct BytesWrapper(pub Bytes);
unsafe impl StableDeref for BytesWrapper {}
impl Deref for BytesWrapper {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[async_trait]
impl FileHandle for CachingFileHandle {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.cache.get(&self.path, byte_range.clone()) {
            return Ok(OwnedBytes::new(BytesWrapper(bytes)));
        }
        let owned_bytes = self.underlying_filehandle.read_bytes(byte_range.clone())?;
        self.cache.put(
            self.path.to_path_buf(),
            byte_range,
            Bytes::from(owned_bytes.to_vec()),
        );
        Ok(owned_bytes)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> AsyncIoResult<OwnedBytes> {
        if let Some(owned_bytes) = self.cache.get(&self.path, byte_range.clone()) {
            return Ok(OwnedBytes::new(BytesWrapper(owned_bytes)));
        }
        let read_bytes = self
            .underlying_filehandle
            .read_bytes_async(byte_range.clone())
            .await?;
        self.cache.put(
            self.path.clone(),
            byte_range,
            Bytes::from(read_bytes.to_vec()),
        );
        Ok(read_bytes)
    }
}

impl HasLen for CachingFileHandle {
    fn len(&self) -> usize {
        self.underlying_filehandle.len()
    }
}

impl Directory for CachingDirectory {
    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        self.underlying.exists(path)
    }

    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Box<dyn FileHandle>, OpenReadError> {
        let underlying_filehandle = self.underlying.get_file_handle(path)?;
        let caching_file_handle = CachingFileHandle {
            path: path.to_path_buf(),
            cache: self.cache.clone(),
            underlying_filehandle,
        };
        Ok(Box::new(caching_file_handle))
    }

    fn delete(&self, _path: &Path) -> std::result::Result<(), DeleteError> {
        unimplemented!("read only");
    }

    fn open_write(&self, _path: &Path) -> std::result::Result<WritePtr, OpenWriteError> {
        unimplemented!("read only");
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        let file_handle = self.get_file_handle(path)?;
        let len = file_handle.len();
        let owned_bytes = file_handle
            .read_bytes(0..len)
            .map_err(|io_error| OpenReadError::wrap_io_error(io_error, path.to_path_buf()))?;
        Ok(owned_bytes.as_slice().to_vec())
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        unimplemented!("read only");
    }

    fn watch(
        &self,
        _watch_callback: tantivy::directory::WatchCallback,
    ) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

#[cfg(test)]
mod tests {

    use super::CachingDirectory;
    use crate::DebugProxyDirectory;
    use std::path::Path;
    use std::sync::Arc;
    use tantivy::directory::RamDirectory;
    use tantivy::Directory;

    #[test]
    fn test_caching_directory() -> tantivy::Result<()> {
        let ram_directory = RamDirectory::default();
        let test_path = Path::new("test");
        ram_directory.atomic_write(test_path, &b"test"[..])?;
        let debug_proxy_directory = Arc::new(DebugProxyDirectory::wrap(ram_directory));
        let caching_directory =
            CachingDirectory::new_with_capacity_in_bytes(debug_proxy_directory.clone(), 10_000);
        caching_directory.atomic_read(test_path)?;
        caching_directory.atomic_read(test_path)?;
        let records: Vec<crate::ReadOperation> =
            debug_proxy_directory.drain_read_operations().collect();
        assert_eq!(records.len(), 1);
        Ok(())
    }
}
