/*
* Copyright (C) 2021 Quickwit Inc.
*
* Quickwit is offered under the AGPL v3.0 and as commercial software.
* For commercial licensing, contact us at hello@quickwit.io.
*
* AGPL:
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::{
    io::{self, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;

use crate::{PutPayload, Storage, StorageErrorKind, StorageResult};
use anyhow::anyhow;

use super::{Cache, CacheState, Capacity, CACHE_STATE_FILE_NAME};

/// A struct used as the caching layer for any [`Storage`].
///
/// It has an instance of [`LocalFileStorage`] as local storage and
/// an [`LruCache`] for managing cache eviction.
pub struct LocalStorageCache {
    pub capacity: Capacity,
    pub local_storage: Arc<dyn Storage>,
    pub lru_cache: LruCache<PathBuf, usize>,
    pub num_bytes: usize,
    pub item_count: usize,
}

impl LocalStorageCache {
    /// Create new instance of [`LocalStorageCache`]
    pub fn new(
        local_storage: Arc<dyn Storage>,
        max_num_bytes: usize,
        max_item_count: usize,
    ) -> Self {
        Self {
            capacity: Capacity {
                max_num_bytes,
                max_item_count,
            },
            local_storage,
            lru_cache: LruCache::unbounded(),
            num_bytes: 0,
            item_count: 0,
        }
    }

    /// Tell if an item will exceed the capacity once inserted.
    fn exceeds_capacity(&self, num_bytes: usize, item_count: usize) -> bool {
        self.capacity.max_num_bytes < num_bytes || self.capacity.max_item_count < item_count
    }
}

#[async_trait]
impl Cache for LocalStorageCache {
    /// Return the cached view of the requested range if available.
    async fn get(
        &mut self,
        path: &Path,
        bytes_range: Range<usize>,
    ) -> StorageResult<Option<Bytes>> {
        if self.lru_cache.get(&path.to_path_buf()).is_none() {
            return Ok(None);
        }

        self.local_storage
            .get_slice(path, bytes_range)
            .await
            .map(Some)
    }

    /// Attempt to put the given payload in the cache.
    async fn put(&mut self, path: &Path, payload: PutPayload) -> StorageResult<()> {
        let payload_length = payload.len().await? as usize;
        if self.exceeds_capacity(payload_length, 0) {
            return Err(StorageErrorKind::InternalError
                .with_error(anyhow!("The payload cannot fit in the cache.")));
        }
        if let Some(item_num_bytes) = self.lru_cache.pop(&path.to_path_buf()) {
            self.num_bytes -= item_num_bytes;
            self.item_count -= 1;
        }
        while self.exceeds_capacity(self.num_bytes + payload_length, self.item_count + 1) {
            if let Some((_, item_num_bytes)) = self.lru_cache.pop_lru() {
                self.num_bytes -= item_num_bytes;
                self.item_count -= 1;
            }
        }

        self.local_storage.put(path, payload).await?;

        self.num_bytes += payload_length;
        self.item_count += 1;
        self.lru_cache.put(path.to_path_buf(), payload_length);
        Ok(())
    }

    /// Attempt to copy the entry from cache if available.
    async fn copy_to_file(&mut self, path: &Path, output_path: &Path) -> StorageResult<bool> {
        if self.lru_cache.get(&path.to_path_buf()).is_none() {
            return Ok(false);
        }
        self.local_storage.copy_to_file(path, output_path).await?;
        Ok(true)
    }

    /// Attempt to delete the entry from cache if available.
    async fn delete(&mut self, path: &Path) -> StorageResult<bool> {
        if self.lru_cache.pop(&path.to_path_buf()).is_some() {
            self.local_storage.delete(path).await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Return a copy of the items in the cache.
    fn get_items(&self) -> Vec<(PathBuf, usize)> {
        let mut cache_items = vec![];
        for (path, size) in self.lru_cache.iter() {
            cache_items.push((path.clone(), *size));
        }
        cache_items
    }

    fn get_capacity(&self) -> Capacity {
        self.capacity
    }

    async fn save_state(&self, parent_uri: String) -> StorageResult<()> {
        let items = self.get_items();
        let num_bytes = items.iter().map(|(_, size)| *size).sum();
        let item_count = items.len();

        let cache_state = CacheState {
            remote_storage_uri: parent_uri,
            local_storage_uri: self.local_storage.uri(),
            capacity: self.get_capacity(),
            items,
            num_bytes,
            item_count,
        };

        let root_path = self.local_storage.root().ok_or_else(|| {
            StorageErrorKind::InternalError
                .with_error(anyhow!("The local storage need to have valid root path."))
        })?;

        let file_path = root_path.join(CACHE_STATE_FILE_NAME);
        let content: Vec<u8> = serde_json::to_vec(&cache_state)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;
        atomic_write(&file_path, &content)?;

        Ok(())
    }
}

/// Writes a file in an atomic manner.
//  Copied from tantivy
fn atomic_write(path: &Path, content: &[u8]) -> io::Result<()> {
    // We create the temporary file in the same directory as the target file.
    // Indeed the canonical temp directory and the target file might sit in different
    // filesystem, in which case the atomic write may actually not work.
    let parent_path = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path {:?} does not have parent directory.",
        )
    })?;
    let mut tempfile = tempfile::Builder::new().tempfile_in(&parent_path)?;
    tempfile.write_all(content)?;
    tempfile.flush()?;
    tempfile.into_temp_path().persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use tempfile::tempdir;

    use crate::RamStorage;

    use super::*;

    #[tokio::test]
    async fn test_cannot_fit_item_in_cache() -> anyhow::Result<()> {
        let ram_storage = Arc::new(RamStorage::default());
        let mut cache = LocalStorageCache::new(ram_storage, 5, 5);

        let payload = PutPayload::InMemory(Bytes::from(b"abc".to_vec()));
        cache.put(Path::new("3"), payload).await?;
        let payload = PutPayload::InMemory(Bytes::from(b"abcdef".to_vec()));
        cache.put(Path::new("6"), payload).await.unwrap_err();

        // first entry should still be around
        assert_eq!(cache.get(Path::new("3"), 0..3).await?.unwrap(), &b"abc"[..]);
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_edge_condition() -> anyhow::Result<()> {
        let ram_storage = Arc::new(RamStorage::default());
        let mut cache = LocalStorageCache::new(ram_storage, 5, 5);

        {
            let payload = PutPayload::InMemory(Bytes::from(b"abc".to_vec()));
            cache.put(Path::new("3"), payload).await?;
            assert_eq!(cache.get(Path::new("3"), 0..3).await?.unwrap(), &b"abc"[..]);
        }
        {
            let payload = PutPayload::InMemory(Bytes::from(b"de".to_vec()));
            cache.put(Path::new("2"), payload).await?;
            // our first entry should still be here.
            assert_eq!(cache.get(Path::new("3"), 0..3).await?.unwrap(), &b"abc"[..]);
            assert_eq!(cache.get(Path::new("2"), 0..2).await?.unwrap(), &b"de"[..]);
        }
        {
            let payload = PutPayload::InMemory(Bytes::from(b"fghij".to_vec()));
            cache.put(Path::new("5"), payload).await?;
            assert_eq!(
                cache.get(Path::new("5"), 0..5).await?.unwrap(),
                &b"fghij"[..]
            );
            // our two first entries should have be removed from the cache
            assert!(cache.get(Path::new("2"), 0..2).await?.is_none());
            assert!(cache.get(Path::new("3"), 0..3).await?.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_item_from_cache() -> anyhow::Result<()> {
        let ram_storage = Arc::new(RamStorage::default());
        let mut cache = LocalStorageCache::new(ram_storage, 5, 5);
        cache
            .put(
                Path::new("1"),
                PutPayload::InMemory(Bytes::from(b"a".to_vec())),
            )
            .await?;
        cache
            .put(
                Path::new("2"),
                PutPayload::InMemory(Bytes::from(b"bc".to_vec())),
            )
            .await?;
        cache
            .put(
                Path::new("3"),
                PutPayload::InMemory(Bytes::from(b"def".to_vec())),
            )
            .await?;

        assert!(!cache.delete(Path::new("6")).await?);

        assert!(cache.delete(Path::new("3")).await?);

        assert!(cache.get(Path::new("3"), 0..3).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_item_from_cache() -> anyhow::Result<()> {
        let ram_storage = Arc::new(RamStorage::default());
        let mut cache = LocalStorageCache::new(ram_storage, 5, 5);
        cache
            .put(
                Path::new("3"),
                PutPayload::InMemory(Bytes::from(b"abc".to_vec())),
            )
            .await?;

        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("3");

        assert!(
            !cache
                .copy_to_file(Path::new("12"), output_path.as_path())
                .await?
        );

        assert!(
            cache
                .copy_to_file(Path::new("3"), output_path.as_path())
                .await?
        );

        let metadata = tokio::fs::metadata(output_path.as_path()).await?;
        assert_eq!(metadata.len(), 3);
        Ok(())
    }
}
