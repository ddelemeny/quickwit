// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

mod local_storage_cache;
mod storage_with_local_cache;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::{ops::Range, path::PathBuf};

use crate::{PutPayload, StorageResult};

pub use storage_with_local_cache::StorageWithLocalStorageCache;

const FULL_SLICE: Range<usize> = 0..usize::MAX;
const CACHE_STATE_FILE_NAME: &str = "cache-sate.json";

/// Capacity encapsulates the maximum number of items a cache can hold.
/// We need to account for number of items as well as the size of each item.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct Capacity {
    /// Maximum of number of items.
    max_item_count: usize,
    /// Maximum number of bytes.
    max_num_bytes: usize,
}

/// CacheState is P.O.D.O for serializing/deserializing the cache state.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct CacheState {
    remote_storage_uri: String,
    local_storage_uri: String,
    capacity: Capacity,
    items: Vec<(PathBuf, usize)>,
    num_bytes: usize,
    item_count: usize,
}

/// The `Cache` trait is the abstraction used to describe the caching logic
/// used in front of a storage. See `FileStorageWithCache`.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Cache: Send + Sync + 'static {
    async fn get(&mut self, path: &Path, bytes_range: Range<usize>)
        -> StorageResult<Option<Bytes>>;

    async fn put(&mut self, path: &Path, payload: PutPayload) -> StorageResult<()>;

    async fn copy_to_file(&mut self, path: &Path, output_path: &Path) -> StorageResult<bool>;

    async fn delete(&mut self, path: &Path) -> StorageResult<bool>;

    fn get_items(&self) -> Vec<(PathBuf, usize)>;

    fn get_capacity(&self) -> Capacity;

    async fn save_state(&self, parent_uri: String) -> StorageResult<()>;
}
