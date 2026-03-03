// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod rocks_kv_engine;

pub use rocks_kv_engine::RocksKvEngine;

use orpc::CommonResult;
use std::sync::Arc;

pub type KvPair = (Vec<u8>, Vec<u8>);

/// KV store abstraction for a single namespace.
///
/// Each `KvStore` instance operates within an isolated namespace (e.g. a
/// RocksDB column family).  Domain stores such as `ConfigStore` and
/// `MountStore` receive an `Arc<dyn KvStore>` and never touch the underlying
/// engine directly.
pub trait KvStore: Send + Sync {
    fn get(&self, key: &[u8]) -> CommonResult<Option<Vec<u8>>>;

    fn put(&self, key: &[u8], value: &[u8]) -> CommonResult<()>;

    fn delete(&self, key: &[u8]) -> CommonResult<()>;

    fn scan_prefix(&self, prefix: &[u8]) -> CommonResult<Vec<KvPair>>;

    fn exists(&self, key: &[u8]) -> CommonResult<bool> {
        Ok(self.get(key)?.is_some())
    }
}

/// Engine-level abstraction that owns the physical storage.
///
/// Responsible for:
/// - Creating per-namespace `KvStore` instances
/// - Checkpoint / snapshot lifecycle (used by `PdAppStorage` for Raft)
pub trait KvEngine: Send + Sync + 'static {
    /// Open (or return) a `KvStore` bound to the given namespace.
    fn open_store(&self, namespace: &str) -> Arc<dyn KvStore>;

    /// Create a checkpoint of the entire engine state.  Returns the
    /// directory that contains the checkpoint files.
    fn create_checkpoint(&self, id: u64) -> CommonResult<String>;

    /// Restore the engine from a checkpoint directory.
    fn restore_from_checkpoint(&self, checkpoint_dir: &str) -> CommonResult<()>;
}

#[cfg(test)]
pub mod memory_kv_engine;
