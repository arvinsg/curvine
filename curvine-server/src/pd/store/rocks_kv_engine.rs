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

use super::{KvEngine, KvPair, KvStore};
use curvine_common::rocksdb::DBEngine;
use orpc::CommonResult;
use std::sync::{Arc, RwLock};

/// RocksDB-backed `KvEngine`.
///
/// Holds a single `DBEngine` behind an `RwLock` and hands out
/// `RocksKvStore` instances that are each bound to one column family.
pub struct RocksKvEngine {
    db: Arc<RwLock<DBEngine>>,
}

impl RocksKvEngine {
    pub fn new(db: DBEngine) -> Self {
        Self {
            db: Arc::new(RwLock::new(db)),
        }
    }
}

impl KvEngine for RocksKvEngine {
    fn open_store(&self, namespace: &str) -> Arc<dyn KvStore> {
        Arc::new(RocksKvStore {
            db: self.db.clone(),
            cf: namespace.to_string(),
        })
    }

    fn create_checkpoint(&self, id: u64) -> CommonResult<String> {
        let db = self.db.read().unwrap();
        db.create_checkpoint(id)
    }

    fn restore_from_checkpoint(&self, checkpoint_dir: &str) -> CommonResult<()> {
        let mut db = self.db.write().unwrap();
        db.restore(checkpoint_dir)?;
        Ok(())
    }
}

/// Per-namespace `KvStore` backed by a RocksDB column family.
struct RocksKvStore {
    db: Arc<RwLock<DBEngine>>,
    cf: String,
}

impl KvStore for RocksKvStore {
    fn get(&self, key: &[u8]) -> CommonResult<Option<Vec<u8>>> {
        let db = self.db.read().unwrap();
        db.get_cf(&self.cf, key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> CommonResult<()> {
        let db = self.db.write().unwrap();
        db.put_cf(&self.cf, key, value)
    }

    fn delete(&self, key: &[u8]) -> CommonResult<()> {
        let db = self.db.write().unwrap();
        db.delete_cf(&self.cf, key)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> CommonResult<Vec<KvPair>> {
        let db = self.db.read().unwrap();
        let iter = db.prefix_scan(&self.cf, prefix)?;
        let mut items = Vec::new();
        for entry in iter {
            let (k, v) = entry?;
            items.push((k.to_vec(), v.to_vec()));
        }
        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::rocksdb::DBConf;
    use tempfile::TempDir;

    fn test_engine() -> (TempDir, RocksKvEngine) {
        let dir = TempDir::new().unwrap();
        let conf = DBConf::new(dir.path().to_str().unwrap());
        let db = DBEngine::new(conf, true).unwrap();
        (dir, RocksKvEngine::new(db))
    }

    #[test]
    fn test_basic_kv_operations() {
        let (_dir, engine) = test_engine();
        let store = engine.open_store("default");

        store.put(b"key1", b"value1").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));

        store.delete(b"key1").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_scan_prefix() {
        let (_dir, engine) = test_engine();
        let store = engine.open_store("default");

        store.put(b"cfg:a", b"1").unwrap();
        store.put(b"cfg:b", b"2").unwrap();
        store.put(b"mnt:x", b"3").unwrap();

        let cfg_items = store.scan_prefix(b"cfg:").unwrap();
        assert_eq!(cfg_items.len(), 2);

        let mnt_items = store.scan_prefix(b"mnt:").unwrap();
        assert_eq!(mnt_items.len(), 1);
    }

    #[test]
    fn test_exists() {
        let (_dir, engine) = test_engine();
        let store = engine.open_store("default");

        assert!(!store.exists(b"nope").unwrap());
        store.put(b"nope", b"yes").unwrap();
        assert!(store.exists(b"nope").unwrap());
    }
}
