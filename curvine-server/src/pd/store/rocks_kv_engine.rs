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

use super::{KvPair, KvStore, KvWrite};
use arc_swap::ArcSwap;
use curvine_common::rocksdb::{DBConf, DBEngine, WriteBatch};
use orpc::common::{FileUtils, Utils};
use orpc::CommonResult;
use std::sync::Arc;

/// RocksDB-backed KV engine
pub struct RocksKvEngine {
    db: ArcSwap<DBEngine>,
}

impl RocksKvEngine {
    pub fn new(db: DBEngine) -> Self {
        Self {
            db: ArcSwap::from_pointee(db),
        }
    }

    pub fn create_checkpoint(&self, id: u64) -> CommonResult<String> {
        self.db.load().create_checkpoint(id)
    }

    /// Restore the engine from a checkpoint directory.
    ///
    /// 1. Save the current DB config (includes CF definitions).
    /// 2. Atomically swap to a throw-away temp DB to redirect traffic.
    /// 3. Replace the data directory with the checkpoint.
    /// 4. Open a fresh DB at the original path and atomically swap it in.
    pub fn restore(&self, checkpoint_dir: &str) -> CommonResult<()> {
        let conf = self.db.load().conf().clone();

        let tmp_path = Utils::temp_file();
        let tmp_db = DBEngine::new(DBConf::new(&tmp_path), true)?;
        let _old = self.db.swap(Arc::new(tmp_db));

        FileUtils::delete_path(&conf.data_dir, true)?;
        FileUtils::copy_dir(checkpoint_dir, &conf.data_dir)?;
        let _ = FileUtils::delete_path(&tmp_path, true);

        let new_db = DBEngine::new(conf, false)?;
        self.db.store(Arc::new(new_db));

        Ok(())
    }
}

impl KvStore for RocksKvEngine {
    fn get(&self, ns: &str, key: &[u8]) -> CommonResult<Option<Vec<u8>>> {
        self.db.load().get_cf(ns, key)
    }

    fn put(&self, ns: &str, key: &[u8], value: &[u8]) -> CommonResult<()> {
        self.db.load().put_cf(ns, key, value)
    }

    fn delete(&self, ns: &str, key: &[u8]) -> CommonResult<()> {
        self.db.load().delete_cf(ns, key)
    }

    fn scan_prefix(&self, ns: &str, prefix: &[u8]) -> CommonResult<Vec<KvPair>> {
        let db = self.db.load();
        let iter = db.prefix_scan(ns, prefix)?;
        let mut items = Vec::new();
        for entry in iter {
            let (k, v) = entry?;
            items.push((k.to_vec(), v.to_vec()));
        }
        Ok(items)
    }

    fn write_batch(&self, ops: Vec<KvWrite>) -> CommonResult<()> {
        let db = self.db.load();
        let mut batch = WriteBatch::new(&db);
        for op in ops {
            match op {
                KvWrite::Put { ns, key, value } => batch.put_cf(&ns, key, value)?,
                KvWrite::Delete { ns, key } => batch.delete_cf(&ns, key)?,
            }
        }
        batch.commit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::rocksdb::DBConf;
    use tempfile::TempDir;

    fn test_engine() -> (TempDir, Arc<RocksKvEngine>) {
        let dir = TempDir::new().unwrap();
        let conf = DBConf::new(dir.path().to_str().unwrap());
        let db = DBEngine::new(conf, true).unwrap();
        (dir, Arc::new(RocksKvEngine::new(db)))
    }

    #[test]
    fn test_basic_kv_operations() {
        let (_dir, engine) = test_engine();

        engine.put("default", b"key1", b"value1").unwrap();
        assert_eq!(
            engine.get("default", b"key1").unwrap(),
            Some(b"value1".to_vec())
        );

        engine.delete("default", b"key1").unwrap();
        assert_eq!(engine.get("default", b"key1").unwrap(), None);
    }

    #[test]
    fn test_scan_prefix() {
        let (_dir, engine) = test_engine();

        engine.put("default", b"cfg:a", b"1").unwrap();
        engine.put("default", b"cfg:b", b"2").unwrap();
        engine.put("default", b"mnt:x", b"3").unwrap();

        let cfg_items = engine.scan_prefix("default", b"cfg:").unwrap();
        assert_eq!(cfg_items.len(), 2);

        let mnt_items = engine.scan_prefix("default", b"mnt:").unwrap();
        assert_eq!(mnt_items.len(), 1);
    }

    #[test]
    fn test_exists() {
        let (_dir, engine) = test_engine();

        assert!(!engine.exists("default", b"nope").unwrap());
        engine.put("default", b"nope", b"yes").unwrap();
        assert!(engine.exists("default", b"nope").unwrap());
    }
}
