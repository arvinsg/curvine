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

use crate::pd::store::{self, KvStore};
use curvine_common::state::ConfigInfo;
use curvine_common::utils::SerdeUtils as Serde;
use log::info;
use orpc::CommonResult;
use std::sync::Arc;

const NS: &str = store::CF_META;
const CONFIG_PREFIX: u8 = store::PREFIX_CONFIG;

pub struct ConfigStore {
    store: Arc<dyn KvStore>,
}

impl ConfigStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    fn make_key(&self, key: &str) -> Vec<u8> {
        let mut buf = vec![CONFIG_PREFIX];
        buf.extend_from_slice(key.as_bytes());
        buf
    }

    pub fn set(&self, item: &ConfigInfo) -> CommonResult<()> {
        let db_key = self.make_key(&item.key);
        let data = Serde::serialize(item)?;
        self.store.put(NS, &db_key, &data)?;
        info!("Set config: {} (version: {})", item.key, item.version);
        Ok(())
    }

    pub fn list(&self, prefix: &str, limit: Option<u32>) -> CommonResult<Vec<ConfigInfo>> {
        let search_prefix = self.make_key(prefix);
        let limit = limit.unwrap_or(1000).min(10000) as usize;
        let pairs = self.store.scan_prefix(NS, &search_prefix)?;
        let mut items = Vec::with_capacity(pairs.len().min(limit));
        for (_key, value) in pairs {
            let config_item: ConfigInfo = Serde::deserialize(&value)?;
            items.push(config_item);
            if items.len() >= limit {
                break;
            }
        }
        Ok(items)
    }

    #[cfg(test)]
    pub fn get(&self, key: &str) -> CommonResult<Option<ConfigInfo>> {
        let db_key = self.make_key(key);
        match self.store.get(NS, &db_key)? {
            Some(data) => {
                let item: ConfigInfo = Serde::deserialize(&data)?;
                Ok(Some(item))
            }
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub fn delete(&self, key: &str) -> CommonResult<bool> {
        let db_key = self.make_key(key);
        if !self.store.exists(NS, &db_key)? {
            return Ok(false);
        }
        self.store.delete(NS, &db_key)?;
        info!("Deleted config: {}", key);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::store::memory_kv_engine::MemoryKvEngine;

    #[test]
    fn test_config_store() {
        let engine: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
        let store = ConfigStore::new(engine);

        let item = ConfigInfo::new("test.key".to_string(), b"test_value".to_vec());
        store.set(&item).unwrap();

        let retrieved = store.get("test.key").unwrap().unwrap();
        assert_eq!(retrieved.key, "test.key");
        assert_eq!(retrieved.value, b"test_value");

        let deleted = store.delete("test.key").unwrap();
        assert!(deleted);

        let not_found = store.get("test.key").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_list_configs() {
        let engine: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
        let store = ConfigStore::new(engine);

        store
            .set(&ConfigInfo::new("pd.test1".to_string(), b"v1".to_vec()))
            .unwrap();
        store
            .set(&ConfigInfo::new("pd.test2".to_string(), b"v2".to_vec()))
            .unwrap();
        store
            .set(&ConfigInfo::new("worker.test1".to_string(), b"v3".to_vec()))
            .unwrap();

        let pd_items = store.list("pd.", Some(10)).unwrap();
        assert_eq!(pd_items.len(), 2);

        let all_items = store.list("", Some(10)).unwrap();
        assert_eq!(all_items.len(), 3);
    }
}
