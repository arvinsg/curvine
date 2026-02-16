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

use crate::pd::config_types::ConfigItem;
use curvine_common::rocksdb::DBEngine;
use curvine_common::{FsError, FsResult};
use log::info;
use orpc::err_box;
use std::sync::{Arc, Mutex};

const CONFIG_CF: &str = "default";
const CONFIG_PREFIX: &str = "cfg:item:";

fn lock_error(e: std::sync::PoisonError<std::sync::MutexGuard<'_, DBEngine>>) -> FsError {
    FsError::from(format!("Lock failed: {}", e))
}

fn serialize_error(e: bincode::Error) -> FsError {
    FsError::from(format!("Failed to serialize config: {}", e))
}

fn deserialize_error(e: bincode::Error) -> FsError {
    FsError::from(format!("Failed to deserialize config: {}", e))
}

pub struct ConfigStore {
    db: Arc<Mutex<DBEngine>>,
}

impl ConfigStore {
    pub fn new(db: Arc<Mutex<DBEngine>>) -> Self {
        Self { db }
    }

    fn make_key(&self, key: &str) -> String {
        format!("{}{}", CONFIG_PREFIX, key)
    }

    pub fn get(&self, key: &str) -> FsResult<Option<ConfigItem>> {
        let db_key = self.make_key(key);
        let db = self.db.lock().map_err(lock_error)?;
        match db.get(db_key.as_bytes())? {
            Some(data) => {
                let item: ConfigItem = bincode::deserialize(&data).map_err(deserialize_error)?;
                Ok(Some(item))
            }
            None => Ok(None),
        }
    }

    pub fn set(&self, item: &ConfigItem) -> FsResult<()> {
        let db_key = self.make_key(&item.key);
        let data = bincode::serialize(item).map_err(serialize_error)?;
        let db = self.db.lock().map_err(lock_error)?;
        db.put(db_key.as_bytes(), &data)?;
        info!("Set config: {} (version: {})", item.key, item.version);
        Ok(())
    }

    pub fn delete(&self, key: &str) -> FsResult<bool> {
        let db_key = self.make_key(key);
        let db = self.db.lock().map_err(lock_error)?;
        if db.get(db_key.as_bytes())?.is_none() {
            return Ok(false);
        }
        db.delete(db_key.as_bytes())?;
        info!("Deleted config: {}", key);
        Ok(true)
    }

    pub fn list(&self, prefix: &str, limit: Option<u32>) -> FsResult<Vec<ConfigItem>> {
        let search_prefix = self.make_key(prefix);
        let limit = limit.unwrap_or(1000).min(10000) as usize;
        let db = self.db.lock().map_err(lock_error)?;
        let iter = db
            .prefix_scan(CONFIG_CF, search_prefix.as_bytes())
            .map_err(|e| FsError::from(format!("Failed to create prefix iterator: {}", e)))?;
        let mut items = Vec::new();
        for item in iter {
            let (_key, value) = item.map_err(FsError::from)?;
            let config_item: ConfigItem = bincode::deserialize(&value).map_err(deserialize_error)?;
            items.push(config_item);
            if items.len() >= limit {
                break;
            }
        }
        Ok(items)
    }

    pub fn exists(&self, key: &str) -> FsResult<bool> {
        let db_key = self.make_key(key);
        let db = self.db.lock().map_err(lock_error)?;
        Ok(db.get(db_key.as_bytes())?.is_some())
    }

    pub fn update(&self, key: &str, value: Vec<u8>) -> FsResult<ConfigItem> {
        let mut item = match self.get(key)? {
            Some(item) => item,
            None => return err_box!("Config key {} not found", key),
        };
        
        item.update_value(value);
        self.set(&item)?;
        Ok(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::rocksdb::DBConf;
    use tempfile::TempDir;

    #[test]
    fn test_config_store() {
        let temp_dir = TempDir::new().unwrap();
        let db_conf = DBConf::new(temp_dir.path().to_str().unwrap());
        let db = DBEngine::new(db_conf, true).unwrap();
        let store = ConfigStore::new(Arc::new(Mutex::new(db)));

        let item = ConfigItem::new("test.key".to_string(), b"test_value".to_vec());
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
        let temp_dir = TempDir::new().unwrap();
        let db_conf = DBConf::new(temp_dir.path().to_str().unwrap());
        let db = DBEngine::new(db_conf, true).unwrap();
        let store = ConfigStore::new(Arc::new(Mutex::new(db)));

        store.set(&ConfigItem::new("pd.test1".to_string(), b"v1".to_vec())).unwrap();
        store.set(&ConfigItem::new("pd.test2".to_string(), b"v2".to_vec())).unwrap();
        store.set(&ConfigItem::new("worker.test1".to_string(), b"v3".to_vec())).unwrap();

        let pd_items = store.list("pd.", Some(10)).unwrap();
        assert_eq!(pd_items.len(), 2);

        let all_items = store.list("", Some(10)).unwrap();
        assert_eq!(all_items.len(), 3);
    }
}
