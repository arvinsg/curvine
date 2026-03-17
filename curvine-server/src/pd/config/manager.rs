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

use super::error::unknown_key_error;
use super::keys::{DynamicConfigItem, DYNAMIC_CONFIG_ITEMS};
use super::store::ConfigStore;
use crate::pd::journal::entry::ConfigEntry;
use crate::pd::journal::{self, PdEntry};
use crate::pd::store::KvStore;
use curvine_common::proto::*;
use curvine_common::state::ConfigInfo;
use curvine_common::utils::ProtoUtils;
use curvine_common::{FsError, FsResult};
use log::{info, warn};
use orpc::common::LocalTime;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

/// Cache for dynamic config items: registry + current effective values.
struct DynamicConfigCache {
    registry: HashMap<String, DynamicConfigItem>,
    values: RwLock<HashMap<String, ConfigInfo>>,
}

impl DynamicConfigCache {
    fn new(config_store: &ConfigStore, conf_overrides: HashMap<String, String>) -> FsResult<Self> {
        let mut registry = HashMap::new();
        for item in DYNAMIC_CONFIG_ITEMS {
            registry.insert(item.key.to_string(), item.clone());
        }

        let mut values: HashMap<String, ConfigInfo> = HashMap::new();
        for (key, item) in &registry {
            let mut effective = item.default.to_string();
            if let Some(conf_v) = conf_overrides.get(key) {
                effective = conf_v.clone();
            }
            values.insert(
                key.clone(),
                ConfigInfo {
                    key: key.clone(),
                    value: effective.into_bytes(),
                    version: 0,
                    mtime: 0,
                },
            );
        }

        let persisted = config_store.list("", None)?;
        for item in persisted {
            values.insert(item.key.clone(), item);
        }

        Ok(Self {
            registry,
            values: RwLock::new(values),
        })
    }

    fn is_valid_key(&self, key: &str) -> bool {
        self.registry.contains_key(key)
    }

    fn get(&self, key: &str) -> Option<ConfigInfo> {
        self.values.read().unwrap().get(key).cloned()
    }

    fn keys_with_prefix(&self, prefix: &str) -> Vec<(String, ConfigInfo)> {
        self.values
            .read()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn update_from_kv(&self, item: &ConfigInfo) {
        self.values
            .write()
            .unwrap()
            .insert(item.key.clone(), item.clone());
    }
}

pub struct ConfigManager {
    config_store: Arc<ConfigStore>,
    journal_client: Arc<journal::Client>,
    dynamic_cache: DynamicConfigCache,
    set_lock: Mutex<()>,
}

impl ConfigManager {
    pub fn new(
        store: Arc<dyn KvStore>,
        journal_client: Arc<journal::Client>,
        conf_dynamic_config: HashMap<String, String>,
    ) -> Self {
        let config_store = Arc::new(ConfigStore::new(store));
        let dynamic_cache = DynamicConfigCache::new(&config_store, conf_dynamic_config)
            .unwrap_or_else(|_| DynamicConfigCache {
                registry: HashMap::new(),
                values: RwLock::new(HashMap::new()),
            });
        Self {
            config_store,
            journal_client,
            dynamic_cache,
            set_lock: Mutex::new(()),
        }
    }

    fn is_valid_key(&self, key: &str) -> bool {
        self.dynamic_cache.is_valid_key(key)
    }

    //  Raft apply callbacks (called by PdAppStorage)

    pub fn apply_set_config(&self, item: &ConfigInfo) -> FsResult<()> {
        if let Some(existing) = self.config_store.get(&item.key)? {
            if existing.version >= item.version {
                warn!(
                    "Apply set config: {} skipped (existing version {} >= {})",
                    item.key, existing.version, item.version
                );
                return Ok(());
            }
        }
        info!("Apply set config: {}", item.key);
        self.config_store.set(item)?;
        self.dynamic_cache.update_from_kv(item);
        Ok(())
    }

    // -- Public API ----------------------------------------------------------

    pub fn get_u32(&self, key: &str, default: u32) -> u32 {
        self.config_value_str(key)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    pub fn get_u64(&self, key: &str, default: u64) -> u64 {
        self.config_value_str(key)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        self.config_value_str(key)
            .map(|s| s == "true" || s == "1")
            .unwrap_or(default)
    }

    fn config_value_str(&self, key: &str) -> Option<String> {
        self.dynamic_cache
            .get(key)
            .and_then(|info| String::from_utf8(info.value).ok())
    }

    pub fn get_config(&self, req: GetConfigRequest) -> FsResult<GetConfigResponse> {
        info!("Get config: {}", req.key);
        let item = self
            .dynamic_cache
            .get(&req.key)
            .map(|i| ProtoUtils::config_info_to_pb(&i));
        Ok(GetConfigResponse { item })
    }

    pub fn list_config(&self, req: ListConfigRequest) -> FsResult<ListConfigResponse> {
        info!("List config with prefix: {}", req.prefix);
        let limit = req.limit.unwrap_or(1000).min(10000) as usize;

        let mut items: Vec<ConfigInfo> = self
            .dynamic_cache
            .keys_with_prefix(&req.prefix)
            .into_iter()
            .map(|(_, info)| info)
            .collect();
        if items.len() > limit {
            items.truncate(limit);
        }

        Ok(ListConfigResponse {
            items: items.iter().map(ProtoUtils::config_info_to_pb).collect(),
        })
    }

    pub fn set_config(&self, req: SetConfigRequest) -> FsResult<SetConfigResponse> {
        if !self.is_valid_key(&req.key) {
            return Err(FsError::common(unknown_key_error(&req.key)));
        }
        info!("Set config: {}", req.key);

        let _guard = self.set_lock.lock().unwrap();

        let mut item = ProtoUtils::set_config_request_to_config_info(req);

        if let Some(existing) = self.dynamic_cache.get(&item.key) {
            item.version = existing.version + 1;
        }

        self.journal_client
            .propose(PdEntry::SetConfig(ConfigEntry {
                op_ms: LocalTime::mills(),
                info: item.clone(),
            }))?;

        Ok(SetConfigResponse {
            success: true,
            version: item.version,
        })
    }
}
