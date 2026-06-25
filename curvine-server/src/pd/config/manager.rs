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
use crate::pd::journal::{self, ApplyOutcome, PdEntry};
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
    conf_overrides: HashMap<String, String>,
    values: RwLock<HashMap<String, ConfigInfo>>,
}

impl DynamicConfigCache {
    fn new(config_store: &ConfigStore, conf_overrides: HashMap<String, String>) -> FsResult<Self> {
        let mut registry = HashMap::new();
        for item in DYNAMIC_CONFIG_ITEMS {
            registry.insert(item.key.to_string(), item.clone());
        }

        let values = Self::load_values(config_store, &registry, &conf_overrides)?;
        Ok(Self {
            registry,
            conf_overrides,
            values: RwLock::new(values),
        })
    }

    fn load_values(
        config_store: &ConfigStore,
        registry: &HashMap<String, DynamicConfigItem>,
        conf_overrides: &HashMap<String, String>,
    ) -> FsResult<HashMap<String, ConfigInfo>> {
        let mut values: HashMap<String, ConfigInfo> = HashMap::new();
        for (key, item) in registry {
            let effective = conf_overrides
                .get(key)
                .cloned()
                .unwrap_or_else(|| item.default.to_string());
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

        for item in config_store.list("", None)? {
            if registry.contains_key(&item.key) {
                values.insert(item.key.clone(), item);
            } else {
                warn!("Ignore unknown persisted config key={}", item.key);
            }
        }
        Ok(values)
    }

    fn reload(&self, config_store: &ConfigStore) -> FsResult<()> {
        let values = Self::load_values(config_store, &self.registry, &self.conf_overrides)?;
        *self.values.write().unwrap() = values;
        Ok(())
    }

    fn is_valid_key(&self, key: &str) -> bool {
        self.registry.contains_key(key)
    }

    fn get(&self, key: &str) -> Option<ConfigInfo> {
        self.values.read().unwrap().get(key).cloned()
    }

    fn keys_with_prefix(&self, prefix: &str) -> Vec<ConfigInfo> {
        let mut items: Vec<ConfigInfo> = self
            .values
            .read()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(_, v)| v.clone())
            .collect();
        items.sort_by(|a, b| a.key.cmp(&b.key));
        items
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
            .expect("failed to initialize dynamic config cache");
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

    pub fn restore(&self) -> FsResult<()> {
        self.dynamic_cache.reload(&self.config_store)
    }

    pub fn apply_set_config(&self, entry: &ConfigEntry) -> FsResult<ApplyOutcome> {
        let item = &entry.info;
        if !self.is_valid_key(&item.key) {
            warn!("Apply set config skipped: unknown key: {}", item.key);
            return Ok(ApplyOutcome::not_found(unknown_key_error(&item.key)));
        }

        let Some(current) = self.dynamic_cache.get(&item.key) else {
            return Ok(ApplyOutcome::not_found(format!(
                "config key {} is not initialized",
                item.key
            )));
        };
        let Some(expected_new_version) = entry.expected_version.checked_add(1) else {
            return Ok(ApplyOutcome::stale(format!(
                "version overflow: expected:{}",
                entry.expected_version
            )));
        };
        if item.version != expected_new_version {
            return Ok(ApplyOutcome::stale(format!(
                "entry version mismatch: expected_new {}, entry {}",
                expected_new_version, item.version
            )));
        }
        if current.version == item.version && current.value == item.value {
            return Ok(ApplyOutcome::SkippedNoop);
        }
        if current.version != entry.expected_version {
            warn!(
                "Apply set config: {} skipped (current version {} != expected {})",
                item.key, current.version, entry.expected_version
            );
            return Ok(ApplyOutcome::stale(format!(
                "version mismatch: current={}, expected={}",
                current.version, entry.expected_version
            )));
        }

        info!("Apply set config: {}", item.key);
        self.config_store.set(item)?;
        self.dynamic_cache.update_from_kv(item);
        Ok(ApplyOutcome::Applied)
    }

    pub fn get_u32(&self, key: &str) -> u32 {
        self.config_value_str(key)
            .and_then(|s| s.parse().ok())
            .unwrap_or_default()
    }

    pub fn get_u64(&self, key: &str) -> u64 {
        self.config_value_str(key)
            .and_then(|s| s.parse().ok())
            .unwrap_or_default()
    }

    pub fn get_bool(&self, key: &str) -> bool {
        self.config_value_str(key)
            .map(|s| s == "true" || s == "1")
            .unwrap_or_default()
    }

    pub fn get_string(&self, key: &str) -> String {
        self.config_value_str(key).unwrap_or_default()
    }

    fn config_value_str(&self, key: &str) -> Option<String> {
        let value = self
            .dynamic_cache
            .get(key)
            .and_then(|info| String::from_utf8(info.value).ok());
        if value.is_none() {
            log::error!(
                "config key {} is not registered in DYNAMIC_CONFIG_ITEMS",
                key
            );
        }
        value
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

        let mut items: Vec<ConfigInfo> = self.dynamic_cache.keys_with_prefix(&req.prefix);
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

        let now = LocalTime::mills();
        let mut item = ProtoUtils::set_config_request_to_config_info(req);
        let expected_version = current.version;
        let version = expected_version
            .checked_add(1)
            .ok_or_else(|| FsError::common(format!("config {} version overflow", item.key)))?;
        item.version = version;
        item.mtime = now;

        let key = item.key.clone();
        let outcome = self
            .journal_client
            .propose_as_leader_with_result(PdEntry::SetConfig(ConfigEntry {
                op_ms: now,
                expected_version,
                info: item,
            }))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(SetConfigResponse {
                success: true,
                version,
            }),
            ApplyOutcome::SkippedStale { reason } => {
                Err(FsError::stale_entry("set_config", key, reason))
            }
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }
}
