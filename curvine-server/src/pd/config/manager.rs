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

use super::configs::unknown_key_error;
use super::store::ConfigStore;
use crate::pd::journal::entry::ConfigEntry;
use crate::pd::journal::PdEntry;
use crate::pd::store::KvStore;
use curvine_common::proto::*;
use curvine_common::raft::RaftClient;
use curvine_common::state::ConfigInfo;
use curvine_common::utils::{ProtoUtils, SerdeUtils as Serde};
use curvine_common::{FsError, FsResult};
use log::{info, warn};
use orpc::common::LocalTime;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ConfigManager {
    config_store: Arc<ConfigStore>,
    raft_client: RaftClient,
    dynamic_config: HashMap<String, String>,
}

impl ConfigManager {
    pub fn new(
        store: Arc<dyn KvStore>,
        raft_client: RaftClient,
        dynamic_config: HashMap<String, String>,
    ) -> Self {
        let config_store = Arc::new(ConfigStore::new(store));
        Self {
            config_store,
            raft_client,
            dynamic_config,
        }
    }

    fn is_valid_key(&self, key: &str) -> bool {
        self.dynamic_config.contains_key(key)
    }

    fn default_config_info(&self, key: &str) -> Option<ConfigInfo> {
        self.dynamic_config
            .get(key)
            .map(|default_value| ConfigInfo {
                key: key.to_string(),
                value: default_value.as_bytes().to_vec(),
                version: 0,
                mtime: 0,
            })
    }

    // -- Raft apply callbacks (called by PdAppStorage) -----------------------

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
        Ok(())
    }

    // -- Public API ----------------------------------------------------------

    fn propose(&self, entry: PdEntry) -> FsResult<()> {
        let data = Serde::serialize(&entry)?;
        self.raft_client.block_on_send_propose(data)?;
        Ok(())
    }

    pub fn get_config(&self, req: GetConfigRequest) -> FsResult<GetConfigResponse> {
        info!("Get config: {}", req.key);

        if let Some(item) = self.config_store.get(&req.key)? {
            return Ok(GetConfigResponse {
                item: Some(ProtoUtils::config_info_to_pb(&item)),
            });
        }

        Ok(GetConfigResponse {
            item: self
                .default_config_info(&req.key)
                .map(|i| ProtoUtils::config_info_to_pb(&i)),
        })
    }

    pub fn list_config(&self, req: ListConfigRequest) -> FsResult<ListConfigResponse> {
        info!("List config with prefix: {}", req.prefix);
        let limit = req.limit.unwrap_or(1000).min(10000) as usize;

        let persisted = self.config_store.list(&req.prefix, Some(limit as u32))?;
        let persisted_keys: std::collections::HashSet<String> =
            persisted.iter().map(|i| i.key.clone()).collect();

        let mut items = persisted;

        for (key, default_value) in &self.dynamic_config {
            if items.len() >= limit {
                break;
            }
            if !key.starts_with(&req.prefix) {
                continue;
            }
            if persisted_keys.contains(key) {
                continue;
            }
            items.push(ConfigInfo {
                key: key.clone(),
                value: default_value.as_bytes().to_vec(),
                version: 0,
                mtime: 0,
            });
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

        let mut item = ProtoUtils::set_config_request_to_config_info(req);

        if let Some(existing) = self.config_store.get(&item.key)? {
            item.version = existing.version + 1;
        }

        self.propose(PdEntry::SetConfig(ConfigEntry {
            op_ms: LocalTime::mills(),
            info: item.clone(),
        }))?;

        Ok(SetConfigResponse {
            success: true,
            version: item.version,
        })
    }
}
