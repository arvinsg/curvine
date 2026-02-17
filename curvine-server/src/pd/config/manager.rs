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

use super::configs::{is_valid_key, unknown_key_error};
use super::store::ConfigStore;
use crate::pd::journal::entry::ConfigEntry;
use crate::pd::journal::PdEntry;
use curvine_common::proto::*;
use curvine_common::raft::RaftClient;
use curvine_common::rocksdb::DBEngine;
use curvine_common::state::ConfigInfo;
use curvine_common::utils::{ProtoUtils, SerdeUtils as Serde};
use curvine_common::{FsError, FsResult};
use log::{info, warn};
use orpc::common::LocalTime;
use std::sync::{Arc, RwLock};

pub struct ConfigManager {
    config_store: Arc<ConfigStore>,
    raft_client: RaftClient,
}

impl ConfigManager {
    pub fn new(db: Arc<RwLock<DBEngine>>, raft_client: RaftClient) -> Self {
        let config_store = Arc::new(ConfigStore::new(db));
        Self {
            config_store,
            raft_client,
        }
    }

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

    pub fn apply_delete_config(&self, key: &str) -> FsResult<()> {
        info!("Apply delete config: {}", key);
        self.config_store.delete(key)?;
        Ok(())
    }

    fn propose(&self, entry: PdEntry) -> FsResult<()> {
        let data = Serde::serialize(&entry)?;
        self.raft_client.block_on_send_propose(data)?;
        Ok(())
    }

    pub fn get_config(&self, req: GetConfigRequest) -> FsResult<GetConfigResponse> {
        info!("Get config: {}", req.key);
        let item = self.config_store.get(&req.key)?;
        Ok(GetConfigResponse {
            item: item.map(|i| ProtoUtils::config_info_to_pb(&i)),
        })
    }

    pub fn list_config(&self, req: ListConfigRequest) -> FsResult<ListConfigResponse> {
        info!("List config with prefix: {}", req.prefix);
        let items = self.config_store.list(&req.prefix, req.limit)?;
        Ok(ListConfigResponse {
            items: items.iter().map(ProtoUtils::config_info_to_pb).collect(),
        })
    }

    pub fn set_config(&self, req: SetConfigRequest) -> FsResult<SetConfigResponse> {
        if !is_valid_key(&req.key) {
            return Err(FsError::common(unknown_key_error(&req.key, "set")));
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

    pub fn delete_config(&self, req: DeleteConfigRequest) -> FsResult<DeleteConfigResponse> {
        if !is_valid_key(&req.key) {
            return Err(FsError::common(unknown_key_error(&req.key, "deleted")));
        }
        info!("Delete config: {}", req.key);

        if let Some(prev_version) = req.prev_version {
            if let Some(item) = self.config_store.get(&req.key)? {
                if item.version != prev_version {
                    return Err(FsError::common(format!(
                        "Version mismatch: expected {}, got {}",
                        prev_version, item.version
                    )));
                }
            }
        }

        if !self.config_store.exists(&req.key)? {
            return Ok(DeleteConfigResponse { success: false });
        }

        self.propose(PdEntry::DeleteConfig(req.key.clone()))?;

        Ok(DeleteConfigResponse { success: true })
    }
}
