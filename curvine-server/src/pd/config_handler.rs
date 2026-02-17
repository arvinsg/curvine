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

use crate::pd::config_store::ConfigStore;
use crate::pd::config_types::*;
use crate::pd::storage::pd_app_storage::PdEntry;
use curvine_common::error::FsError;
use curvine_common::raft::RaftClient;
use curvine_common::FsResult;
use log::info;
use orpc::err_box;
use std::sync::Arc;

pub struct ConfigHandler {
    config_store: Arc<ConfigStore>,
    raft_client: RaftClient,
}

impl ConfigHandler {
    pub fn new(config_store: Arc<ConfigStore>, raft_client: RaftClient) -> Self {
        Self {
            config_store,
            raft_client,
        }
    }

    pub fn get_config(&self, req: GetConfigRequest) -> FsResult<GetConfigResponse> {
        info!("Get config: {}", req.key);
        let item = self.config_store.get(&req.key)?;
        Ok(GetConfigResponse { item })
    }

    pub fn list_config(&self, req: ListConfigRequest) -> FsResult<ListConfigResponse> {
        info!("List config with prefix: {}", req.prefix);
        let items = self.config_store.list(&req.prefix, req.limit)?;
        Ok(ListConfigResponse { items })
    }

    pub async fn set_config(&self, req: SetConfigRequest) -> FsResult<SetConfigResponse> {
        info!("Set config: {}", req.key);

        let mut item = ConfigItem::new(req.key.clone(), req.value);
        if let Some(scope) = req.scope {
            item = item.with_scope(scope);
        }

        if let Some(existing) = self.config_store.get(&req.key)? {
            item.version = existing.version + 1;
        }

        let entry = PdEntry::SetConfig(item.clone());
        let data = bincode::serialize(&entry)
            .map_err(|e| FsError::from(format!("Failed to serialize entry: {}", e)))?;

        self.raft_client
            .send_propose(data)
            .await
            .map_err(|e| FsError::from(format!("Failed to propose: {}", e)))?;

        Ok(SetConfigResponse {
            success: true,
            version: item.version,
        })
    }

    pub async fn delete_config(&self, req: DeleteConfigRequest) -> FsResult<DeleteConfigResponse> {
        info!("Delete config: {}", req.key);

        if let Some(prev_version) = req.prev_version {
            if let Some(item) = self.config_store.get(&req.key)? {
                if item.version != prev_version {
                    return err_box!(
                        "Version mismatch: expected {}, got {}",
                        prev_version,
                        item.version
                    );
                }
            }
        }

        if !self.config_store.exists(&req.key)? {
            return Ok(DeleteConfigResponse { success: false });
        }

        let entry = PdEntry::DeleteConfig(req.key.clone());
        let data = bincode::serialize(&entry)
            .map_err(|e| FsError::from(format!("Failed to serialize entry: {}", e)))?;

        self.raft_client
            .send_propose(data)
            .await
            .map_err(|e| FsError::from(format!("Failed to propose: {}", e)))?;

        Ok(DeleteConfigResponse { success: true })
    }
}
