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

use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::PdEntry;
use crate::pd::mount::MountManager;
use crate::pd::store::KvEngine;
use curvine_common::proto::raft::SnapshotData;
use curvine_common::raft::storage::AppStorage;
use curvine_common::raft::{RaftError, RaftResult, RaftUtils};
use curvine_common::utils::SerdeUtils as Serde;
use log::info;
use orpc::common::FileUtils;
use std::sync::Arc;

#[derive(Clone)]
pub struct PdAppStorage {
    engine: Arc<dyn KvEngine>,
    snapshot_dir: String,
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
}

impl PdAppStorage {
    pub fn new(
        engine: Arc<dyn KvEngine>,
        snapshot_dir: String,
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
    ) -> Self {
        Self {
            engine,
            snapshot_dir,
            config_manager,
            mount_manager,
        }
    }

    fn apply_entry(&self, _is_leader: bool, message: &[u8]) -> RaftResult<()> {
        if message.is_empty() {
            return Ok(());
        }

        let pd_entry: PdEntry = Serde::deserialize(message)?;
        match pd_entry {
            PdEntry::Noop => {
                info!("Apply noop entry");
            }
            PdEntry::SetConfig(entry) => self
                .config_manager
                .apply_set_config(&entry.info)
                .map_err(|e| RaftError::from(e.to_string()))?,
            PdEntry::DeleteConfig(key) => self
                .config_manager
                .apply_delete_config(&key)
                .map_err(|e| RaftError::from(e.to_string()))?,
            PdEntry::Mount(entry) => self
                .mount_manager
                .apply_mount(entry.info)
                .map_err(|e| RaftError::from(e.to_string()))?,
            PdEntry::Unmount(mount_id) => self
                .mount_manager
                .apply_unmount(mount_id)
                .map_err(|e| RaftError::from(e.to_string()))?,
        }

        Ok(())
    }
}

impl AppStorage for PdAppStorage {
    fn apply(&self, is_leader: bool, message: &[u8]) -> RaftResult<()> {
        self.apply_entry(is_leader, message)
    }

    fn create_snapshot(&self, node_id: u64, last_applied: u64) -> RaftResult<SnapshotData> {
        let dir = self
            .engine
            .create_checkpoint(last_applied)
            .map_err(|e| RaftError::from(format!("Create checkpoint failed: {}", e)))?;

        let data = RaftUtils::create_file_snapshot(&dir, node_id, last_applied)?;

        info!(
            "Created snapshot at {} for node {} with snapshot_id {}",
            dir, node_id, last_applied
        );

        Ok(data)
    }

    fn apply_snapshot(&self, snapshot: &SnapshotData) -> RaftResult<()> {
        info!(
            "Applying snapshot from node {} with id {}",
            snapshot.node_id, snapshot.snapshot_id
        );

        let files = snapshot
            .files_data
            .as_ref()
            .ok_or_else(|| RaftError::from("Snapshot has no files_data".to_string()))?;

        self.engine
            .restore_from_checkpoint(&files.dir)
            .map_err(|e| RaftError::from(format!("Restore from checkpoint failed: {}", e)))?;
        info!("Restored store from snapshot checkpoint {}", files.dir);

        self.mount_manager
            .restore()
            .map_err(|e| RaftError::from(e.to_string()))?;
        Ok(())
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        Ok(format!("{}/checkpoint_{}", self.snapshot_dir, snapshot_id))
    }
}
