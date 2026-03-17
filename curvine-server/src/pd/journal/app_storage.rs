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

use crate::pd::bg::BGManager;
use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::PdEntry;
use crate::pd::meta::MetaManager;
use crate::pd::mount::MountManager;
use crate::pd::node::NodeManager;
use crate::pd::store::RocksKvEngine;
use curvine_common::proto::raft::SnapshotData;
use curvine_common::raft::storage::AppStorage;
use curvine_common::raft::{RaftError, RaftResult, RaftUtils};
use curvine_common::utils::SerdeUtils as Serde;
use log::info;
use std::sync::Arc;

/// PD application storage for Raft state machine.
#[derive(Clone)]
pub struct PdAppStorage {
    engine: Arc<RocksKvEngine>,
    snapshot_dir: String,
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
    node_manager: Option<Arc<NodeManager>>,
    bg_manager: Option<Arc<BGManager>>,
    meta_manager: Option<Arc<MetaManager>>,
}

impl PdAppStorage {
    pub fn new(
        engine: Arc<RocksKvEngine>,
        snapshot_dir: String,
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
        node_manager: Option<Arc<NodeManager>>,
        bg_manager: Option<Arc<BGManager>>,
        meta_manager: Option<Arc<MetaManager>>,
    ) -> Self {
        Self {
            engine,
            snapshot_dir,
            config_manager,
            mount_manager,
            node_manager,
            bg_manager,
            meta_manager,
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
            PdEntry::SetConfig(entry) => self.config_manager.apply_set_config(&entry.info)?,
            PdEntry::Mount(entry) => self.mount_manager.apply_mount(entry.info)?,
            PdEntry::Unmount(mount_id) => self.mount_manager.apply_unmount(mount_id)?,
            PdEntry::RegisterNode(entry) => {
                info!(
                    "Apply RegisterNode node_id:{}, address:{:?}",
                    entry.info.base.node_id, entry.info.base.address
                );
                if let Some(ref nm) = self.node_manager {
                    nm.apply_register_node(&entry)?;
                }
            }
            PdEntry::SaveNode(entry) => {
                info!(
                    "Apply SaveNode node_id:{}, state:{:?}",
                    entry.info.base.node_id, entry.info.state
                );
                if let Some(ref nm) = self.node_manager {
                    nm.apply_save_node(&entry)?;
                }
            }
            PdEntry::CreateBG(entry) => {
                info!("Apply CreateBG bg_id={}", entry.info.bg_id);
                if let Some(ref bm) = self.bg_manager {
                    bm.apply_create_bg(&entry)?;
                }
            }
            PdEntry::UpdateBG(entry) => {
                info!("Apply UpdateBG bg_id={}", entry.bg_id);
                if let Some(ref bm) = self.bg_manager {
                    bm.apply_update_bg(&entry)?;
                }
            }
            PdEntry::DeleteBG(bg_id) => {
                info!("Apply DeleteBG bg_id={}", bg_id);
                if let Some(ref bm) = self.bg_manager {
                    bm.apply_delete_bg(bg_id)?;
                }
            }
            PdEntry::AddPathRoute(ref entry) => {
                info!("Apply AddPathRoute path={}", entry.path);
                if let Some(ref pt) = self.meta_manager {
                    pt.apply_add_route(entry)?;
                }
            }
            PdEntry::RemovePathRoute(ref path) => {
                info!("Apply RemovePathRoute path={}", path);
                if let Some(ref pt) = self.meta_manager {
                    pt.apply_remove_route(path)?;
                }
            }
        }

        Ok(())
    }
}

impl AppStorage for PdAppStorage {
    fn apply(&self, is_leader: bool, message: &[u8]) -> RaftResult<()> {
        self.apply_entry(is_leader, message)
    }

    fn create_snapshot(&self, node_id: u64, last_applied: u64) -> RaftResult<SnapshotData> {
        let dir = self.engine.create_checkpoint(last_applied)?;
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

        self.engine.restore(&files.dir)?;
        info!("Restored store from snapshot checkpoint {}", files.dir);

        self.mount_manager
            .restore()
            .map_err(|e| RaftError::from(e.to_string()))?;
        if let Some(ref nm) = self.node_manager {
            nm.restore().map_err(|e| RaftError::from(e.to_string()))?;
        }
        if let Some(ref bm) = self.bg_manager {
            bm.restore().map_err(|e| RaftError::from(e.to_string()))?;
        }
        if let Some(ref mm) = self.meta_manager {
            mm.restore().map_err(|e| RaftError::from(e.to_string()))?;
        }
        Ok(())
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        Ok(format!("{}/checkpoint_{}", self.snapshot_dir, snapshot_id))
    }
}
