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
use crate::pd::pd_server::Pd;
use crate::pd::pool::PoolManager;
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
    node_manager: Arc<NodeManager>,
    pool_manager: Arc<PoolManager>,
    bg_manager: Arc<BGManager>,
    meta_manager: Arc<MetaManager>,
}

impl PdAppStorage {
    pub fn new(
        engine: Arc<RocksKvEngine>,
        snapshot_dir: String,
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
        node_manager: Arc<NodeManager>,
        pool_manager: Arc<PoolManager>,
        bg_manager: Arc<BGManager>,
        meta_manager: Arc<MetaManager>,
    ) -> Self {
        Self {
            engine,
            snapshot_dir,
            config_manager,
            mount_manager,
            node_manager,
            pool_manager,
            bg_manager,
            meta_manager,
        }
    }

    fn apply_entry(&self, is_leader: bool, message: &[u8]) -> RaftResult<Vec<u8>> {
        if message.is_empty() {
            return Ok(Vec::new());
        }

        let pd_entry: PdEntry = Serde::deserialize(message)?;
        Pd::get_metrics()
            .raft_apply_total
            .with_label_values(&[pd_entry.entry_type_str()])
            .inc();

        match pd_entry {
            PdEntry::Noop => {
                info!("Apply noop entry");
            }
            PdEntry::SetConfig(entry) => {
                let outcome = self.config_manager.apply_set_config(&entry.info)?;
                return Ok(outcome.encode()?);
            }
            PdEntry::Mount(entry) => {
                let outcome = self.mount_manager.apply_mount(entry.info)?;
                return Ok(outcome.encode()?);
            }
            PdEntry::Unmount(mount_id) => {
                let outcome = self.mount_manager.apply_unmount(mount_id)?;
                return Ok(outcome.encode()?);
            }
            PdEntry::RegisterNode(entry) => {
                info!(
                    "Apply RegisterNode node_id:{}, address:{:?}",
                    entry.info.base.node_id, entry.info.base.address
                );
                self.node_manager.apply_register_node(&entry)?;
            }
            PdEntry::SaveNode(entry) => {
                info!(
                    "Apply SaveNode node_id:{}, state:{:?}",
                    entry.info.base.node_id, entry.info.state
                );
                self.node_manager.apply_save_node(&entry)?;
            }
            PdEntry::UpdateNodeState(entry) => {
                info!(
                    "Apply UpdateNodeState node_id:{}, expected_epoch:{}, expected_state:{:?}, new_state:{:?}",
                    entry.node_id, entry.expected_epoch, entry.expected_state, entry.new_state
                );
                self.node_manager.apply_update_node_state(&entry)?;
            }
            PdEntry::BatchUpdateNodeState(entry) => {
                info!("Apply BatchUpdateNodeState entries={}", entry.entries.len());
                self.node_manager.apply_batch_update_node_state(&entry)?;
            }
            PdEntry::HeartbeatCheckpoint(entry) => {
                self.node_manager.apply_heartbeat_checkpoint(&entry)?;
            }
            PdEntry::DeleteNode(entry) => {
                info!(
                    "Apply DeleteNode node_id:{}, expected_epoch:{}, expected_state:{:?}",
                    entry.node_id, entry.expected_epoch, entry.expected_state
                );
                self.node_manager.apply_delete_node(&entry)?;
            }
            PdEntry::SavePool(entry) => {
                info!(
                    "Apply SavePool pool_id:{}, workers:{}, expected_epoch:{}, info_epoch:{}",
                    entry.info.pool_id,
                    entry.info.workers.len(),
                    entry.expected_epoch,
                    entry.info.epoch
                );
                let outcome = self.pool_manager.apply_save_pool(&entry)?;
                // Migrated to ApplyOutcome (P1.1): leaders receive structured
                // result via ProposeResponse.apply_result.
                return Ok(outcome.encode()?);
            }
            PdEntry::CreateBG(entry) => {
                info!("Apply CreateBG bg_id={}", entry.info.bg_id);
                self.bg_manager
                    .apply_create_bg_with_role(&entry, is_leader)?;
            }
            PdEntry::UpdateBG(entry) => {
                info!(
                    "Apply UpdateBG bg_id={}, expected_epoch={}, new_epoch={}",
                    entry.bg_id, entry.expected_bg_epoch, entry.new_bg_epoch
                );
                let outcome = self
                    .bg_manager
                    .apply_update_bg_with_role(&entry, is_leader)?;
                return Ok(outcome.encode()?);
            }
            PdEntry::DeleteBG(ref entry) => {
                info!(
                    "Apply DeleteBG bg_id={}, expected_epoch={}",
                    entry.bg_id, entry.expected_bg_epoch
                );
                let outcome = self
                    .bg_manager
                    .apply_delete_bg_with_role(entry, is_leader)?;
                return Ok(outcome.encode()?);
            }
            PdEntry::BatchBG(entry) => {
                info!(
                    "Apply BatchBG table={}, creates={}, updates={}",
                    entry.table.is_some(),
                    entry.creates.len(),
                    entry.updates.len()
                );
                let outcome = self
                    .bg_manager
                    .apply_batch_bg_with_role(&entry, is_leader)?;
                return Ok(outcome.encode()?);
            }
            PdEntry::BumpTableEpoch(entry) => {
                info!("Apply BumpTableEpoch updates={}", entry.updates.len());
                self.bg_manager
                    .apply_bump_table_epoch_with_role(&entry, is_leader)?;
            }
            PdEntry::AddPathRoute(ref entry) => {
                info!(
                    "Apply AddPathRoute path={}, expected_table_version={}",
                    entry.path, entry.expected_table_version
                );
                let outcome = self.meta_manager.apply_add_route(entry)?;
                return Ok(outcome.encode()?);
            }
            PdEntry::RemovePathRoute(ref path) => {
                info!("Apply RemovePathRoute path={}", path);
                let outcome = self.meta_manager.apply_remove_route(path)?;
                return Ok(outcome.encode()?);
            }
        }

        // For now, modules return FsResult<()> and we surface no structured
        // outcome. An empty byte slice on the wire is decoded as
        // `ApplyOutcome::Applied` by the propose caller (see
        // `pd/journal/apply_outcome.rs`). Per-module migrations in P1/P2/P3
        // will replace `Ok(Vec::new())` with `outcome.encode()?`.
        Ok(Vec::new())
    }
}

impl AppStorage for PdAppStorage {
    fn apply(&self, is_leader: bool, message: &[u8]) -> RaftResult<Vec<u8>> {
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

        self.mount_manager.restore()?;
        self.node_manager.restore()?;
        self.pool_manager.restore()?;
        self.bg_manager.restore()?;
        self.bg_manager.reset_runtime_route_state_after_snapshot();
        self.meta_manager.restore()?;
        Ok(())
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        Ok(format!("{}/checkpoint_{}", self.snapshot_dir, snapshot_id))
    }
}
