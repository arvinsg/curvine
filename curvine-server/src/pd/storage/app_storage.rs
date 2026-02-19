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

use crate::pd::config::ConfigStore;
use crate::pd::storage::entry::PdEntry;
use curvine_common::proto::raft::SnapshotData;
use curvine_common::raft::storage::AppStorage;
use curvine_common::raft::{RaftError, RaftResult, RaftUtils};
use curvine_common::rocksdb::DBEngine;
use log::info;
use orpc::common::FileUtils;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct PdAppStorage {
    db: Arc<Mutex<DBEngine>>,
    snapshot_dir: String,
    config_store: Arc<ConfigStore>,
}

impl PdAppStorage {
    pub fn new(db: DBEngine, snapshot_dir: String) -> Self {
        let db = Arc::new(Mutex::new(db));
        let config_store = Arc::new(ConfigStore::new(db.clone()));

        Self {
            db,
            snapshot_dir,
            config_store,
        }
    }

    pub fn config_store(&self) -> Arc<ConfigStore> {
        self.config_store.clone()
    }

    fn apply_entry(&self, _is_leader: bool, message: &[u8]) -> RaftResult<()> {
        if message.is_empty() {
            return Ok(());
        }

        let pd_entry: PdEntry = bincode::deserialize(message)
            .map_err(|e| format!("Failed to deserialize entry: {}", e))?;

        match pd_entry {
            PdEntry::Noop => {
                info!("Apply noop entry");
            }
            PdEntry::SetConfig(item) => {
                // Apply only if this entry is not stale (last write wins by log order).
                if let Ok(Some(existing)) = self.config_store.get(&item.key) {
                    if existing.version >= item.version {
                        info!("Apply set config: {} skipped (existing version {} >= {})", item.key, existing.version, item.version);
                        return Ok(());
                    }
                }
                info!("Apply set config: {}", item.key);
                self.config_store.set(&item)
                    .map_err(|e| format!("Failed to set config: {}", e))?;
            }
            PdEntry::DeleteConfig(key) => {
                info!("Apply delete config: {}", key);
                self.config_store.delete(&key)
                    .map_err(|e| format!("Failed to delete config: {}", e))?;
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
        let checkpoint_dir = format!("{}/checkpoint_{}", self.snapshot_dir, last_applied);

        FileUtils::create_dir(&checkpoint_dir, true)?;

        let data = RaftUtils::create_file_snapshot(&checkpoint_dir, node_id, last_applied)?;

        info!(
            "Created snapshot at {} for node {} with snapshot_id {}",
            checkpoint_dir, node_id, last_applied
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

        let mut db = self
            .db
            .lock()
            .map_err(|e| RaftError::from(format!("Lock db failed: {}", e)))?;
        RaftUtils::apply_rocks_snapshot(&mut *db, files)?;
        info!("Restored RocksDB from snapshot checkpoint {}", files.dir);
        Ok(())
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        let dir = format!("{}/checkpoint_{}", self.snapshot_dir, snapshot_id);
        Ok(dir)
    }
}
