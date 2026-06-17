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

use super::index::MountTableIndex;
use super::store::MountStore;
use crate::pd::journal::entry::MountEntry;
use crate::pd::journal::{self, ApplyOutcome, PdEntry};
use crate::pd::store::KvStore;
use curvine_common::fs::Path;
use curvine_common::state::{MountInfo, MountOptions};
use curvine_common::{FsError, FsResult};
use log::{info, warn};
use orpc::common::LocalTime;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

pub struct MountManager {
    index: Arc<RwLock<MountTableIndex>>,
    store: Arc<MountStore>,
    journal_client: Arc<journal::Client>,
    version: AtomicU64,
}

impl MountManager {
    pub fn new(store: Arc<dyn KvStore>, journal_client: Arc<journal::Client>) -> Self {
        let store = Arc::new(MountStore::new(store));
        Self {
            index: Arc::new(RwLock::new(MountTableIndex::new())),
            store,
            journal_client,
            version: AtomicU64::new(0),
        }
    }

    pub fn restore(&self) -> FsResult<()> {
        let mounts = self.store.list_all_mounts()?;
        let mut index = self.index.write().unwrap();
        for mnt in mounts {
            info!(
                "Restore mount: {} -> {} (id={})",
                mnt.cv_path, mnt.ufs_path, mnt.mount_id
            );
            index.insert(mnt);
        }
        let version = self.store.get_version()?;
        self.version.store(version, Ordering::Relaxed);
        Ok(())
    }

    /// Apply a mount entry from Raft.
    ///
    /// P3.2: re-check prefix conflict at apply time. Pre-P3.2, the conflict
    /// check ran only in the propose path — two concurrent add_mount calls
    /// could both pass propose-time check (snapshot has neither) and both
    /// install conflicting entries. Now apply rejects the second one as
    /// `SkippedStale` and the index never holds prefix-conflicting mounts.
    pub fn apply_mount(&self, info: MountInfo) -> FsResult<ApplyOutcome> {
        let mut index = self.index.write().unwrap();
        // Apply-side prefix conflict re-check. Skip when this is an in-place
        // replace of the same mount_id (update_mount path).
        let is_replace = index.contains_id(info.mount_id);
        if !is_replace {
            if index.get_by_ufs_path(&info.ufs_path).is_some() {
                warn!(
                    "Apply mount skipped: ufs_path={} already mounted (concurrent add_mount)",
                    info.ufs_path
                );
                return Ok(ApplyOutcome::stale(format!(
                    "ufs_path {} already mounted",
                    info.ufs_path
                )));
            }
            if index.get_by_cv_path(&info.cv_path).is_some() {
                warn!(
                    "Apply mount skipped: cv_path={} already mounted (concurrent add_mount)",
                    info.cv_path
                );
                return Ok(ApplyOutcome::stale(format!(
                    "cv_path {} already mounted",
                    info.cv_path
                )));
            }
            if let Err(e) = index.check_conflict(&info.cv_path, &info.ufs_path) {
                warn!(
                    "Apply mount skipped: prefix conflict cv_path={}, ufs_path={}, err={}",
                    info.cv_path, info.ufs_path, e
                );
                return Ok(ApplyOutcome::stale(format!(
                    "prefix conflict on cv_path={}, ufs_path={}: {}",
                    info.cv_path, info.ufs_path, e
                )));
            }
        }
        info!(
            "Apply mount: {} -> {} (id={}, mode={})",
            info.cv_path,
            info.ufs_path,
            info.mount_id,
            if is_replace { "replace" } else { "insert" }
        );
        self.store.put_mount(&info)?;
        index.insert(info);
        let v = self.version.fetch_add(1, Ordering::Relaxed) + 1;
        self.store.put_version(v)?;
        Ok(ApplyOutcome::Applied)
    }

    pub fn apply_unmount(&self, mount_id: u32) -> FsResult<ApplyOutcome> {
        let mut index = self.index.write().unwrap();
        let info = match index.remove(mount_id) {
            Some(i) => i,
            None => {
                warn!(
                    "Apply unmount skipped: mount_id={} not present in index",
                    mount_id
                );
                return Ok(ApplyOutcome::not_found(format!(
                    "mount_id {} not present",
                    mount_id
                )));
            }
        };
        drop(index);
        self.store.delete_mount(mount_id)?;
        let v = self.version.fetch_add(1, Ordering::Relaxed) + 1;
        self.store.put_version(v)?;
        info!("Apply unmount: {} (id={})", info.cv_path, mount_id);
        Ok(ApplyOutcome::Applied)
    }

    fn assign_mount_id(&self) -> FsResult<u32> {
        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let id = rng.gen::<u32>();
            if !self.index.read().unwrap().contains_id(id) {
                return Ok(id);
            }
        }
        Err(FsError::common("failed assign mount id"))
    }

    fn add_mount(
        &self,
        mnt_id: Option<u32>,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        {
            let index = self.index.read().unwrap();
            if index.get_by_ufs_path(ufs_path).is_some() {
                return Err(FsError::mount_path_exists(ufs_path));
            }
            if index.get_by_cv_path(cv_path).is_some() {
                return Err(FsError::mount_path_exists(cv_path));
            }
            index.check_conflict(cv_path, ufs_path)?;
        }

        let mount_id = match mnt_id {
            Some(id) => id,
            None => self.assign_mount_id()?,
        };

        let info = mnt_opt.clone().to_info(mount_id, cv_path, ufs_path);
        self.propose_mount(info, "add_mount")
    }

    fn update_mount(
        &self,
        mnt_id: Option<u32>,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        if self.index.read().unwrap().get_by_cv_path(cv_path).is_none() {
            return Err(FsError::common(format!(
                "update mode: mount point {} does not exist",
                cv_path
            )));
        }

        self.umount(cv_path)?;

        let assign_id = match mnt_id {
            Some(id) => id,
            None => self.assign_mount_id()?,
        };

        let info = mnt_opt.clone().to_info(assign_id, cv_path, ufs_path);
        self.propose_mount(info, "update_mount")
    }

    /// Common Mount propose path: leader-fenced + ApplyOutcome translation
    /// (#6 fix). Pre-#6 used plain `propose()` which discarded apply-side
    /// SkippedStale (e.g., P3.2 prefix conflict re-check).
    fn propose_mount(&self, info: MountInfo, kind: &str) -> FsResult<()> {
        let cv_path = info.cv_path.clone();
        let entry = MountEntry {
            op_ms: LocalTime::mills(),
            info,
        };
        let outcome = self
            .journal_client
            .propose_as_leader_with_result(PdEntry::Mount(entry))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => {
                warn!("{} cv_path={} returned Stale: {}", kind, cv_path, reason);
                Err(FsError::stale_entry("mount", cv_path, reason))
            }
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }

    pub fn mount(
        &self,
        mnt_id: Option<u32>,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        if mnt_opt.update {
            return self.update_mount(mnt_id, cv_path, ufs_path, mnt_opt);
        }
        self.add_mount(mnt_id, cv_path, ufs_path, mnt_opt)
    }

    /// Test-only: insert a mount directly into the in-memory index, bypassing
    /// Raft propose. PD module callers MUST go through `mount()` so the entry
    /// reaches all replicas. This method is `#[cfg(test)]` to prevent misuse
    /// (P3.4 from §15).
    #[cfg(test)]
    pub fn unprotected_add_mount(&self, info: MountInfo) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        index.insert(info);
        Ok(())
    }

    pub fn umount(&self, cv_path: &str) -> FsResult<()> {
        let mount_id = {
            let index = self.index.read().unwrap();
            let info = index
                .get_by_cv_path(cv_path)
                .ok_or_else(|| FsError::common(format!("failed found {} to umount", cv_path)))?;
            info.mount_id
        };
        let outcome = self
            .journal_client
            .propose_as_leader_with_result(PdEntry::Unmount(mount_id))?;
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => {
                Err(FsError::stale_entry("unmount", cv_path.to_string(), reason))
            }
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }

    pub fn unmount_by_id(&self, id: u32) -> FsResult<()> {
        let info = self.get_mount_info_by_id(id)?;
        self.umount(&info.cv_path)
    }

    /// Test-only: remove a mount directly from the in-memory index, bypassing
    /// Raft propose. PD module callers MUST go through `umount()`. This method
    /// is `#[cfg(test)]` to prevent misuse (P3.4 from §15).
    #[cfg(test)]
    pub fn unprotected_umount_by_id(&self, id: u32) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        match index.remove(id) {
            Some(_) => Ok(()),
            None => Err(FsError::common(format!("failed found {} entry", id))),
        }
    }

    pub fn get_mount_info(&self, path: &Path) -> FsResult<Option<Arc<MountInfo>>> {
        let list = path.get_possible_mounts();
        let is_cv = path.is_cv();
        let index = self.index.read().unwrap();

        for mnt in list {
            let opt = if is_cv {
                index.get_by_cv_path(&mnt)
            } else {
                index.get_by_ufs_path(&mnt)
            };
            if opt.is_some() {
                return Ok(opt);
            }
        }
        Ok(None)
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<Arc<MountInfo>>> {
        let index = self.index.read().unwrap();
        Ok(index.get_all())
    }

    pub fn get_mount_info_by_id(&self, mount_id: u32) -> FsResult<Arc<MountInfo>> {
        let index = self.index.read().unwrap();
        index
            .get_by_id(mount_id)
            .ok_or_else(|| FsError::common(format!("failed found {} entry", mount_id)))
    }

    /// Monotonic mount version (incremented on each mount/unmount apply).
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }
}

// =============================================================================
// REGRESSION-BASELINE tests (P0.4 from docs/pd-raft-consistency.md §15).
//
// These tests document the CURRENT bug behavior in mount apply paths:
//   - apply_mount does NOT re-check prefix conflicts; two add_mount calls
//     that pass propose-time check before either applies can both commit
//     and corrupt the index (P3.2 will fix).
//   - MountTableIndex::insert overwrites a same-ID entry without removing
//     its old cv/ufs reverse mappings (P3.3 will fix).
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::journal;
    use crate::pd::store::memory_kv_engine::MemoryKvEngine;
    use crate::pd::store::KvStore;
    use curvine_common::conf::JournalConf;
    use curvine_common::raft::RaftClient;
    use curvine_common::state::{MountInfo, MountOptions};

    fn test_mount_manager() -> MountManager {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
        let journal_conf = JournalConf::default();
        let rt = journal_conf.create_runtime();
        let raft = RaftClient::from_conf(rt, &journal_conf);
        let jc = Arc::new(journal::Client::new(raft));
        MountManager::new(store, jc)
    }

    fn build_mount(mount_id: u32, cv_path: &str, ufs_path: &str) -> MountInfo {
        let opts = MountOptions::builder().build();
        opts.to_info(mount_id, cv_path, ufs_path)
    }

    /// REGRESSION (post-P3.2): apply_mount re-runs prefix conflict check; the
    /// second of two concurrently-passing-propose-time entries is rejected
    /// with `SkippedStale` and the index never holds prefix-conflicting mounts.
    ///
    /// Pre-P3.2 (now removed): both entries were inserted, leaving a corrupt
    /// mount table.
    #[test]
    fn apply_mount_recheck_prefix_conflict() {
        let mgr = test_mount_manager();
        let info_a = build_mount(1, "/data", "/ufs/data");
        let info_b = build_mount(2, "/data/sub", "/ufs/data2");

        // First applies cleanly.
        let outcome_a = mgr.apply_mount(info_a).unwrap();
        assert_eq!(outcome_a, ApplyOutcome::Applied);
        // Second is rejected by the apply-side prefix conflict check.
        let outcome_b = mgr.apply_mount(info_b).unwrap();
        assert!(
            matches!(outcome_b, ApplyOutcome::SkippedStale { .. }),
            "expected SkippedStale on prefix conflict, got {:?}",
            outcome_b
        );

        let table = mgr.get_mount_table().unwrap();
        assert_eq!(table.len(), 1, "only path A survives");
        assert_eq!(table[0].cv_path, "/data");
    }

    /// REGRESSION (post-P3.3): MountTableIndex::insert now removes prior
    /// cv_path / ufs_path reverse mappings when a same mount_id is replaced.
    /// Pre-P3.3 (now removed): the old reverse mappings stayed, so lookup by
    /// the old paths still resolved to mount_id=N but pointed at the NEW
    /// MountInfo (an inconsistent state).
    #[test]
    fn mount_index_insert_cleans_old_reverse_mappings() {
        let mgr = test_mount_manager();

        let original = build_mount(7, "/old/cv", "/old/ufs");
        let replacement = build_mount(7, "/new/cv", "/new/ufs");

        mgr.apply_mount(original).unwrap();
        // Sanity: original is reachable by old paths.
        let by_cv = mgr
            .get_mount_info(&curvine_common::fs::Path::from_str("/old/cv").unwrap())
            .unwrap();
        assert!(
            by_cv.is_some(),
            "original should be reachable by old cv path"
        );

        // Re-insert same mount_id with completely different paths.
        mgr.apply_mount(replacement).unwrap();

        // Post-P3.3: lookup by old paths returns None (clean reverse mappings).
        let stale_by_cv = mgr
            .get_mount_info(&curvine_common::fs::Path::from_str("/old/cv").unwrap())
            .unwrap();
        assert!(
            stale_by_cv.is_none(),
            "post-P3.3: old cv path should no longer resolve"
        );

        // New paths resolve correctly to the replacement MountInfo.
        let by_id = mgr.get_mount_info_by_id(7).unwrap();
        assert_eq!(by_id.cv_path, "/new/cv");
        assert_eq!(by_id.ufs_path, "/new/ufs");
        let by_new_cv = mgr
            .get_mount_info(&curvine_common::fs::Path::from_str("/new/cv").unwrap())
            .unwrap()
            .expect("new cv path resolves");
        assert_eq!(by_new_cv.cv_path, "/new/cv");
    }
}
