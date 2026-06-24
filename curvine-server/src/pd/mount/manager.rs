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
use crate::pd::journal::entry::{MountAddEntry, MountEntry, MountUpdateEntry, UnMountEntry};
use crate::pd::journal::{self, ApplyOutcome, PdEntry};
use crate::pd::namespace::NamespaceManager;
use crate::pd::store::KvStore;
use curvine_common::fs::Path;
use curvine_common::state::{MountInfo, MountOptions, NamespaceId, INVALID_NAMESPACE_ID};
use curvine_common::{FsError, FsResult};
use log::{info, warn};
use orpc::common::LocalTime;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

pub struct MountManager {
    index: Arc<RwLock<MountTableIndex>>,
    store: Arc<MountStore>,
    journal_client: Arc<journal::Client>,
    namespace_manager: Arc<NamespaceManager>,
    version: AtomicU64,
    /// Committed mount-id counter. Advanced only by Raft apply.
    next_mount_id: AtomicU32,
    /// Leader-local reservation counter. Advanced by propose-time id allocation
    /// and never persisted directly. Gaps are acceptable when propose fails.
    reserved_next_mount_id: AtomicU32,
}

impl MountManager {
    pub fn new(
        store: Arc<dyn KvStore>,
        journal_client: Arc<journal::Client>,
        namespace_manager: Arc<NamespaceManager>,
    ) -> Self {
        let store = Arc::new(MountStore::new(store));
        Self {
            index: Arc::new(RwLock::new(MountTableIndex::new())),
            store,
            journal_client,
            namespace_manager,
            version: AtomicU64::new(0),
            next_mount_id: AtomicU32::new(1),
            reserved_next_mount_id: AtomicU32::new(1),
        }
    }

    pub fn restore(&self) -> FsResult<()> {
        let mounts = self.store.list_all_mounts()?;
        let mut index = self.index.write().unwrap();
        index.clear();
        let mut max_mount_id = 0u32;
        let mut max_mount_version = 0u64;
        for mnt in mounts {
            info!(
                "Restore mount: {} -> {} (id={})",
                mnt.cv_path, mnt.ufs_path, mnt.mount_id
            );
            max_mount_id = max_mount_id.max(mnt.mount_id);
            max_mount_version = max_mount_version.max(mnt.version);
            index.insert(mnt);
        }
        drop(index);

        // Restore derives safe runtime counters from persisted counters and rows.
        let next_mount_id = self
            .store
            .get_next_mount_id()?
            .max(max_mount_id.saturating_add(1))
            .max(1);
        let version = self.store.get_version()?.max(max_mount_version);
        self.next_mount_id.store(next_mount_id, Ordering::Relaxed);
        self.reserved_next_mount_id
            .store(next_mount_id, Ordering::Relaxed);
        self.version.store(version, Ordering::Relaxed);
        Ok(())
    }

    fn validate_namespace_id(&self, namespace_id: NamespaceId) -> FsResult<()> {
        if namespace_id == INVALID_NAMESPACE_ID {
            return Err(FsError::invalid_argument(
                "mount namespace_id must be specified",
            ));
        }
        if self.namespace_manager.get_namespace(namespace_id).is_none() {
            return Err(FsError::not_found(format!(
                "namespace {} not found",
                namespace_id
            )));
        }
        Ok(())
    }

    fn resolve_namespace_name(&self, mnt_opt: &MountOptions) -> FsResult<NamespaceId> {
        let name = mnt_opt
            .namespace_name
            .as_deref()
            .ok_or_else(|| FsError::invalid_argument("mount namespace name must be specified"))?;
        if name.trim().is_empty() {
            return Err(FsError::invalid_argument(
                "mount namespace name must not be empty",
            ));
        }
        let namespace = self
            .namespace_manager
            .get_namespace_by_name(name)
            .ok_or_else(|| FsError::not_found(format!("namespace {} not found", name)))?;
        Ok(namespace.id)
    }

    fn next_version(&self) -> u64 {
        self.version.load(Ordering::Relaxed).saturating_add(1)
    }

    fn commit_runtime_version(&self, version: u64) {
        self.version.store(version, Ordering::Relaxed);
    }

    fn commit_runtime_next_mount_id(&self, next_mount_id: u32) {
        self.next_mount_id.store(next_mount_id, Ordering::Relaxed);
    }

    fn reserve_next_mount_id(&self, index: &MountTableIndex) -> FsResult<u32> {
        loop {
            let reserved = self.reserved_next_mount_id.load(Ordering::Acquire);
            let committed = self.next_mount_id.load(Ordering::Acquire);
            let mut candidate = reserved.max(committed).max(1);
            while index.get_by_id(candidate).is_some() {
                candidate = next_mount_id_after(candidate)?;
            }
            let next = next_mount_id_after(candidate)?;
            if self
                .reserved_next_mount_id
                .compare_exchange(reserved, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(candidate);
            }
        }
    }

    fn next_mount_id_to_commit(&self, mount_id: u32) -> FsResult<u32> {
        Ok(self
            .next_mount_id
            .load(Ordering::Relaxed)
            .max(next_mount_id_after(mount_id)?)
            .max(1))
    }

    fn validate_insert_paths(
        &self,
        index: &MountTableIndex,
        info: &MountInfo,
        exclude_id: Option<u32>,
    ) -> FsResult<Option<ApplyOutcome>> {
        if let Some(dup) = index.get_by_ufs_path(&info.ufs_path) {
            if Some(dup.mount_id) != exclude_id {
                return Ok(Some(ApplyOutcome::stale(format!(
                    "ufs_path {} already mounted",
                    info.ufs_path
                ))));
            }
        }
        if let Some(dup) = index.get_by_cv_path(&info.cv_path) {
            if Some(dup.mount_id) != exclude_id {
                return Ok(Some(ApplyOutcome::stale(format!(
                    "cv_path {} already mounted",
                    info.cv_path
                ))));
            }
        }
        if let Err(e) = index.check_conflict_excluding(&info.cv_path, &info.ufs_path, exclude_id) {
            return Ok(Some(ApplyOutcome::stale(format!(
                "prefix conflict on cv_path={}, ufs_path={}: {}",
                info.cv_path, info.ufs_path, e
            ))));
        }
        Ok(None)
    }

    fn validate_add_request_paths(
        &self,
        index: &MountTableIndex,
        cv_path: &str,
        ufs_path: &str,
    ) -> FsResult<()> {
        if index.get_by_ufs_path(ufs_path).is_some() {
            return Err(FsError::mount_path_exists(ufs_path));
        }
        if index.get_by_cv_path(cv_path).is_some() {
            return Err(FsError::mount_path_exists(cv_path));
        }
        index.check_conflict(cv_path, ufs_path)
    }

    fn validate_update_request(&self, cv_path: &str) -> FsResult<Arc<MountInfo>> {
        let index = self.index.read().unwrap();
        index.get_by_cv_path(cv_path).ok_or_else(|| {
            FsError::not_found(format!(
                "update mode: mount point {} does not exist",
                cv_path
            ))
        })
    }

    fn validate_add_entry(
        &self,
        index: &MountTableIndex,
        info: &MountInfo,
    ) -> FsResult<Option<ApplyOutcome>> {
        if let Some(outcome) = self.validate_insert_paths(index, info, None)? {
            return Ok(Some(outcome));
        }
        if info.mount_id == 0 {
            return Ok(Some(ApplyOutcome::stale("mount_id must not be 0")));
        }
        if index.get_by_id(info.mount_id).is_some() {
            return Ok(Some(ApplyOutcome::stale(format!(
                "mount_id {} already exists",
                info.mount_id
            ))));
        }
        Ok(None)
    }

    fn validate_update_entry(
        &self,
        index: &MountTableIndex,
        expected_mount_id: u32,
        expected_cv_path: &str,
        expected_version: u64,
        info: &MountInfo,
    ) -> FsResult<Result<Arc<MountInfo>, ApplyOutcome>> {
        let old = match index.get_by_cv_path(expected_cv_path) {
            Some(old) => old,
            None => {
                return Ok(Err(ApplyOutcome::not_found(format!(
                    "update mount: cv_path {} does not exist",
                    expected_cv_path
                ))));
            }
        };
        if old.mount_id != expected_mount_id {
            return Ok(Err(ApplyOutcome::stale(format!(
                "update stale: cv_path {} points to mount_id {}, expected {}",
                expected_cv_path, old.mount_id, expected_mount_id
            ))));
        }
        if old.version != expected_version {
            return Ok(Err(ApplyOutcome::stale(format!(
                "update stale: mount_id {} version {}, expected {}",
                old.mount_id, old.version, expected_version
            ))));
        }
        if let Some(outcome) = self.validate_insert_paths(index, info, Some(old.mount_id))? {
            return Ok(Err(outcome));
        }
        if info.mount_id != expected_mount_id {
            return Ok(Err(ApplyOutcome::stale(format!(
                "update stale: mount_id change is not allowed, expected {}, actual {}",
                expected_mount_id, info.mount_id
            ))));
        }
        Ok(Ok(old))
    }

    /// Apply a mount entry from Raft.
    pub fn apply_mount(&self, entry: MountEntry) -> FsResult<ApplyOutcome> {
        match entry {
            MountEntry::Add(entry) => self.apply_mount_add(entry),
            MountEntry::Update(entry) => self.apply_mount_update(entry),
        }
    }

    fn apply_mount_add(&self, entry: MountAddEntry) -> FsResult<ApplyOutcome> {
        let MountAddEntry { mut info, .. } = entry;

        if let Err(e) = self.validate_namespace_id(info.namespace_id) {
            warn!(
                "Apply mount add skipped: invalid namespace cv_path={}, namespace={}, err={}",
                info.cv_path, info.namespace_id, e
            );
            return Ok(ApplyOutcome::stale(e.to_string()));
        }

        let mut index = self.index.write().unwrap();
        if let Some(outcome) = self.validate_add_entry(&index, &info)? {
            return Ok(outcome);
        }
        let next_mount_id = self.next_mount_id_to_commit(info.mount_id)?;

        let version = self.next_version();
        info.version = version;
        self.store.apply_add_mount(&info, version, next_mount_id)?;
        index.insert(info.clone());
        self.commit_runtime_next_mount_id(next_mount_id);
        self.commit_runtime_version(version);
        info!(
            "Apply mount add: {} -> {} (id={})",
            info.cv_path, info.ufs_path, info.mount_id
        );
        Ok(ApplyOutcome::Applied)
    }

    fn apply_mount_update(&self, entry: MountUpdateEntry) -> FsResult<ApplyOutcome> {
        let MountUpdateEntry {
            expected_mount_id,
            expected_cv_path,
            expected_version,
            mut info,
            ..
        } = entry;

        if let Err(e) = self.validate_namespace_id(info.namespace_id) {
            warn!(
                "Apply mount update skipped: invalid namespace cv_path={}, namespace={}, err={}",
                info.cv_path, info.namespace_id, e
            );
            return Ok(ApplyOutcome::stale(e.to_string()));
        }

        let mut index = self.index.write().unwrap();
        let old = match self.validate_update_entry(
            &index,
            expected_mount_id,
            &expected_cv_path,
            expected_version,
            &info,
        )? {
            Ok(old) => old,
            Err(outcome) => return Ok(outcome),
        };

        let version = self.next_version();
        info.version = version;
        self.store.apply_update_mount(&info, version)?;
        index.insert(info.clone());
        self.commit_runtime_version(version);
        info!(
            "Apply mount update: {} -> {} (id={}, old_id={})",
            info.cv_path, info.ufs_path, info.mount_id, old.mount_id
        );
        Ok(ApplyOutcome::Applied)
    }

    pub fn apply_unmount(&self, entry: UnMountEntry) -> FsResult<ApplyOutcome> {
        let mut index = self.index.write().unwrap();
        let info = match index.get_by_id(entry.id) {
            Some(i) => i,
            None => {
                warn!(
                    "Apply unmount skipped: mount_id={} not present in index",
                    entry.id
                );
                return Ok(ApplyOutcome::not_found(format!(
                    "mount_id {} not present",
                    entry.id
                )));
            }
        };
        if info.cv_path != entry.expected_cv_path {
            return Ok(ApplyOutcome::stale(format!(
                "unmount stale: mount_id {} points to {}, expected {}",
                entry.id, info.cv_path, entry.expected_cv_path
            )));
        }
        if info.version != entry.expected_version {
            return Ok(ApplyOutcome::stale(format!(
                "unmount stale: mount_id {} version {}, expected {}",
                entry.id, info.version, entry.expected_version
            )));
        }
        let version = self.next_version();
        self.store.apply_unmount(entry.id, version)?;
        index.remove(entry.id);
        self.commit_runtime_version(version);
        info!("Apply unmount: {} (id={})", info.cv_path, entry.id);
        Ok(ApplyOutcome::Applied)
    }

    fn add_mount(&self, cv_path: &str, ufs_path: &str, mnt_opt: &MountOptions) -> FsResult<()> {
        let namespace_id = self.resolve_namespace_name(mnt_opt)?;
        let mount_id = {
            let index = self.index.read().unwrap();
            self.validate_add_request_paths(&index, cv_path, ufs_path)?;
            self.reserve_next_mount_id(&index)?
        };
        let info = mnt_opt
            .clone()
            .to_info(mount_id, cv_path, ufs_path, namespace_id);
        let entry = MountEntry::Add(MountAddEntry {
            op_ms: LocalTime::mills(),
            info,
        });
        self.propose_mount(entry, cv_path.to_string(), "add_mount")
    }

    fn update_mount(&self, cv_path: &str, ufs_path: &str, mnt_opt: &MountOptions) -> FsResult<()> {
        let old = self.validate_update_request(cv_path)?;
        let namespace_id = self.resolve_namespace_name(mnt_opt)?;
        let info = mnt_opt
            .clone()
            .to_info(old.mount_id, cv_path, ufs_path, namespace_id);
        let entry = MountEntry::Update(MountUpdateEntry {
            op_ms: LocalTime::mills(),
            expected_mount_id: old.mount_id,
            expected_cv_path: cv_path.to_string(),
            expected_version: old.version,
            info,
        });
        self.propose_mount(entry, cv_path.to_string(), "update_mount")
    }

    /// Leader-fenced propose + ApplyOutcome translation.
    fn propose_mount(&self, entry: MountEntry, cv_path: String, kind: &str) -> FsResult<()> {
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

    pub fn mount(&self, cv_path: &str, ufs_path: &str, mnt_opt: &MountOptions) -> FsResult<()> {
        if mnt_opt.update {
            return self.update_mount(cv_path, ufs_path, mnt_opt);
        }
        self.add_mount(cv_path, ufs_path, mnt_opt)
    }

    pub fn umount(&self, cv_path: &str) -> FsResult<()> {
        let (mount_id, expected_version) = {
            let index = self.index.read().unwrap();
            let info = index
                .get_by_cv_path(cv_path)
                .ok_or_else(|| FsError::not_found(format!("mount point {} not found", cv_path)))?;
            (info.mount_id, info.version)
        };
        let entry = UnMountEntry {
            op_ms: LocalTime::mills(),
            id: mount_id,
            expected_cv_path: cv_path.to_string(),
            expected_version,
        };
        let outcome = self
            .journal_client
            .propose_as_leader_with_result(PdEntry::Unmount(entry))?;
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
            .ok_or_else(|| FsError::not_found(format!("mount_id {} not found", mount_id)))
    }

    /// Monotonic mount version (incremented on each mount/unmount apply).
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// Test-only: insert a mount directly into the in-memory index, bypassing
    /// Raft propose.
    #[cfg(test)]
    pub fn unprotected_add_mount(&self, info: MountInfo) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        index.insert(info);
        Ok(())
    }

    /// Test-only: remove a mount directly from the in-memory index, bypassing
    /// Raft propose.
    #[cfg(test)]
    pub fn unprotected_umount_by_id(&self, id: u32) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        match index.remove(id) {
            Some(_) => Ok(()),
            None => Err(FsError::not_found(format!("mount_id {} not found", id))),
        }
    }

    #[cfg(test)]
    fn next_mount_id(&self) -> u32 {
        self.next_mount_id.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn reserved_next_mount_id(&self) -> u32 {
        self.reserved_next_mount_id.load(Ordering::Relaxed)
    }
}

fn next_mount_id_after(mount_id: u32) -> FsResult<u32> {
    mount_id
        .checked_add(1)
        .ok_or_else(|| FsError::common("mount id exhausted: cannot allocate after u32::MAX"))
}

// Mount apply-path tests: id allocation, atomic replace, and conflict handling
// are all validated inside the single-threaded apply loop.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::namespace::NamespaceManager;
    use crate::pd::store::memory_kv_engine::MemoryKvEngine;
    use crate::pd::store::KvStore;
    use curvine_common::conf::JournalConf;
    use curvine_common::raft::RaftClient;
    use curvine_common::state::{MountInfo, MountOptions, NamespaceInfo};

    fn test_namespace_manager() -> Arc<NamespaceManager> {
        let namespace_manager = NamespaceManager::new_for_test();
        namespace_manager.test_insert_namespace(NamespaceInfo {
            id: 1,
            name: "default".to_string(),
            ..Default::default()
        });
        namespace_manager
    }

    fn test_mount_manager_with_store(store: Arc<dyn KvStore>) -> MountManager {
        let journal_conf = JournalConf::default();
        let raft = RaftClient::from_conf(journal_conf.create_runtime(), &journal_conf);
        let jc = Arc::new(journal::Client::new(raft));
        MountManager::new(store, jc, test_namespace_manager())
    }

    fn test_mount_manager() -> MountManager {
        test_mount_manager_with_store(Arc::new(MemoryKvEngine::new()))
    }

    fn build_mount(mount_id: u32, cv_path: &str, ufs_path: &str) -> MountInfo {
        let opts = MountOptions::builder().namespace_name("default").build();
        opts.to_info(mount_id, cv_path, ufs_path, 1)
    }

    fn insert_entry(info: MountInfo) -> MountEntry {
        MountEntry::Add(MountAddEntry { op_ms: 0, info })
    }

    fn update_entry(
        expected_mount_id: u32,
        expected_cv_path: &str,
        expected_version: u64,
        info: MountInfo,
    ) -> MountEntry {
        MountEntry::Update(MountUpdateEntry {
            op_ms: 0,
            expected_mount_id,
            expected_cv_path: expected_cv_path.to_string(),
            expected_version,
            info,
        })
    }

    fn unmount_entry_with_version(
        id: u32,
        expected_cv_path: &str,
        expected_version: u64,
    ) -> UnMountEntry {
        UnMountEntry {
            op_ms: 0,
            id,
            expected_cv_path: expected_cv_path.to_string(),
            expected_version,
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum OutcomeKind {
        Applied,
        Stale,
        NotFound,
    }

    fn assert_outcome(case_name: &str, actual: ApplyOutcome, expected: OutcomeKind) {
        match expected {
            OutcomeKind::Applied => assert_eq!(
                actual,
                ApplyOutcome::Applied,
                "case {} should be applied",
                case_name
            ),
            OutcomeKind::Stale => assert!(
                matches!(actual, ApplyOutcome::SkippedStale { .. }),
                "case {} should be stale, got {:?}",
                case_name,
                actual
            ),
            OutcomeKind::NotFound => assert!(
                matches!(actual, ApplyOutcome::NotFound { .. }),
                "case {} should be not-found, got {:?}",
                case_name,
                actual
            ),
        }
    }

    fn assert_mount_table(mgr: &MountManager, expected: &[(u32, &str, &str)]) {
        let mut actual: Vec<_> = mgr
            .get_mount_table()
            .unwrap()
            .into_iter()
            .map(|m| (m.mount_id, m.cv_path.clone(), m.ufs_path.clone()))
            .collect();
        actual.sort_by_key(|(id, _, _)| *id);

        let mut expected: Vec<_> = expected
            .iter()
            .map(|(id, cv, ufs)| (*id, (*cv).to_string(), (*ufs).to_string()))
            .collect();
        expected.sort_by_key(|(id, _, _)| *id);

        assert_eq!(actual, expected);
    }

    fn apply_setup(mgr: &MountManager, entries: Vec<MountEntry>) {
        for entry in entries {
            assert_eq!(mgr.apply_mount(entry).unwrap(), ApplyOutcome::Applied);
        }
    }

    #[test]
    fn apply_mount_add_cases() {
        struct AddCase {
            name: &'static str,
            setup: Vec<MountEntry>,
            entry: MountEntry,
            expected: OutcomeKind,
            expected_table: Vec<(u32, &'static str, &'static str)>,
            expected_next_id: Option<u32>,
        }

        let invalid_namespace_info = MountOptions::builder()
            .namespace_name("default")
            .build()
            .to_info(1, "/data", "/ufs/data", INVALID_NAMESPACE_ID);

        let cases = vec![
            AddCase {
                name: "success with explicit id advances next id",
                setup: vec![],
                entry: insert_entry(build_mount(9, "/explicit", "/ufs/explicit")),
                expected: OutcomeKind::Applied,
                expected_table: vec![(9, "/explicit", "/ufs/explicit")],
                expected_next_id: Some(10),
            },
            AddCase {
                name: "prefix conflict is rejected",
                setup: vec![insert_entry(build_mount(1, "/data", "/ufs/data"))],
                entry: insert_entry(build_mount(2, "/data/sub", "/ufs/data2")),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/data", "/ufs/data")],
                expected_next_id: Some(2),
            },
            AddCase {
                name: "missing namespace is rejected",
                setup: vec![],
                entry: insert_entry(invalid_namespace_info),
                expected: OutcomeKind::Stale,
                expected_table: vec![],
                expected_next_id: Some(1),
            },
            AddCase {
                name: "duplicate mount id is rejected",
                setup: vec![insert_entry(build_mount(1, "/a", "/ufs/a"))],
                entry: insert_entry(build_mount(1, "/b", "/ufs/b")),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/a", "/ufs/a")],
                expected_next_id: Some(2),
            },
            AddCase {
                name: "duplicate cv path is rejected",
                setup: vec![insert_entry(build_mount(1, "/a", "/ufs/a"))],
                entry: insert_entry(build_mount(2, "/a", "/ufs/b")),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/a", "/ufs/a")],
                expected_next_id: Some(2),
            },
            AddCase {
                name: "duplicate ufs path is rejected",
                setup: vec![insert_entry(build_mount(1, "/a", "/ufs/a"))],
                entry: insert_entry(build_mount(2, "/b", "/ufs/a")),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/a", "/ufs/a")],
                expected_next_id: Some(2),
            },
        ];

        for case in cases {
            let mgr = test_mount_manager();
            apply_setup(&mgr, case.setup);

            let outcome = mgr.apply_mount(case.entry).unwrap();

            assert_outcome(case.name, outcome, case.expected);
            assert_mount_table(&mgr, &case.expected_table);
            if let Some(next_id) = case.expected_next_id {
                assert_eq!(mgr.next_mount_id(), next_id, "case {}", case.name);
                assert_eq!(
                    mgr.store.get_next_mount_id().unwrap(),
                    next_id,
                    "case {}",
                    case.name
                );
            }
        }
    }

    #[test]
    fn reserve_mount_id_cases() {
        struct ReserveCase {
            name: &'static str,
            setup: Vec<MountEntry>,
            reserve_count: usize,
            expected_reserved_ids: Vec<u32>,
            expected_table: Vec<(u32, &'static str, &'static str)>,
            expected_committed_next_id: u32,
            expected_reserved_next_id: u32,
        }

        let cases = vec![
            ReserveCase {
                name: "two reservations are monotonic",
                setup: vec![],
                reserve_count: 2,
                expected_reserved_ids: vec![1, 2],
                expected_table: vec![],
                expected_committed_next_id: 1,
                expected_reserved_next_id: 3,
            },
            ReserveCase {
                name: "reservation skips existing explicit id",
                setup: vec![insert_entry(build_mount(1, "/a", "/ufs/a"))],
                reserve_count: 1,
                expected_reserved_ids: vec![2],
                expected_table: vec![(1, "/a", "/ufs/a")],
                expected_committed_next_id: 2,
                expected_reserved_next_id: 3,
            },
        ];

        for case in cases {
            let mgr = test_mount_manager();
            apply_setup(&mgr, case.setup);

            let mut reserved_ids = Vec::new();
            for _ in 0..case.reserve_count {
                let index = mgr.index.read().unwrap();
                reserved_ids.push(mgr.reserve_next_mount_id(&index).unwrap());
            }

            assert_eq!(
                reserved_ids, case.expected_reserved_ids,
                "case {}",
                case.name
            );
            assert_mount_table(&mgr, &case.expected_table);
            assert_eq!(
                mgr.next_mount_id(),
                case.expected_committed_next_id,
                "case {}",
                case.name
            );
            assert_eq!(
                mgr.reserved_next_mount_id(),
                case.expected_reserved_next_id,
                "case {}",
                case.name
            );
        }
    }

    #[test]
    fn apply_mount_update_cases() {
        struct UpdateCase {
            name: &'static str,
            setup: Vec<MountEntry>,
            before: Vec<MountEntry>,
            entry: MountEntry,
            expected: OutcomeKind,
            expected_table: Vec<(u32, &'static str, &'static str)>,
            expected_next_id: Option<u32>,
        }

        let cases = vec![
            UpdateCase {
                name: "update success swaps paths in place",
                setup: vec![insert_entry(build_mount(7, "/cv", "/ufs/old"))],
                before: vec![],
                entry: update_entry(7, "/cv", 1, build_mount(7, "/cv", "/ufs/new")),
                expected: OutcomeKind::Applied,
                expected_table: vec![(7, "/cv", "/ufs/new")],
                expected_next_id: Some(8),
            },
            UpdateCase {
                name: "conflicting replace keeps old mount",
                setup: vec![
                    insert_entry(build_mount(1, "/data", "/ufs/data")),
                    insert_entry(build_mount(2, "/other", "/ufs/other")),
                ],
                before: vec![],
                entry: update_entry(1, "/data", 1, build_mount(1, "/data", "/ufs/other")),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/data", "/ufs/data"), (2, "/other", "/ufs/other")],
                expected_next_id: Some(3),
            },
            UpdateCase {
                name: "stale version is rejected",
                setup: vec![insert_entry(build_mount(1, "/a", "/ufs/a"))],
                before: vec![update_entry(1, "/a", 1, build_mount(1, "/a", "/ufs/a2"))],
                entry: update_entry(1, "/a", 1, build_mount(1, "/a", "/ufs/a3")),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/a", "/ufs/a2")],
                expected_next_id: Some(2),
            },
            UpdateCase {
                name: "stale mount id is rejected",
                setup: vec![insert_entry(build_mount(1, "/a", "/ufs/a"))],
                before: vec![],
                entry: update_entry(2, "/a", 1, build_mount(1, "/a", "/ufs/a2")),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/a", "/ufs/a")],
                expected_next_id: Some(2),
            },
            UpdateCase {
                name: "missing cv path is not found",
                setup: vec![],
                before: vec![],
                entry: update_entry(1, "/missing", 1, build_mount(1, "/missing", "/ufs/missing")),
                expected: OutcomeKind::NotFound,
                expected_table: vec![],
                expected_next_id: Some(1),
            },
            UpdateCase {
                name: "replacement mount id collision is rejected",
                setup: vec![
                    insert_entry(build_mount(1, "/a", "/ufs/a")),
                    insert_entry(build_mount(2, "/b", "/ufs/b")),
                ],
                before: vec![],
                entry: update_entry(1, "/a", 1, build_mount(2, "/a", "/ufs/a2")),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/a", "/ufs/a"), (2, "/b", "/ufs/b")],
                expected_next_id: Some(3),
            },
        ];

        for case in cases {
            let mgr = test_mount_manager();
            apply_setup(&mgr, case.setup);
            apply_setup(&mgr, case.before);

            let outcome = mgr.apply_mount(case.entry).unwrap();

            assert_outcome(case.name, outcome, case.expected);
            assert_mount_table(&mgr, &case.expected_table);
            if let Some(next_id) = case.expected_next_id {
                assert_eq!(mgr.next_mount_id(), next_id, "case {}", case.name);
                assert_eq!(
                    mgr.store.get_next_mount_id().unwrap(),
                    next_id,
                    "case {}",
                    case.name
                );
            }
        }
    }

    #[test]
    fn apply_unmount_cases() {
        struct UnmountCase {
            name: &'static str,
            setup: Vec<MountEntry>,
            before: Vec<MountEntry>,
            entry: UnMountEntry,
            expected: OutcomeKind,
            expected_table: Vec<(u32, &'static str, &'static str)>,
        }

        let cases = vec![
            UnmountCase {
                name: "unmount success removes mount",
                setup: vec![insert_entry(build_mount(1, "/a", "/ufs/a"))],
                before: vec![],
                entry: unmount_entry_with_version(1, "/a", 1),
                expected: OutcomeKind::Applied,
                expected_table: vec![],
            },
            UnmountCase {
                name: "stale cv path is rejected",
                setup: vec![insert_entry(build_mount(1, "/a", "/ufs/a"))],
                before: vec![update_entry(1, "/a", 1, build_mount(1, "/b", "/ufs/b"))],
                entry: unmount_entry_with_version(1, "/a", 1),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/b", "/ufs/b")],
            },
            UnmountCase {
                name: "stale version is rejected",
                setup: vec![insert_entry(build_mount(1, "/a", "/ufs/a"))],
                before: vec![update_entry(1, "/a", 1, build_mount(1, "/a", "/ufs/a2"))],
                entry: unmount_entry_with_version(1, "/a", 1),
                expected: OutcomeKind::Stale,
                expected_table: vec![(1, "/a", "/ufs/a2")],
            },
            UnmountCase {
                name: "missing mount id is not found",
                setup: vec![],
                before: vec![],
                entry: unmount_entry_with_version(99, "/missing", 1),
                expected: OutcomeKind::NotFound,
                expected_table: vec![],
            },
        ];

        for case in cases {
            let mgr = test_mount_manager();
            apply_setup(&mgr, case.setup);
            apply_setup(&mgr, case.before);

            let outcome = mgr.apply_unmount(case.entry).unwrap();

            assert_outcome(case.name, outcome, case.expected);
            assert_mount_table(&mgr, &case.expected_table);
        }
    }

    #[test]
    fn restore_cases() {
        enum RestoreCase {
            ClearStaleRuntimeIndex,
            RepairVersionFromMountRows,
            RepairNextMountIdFromMountRows,
        }

        let cases = vec![
            (
                "clear stale runtime index",
                RestoreCase::ClearStaleRuntimeIndex,
            ),
            (
                "repair version from mount rows",
                RestoreCase::RepairVersionFromMountRows,
            ),
            (
                "repair next mount id from mount rows",
                RestoreCase::RepairNextMountIdFromMountRows,
            ),
        ];

        for (name, case) in cases {
            match case {
                RestoreCase::ClearStaleRuntimeIndex => {
                    let mgr = test_mount_manager();
                    mgr.unprotected_add_mount(build_mount(1, "/stale", "/ufs/stale"))
                        .unwrap();
                    assert_mount_table(&mgr, &[(1, "/stale", "/ufs/stale")]);

                    mgr.restore().unwrap();

                    assert_mount_table(&mgr, &[]);
                }
                RestoreCase::RepairVersionFromMountRows => {
                    let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
                    let mgr = test_mount_manager_with_store(store.clone());
                    assert_eq!(
                        mgr.apply_mount(insert_entry(build_mount(1, "/a", "/ufs/a")))
                            .unwrap(),
                        ApplyOutcome::Applied,
                        "case {}",
                        name
                    );
                    assert_eq!(mgr.version(), 1, "case {}", name);
                    mgr.store.put_version(0).unwrap();

                    let restored = test_mount_manager_with_store(store);
                    restored.restore().unwrap();

                    assert_eq!(restored.version(), 1, "case {}", name);
                    assert_mount_table(&restored, &[(1, "/a", "/ufs/a")]);
                }
                RestoreCase::RepairNextMountIdFromMountRows => {
                    let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
                    let mgr = test_mount_manager_with_store(store.clone());
                    assert_eq!(
                        mgr.apply_mount(insert_entry(build_mount(9, "/a", "/ufs/a")))
                            .unwrap(),
                        ApplyOutcome::Applied,
                        "case {}",
                        name
                    );
                    assert_eq!(mgr.store.get_next_mount_id().unwrap(), 10, "case {}", name);
                    mgr.store.put_next_mount_id(1).unwrap();

                    let restored = test_mount_manager_with_store(store);
                    restored.restore().unwrap();

                    assert_eq!(restored.next_mount_id(), 10, "case {}", name);
                    assert_eq!(
                        restored.store.get_next_mount_id().unwrap(),
                        1,
                        "case {}",
                        name
                    );
                    assert_mount_table(&restored, &[(9, "/a", "/ufs/a")]);
                }
            }
        }
    }

    /// MountTableIndex::insert removes prior cv_path / ufs_path reverse mappings
    /// when a same mount_id is replaced in place.
    #[test]
    fn mount_index_insert_cleans_old_reverse_mappings() {
        let mut index = MountTableIndex::new();
        index.insert(build_mount(7, "/old/cv", "/old/ufs"));
        assert!(index.get_by_cv_path("/old/cv").is_some());

        // Re-insert same mount_id with completely different paths.
        index.insert(build_mount(7, "/new/cv", "/new/ufs"));

        // Old paths no longer resolve; new paths point at the replacement.
        assert!(index.get_by_cv_path("/old/cv").is_none());
        assert!(index.get_by_ufs_path("/old/ufs").is_none());
        let by_id = index.get_by_id(7).expect("id resolves");
        assert_eq!(by_id.cv_path, "/new/cv");
        assert_eq!(by_id.ufs_path, "/new/ufs");
    }
}
