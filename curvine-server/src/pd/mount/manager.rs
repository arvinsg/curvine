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
use crate::pd::journal::PdEntry;
use crate::pd::store::KvStore;
use curvine_common::fs::Path;
use curvine_common::raft::RaftClient;
use curvine_common::state::{MountInfo, MountOptions};
use curvine_common::utils::SerdeUtils as Serde;
use curvine_common::{FsError, FsResult};
use log::info;
use orpc::common::LocalTime;
use rand::Rng;
use std::sync::Arc;
use std::sync::RwLock;

pub struct MountManager {
    index: Arc<RwLock<MountTableIndex>>,
    store: Arc<MountStore>,
    raft_client: RaftClient,
}

impl MountManager {
    pub fn new(store: Arc<dyn KvStore>, raft_client: RaftClient) -> Self {
        let store = Arc::new(MountStore::new(store));
        Self {
            index: Arc::new(RwLock::new(MountTableIndex::new())),
            store,
            raft_client,
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
        Ok(())
    }

    pub fn apply_mount(&self, info: MountInfo) -> FsResult<()> {
        info!(
            "Apply mount: {} -> {} (id={})",
            info.cv_path, info.ufs_path, info.mount_id
        );
        self.store.put_mount(&info)?;
        let mut index = self.index.write().unwrap();
        index.insert(info);
        Ok(())
    }

    pub fn apply_unmount(&self, mount_id: u32) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        let info = match index.remove(mount_id) {
            Some(i) => i,
            None => return Ok(()),
        };
        drop(index);
        self.store.delete_mount(mount_id)?;
        info!("Apply unmount: {} (id={})", info.cv_path, mount_id);
        Ok(())
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

    fn propose(&self, entry: PdEntry) -> FsResult<()> {
        let data = Serde::serialize(&entry)?;
        self.raft_client.block_on_send_propose(data)?;
        Ok(())
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
        self.propose(PdEntry::Mount(MountEntry {
            op_ms: LocalTime::mills(),
            info,
        }))
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
        self.propose(PdEntry::Mount(MountEntry {
            op_ms: LocalTime::mills(),
            info,
        }))
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
        self.propose(PdEntry::Unmount(mount_id))
    }

    pub fn unmount_by_id(&self, id: u32) -> FsResult<()> {
        let info = self.get_mount_info_by_id(id)?;
        self.umount(&info.cv_path)
    }

    pub fn unprotected_umount_by_id(&self, id: u32) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        match index.remove(id) {
            Some(_) => Ok(()),
            None => Err(FsError::common(format!("failed found {} entry", id))),
        }
    }

    pub fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        let list = path.get_possible_mounts();
        let is_cv = path.is_cv();
        let index = self.index.read().unwrap();

        for mnt in list {
            let opt = if is_cv {
                index.get_by_cv_path(&mnt)
            } else {
                index.get_by_ufs_path(&mnt)
            };
            if let Some(info) = opt {
                return Ok(Some(info.clone()));
            }
        }
        Ok(None)
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        let index = self.index.read().unwrap();
        Ok(index.get_all())
    }

    pub fn get_mount_info_by_id(&self, mount_id: u32) -> FsResult<MountInfo> {
        let index = self.index.read().unwrap();
        index
            .get_by_id(mount_id)
            .cloned()
            .ok_or_else(|| FsError::common(format!("failed found {} entry", mount_id)))
    }
}
