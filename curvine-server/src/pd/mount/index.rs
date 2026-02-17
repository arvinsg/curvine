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

use curvine_common::fs::Path;
use curvine_common::state::MountInfo;
use curvine_common::{FsError, FsResult};
use std::collections::HashMap;

pub struct MountTableIndex {
    ufs2mountid: HashMap<String, u32>,
    mountpath2id: HashMap<String, u32>,
    mountid2entry: HashMap<u32, MountInfo>,
}

impl MountTableIndex {
    pub fn new() -> Self {
        Self {
            ufs2mountid: HashMap::new(),
            mountpath2id: HashMap::new(),
            mountid2entry: HashMap::new(),
        }
    }

    pub fn insert(&mut self, info: MountInfo) {
        self.ufs2mountid
            .insert(info.ufs_path.clone(), info.mount_id);
        self.mountpath2id
            .insert(info.cv_path.clone(), info.mount_id);
        self.mountid2entry.insert(info.mount_id, info);
    }

    pub fn remove(&mut self, mount_id: u32) -> Option<MountInfo> {
        if let Some(entry) = self.mountid2entry.remove(&mount_id) {
            self.ufs2mountid.remove(&entry.ufs_path);
            self.mountpath2id.remove(&entry.cv_path);
            Some(entry)
        } else {
            None
        }
    }

    pub fn get_by_id(&self, mount_id: u32) -> Option<&MountInfo> {
        self.mountid2entry.get(&mount_id)
    }

    pub fn get_by_cv_path(&self, cv_path: &str) -> Option<&MountInfo> {
        self.mountpath2id
            .get(cv_path)
            .and_then(|id| self.mountid2entry.get(id))
    }

    pub fn get_by_ufs_path(&self, ufs_path: &str) -> Option<&MountInfo> {
        self.ufs2mountid
            .get(ufs_path)
            .and_then(|id| self.mountid2entry.get(id))
    }

    pub fn contains_id(&self, mount_id: u32) -> bool {
        self.mountid2entry.contains_key(&mount_id)
    }

    pub fn get_all(&self) -> Vec<MountInfo> {
        self.mountid2entry.values().cloned().collect()
    }

    pub fn check_conflict(&self, cv_path: &str, ufs_path: &str) -> FsResult<()> {
        for info in self.mountid2entry.values() {
            if Path::has_prefix(cv_path, &info.cv_path) {
                return Err(FsError::mount_path_conflict(format!(
                    "mount point {} is a prefix of {}",
                    info.cv_path, cv_path
                )));
            }
            if Path::has_prefix(&info.cv_path, cv_path) {
                return Err(FsError::mount_path_conflict(format!(
                    "mount point {} is a prefix of {}",
                    cv_path, info.cv_path
                )));
            }
            if Path::has_prefix(ufs_path, &info.ufs_path) {
                return Err(FsError::mount_path_conflict(format!(
                    "mount point {} is a prefix of {}",
                    info.ufs_path, ufs_path
                )));
            }
            if Path::has_prefix(&info.ufs_path, ufs_path) {
                return Err(FsError::mount_path_conflict(format!(
                    "mount point {} is a prefix of {}",
                    ufs_path, info.ufs_path
                )));
            }
        }
        Ok(())
    }
}
