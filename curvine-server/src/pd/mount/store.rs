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

use crate::pd::store::{self, KvStore};
use curvine_common::state::MountInfo;
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use std::sync::Arc;

const NS: &str = store::CF_META;
const VERSION_PREFIX: u8 = store::PREFIX_MOUNT_VERSION;
const MOUNT_PREFIX: u8 = store::PREFIX_MOUNT;
const VERSION_KEY: [u8; 5] = [VERSION_PREFIX, 0, 0, 0, 0];

pub struct MountStore {
    store: Arc<dyn KvStore>,
}

impl MountStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    pub fn get_version(&self) -> CommonResult<u64> {
        match self.store.get(NS, &VERSION_KEY)? {
            Some(v) => {
                let bytes: [u8; 8] = v
                    .as_slice()
                    .try_into()
                    .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                        "invalid mount version bytes".into()
                    })?;
                Ok(u64::from_be_bytes(bytes))
            }
            None => Ok(0),
        }
    }

    pub fn put_version(&self, version: u64) -> CommonResult<()> {
        self.store.put(NS, &VERSION_KEY, &version.to_be_bytes())?;
        Ok(())
    }

    fn make_key(&self, mount_id: u32) -> [u8; 5] {
        let mut v = [0u8; 5];
        v[0] = MOUNT_PREFIX;
        v[1..5].copy_from_slice(&mount_id.to_be_bytes());
        v
    }

    pub fn put_mount(&self, info: &MountInfo) -> CommonResult<()> {
        let key = self.make_key(info.mount_id);
        let value = Serde::serialize(info)?;
        self.store.put(NS, &key, &value)?;
        Ok(())
    }

    pub fn delete_mount(&self, mount_id: u32) -> CommonResult<()> {
        let key = self.make_key(mount_id);
        self.store.delete(NS, &key)?;
        Ok(())
    }

    pub fn get_mount(&self, mount_id: u32) -> CommonResult<Option<MountInfo>> {
        let key = self.make_key(mount_id);
        match self.store.get(NS, &key)? {
            Some(v) => {
                let info: MountInfo = Serde::deserialize(&v)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    pub fn list_all_mounts(&self) -> CommonResult<Vec<MountInfo>> {
        let pairs = self.store.scan_prefix(NS, &[MOUNT_PREFIX])?;
        let mut vec = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            let info: MountInfo = Serde::deserialize(&value)?;
            vec.push(info);
        }
        Ok(vec)
    }
}
