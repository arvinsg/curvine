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

use crate::pd::store::{self, KvStore, KvWrite};
use curvine_common::state::MountInfo;
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use std::sync::Arc;

const NS: &str = store::CF_META;
const VERSION_PREFIX: u8 = store::PREFIX_MOUNT_VERSION;
const MOUNT_PREFIX: u8 = store::PREFIX_MOUNT;
const VERSION_KEY: [u8; 1] = [VERSION_PREFIX];
const NEXT_ID_KEY: [u8; 1] = [store::PREFIX_MOUNT_NEXT_ID];

pub struct MountStore {
    store: Arc<dyn KvStore>,
}

impl MountStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    pub fn get_version(&self) -> CommonResult<u64> {
        self.store
            .get(NS, &VERSION_KEY)?
            .map(|v| decode_u64("mount version", &v))
            .transpose()
            .map(|v| v.unwrap_or(0))
    }

    #[cfg(test)]
    pub fn put_version(&self, version: u64) -> CommonResult<()> {
        self.store.put(NS, &VERSION_KEY, &version.to_be_bytes())
    }

    pub fn get_next_mount_id(&self) -> CommonResult<u32> {
        self.store
            .get(NS, &NEXT_ID_KEY)?
            .map(|v| decode_u32("mount next-id", &v))
            .transpose()
            .map(|v| v.unwrap_or(1))
    }

    #[cfg(test)]
    pub fn put_next_mount_id(&self, next_id: u32) -> CommonResult<()> {
        self.store.put(NS, &NEXT_ID_KEY, &next_id.to_be_bytes())
    }

    pub fn apply_add_mount(
        &self,
        info: &MountInfo,
        version: u64,
        next_mount_id: u32,
    ) -> CommonResult<()> {
        self.store.write_batch(vec![
            self.next_id_op(next_mount_id),
            self.mount_put_op(info)?,
            self.version_op(version),
        ])
    }

    pub fn apply_update_mount(&self, info: &MountInfo, version: u64) -> CommonResult<()> {
        self.store
            .write_batch(vec![self.mount_put_op(info)?, self.version_op(version)])
    }

    pub fn apply_unmount(&self, mount_id: u32, version: u64) -> CommonResult<()> {
        self.store.write_batch(vec![
            self.mount_delete_op(mount_id),
            self.version_op(version),
        ])
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

    fn make_key(&self, mount_id: u32) -> [u8; 5] {
        let mut v = [0u8; 5];
        v[0] = MOUNT_PREFIX;
        v[1..5].copy_from_slice(&mount_id.to_be_bytes());
        v
    }

    fn put_op(&self, key: Vec<u8>, value: Vec<u8>) -> KvWrite {
        KvWrite::Put {
            ns: NS.to_string(),
            key,
            value,
        }
    }

    fn delete_op(&self, key: Vec<u8>) -> KvWrite {
        KvWrite::Delete {
            ns: NS.to_string(),
            key,
        }
    }

    fn version_op(&self, version: u64) -> KvWrite {
        self.put_op(VERSION_KEY.to_vec(), version.to_be_bytes().to_vec())
    }

    fn next_id_op(&self, next_id: u32) -> KvWrite {
        self.put_op(NEXT_ID_KEY.to_vec(), next_id.to_be_bytes().to_vec())
    }

    fn mount_put_op(&self, info: &MountInfo) -> CommonResult<KvWrite> {
        let key = self.make_key(info.mount_id).to_vec();
        let value = Serde::serialize(info)?;
        Ok(self.put_op(key, value))
    }

    fn mount_delete_op(&self, mount_id: u32) -> KvWrite {
        self.delete_op(self.make_key(mount_id).to_vec())
    }
}

fn decode_u64(name: &str, bytes: &[u8]) -> CommonResult<u64> {
    if bytes.len() != 8 {
        return Err(format!(
            "invalid {} bytes: expected 8 bytes, got {}",
            name,
            bytes.len()
        )
        .into());
    }
    let mut array = [0u8; 8];
    array.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(array))
}

fn decode_u32(name: &str, bytes: &[u8]) -> CommonResult<u32> {
    if bytes.len() != 4 {
        return Err(format!(
            "invalid {} bytes: expected 4 bytes, got {}",
            name,
            bytes.len()
        )
        .into());
    }
    let mut array = [0u8; 4];
    array.copy_from_slice(bytes);
    Ok(u32::from_be_bytes(array))
}
