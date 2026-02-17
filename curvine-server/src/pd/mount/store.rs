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

use curvine_common::rocksdb::{DBEngine, RocksUtils};
use curvine_common::state::MountInfo;
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use std::sync::{Arc, RwLock};

const MOUNT_CF: &str = "mount";
const MOUNT_PREFIX: u8 = 0x02;

// Persistent storage for mount table in DB.
pub struct MountStore {
    db: Arc<RwLock<DBEngine>>,
}

impl MountStore {
    pub fn new(db: Arc<RwLock<DBEngine>>) -> Self {
        Self { db }
    }

    fn make_key(&self, mount_id: u32) -> [u8; 5] {
        RocksUtils::u8_u32_to_bytes(MOUNT_PREFIX, mount_id)
    }

    pub fn put_mount(&self, info: &MountInfo) -> CommonResult<()> {
        let key = self.make_key(info.mount_id);
        let value = Serde::serialize(info)?;
        let db = self.db.write().unwrap();
        db.put_cf(MOUNT_CF, key, value)?;
        Ok(())
    }

    pub fn delete_mount(&self, mount_id: u32) -> CommonResult<()> {
        let key = self.make_key(mount_id);
        let db = self.db.write().unwrap();
        db.delete_cf(MOUNT_CF, key)?;
        Ok(())
    }

    pub fn get_mount(&self, mount_id: u32) -> CommonResult<Option<MountInfo>> {
        let key = self.make_key(mount_id);
        let db = self.db.read().unwrap();
        let opt = db.get_cf(MOUNT_CF, key)?;
        match opt {
            Some(v) => {
                let info: MountInfo = Serde::deserialize(&v)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    pub fn list_all_mounts(&self) -> CommonResult<Vec<MountInfo>> {
        let db = self.db.read().unwrap();
        let iter = db.prefix_scan(MOUNT_CF, [MOUNT_PREFIX])?;
        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let (_key, value): curvine_common::rocksdb::KVBytes = item?;
            let info: MountInfo = Serde::deserialize(&value)?;
            vec.push(info);
        }
        Ok(vec)
    }
}
