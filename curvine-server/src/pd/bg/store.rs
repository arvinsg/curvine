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

use crate::pd::store::KvStore;
use curvine_common::state::BlockGroupInfo;
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use std::sync::Arc;

const NS: &str = "bg";
const BG_INFO_PREFIX: u8 = 0x01;
const BG_NEXT_ID_KEY: &[u8] = &[0x02];

pub struct BGStore {
    store: Arc<dyn KvStore>,
}

impl BGStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    fn bg_info_key(&self, bg_id: u32) -> [u8; 5] {
        let mut k = [0u8; 5];
        k[0] = BG_INFO_PREFIX;
        k[1..5].copy_from_slice(&bg_id.to_be_bytes());
        k
    }

    pub fn put(&self, info: &BlockGroupInfo) -> CommonResult<()> {
        let key = self.bg_info_key(info.bg_id);
        let value = Serde::serialize(info)?;
        self.store.put(NS, &key, &value)?;
        Ok(())
    }

    pub fn get(&self, bg_id: u32) -> CommonResult<Option<BlockGroupInfo>> {
        let key = self.bg_info_key(bg_id);
        match self.store.get(NS, &key)? {
            Some(data) => {
                let info: BlockGroupInfo = Serde::deserialize(&data)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, bg_id: u32) -> CommonResult<()> {
        let key = self.bg_info_key(bg_id);
        self.store.delete(NS, &key)?;
        Ok(())
    }

    pub fn list_all(&self) -> CommonResult<Vec<BlockGroupInfo>> {
        let pairs = self.store.scan_prefix(NS, &[BG_INFO_PREFIX])?;
        let mut bgs = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            let info: BlockGroupInfo = Serde::deserialize(&value)?;
            bgs.push(info);
        }
        Ok(bgs)
    }

    pub fn get_next_bg_id(&self) -> CommonResult<u32> {
        match self.store.get(NS, BG_NEXT_ID_KEY)? {
            Some(data) => {
                let id: u32 = Serde::deserialize(&data)?;
                Ok(id)
            }
            None => Ok(1),
        }
    }

    pub fn set_next_bg_id(&self, next_id: u32) -> CommonResult<()> {
        let value = Serde::serialize(&next_id)?;
        self.store.put(NS, BG_NEXT_ID_KEY, &value)?;
        Ok(())
    }
}
