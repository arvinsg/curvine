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
use curvine_common::state::{BgId, BlockGroupInfo};
use curvine_common::utils::SerdeUtils as Serde;
use orpc::{error::StringError, CommonResult};
use std::sync::Arc;

const NS: &str = store::CF_DATA;
const BG_INFO_PREFIX: u8 = store::PREFIX_BG_INFO;
const BG_NEXT_ID_KEY: &[u8] = &[store::PREFIX_BG_NEXT_ID];

pub struct BGStore {
    store: Arc<dyn KvStore>,
}

impl BGStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    fn bg_info_key(&self, bg_id: BgId) -> [u8; 9] {
        let mut k = [0u8; 9];
        k[0] = BG_INFO_PREFIX;
        k[1..9].copy_from_slice(&bg_id.to_be_bytes());
        k
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

    pub fn bg_put_op(&self, info: &BlockGroupInfo) -> CommonResult<KvWrite> {
        Ok(self.put_op(
            self.bg_info_key(info.bg_id).to_vec(),
            Serde::serialize(info)?,
        ))
    }

    pub fn bg_delete_op(&self, bg_id: BgId) -> KvWrite {
        self.delete_op(self.bg_info_key(bg_id).to_vec())
    }

    pub fn next_bg_id_op(&self, next_id: BgId) -> CommonResult<KvWrite> {
        Ok(self.put_op(BG_NEXT_ID_KEY.to_vec(), Serde::serialize(&next_id)?))
    }

    pub fn write_batch(&self, ops: Vec<KvWrite>) -> CommonResult<()> {
        self.store.write_batch(ops)
    }

    pub fn put(&self, info: &BlockGroupInfo) -> CommonResult<()> {
        let key = self.bg_info_key(info.bg_id);
        let value = Serde::serialize(info)?;
        self.store.put(NS, &key, &value)?;
        Ok(())
    }

    pub fn get(&self, bg_id: BgId) -> CommonResult<Option<BlockGroupInfo>> {
        let key = self.bg_info_key(bg_id);
        match self.store.get(NS, &key)? {
            Some(data) => {
                let info: BlockGroupInfo = Serde::deserialize(&data)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, bg_id: BgId) -> CommonResult<()> {
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

    pub fn get_next_bg_id(&self) -> CommonResult<BgId> {
        match self.store.get(NS, BG_NEXT_ID_KEY)? {
            Some(data) => {
                let id: BgId = Serde::deserialize(&data)?;
                Ok(id)
            }
            None => Ok(1),
        }
    }

    pub fn set_next_bg_id(&self, next_id: BgId) -> CommonResult<()> {
        let current = self.get_next_bg_id()?;
        if next_id < current {
            return Err(StringError::from(format!(
                "next_bg_id rollback rejected: current={}, next={}",
                current, next_id
            ))
            .into());
        }
        if next_id == current {
            return Ok(());
        }
        let value = Serde::serialize(&next_id)?;
        self.store.put(NS, BG_NEXT_ID_KEY, &value)?;
        Ok(())
    }
}
