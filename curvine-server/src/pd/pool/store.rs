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
use curvine_common::state::PoolInfo;
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use std::sync::Arc;

const NS: &str = store::CF_META;
const POOL_INFO_PREFIX: u8 = store::PREFIX_POOL;

pub struct PoolStore {
    store: Arc<dyn KvStore>,
}

impl PoolStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    fn pool_info_key(&self, pool_id: u16) -> [u8; 3] {
        let mut k = [0u8; 3];
        k[0] = POOL_INFO_PREFIX;
        k[1..3].copy_from_slice(&pool_id.to_be_bytes());
        k
    }

    pub fn put_pool(&self, info: &PoolInfo) -> CommonResult<()> {
        let key = self.pool_info_key(info.pool_id);
        let value = Serde::serialize(info)?;
        self.store.put(NS, &key, &value)?;
        Ok(())
    }

    pub fn get_pool(&self, pool_id: u16) -> CommonResult<Option<PoolInfo>> {
        let key = self.pool_info_key(pool_id);
        match self.store.get(NS, &key)? {
            Some(data) => {
                let info: PoolInfo = Serde::deserialize(&data)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    pub fn list_pools(&self) -> CommonResult<Vec<PoolInfo>> {
        let pairs = self.store.scan_prefix(NS, &[POOL_INFO_PREFIX])?;
        let mut pools = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            let info: PoolInfo = Serde::deserialize(&value)?;
            pools.push(info);
        }
        Ok(pools)
    }
}
