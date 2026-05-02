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
use curvine_common::state::NodeInfo;
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use std::sync::Arc;

const NS: &str = store::CF_META;
const NODE_INFO_PREFIX: u8 = store::PREFIX_NODE;

pub struct NodeStore {
    store: Arc<dyn KvStore>,
}

impl NodeStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    fn node_info_key(&self, node_id: u32) -> [u8; 5] {
        let mut k = [0u8; 5];
        k[0] = NODE_INFO_PREFIX;
        k[1..5].copy_from_slice(&node_id.to_be_bytes());
        k
    }

    pub fn put(&self, info: &NodeInfo) -> CommonResult<()> {
        let key = self.node_info_key(info.base.node_id);
        let value = Serde::serialize(info)?;
        self.store.put(NS, &key, &value)?;
        Ok(())
    }

    pub fn get(&self, node_id: u32) -> CommonResult<Option<NodeInfo>> {
        let key = self.node_info_key(node_id);
        match self.store.get(NS, &key)? {
            Some(data) => {
                let info: NodeInfo = Serde::deserialize(&data)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, node_id: u32) -> CommonResult<()> {
        let key = self.node_info_key(node_id);
        self.store.delete(NS, &key)?;
        Ok(())
    }

    pub fn list_all(&self) -> CommonResult<Vec<NodeInfo>> {
        let pairs = self.store.scan_prefix(NS, &[NODE_INFO_PREFIX])?;
        let mut nodes = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            let info: NodeInfo = Serde::deserialize(&value)?;
            nodes.push(info);
        }
        Ok(nodes)
    }
}
