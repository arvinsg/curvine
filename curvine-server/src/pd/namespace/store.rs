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
use curvine_common::state::{NamespaceId, NamespaceInfo};
use curvine_common::utils::SerdeUtils as Serde;
use orpc::{error::StringError, CommonResult};
use std::sync::Arc;

const NS: &str = store::CF_META;
const NAMESPACE_PREFIX: u8 = store::PREFIX_NAMESPACE;
const NEXT_NAMESPACE_ID_KEY: &[u8] = &[store::PREFIX_NAMESPACE_NEXT_ID];

pub struct NamespaceStore {
    store: Arc<dyn KvStore>,
}

impl NamespaceStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    fn namespace_key(id: NamespaceId) -> [u8; 3] {
        let mut key = [0u8; 3];
        key[0] = NAMESPACE_PREFIX;
        key[1..3].copy_from_slice(&id.to_be_bytes());
        key
    }

    fn put_op(&self, key: Vec<u8>, value: Vec<u8>) -> KvWrite {
        KvWrite::Put {
            ns: NS.to_string(),
            key,
            value,
        }
    }

    pub fn namespace_put_op(&self, info: &NamespaceInfo) -> CommonResult<KvWrite> {
        Ok(self.put_op(
            Self::namespace_key(info.id).to_vec(),
            Serde::serialize(info)?,
        ))
    }

    pub fn next_namespace_id_op(&self, next_id: NamespaceId) -> CommonResult<KvWrite> {
        Ok(self.put_op(NEXT_NAMESPACE_ID_KEY.to_vec(), Serde::serialize(&next_id)?))
    }

    /// Atomically commit a batch of KvWrites.
    pub fn commit_batch(&self, ops: Vec<KvWrite>) -> CommonResult<()> {
        self.store.write_batch(ops)
    }

    pub fn get_namespace(&self, id: NamespaceId) -> CommonResult<Option<NamespaceInfo>> {
        let key = Self::namespace_key(id);
        match self.store.get(NS, &key)? {
            Some(data) => Ok(Some(Serde::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    pub fn list_namespaces(&self) -> CommonResult<Vec<NamespaceInfo>> {
        let pairs = self.store.scan_prefix(NS, &[NAMESPACE_PREFIX])?;
        let mut out = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            out.push(Serde::deserialize(&value)?);
        }
        Ok(out)
    }

    pub fn get_next_namespace_id(&self) -> CommonResult<NamespaceId> {
        match self.store.get(NS, NEXT_NAMESPACE_ID_KEY)? {
            Some(data) => Ok(Serde::deserialize(&data)?),
            None => Ok(1),
        }
    }

    pub fn set_next_namespace_id(&self, next_id: NamespaceId) -> CommonResult<()> {
        let current = self.get_next_namespace_id()?;
        if next_id < current {
            return Err(StringError::from(format!(
                "next_namespace_id rollback rejected: current={}, next={}",
                current, next_id
            ))
            .into());
        }
        if next_id == current {
            return Ok(());
        }
        let value = Serde::serialize(&next_id)?;
        self.store.put(NS, NEXT_NAMESPACE_ID_KEY, &value)?;
        Ok(())
    }
}
