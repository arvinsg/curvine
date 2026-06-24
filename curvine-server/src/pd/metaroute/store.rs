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
use curvine_common::state::PathRouteEntry;
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use std::sync::Arc;

/// Namespace for path route table (Federation Static).
const NS: &str = store::CF_META;
const ROUTE_PREFIX: u8 = store::PREFIX_ROUTE;
const VERSION_KEY: [u8; 1] = [store::PREFIX_ROUTE_VERSION];

/// Store for meta route data.
pub struct MetaRouteStore {
    store: Arc<dyn KvStore>,
}

impl MetaRouteStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    fn route_key(path: &str) -> Vec<u8> {
        let mut buf = vec![ROUTE_PREFIX];
        buf.extend_from_slice(path.as_bytes());
        buf
    }

    pub fn apply_add_route(&self, entry: &PathRouteEntry, version: u64) -> CommonResult<()> {
        self.store.write_batch(vec![
            self.route_put_op(entry)?,
            self.version_op(version),
        ])
    }

    pub fn apply_remove_route(&self, path: &str, version: u64) -> CommonResult<()> {
        self.store.write_batch(vec![
            self.route_delete_op(path),
            self.version_op(version),
        ])
    }

    pub fn get_path_route(&self, path: &str) -> CommonResult<Option<PathRouteEntry>> {
        let key = Self::route_key(path);
        match self.store.get(NS, &key)? {
            Some(data) => {
                let entry: PathRouteEntry = Serde::deserialize(&data)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    pub fn list_path_routes(&self) -> CommonResult<Vec<PathRouteEntry>> {
        let pairs = self.store.scan_prefix(NS, &[ROUTE_PREFIX])?;
        let mut routes = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            let entry: PathRouteEntry = Serde::deserialize(&value)?;
            routes.push(entry);
        }
        Ok(routes)
    }

    /// Persisted path route table version; must be monotonically increasing for client change detection.
    pub fn get_path_route_version(&self) -> CommonResult<u64> {
        self.store
            .get(NS, &VERSION_KEY)?
            .map(|v| decode_u64("path route version", &v))
            .transpose()
            .map(|v| v.unwrap_or(0))
    }

    #[cfg(test)]
    pub fn put_path_route_version(&self, version: u64) -> CommonResult<()> {
        self.store.put(NS, &VERSION_KEY, &version.to_be_bytes())
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

    fn route_put_op(&self, entry: &PathRouteEntry) -> CommonResult<KvWrite> {
        let key = Self::route_key(&entry.path);
        let value = Serde::serialize(entry)?;
        Ok(self.put_op(key, value))
    }

    fn route_delete_op(&self, path: &str) -> KvWrite {
        self.delete_op(Self::route_key(path))
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
