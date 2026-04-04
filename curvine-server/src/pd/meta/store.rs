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
use curvine_common::state::PathRouteEntry;
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use std::sync::Arc;

/// Namespace for path route table (Federation Static).
const NS: &str = "meta";
const ROUTE_PREFIX: u8 = 0x51;
const VERSION_KEY: &[u8] = &[0x50];

/// Store for meta route data
pub struct RouteStore {
    store: Arc<dyn KvStore>,
}

impl RouteStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    fn route_key(path: &str) -> Vec<u8> {
        let mut buf = vec![ROUTE_PREFIX];
        buf.extend_from_slice(path.as_bytes());
        buf
    }

    pub fn put_path_route(&self, entry: &PathRouteEntry) -> CommonResult<()> {
        let key = Self::route_key(&entry.path);
        let value = Serde::serialize(entry)?;
        self.store.put(NS, &key, &value)?;
        Ok(())
    }

    pub fn delete_path_route(&self, path: &str) -> CommonResult<()> {
        let key = Self::route_key(path);
        self.store.delete(NS, &key)?;
        Ok(())
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
        match self.store.get(NS, VERSION_KEY)? {
            Some(data) => {
                let v: u64 = Serde::deserialize(&data)?;
                Ok(v)
            }
            None => Ok(0),
        }
    }

    pub fn put_path_route_version(&self, version: u64) -> CommonResult<()> {
        let value = Serde::serialize(&version)?;
        self.store.put(NS, VERSION_KEY, &value)?;
        Ok(())
    }
}
