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

use super::{KvPair, KvStore};
use orpc::CommonResult;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// In-memory KV engine for unit tests.
///
/// Keys are internally prefixed with `namespace\0` so that scans are
/// correctly scoped to each namespace.
pub struct MemoryKvEngine {
    data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryKvEngine {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(BTreeMap::new()),
        }
    }

    fn full_key(ns: &str, key: &[u8]) -> Vec<u8> {
        let mut k = Vec::with_capacity(ns.len() + 1 + key.len());
        k.extend_from_slice(ns.as_bytes());
        k.push(0);
        k.extend_from_slice(key);
        k
    }

    fn ns_prefix(ns: &str) -> Vec<u8> {
        let mut k = Vec::with_capacity(ns.len() + 1);
        k.extend_from_slice(ns.as_bytes());
        k.push(0);
        k
    }
}

impl KvStore for MemoryKvEngine {
    fn get(&self, ns: &str, key: &[u8]) -> CommonResult<Option<Vec<u8>>> {
        let k = Self::full_key(ns, key);
        let data = self.data.lock().unwrap();
        Ok(data.get(&k).cloned())
    }

    fn put(&self, ns: &str, key: &[u8], value: &[u8]) -> CommonResult<()> {
        let k = Self::full_key(ns, key);
        let mut data = self.data.lock().unwrap();
        data.insert(k, value.to_vec());
        Ok(())
    }

    fn delete(&self, ns: &str, key: &[u8]) -> CommonResult<()> {
        let k = Self::full_key(ns, key);
        let mut data = self.data.lock().unwrap();
        data.remove(&k);
        Ok(())
    }

    fn scan_prefix(&self, ns: &str, prefix: &[u8]) -> CommonResult<Vec<KvPair>> {
        let full_prefix = Self::full_key(ns, prefix);
        let ns_prefix = Self::ns_prefix(ns);
        let data = self.data.lock().unwrap();
        let items = data
            .range(full_prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&full_prefix))
            .map(|(k, v)| {
                let user_key = k[ns_prefix.len()..].to_vec();
                (user_key, v.clone())
            })
            .collect();
        Ok(items)
    }
}
