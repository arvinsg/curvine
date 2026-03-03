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

use super::{KvEngine, KvPair, KvStore};
use orpc::CommonResult;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// In-memory `KvEngine` for unit tests.
///
/// All namespaces share a single `BTreeMap` with a `namespace\0` prefix so
/// that scans are correctly scoped.
pub struct MemoryKvEngine {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryKvEngine {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl KvEngine for MemoryKvEngine {
    fn open_store(&self, namespace: &str) -> Arc<dyn KvStore> {
        Arc::new(MemoryKvStore {
            data: self.data.clone(),
            ns_prefix: format!("{}\0", namespace).into_bytes(),
        })
    }

    fn create_checkpoint(&self, _id: u64) -> CommonResult<String> {
        Ok(String::new())
    }

    fn restore_from_checkpoint(&self, _dir: &str) -> CommonResult<()> {
        Ok(())
    }
}

struct MemoryKvStore {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    ns_prefix: Vec<u8>,
}

impl MemoryKvStore {
    fn full_key(&self, key: &[u8]) -> Vec<u8> {
        let mut k = self.ns_prefix.clone();
        k.extend_from_slice(key);
        k
    }
}

impl KvStore for MemoryKvStore {
    fn get(&self, key: &[u8]) -> CommonResult<Option<Vec<u8>>> {
        let k = self.full_key(key);
        let data = self.data.lock().unwrap();
        Ok(data.get(&k).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> CommonResult<()> {
        let k = self.full_key(key);
        let mut data = self.data.lock().unwrap();
        data.insert(k, value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> CommonResult<()> {
        let k = self.full_key(key);
        let mut data = self.data.lock().unwrap();
        data.remove(&k);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> CommonResult<Vec<KvPair>> {
        let full_prefix = self.full_key(prefix);
        let data = self.data.lock().unwrap();
        let items = data
            .range(full_prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&full_prefix))
            .map(|(k, v)| {
                let user_key = k[self.ns_prefix.len()..].to_vec();
                (user_key, v.clone())
            })
            .collect();
        Ok(items)
    }
}
