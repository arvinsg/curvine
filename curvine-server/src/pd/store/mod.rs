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

mod rocks_kv_engine;

pub use rocks_kv_engine::RocksKvEngine;

use orpc::CommonResult;

// ---- Column Family Names ----
pub const CF_META: &str = "meta";
pub const CF_DATA: &str = "data";

// ---- Key Prefixes for CF_META ----
pub const PREFIX_CONFIG: u8 = 0x10;
pub const PREFIX_NODE: u8 = 0x20;
pub const PREFIX_POOL: u8 = 0x30;
pub const PREFIX_MOUNT_VERSION: u8 = 0x40;
pub const PREFIX_MOUNT: u8 = 0x41;
pub const PREFIX_ROUTE_VERSION: u8 = 0x50;
pub const PREFIX_ROUTE: u8 = 0x51;

// ---- Key Prefixes for CF_DATA ----
pub const PREFIX_BG_INFO: u8 = 0x01;
pub const PREFIX_BG_NEXT_ID: u8 = 0x02;
pub const PREFIX_BG_TABLE: u8 = 0x03;

pub type KvPair = (Vec<u8>, Vec<u8>);

/// Namespace-aware KV store abstraction.
pub trait KvStore: Send + Sync {
    fn get(&self, ns: &str, key: &[u8]) -> CommonResult<Option<Vec<u8>>>;

    fn put(&self, ns: &str, key: &[u8], value: &[u8]) -> CommonResult<()>;

    fn delete(&self, ns: &str, key: &[u8]) -> CommonResult<()>;

    fn scan_prefix(&self, ns: &str, prefix: &[u8]) -> CommonResult<Vec<KvPair>>;

    fn exists(&self, ns: &str, key: &[u8]) -> CommonResult<bool> {
        Ok(self.get(ns, key)?.is_some())
    }
}

#[cfg(test)]
pub mod memory_kv_engine;
