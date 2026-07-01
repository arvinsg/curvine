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

use super::{StorageType, TableId, TtlAction};
use crate::{FsError, FsResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type NamespaceId = u16;

// A `TableId` (u16) packs the owning namespace and the table's index within
// that namespace:
//
//   15                    4 3          0
//  ┌───────────────────────┬────────────┐
//  │     namespace_id      │ table_index│
//  └───────────────────────┴────────────┘
//
// table_index is 4 bits, so a namespace can hold up to 16 tables (indices
// 0..=15). Any index may be used freely; the caller decides which tables are
// cache tiers and which is the write buffer.
pub const TABLE_INDEX_BITS: u16 = 4;
pub const TABLE_INDEX_MASK: u16 = (1 << TABLE_INDEX_BITS) - 1;
pub const MAX_TABLES_PER_NAMESPACE: usize = 1 << TABLE_INDEX_BITS;

pub const INVALID_NAMESPACE_ID: NamespaceId = 0;
pub const MAX_NAMESPACE_ID: NamespaceId = (1 << (16 - TABLE_INDEX_BITS)) - 1;

#[inline]
pub fn make_table_id(namespace_id: NamespaceId, table_index: u8) -> FsResult<TableId> {
    if namespace_id == INVALID_NAMESPACE_ID || namespace_id > MAX_NAMESPACE_ID {
        return Err(FsError::common(format!(
            "namespace_id out of range: {}",
            namespace_id
        )));
    }
    if table_index as usize >= MAX_TABLES_PER_NAMESPACE {
        return Err(FsError::common(format!(
            "table_index out of range: {} (valid 0..{})",
            table_index, MAX_TABLES_PER_NAMESPACE
        )));
    }
    Ok((namespace_id << TABLE_INDEX_BITS) | table_index as TableId)
}

#[inline]
pub fn namespace_id_of(table_id: TableId) -> NamespaceId {
    table_id >> TABLE_INDEX_BITS
}

#[inline]
pub fn table_index_of(table_id: TableId) -> u8 {
    (table_id & TABLE_INDEX_MASK) as u8
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LabelMatch {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CacheAckPolicy {
    One,
    Majority,
    AtLeast(u16),
}

impl Default for CacheAckPolicy {
    fn default() -> Self {
        CacheAckPolicy::One
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CacheReadPolicy {
    Nearest,
    PrimaryFirst,
    Random,
}

impl Default for CacheReadPolicy {
    fn default() -> Self {
        CacheReadPolicy::Nearest
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheReplicaPolicy {
    pub ack_policy: CacheAckPolicy,
    pub read_policy: CacheReadPolicy,
    pub min_isr: u16,
}

impl Default for CacheReplicaPolicy {
    fn default() -> Self {
        Self {
            ack_policy: CacheAckPolicy::default(),
            read_policy: CacheReadPolicy::default(),
            min_isr: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheTierConfig {
    pub pools: Vec<StorageType>,
    pub replica_count: u16,
    pub bucket_count: u32,
    pub worker_labels: Vec<LabelMatch>,
}

impl Default for CacheTierConfig {
    fn default() -> Self {
        Self {
            pools: vec![StorageType::Ssd],
            replica_count: 1,
            bucket_count: 1024,
            worker_labels: vec![],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteBufferConfig {
    pub pool: StorageType,
    pub replica_count: u16,
    pub capacity_bg_size: u64,
    pub min_active_bgs: u32,
    pub worker_labels: Vec<LabelMatch>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceInfo {
    pub id: NamespaceId,
    pub name: String,
    pub block_size: u32,
    pub cache_tier_tables: Vec<TableId>,
    pub write_buffer_table: Option<TableId>,
    pub cache_tier_config: CacheTierConfig,
    pub write_buffer_config: Option<WriteBufferConfig>,
    pub cache_replica_policy: CacheReplicaPolicy,
    pub default_ttl_ms: Option<u64>,
    pub ttl_action: TtlAction,
    pub version: u64,
    pub create_time_ms: u64,
    pub update_time_ms: u64,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateNamespaceRequest {
    pub name: String,
    pub block_size: u32,
    pub cache_tier_config: CacheTierConfig,
    pub write_buffer_config: Option<WriteBufferConfig>,
    pub cache_replica_policy: CacheReplicaPolicy,
    pub default_ttl_ms: Option<u64>,
    pub ttl_action: TtlAction,
    pub properties: HashMap<String, String>,
}

impl CreateNamespaceRequest {
    pub fn new(name: impl Into<String>, cache_tier_config: CacheTierConfig) -> Self {
        Self {
            name: name.into(),
            block_size: 128 * 1024 * 1024,
            cache_tier_config,
            write_buffer_config: None,
            cache_replica_policy: CacheReplicaPolicy::default(),
            default_ttl_ms: None,
            ttl_action: TtlAction::None,
            properties: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_id_round_trip() {
        let table_id = make_table_id(0x0123, 0x0a).unwrap();
        assert_eq!(namespace_id_of(table_id), 0x0123);
        assert_eq!(table_index_of(table_id), 0x0a);
    }

    #[test]
    fn table_id_rejects_invalid_namespace() {
        assert!(make_table_id(0, 0).is_err());
        assert!(make_table_id(MAX_NAMESPACE_ID + 1, 0).is_err());
    }

    #[test]
    fn table_id_rejects_out_of_range_index() {
        assert!(make_table_id(1, MAX_TABLES_PER_NAMESPACE as u8).is_err());
        assert!(make_table_id(1, (MAX_TABLES_PER_NAMESPACE - 1) as u8).is_ok());
    }
}
