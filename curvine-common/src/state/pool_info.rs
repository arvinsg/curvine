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

use crate::state::StorageType;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Fixed pool media used by PD placement.
pub const POOL_STORAGE_TYPES: [StorageType; 3] =
    [StorageType::Mem, StorageType::Ssd, StorageType::Hdd];

#[inline]
pub fn is_pool_storage_type(media: StorageType) -> bool {
    matches!(
        media,
        StorageType::Mem | StorageType::Ssd | StorageType::Hdd
    )
}

#[inline]
pub fn pool_name(media: StorageType) -> &'static str {
    match media {
        StorageType::Mem => "mem_pool",
        StorageType::Ssd => "ssd_pool",
        StorageType::Hdd => "hdd_pool",
        _ => "invalid_pool",
    }
}

#[inline]
pub fn pool_storage_code(media: StorageType) -> Option<u16> {
    match media {
        StorageType::Mem => Some(1),
        StorageType::Ssd => Some(2),
        StorageType::Hdd => Some(3),
        _ => None,
    }
}

#[inline]
pub fn pool_storage_from_code(code: u16) -> Option<StorageType> {
    match code {
        1 => Some(StorageType::Mem),
        2 => Some(StorageType::Ssd),
        3 => Some(StorageType::Hdd),
        _ => None,
    }
}

/// Pool stats
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolStats {
    pub capacity_bytes: u64,
    pub available_bytes: u64,
    pub used_bytes: u64,
    pub block_count: u64,
}

/// Runtime pool view.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolInfo {
    pub media: StorageType,
    pub name: String,

    // ========== Non-persisted ==========
    #[serde(skip)]
    pub workers: HashSet<u32>,
    #[serde(skip)]
    pub stats: PoolStats,
}

impl PoolInfo {
    pub fn new(media: StorageType) -> Self {
        Self {
            media,
            name: pool_name(media).to_string(),
            workers: HashSet::new(),
            stats: PoolStats::default(),
        }
    }
}
