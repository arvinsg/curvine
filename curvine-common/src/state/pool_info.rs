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
use orpc::{err_box, CommonError};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

/// Fixed resource domains used by PD placement.
#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash, Ord, PartialOrd,
)]
pub enum PoolType {
    Mem,
    #[default]
    Ssd,
    Hdd,
}

impl PoolType {
    pub const ALL: [PoolType; 3] = [PoolType::Mem, PoolType::Ssd, PoolType::Hdd];

    pub fn as_str(&self) -> &'static str {
        match self {
            PoolType::Mem => "MEM",
            PoolType::Ssd => "SSD",
            PoolType::Hdd => "HDD",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            PoolType::Mem => "mem_pool",
            PoolType::Ssd => "ssd_pool",
            PoolType::Hdd => "hdd_pool",
        }
    }

    pub fn media(&self) -> StorageType {
        match self {
            PoolType::Mem => StorageType::Mem,
            PoolType::Ssd => StorageType::Ssd,
            PoolType::Hdd => StorageType::Hdd,
        }
    }

    pub fn code(&self) -> u16 {
        match self {
            PoolType::Mem => 1,
            PoolType::Ssd => 2,
            PoolType::Hdd => 3,
        }
    }

    pub fn from_code(code: u16) -> Option<Self> {
        match code {
            1 => Some(PoolType::Mem),
            2 => Some(PoolType::Ssd),
            3 => Some(PoolType::Hdd),
            _ => None,
        }
    }

    pub fn from_media(media: StorageType) -> Option<Self> {
        match media {
            StorageType::Mem => Some(PoolType::Mem),
            StorageType::Ssd => Some(PoolType::Ssd),
            StorageType::Hdd => Some(PoolType::Hdd),
            _ => None,
        }
    }
}

impl fmt::Display for PoolType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for PoolType {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "MEM" | "MEM_POOL" => Ok(PoolType::Mem),
            "SSD" | "SSD_POOL" => Ok(PoolType::Ssd),
            "HDD" | "HDD_POOL" => Ok(PoolType::Hdd),
            _ => err_box!("invalid pool type: {}", value),
        }
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
    pub pool_type: PoolType,
    pub name: String,
    pub media: StorageType,

    // ========== Non-persisted ==========
    #[serde(skip)]
    pub workers: HashSet<u32>,
    #[serde(skip)]
    pub stats: PoolStats,
}

impl PoolInfo {
    pub fn new(pool_type: PoolType) -> Self {
        Self {
            pool_type,
            name: pool_type.name().to_string(),
            media: pool_type.media(),
            workers: HashSet::new(),
            stats: PoolStats::default(),
        }
    }
}
