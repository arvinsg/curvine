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

/// Pool stats
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolStats {
    pub capacity_bytes: u64,
    pub available_bytes: u64,
    pub used_bytes: u64,
}

/// Pool info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolInfo {
    pub pool_id: u16,
    pub name: String,
    pub media: StorageType,
    pub workers: HashSet<u32>,
    pub epoch: u64,

    // ========== Non-persisted ==========
    #[serde(skip)]
    pub stats: PoolStats,
}

impl PoolInfo {
    pub fn new(pool_id: u16, name: String, media: StorageType) -> Self {
        Self {
            pool_id,
            name,
            media,
            workers: HashSet::new(),
            epoch: 0,
            stats: PoolStats::default(),
        }
    }
}
