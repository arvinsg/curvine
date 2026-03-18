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

use super::StorageType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Worker node persisted payload
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerNodePayload {
    pub storage_specs: HashMap<String, StorageSpec>,
    pub az: Option<String>,
    pub rack: Option<String>,

    /// BGs currently held by the worker (updated from heartbeat, not persisted)
    #[serde(skip)]
    pub bg_ids: Vec<u32>,
    #[serde(skip)]
    pub storage_stats: HashMap<String, StorageStats>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StorageSpec {
    pub dir_id: u32,
    pub storage_id: String,
    pub failed: bool,
    pub storage_type: StorageType,
    pub dir_path: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub capacity: i64,
    pub fs_used: i64,
    pub non_fs_used: i64,
    pub available: i64,
    pub reserved_bytes: i64,
    pub block_num: i64,
    pub dir_path: String,
}
