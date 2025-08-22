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

use serde::{Deserialize, Serialize};

/// Master load function configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MasterLoadConf {
    /// Task expiration time (seconds)
    pub job_ttl_seconds: u64,
    /// Clean up expired task interval (seconds)
    pub job_cleanup_interval_seconds: u64,
}

impl Default for MasterLoadConf {
    fn default() -> Self {
        Self {
            job_ttl_seconds: 24 * 3600,
            job_cleanup_interval_seconds: 3600,
        }
    }
}

///Configuration of Worker Loading Function
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerLoadConf {
    /// Progress report interval (milliseconds)
    pub task_status_report_interval_ms: u64,
    /// Default block size (bytes)
    pub task_read_chunk_size_bytes: u64,
    /// The default buffer cache number
    pub task_transfer_buffer_count: usize,
    /// Task timeout (seconds), -1 means no timeout
    pub task_timeout_seconds: i64,
    /// Maximum number of concurrent tasks
    pub task_transfer_max_concurrent_tasks: u8,
}

impl Default for WorkerLoadConf {
    fn default() -> Self {
        Self {
            task_status_report_interval_ms: 5000,
            task_read_chunk_size_bytes: 1024 * 1024,
            task_transfer_buffer_count: 16,
            task_timeout_seconds: 3600,
            task_transfer_max_concurrent_tasks: 5,
        }
    }
}
