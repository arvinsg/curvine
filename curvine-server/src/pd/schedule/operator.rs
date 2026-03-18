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

use curvine_common::state::BlockGroupInfo;
use serde::{Deserialize, Serialize};

/// Operator status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpStatus {
    Pending,
    Running,
    Success,
    Failed,
    Timeout,
    Cancelled,
}

/// A single step of an operator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpStep {
    AddReplica { worker_id: u32 },
    RemoveReplica { worker_id: u32 },
    TransferLease { from_worker: u32, to_worker: u32 },
    WaitSync { worker_id: u32 },
    RebuildTable { pool_id: u16, reason: RebuildReason },
}

/// Reason for BGTable rebuild
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RebuildReason {
    NodeJoined { node_ids: Vec<u32> },
    NodeRemoved { node_ids: Vec<u32> },
    Manual,
}

/// Operator: a sequence of steps applied to one BG
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BGOperator {
    pub id: u64,
    pub bg_id: u32,
    pub description: String,
    pub steps: Vec<OpStep>,
    pub current_step: usize,
    pub status: OpStatus,
    pub create_time_ms: u64,
    pub priority: u32,
}

/// Commands for a worker (add/remove BGs), returned via heartbeat response
#[derive(Debug, Clone, Default)]
pub struct BGCommands {
    pub add_bgs: Vec<BlockGroupInfo>,
    pub remove_bgs: Vec<u32>,
}
