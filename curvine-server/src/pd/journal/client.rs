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

use super::PdEntry;
use curvine_common::raft::RaftClient;
use curvine_common::utils::SerdeUtils as Serde;
use curvine_common::FsResult;

/// Unified Raft propose client for all PD modules.
pub struct Client {
    raft_client: RaftClient,
}

impl Client {
    pub fn new(raft_client: RaftClient) -> Self {
        Self { raft_client }
    }

    /// Propose a PdEntry through Raft consensus.
    pub fn propose(&self, entry: PdEntry) -> FsResult<()> {
        let data = Serde::serialize(&entry)?;
        self.raft_client.block_on_send_propose(data)?;
        Ok(())
    }
}
