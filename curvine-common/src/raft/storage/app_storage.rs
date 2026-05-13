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

use crate::proto::raft::SnapshotData;
use crate::raft::RaftResult;

/// Application layer storage.
/// Replay raft log
pub trait AppStorage: Clone + Send + Sync + 'static {
    /// Apply a committed entry to application state.
    ///
    /// Returns bytes that the leader will forward to the propose caller via
    /// `ProposeResponse.apply_result`. Implementations that have no structured
    /// result to return should return `Ok(Vec::new())`. PD interprets the
    /// returned bytes as an `ApplyOutcome`; an empty slice is decoded as
    /// `ApplyOutcome::Applied` for forward compatibility with un-upgraded
    /// leaders.
    fn apply(&self, is_leader: bool, message: &[u8]) -> RaftResult<Vec<u8>>;

    fn create_snapshot(&self, node_id: u64, last_applied: u64) -> RaftResult<SnapshotData>;

    fn apply_snapshot(&self, snapshot: &SnapshotData) -> RaftResult<()>;

    // Get the snapshot to save the directory.
    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String>;
}
