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

use super::apply_outcome::ApplyOutcome;
use super::leader::LeaderChecker;
use super::PdEntry;
use curvine_common::raft::RaftClient;
use curvine_common::utils::SerdeUtils as Serde;
use curvine_common::{FsError, FsResult};
use std::sync::{Arc, OnceLock};

/// Unified Raft propose client for all PD modules.
///
/// Holds an optional `LeaderChecker` so that `propose_as_leader*` paths can
/// fast-fail when this PD node has lost leadership (avoiding a wasted RPC).
/// The checker is set once after construction via `set_leader_checker`; before
/// it is set, `propose_as_leader*` falls back to plain propose and relies on
/// the raft server-side `handle_propose` leader fence.
pub struct Client {
    raft_client: RaftClient,
    leader_checker: OnceLock<Arc<dyn LeaderChecker>>,
}

impl Client {
    pub fn new(raft_client: RaftClient) -> Self {
        Self {
            raft_client,
            leader_checker: OnceLock::new(),
        }
    }

    /// Install the leader checker. Should be called once during PD startup,
    /// after the raft `RoleMonitor` has been initialized. Subsequent calls are
    /// silently ignored (`OnceLock` semantics).
    pub fn set_leader_checker(&self, checker: Arc<dyn LeaderChecker>) {
        let _ = self.leader_checker.set(checker);
    }

    fn ensure_leader(&self) -> FsResult<()> {
        if let Some(checker) = self.leader_checker.get() {
            if !checker.is_leader() {
                return Err(FsError::not_leader(
                    "propose rejected: local PD node is not the raft leader",
                ));
            }
        }
        Ok(())
    }

    /// Public helper: returns true when the local PD node is the raft leader.
    /// Used by RPC entry points (register / heartbeat) to fast-fail before
    /// they mutate any in-memory state. When no leader checker has been
    /// installed (test mode), conservatively returns true so existing
    /// non-RPC test paths keep working.
    pub fn is_leader(&self) -> bool {
        match self.leader_checker.get() {
            Some(checker) => checker.is_leader(),
            None => true,
        }
    }

    /// Propose a PdEntry through Raft consensus, ignoring the structured apply
    /// result. Use this for fire-and-forget paths that only care about whether
    /// the entry was committed (e.g. heartbeat checkpoints, log-only updates).
    /// Per §17 design contract, prefer `propose_with_result` for any path that
    /// needs to know whether the apply actually mutated state.
    pub fn propose(&self, entry: PdEntry) -> FsResult<()> {
        let data = Serde::serialize(&entry)?;
        let _apply_result = self.raft_client.block_on_send_propose(data)?;
        Ok(())
    }

    /// Propose a PdEntry and return the structured `ApplyOutcome` from the
    /// apply path (carried via Raft `ProposeResponse.apply_result`). This is
    /// the §17 standard propose entry point — callers `match` on the outcome
    /// and let upper-layer scheduling retry stale conflicts.
    ///
    /// Backward compatibility: if the leader has not yet been upgraded with
    /// P0.6, the returned `apply_result` bytes are empty and decode as
    /// `ApplyOutcome::Applied`. Likewise, modules that have not yet been
    /// migrated to return per-call outcomes also produce empty bytes →
    /// `Applied`.
    pub fn propose_with_result(&self, entry: PdEntry) -> FsResult<ApplyOutcome> {
        let data = Serde::serialize(&entry)?;
        let apply_result = self.raft_client.block_on_send_propose(data)?;
        ApplyOutcome::decode(&apply_result)
    }

    /// Like `propose`, but fast-fails with `FsError::NotLeaderMaster` when the
    /// local PD node is not the raft leader. Use this for any path whose
    /// correctness depends on leader identity (ID allocation, replica
    /// placement, leader-only background jobs). When no leader checker is
    /// installed, behaves identically to `propose` (the raft layer's own
    /// `handle_propose` still rejects non-leaders, just with one extra RPC).
    pub fn propose_as_leader(&self, entry: PdEntry) -> FsResult<()> {
        self.ensure_leader()?;
        self.propose(entry)
    }

    /// Like `propose_with_result`, but fast-fails with
    /// `FsError::NotLeaderMaster` when the local PD node is not the raft
    /// leader. This is the §17 standard entry point for leader-only
    /// modifications (Pool / BG / Mount / Meta CAS paths).
    pub fn propose_as_leader_with_result(&self, entry: PdEntry) -> FsResult<ApplyOutcome> {
        self.ensure_leader()?;
        self.propose_with_result(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::conf::JournalConf;

    /// Test-only checker whose answer can be flipped at runtime.
    struct ToggleLeader(std::sync::atomic::AtomicBool);
    impl ToggleLeader {
        fn new(is_leader: bool) -> Self {
            Self(std::sync::atomic::AtomicBool::new(is_leader))
        }
        fn set(&self, v: bool) {
            self.0.store(v, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl LeaderChecker for ToggleLeader {
        fn is_leader(&self) -> bool {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    fn test_client() -> Client {
        let journal_conf = JournalConf::default();
        let rt = journal_conf.create_runtime();
        let raft = RaftClient::from_conf(rt, &journal_conf);
        Client::new(raft)
    }

    #[test]
    fn ensure_leader_passes_without_checker() {
        // Before set_leader_checker, propose_as_leader falls through (relies
        // on raft server-side fence). ensure_leader must return Ok.
        let client = test_client();
        client.ensure_leader().expect("no checker installed");
    }

    #[test]
    fn ensure_leader_fails_when_not_leader() {
        let client = test_client();
        client.set_leader_checker(Arc::new(ToggleLeader::new(false)));
        let err = client.ensure_leader().unwrap_err();
        assert!(matches!(err, FsError::NotLeaderMaster(_)));
    }

    #[test]
    fn ensure_leader_passes_when_leader() {
        let client = test_client();
        client.set_leader_checker(Arc::new(ToggleLeader::new(true)));
        client.ensure_leader().expect("leader checker says yes");
    }

    #[test]
    fn set_leader_checker_is_idempotent() {
        // OnceLock semantics: subsequent set() calls are silently ignored.
        let client = test_client();
        let first = Arc::new(ToggleLeader::new(true));
        client.set_leader_checker(first.clone());
        // Try to install a second checker that says false; should be ignored.
        client.set_leader_checker(Arc::new(ToggleLeader::new(false)));
        // First checker still wins.
        client.ensure_leader().expect("first checker says yes");
        // Flip the first checker; ensure_leader follows.
        first.set(false);
        let err = client.ensure_leader().unwrap_err();
        assert!(matches!(err, FsError::NotLeaderMaster(_)));
    }
}
