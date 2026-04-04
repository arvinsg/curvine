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

use self::bg_assignment::BGAssignmentChecker;
use self::lease_validity::LeaseValidityChecker;
use self::placement_rule::PlacementRuleChecker;
use self::replica::ReplicaChecker;
use crate::pd::schedule::operator::BGOperator;
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::BlockGroupInfo;
use std::sync::Arc;

pub mod bg_assignment;
pub mod lease_validity;
pub mod placement_rule;
pub mod replica;

/// Context passed to checkers (read-only refs to managers).
pub struct CheckerContext<'a> {
    pub pool_manager: &'a crate::pd::pool::PoolManager,
    pub bg_manager: &'a crate::pd::bg::BGManager,
    pub node_manager: &'a crate::pd::node::NodeManager,
    pub config_manager: &'a crate::pd::config::ConfigManager,
}

/// Direct BG push command for a worker (via heartbeat response).
#[derive(Debug, Clone)]
pub struct BGPushCommand {
    pub worker_id: u32,
    pub add_bgs: Vec<BlockGroupInfo>,
    pub remove_bgs: Vec<u32>,
}

/// Checker trait: ensures correctness via per-BG patrol.
///
/// Checkers are always active and cannot be paused. During patrol,
/// checkers execute in priority order (lower number = higher priority).
/// For each BG, the first checker producing an operator wins (short-circuit).
pub trait Checker: Send + Sync {
    /// Unique checker name.
    fn name(&self) -> &str;

    /// Check a single BG. Returns an operator if the BG needs correction.
    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &CheckerContext<'_>) -> Option<BGOperator>;

    /// Checker priority (lower = executed first).
    fn priority(&self) -> u32;
}

/// Build the default set of checkers (sorted by priority).
pub fn default_checkers(ctx: Arc<CoordinatorContext>) -> Vec<Box<dyn Checker>> {
    let mut checkers: Vec<Box<dyn Checker>> = vec![
        Box::new(LeaseValidityChecker::new(ctx.clone())),
        Box::new(ReplicaChecker::new(ctx.clone())),
        Box::new(PlacementRuleChecker::new(ctx.clone())),
        Box::new(BGAssignmentChecker::new(ctx)),
    ];
    checkers.sort_by_key(|c| c.priority());
    checkers
}

/// Shared test helpers for checker and scheduler tests.
#[cfg(test)]
pub mod tests_common {
    use crate::pd::schedule::CoordinatorContext;
    use std::collections::HashMap;
    use std::sync::Arc;

    pub fn test_coordinator_context(
        overrides: HashMap<String, String>,
    ) -> Arc<CoordinatorContext> {
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let raft = curvine_common::raft::RaftClient::from_conf(
            curvine_common::conf::JournalConf::default().create_runtime(),
            &curvine_common::conf::JournalConf::default(),
        );
        let jc = Arc::new(crate::pd::journal::Client::new(raft));
        let config = Arc::new(crate::pd::config::ConfigManager::new(
            store.clone(),
            jc.clone(),
            overrides,
        ));
        let node_store = Arc::new(crate::pd::node::NodeStore::new(store.clone()));
        let node_mgr = Arc::new(crate::pd::node::NodeManager::new(
            node_store,
            config.clone(),
            jc.clone(),
        ));
        let pool_store = Arc::new(crate::pd::pool::PoolStore::new(store.clone()));
        let pool_mgr = Arc::new(crate::pd::pool::PoolManager::new(
            pool_store,
            node_mgr.clone(),
            jc.clone(),
        ));
        let bg_store = Arc::new(crate::pd::bg::BGStore::new(store));
        let bg_mgr = Arc::new(crate::pd::bg::BGManager::new(
            bg_store,
            pool_mgr.clone(),
            jc.clone(),
            1024,
            vec![3],
            vec![],
        ));
        Arc::new(CoordinatorContext {
            node_manager: node_mgr,
            pool_manager: pool_mgr,
            bg_manager: bg_mgr,
            config_manager: config,
            journal_client: jc,
            leader_checker: Arc::new(crate::pd::schedule::coordinator::AlwaysLeader),
        })
    }
}
