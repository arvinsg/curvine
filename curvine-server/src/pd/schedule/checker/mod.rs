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
use crate::pd::schedule::ManagerContext;
use curvine_common::state::{BlockGroupInfo, NodeInfo};

pub mod bg_assignment;
pub mod lease_validity;
pub mod placement_rule;
pub mod replica;

/// Priority levels for `Checker::priority()`.
pub struct CheckerPriority;
impl CheckerPriority {
    /// Replica count wrong (under/over) — affects data availability.
    pub const REPLICA: u32 = 10;
    /// Lease invalid (orphaned or on dead worker) — affects data consistency.
    pub const LEASE_VALIDITY: u32 = 20;
    /// Placement rule violated — affects fault isolation but not data safety.
    pub const PLACEMENT_RULE: u32 = 30;
    /// Worker-vs-PD assignment drift — eventually-consistent reconciliation.
    pub const BG_ASSIGNMENT: u32 = 40;
}

/// Checker trait: ensures correctness via patrol.
///
/// Checkers are stateless. During patrol, checkers execute in priority order.
/// For each BG/Worker, the first checker producing an operator wins (short-circuit).
pub trait Checker: Send + Sync {
    fn name(&self) -> &str;
    fn priority(&self) -> u32;

    /// Check a single BG. Returns an operator if the BG needs correction.
    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &ManagerContext) -> Option<BGOperator>;

    /// Check a single worker. Returns an operator if the worker needs correction.
    fn check_worker(&self, _worker: &NodeInfo, _ctx: &ManagerContext) -> Option<BGOperator> {
        None
    }
}

/// Build the default set of checkers (sorted by priority).
pub fn default_checkers() -> Vec<Box<dyn Checker>> {
    let mut checkers: Vec<Box<dyn Checker>> = vec![
        Box::new(ReplicaChecker),
        Box::new(LeaseValidityChecker),
        Box::new(PlacementRuleChecker),
        Box::new(BGAssignmentChecker),
    ];
    checkers.sort_by_key(|c| c.priority());
    checkers
}

/// Shared test helpers for checker and scheduler tests.
#[cfg(test)]
pub mod tests_common {
    use crate::pd::schedule::ManagerContext;
    use std::collections::HashMap;
    use std::sync::Arc;

    pub fn test_context(overrides: HashMap<String, String>) -> Arc<ManagerContext> {
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
            config.clone(),
            1024,
            vec![3],
            vec![],
        ));
        let operator_controller = Arc::new(
            crate::pd::schedule::operator_controller::OperatorController::new(
                config.clone(),
                bg_mgr.clone(),
            ),
        );
        Arc::new(ManagerContext {
            node_manager: node_mgr,
            pool_manager: pool_mgr,
            bg_manager: bg_mgr,
            config_manager: config,
            operator_controller,
            runtime: Arc::new(orpc::runtime::Runtime::new("test", 1, 1)),
        })
    }
}
