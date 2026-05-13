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

use crate::pd::config::keys;
use crate::pd::schedule::{BGOperator, ManagerContext};
use curvine_common::state::{BlockGroupInfo, NodeInfo, NodePayload, NodeState};

pub struct PoolMembershipChecker;

impl super::Checker for PoolMembershipChecker {
    fn name(&self) -> &str {
        "pool-membership-checker"
    }

    fn priority(&self) -> u32 {
        super::CheckerPriority::POOL_MEMBERSHIP
    }

    fn check_bg(&self, _bg: &BlockGroupInfo, _ctx: &ManagerContext) -> Option<BGOperator> {
        None
    }

    fn check_worker(&self, worker: &NodeInfo, ctx: &ManagerContext) -> Option<BGOperator> {
        if worker.state != NodeState::Live {
            return None;
        }
        let wid = worker.base.node_id;
        if !ctx.pool_manager.get_pools_by_worker(wid).is_empty() {
            return None;
        }

        log::warn!("Worker {} is Live but not assigned to any pool", wid);

        if !ctx
            .config_manager
            .get_bool(keys::PD_CHECKER_POOL_MEMBERSHIP_AUTO_REPAIR)
        {
            return None;
        }

        let NodePayload::Worker(payload) = &worker.payload else {
            return None;
        };
        match ctx
            .pool_manager
            .assign_worker_to_pools(wid, &payload.storage_specs)
        {
            Ok(result) if result.target_pool_ids.is_empty() => {
                log::error!(
                    "PoolMembershipChecker: worker {} has no usable storage specs for pool assignment",
                    wid
                );
            }
            Ok(result) => {
                log::info!(
                    "PoolMembershipChecker repaired worker {}: target_pools={:?}, changed_pools={:?}",
                    wid,
                    result.target_pool_ids,
                    result.changed_pool_ids
                );
            }
            Err(e) => {
                log::error!(
                    "PoolMembershipChecker repair failed for worker {}: {}",
                    wid,
                    e
                );
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::pool::POOL_ID_SSD;
    use crate::pd::schedule::checker::tests_common::Fixture;
    use crate::pd::schedule::checker::Checker;
    use curvine_common::state::{
        NodeAddress, NodeBase, NodeInfo, NodePayload, NodeType, WorkerNodePayload,
    };

    fn insert_orphan_worker(f: &Fixture, wid: u32, state: NodeState) {
        f.ctx.node_manager.test_insert_node(NodeInfo {
            base: NodeBase {
                node_id: wid,
                node_type: NodeType::Worker,
                address: NodeAddress {
                    hostname: format!("w-{}", wid),
                    ip: format!("10.0.0.{}", wid),
                    rpc_port: 8000 + wid as u16,
                    web_port: 9000 + wid as u16,
                },
                ..Default::default()
            },
            state,
            payload: NodePayload::Worker(WorkerNodePayload::default()),
            ..Default::default()
        });
    }

    #[test]
    fn name_and_priority() {
        let checker = PoolMembershipChecker;
        assert_eq!(checker.name(), "pool-membership-checker");
        assert_eq!(
            checker.priority(),
            super::super::CheckerPriority::POOL_MEMBERSHIP
        );
    }

    #[test]
    fn live_worker_in_pool_is_noop() {
        let f = Fixture::new();
        f.add_workers(&[100], POOL_ID_SSD);
        let worker = f.ctx.node_manager.get_node(100).unwrap();
        assert!(PoolMembershipChecker
            .check_worker(&worker, &f.ctx)
            .is_none());
        assert!(!f.ctx.pool_manager.get_pools_by_worker(100).is_empty());
    }

    #[test]
    fn orphaned_worker_not_repaired_when_flag_disabled() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            keys::PD_CHECKER_POOL_MEMBERSHIP_AUTO_REPAIR.to_string(),
            "false".to_string(),
        );
        let f = Fixture::with_overrides(overrides);
        insert_orphan_worker(&f, 100, NodeState::Live);
        assert!(f.ctx.pool_manager.get_pools_by_worker(100).is_empty());

        let worker = f.ctx.node_manager.get_node(100).unwrap();
        // With the flag off, the checker must NOT attempt a Raft propose;
        // if it did, this test would error with a Raft connection refused.
        assert!(PoolMembershipChecker
            .check_worker(&worker, &f.ctx)
            .is_none());
        assert!(f.ctx.pool_manager.get_pools_by_worker(100).is_empty());
    }

    #[test]
    fn non_live_worker_is_ignored() {
        let f = Fixture::new();
        insert_orphan_worker(&f, 100, NodeState::Decommission);

        let worker = f.ctx.node_manager.get_node(100).unwrap();
        // Non-Live nodes are outside the checker's scope — no warning, no
        // repair attempt (so no Raft propose attempt either).
        assert!(PoolMembershipChecker
            .check_worker(&worker, &f.ctx)
            .is_none());
        assert!(f.ctx.pool_manager.get_pools_by_worker(100).is_empty());
    }
}
