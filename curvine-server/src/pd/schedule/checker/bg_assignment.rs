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

use super::{BGPushCommand, CheckResult, CheckerContext};
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::{NodeType, BG_FLAG_ASSIGNMENT_MISMATCH};
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Compares worker-reported BG list with expected assignments;
/// generates push commands for missing or extra BGs.
/// Also stores pending push commands for heartbeat delivery.
pub struct BGAssignmentChecker {
    ctx: Arc<CoordinatorContext>,
    pending_push_commands: DashMap<u32, BGPushCommand>,
}

impl BGAssignmentChecker {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self {
            ctx,
            pending_push_commands: DashMap::new(),
        }
    }

    /// Take pending push command for a worker (consumed on heartbeat response).
    pub fn take_pending_command(&self, worker_id: u32) -> Option<BGPushCommand> {
        self.pending_push_commands.remove(&worker_id).map(|e| e.1)
    }

    /// Check if there is a pending push command for a worker.
    pub fn has_pending_command(&self, worker_id: u32) -> bool {
        self.pending_push_commands.contains_key(&worker_id)
    }
}

impl super::Checker for BGAssignmentChecker {
    fn name(&self) -> &str {
        "bg-assignment-checker"
    }

    fn interval_ms(&self) -> u64 {
        self.ctx
            .config_manager
            .get_u64("pd.schedule.bg_check_interval_ms", 10_000)
    }

    fn check(&self, ctx: &CheckerContext<'_>) -> CheckResult {
        let mut result = CheckResult::default();

        let workers = ctx
            .node_manager
            .get_nodes_by_type(NodeType::Worker)
            .into_iter()
            .filter(|n| n.state == curvine_common::state::NodeState::Live)
            .collect::<Vec<_>>();

        for worker in &workers {
            let node_id = worker.base.node_id;
            let expected_bgs = ctx.bg_manager.get_bgs_on_worker(node_id);
            let expected_ids: HashSet<u32> = expected_bgs.iter().map(|bg| bg.bg_id).collect();

            let reported_ids: HashSet<u32> = match &worker.payload {
                curvine_common::state::NodePayload::Worker(ref p) => {
                    p.bg_ids.iter().copied().collect()
                }
                _ => continue,
            };

            let missing: Vec<u32> = expected_ids.difference(&reported_ids).copied().collect();
            let extra: Vec<u32> = reported_ids.difference(&expected_ids).copied().collect();

            if missing.is_empty() && extra.is_empty() {
                continue;
            }

            for bg in &expected_bgs {
                if missing.contains(&bg.bg_id) || extra.contains(&bg.bg_id) {
                    ctx.bg_manager.add_bg_flag(bg.bg_id, BG_FLAG_ASSIGNMENT_MISMATCH);
                }
            }

            let add_bgs: Vec<_> = missing
                .iter()
                .filter_map(|&bg_id| ctx.bg_manager.get_bg(bg_id))
                .collect();

            let cmd = BGPushCommand {
                worker_id: node_id,
                add_bgs,
                remove_bgs: extra,
            };

            self.pending_push_commands.insert(node_id, cmd.clone());
            result.bg_push_commands.push(cmd);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::journal::entry::BGEntry;
    use crate::pd::schedule::checker::{Checker, CheckerContext};
    use curvine_common::state::{
        BGLease, BGOpState, BGState, BlockGroupInfo, NodeAddress, NodeBase, NodeInfo, NodePayload,
        NodeState, PlacementPolicy, WorkerNodePayload, BG_FLAG_ASSIGNMENT_MISMATCH, BG_FLAG_NONE,
    };

    fn test_ctx(
        overrides: std::collections::HashMap<String, String>,
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

    fn make_bg(bg_id: u32, table_id: u32, replica_set: Vec<u32>, flags: u32) -> BlockGroupInfo {
        let leader = replica_set.first().copied().unwrap_or(0);
        BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            lease_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            flags,
            op_state: BGOpState::Idle,
            lease_owner: Some(BGLease {
                node_id: leader,
                expire_time_ms: 0,
            }),
            placement: PlacementPolicy::Default,
            stats: Default::default(),
        }
    }

    fn checker_ctx<'a>(ctx: &'a CoordinatorContext) -> CheckerContext<'a> {
        CheckerContext {
            pool_manager: &ctx.pool_manager,
            bg_manager: &ctx.bg_manager,
            node_manager: &ctx.node_manager,
            config_manager: &ctx.config_manager,
            suspect_bgs: vec![],
        }
    }

    fn make_worker_node(node_id: u32, bg_ids: Vec<u32>) -> NodeInfo {
        NodeInfo {
            base: NodeBase {
                node_id,
                node_type: NodeType::Worker,
                address: NodeAddress {
                    hostname: format!("worker-{}", node_id),
                    ip: "127.0.0.1".to_string(),
                    rpc_port: 9000,
                    web_port: 8000,
                },
                labels: Default::default(),
                software_version: "1.0".to_string(),
                startup_time_ms: 0,
            },
            epoch: 1,
            state: NodeState::Live,
            last_heartbeat_ms: orpc::common::LocalTime::mills(),
            last_persist_ms: 0,
            sys_stats: Default::default(),
            payload: NodePayload::Worker(WorkerNodePayload {
                bg_ids,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn no_ops_when_no_live_workers() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = BGAssignmentChecker::new(ctx.clone());

        // Insert a BG but no workers registered
        let bg = make_bg(1, 0x0001_0001, vec![100, 101], BG_FLAG_NONE);
        ctx.bg_manager
            .apply_create_bg(&BGEntry {
                op_ms: 0,
                info: bg,
            })
            .unwrap();

        let cctx = checker_ctx(&ctx);
        let result = checker.check(&cctx);
        assert!(
            result.bg_push_commands.is_empty(),
            "no live workers should produce no push commands"
        );
    }

    #[test]
    fn name_returns_correct_value() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = BGAssignmentChecker::new(ctx);
        assert_eq!(checker.name(), "bg-assignment-checker");
    }

    #[test]
    fn flag_is_set_on_mismatch() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = BGAssignmentChecker::new(ctx.clone());

        let worker_id = 100;

        // Register a worker node with empty bg_ids (reports no BGs)
        let worker_node = make_worker_node(worker_id, vec![]);
        ctx.node_manager.test_insert_node(worker_node);

        // Create a BG that has this worker in its replica_set
        let bg = make_bg(1, 0x0001_0001, vec![worker_id], BG_FLAG_NONE);
        ctx.bg_manager
            .apply_create_bg(&BGEntry {
                op_ms: 0,
                info: bg,
            })
            .unwrap();

        let cctx = checker_ctx(&ctx);
        let result = checker.check(&cctx);

        // The worker reports no BGs but has BG 1 in replica_set -> mismatch
        assert!(
            !result.bg_push_commands.is_empty(),
            "mismatch should generate push commands"
        );

        // Verify the BG_FLAG_ASSIGNMENT_MISMATCH flag is set
        let updated_bg = ctx.bg_manager.get_bg(1).unwrap();
        assert_ne!(
            updated_bg.flags & BG_FLAG_ASSIGNMENT_MISMATCH,
            0,
            "BG should have ASSIGNMENT_MISMATCH flag set"
        );
    }
}
