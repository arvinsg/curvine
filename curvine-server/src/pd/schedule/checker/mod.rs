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

use self::lease_validity::LeaseValidityChecker;
use self::placement_rule::PlacementRuleChecker;
use self::pool_membership::PoolMembershipChecker;
use self::replica::ReplicaChecker;
use crate::pd::config::keys;
use crate::pd::schedule::{BGOperator, ManagerContext};
use curvine_common::state::{BlockGroupInfo, NodeInfo, NodeState};

pub mod lease_validity;
pub mod placement_rule;
pub mod pool_membership;
pub mod replica;

/// True iff a BG has a replica on a Decommission/Offline node AND the grace
/// window since the node entered that state has not yet elapsed.
pub(crate) fn bg_in_leaving_grace(bg: &BlockGroupInfo, ctx: &ManagerContext) -> bool {
    let grace_ms = ctx
        .config_manager
        .get_u64(keys::PD_CHECKER_LEAVING_GRACE_MS);
    let now_ms = orpc::common::LocalTime::mills();
    bg.replica_set.iter().any(|&wid| {
        let Some(node) = ctx.node_manager.get_node(wid) else {
            return false;
        };
        if !matches!(node.state, NodeState::Offline | NodeState::Decommission) {
            return false;
        }
        node.state_since_ms == 0 || now_ms.saturating_sub(node.state_since_ms) < grace_ms
    })
}

/// Priority levels for `Checker::priority()`.
pub struct CheckerPriority;
impl CheckerPriority {
    /// Replica count wrong (under/over) — affects data availability.
    pub const REPLICA: u32 = 10;
    /// Lease invalid (orphaned or on dead worker) — affects data consistency.
    pub const LEASE_VALIDITY: u32 = 20;
    /// Placement rule violated — affects fault isolation but not data safety.
    pub const PLACEMENT_RULE: u32 = 30;
    /// Pool membership missing — diagnostic, runs after the BG-centric checkers.
    pub const POOL_MEMBERSHIP: u32 = 100;
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
        Box::new(PoolMembershipChecker),
    ];
    checkers.sort_by_key(|c| c.priority());
    checkers
}

/// Shared test helpers for checker and scheduler tests.
#[cfg(test)]
pub mod tests_common {
    use crate::pd::bg::{BGTable, BGTableStats};
    use crate::pd::journal::{BGEntry, PoolEntry};
    use crate::pd::pool::{POOL_ID_HDD, POOL_ID_MEM, POOL_ID_SSD};
    use crate::pd::schedule::ManagerContext;
    use curvine_common::state::{
        gen_table_id, table_id_replica_count, BGLease, BGOpState, BGState, BlockGroupInfo,
        NodeAddress, NodeBase, NodeInfo, NodePayload, NodeState, NodeType, PoolInfo, ReplicaState,
        StorageType, WorkerNodePayload,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    pub fn test_context(overrides: HashMap<String, String>) -> Arc<ManagerContext> {
        test_context_with_labels(overrides, vec![])
    }

    pub fn test_context_with_labels(
        overrides: HashMap<String, String>,
        location_labels: Vec<String>,
    ) -> Arc<ManagerContext> {
        crate::pd::pd_server::init_metrics_for_test();
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
            location_labels,
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

    /// End-to-end fixture that wires up a seeded pool with workers, a table, and BGs.
    /// Used by checker/scheduler tests to avoid per-test boilerplate.
    pub struct Fixture {
        pub ctx: Arc<ManagerContext>,
    }

    impl Fixture {
        pub fn new() -> Self {
            Self::with_overrides(HashMap::new())
        }

        pub fn with_overrides(overrides: HashMap<String, String>) -> Self {
            Self::build(overrides, vec![])
        }

        pub fn with_topology(
            location_labels: Vec<&'static str>,
            min_isolation_level: Option<&'static str>,
        ) -> Self {
            let mut overrides = HashMap::new();
            overrides.insert(
                crate::pd::config::keys::PD_BG_PLACEMENT_POLICY.to_string(),
                "topology_aware".to_string(),
            );
            if let Some(lvl) = min_isolation_level {
                overrides.insert(
                    crate::pd::config::keys::PD_BG_MIN_ISOLATION_LEVEL.to_string(),
                    lvl.to_string(),
                );
            }
            Self::build(
                overrides,
                location_labels.into_iter().map(String::from).collect(),
            )
        }

        fn build(overrides: HashMap<String, String>, location_labels: Vec<String>) -> Self {
            let ctx = test_context_with_labels(overrides, location_labels);
            for (pool_id, name, media) in [
                (POOL_ID_MEM, "mem_pool", StorageType::Mem),
                (POOL_ID_SSD, "ssd_pool", StorageType::Ssd),
                (POOL_ID_HDD, "hdd_pool", StorageType::Hdd),
            ] {
                ctx.pool_manager
                    .apply_save_pool(&PoolEntry {
                        op_ms: 0,
                        info: PoolInfo::new(pool_id, name.to_string(), media),
                    })
                    .unwrap();
            }
            Self { ctx }
        }

        /// Register a worker as Live, assign labels, and add to the pool.
        pub fn add_worker(&self, worker_id: u32, pool_id: u16, labels: &[(&str, &str)]) {
            let mut label_map = HashMap::new();
            for (k, v) in labels {
                label_map.insert(k.to_string(), v.to_string());
            }
            self.ctx.node_manager.test_insert_node(NodeInfo {
                base: NodeBase {
                    node_id: worker_id,
                    node_type: NodeType::Worker,
                    address: NodeAddress {
                        hostname: format!("w-{}", worker_id),
                        ip: format!("10.0.0.{}", worker_id),
                        rpc_port: 8000 + worker_id as u16,
                        web_port: 9000 + worker_id as u16,
                    },
                    labels: label_map,
                    ..Default::default()
                },
                state: NodeState::Live,
                payload: NodePayload::Worker(WorkerNodePayload::default()),
                ..Default::default()
            });
            let mut pool = self.ctx.pool_manager.get_pool(pool_id).unwrap();
            pool.workers.insert(worker_id);
            self.ctx
                .pool_manager
                .apply_save_pool(&PoolEntry {
                    op_ms: 0,
                    info: pool,
                })
                .unwrap();
        }

        /// Register multiple workers (no labels).
        pub fn add_workers(&self, worker_ids: &[u32], pool_id: u16) {
            for &wid in worker_ids {
                self.add_worker(wid, pool_id, &[]);
            }
        }

        /// Transition a worker's node state (e.g. Lost, Decommission).
        pub fn set_worker_state(&self, worker_id: u32, state: NodeState) {
            let mut node = self.ctx.node_manager.get_node(worker_id).expect("worker");
            node.state = state;
            node.state_since_ms = orpc::common::LocalTime::mills();
            self.ctx.node_manager.test_insert_node(node);
        }

        /// Insert a BGTable for (pool_id, replica_count). Returns the composed table_id.
        pub fn insert_table(&self, pool_id: u16, replica_count: u16) -> u32 {
            let table_id = gen_table_id(pool_id, replica_count);
            debug_assert_eq!(table_id_replica_count(table_id), replica_count);
            self.ctx.bg_manager.test_insert_table(BGTable {
                table_id,
                bucket_count: 16,
                buckets: vec![],
                epoch: 1,
                create_time_ms: 0,
                last_rebuild_ms: 0,
                stats: BGTableStats::default(),
            });
            table_id
        }

        /// Create a BG with the given replica_set. `lease_owner` defaults to the first replica.
        pub fn insert_bg(
            &self,
            bg_id: u32,
            table_id: u32,
            replica_set: Vec<u32>,
            lease_owner: Option<u32>,
        ) -> BlockGroupInfo {
            let leader = lease_owner
                .or_else(|| replica_set.first().copied())
                .unwrap_or(0);
            let bg = BlockGroupInfo {
                bg_id,
                table_id,
                bg_epoch: 1,
                replica_set: replica_set.clone(),
                state: BGState::Active,
                op_state: BGOpState::Idle,
                lease_owner: Some(BGLease {
                    node_id: leader,
                    epoch: 1,
                    grant_time_ms: 0,
                }),
                stats: Default::default(),
            };
            self.ctx
                .bg_manager
                .apply_create_bg(&BGEntry {
                    op_ms: 0,
                    info: bg.clone(),
                })
                .unwrap();
            bg
        }

        /// Set replica states for the given BG. All others default to Pending.
        pub fn set_replica_states(&self, bg_id: u32, states: &[(u32, ReplicaState)]) {
            for &(wid, st) in states {
                self.ctx.bg_manager.set_replica_state(bg_id, wid, st);
            }
        }

        /// Convenience: mark every replica in the BG's replica_set as Active.
        /// Balance schedulers require serving.len() == replica_set.len() to consider a BG.
        pub fn activate_all_replicas(&self, bg_id: u32) {
            let bg = self.ctx.bg_manager.get_bg(bg_id).expect("bg");
            for wid in bg.replica_set {
                self.ctx
                    .bg_manager
                    .set_replica_state(bg_id, wid, ReplicaState::Active);
            }
        }

        pub fn set_table_buckets(&self, table_id: u32, bg_ids: &[u32]) {
            let mut table = self
                .ctx
                .bg_manager
                .get_table(table_id)
                .expect("table must exist");
            table.buckets = bg_ids.to_vec();
            table.bucket_count = bg_ids.len() as u32;
            self.ctx.bg_manager.test_insert_table(table);
        }
    }

    /// Decompose an operator into (AddReplica workers, RemoveReplica workers, TransferLease pairs).
    /// Used by checker/scheduler tests to assert op shape without locking exact step order.
    pub fn decompose(
        op: &crate::pd::schedule::BGOperator,
    ) -> (Vec<u32>, Vec<u32>, Vec<(u32, u32)>) {
        use crate::pd::schedule::OpStep;
        let mut add = Vec::new();
        let mut remove = Vec::new();
        let mut transfer = Vec::new();
        for step in &op.steps {
            match step {
                OpStep::AddReplica { worker_id } => add.push(*worker_id),
                OpStep::RemoveReplica { worker_id } => remove.push(*worker_id),
                OpStep::TransferLease {
                    from_worker,
                    to_worker,
                } => transfer.push((*from_worker, *to_worker)),
                OpStep::WaitReplicaReady { .. } => {}
            }
        }
        (add, remove, transfer)
    }
}
