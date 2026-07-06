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

use self::hash_placement_rule::HashPlacementRuleChecker;
use self::hash_primary_validity::HashPrimaryValidityChecker;
use self::hash_replica::HashReplicaChecker;
use self::pool_membership::PoolMembershipChecker;
use crate::pd::config::keys;
use crate::pd::coordinator::{BGOperator, CoordinatorContext};
use curvine_common::state::{BlockGroupInfo, NodeInfo, NodeState};

pub mod hash_placement_rule;
pub mod hash_primary_validity;
pub mod hash_replica;
pub mod pool_membership;

/// True iff a BG has a replica on a Decommission/Offline node AND the grace
/// window since the node entered that state has not yet elapsed.
pub(crate) fn bg_in_leaving_grace(bg: &BlockGroupInfo, ctx: &CoordinatorContext) -> bool {
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
    /// Primary invalid (orphaned or on dead worker) — affects data consistency.
    pub const PRIMARY_VALIDITY: u32 = 20;
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
    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &CoordinatorContext) -> Option<BGOperator>;

    /// Check a single worker. Returns an operator if the worker needs correction.
    fn check_worker(&self, _worker: &NodeInfo, _ctx: &CoordinatorContext) -> Option<BGOperator> {
        None
    }
}

/// Build the default set of checkers (sorted by priority).
pub fn default_checkers() -> Vec<Box<dyn Checker>> {
    let mut checkers: Vec<Box<dyn Checker>> = vec![
        Box::new(HashReplicaChecker),
        Box::new(HashPrimaryValidityChecker),
        Box::new(HashPlacementRuleChecker),
        Box::new(PoolMembershipChecker),
    ];
    checkers.sort_by_key(|c| c.priority());
    checkers
}

/// Shared test helpers for checker and scheduler tests.
#[cfg(test)]
pub mod tests_common {
    use crate::pd::bgtable::BGTable;
    use crate::pd::coordinator::CoordinatorContext;
    use curvine_common::state::{
        BGKind, BGOpState, BGPrimary, BGState, BgId, BlockGroupInfo, NodeAddress, NodeBase,
        NodeInfo, NodePayload, NodeState, NodeType, ReplicaState, StorageSpec, StorageType,
        TableId, WorkerNodePayload,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::Arc;

    pub fn test_context(overrides: HashMap<String, String>) -> Arc<CoordinatorContext> {
        test_context_with_labels(overrides, vec![])
    }

    pub fn test_context_with_labels(
        overrides: HashMap<String, String>,
        location_labels: Vec<String>,
    ) -> Arc<CoordinatorContext> {
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
        let pool_mgr = Arc::new(crate::pd::pool::PoolManager::new(node_mgr.clone()));
        let bg_store = Arc::new(crate::pd::bg::BGStore::new(store.clone()));
        let bg_mgr = Arc::new(crate::pd::bg::BGManager::new(bg_store, jc.clone()));
        let table_store = Arc::new(crate::pd::bgtable::BGTableStore::new(store));
        let bgtable_mgr = Arc::new(crate::pd::bgtable::BGTableManager::new(
            table_store,
            bg_mgr.clone(),
            pool_mgr.clone(),
            config.clone(),
            location_labels,
        ));
        let operator_controller = Arc::new(
            crate::pd::coordinator::operator_controller::OperatorController::new(
                config.clone(),
                bgtable_mgr.clone(),
            ),
        );
        Arc::new(CoordinatorContext {
            node_manager: node_mgr,
            pool_manager: pool_mgr,
            bgtable_manager: bgtable_mgr,
            config_manager: config,
            operator_controller,
            runtime: Arc::new(orpc::runtime::Runtime::new("test", 1, 1)),
        })
    }

    /// End-to-end fixture that wires up a seeded pool with workers, a table, and BGs.
    /// Used by checker/scheduler tests to avoid per-test boilerplate.
    pub struct Fixture {
        pub ctx: Arc<CoordinatorContext>,
        next_table_id: AtomicU16,
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
            Self {
                ctx,
                next_table_id: AtomicU16::new(1),
            }
        }

        /// Register a worker as Live, assign labels, and add storage specs for the pool.
        pub fn add_worker(&self, worker_id: u32, pool_type: StorageType, labels: &[(&str, &str)]) {
            let mut label_map = HashMap::new();
            for (k, v) in labels {
                label_map.insert(k.to_string(), v.to_string());
            }
            let mut payload = WorkerNodePayload::default();
            payload.storage_specs.insert(
                "s0".to_string(),
                StorageSpec {
                    dir_id: 0,
                    storage_id: "s0".to_string(),
                    failed: false,
                    storage_type: pool_type,
                    dir_path: "/tmp/s0".to_string(),
                },
            );
            let storage_specs = payload.storage_specs.clone();
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
                payload: NodePayload::Worker(payload),
                ..Default::default()
            });
            self.ctx
                .pool_manager
                .assign_worker_to_pools(worker_id, &storage_specs)
                .unwrap();
        }

        /// Register multiple workers (no labels).
        pub fn add_workers(&self, worker_ids: &[u32], pool_type: StorageType) {
            for &wid in worker_ids {
                self.add_worker(wid, pool_type, &[]);
            }
        }

        /// Transition a worker's node state (e.g. Lost, Decommission).
        pub fn set_worker_state(&self, worker_id: u32, state: NodeState) {
            let mut node = self.ctx.node_manager.get_node(worker_id).expect("worker");
            node.state = state;
            node.state_since_ms = orpc::common::LocalTime::mills();
            self.ctx.node_manager.test_insert_node(node);
        }

        /// Insert a BGTable for (pool_type, replica_count).
        pub fn insert_table(&self, pool_type: StorageType, replica_count: u16) -> TableId {
            let table_id = self.next_table_id.fetch_add(1, Ordering::Relaxed);
            self.ctx
                .bgtable_manager
                .test_insert_table(BGTable::new_hash_table_with_config(
                    table_id,
                    0,
                    pool_type,
                    replica_count,
                    vec![],
                    vec![],
                    Default::default(),
                ));
            table_id
        }

        /// Create a BG with the given replica_set. `primary` defaults to the first replica.
        pub fn insert_bg<B: TryInto<BgId> + Copy>(
            &self,
            bg_id: B,
            table_id: TableId,
            replica_set: Vec<u32>,
            primary: Option<u32>,
        ) -> BlockGroupInfo {
            let bg_id = bg_id.try_into().ok().expect("bg_id out of range");
            let primary_node = primary
                .or_else(|| replica_set.first().copied())
                .unwrap_or(0);
            let bg = BlockGroupInfo {
                bg_id,
                table_id,
                kind: BGKind::Hash,
                bg_epoch: 1,
                replica_set: replica_set.clone(),
                isr: replica_set.clone(),
                state: BGState::Active,
                op_state: BGOpState::Idle,
                primary: BGPrimary {
                    node_id: primary_node,
                    epoch: 1,
                    grant_time_ms: 0,
                },
                stats: Default::default(),
                replicas: Default::default(),
            };
            self.ctx
                .bgtable_manager
                .test_seed_bg(bg.clone())
                .unwrap();
            bg
        }

        /// Set replica states for the given BG. All others default to Pending.
        pub fn set_replica_states<B: TryInto<BgId> + Copy>(
            &self,
            bg_id: B,
            states: &[(u32, ReplicaState)],
        ) {
            let bg_id = bg_id.try_into().ok().expect("bg_id out of range");
            for &(wid, st) in states {
                self.ctx
                    .bgtable_manager
                    .bg()
                    .set_replica_state(BGKind::Hash, bg_id, wid, st);
            }
        }

        /// Convenience: mark every replica in the BG's replica_set as Active.
        /// Balance schedulers require serving.len() == replica_set.len() to consider a BG.
        pub fn activate_all_replicas<B: TryInto<BgId> + Copy>(&self, bg_id: B) {
            let bg_id = bg_id.try_into().ok().expect("bg_id out of range");
            let bg = self.ctx.bgtable_manager.bg().get_bg(BGKind::Hash, bg_id).expect("bg");
            for wid in bg.replica_set.iter().copied() {
                self.ctx.bgtable_manager.bg().set_replica_state(
                    BGKind::Hash,
                    bg_id,
                    wid,
                    ReplicaState::Active,
                );
            }
        }

        pub fn set_table_buckets<B: TryInto<BgId> + Copy>(&self, table_id: TableId, bg_ids: &[B]) {
            let mut table = (*self
                .ctx
                .bgtable_manager
                .get_table(table_id)
                .expect("table must exist"))
            .clone();
            let buckets = bg_ids
                .iter()
                .map(|id| (*id).try_into().ok().expect("bg_id out of range"))
                .collect();
            match &mut table {
                BGTable::Hash(hash_table) => hash_table.set_buckets(buckets),
                BGTable::Capacity(_) => panic!("expected hash table"),
            }
            self.ctx.bgtable_manager.test_insert_table(table);
        }
    }

    /// Decompose an operator into (AddReplica workers, RemoveReplica workers, TransferPrimary pairs).
    /// Used by checker/scheduler tests to assert op shape without locking exact step order.
    pub fn decompose(
        op: &crate::pd::coordinator::BGOperator,
    ) -> (Vec<u32>, Vec<u32>, Vec<(u32, u32)>) {
        use crate::pd::coordinator::OpStep;
        let mut add = Vec::new();
        let mut remove = Vec::new();
        let mut transfer = Vec::new();
        for step in &op.steps {
            match step {
                OpStep::AddReplica { worker_id } => add.push(*worker_id),
                OpStep::RemoveReplica { worker_id } => remove.push(*worker_id),
                OpStep::TransferPrimary {
                    from_worker,
                    to_worker,
                } => transfer.push((*from_worker, *to_worker)),
                OpStep::WaitReplicaReady { .. } | OpStep::SealBG | OpStep::DeleteBG => {}
            }
        }
        (add, remove, transfer)
    }
}
