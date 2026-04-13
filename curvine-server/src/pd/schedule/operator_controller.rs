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

use super::operator::{BGCommands, BGOperator, OpStatus, OpStep};
use crate::pd::bg::BGManager;
use crate::pd::config::ConfigManager;
use crate::pd::pd_server::Pd;
use curvine_common::state::BGOpState;
use dashmap::DashMap;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

// ========== Token Bucket ==========

struct TokenBucket {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last_refill_ms: u64,
}

impl TokenBucket {
    fn new(rate: f64, capacity: f64) -> Self {
        Self {
            rate,
            capacity,
            tokens: capacity,
            last_refill_ms: 0,
        }
    }

    fn refill(&mut self, now_ms: u64) {
        if now_ms <= self.last_refill_ms {
            return;
        }
        let elapsed_sec = (now_ms - self.last_refill_ms) as f64 / 1000.0;
        self.tokens = (self.tokens + elapsed_sec * self.rate).min(self.capacity);
        self.last_refill_ms = now_ms;
    }

    fn try_consume(&mut self, cost: f64, now_ms: u64) -> bool {
        self.refill(now_ms);
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }
}

// ========== Store Limiter ==========

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StoreLimitType {
    AddReplica,
    RemoveReplica,
}

struct StoreLimiter {
    buckets: Mutex<std::collections::HashMap<(u32, StoreLimitType), TokenBucket>>,
    config_manager: Arc<ConfigManager>,
}

impl StoreLimiter {
    fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self {
            buckets: Mutex::new(std::collections::HashMap::new()),
            config_manager,
        }
    }

    fn get_config(&self, limit_type: StoreLimitType) -> (f64, f64) {
        match limit_type {
            StoreLimitType::AddReplica => {
                let rate = self.config_manager.get_u32(
                    crate::pd::config::keys::PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_RATE,
                    crate::pd::config::keys::PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_RATE_DEFAULT,
                ) as f64;
                let capacity = self.config_manager.get_u32(
                    crate::pd::config::keys::PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_CAPACITY,
                    crate::pd::config::keys::PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_CAPACITY_DEFAULT,
                ) as f64;
                (rate, capacity)
            }
            StoreLimitType::RemoveReplica => {
                let rate = self.config_manager.get_u32(
                    crate::pd::config::keys::PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_RATE,
                    crate::pd::config::keys::PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_RATE_DEFAULT,
                ) as f64;
                let capacity = self.config_manager.get_u32(
                    crate::pd::config::keys::PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_CAPACITY,
                    crate::pd::config::keys::PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_CAPACITY_DEFAULT,
                ) as f64;
                (rate, capacity)
            }
        }
    }

    /// Check whether an operator can proceed without consuming tokens.
    fn check_operator(&self, op: &BGOperator, now_ms: u64) -> bool {
        let mut buckets = self.buckets.lock().unwrap();
        for step in &op.steps {
            match step {
                OpStep::AddReplica { worker_id } => {
                    let key = (*worker_id, StoreLimitType::AddReplica);
                    let (rate, capacity) = self.get_config(StoreLimitType::AddReplica);
                    let bucket = buckets.entry(key).or_insert_with(|| TokenBucket::new(rate, capacity));
                    bucket.refill(now_ms);
                    if bucket.tokens < 1.0 {
                        return false;
                    }
                }
                OpStep::RemoveReplica { worker_id } => {
                    let key = (*worker_id, StoreLimitType::RemoveReplica);
                    let (rate, capacity) = self.get_config(StoreLimitType::RemoveReplica);
                    let bucket = buckets.entry(key).or_insert_with(|| TokenBucket::new(rate, capacity));
                    bucket.refill(now_ms);
                    if bucket.tokens < 1.0 {
                        return false;
                    }
                }
                OpStep::TransferLease { .. } => {} // lease transfers are not rate-limited
            }
        }
        true
    }

    /// Consume tokens for an operator's steps.
    fn consume_operator(&self, op: &BGOperator, now_ms: u64) {
        let mut buckets = self.buckets.lock().unwrap();
        for step in &op.steps {
            match step {
                OpStep::AddReplica { worker_id } => {
                    let key = (*worker_id, StoreLimitType::AddReplica);
                    let (rate, capacity) = self.get_config(StoreLimitType::AddReplica);
                    let bucket = buckets.entry(key).or_insert_with(|| TokenBucket::new(rate, capacity));
                    bucket.try_consume(1.0, now_ms);
                }
                OpStep::RemoveReplica { worker_id } => {
                    let key = (*worker_id, StoreLimitType::RemoveReplica);
                    let (rate, capacity) = self.get_config(StoreLimitType::RemoveReplica);
                    let bucket = buckets.entry(key).or_insert_with(|| TokenBucket::new(rate, capacity));
                    bucket.try_consume(1.0, now_ms);
                }
                OpStep::TransferLease { .. } => {}
            }
        }
    }
}

/// Wrapper to make BinaryHeap a max-heap by priority then by oldest first
struct PriorityOperator(BGOperator);

impl PartialEq for PriorityOperator {
    fn eq(&self, other: &Self) -> bool {
        self.0.priority == other.0.priority && self.0.create_time_ms == other.0.create_time_ms
    }
}

impl Eq for PriorityOperator {}

impl PartialOrd for PriorityOperator {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityOperator {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.0.priority.cmp(&other.0.priority) {
            Ordering::Equal => other.0.create_time_ms.cmp(&self.0.create_time_ms),
            o => o,
        }
    }
}

pub struct OperatorController {
    waiting_operators: Mutex<BinaryHeap<PriorityOperator>>,
    running_operators: DashMap<u32, BGOperator>,
    /// Per-worker running operator count for concurrency limiting
    worker_op_count: DashMap<u32, u32>,
    config_manager: Arc<ConfigManager>,
    bg_manager: Arc<BGManager>,
    next_op_id: AtomicU64,
    store_limiter: StoreLimiter,
}

impl OperatorController {
    pub fn new(config_manager: Arc<ConfigManager>, bg_manager: Arc<BGManager>) -> Self {
        let store_limiter = StoreLimiter::new(config_manager.clone());
        Self {
            waiting_operators: Mutex::new(BinaryHeap::new()),
            running_operators: DashMap::new(),
            worker_op_count: DashMap::new(),
            config_manager,
            bg_manager,
            next_op_id: AtomicU64::new(1),
            store_limiter,
        }
    }

    pub fn add_operator(&self, op: BGOperator) -> bool {
        let max_waiting = self
            .config_manager
            .get_u32("pd.schedule.max_waiting_operators", 100);

        let mut queue = self.waiting_operators.lock().unwrap();

        // Priority replacement: if a running operator exists for this BG,
        // replace it only if the new operator has higher priority.
        if let Some(existing) = self.running_operators.get(&op.bg_id) {
            if op.priority > existing.priority {
                // New operator has higher priority -> replace
                drop(existing);
                if let Some((_, mut old_op)) = self.running_operators.remove(&op.bg_id) {
                    old_op.status = OpStatus::Replaced;
                    self.decrement_worker_counts(&old_op);
                    self.bg_manager
                        .set_op_state(op.bg_id, curvine_common::state::BGOpState::Idle);
                    self.bg_manager.mark_suspect(op.bg_id);
                }
            } else {
                return false;
            }
        }

        if queue.len() >= max_waiting as usize {
            return false;
        }
        queue.push(PriorityOperator(op));
        true
    }

    /// Dispatch next batch of operators from waiting to running.
    /// Respects per-worker concurrency limit and store rate limiting.
    pub fn dispatch_next(&self) -> Vec<BGOperator> {
        let max_per_worker = self
            .config_manager
            .get_u32("pd.schedule.max_operators_per_worker", 5);
        let now_ms = orpc::common::LocalTime::mills();

        let mut queue = self.waiting_operators.lock().unwrap();
        let mut to_dispatch = Vec::new();
        let mut deferred = Vec::new();

        while let Some(PriorityOperator(op)) = queue.pop() {
            if self.running_operators.contains_key(&op.bg_id) {
                continue;
            }

            // Check per-worker concurrency limit
            let workers = Self::workers_in_op(&op);
            let exceeds_limit = workers.iter().any(|w| {
                self.worker_op_count
                    .get(w)
                    .map(|c| *c >= max_per_worker)
                    .unwrap_or(false)
            });
            if exceeds_limit {
                deferred.push(PriorityOperator(op));
                continue;
            }

            // Check store rate limit (soft limit)
            if !self.store_limiter.check_operator(&op, now_ms) {
                deferred.push(PriorityOperator(op));
                continue;
            }

            // Consume tokens
            self.store_limiter.consume_operator(&op, now_ms);

            let mut op = op;
            op.status = OpStatus::Running;
            op.step_start_time_ms = orpc::common::LocalTime::mills();

            // Set BG op_state to reflect the operation in progress
            self.bg_manager.set_op_state(op.bg_id, op.bg_op_state());
            self.increment_worker_counts(&op);
            self.running_operators.insert(op.bg_id, op.clone());
            to_dispatch.push(op);
        }

        // Put deferred operators back into the queue
        for op in deferred {
            queue.push(op);
        }

        to_dispatch
    }

    /// Build commands for a worker from running operators (add_bgs / remove_bgs).
    pub fn dispatch_to_worker(&self, worker_id: u32) -> BGCommands {
        let mut add_bgs = Vec::new();
        let mut remove_bgs = Vec::new();
        for mut entry in self.running_operators.iter_mut() {
            let op = entry.value_mut();
            let step = match op.steps.get(op.current_step) {
                Some(s) => s,
                None => continue,
            };
            match step {
                OpStep::AddReplica { worker_id: w } if *w == worker_id => {
                    if let Some(bg) = self.bg_manager.get_bg(op.bg_id) {
                        add_bgs.push(bg);
                    }
                }
                OpStep::RemoveReplica { worker_id: w } if *w == worker_id => {
                    remove_bgs.push(op.bg_id);
                }
                _ => {}
            }
        }
        BGCommands { add_bgs, remove_bgs }
    }

    /// Check progress via step.is_finish(bg) and handle per-step timeouts.
    /// Also detects stale operators (epoch mismatch or max lifetime exceeded).
    /// On terminal states: resets BG op_state to Idle and refreshes flags on success.
    pub fn check_progress(&self, now_ms: u64) {
        let max_lifetime = self.config_manager.get_u64(
            crate::pd::config::keys::PD_SCHEDULE_OPERATOR_MAX_LIFETIME_MS,
            crate::pd::config::keys::PD_SCHEDULE_OPERATOR_MAX_LIFETIME_MS_DEFAULT,
        );

        let mut to_remove = Vec::new();
        for mut entry in self.running_operators.iter_mut() {
            let op = entry.value_mut();

            // Check operator max lifetime
            if now_ms.saturating_sub(op.create_time_ms) > max_lifetime {
                log::warn!(
                    "Operator {} for bg {} exceeded max lifetime {}ms, cancelling",
                    op.id, op.bg_id, max_lifetime
                );
                op.status = OpStatus::Cancelled;
                to_remove.push(op.bg_id);
                continue;
            }

            // Check bg_epoch staleness (skip if bg_epoch==0 for backward compat)
            if op.bg_epoch > 0 {
                if let Some(bg) = self.bg_manager.get_bg(op.bg_id) {
                    if bg.bg_epoch != op.bg_epoch {
                        log::warn!(
                            "Operator {} for bg {} stale: op_epoch={} current_epoch={}, cancelling",
                            op.id, op.bg_id, op.bg_epoch, bg.bg_epoch
                        );
                        op.status = OpStatus::Cancelled;
                        to_remove.push(op.bg_id);
                        continue;
                    }
                }
            }

            // Check per-step timeout based on step type
            let step_timeout = match op.steps.get(op.current_step) {
                Some(OpStep::TransferLease { .. }) => self.config_manager.get_u64(
                    crate::pd::config::keys::PD_SCHEDULE_STEP_TIMEOUT_TRANSFER_LEASE_MS,
                    crate::pd::config::keys::PD_SCHEDULE_STEP_TIMEOUT_TRANSFER_LEASE_MS_DEFAULT,
                ),
                Some(OpStep::AddReplica { .. }) => self.config_manager.get_u64(
                    crate::pd::config::keys::PD_SCHEDULE_STEP_TIMEOUT_ADD_REPLICA_MS,
                    crate::pd::config::keys::PD_SCHEDULE_STEP_TIMEOUT_ADD_REPLICA_MS_DEFAULT,
                ),
                Some(OpStep::RemoveReplica { .. }) => self.config_manager.get_u64(
                    crate::pd::config::keys::PD_SCHEDULE_STEP_TIMEOUT_REMOVE_REPLICA_MS,
                    crate::pd::config::keys::PD_SCHEDULE_STEP_TIMEOUT_REMOVE_REPLICA_MS_DEFAULT,
                ),
                None => 3_000,
            };
            if now_ms.saturating_sub(op.step_start_time_ms) > step_timeout {
                op.status = OpStatus::Timeout;
                to_remove.push(op.bg_id);
                continue;
            }

            // Check step completion via is_finish(bg)
            if let Some(step) = op.steps.get(op.current_step) {
                if let Some(bg) = self.bg_manager.get_bg(op.bg_id) {
                    if step.is_finish(&bg) {
                        op.current_step += 1;
                        op.step_start_time_ms = now_ms;
                    }
                }
            }

            if op.current_step >= op.steps.len() {
                op.status = OpStatus::Success;
                to_remove.push(op.bg_id);
            }
        }

        // Terminal state cleanup
        for bg_id in to_remove {
            if let Some((_, op)) = self.running_operators.remove(&bg_id) {
                Pd::get_metrics()
                    .operator_finish_total
                    .with_label_values(&[op.status.as_str()])
                    .inc();
                self.decrement_worker_counts(&op);
                // Reset op_state to Idle so checkers can re-evaluate this BG
                self.bg_manager.set_op_state(bg_id, BGOpState::Idle);
                // Mark BG as suspect so checkers re-evaluate it promptly
                self.bg_manager.mark_suspect(bg_id);
            }
        }
    }

    pub fn running_count(&self) -> usize {
        self.running_operators.len()
    }

    pub fn waiting_count(&self) -> usize {
        self.waiting_operators.lock().unwrap().len()
    }

    /// Check if there are any running operators involving the given node.
    pub fn has_running_operators_for_node(&self, node_id: u32) -> bool {
        self.running_operators.iter().any(|entry| {
            let op = entry.value();
            op.steps.iter().any(|step| match step {
                OpStep::AddReplica { worker_id } => *worker_id == node_id,
                OpStep::RemoveReplica { worker_id } => *worker_id == node_id,
                OpStep::TransferLease {
                    from_worker,
                    to_worker,
                } => *from_worker == node_id || *to_worker == node_id,
            })
        })
    }

    /// Generate next operator id
    pub fn next_operator_id(&self) -> u64 {
        self.next_op_id.fetch_add(1, AtomicOrdering::SeqCst)
    }

    /// Get the net BG count influence of all running operators on a worker.
    pub fn get_bg_influence(&self, worker_id: u32) -> i32 {
        let mut delta = 0i32;
        for entry in self.running_operators.iter() {
            let influence = entry.value().compute_influence();
            delta += influence.bg_count_delta.get(&worker_id).copied().unwrap_or(0);
        }
        delta
    }

    /// Get the net leader count influence of all running operators on a worker.
    pub fn get_leader_influence(&self, worker_id: u32) -> i32 {
        let mut delta = 0i32;
        for entry in self.running_operators.iter() {
            let influence = entry.value().compute_influence();
            delta += influence.leader_count_delta.get(&worker_id).copied().unwrap_or(0);
        }
        delta
    }

    // ========== Per-worker concurrency helpers ==========

    fn workers_in_op(op: &BGOperator) -> Vec<u32> {
        let mut workers = Vec::new();
        for step in &op.steps {
            match step {
                OpStep::AddReplica { worker_id } => workers.push(*worker_id),
                OpStep::RemoveReplica { worker_id } => workers.push(*worker_id),
                OpStep::TransferLease {
                    from_worker,
                    to_worker,
                } => {
                    workers.push(*from_worker);
                    workers.push(*to_worker);
                }
            }
        }
        workers.sort_unstable();
        workers.dedup();
        workers
    }

    fn increment_worker_counts(&self, op: &BGOperator) {
        for w in Self::workers_in_op(op) {
            *self.worker_op_count.entry(w).or_insert(0) += 1;
        }
    }

    fn decrement_worker_counts(&self, op: &BGOperator) {
        for w in Self::workers_in_op(op) {
            if let Some(mut count) = self.worker_op_count.get_mut(&w) {
                *count = count.saturating_sub(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::node::NodeManager;
    use crate::pd::pool::PoolManager;

    fn test_controller_with_config(
        overrides: std::collections::HashMap<String, String>,
    ) -> (OperatorController, Arc<ConfigManager>, Arc<BGManager>) {
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let raft = curvine_common::raft::RaftClient::from_conf(
            curvine_common::conf::JournalConf::default().create_runtime(),
            &curvine_common::conf::JournalConf::default(),
        );
        let jc = Arc::new(crate::pd::journal::Client::new(raft));
        let config = Arc::new(ConfigManager::new(store.clone(), jc.clone(), overrides));
        let node_store = Arc::new(crate::pd::node::NodeStore::new(store.clone()));
        let node_mgr = Arc::new(NodeManager::new(node_store, config.clone(), jc.clone()));
        let pool_store = Arc::new(crate::pd::pool::PoolStore::new(store.clone()));
        let pool_mgr = Arc::new(PoolManager::new(pool_store, node_mgr, jc.clone()));
        let bg_store = Arc::new(crate::pd::bg::BGStore::new(store));
        let bg_mgr = Arc::new(BGManager::new(bg_store, pool_mgr, jc, 1024, vec![3], vec![]));
        let ctrl = OperatorController::new(config.clone(), bg_mgr.clone());
        (ctrl, config, bg_mgr)
    }

    fn test_controller() -> (OperatorController, Arc<ConfigManager>, Arc<BGManager>) {
        test_controller_with_config(std::collections::HashMap::new())
    }

    fn make_op(id: u64, bg_id: u32, steps: Vec<OpStep>, priority: u32) -> BGOperator {
        BGOperator {
            id,
            kind: super::super::operator::OperatorKind::Repair,
            bg_id,
            description: "test".to_string(),
            steps,
            current_step: 0,
            status: OpStatus::Pending,
            create_time_ms: 0,
            step_start_time_ms: 0,
            priority,
            bg_epoch: 0,
        }
    }

    #[test]
    fn add_and_dispatch_operator() {
        let (ctrl, _config, _bg_mgr) = test_controller();
        let op = make_op(1, 10, vec![], 1);
        assert!(ctrl.add_operator(op));
        assert_eq!(ctrl.waiting_count(), 1);
        let dispatched = ctrl.dispatch_next();
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].bg_id, 10);
        assert_eq!(ctrl.waiting_count(), 0);
        assert_eq!(ctrl.running_count(), 1);
    }

    #[test]
    fn check_progress_advances_step_via_is_finish() {
        let (ctrl, _config, bg_mgr) = test_controller();

        // Create a BG with worker 1 in replica_set so AddReplica is_finish returns true
        use curvine_common::state::{BGLease, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 10,
            table_id: 1,
            bg_epoch: 1,
            replica_set: vec![1],
            state: BGState::Active,
            op_state: Default::default(),
            lease_owner: Some(BGLease {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&crate::pd::journal::entry::BGEntry { op_ms: 0, info: bg })
            .unwrap();

        let op = make_op(42, 10, vec![OpStep::AddReplica { worker_id: 1 }], 1);
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();

        // check_progress should advance step since worker 1 is in replica_set
        ctrl.check_progress(1);
        // Operator should be completed and removed; op_state reset to Idle
        assert_eq!(ctrl.running_count(), 0);
        let bg = bg_mgr.get_bg(10).unwrap();
        assert_eq!(bg.op_state, BGOpState::Idle);
    }

    #[test]
    fn check_progress_per_step_timeout() {
        let (ctrl, _config, _bg_mgr) = test_controller();
        let op = make_op(43, 11, vec![OpStep::AddReplica { worker_id: 1 }], 1);
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();
        // step_start_time_ms is set to current time during dispatch_next,
        // but in tests it will be near 0 or current. Simulate by calling check_progress
        // with a time far in the future (700s after step start).
        // Since step_start_time_ms is set during dispatch, we need to check relative to that.
        // In test, dispatch_next sets step_start_time_ms = LocalTime::mills() (real clock).
        // We'll just use a huge now_ms to ensure timeout.
        ctrl.check_progress(orpc::common::LocalTime::mills() + 700_000);
        assert_eq!(ctrl.running_count(), 0);
    }

    #[test]
    fn per_worker_concurrency_limit() {
        let overrides: std::collections::HashMap<String, String> = [(
            "pd.schedule.max_operators_per_worker".to_string(),
            "1".to_string(),
        )]
        .into_iter()
        .collect();
        let (ctrl, _config, _bg_mgr) = test_controller_with_config(overrides);

        let op1 = make_op(1, 10, vec![OpStep::AddReplica { worker_id: 100 }], 10);
        let op2 = make_op(2, 11, vec![OpStep::AddReplica { worker_id: 100 }], 5);
        assert!(ctrl.add_operator(op1));
        assert!(ctrl.add_operator(op2));

        let dispatched = ctrl.dispatch_next();
        // Only 1 should be dispatched due to per-worker limit
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].bg_id, 10); // higher priority
        assert_eq!(ctrl.running_count(), 1);
        assert_eq!(ctrl.waiting_count(), 1); // op2 deferred
    }

    #[test]
    fn reject_duplicate_bg_id_when_running() {
        let (ctrl, _config, _bg_mgr) = test_controller();
        let op1 = make_op(1, 10, vec![OpStep::AddReplica { worker_id: 1 }], 1);
        assert!(ctrl.add_operator(op1));
        ctrl.dispatch_next(); // op1 now running for bg_id=10

        // Adding another operator for the same bg_id should be rejected
        let op2 = make_op(2, 10, vec![OpStep::AddReplica { worker_id: 2 }], 1);
        assert!(!ctrl.add_operator(op2));
    }

    #[test]
    fn priority_ordering_higher_first() {
        let (ctrl, _config, _bg_mgr) = test_controller();
        let low = make_op(1, 10, vec![], 1);
        let high = make_op(2, 11, vec![], 100);
        let mid = make_op(3, 12, vec![], 50);
        assert!(ctrl.add_operator(low));
        assert!(ctrl.add_operator(high));
        assert!(ctrl.add_operator(mid));

        let dispatched = ctrl.dispatch_next();
        assert_eq!(dispatched.len(), 3);
        // Higher priority dispatched first
        assert_eq!(dispatched[0].bg_id, 11); // priority 100
        assert_eq!(dispatched[1].bg_id, 12); // priority 50
        assert_eq!(dispatched[2].bg_id, 10); // priority 1
    }

    #[test]
    fn max_waiting_queue_limit() {
        let overrides: std::collections::HashMap<String, String> = [(
            "pd.schedule.max_waiting_operators".to_string(),
            "2".to_string(),
        )]
        .into_iter()
        .collect();
        let (ctrl, _config, _bg_mgr) = test_controller_with_config(overrides);

        let op1 = make_op(1, 10, vec![], 1);
        let op2 = make_op(2, 11, vec![], 1);
        let op3 = make_op(3, 12, vec![], 1);
        assert!(ctrl.add_operator(op1));
        assert!(ctrl.add_operator(op2));
        assert!(!ctrl.add_operator(op3)); // rejected: queue full
        assert_eq!(ctrl.waiting_count(), 2);
    }

    #[test]
    fn dispatch_to_worker_add_and_remove() {
        let (ctrl, _config, bg_mgr) = test_controller();

        // Create a BG so dispatch_to_worker can find it
        use curvine_common::state::{BGLease, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 20,
            table_id: 1,
            bg_epoch: 1,
            replica_set: vec![1, 2],
            state: BGState::Active,
            op_state: Default::default(),
            lease_owner: Some(BGLease {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&crate::pd::journal::entry::BGEntry { op_ms: 0, info: bg })
            .unwrap();

        // Operator with AddReplica step targeting worker 5
        let op = make_op(
            1,
            20,
            vec![OpStep::AddReplica { worker_id: 5 }],
            1,
        );
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();

        // Worker 5 should get the add command
        let cmds = ctrl.dispatch_to_worker(5);
        assert_eq!(cmds.add_bgs.len(), 1);
        assert_eq!(cmds.add_bgs[0].bg_id, 20);

        // Worker 99 should get nothing
        let cmds2 = ctrl.dispatch_to_worker(99);
        assert!(cmds2.add_bgs.is_empty());
        assert!(cmds2.remove_bgs.is_empty());
    }

    #[test]
    fn operator_success_resets_op_state_to_idle() {
        let (ctrl, _config, bg_mgr) = test_controller();

        use curvine_common::state::{BGLease, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 30,
            table_id: 1,
            bg_epoch: 1,
            replica_set: vec![1],
            state: BGState::Active,
            op_state: Default::default(),
            lease_owner: Some(BGLease {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&crate::pd::journal::entry::BGEntry {
                op_ms: 0,
                info: bg,
            })
            .unwrap();

        // Step already satisfied (worker 1 in replica_set)
        let op = make_op(1, 30, vec![OpStep::AddReplica { worker_id: 1 }], 1);
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();

        // After dispatch, BG op_state should be Recovering
        let bg_state = bg_mgr.get_bg(30).unwrap();
        assert_eq!(bg_state.op_state, BGOpState::Recovering);

        // check_progress completes the step
        ctrl.check_progress(1);
        assert_eq!(ctrl.running_count(), 0);
        let bg_after = bg_mgr.get_bg(30).unwrap();
        assert_eq!(bg_after.op_state, BGOpState::Idle);
    }

    #[test]
    fn check_progress_cancels_stale_epoch() {
        let (ctrl, _config, bg_mgr) = test_controller();

        use curvine_common::state::{BGLease, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 40,
            table_id: 1,
            bg_epoch: 1,
            replica_set: vec![1, 2],
            state: BGState::Active,
            op_state: Default::default(),
            lease_owner: Some(BGLease {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&crate::pd::journal::entry::BGEntry { op_ms: 0, info: bg })
            .unwrap();

        // Create operator with bg_epoch=1
        let mut op = make_op(1, 40, vec![OpStep::AddReplica { worker_id: 99 }], 1);
        op.bg_epoch = 1;
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();
        assert_eq!(ctrl.running_count(), 1);

        // Simulate epoch change by updating replica_set
        bg_mgr
            .apply_update_bg(&crate::pd::journal::entry::BGUpdateEntry {
                bg_id: 40,
                op_ms: 0,
                replica_set: Some(vec![1, 2, 3]),
                state: None,
                lease_owner: None,
                bg_epoch: None,
            })
            .unwrap();
        // bg_epoch should now be 2
        assert_eq!(bg_mgr.get_bg(40).unwrap().bg_epoch, 2);

        // check_progress should detect stale epoch and cancel
        ctrl.check_progress(orpc::common::LocalTime::mills());
        assert_eq!(ctrl.running_count(), 0);
    }

    #[test]
    fn check_progress_cancels_expired_operator() {
        let overrides: std::collections::HashMap<String, String> = [(
            "pd.schedule.operator_max_lifetime_ms".to_string(),
            "1000".to_string(), // 1 second max lifetime
        )]
        .into_iter()
        .collect();
        let (ctrl, _config, _bg_mgr) = test_controller_with_config(overrides);

        let mut op = make_op(1, 50, vec![OpStep::AddReplica { worker_id: 1 }], 1);
        op.create_time_ms = 1000; // created at t=1000
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();
        assert_eq!(ctrl.running_count(), 1);

        // Check at t=3000 (2s after creation, exceeds 1s max lifetime)
        ctrl.check_progress(3000);
        assert_eq!(ctrl.running_count(), 0);
    }

    #[test]
    fn token_bucket_refill_and_consume() {
        let mut bucket = TokenBucket::new(10.0, 5.0); // 10 tokens/sec, capacity 5
        bucket.last_refill_ms = 1000;

        // Consume all 5 tokens
        assert!(bucket.try_consume(5.0, 1000));
        assert!(!bucket.try_consume(1.0, 1000)); // empty

        // After 500ms, should have refilled 5 tokens (10/sec * 0.5s = 5)
        assert!(bucket.try_consume(5.0, 1500));
        assert!(!bucket.try_consume(1.0, 1500)); // empty again
    }

    #[test]
    fn store_limit_defers_when_exhausted() {
        // Set store limit capacity to 1 for AddReplica
        let overrides: std::collections::HashMap<String, String> = [
            (
                "pd.schedule.store_limit.add_replica_rate".to_string(),
                "1".to_string(),
            ),
            (
                "pd.schedule.store_limit.add_replica_capacity".to_string(),
                "1".to_string(),
            ),
        ]
        .into_iter()
        .collect();
        let (ctrl, _config, _bg_mgr) = test_controller_with_config(overrides);

        // Two operators targeting the same worker with AddReplica
        let op1 = make_op(1, 10, vec![OpStep::AddReplica { worker_id: 100 }], 10);
        let op2 = make_op(2, 11, vec![OpStep::AddReplica { worker_id: 100 }], 5);
        assert!(ctrl.add_operator(op1));
        assert!(ctrl.add_operator(op2));

        let dispatched = ctrl.dispatch_next();
        // Only 1 should be dispatched (capacity=1), the other deferred
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].bg_id, 10); // higher priority
        assert_eq!(ctrl.waiting_count(), 1); // op2 deferred
    }
}
