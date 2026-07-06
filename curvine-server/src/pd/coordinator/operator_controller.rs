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

use super::operator::{BGCommands, BGOperator, OpStatus, OpStep, OperatorClass};
use crate::pd::bgtable::BGTableManager;
use crate::pd::config::{keys, ConfigManager};
use crate::pd::journal::ApplyOutcome;
use crate::pd::pd_server::Pd;
use curvine_common::state::{BGKind, BGOpState, BgId, ReplicaState};
use curvine_common::FsResult;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StoreLimitType {
    AddReplica,
    RemoveReplica,
    TransferPrimary,
    /// Burst class for rebuild operators — separate quota allows fast bulk rebalance.
    RebuildAddReplica,
    RebuildRemoveReplica,
}

impl StoreLimitType {
    fn for_step(step: &OpStep, class: OperatorClass) -> Option<Self> {
        match (step, class) {
            (OpStep::AddReplica { .. }, OperatorClass::Normal) => Some(Self::AddReplica),
            (OpStep::RemoveReplica { .. }, OperatorClass::Normal) => Some(Self::RemoveReplica),
            (OpStep::AddReplica { .. }, OperatorClass::Burst) => Some(Self::RebuildAddReplica),
            (OpStep::RemoveReplica { .. }, OperatorClass::Burst) => {
                Some(Self::RebuildRemoveReplica)
            }
            (OpStep::TransferPrimary { .. }, _) => Some(Self::TransferPrimary),
            (OpStep::WaitReplicaReady { .. } | OpStep::SealBG | OpStep::DeleteBG, _) => None,
        }
    }

    fn worker_id(step: &OpStep) -> u32 {
        match step {
            OpStep::AddReplica { worker_id } => *worker_id,
            OpStep::RemoveReplica { worker_id } => *worker_id,
            OpStep::TransferPrimary { from_worker, .. } => *from_worker,
            OpStep::WaitReplicaReady { worker_id, .. } => *worker_id,
            OpStep::SealBG | OpStep::DeleteBG => 0,
        }
    }
}

struct StoreLimiter {
    buckets: Mutex<HashMap<(u32, StoreLimitType), TokenBucket>>,
    config_manager: Arc<ConfigManager>,
}

impl StoreLimiter {
    fn new(config_manager: Arc<ConfigManager>) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            config_manager,
        }
    }

    fn get_config(&self, limit_type: StoreLimitType) -> (f64, f64) {
        match limit_type {
            StoreLimitType::AddReplica => {
                let rate = self
                    .config_manager
                    .get_u32(keys::PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_RATE)
                    as f64;
                let capacity = self
                    .config_manager
                    .get_u32(keys::PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_CAPACITY)
                    as f64;
                (rate, capacity)
            }
            StoreLimitType::RemoveReplica => {
                let rate = self
                    .config_manager
                    .get_u32(keys::PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_RATE)
                    as f64;
                let capacity = self
                    .config_manager
                    .get_u32(keys::PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_CAPACITY)
                    as f64;
                (rate, capacity)
            }
            StoreLimitType::TransferPrimary => {
                let rate = self
                    .config_manager
                    .get_u32(keys::PD_SCHEDULE_STORE_LIMIT_TRANSFER_LEASE_RATE)
                    as f64;
                let capacity = self
                    .config_manager
                    .get_u32(keys::PD_SCHEDULE_STORE_LIMIT_TRANSFER_LEASE_CAPACITY)
                    as f64;
                (rate, capacity)
            }
            StoreLimitType::RebuildAddReplica | StoreLimitType::RebuildRemoveReplica => {
                let rate = self
                    .config_manager
                    .get_u32(keys::PD_SCHEDULE_STORE_LIMIT_REBUILD_RATE)
                    as f64;
                let capacity = self
                    .config_manager
                    .get_u32(keys::PD_SCHEDULE_STORE_LIMIT_REBUILD_CAPACITY)
                    as f64;
                (rate, capacity)
            }
        }
    }

    /// Check whether an operator can proceed without consuming tokens.
    fn check_operator(&self, op: &BGOperator, now_ms: u64) -> bool {
        let class = op.class();
        let mut buckets = self.buckets.lock().unwrap();
        for step in &op.steps {
            let limit_type = match StoreLimitType::for_step(step, class) {
                Some(t) => t,
                None => continue,
            };
            let key = (StoreLimitType::worker_id(step), limit_type);
            let (rate, capacity) = self.get_config(limit_type);
            let bucket = buckets
                .entry(key)
                .or_insert_with(|| TokenBucket::new(rate, capacity));
            bucket.refill(now_ms);
            if bucket.tokens < 1.0 {
                return false;
            }
        }
        true
    }

    /// Consume tokens for an operator's steps.
    fn consume_operator(&self, op: &BGOperator, now_ms: u64) {
        let class = op.class();
        let mut buckets = self.buckets.lock().unwrap();
        for step in &op.steps {
            let limit_type = match StoreLimitType::for_step(step, class) {
                Some(t) => t,
                None => continue,
            };
            let key = (StoreLimitType::worker_id(step), limit_type);
            let (rate, capacity) = self.get_config(limit_type);
            let bucket = buckets
                .entry(key)
                .or_insert_with(|| TokenBucket::new(rate, capacity));
            bucket.try_consume(1.0, now_ms);
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

/// Mutable state protected by a single Mutex inside OperatorController.
struct OperatorState {
    waiting: BinaryHeap<PriorityOperator>,
    /// bg_id → operator id of the most recent waiting entry per BG.
    waiting_bg_ops: HashMap<BgId, u64>,
    /// Active operators keyed by bg_id. At most one running operator per BG.
    running: HashMap<BgId, BGOperator>,
    /// Per-worker running operator count for concurrency limiting.
    worker_op_count: HashMap<u32, u32>,
}

impl OperatorState {
    fn new() -> Self {
        Self {
            waiting: BinaryHeap::new(),
            waiting_bg_ops: HashMap::new(),
            running: HashMap::new(),
            worker_op_count: HashMap::new(),
        }
    }

    fn increment_worker_counts(&mut self, op: &BGOperator) {
        for w in workers_in_op(op) {
            *self.worker_op_count.entry(w).or_insert(0) += 1;
        }
    }

    fn decrement_worker_counts(&mut self, op: &BGOperator) {
        for w in workers_in_op(op) {
            if let Some(count) = self.worker_op_count.get_mut(&w) {
                *count = count.saturating_sub(1);
            }
        }
    }
}

pub struct OperatorController {
    inner: RwLock<OperatorState>,
    config_manager: Arc<ConfigManager>,
    bgtable_manager: Arc<BGTableManager>,
    next_op_id: AtomicU64,
    store_limiter: StoreLimiter,
}

impl OperatorController {
    pub fn new(
        config_manager: Arc<ConfigManager>,
        bgtable_manager: Arc<BGTableManager>,
    ) -> Self {
        let store_limiter = StoreLimiter::new(config_manager.clone());
        Self {
            inner: RwLock::new(OperatorState::new()),
            config_manager,
            bgtable_manager,
            next_op_id: AtomicU64::new(1),
            store_limiter,
        }
    }

    /// Add an operator. Returns true if accepted into the waiting queue.
    pub fn add_operator(&self, op: BGOperator) -> bool {
        let max_waiting = self
            .config_manager
            .get_u32(keys::PD_SCHEDULE_MAX_WAITING_OPERATORS);

        let mut state = self.inner.write().unwrap();

        if state.waiting.len() >= max_waiting as usize {
            return false;
        }

        // Already in waiting queue for this BG → priority replacement (lazy).
        if let Some(&existing_id) = state.waiting_bg_ops.get(&op.bg_id) {
            // Look up the existing op's priority via the heap (rare slow path).
            let existing_priority = state
                .waiting
                .iter()
                .find(|po| po.0.id == existing_id)
                .map(|po| po.0.priority)
                .unwrap_or(0);
            if op.priority <= existing_priority {
                return false;
            }
        }

        // Already running for this BG → priority replacement.
        if let Some(existing) = state.running.get(&op.bg_id) {
            if op.priority > existing.priority {
                if let Some(mut old_op) = state.running.remove(&op.bg_id) {
                    old_op.status = OpStatus::Replaced;
                    state.decrement_worker_counts(&old_op);
                    self.bgtable_manager.bg().set_op_state(op.bg_kind, op.bg_id, BGOpState::Idle);
                }
            } else {
                return false;
            }
        }

        state.waiting_bg_ops.insert(op.bg_id, op.id);
        state.waiting.push(PriorityOperator(op));
        true
    }

    pub fn tick(&self, now_ms: u64) {
        // #5: stale check BEFORE execute, so a stale operator is cancelled
        // before its current step gets pushed through propose. Pre-#5 order
        // (dispatch → execute → check) wasted one Raft round-trip per stale
        // op cycle.
        self.dispatch_operator(now_ms);
        self.check_progress(now_ms);
        self.execute_steps();
    }

    fn get_step_timeout(&self, step: Option<&OpStep>) -> u64 {
        match step {
            Some(OpStep::TransferPrimary { .. }) => self
                .config_manager
                .get_u64(keys::PD_SCHEDULE_STEP_TIMEOUT_TRANSFER_LEASE_MS),
            Some(OpStep::AddReplica { .. }) => self
                .config_manager
                .get_u64(keys::PD_SCHEDULE_STEP_TIMEOUT_ADD_REPLICA_MS),
            Some(OpStep::RemoveReplica { .. }) => self
                .config_manager
                .get_u64(keys::PD_SCHEDULE_STEP_TIMEOUT_REMOVE_REPLICA_MS),
            Some(OpStep::WaitReplicaReady { .. }) => self
                .config_manager
                .get_u64(keys::PD_SCHEDULE_STEP_TIMEOUT_WAIT_REPLICA_READY_MS),
            Some(OpStep::SealBG | OpStep::DeleteBG) => self
                .config_manager
                .get_u64(keys::PD_SCHEDULE_STEP_TIMEOUT_REMOVE_REPLICA_MS),
            None => 180_000,
        }
    }

    fn check_progress(&self, now_ms: u64) {
        let mut state = self.inner.write().unwrap();
        let max_lifetime = self
            .config_manager
            .get_u64(keys::PD_SCHEDULE_OPERATOR_MAX_LIFETIME_MS);

        let mut to_remove = Vec::new();
        for (&bg_id, op) in state.running.iter_mut() {
            // Max lifetime check
            if now_ms.saturating_sub(op.create_time_ms) > max_lifetime {
                log::warn!(
                    "Operator {} for bg {} exceeded max lifetime {}ms, cancelling",
                    op.id,
                    op.bg_id,
                    max_lifetime
                );
                op.status = OpStatus::Cancelled;
                to_remove.push(bg_id);
                continue;
            }

            // Per-step timeout
            let step_timeout = self.get_step_timeout(op.steps.get(op.current_step));
            if now_ms.saturating_sub(op.step_start_time_ms) > step_timeout {
                op.status = OpStatus::Timeout;
                to_remove.push(bg_id);
                continue;
            }

            // P2.5: ConfVerChanged-style stale detection (TiKV PD §14.2).
            // - delta = bg.bg_epoch - op.bg_epoch (the origin observed at dispatch)
            // - consumed = sum of epoch_consumed() for completed steps + the
            //   current step IF it just finished
            // - delta > consumed → external mutation has advanced bg_epoch
            //   beyond what this operator can claim → cancel and let the
            //   checker re-plan on a fresh snapshot.
            let Some(step) = op.steps.get(op.current_step).cloned() else {
                if op.current_step >= op.steps.len() {
                    op.status = OpStatus::Success;
                    to_remove.push(bg_id);
                }
                continue;
            };
            let Some(bg) = self.bgtable_manager.bg().get_bg(op.bg_kind, op.bg_id) else {
                if matches!(step, OpStep::DeleteBG) {
                    op.current_step += 1;
                    op.step_start_time_ms = now_ms;
                    if op.current_step >= op.steps.len() {
                        op.status = OpStatus::Success;
                        to_remove.push(bg_id);
                    }
                }
                continue;
            };

            // #3-C: semantic safety check. Some external mutations don't
            // change bg_epoch by more than our step budget allows, so the
            // delta-based stale detection misses them — but they still
            // break the step's precondition. Example: another op did
            // remove(X) + add(Y), epoch delta = 2 == our step budget for
            // a 2-step op, but our TransferPrimary's `to_worker` was X, now
            // gone from replica_set. Catch these here.
            if let Err(reason) = op.steps[op.current_step].check_safety(&bg) {
                log::warn!(
                    "Operator {} for bg {} failed safety check at step {}: {}; cancelling",
                    op.id,
                    op.bg_id,
                    op.current_step,
                    reason
                );
                op.status = OpStatus::Cancelled;
                to_remove.push(bg_id);
                continue;
            }

            // Then: stale detection. If delta exceeds what completed-or-current
            // steps could have consumed, somebody else mutated the BG.
            // Note: op.bg_epoch is the origin from dispatch and never advances.
            //
            // #3-A fix: condition is `bg.bg_epoch > op.bg_epoch` (delta exists),
            // not `op.bg_epoch > 0`. The pre-#3-A short-circuit silently
            // disabled stale detection for any operator whose builder forgot
            // to call `.bg_epoch(...)` (default 0). Now we always run the
            // check when the BG has actually moved past the origin; if origin
            // and current both happen to be 0 the check is a trivial no-op.
            if bg.bg_epoch > op.bg_epoch {
                let delta = bg.bg_epoch - op.bg_epoch;
                let consumed: u64 = op.steps[..=op.current_step]
                    .iter()
                    .map(|s| s.epoch_consumed())
                    .sum();
                if delta > consumed {
                    log::warn!(
                        "Operator {} for bg {} stale via ConfVerChanged: \
                         origin_epoch={}, current_bg_epoch={}, delta={}, \
                         consumed_by_steps={}, cancelling for re-plan",
                        op.id,
                        op.bg_id,
                        op.bg_epoch,
                        bg.bg_epoch,
                        delta,
                        consumed
                    );
                    op.status = OpStatus::Cancelled;
                    to_remove.push(bg_id);
                    continue;
                }
            }

            // Then: step completion. NO MORE in-place op.bg_epoch += 1
            // (P2.5 removes the "white-stealing" assignment that made stale
            // detection slip past one step).
            if step.is_finish(&bg, self.bgtable_manager.bg()) {
                op.current_step += 1;
                op.step_start_time_ms = now_ms;
            }

            if op.current_step >= op.steps.len() {
                op.status = OpStatus::Success;
                to_remove.push(bg_id);
            }
        }

        for bg_id in to_remove {
            if let Some(op) = state.running.remove(&bg_id) {
                Pd::get_metrics()
                    .operator_finish_total
                    .with_label_values(&[op.status.as_str()])
                    .inc();
                state.decrement_worker_counts(&op);
                self.bgtable_manager.bg().set_op_state(op.bg_kind, bg_id, BGOpState::Idle);
            }
        }
    }

    /// Execute pending steps for each running operator.
    ///
    /// Keep the controller read lock while proposing a step. This allows other
    /// readers, but fences write-side replacement/cancellation so a step cannot
    /// outlive the running operator it was read from.
    fn execute_steps(&self) {
        let state = self.inner.read().unwrap();
        for op in state.running.values() {
            if let Some(step) = op.steps.get(op.current_step) {
                self.execute_single_step(op.bg_kind, op.bg_id, step);
            }
        }
    }

    fn execute_single_step(&self, bg_kind: BGKind, bg_id: BgId, step: &OpStep) {
        // Idempotent check: skip if effect already achieved
        if let Some(bg) = self.bgtable_manager.bg().get_bg(bg_kind, bg_id) {
            if step.is_finish(&bg, self.bgtable_manager.bg()) {
                return;
            }
        }

        match step {
            OpStep::AddReplica { worker_id } => {
                Self::log_propose_outcome(
                    "AddReplica",
                    bg_id,
                    self.bgtable_manager.propose_add_replica(bg_kind, bg_id, *worker_id),
                );
            }
            OpStep::RemoveReplica { worker_id } => {
                let serving = self.bgtable_manager.bg().active_isr_workers(bg_kind, bg_id);
                let serving_after = serving.iter().filter(|&&s| s != *worker_id).count();
                let desired = self.bgtable_manager.bg().get_bg(bg_kind, bg_id)
                    .and_then(|bg| self.bgtable_manager.get_table(bg.table_id))
                    .map(|t| t.replica_count() as usize)
                    .unwrap_or(1);
                if serving_after >= desired {
                    Self::log_propose_outcome(
                        "RemoveReplica",
                        bg_id,
                        self.bgtable_manager.propose_remove_replica(bg_kind, bg_id, *worker_id),
                    );
                }
            }
            OpStep::TransferPrimary {
                from_worker,
                to_worker,
            } => {
                if self.bgtable_manager.bg().get_replica_state(bg_kind, bg_id, *to_worker)
                    != ReplicaState::Active
                {
                    return;
                }
                Self::log_propose_outcome(
                    "TransferPrimary",
                    bg_id,
                    self.bgtable_manager.propose_transfer_primary(
                        bg_kind,
                        bg_id,
                        *from_worker,
                        *to_worker,
                    ),
                );
            }
            OpStep::WaitReplicaReady { .. } => {
                // Pure wait — no action needed
            }
            OpStep::SealBG => {
                Self::log_propose_outcome(
                    "SealBG",
                    bg_id,
                    self.bgtable_manager.propose_seal_bg(bg_kind, bg_id),
                );
            }
            OpStep::DeleteBG => {
                Self::log_propose_outcome(
                    "DeleteBG",
                    bg_id,
                    self.bgtable_manager.propose_delete_bg(bg_kind, bg_id),
                );
            }
        }
    }

    fn log_propose_outcome(action: &str, bg_id: BgId, result: FsResult<ApplyOutcome>) {
        match result {
            Ok(outcome) if outcome.is_success() => {}
            Ok(outcome) => {
                log::warn!(
                    "propose {} bg {} returned non-success outcome: {:?}",
                    action,
                    bg_id,
                    outcome
                );
            }
            Err(e) => {
                log::warn!("propose {} bg {} failed: {}", action, bg_id, e);
            }
        }
    }

    fn dispatch_operator(&self, now_ms: u64) {
        let mut state = self.inner.write().unwrap();
        let max_per_worker = self
            .config_manager
            .get_u32(keys::PD_SCHEDULE_MAX_OPERATORS_PER_WORKER);

        let mut deferred = Vec::new();

        while let Some(PriorityOperator(op)) = state.waiting.pop() {
            // Lazy delete: if waiting_bg_ops no longer points at this op id,
            // it was superseded by a higher-priority op for the same BG.
            // Discard this stale entry without touching the bg_id mapping.
            match state.waiting_bg_ops.get(&op.bg_id).copied() {
                Some(current_id) if current_id == op.id => {}
                _ => continue,
            }

            if state.running.contains_key(&op.bg_id) {
                state.waiting_bg_ops.remove(&op.bg_id);
                continue;
            }

            // Per-worker concurrency limit
            let workers = workers_in_op(&op);
            let exceeds_limit = workers
                .iter()
                .any(|w| state.worker_op_count.get(w).copied().unwrap_or(0) >= max_per_worker);
            if exceeds_limit {
                deferred.push(PriorityOperator(op));
                continue;
            }

            // Store rate limit
            if !self.store_limiter.check_operator(&op, now_ms) {
                deferred.push(PriorityOperator(op));
                continue;
            }

            self.store_limiter.consume_operator(&op, now_ms);

            let mut op = op;
            op.status = OpStatus::Running;
            op.step_start_time_ms = now_ms;

            self.bgtable_manager.bg().set_op_state(op.bg_kind, op.bg_id, op.bg_op_state());
            state.increment_worker_counts(&op);
            state.waiting_bg_ops.remove(&op.bg_id);
            state.running.insert(op.bg_id, op);
        }

        for op in deferred {
            state.waiting.push(op);
        }
    }

    /// Build commands for a worker based on state diff (non-blocking, read-only).
    pub fn build_worker_commands(
        &self,
        worker_id: u32,
        reported_bg_epoch_by_id: &HashMap<BgId, u64>,
    ) -> BGCommands {
        let expected_bgs = self.bgtable_manager.bg().bgs_on_worker(BGKind::Hash, worker_id, None);
        let expected_ids: HashSet<BgId> = expected_bgs.iter().map(|bg| bg.bg_id).collect();
        let reported_ids: HashSet<BgId> = reported_bg_epoch_by_id.keys().copied().collect();

        // Wire format expects Vec<BlockGroupInfo>; deref-clone Arc-wrapped values.
        let add_bgs: Vec<curvine_common::state::BlockGroupInfo> = expected_ids
            .difference(&reported_ids)
            .filter_map(|&bg_id| {
                self.bgtable_manager.bg().get_bg(BGKind::Hash, bg_id)
                    .map(|arc| (*arc).clone())
            })
            .collect();

        let remove_bgs: Vec<BgId> = reported_ids.difference(&expected_ids).copied().collect();

        let update_bgs: Vec<curvine_common::state::BlockGroupInfo> = reported_bg_epoch_by_id
            .iter()
            .filter(|(&bg_id, _)| expected_ids.contains(&bg_id))
            .filter_map(|(&bg_id, &worker_epoch)| {
                self.bgtable_manager.bg().get_bg(BGKind::Hash, bg_id)
                    .filter(|bg| bg.bg_epoch > worker_epoch)
                    .map(|arc| (*arc).clone())
            })
            .collect();

        BGCommands {
            add_bgs,
            remove_bgs,
            update_bgs,
        }
    }

    pub fn running_count(&self) -> usize {
        self.inner.read().unwrap().running.len()
    }

    pub fn waiting_count(&self) -> usize {
        self.inner.read().unwrap().waiting.len()
    }

    pub fn has_running_operators_for_node(&self, node_id: u32) -> bool {
        let state = self.inner.read().unwrap();
        state
            .running
            .values()
            .any(|op| op.steps.iter().any(|step| step.involves_worker(node_id)))
    }

    pub fn next_operator_id(&self) -> u64 {
        self.next_op_id.fetch_add(1, AtomicOrdering::SeqCst)
    }

    /// Net BG count delta on a worker from all running operators.
    pub fn get_bg_influence(&self, worker_id: u32) -> i32 {
        let state = self.inner.read().unwrap();
        let mut delta = 0i32;
        for op in state.running.values() {
            let influence = op.compute_influence();
            delta += influence
                .bg_count_delta
                .get(&worker_id)
                .copied()
                .unwrap_or(0);
        }
        delta
    }

    pub fn get_primary_influence(&self, worker_id: u32) -> i32 {
        let state = self.inner.read().unwrap();
        let mut delta = 0i32;
        for op in state.running.values() {
            let influence = op.compute_influence();
            delta += influence
                .primary_count_delta
                .get(&worker_id)
                .copied()
                .unwrap_or(0);
        }
        delta
    }

    pub fn get_worker_pending_bg_delta(&self, worker_id: u32) -> (u32, u32) {
        let state = self.inner.read().unwrap();
        let mut add = 0u32;
        let mut remove = 0u32;
        for op in state.running.values() {
            for step in &op.steps {
                match step {
                    OpStep::AddReplica { worker_id: w } if *w == worker_id => add += 1,
                    OpStep::RemoveReplica { worker_id: w } if *w == worker_id => remove += 1,
                    _ => {}
                }
            }
        }
        (add, remove)
    }

    pub fn get_worker_pending_primary_delta(&self, worker_id: u32) -> (u32, u32) {
        let state = self.inner.read().unwrap();
        let mut primary_in = 0u32;
        let mut primary_out = 0u32;
        for op in state.running.values() {
            for step in &op.steps {
                if let OpStep::TransferPrimary {
                    from_worker,
                    to_worker,
                } = step
                {
                    if *to_worker == worker_id {
                        primary_in += 1;
                    }
                    if *from_worker == worker_id {
                        primary_out += 1;
                    }
                }
            }
        }
        (primary_in, primary_out)
    }

    /// Get all pending deltas for all workers in one pass.
    pub fn get_all_pending_deltas(&self) -> (HashMap<u32, (u32, u32)>, HashMap<u32, (u32, u32)>) {
        let state = self.inner.read().unwrap();
        let mut bg_delta: HashMap<u32, (u32, u32)> = HashMap::new();
        let mut primary_delta: HashMap<u32, (u32, u32)> = HashMap::new();
        for op in state.running.values() {
            for step in &op.steps {
                match step {
                    OpStep::AddReplica { worker_id } => {
                        bg_delta.entry(*worker_id).or_default().0 += 1;
                    }
                    OpStep::RemoveReplica { worker_id } => {
                        bg_delta.entry(*worker_id).or_default().1 += 1;
                    }
                    OpStep::TransferPrimary {
                        from_worker,
                        to_worker,
                    } => {
                        primary_delta.entry(*to_worker).or_default().0 += 1;
                        primary_delta.entry(*from_worker).or_default().1 += 1;
                    }
                    OpStep::WaitReplicaReady { .. } | OpStep::SealBG | OpStep::DeleteBG => {}
                }
            }
        }
        (bg_delta, primary_delta)
    }

    #[cfg(test)]
    pub fn dispatch_next(&self) -> Vec<BGOperator> {
        let now_ms = orpc::common::LocalTime::mills();
        let before: HashSet<BgId> = self.inner.read().unwrap().running.keys().copied().collect();
        self.dispatch_operator(now_ms);
        self.inner
            .read()
            .unwrap()
            .running
            .iter()
            .filter(|(bg_id, _)| !before.contains(bg_id))
            .map(|(_, op)| op.clone())
            .collect()
    }
}

fn workers_in_op(op: &BGOperator) -> Vec<u32> {
    let mut workers = Vec::new();
    for step in &op.steps {
        match step {
            OpStep::AddReplica { worker_id } => workers.push(*worker_id),
            OpStep::RemoveReplica { worker_id } => workers.push(*worker_id),
            OpStep::TransferPrimary {
                from_worker,
                to_worker,
            } => {
                workers.push(*from_worker);
                workers.push(*to_worker);
            }
            OpStep::WaitReplicaReady { worker_id, .. } => workers.push(*worker_id),
            OpStep::SealBG | OpStep::DeleteBG => {}
        }
    }
    workers.sort_unstable();
    workers.dedup();
    workers
}

#[cfg(test)]
mod tests {
    use super::super::operator::OperatorKind;
    use super::*;
    use crate::pd::bg::{BGManager, BGStore};
    use crate::pd::bgtable::BGTable;
    use crate::pd::journal::{BGEntry, BGUpdateEntry, Client as JournalClient};
    use crate::pd::node::{NodeManager, NodeStore};
    use crate::pd::pd_server::init_metrics_for_test;
    use crate::pd::pool::PoolManager;
    use crate::pd::store::{KvStore, MemoryKvEngine};
    use curvine_common::state::StorageType;
    use std::collections::HashMap;

    fn test_controller_with_config(
        overrides: HashMap<String, String>,
    ) -> (OperatorController, Arc<ConfigManager>, Arc<BGManager>) {
        init_metrics_for_test();
        let store: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
        let raft = curvine_common::raft::RaftClient::from_conf(
            curvine_common::conf::JournalConf::default().create_runtime(),
            &curvine_common::conf::JournalConf::default(),
        );
        let jc = Arc::new(JournalClient::new(raft));
        let config = Arc::new(ConfigManager::new(store.clone(), jc.clone(), overrides));
        let node_store = Arc::new(NodeStore::new(store.clone()));
        let node_mgr = Arc::new(NodeManager::new(node_store, config.clone(), jc.clone()));
        let pool_mgr = Arc::new(PoolManager::new(node_mgr));
        let bg_store = Arc::new(BGStore::new(store.clone()));
        let bg_mgr = Arc::new(BGManager::new(bg_store, jc.clone()));
        let table_store = Arc::new(crate::pd::bgtable::BGTableStore::new(store));
        let bgtable_mgr = Arc::new(crate::pd::bgtable::BGTableManager::new(
            table_store,
            bg_mgr.clone(),
            pool_mgr,
            config.clone(),
            vec![],
        ));
        bgtable_mgr.test_insert_table(BGTable::new_hash_table_with_config(
            1,
            0,
            StorageType::Ssd,
            3,
            vec![0],
            vec![],
            Default::default(),
        ));
        let ctrl = OperatorController::new(config.clone(), bgtable_mgr);
        (ctrl, config, bg_mgr)
    }

    fn test_controller() -> (OperatorController, Arc<ConfigManager>, Arc<BGManager>) {
        test_controller_with_config(HashMap::new())
    }

    fn make_op(id: u64, bg_id: BgId, steps: Vec<OpStep>, priority: u32) -> BGOperator {
        BGOperator {
            id,
            bg_kind: BGKind::Hash,
            kind: OperatorKind::Repair,
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
        use curvine_common::state::{BGPrimary, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 10,
            table_id: 1,
            kind: curvine_common::state::BGKind::Hash,
            bg_epoch: 1,
            replica_set: vec![1],
            isr: vec![1],
            state: BGState::Active,
            op_state: Default::default(),
            primary: BGPrimary {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();

        let op = make_op(42, 10, vec![OpStep::AddReplica { worker_id: 1 }], 1);
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();

        // check_progress should advance step since worker 1 is in replica_set
        ctrl.check_progress(1);
        // Operator should be completed and removed; op_state reset to Idle
        assert_eq!(ctrl.running_count(), 0);
        let bg = bg_mgr.get_bg(BGKind::Hash, 10).unwrap();
        assert_eq!(bg.op_state, BGOpState::Idle);
    }

    #[test]
    fn check_progress_per_step_timeout() {
        let (ctrl, _config, _bg_mgr) = test_controller();
        let op = make_op(43, 11, vec![OpStep::AddReplica { worker_id: 1 }], 1);
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();
        ctrl.check_progress(orpc::common::LocalTime::mills() + 700_000);
        assert_eq!(ctrl.running_count(), 0);
    }

    #[test]
    fn per_worker_concurrency_limit() {
        let overrides: HashMap<String, String> = [(
            keys::PD_SCHEDULE_MAX_OPERATORS_PER_WORKER.to_string(),
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
    fn waiting_priority_replacement_accepts_higher() {
        let (ctrl, _config, _bg_mgr) = test_controller();
        let low = make_op(1, 10, vec![OpStep::AddReplica { worker_id: 1 }], 50);
        assert!(ctrl.add_operator(low));

        // Same bg_id, higher priority → accepted, replaces previous waiting entry.
        let high = make_op(2, 10, vec![OpStep::AddReplica { worker_id: 2 }], 100);
        assert!(ctrl.add_operator(high));

        // Dispatch: only the high-priority op should reach running; the low one
        // is dropped via lazy delete (its id no longer matches waiting_bg_ops).
        let dispatched = ctrl.dispatch_next();
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].id, 2);
        assert_eq!(dispatched[0].priority, 100);
    }

    #[test]
    fn waiting_priority_replacement_rejects_equal_or_lower() {
        let (ctrl, _config, _bg_mgr) = test_controller();
        let high = make_op(1, 10, vec![OpStep::AddReplica { worker_id: 1 }], 100);
        assert!(ctrl.add_operator(high));

        // Same bg_id, equal priority → rejected.
        let same = make_op(2, 10, vec![OpStep::AddReplica { worker_id: 2 }], 100);
        assert!(!ctrl.add_operator(same));

        // Same bg_id, lower priority → rejected.
        let low = make_op(3, 10, vec![OpStep::AddReplica { worker_id: 3 }], 50);
        assert!(!ctrl.add_operator(low));

        let dispatched = ctrl.dispatch_next();
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].id, 1);
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

        let mut dispatched = ctrl.dispatch_next();
        assert_eq!(dispatched.len(), 3);
        dispatched.sort_by(|a, b| b.priority.cmp(&a.priority));
        assert_eq!(dispatched[0].bg_id, 11); // priority 100
        assert_eq!(dispatched[1].bg_id, 12); // priority 50
        assert_eq!(dispatched[2].bg_id, 10); // priority 1
    }

    #[test]
    fn max_waiting_queue_limit() {
        let overrides: HashMap<String, String> = [(
            keys::PD_SCHEDULE_MAX_WAITING_OPERATORS.to_string(),
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
    fn build_worker_commands_add_remove_update() {
        let (ctrl, _config, bg_mgr) = test_controller();

        use curvine_common::state::{BGPrimary, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 20,
            table_id: 1,
            kind: curvine_common::state::BGKind::Hash,
            bg_epoch: 3,
            replica_set: vec![1, 2, 5],
            isr: vec![1, 2, 5],
            state: BGState::Active,
            op_state: Default::default(),
            primary: BGPrimary {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();

        // Worker 5 doesn't report BG 20 → add
        let reported: HashMap<BgId, u64> = HashMap::new();
        let cmds = ctrl.build_worker_commands(5, &reported);
        assert_eq!(cmds.add_bgs.len(), 1);
        assert_eq!(cmds.add_bgs[0].bg_id, 20);

        // Worker 5 reports BG 20 with current epoch → no add, no update
        let reported: HashMap<BgId, u64> = [(20, 3)].into_iter().collect();
        let cmds = ctrl.build_worker_commands(5, &reported);
        assert!(cmds.add_bgs.is_empty());
        assert!(cmds.remove_bgs.is_empty());
        assert!(cmds.update_bgs.is_empty());

        // Worker 5 reports BG 20 with stale epoch → update
        let reported: HashMap<BgId, u64> = [(20, 1)].into_iter().collect();
        let cmds = ctrl.build_worker_commands(5, &reported);
        assert!(cmds.add_bgs.is_empty());
        assert_eq!(cmds.update_bgs.len(), 1);
        assert_eq!(cmds.update_bgs[0].bg_id, 20);

        // Worker 99 reports BG 20 but isn't in replica_set → remove
        let reported: HashMap<BgId, u64> = [(20, 1)].into_iter().collect();
        let cmds = ctrl.build_worker_commands(99, &reported);
        assert!(cmds.add_bgs.is_empty());
        assert_eq!(cmds.remove_bgs, vec![20]);
    }

    #[test]
    fn operator_success_resets_op_state_to_idle() {
        let (ctrl, _config, bg_mgr) = test_controller();

        use curvine_common::state::{BGPrimary, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 30,
            table_id: 1,
            kind: curvine_common::state::BGKind::Hash,
            bg_epoch: 1,
            replica_set: vec![1],
            isr: vec![1],
            state: BGState::Active,
            op_state: Default::default(),
            primary: BGPrimary {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();

        // Step already satisfied (worker 1 in replica_set)
        let op = make_op(1, 30, vec![OpStep::AddReplica { worker_id: 1 }], 1);
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();

        // After dispatch, BG op_state should be Repairing
        let bg_state = bg_mgr.get_bg(BGKind::Hash, 30).unwrap();
        assert_eq!(bg_state.op_state, BGOpState::Repairing);

        // check_progress completes the step
        ctrl.check_progress(1);
        assert_eq!(ctrl.running_count(), 0);
        let bg_after = bg_mgr.get_bg(BGKind::Hash, 30).unwrap();
        assert_eq!(bg_after.op_state, BGOpState::Idle);
    }

    #[test]
    fn check_progress_cancels_stale_epoch() {
        let (ctrl, _config, bg_mgr) = test_controller();

        use curvine_common::state::{BGPrimary, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 40,
            table_id: 1,
            kind: curvine_common::state::BGKind::Hash,
            bg_epoch: 1,
            replica_set: vec![1, 2],
            isr: vec![1, 2],
            state: BGState::Active,
            op_state: Default::default(),
            primary: BGPrimary {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();

        // Create operator with bg_epoch=1
        let mut op = make_op(1, 40, vec![OpStep::AddReplica { worker_id: 99 }], 1);
        op.bg_epoch = 1;
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();
        assert_eq!(ctrl.running_count(), 1);

        // Simulate epoch change by updating replica_set
        bg_mgr
            .apply_update_bg(&BGUpdateEntry {
                kind: BGKind::Hash,
                bg_id: 40,
                op_ms: 0,
                replica_set: Some(vec![1, 2, 3]),
                state: None,
                isr: None,
                primary: None,
                expected_bg_epoch: 1,
                bump_table_epoch: false,
            })
            .unwrap();
        // bg_epoch should now be 2
        assert_eq!(bg_mgr.get_bg(BGKind::Hash, 40).unwrap().bg_epoch, 2);

        // check_progress should detect stale epoch and cancel
        ctrl.check_progress(orpc::common::LocalTime::mills());
        assert_eq!(ctrl.running_count(), 0);
    }

    #[test]
    fn check_progress_cancels_expired_operator() {
        let overrides: HashMap<String, String> = [(
            keys::PD_SCHEDULE_OPERATOR_MAX_LIFETIME_MS.to_string(),
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
        let overrides: HashMap<String, String> = [
            (
                keys::PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_RATE.to_string(),
                "1".to_string(),
            ),
            (
                keys::PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_CAPACITY.to_string(),
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

    /// #3-C: check_safety catches cases the epoch-delta check misses.
    /// Setup: a TransferPrimary operator whose target worker is silently
    /// removed from replica_set by an external actor. Epoch advance matches
    /// the operator's step budget (delta == consumed), so delta-based stale
    /// detection says "within budget, keep going" — but the step's
    /// precondition (target ∈ replica_set) is now violated. check_safety
    /// must fire and cancel.
    #[test]
    fn check_progress_cancels_on_safety_violation() {
        let (ctrl, _config, bg_mgr) = test_controller();

        use curvine_common::state::{BGPrimary, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 300,
            table_id: 1,
            kind: curvine_common::state::BGKind::Hash,
            bg_epoch: 1,
            replica_set: vec![1, 2],
            isr: vec![1, 2],
            state: BGState::Active,
            op_state: Default::default(),
            primary: BGPrimary {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();

        // Operator: transfer primary 1 → 2. Budget = 1 epoch.
        let mut op = make_op(
            2000,
            300,
            vec![OpStep::TransferPrimary {
                from_worker: 1,
                to_worker: 2,
            }],
            1,
        );
        op.bg_epoch = 1;
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();
        assert_eq!(ctrl.running_count(), 1);

        // External actor removes worker 2 (our intended primary target) and
        // advances bg_epoch by exactly 1 — within our budget, so delta-based
        // stale detection would let us pass. But `to_worker` is gone from
        // replica_set now, so check_safety must catch it.
        bg_mgr
            .apply_update_bg(&BGUpdateEntry {
                op_ms: 1,
                kind: BGKind::Hash,
                bg_id: 300,
                state: None,
                replica_set: Some(vec![1]),
                isr: None,
                primary: None,
                expected_bg_epoch: 1,
                bump_table_epoch: false,
            })
            .unwrap();

        ctrl.check_progress(1);
        assert_eq!(
            ctrl.running_count(),
            0,
            "#3-C: safety check must cancel when target worker is gone even if epoch delta is within budget"
        );
    }

    // =========================================================================
    // REGRESSION-BASELINE tests (P0.4 from docs/pd-raft-consistency.md §15).
    //
    // After P2.5 lands (ConfVerChanged 差量法 — see §14.2 + §15 Phase 2.5),
    // this test must be UPDATED to reflect the new contract: when an external
    // mutation advances bg_epoch beyond what the operator's accumulated
    // step.epoch_consumed() can explain, check_progress cancels the operator
    // immediately instead of "white-stealing" the epoch advance.
    // =========================================================================

    /// REGRESSION (post-P2.5): ConfVerChanged-style stale detection.
    /// When external mutations advance bg_epoch by MORE than the operator's
    /// completed-or-current steps could have consumed, check_progress cancels
    /// the operator and the checker can re-plan on a fresh snapshot.
    ///
    /// Pre-P2.5 (now removed): op.bg_epoch += 1 on is_finish caused
    /// "white-stealing" — the operator silently absorbed external advances
    /// up to one step's worth, hiding the conflict for one extra tick.
    #[test]
    fn check_progress_cancels_when_external_delta_exceeds_consumed() {
        let (ctrl, _config, bg_mgr) = test_controller();

        use crate::pd::journal::BGUpdateEntry;
        use curvine_common::state::{BGPrimary, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 200,
            table_id: 1,
            kind: curvine_common::state::BGKind::Hash,
            bg_epoch: 1,
            replica_set: vec![1],
            isr: vec![1],
            state: BGState::Active,
            op_state: Default::default(),
            primary: BGPrimary {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();

        // Operator wants AddReplica 99 — one step that consumes 1 epoch.
        let mut op = make_op(1000, 200, vec![OpStep::AddReplica { worker_id: 99 }], 1);
        op.bg_epoch = 1;
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();
        assert_eq!(ctrl.running_count(), 1);

        // External path advances bg_epoch by TWO (e.g. another op did
        // add+remove). This exceeds what step[0] could possibly consume (1).
        bg_mgr
            .apply_update_bg(&BGUpdateEntry {
                op_ms: 1,
                kind: BGKind::Hash,
                bg_id: 200,
                state: None,
                replica_set: Some(vec![1, 77]),
                isr: None,
                primary: None,
                expected_bg_epoch: 1,
                bump_table_epoch: false,
            })
            .unwrap();
        bg_mgr
            .apply_update_bg(&BGUpdateEntry {
                op_ms: 2,
                kind: BGKind::Hash,
                bg_id: 200,
                state: None,
                replica_set: Some(vec![1]),
                isr: None,
                primary: None,
                expected_bg_epoch: 2,
                bump_table_epoch: false,
            })
            .unwrap();
        assert_eq!(bg_mgr.get_bg(BGKind::Hash, 200).unwrap().bg_epoch, 3);

        // P2.5: delta = 3-1 = 2, consumed = step[0].epoch_consumed() = 1.
        // delta > consumed → cancel.
        ctrl.check_progress(1);
        assert_eq!(
            ctrl.running_count(),
            0,
            "P2.5: external delta (2) exceeded operator consumed (1) → must cancel"
        );
    }

    /// Negative case: external delta == consumed → operator continues (we
    /// can't distinguish "we did it" from "they did it" within a single step).
    /// This is the correct design tradeoff (§14.2 TiKV PD spec).
    #[test]
    fn check_progress_keeps_running_when_delta_within_consumed_budget() {
        let (ctrl, _config, bg_mgr) = test_controller();

        use crate::pd::journal::BGUpdateEntry;
        use curvine_common::state::{BGPrimary, BGState};
        let bg = curvine_common::state::BlockGroupInfo {
            bg_id: 201,
            table_id: 1,
            kind: curvine_common::state::BGKind::Hash,
            bg_epoch: 1,
            replica_set: vec![1],
            isr: vec![1],
            state: BGState::Active,
            op_state: Default::default(),
            primary: BGPrimary {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        };
        bg_mgr
            .apply_create_bg(&BGEntry { op_ms: 0, info: bg })
            .unwrap();

        let mut op = make_op(1001, 201, vec![OpStep::AddReplica { worker_id: 99 }], 1);
        op.bg_epoch = 1;
        assert!(ctrl.add_operator(op));
        ctrl.dispatch_next();

        // External path adds worker 99 (so step also looks finished). Epoch
        // advances by exactly 1, matching step[0]'s budget.
        bg_mgr
            .apply_update_bg(&BGUpdateEntry {
                op_ms: 1,
                kind: BGKind::Hash,
                bg_id: 201,
                state: None,
                replica_set: Some(vec![1, 99]),
                isr: None,
                primary: None,
                expected_bg_epoch: 1,
                bump_table_epoch: false,
            })
            .unwrap();

        // delta=1, consumed=1 → not cancelled. Step is is_finish → advance.
        ctrl.check_progress(1);
        // Operator finished its only step → status Success → removed.
        assert_eq!(ctrl.running_count(), 0);
    }
}
