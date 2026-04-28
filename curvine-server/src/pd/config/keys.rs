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

/// Static description of a dynamic config item.
#[derive(Debug, Clone)]
pub struct DynamicConfigItem {
    pub key: &'static str,
    pub default: &'static str,
    pub desc: &'static str,
}

/// Heartbeat timeout for PD node manager, in milliseconds.
pub const PD_NODE_HEARTBEAT_TIMEOUT_MS: &str = "pd.node.heartbeat_timeout_ms";

/// Lost recovery window for node health checker, in milliseconds.
pub const PD_NODE_LOST_RECOVERY_WINDOW_MS: &str = "pd.node.lost_recovery_window_ms";

/// Interval between periodic persists of node info via Raft, in milliseconds.
pub const PD_NODE_PERSIST_INTERVAL_MS: &str = "pd.node.persist_interval_ms";

/// Cooldown before executing a rebuild (in milliseconds).
pub const PD_BG_REBUILD_COOLDOWN_MS: &str = "pd.bg.rebuild.cooldown_ms";

/// Enable auto-rebuild of BGTable on node join/remove.
pub const PD_BG_REBUILD_AUTO_ENABLED: &str = "pd.bg.rebuild.auto_enabled";

/// Placement policy strategy (`quota` / `capacity`).
pub const PD_BG_BALANCE_POLICY: &str = "pd.bg.balance_policy";

/// Placement policy for replica isolation (`default` / `topology_aware`).
/// `topology_aware` uses `location_labels` from server config for hierarchical isolation.
pub const PD_BG_PLACEMENT_POLICY: &str = "pd.bg.placement_policy";

/// Hard minimum isolation level (a label name from `location_labels`).
/// When set, replicas MUST NOT share the same value at this level.
/// Empty string = no hard isolation (only soft score preference).
pub const PD_BG_MIN_ISOLATION_LEVEL: &str = "pd.bg.min_isolation_level";

/// Tolerant ratio (basis points) applied by the planner during build/rebuild.
/// 100 bps = 1%. Default 1000 bps = 10%.
pub const PD_BG_REBUILD_TOLERANT_RATIO_BPS: &str = "pd.bg.rebuild.tolerant_ratio_bps";

/// BG assignment check interval (in milliseconds).
pub const PD_SCHEDULE_BG_CHECK_INTERVAL_MS: &str = "pd.schedule.bg_check_interval_ms";

/// Patrol interval for checkers (in milliseconds).
pub const PD_SCHEDULE_PATROL_INTERVAL_MS: &str = "pd.schedule.patrol_interval_ms";

/// Operator tick interval (in milliseconds).
pub const PD_SCHEDULE_OPERATOR_TICK_INTERVAL_MS: &str = "pd.schedule.operator_tick_interval_ms";

/// Lease check interval (in milliseconds).
pub const PD_SCHEDULE_LEASE_CHECK_INTERVAL_MS: &str = "pd.schedule.lease_check_interval_ms";

/// Max waiting operators in queue.
pub const PD_SCHEDULE_MAX_WAITING_OPERATORS: &str = "pd.schedule.max_waiting_operators";

/// Max concurrent operators per worker.
pub const PD_SCHEDULE_MAX_OPERATORS_PER_WORKER: &str = "pd.schedule.max_operators_per_worker";

/// Max concurrent recovery operators.
pub const PD_RECOVERY_MAX_CONCURRENT: &str = "pd.recovery.max_concurrent";

/// Liveness check interval for node manager, in milliseconds.
pub const PD_NODE_LIVENESS_CHECK_INTERVAL_MS: &str = "pd.node.liveness_check_interval_ms";

/// Per-step timeout for TransferLease operators, in milliseconds.
pub const PD_SCHEDULE_STEP_TIMEOUT_TRANSFER_LEASE_MS: &str =
    "pd.schedule.step_timeout.transfer_lease_ms";

/// Per-step timeout for AddReplica operators, in milliseconds.
pub const PD_SCHEDULE_STEP_TIMEOUT_ADD_REPLICA_MS: &str = "pd.schedule.step_timeout.add_replica_ms";

/// Per-step timeout for RemoveReplica operators, in milliseconds.
pub const PD_SCHEDULE_STEP_TIMEOUT_REMOVE_REPLICA_MS: &str =
    "pd.schedule.step_timeout.remove_replica_ms";

/// Per-step timeout for WaitReplicaReady operators, in milliseconds.
pub const PD_SCHEDULE_STEP_TIMEOUT_WAIT_REPLICA_READY_MS: &str =
    "pd.schedule.step_timeout.wait_replica_ready_ms";

/// Max operator lifetime before cancellation, in milliseconds.
pub const PD_SCHEDULE_OPERATOR_MAX_LIFETIME_MS: &str = "pd.schedule.operator_max_lifetime_ms";

/// Store limit: AddReplica token refill rate (tokens/sec per worker).
pub const PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_RATE: &str =
    "pd.schedule.store_limit.add_replica_rate";

/// Store limit: AddReplica burst capacity per worker.
pub const PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_CAPACITY: &str =
    "pd.schedule.store_limit.add_replica_capacity";

/// Store limit: RemoveReplica token refill rate (tokens/sec per worker).
pub const PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_RATE: &str =
    "pd.schedule.store_limit.remove_replica_rate";

/// Store limit: RemoveReplica burst capacity per worker.
pub const PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_CAPACITY: &str =
    "pd.schedule.store_limit.remove_replica_capacity";

/// Store limit: TransferLease token refill rate (tokens/sec per worker).
pub const PD_SCHEDULE_STORE_LIMIT_TRANSFER_LEASE_RATE: &str =
    "pd.schedule.store_limit.transfer_lease_rate";

/// Store limit: TransferLease burst capacity per worker.
pub const PD_SCHEDULE_STORE_LIMIT_TRANSFER_LEASE_CAPACITY: &str =
    "pd.schedule.store_limit.transfer_lease_capacity";

/// Store limit: Rebuild AddReplica/RemoveReplica token refill rate (tokens/sec per worker).
/// Higher than normal to allow fast bulk rebalancing after expansion.
pub const PD_SCHEDULE_STORE_LIMIT_REBUILD_RATE: &str = "pd.schedule.store_limit.rebuild_rate";

/// Store limit: Rebuild burst capacity per worker.
pub const PD_SCHEDULE_STORE_LIMIT_REBUILD_CAPACITY: &str =
    "pd.schedule.store_limit.rebuild_capacity";

/// Enable BG count balance scheduler.
pub const PD_SCHEDULE_BALANCE_BG_ENABLED: &str = "pd.schedule.balance_bg_enabled";

/// BG balance check interval (in milliseconds).
pub const PD_SCHEDULE_BALANCE_BG_INTERVAL_MS: &str = "pd.schedule.balance_bg_interval_ms";

/// Enable leader balance scheduler.
pub const PD_SCHEDULE_BALANCE_LEADER_ENABLED: &str = "pd.schedule.balance_leader_enabled";

/// Leader balance check interval (in milliseconds).
pub const PD_SCHEDULE_BALANCE_LEADER_INTERVAL_MS: &str = "pd.schedule.balance_leader_interval_ms";

/// Balance tolerant ratio in basis points (e.g. 500 = 5%).
pub const PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS: &str = "pd.schedule.balance_tolerant_ratio_bps";

/// Max balance operators per cycle.
pub const PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE: &str = "pd.schedule.balance_max_ops_per_cycle";

/// Cooldown after a worker `Registered` event before balance schedulers.
pub const PD_SCHEDULE_BALANCE_POST_REGISTER_DELAY_MS: &str =
    "pd.schedule.balance_post_register_delay_ms";

/// Enable all checkers globally.
pub const PD_SCHEDULE_CHECKER_ENABLED: &str = "pd.schedule.checker.enabled";

/// Placement rule check interval (in milliseconds).
pub const PD_SCHEDULE_PLACEMENT_CHECK_INTERVAL_MS: &str = "pd.schedule.placement_check_interval_ms";

/// Auto-repair Live workers that are not assigned to any pool.
pub const PD_CHECKER_POOL_MEMBERSHIP_AUTO_REPAIR: &str = "pd.checker.pool_membership_auto_repair";

/// Grace window (ms) for checker to  taking over a BG whose replicas include Decommission / Offline nodes.
pub const PD_CHECKER_LEAVING_GRACE_MS: &str = "pd.checker.leaving_grace_ms";

/// All registered dynamic config items.
pub static DYNAMIC_CONFIG_ITEMS: &[DynamicConfigItem] = &[
    DynamicConfigItem {
        key: PD_NODE_HEARTBEAT_TIMEOUT_MS,
        default: "60000",
        desc: "Node heartbeat timeout in milliseconds",
    },
    DynamicConfigItem {
        key: PD_NODE_LOST_RECOVERY_WINDOW_MS,
        default: "300000",
        desc: "Lost worker recovery window in milliseconds",
    },
    DynamicConfigItem {
        key: PD_NODE_PERSIST_INTERVAL_MS,
        default: "300000",
        desc: "Interval between periodic persists of node info via Raft",
    },
    DynamicConfigItem {
        key: PD_BG_REBUILD_COOLDOWN_MS,
        default: "60000",
        desc: "Cooldown before executing a BGTable rebuild in milliseconds",
    },
    DynamicConfigItem {
        key: PD_BG_REBUILD_AUTO_ENABLED,
        default: "true",
        desc: "Enable auto-rebuild of BGTable on node join/remove",
    },
    DynamicConfigItem {
        key: PD_BG_BALANCE_POLICY,
        default: "quota",
        desc: "Placement policy strategy: quota / capacity",
    },
    DynamicConfigItem {
        key: PD_BG_PLACEMENT_POLICY,
        default: "default",
        desc: "Placement policy: default / topology_aware",
    },
    DynamicConfigItem {
        key: PD_BG_MIN_ISOLATION_LEVEL,
        default: "",
        desc: "Hard minimum isolation level (label name). Empty = soft only",
    },
    DynamicConfigItem {
        key: PD_BG_REBUILD_TOLERANT_RATIO_BPS,
        default: "1000",
        desc: "Planner tolerant ratio (basis points) used during build/rebuild",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_BG_CHECK_INTERVAL_MS,
        default: "10000",
        desc: "BG assignment check interval in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_PATROL_INTERVAL_MS,
        default: "10000",
        desc: "Patrol interval for checkers in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_OPERATOR_TICK_INTERVAL_MS,
        default: "200",
        desc: "Operator tick interval in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_LEASE_CHECK_INTERVAL_MS,
        default: "10000",
        desc: "Lease check interval in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_MAX_WAITING_OPERATORS,
        default: "100",
        desc: "Max waiting operators in queue",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_MAX_OPERATORS_PER_WORKER,
        default: "5",
        desc: "Max concurrent operators per worker",
    },
    DynamicConfigItem {
        key: PD_RECOVERY_MAX_CONCURRENT,
        default: "10",
        desc: "Max concurrent recovery operators",
    },
    DynamicConfigItem {
        key: PD_NODE_LIVENESS_CHECK_INTERVAL_MS,
        default: "5000",
        desc: "Liveness check interval for node manager in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STEP_TIMEOUT_TRANSFER_LEASE_MS,
        default: "30000",
        desc: "Per-step timeout for TransferLease operators in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STEP_TIMEOUT_ADD_REPLICA_MS,
        default: "600000",
        desc: "Per-step timeout for AddReplica operators in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STEP_TIMEOUT_REMOVE_REPLICA_MS,
        default: "60000",
        desc: "Per-step timeout for RemoveReplica operators in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STEP_TIMEOUT_WAIT_REPLICA_READY_MS,
        default: "900000",
        desc: "Per-step timeout for WaitReplicaReady operators in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_OPERATOR_MAX_LIFETIME_MS,
        default: "3600000",
        desc: "Max operator lifetime before cancellation in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_RATE,
        default: "5",
        desc: "Store limit: AddReplica token refill rate per worker per second",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_CAPACITY,
        default: "5",
        desc: "Store limit: AddReplica burst capacity per worker",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_RATE,
        default: "5",
        desc: "Store limit: RemoveReplica token refill rate per worker per second",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_CAPACITY,
        default: "5",
        desc: "Store limit: RemoveReplica burst capacity per worker",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STORE_LIMIT_TRANSFER_LEASE_RATE,
        default: "15",
        desc: "Store limit: TransferLease token refill rate per worker per second",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STORE_LIMIT_TRANSFER_LEASE_CAPACITY,
        default: "15",
        desc: "Store limit: TransferLease burst capacity per worker",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STORE_LIMIT_REBUILD_RATE,
        default: "100",
        desc: "Store limit: Rebuild token refill rate per worker per second (burst class)",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_STORE_LIMIT_REBUILD_CAPACITY,
        default: "100",
        desc: "Store limit: Rebuild burst capacity per worker",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_BALANCE_BG_ENABLED,
        default: "true",
        desc: "Enable BG count balance scheduler",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_BALANCE_BG_INTERVAL_MS,
        default: "30000",
        desc: "BG balance check interval in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_BALANCE_LEADER_ENABLED,
        default: "true",
        desc: "Enable leader balance scheduler",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_BALANCE_LEADER_INTERVAL_MS,
        default: "30000",
        desc: "Leader balance check interval in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS,
        default: "500",
        desc: "Balance tolerant ratio in basis points (500 = 5%)",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE,
        default: "3",
        desc: "Max balance operators per cycle",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_BALANCE_POST_REGISTER_DELAY_MS,
        default: "300000",
        desc: "Cooldown in ms after a worker register event before balance schedulers run",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_CHECKER_ENABLED,
        default: "true",
        desc: "Enable all checkers globally",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_PLACEMENT_CHECK_INTERVAL_MS,
        default: "30000",
        desc: "Placement rule check interval in milliseconds",
    },
    DynamicConfigItem {
        key: PD_CHECKER_POOL_MEMBERSHIP_AUTO_REPAIR,
        default: "true",
        desc: "Auto-repair Live workers not assigned to any pool",
    },
    DynamicConfigItem {
        key: PD_CHECKER_LEAVING_GRACE_MS,
        default: "900000",
        desc: "Grace window in ms for checker to defer to scheduler on leaving replicas",
    },
];
