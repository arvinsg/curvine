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

//! Keys and default values for PD dynamic config.
//! These are the authoritative registry of dynamic config items;
//! `PdConf.dynamic_config` and KV storage override these defaults at runtime.

/// Static description of a dynamic config item.
#[derive(Debug, Clone)]
pub struct DynamicConfigItem {
    pub key: &'static str,
    pub default: &'static str,
    pub desc: &'static str,
}

/// Heartbeat timeout for PD node manager, in milliseconds.
pub const PD_NODE_HEARTBEAT_TIMEOUT_MS: &str = "pd.node.heartbeat_timeout_ms";
pub const PD_NODE_HEARTBEAT_TIMEOUT_MS_DEFAULT: u64 = 60_000;

/// Lost recovery window for node health checker, in milliseconds.
pub const PD_NODE_LOST_RECOVERY_WINDOW_MS: &str = "pd.node.lost_recovery_window_ms";
pub const PD_NODE_LOST_RECOVERY_WINDOW_MS_DEFAULT: u64 = 300_000;

/// Interval between periodic persists of node info via Raft, in milliseconds.
pub const PD_NODE_PERSIST_INTERVAL_MS: &str = "pd.node.persist_interval_ms";
pub const PD_NODE_PERSIST_INTERVAL_MS_DEFAULT: u64 = 300_000;

/// Default bucket count for new BGTables.
pub const PD_BG_DEFAULT_BUCKET_COUNT: &str = "pd.bg.default_bucket_count";
pub const PD_BG_DEFAULT_BUCKET_COUNT_DEFAULT: u32 = 1024;

/// Default replica count for new BGTables.
pub const PD_BG_DEFAULT_REPLICA_COUNT: &str = "pd.bg.default_replica_count";
pub const PD_BG_DEFAULT_REPLICA_COUNT_DEFAULT: u32 = 3;

/// Cooldown before executing a rebuild (in milliseconds).
pub const PD_BG_REBUILD_COOLDOWN_MS: &str = "pd.bg.rebuild.cooldown_ms";
pub const PD_BG_REBUILD_COOLDOWN_MS_DEFAULT: u64 = 60_000;

/// Enable auto-rebuild of BGTable on node join/remove.
pub const PD_BG_REBUILD_AUTO_ENABLED: &str = "pd.bg.rebuild.auto_enabled";
pub const PD_BG_REBUILD_AUTO_ENABLED_DEFAULT: bool = true;

/// BG assignment check interval (in milliseconds).
pub const PD_SCHEDULE_BG_CHECK_INTERVAL_MS: &str = "pd.schedule.bg_check_interval_ms";
pub const PD_SCHEDULE_BG_CHECK_INTERVAL_MS_DEFAULT: u64 = 10_000;

/// Patrol interval for checkers (in milliseconds).
pub const PD_SCHEDULE_PATROL_INTERVAL_MS: &str = "pd.schedule.patrol_interval_ms";
pub const PD_SCHEDULE_PATROL_INTERVAL_MS_DEFAULT: u64 = 10_000;

/// Lease check interval (in milliseconds).
pub const PD_SCHEDULE_LEASE_CHECK_INTERVAL_MS: &str = "pd.schedule.lease_check_interval_ms";
pub const PD_SCHEDULE_LEASE_CHECK_INTERVAL_MS_DEFAULT: u64 = 10_000;

/// Max waiting operators in queue.
pub const PD_SCHEDULE_MAX_WAITING_OPERATORS: &str = "pd.schedule.max_waiting_operators";
pub const PD_SCHEDULE_MAX_WAITING_OPERATORS_DEFAULT: u32 = 100;

/// Max concurrent operators per worker.
pub const PD_SCHEDULE_MAX_OPERATORS_PER_WORKER: &str = "pd.schedule.max_operators_per_worker";
pub const PD_SCHEDULE_MAX_OPERATORS_PER_WORKER_DEFAULT: u32 = 5;

/// Max concurrent recovery operators.
pub const PD_RECOVERY_MAX_CONCURRENT: &str = "pd.recovery.max_concurrent";
pub const PD_RECOVERY_MAX_CONCURRENT_DEFAULT: u32 = 10;

/// Liveness check interval for node manager, in milliseconds.
pub const PD_NODE_LIVENESS_CHECK_INTERVAL_MS: &str = "pd.node.liveness_check_interval_ms";
pub const PD_NODE_LIVENESS_CHECK_INTERVAL_MS_DEFAULT: u64 = 5_000;

/// Per-step timeout for TransferLease operators, in milliseconds.
pub const PD_SCHEDULE_STEP_TIMEOUT_TRANSFER_LEASE_MS: &str = "pd.schedule.step_timeout.transfer_lease_ms";
pub const PD_SCHEDULE_STEP_TIMEOUT_TRANSFER_LEASE_MS_DEFAULT: u64 = 30_000;

/// Per-step timeout for AddReplica operators, in milliseconds.
pub const PD_SCHEDULE_STEP_TIMEOUT_ADD_REPLICA_MS: &str = "pd.schedule.step_timeout.add_replica_ms";
pub const PD_SCHEDULE_STEP_TIMEOUT_ADD_REPLICA_MS_DEFAULT: u64 = 600_000;

/// Per-step timeout for RemoveReplica operators, in milliseconds.
pub const PD_SCHEDULE_STEP_TIMEOUT_REMOVE_REPLICA_MS: &str = "pd.schedule.step_timeout.remove_replica_ms";
pub const PD_SCHEDULE_STEP_TIMEOUT_REMOVE_REPLICA_MS_DEFAULT: u64 = 60_000;

/// Max operator lifetime before cancellation, in milliseconds.
pub const PD_SCHEDULE_OPERATOR_MAX_LIFETIME_MS: &str = "pd.schedule.operator_max_lifetime_ms";
pub const PD_SCHEDULE_OPERATOR_MAX_LIFETIME_MS_DEFAULT: u64 = 3_600_000; // 1 hour

/// Max times a suspect BG is checked before auto-clearing.
pub const PD_SCHEDULE_SUSPECT_MAX_CHECKS: &str = "pd.schedule.suspect_max_checks";
pub const PD_SCHEDULE_SUSPECT_MAX_CHECKS_DEFAULT: u32 = 5;

/// TTL for suspect BG entries, in milliseconds.
pub const PD_SCHEDULE_SUSPECT_TTL_MS: &str = "pd.schedule.suspect_ttl_ms";
pub const PD_SCHEDULE_SUSPECT_TTL_MS_DEFAULT: u64 = 300_000; // 5 minutes

/// Store limit: AddReplica token refill rate (tokens/sec per worker).
pub const PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_RATE: &str = "pd.schedule.store_limit.add_replica_rate";
pub const PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_RATE_DEFAULT: u32 = 5;

/// Store limit: AddReplica burst capacity per worker.
pub const PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_CAPACITY: &str = "pd.schedule.store_limit.add_replica_capacity";
pub const PD_SCHEDULE_STORE_LIMIT_ADD_REPLICA_CAPACITY_DEFAULT: u32 = 5;

/// Store limit: RemoveReplica token refill rate (tokens/sec per worker).
pub const PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_RATE: &str = "pd.schedule.store_limit.remove_replica_rate";
pub const PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_RATE_DEFAULT: u32 = 5;

/// Store limit: RemoveReplica burst capacity per worker.
pub const PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_CAPACITY: &str = "pd.schedule.store_limit.remove_replica_capacity";
pub const PD_SCHEDULE_STORE_LIMIT_REMOVE_REPLICA_CAPACITY_DEFAULT: u32 = 5;

/// Enable BG count balance scheduler.
pub const PD_SCHEDULE_BALANCE_BG_ENABLED: &str = "pd.schedule.balance_bg_enabled";
pub const PD_SCHEDULE_BALANCE_BG_ENABLED_DEFAULT: bool = true;

/// BG balance check interval (in milliseconds).
pub const PD_SCHEDULE_BALANCE_BG_INTERVAL_MS: &str = "pd.schedule.balance_bg_interval_ms";
pub const PD_SCHEDULE_BALANCE_BG_INTERVAL_MS_DEFAULT: u64 = 30_000;

/// Enable leader balance scheduler.
pub const PD_SCHEDULE_BALANCE_LEADER_ENABLED: &str = "pd.schedule.balance_leader_enabled";
pub const PD_SCHEDULE_BALANCE_LEADER_ENABLED_DEFAULT: bool = true;

/// Leader balance check interval (in milliseconds).
pub const PD_SCHEDULE_BALANCE_LEADER_INTERVAL_MS: &str = "pd.schedule.balance_leader_interval_ms";
pub const PD_SCHEDULE_BALANCE_LEADER_INTERVAL_MS_DEFAULT: u64 = 30_000;

/// Balance tolerant ratio in basis points (e.g. 500 = 5%).
pub const PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS: &str = "pd.schedule.balance_tolerant_ratio_bps";
pub const PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS_DEFAULT: u32 = 500;

/// Max balance operators per cycle.
pub const PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE: &str = "pd.schedule.balance_max_ops_per_cycle";
pub const PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE_DEFAULT: u32 = 3;

/// Enable placement rule violation checker.
pub const PD_SCHEDULE_PLACEMENT_CHECK_ENABLED: &str = "pd.schedule.placement_check_enabled";
pub const PD_SCHEDULE_PLACEMENT_CHECK_ENABLED_DEFAULT: bool = true;

/// Placement rule check interval (in milliseconds).
pub const PD_SCHEDULE_PLACEMENT_CHECK_INTERVAL_MS: &str = "pd.schedule.placement_check_interval_ms";
pub const PD_SCHEDULE_PLACEMENT_CHECK_INTERVAL_MS_DEFAULT: u64 = 30_000;

/// Default location labels for isolation (comma-separated, e.g. "az,rack,host").
pub const PD_POOL_DEFAULT_LOCATION_LABELS: &str = "pd.pool.default_location_labels";
pub const PD_POOL_DEFAULT_LOCATION_LABELS_DEFAULT: &str = "";

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
        key: PD_BG_DEFAULT_BUCKET_COUNT,
        default: "1024",
        desc: "Default bucket count for new BGTables",
    },
    DynamicConfigItem {
        key: PD_BG_DEFAULT_REPLICA_COUNT,
        default: "3",
        desc: "Default replica count for new BGTables",
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
        key: PD_SCHEDULE_OPERATOR_MAX_LIFETIME_MS,
        default: "3600000",
        desc: "Max operator lifetime before cancellation in milliseconds",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_SUSPECT_MAX_CHECKS,
        default: "5",
        desc: "Max times a suspect BG is checked before auto-clearing",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_SUSPECT_TTL_MS,
        default: "300000",
        desc: "TTL for suspect BG entries in milliseconds",
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
        key: PD_SCHEDULE_PLACEMENT_CHECK_ENABLED,
        default: "true",
        desc: "Enable placement rule violation checker",
    },
    DynamicConfigItem {
        key: PD_SCHEDULE_PLACEMENT_CHECK_INTERVAL_MS,
        default: "30000",
        desc: "Placement rule check interval in milliseconds",
    },
    DynamicConfigItem {
        key: PD_POOL_DEFAULT_LOCATION_LABELS,
        default: "",
        desc: "Default location labels for isolation (comma-separated)",
    },
];
