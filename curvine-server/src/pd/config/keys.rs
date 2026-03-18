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
        key: PD_SCHEDULE_BG_CHECK_INTERVAL_MS,
        default: "10000",
        desc: "BG assignment check interval in milliseconds",
    },
];
