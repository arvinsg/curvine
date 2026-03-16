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
];
