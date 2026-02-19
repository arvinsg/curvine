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

use std::collections::HashMap;
use std::sync::OnceLock;

/// Known config keys -> description. Add entries when introducing new config items.
fn build_all_configs_keys() -> HashMap<&'static str, &'static str> {
    [(
        "pd.scheduler.max_inflight_moves",
        "max inflight moves for pd scheduler",
    )]
    .into_iter()
    .collect()
}

static ALL_CONFIGS_KEYS: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();

fn all_configs_keys() -> &'static HashMap<&'static str, &'static str> {
    ALL_CONFIGS_KEYS.get_or_init(build_all_configs_keys)
}

/// Returns true if the key is a known config key that can be set/deleted.
#[inline]
pub fn is_valid_key(key: &str) -> bool {
    all_configs_keys().contains_key(key)
}

/// Returns the description for a config key, or None if unknown.
pub fn config_description(key: &str) -> Option<&'static str> {
    all_configs_keys().get(key).copied()
}

/// Returns all known config keys (for listing / docs).
pub fn all_config_keys() -> Vec<&'static str> {
    all_configs_keys().keys().copied().collect()
}

/// Returns an error message for unknown key (caller can wrap in FsError).
pub fn unknown_key_error(key: &str, action: &str) -> String {
    format!(
        "unknown config key: {} (only known keys can be {})",
        key, action
    )
}
