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

use crate::state::meta_node_info::NodeGroupInfo;
use crate::FsError;
use crate::FsResult;
use once_cell::sync::OnceCell;
use orpc::common::Utils;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MetaNode service mode: Proxy, Shard, or Federation. Only one mode per cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MetaNodeMode {
    /// MetaNode as proxy; metadata in distributed KV; client picks any MetaNode. Not implemented.
    Proxy,
    /// MetaNode with shard info; client must select by shard. Not implemented.
    Shard,
    /// MetaNode as Raft groups; PD maintains path table (Static or Hash). Implemented.
    #[default]
    Federation,
}

/// Federation sub-mode: how path maps to group (mutually exclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum FederationRouteMode {
    /// Static path -> group_id table (longest prefix match).
    Static,
    /// Hash by path component at configured level to select group.
    #[default]
    Hash,
}

/// Single path -> group_id entry (Federation Static).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PathRouteEntry {
    pub path: String,
    pub group_id: u64,
    pub create_time_ms: u64,
    pub update_time_ms: u64,
}

/// Trie node for longest-prefix path lookup, built from routes on first use.
struct PathTrieNode {
    group_id: Option<u64>,
    children: HashMap<char, Box<PathTrieNode>>,
}

impl PathTrieNode {
    fn new() -> Self {
        Self {
            group_id: None,
            children: HashMap::new(),
        }
    }

    fn insert(&mut self, path: &str, group_id: u64) {
        let mut node = self;
        for c in path.chars() {
            node = node
                .children
                .entry(c)
                .or_insert_with(|| Box::new(PathTrieNode::new()));
        }
        node.group_id = Some(group_id);
    }

    /// Longest prefix match: walk path, remember last group_id seen. O(path len).
    fn lookup(&self, path: &str) -> Option<u64> {
        let mut last = None;
        let mut node = self;
        for c in path.chars() {
            if let Some(child) = node.children.get(&c) {
                node = child;
                if let Some(gid) = node.group_id {
                    last = Some(gid);
                }
            } else {
                break;
            }
        }
        last
    }
}

/// Path route table (Federation Static). Version must be persisted and monotonically increasing.
#[derive(Serialize, Deserialize)]
pub struct PathRouteTable {
    pub version: u64,
    pub routes: Vec<PathRouteEntry>,
    pub last_update_ms: u64,
    #[serde(skip)]
    trie_cache: OnceCell<PathTrieNode>,
}

impl Clone for PathRouteTable {
    fn clone(&self) -> Self {
        Self {
            version: self.version,
            routes: self.routes.clone(),
            last_update_ms: self.last_update_ms,
            trie_cache: OnceCell::new(),
        }
    }
}

impl Default for PathRouteTable {
    fn default() -> Self {
        Self {
            version: 0,
            routes: Vec::new(),
            last_update_ms: 0,
            trie_cache: OnceCell::new(),
        }
    }
}

impl std::fmt::Debug for PathRouteTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathRouteTable")
            .field("version", &self.version)
            .field("routes", &self.routes)
            .field("last_update_ms", &self.last_update_ms)
            .finish()
    }
}

impl PathRouteTable {
    fn ensure_trie(&self) -> &PathTrieNode {
        self.trie_cache.get_or_init(|| {
            let mut root = PathTrieNode::new();
            for entry in &self.routes {
                root.insert(&entry.path, entry.group_id);
            }
            root
        })
    }

    /// Longest prefix match. O(path length) after first call (trie built once).
    pub fn lookup(&self, path: &str) -> Option<&PathRouteEntry> {
        let gid = self.ensure_trie().lookup(path)?;
        self.routes.iter().find(|e| e.group_id == gid)
    }

    pub fn lookup_group_id(&self, path: &str) -> Option<u64> {
        self.ensure_trie().lookup(path)
    }
}

/// Client caches this and uses route(path) for path -> group_id; no match falls back to min group_id.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetaRouteSummary {
    pub mode: MetaNodeMode,
    /// Monotonically increasing; client uses this to detect changes.
    pub version: u64,
    /// Some when mode is Federation.
    pub federation_route_mode: Option<FederationRouteMode>,
    /// Some when Federation + Static.
    pub path_table: Option<PathRouteTable>,
    /// Meta groups by group_id for O(1) lookup.
    pub meta_groups: HashMap<u64, NodeGroupInfo>,
    /// Ordered group ids for Federation Hash (hash % len indexes into this). Kept sorted for determinism.
    pub group_id_order: Vec<u64>,
    /// Some when Federation + Hash (directory level for shard_key).
    pub federation_hash_level: Option<u8>,
}

/// Extract path component at configured level (1-based). E.g. "/a/b/c" level 2 -> "b".
/// Returns empty string if path has fewer segments than level.
pub fn extract_shard_key(path: &str, hash_level: u8) -> String {
    let parts: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let level = hash_level as usize;
    if level == 0 || parts.len() < level {
        return String::new();
    }
    parts[level - 1].to_string()
}

impl MetaRouteSummary {
    /// Resolve path to group_id
    pub fn route(&self, path: &str) -> FsResult<u64> {
        match self.mode {
            MetaNodeMode::Proxy => Err(FsError::unsupported("MetaNode mode Proxy")),
            MetaNodeMode::Shard => Err(FsError::unsupported("MetaNode mode Shard")),
            MetaNodeMode::Federation => self.route_federation(path),
        }
    }

    fn route_federation(&self, path: &str) -> FsResult<u64> {
        if self.meta_groups.is_empty() {
            return Err(FsError::common("no meta groups available".to_string()));
        }
        let mode = self
            .federation_route_mode
            .ok_or_else(|| FsError::common("Federation route mode not set".to_string()))?;
        match mode {
            FederationRouteMode::Static => {
                let group_id = self
                    .path_table
                    .as_ref()
                    .and_then(|t| t.lookup_group_id(path));
                Ok(group_id.unwrap_or_else(|| self.meta_groups.keys().min().copied().unwrap_or(1)))
            }
            FederationRouteMode::Hash => {
                if self.group_id_order.is_empty() {
                    return Err(FsError::common(
                        "no group_id_order for Hash mode".to_string(),
                    ));
                }
                let level = self.federation_hash_level.unwrap_or(2);
                let shard_key = extract_shard_key(path, level);
                let hash = Utils::murmur3(shard_key.as_bytes());
                let i = (hash as usize) % self.group_id_order.len();
                Ok(self.group_id_order[i])
            }
        }
    }

    /// Lookup path and return the matching (or fallback) NodeGroupInfo.
    pub fn route_to_group(&self, path: &str) -> FsResult<&NodeGroupInfo> {
        let gid = self.route(path)?;
        self.meta_groups
            .get(&gid)
            .ok_or_else(|| FsError::common(format!("group {} not found in summary", gid)))
    }

    pub fn get_group(&self, group_id: u64) -> Option<&NodeGroupInfo> {
        self.meta_groups.get(&group_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, group_id: u64) -> PathRouteEntry {
        PathRouteEntry {
            path: path.to_string(),
            group_id,
            create_time_ms: 0,
            update_time_ms: 0,
        }
    }

    #[test]
    fn path_route_table_lookup() {
        let table = PathRouteTable {
            version: 1,
            routes: vec![
                entry("/user", 1),
                entry("/user/a", 2),
                entry("/user/a/b", 3),
            ],
            last_update_ms: 0,
            trie_cache: once_cell::sync::OnceCell::new(),
        };
        assert_eq!(table.lookup_group_id("/user"), Some(1));
        assert_eq!(table.lookup_group_id("/user/a"), Some(2));
        assert_eq!(table.lookup_group_id("/user/a/b/c"), Some(3));
        assert_eq!(table.lookup_group_id("/other"), None);
    }

    #[test]
    fn summary_federation_static_fallback() {
        let mut meta_groups = HashMap::new();
        meta_groups.insert(
            5,
            NodeGroupInfo {
                group_id: 5,
                peers: vec![],
            },
        );
        meta_groups.insert(
            10,
            NodeGroupInfo {
                group_id: 10,
                peers: vec![],
            },
        );
        let summary = MetaRouteSummary {
            mode: MetaNodeMode::Federation,
            version: 1,
            federation_route_mode: Some(FederationRouteMode::Static),
            path_table: Some(PathRouteTable {
                version: 1,
                routes: vec![entry("/user/a", 10)],
                last_update_ms: 0,
                trie_cache: once_cell::sync::OnceCell::new(),
            }),
            meta_groups,
            group_id_order: vec![5, 10],
            federation_hash_level: None,
        };
        assert_eq!(summary.route("/user/a").unwrap(), 10);
        assert_eq!(summary.route("/no/match").unwrap(), 5);
        assert!(summary.get_group(5).is_some());
        assert!(summary.get_group(10).is_some());
    }

    #[test]
    fn summary_proxy_returns_error() {
        let summary = MetaRouteSummary {
            mode: MetaNodeMode::Proxy,
            ..Default::default()
        };
        assert!(summary.route("/any").is_err());
    }
}
