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
    /// MetaNode as Raft groups; PD maintains federation metadata. Implemented.
    #[default]
    Federation,
}

/// Federation route configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FederationRouteConfig {
    pub hash_level: Option<u8>,
}

impl FederationRouteConfig {
    pub fn new(hash_level: Option<u8>) -> Self {
        Self { hash_level }
    }
}

/// Single path -> group_id entry (Federation Static).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PathRouteEntry {
    pub path: String,
    pub group_id: u64,
    pub create_time_ms: u64,
    pub update_time_ms: u64,
}

/// Trie node for component-level longest-prefix path lookup.
#[derive(Debug)]
struct PathTrieNode {
    route_index: Option<usize>,
    children: HashMap<String, Box<PathTrieNode>>,
}

impl PathTrieNode {
    fn new() -> Self {
        Self {
            route_index: None,
            children: HashMap::new(),
        }
    }

    fn insert(&mut self, path: &str, route_index: usize) {
        let mut node = self;
        for component in path_components(path) {
            node = node
                .children
                .entry(component.to_string())
                .or_insert_with(|| Box::new(PathTrieNode::new()));
        }
        node.route_index = Some(route_index);
    }

    /// Longest path-component prefix match. `/user` matches `/user/a`,
    /// but does not match `/user2`.
    fn lookup(&self, path: &str) -> Option<usize> {
        let mut last = self.route_index;
        let mut node = self;
        for component in path_components(path) {
            let Some(child) = node.children.get(component) else {
                break;
            };
            node = child;
            if let Some(index) = node.route_index {
                last = Some(index);
            }
        }
        last
    }
}

fn path_components(path: &str) -> impl Iterator<Item = &str> {
    path.trim_matches('/').split('/').filter(|s| !s.is_empty())
}

/// Path route table (Federation Static).
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
            for (index, entry) in self.routes.iter().enumerate() {
                root.insert(&entry.path, index);
            }
            root
        })
    }

    /// Reset the cached trie. Must be called whenever `routes` is mutated.
    pub fn invalidate_cache(&mut self) {
        self.trie_cache = OnceCell::new();
    }

    /// Longest component-prefix match. O(path component count) after first lookup.
    pub fn lookup(&self, path: &str) -> Option<&PathRouteEntry> {
        let index = self.ensure_trie().lookup(path)?;
        self.routes.get(index)
    }

    pub fn lookup_group_id(&self, path: &str) -> Option<u64> {
        self.lookup(path).map(|entry| entry.group_id)
    }
}

/// Client caches this and uses route(path) for path -> meta group.
///
/// Federation routing order:
/// 1. Static longest-prefix route if configured and matched.
/// 2. Optional hash fallback when `federation_route_config.hash_level` is configured.
/// 3. Minimum group id fallback when no hash fallback is configured.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetaRouteSummary {
    pub mode: MetaNodeMode,
    /// Monotonically increasing; client uses this to detect changes.
    pub version: u64,
    /// Some when mode is Federation.
    pub federation_route_config: FederationRouteConfig,
    /// Some when Federation + Static.
    pub path_table: Option<PathRouteTable>,
    /// Meta groups by group_id for O(1) lookup.
    pub meta_groups: HashMap<u64, NodeGroupInfo>,
    /// Ordered group ids for Federation Hash. Kept sorted for determinism.
    pub group_id_order: Vec<u64>,
}

/// Extract path component at configured level (1-based). E.g. "/a/b/c" level 2 -> "b".
/// Returns empty string if path has fewer segments than level.
pub fn extract_shard_key(path: &str, hash_level: u8) -> String {
    let level = hash_level as usize;
    if level == 0 {
        return String::new();
    }
    path_components(path)
        .nth(level - 1)
        .unwrap_or_default()
        .to_string()
}

impl MetaRouteSummary {
    /// Resolve path to the target MetaNode group.
    pub fn route(&self, path: &str) -> FsResult<&NodeGroupInfo> {
        let group_id = self.route_group_id(path)?;
        self.meta_groups
            .get(&group_id)
            .ok_or_else(|| FsError::common(format!("group {} not found in summary", group_id)))
    }

    fn route_group_id(&self, path: &str) -> FsResult<u64> {
        match self.mode {
            MetaNodeMode::Proxy => Err(FsError::unsupported("MetaNode mode Proxy")),
            MetaNodeMode::Shard => Err(FsError::unsupported("MetaNode mode Shard")),
            MetaNodeMode::Federation => self.route_federation_group_id(path),
        }
    }

    fn route_federation_group_id(&self, path: &str) -> FsResult<u64> {
        if self.meta_groups.is_empty() {
            return Err(FsError::common("no meta groups available".to_string()));
        }
        if let Some(group_id) = self
            .path_table
            .as_ref()
            .and_then(|t| t.lookup_group_id(path))
        {
            return Ok(group_id);
        }
        if let Some(level) = self.federation_route_config.hash_level {
            return self.route_federation_hash_group_id(path, level);
        }
        self.min_group_id()
    }

    fn route_federation_hash_group_id(&self, path: &str, hash_level: u8) -> FsResult<u64> {
        if hash_level == 0 {
            return Err(FsError::common("federation hash_level must be >= 1"));
        }
        if self.group_id_order.is_empty() {
            return Err(FsError::common("group_id_order is empty"));
        }
        let shard_key = extract_shard_key(path, hash_level);
        let hash = Utils::murmur3(shard_key.as_bytes()) as usize;
        Ok(self.group_id_order[hash % self.group_id_order.len()])
    }

    fn min_group_id(&self) -> FsResult<u64> {
        self.group_id_order
            .iter()
            .min()
            .copied()
            .ok_or_else(|| FsError::common("group_id_order is empty".to_string()))
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

    fn group(group_id: u64) -> NodeGroupInfo {
        NodeGroupInfo {
            group_id,
            peers: vec![],
        }
    }

    fn groups(ids: &[u64]) -> HashMap<u64, NodeGroupInfo> {
        ids.iter().map(|id| (*id, group(*id))).collect()
    }

    fn table(routes: Vec<PathRouteEntry>) -> PathRouteTable {
        PathRouteTable {
            version: 1,
            routes,
            last_update_ms: 0,
            trie_cache: OnceCell::new(),
        }
    }

    fn summary(
        routes: Option<Vec<PathRouteEntry>>,
        group_ids: Vec<u64>,
        hash_level: Option<u8>,
    ) -> MetaRouteSummary {
        let mut group_id_order = group_ids.clone();
        group_id_order.sort_unstable();
        MetaRouteSummary {
            mode: MetaNodeMode::Federation,
            version: 1,
            federation_route_config: FederationRouteConfig::new(hash_level),
            path_table: routes.map(table),
            meta_groups: groups(&group_ids),
            group_id_order,
        }
    }

    #[test]
    fn extract_shard_key_cases() {
        for (path, level, expected) in [
            ("/user/a/file", 1, "user"),
            ("/user/a/file", 2, "a"),
            ("/user/a/file", 3, "file"),
            ("/user/a/file", 4, ""),
            ("/user//a/", 2, "a"),
            ("/a", 0, ""),
        ] {
            assert_eq!(
                extract_shard_key(path, level),
                expected,
                "path={path}, level={level}"
            );
        }
    }

    #[test]
    fn path_route_table_lookup_cases() {
        let table = table(vec![
            entry("/user", 1),
            entry("/user/a", 2),
            entry("/user/a/b", 3),
            entry("/same/group/a", 10),
            entry("/same/group/b", 10),
            entry("/", 99),
        ]);

        struct Case {
            path: &'static str,
            expected_group: Option<u64>,
            expected_route_path: Option<&'static str>,
        }

        for case in [
            Case {
                path: "/user",
                expected_group: Some(1),
                expected_route_path: Some("/user"),
            },
            Case {
                path: "/user/a",
                expected_group: Some(2),
                expected_route_path: Some("/user/a"),
            },
            Case {
                path: "/user/a/b/c",
                expected_group: Some(3),
                expected_route_path: Some("/user/a/b"),
            },
            Case {
                path: "/user2",
                expected_group: Some(99),
                expected_route_path: Some("/"),
            },
            Case {
                path: "/same/group/b/file",
                expected_group: Some(10),
                expected_route_path: Some("/same/group/b"),
            },
            Case {
                path: "/other",
                expected_group: Some(99),
                expected_route_path: Some("/"),
            },
        ] {
            assert_eq!(
                table.lookup_group_id(case.path),
                case.expected_group,
                "path={}",
                case.path
            );
            assert_eq!(
                table.lookup(case.path).map(|entry| entry.path.as_str()),
                case.expected_route_path,
                "path={}",
                case.path
            );
        }
    }

    #[test]
    fn path_route_table_invalidate_cache_cases() {
        let mut table = table(vec![entry("/data", 1)]);
        assert_eq!(table.lookup_group_id("/data/file"), Some(1));

        table.routes.push(entry("/data/file", 2));
        assert_eq!(
            table.lookup_group_id("/data/file"),
            Some(1),
            "lookup should use old trie before invalidation"
        );

        table.invalidate_cache();
        assert_eq!(table.lookup_group_id("/data/file"), Some(2));
    }

    #[test]
    fn summary_route_cases() {
        struct Case {
            name: &'static str,
            routes: Option<Vec<PathRouteEntry>>,
            group_ids: Vec<u64>,
            group_id_order_override: Option<Vec<u64>>,
            hash_level: Option<u8>,
            path: &'static str,
            expected_group: Option<u64>,
            expected_error: bool,
        }

        let hash_expected = |path: &str, level: u8, groups: &[u64]| {
            let shard_key = extract_shard_key(path, level);
            let hash = Utils::murmur3(shard_key.as_bytes());
            groups[(hash as usize) % groups.len()]
        };

        let cases = vec![
            Case {
                name: "static hit wins even when hash is configured",
                routes: Some(vec![entry("/user/a", 10)]),
                group_ids: vec![5, 10],
                group_id_order_override: None,
                hash_level: Some(2),
                path: "/user/a/file",
                expected_group: Some(10),
                expected_error: false,
            },
            Case {
                name: "static miss without hash falls back to min group id",
                routes: Some(vec![entry("/user/a", 10)]),
                group_ids: vec![5, 10],
                group_id_order_override: None,
                hash_level: None,
                path: "/no/match",
                expected_group: Some(5),
                expected_error: false,
            },
            Case {
                name: "empty static without hash falls back to min group id",
                routes: Some(vec![]),
                group_ids: vec![10, 5],
                group_id_order_override: None,
                hash_level: None,
                path: "/no/match",
                expected_group: Some(5),
                expected_error: false,
            },
            Case {
                name: "no static table without hash falls back to min group id",
                routes: None,
                group_ids: vec![10, 5],
                group_id_order_override: None,
                hash_level: None,
                path: "/no/match",
                expected_group: Some(5),
                expected_error: false,
            },
            Case {
                name: "static miss with hash uses hash fallback",
                routes: Some(vec![entry("/user/a", 10)]),
                group_ids: vec![5, 10],
                group_id_order_override: None,
                hash_level: Some(2),
                path: "/cold/a",
                expected_group: Some(hash_expected("/cold/a", 2, &[5, 10])),
                expected_error: false,
            },
            Case {
                name: "invalid hash level returns error",
                routes: None,
                group_ids: vec![5, 10],
                group_id_order_override: None,
                hash_level: Some(0),
                path: "/cold/a",
                expected_group: None,
                expected_error: true,
            },
            Case {
                name: "empty meta groups returns error",
                routes: None,
                group_ids: vec![],
                group_id_order_override: None,
                hash_level: None,
                path: "/cold/a",
                expected_group: None,
                expected_error: true,
            },
            Case {
                name: "hash fallback requires group id order",
                routes: None,
                group_ids: vec![5, 10],
                group_id_order_override: Some(vec![]),
                hash_level: Some(2),
                path: "/cold/a",
                expected_group: None,
                expected_error: true,
            },
        ];

        for case in cases {
            let mut summary = summary(case.routes, case.group_ids, case.hash_level);
            if let Some(group_id_order) = case.group_id_order_override {
                summary.group_id_order = group_id_order;
            }
            match (summary.route(case.path), case.expected_group, case.expected_error) {
                (Ok(group), Some(expected), false) => {
                    assert_eq!(group.group_id, expected, "{}", case.name)
                }
                (Err(_), None, true) => {}
                (actual, expected, expected_error) => panic!(
                    "unexpected route result for {}: actual={:?}, expected_group={:?}, expected_error={}",
                    case.name, actual, expected, expected_error
                ),
            }
        }
    }

    #[test]
    fn summary_get_group_cases() {
        let summary = summary(Some(vec![entry("/user/a", 10)]), vec![5, 10], None);
        assert_eq!(summary.route("/user/a/file").unwrap().group_id, 10);
        assert_eq!(summary.route("/no/match").unwrap().group_id, 5);
        assert!(summary.get_group(5).is_some());
        assert!(summary.get_group(99).is_none());
    }

    #[test]
    fn non_federation_modes_return_error() {
        for mode in [MetaNodeMode::Proxy, MetaNodeMode::Shard] {
            let summary = MetaRouteSummary {
                mode,
                ..Default::default()
            };
            assert!(summary.route("/any").is_err(), "mode={mode:?}");
        }
    }
}
