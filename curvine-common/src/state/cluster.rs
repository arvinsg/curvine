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

use super::{BGKind, MetaNodeMode, NamespaceId, StorageType, TableId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Lightweight cluster manifest used by clients/nodes to decide which detailed
/// views should be refreshed. It intentionally excludes node topology and
/// detailed BG/Meta/Mount data.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleClusterView {
    pub cluster_id: String,
    pub namespaces: Vec<SimpleNamespaceView>,
    pub mount: SimpleMountView,
    pub meta_route: SimpleMetaRouteView,
}

/// Namespace-to-BGTable binding summary. Namespace policy details are fetched
/// through the namespace APIs when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleNamespaceView {
    pub namespace_id: NamespaceId,
    pub name: String,
    pub cache_tier_tables: Vec<SimpleBGTableView>,
    pub write_buffer_table: Option<SimpleBGTableView>,
}

/// BGTable routing summary. The detailed route view is `BGTableSummary`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleBGTableView {
    pub table_id: TableId,
    pub kind: BGKind,
    pub storage_type: StorageType,
    pub replica_count: u16,
    pub route_epoch: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleMountView {
    pub version: u64,
    pub mounts: Vec<SimpleMountBrief>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleMountBrief {
    pub mount_id: u32,
    pub cv_path: String,
    pub namespace_id: NamespaceId,
    pub version: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleMetaRouteView {
    pub mode: MetaNodeMode,
    pub static_route_version: u64,
    pub group_view_epoch: u64,
}

/// Heartbeat hint for SimpleClusterView. Callers compare this with their local
/// manifest and fetch only the changed detailed views.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleClusterViewHint {
    pub mount_version: u64,
    pub bg_table_route_epochs: HashMap<TableId, u64>,
    pub meta_route: SimpleMetaRouteHint,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleMetaRouteHint {
    pub static_route_version: u64,
    pub group_view_epoch: u64,
}
