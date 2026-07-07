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

use super::manager::ClusterManager;
use crate::pd::bgtable::BGTable;
use curvine_common::state::{
    NodePayload, SimpleBGTableView, SimpleClusterView, SimpleClusterViewHint, SimpleMetaRouteHint,
    SimpleMetaRouteView, SimpleMountBrief, SimpleMountView, SimpleNamespaceView, TableId,
};
use curvine_common::{FsError, FsResult};
use std::hash::{Hash, Hasher};

impl ClusterManager {
    pub fn build_simple_cluster_view(&self) -> FsResult<SimpleClusterView> {
        Ok(SimpleClusterView {
            cluster_id: self.cluster_id.clone(),
            namespaces: self.build_simple_namespace_views()?,
            mount: self.build_simple_mount_view()?,
            meta_route: self.build_simple_meta_route_view(),
        })
    }

    pub fn build_simple_cluster_view_hint(&self) -> SimpleClusterViewHint {
        SimpleClusterViewHint {
            mount_version: self.mount_manager.version(),
            bg_table_route_epochs: self.bgtable_manager.get_table_epochs(),
            meta_route: self.build_simple_meta_route_hint(),
        }
    }

    fn build_simple_namespace_views(&self) -> FsResult<Vec<SimpleNamespaceView>> {
        let mut namespaces = self.namespace_manager.list_namespaces();
        namespaces.sort_by_key(|namespace| namespace.id);

        let mut views = Vec::with_capacity(namespaces.len());
        for namespace in namespaces {
            let mut cache_tier_tables = Vec::with_capacity(namespace.cache_tier_tables.len());
            for table_id in &namespace.cache_tier_tables {
                cache_tier_tables.push(self.build_simple_bg_table_view(*table_id)?);
            }
            let write_buffer_table = namespace
                .write_buffer_table
                .map(|table_id| self.build_simple_bg_table_view(table_id))
                .transpose()?;
            views.push(SimpleNamespaceView {
                namespace_id: namespace.id,
                name: namespace.name.clone(),
                cache_tier_tables,
                write_buffer_table,
            });
        }
        Ok(views)
    }

    fn build_simple_bg_table_view(&self, table_id: TableId) -> FsResult<SimpleBGTableView> {
        let table = self.bgtable_manager.get_table(table_id).ok_or_else(|| {
            FsError::common(format!(
                "namespace references missing bg table: table_id={}",
                table_id
            ))
        })?;
        Ok(simple_bg_table_view(&table))
    }

    fn build_simple_mount_view(&self) -> FsResult<SimpleMountView> {
        let mut mounts: Vec<SimpleMountBrief> = self
            .mount_manager
            .get_mount_table()?
            .into_iter()
            .map(|mount| SimpleMountBrief {
                mount_id: mount.mount_id,
                cv_path: mount.cv_path.clone(),
                namespace_id: mount.namespace_id,
                version: mount.version,
            })
            .collect();
        mounts.sort_by_key(|mount| mount.mount_id);
        Ok(SimpleMountView {
            version: self.mount_manager.version(),
            mounts,
        })
    }

    fn build_simple_meta_route_view(&self) -> SimpleMetaRouteView {
        let hint = self.build_simple_meta_route_hint();
        SimpleMetaRouteView {
            mode: self.metaroute_manager.mode(),
            static_route_version: hint.static_route_version,
            group_view_epoch: hint.group_view_epoch,
        }
    }

    fn build_simple_meta_route_hint(&self) -> SimpleMetaRouteHint {
        let path_table = self.metaroute_manager.get_path_route_table();
        SimpleMetaRouteHint {
            static_route_version: path_table.version,
            group_view_epoch: self.meta_group_view_epoch(),
        }
    }

    fn meta_group_view_epoch(&self) -> u64 {
        let mut nodes = self.metaroute_manager.list_meta_nodes();
        nodes.sort_by_key(|node| node.base.node_id);

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for node in nodes {
            node.base.node_id.hash(&mut hasher);
            node.epoch.hash(&mut hasher);
            node.state.hash(&mut hasher);
            node.base.address.hostname.hash(&mut hasher);
            node.base.address.ip.hash(&mut hasher);
            node.base.address.rpc_port.hash(&mut hasher);
            node.base.address.web_port.hash(&mut hasher);
            if let NodePayload::Meta(payload) = node.payload {
                payload.group_id.hash(&mut hasher);
                payload.group_epoch.hash(&mut hasher);
                payload.peers.len().hash(&mut hasher);
                for peer in payload.peers {
                    peer.node_id.hash(&mut hasher);
                    peer.is_leader.hash(&mut hasher);
                }
            }
        }
        hasher.finish()
    }
}

fn simple_bg_table_view(table: &BGTable) -> SimpleBGTableView {
    SimpleBGTableView {
        table_id: table.table_id(),
        kind: table.kind(),
        storage_type: table.storage_type(),
        replica_count: table.replica_count(),
        route_epoch: table.epoch(),
    }
}
