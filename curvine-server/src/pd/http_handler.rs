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

use crate::pd::cluster::http_handler::*;
use crate::pd::cluster::ClusterManager;
use crate::pd::config::http_handler::*;
use crate::pd::config::ConfigManager;
use crate::pd::meta::http_handler::*;
use crate::pd::mount::http_handler::*;
use crate::pd::mount::MountManager;
use crate::pd::pd_server::Pd;
use axum::routing::{delete, get, post, put};
use axum::{Extension, Router};
use curvine_web::router::RouterHandler;
use std::sync::Arc;

#[derive(Clone)]
pub struct PdHttpHandler {
    pub(crate) config_manager: Arc<ConfigManager>,
    pub(crate) mount_manager: Arc<MountManager>,
    pub(crate) cluster_manager: Arc<ClusterManager>,
}

impl PdHttpHandler {
    pub fn new(
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        Self {
            config_manager,
            mount_manager,
            cluster_manager,
        }
    }
}

async fn pd_metrics_handler() -> String {
    Pd::get_metrics().text_output().unwrap_or_default()
}

impl RouterHandler for PdHttpHandler {
    fn router(&self) -> Router {
        let instance = Arc::new(self.clone());
        Router::new()
            // Metrics
            .route("/metrics", get(pd_metrics_handler))
            // Config
            .route("/api/v1/config/set", put(set_config_by_query_handler))
            .route("/api/v1/config/:key", get(get_config_handler))
            .route("/api/v1/config/:key", put(set_config_handler))
            .route("/api/v1/config", get(list_configs_handler))
            // Mount
            .route("/api/v1/mount", get(list_mounts_handler))
            .route("/api/v1/mount", post(create_mount_handler))
            .route("/api/v1/mount", delete(delete_mount_handler))
            .route("/api/v1/mount/path", get(get_mount_by_path_handler))
            // Node
            .route("/api/v1/node/:node_type", get(list_nodes_by_type_handler))
            .route("/api/v1/node/detail/:node_id", get(get_node_detail_handler))
            .route(
                "/api/v1/node/decommission/:node_id",
                post(decommission_node_handler),
            )
            // Pool
            .route("/api/v1/pool", get(list_pools_handler))
            .route("/api/v1/pool/:pool_type", get(get_pool_handler))
            // BG
            .route("/api/v1/bg/table", get(list_bg_tables_handler))
            .route("/api/v1/bg/table/:table_id", get(get_bg_table_handler))
            .route("/api/v1/bg/rebuild", post(rebuild_bg_handler))
            // Meta
            .route("/api/v1/meta/route", get(get_path_route_handler))
            .route("/api/v1/meta/route", post(post_path_route_handler))
            .route("/api/v1/meta/route", put(put_path_route_handler))
            .route("/api/v1/meta/route", delete(delete_path_route_handler))
            .route("/api/v1/meta/group", get(list_meta_groups_handler))
            .route("/api/v1/meta/group/:group_id", get(get_meta_group_handler))
            .layer(Extension(instance))
    }
}
