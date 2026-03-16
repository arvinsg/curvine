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

use crate::pd::http::ApiResponse;
use crate::pd::http_handler::PdHttpHandler;
use axum::{
    extract::Path as PathParam,
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use curvine_common::state::{NodeInfo, NodeState, NodeType};
use serde::Deserialize;
use std::sync::Arc;

/// GET /api/v1/node/:node_type — list nodes by type ("worker" or "meta").
pub async fn list_nodes_by_type_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    PathParam(node_type): PathParam<String>,
) -> impl IntoResponse {
    let cluster = &instance.cluster_manager;
    let nt = match node_type.to_lowercase().as_str() {
        "worker" => NodeType::Worker,
        "meta" => NodeType::Meta,
        _ => {
            return ApiResponse::<Vec<NodeInfo>>::error(
                "INVALID_INPUT".to_string(),
                format!("unknown node type: {}, expected worker or meta", node_type),
                StatusCode::BAD_REQUEST,
            );
        }
    };
    let nodes = cluster.node_manager().get_nodes_by_type(nt);
    ApiResponse::success(nodes)
}

/// GET /api/v1/node/detail/:node_id — get single node detail.
pub async fn get_node_detail_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    PathParam(node_id): PathParam<u32>,
) -> impl IntoResponse {
    let cluster = &instance.cluster_manager;
    match cluster.node_manager().get_node(node_id) {
        Some(node) => ApiResponse::success(node),
        None => ApiResponse::<NodeInfo>::error(
            "NOT_FOUND".to_string(),
            format!("node {} not found", node_id),
            StatusCode::NOT_FOUND,
        ),
    }
}

#[derive(Debug, Deserialize)]
pub struct DecommissionBody {
    pub node_id: u32,
    #[serde(default)]
    pub wait_migration: bool,
}

/// POST /api/v1/node/decommission — decommission a node.
pub async fn decommission_node_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Json(body): Json<DecommissionBody>,
) -> impl IntoResponse {
    let cluster = &instance.cluster_manager;
    match cluster.handle_decommission(body.node_id, body.wait_migration) {
        Ok(state) => ApiResponse::success(state),
        Err(e) => ApiResponse::<NodeState>::error(
            "DECOMMISSION_ERROR".to_string(),
            e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}
