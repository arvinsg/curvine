use super::error::ClusterError;
use crate::pd::http::ApiResponse;
use crate::pd::http_handler::PdHttpHandler;
use axum::{extract::Path as PathParam, http::StatusCode, response::IntoResponse, Extension, Json};
use curvine_common::state::{BGTableSummary, NodeInfo, NodeState, NodeType, PoolInfo, StorageType};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ========== Node ==========

/// GET /api/v1/node/:node_type — list nodes by type.
pub async fn list_nodes_by_type_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    PathParam(node_type): PathParam<String>,
) -> impl IntoResponse {
    let cluster = &instance.cluster_manager;
    let nt = match node_type.to_lowercase().as_str() {
        "worker" => NodeType::Worker,
        "meta" => NodeType::Meta,
        "task" => NodeType::Task,
        _ => {
            let err = ClusterError::invalid_node_type(node_type);
            return ApiResponse::<Vec<NodeInfo>>::error(
                err.code().into(),
                err.to_string(),
                err.status_code(),
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
        None => {
            let err = ClusterError::node_not_found(node_id);
            ApiResponse::<NodeInfo>::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

/// POST /api/v1/node/decommission/:node_id — start decommissioning a node.
pub async fn decommission_node_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    PathParam(node_id): PathParam<u32>,
) -> impl IntoResponse {
    let node_manager = instance.cluster_manager.node_manager();
    match node_manager.start_decommission(node_id) {
        Ok(state) => ApiResponse::success(state),
        Err(e) => {
            let err = ClusterError::internal_error(e);
            ApiResponse::<NodeState>::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

// ========== Pool ==========

/// GET /api/v1/pool — list active pools.
pub async fn list_pools_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
) -> impl IntoResponse {
    ApiResponse::success(instance.cluster_manager.pool_manager().list_active_pools())
}

/// GET /api/v1/pool/:pool_type — get single pool info.
pub async fn get_pool_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    PathParam(pool_type): PathParam<String>,
) -> impl IntoResponse {
    let parsed = match StorageType::try_from(pool_type.as_str()) {
        Ok(v) => v,
        Err(_) => {
            let err = ClusterError::pool_not_found(pool_type);
            return ApiResponse::<PoolInfo>::error(
                err.code().into(),
                err.to_string(),
                err.status_code(),
            );
        }
    };
    let pool_mgr = instance.cluster_manager.pool_manager();
    match pool_mgr.get_pool(parsed) {
        Ok(pool) => ApiResponse::success(pool),
        Err(_) => {
            let err = ClusterError::pool_not_found(parsed.to_string());
            ApiResponse::<PoolInfo>::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

// ========== BG ==========

/// GET /api/v1/bg/table — list all BG tables (internal view).
pub async fn list_bg_tables_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
) -> impl IntoResponse {
    let bgtable_mgr = instance.cluster_manager.bgtable_manager();
    // Deref Arc<BGTable> for serde — the Arc wrapper isn't Serialize without
    // the `serde_with` rc feature.
    let tables: Vec<crate::pd::bgtable::BGTable> = bgtable_mgr
        .list_tables()
        .into_iter()
        .map(|arc| (*arc).clone())
        .collect();
    ApiResponse::success(tables)
}

/// GET /api/v1/bg/table/:table_id — get BG table summary (client-facing view with replica addresses).
pub async fn get_bg_table_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    PathParam(table_id): PathParam<u32>,
) -> impl IntoResponse {
    match instance
        .cluster_manager
        .bgtable_manager()
        .build_table_summary(table_id as curvine_common::state::TableId)
    {
        Some(summary) => ApiResponse::success(summary),
        None => ApiResponse::<BGTableSummary>::success_with_status_code(StatusCode::NOT_FOUND),
    }
}

#[derive(Debug, Deserialize)]
pub struct RebuildBody {
    pub pool_type: StorageType,
}

#[derive(Debug, Serialize, Default)]
pub struct RebuildResult {
    pub success: bool,
}

/// POST /api/v1/bg/rebuild — manually trigger BGTable rebuild for a pool.
pub async fn rebuild_bg_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Json(body): Json<RebuildBody>,
) -> impl IntoResponse {
    match instance
        .cluster_manager
        .bgtable_manager()
        .hash_placement()
        .rebuild_tables_for_pool(body.pool_type)
    {
        Ok(()) => ApiResponse::success(RebuildResult { success: true }),
        Err(e) => {
            let err = ClusterError::internal_error(e);
            ApiResponse::<RebuildResult>::error(
                err.code().into(),
                err.to_string(),
                err.status_code(),
            )
        }
    }
}
