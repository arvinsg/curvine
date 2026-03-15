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
use axum::{extract::Path as PathParam, http::StatusCode, response::IntoResponse, Extension, Json};
use curvine_common::state::BGTableSummary;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// GET /api/v1/bg/table — list all BG tables (internal view).
pub async fn list_bg_tables_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
) -> impl IntoResponse {
    let bg_mgr = instance.cluster_manager.bg_manager();
    ApiResponse::success(bg_mgr.list_tables())
}

/// GET /api/v1/bg/table/:table_id — get BG table summary (client-facing view with replica addresses).
pub async fn get_bg_table_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    PathParam(table_id): PathParam<u32>,
) -> impl IntoResponse {
    match instance
        .cluster_manager
        .bg_manager()
        .build_table_summary(table_id, &instance.cluster_manager.node_manager())
    {
        Some(summary) => ApiResponse::success(summary),
        None => ApiResponse::<BGTableSummary>::success_with_status_code(StatusCode::NOT_FOUND),
    }
}

#[derive(Debug, Deserialize)]
pub struct RebuildBody {
    pub pool_id: u16,
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
        .bg_manager()
        .rebuild_tables_for_pool(body.pool_id)
    {
        Ok(()) => ApiResponse::success(RebuildResult { success: true }),
        Err(e) => ApiResponse::<RebuildResult>::error(
            "REBUILD_ERROR".to_string(),
            e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}
