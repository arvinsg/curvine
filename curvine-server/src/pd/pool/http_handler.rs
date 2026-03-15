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
use axum::{extract::Path as PathParam, http::StatusCode, response::IntoResponse, Extension};
use curvine_common::state::PoolInfo;
use std::sync::Arc;

/// GET /api/v1/pool — list active pools.
pub async fn list_pools_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
) -> impl IntoResponse {
    ApiResponse::success(instance.cluster_manager.pool_manager().list_active_pools())
}

/// GET /api/v1/pool/:pool_id — get single pool info.
pub async fn get_pool_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    PathParam(pool_id): PathParam<u16>,
) -> impl IntoResponse {
    let pool_mgr = instance.cluster_manager.pool_manager();
    match pool_mgr.get_pool(pool_id) {
        Ok(pool) => ApiResponse::success(pool),
        Err(_) => ApiResponse::<PoolInfo>::success_with_status_code(StatusCode::NOT_FOUND),
    }
}
