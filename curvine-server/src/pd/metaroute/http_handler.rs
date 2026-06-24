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
use crate::pd::metaroute::MetaRouteError;
use axum::{
    extract::{Path as PathParam, Query},
    response::IntoResponse,
    Extension, Json,
};
use curvine_common::state::{NodeGroupInfo, PathRouteEntry};
use serde::Deserialize;
use std::sync::Arc;

/// GET /api/v1/meta/route — path route table (Federation Static).
pub async fn get_path_route_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
) -> impl IntoResponse {
    let mm = instance.cluster_manager.metaroute_manager();
    ApiResponse::success(mm.get_path_route_table())
}

#[derive(Debug, Deserialize)]
pub struct AddOrUpdatePathRouteBody {
    pub path: String,
    pub group_id: u64,
}

/// POST /api/v1/meta/route — add or update path route.
pub async fn post_path_route_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Json(body): Json<AddOrUpdatePathRouteBody>,
) -> impl IntoResponse {
    if body.path.is_empty() {
        let err = MetaRouteError::path_empty();
        return ApiResponse::<()>::error(err.code().into(), err.to_string(), err.status_code());
    }
    let mm = instance.cluster_manager.metaroute_manager();
    let entry = PathRouteEntry {
        path: body.path,
        group_id: body.group_id,
        create_time_ms: 0,
        update_time_ms: 0,
        expected_table_version: 0,
    };
    match mm.add_route(entry) {
        Ok(()) => ApiResponse::<()>::success_with_status_code(axum::http::StatusCode::OK),
        Err(e) => {
            let err = MetaRouteError::route_error(e);
            ApiResponse::<()>::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

/// PUT /api/v1/meta/route — add or update path route (alias of POST).
pub async fn put_path_route_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Json(body): Json<AddOrUpdatePathRouteBody>,
) -> impl IntoResponse {
    post_path_route_handler(Extension(instance), Json(body)).await
}

#[derive(Debug, Deserialize)]
pub struct DeletePathRouteParams {
    pub path: Option<String>,
}

/// DELETE /api/v1/meta/route?path=/user/a — remove path route.
pub async fn delete_path_route_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Query(params): Query<DeletePathRouteParams>,
) -> impl IntoResponse {
    let path = match &params.path {
        Some(p) if !p.is_empty() => p.clone(),
        _ => {
            let err = MetaRouteError::path_required();
            return ApiResponse::<()>::error(err.code().into(), err.to_string(), err.status_code());
        }
    };
    let mm = instance.cluster_manager.metaroute_manager();
    match mm.remove_route(&path) {
        Ok(()) => ApiResponse::<()>::success_with_status_code(axum::http::StatusCode::NO_CONTENT),
        Err(e) => {
            let err = MetaRouteError::route_error(e);
            ApiResponse::<()>::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

/// GET /api/v1/meta/group
pub async fn list_meta_groups_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
) -> impl IntoResponse {
    let mm = instance.cluster_manager.metaroute_manager();
    ApiResponse::success(mm.get_active_groups())
}

/// GET /api/v1/meta/group/:group_id
pub async fn get_meta_group_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    PathParam(group_id): PathParam<u64>,
) -> impl IntoResponse {
    let mm = instance.cluster_manager.metaroute_manager();
    let groups = mm.get_active_groups();
    match groups.into_iter().find(|g| g.group_id == group_id) {
        Some(g) => ApiResponse::success(g),
        None => {
            let err = crate::pd::metaroute::MetaRouteError::group_not_found(group_id);
            ApiResponse::<NodeGroupInfo>::error(
                err.code().into(),
                err.to_string(),
                err.status_code(),
            )
        }
    }
}
