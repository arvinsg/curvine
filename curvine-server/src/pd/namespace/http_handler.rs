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
use crate::pd::namespace::{NamespaceError, NamespaceManager};
use axum::{extract::Query, http::StatusCode, response::IntoResponse, Extension, Json};
use curvine_common::error::FsError;
use curvine_common::state::{
    CreateNamespaceRequest, NamespaceId, NamespaceInfo, UpdateNamespaceRequest,
};
use curvine_common::FsResult;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct NamespaceQueryParams {
    pub id: Option<NamespaceId>,
    pub name: Option<String>,
}

/// Turn a namespace `FsResult<NamespaceInfo>` into an API response, mapping
/// errors through `NamespaceError` for a stable code + HTTP status.
fn namespace_response(result: FsResult<NamespaceInfo>) -> ApiResponse<NamespaceInfo> {
    match result {
        Ok(namespace) => ApiResponse::success(namespace),
        Err(e) => {
            let err = NamespaceError::from_fs_error(e);
            ApiResponse::<NamespaceInfo>::error(
                err.code().to_string(),
                err.to_string(),
                err.status_code(),
            )
        }
    }
}

/// POST /api/v1/namespace — create a namespace and return the created info.
pub async fn create_namespace_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Json(request): Json<CreateNamespaceRequest>,
) -> impl IntoResponse {
    let name = request.name.clone();
    let manager = &instance.namespace_manager;
    let result = manager.create_namespace(request).and_then(|()| {
        manager
            .get_namespace_by_name(&name)
            .map(|ns| (*ns).clone())
            .ok_or_else(|| FsError::common(format!("namespace {} created but not found", name)))
    });
    namespace_response(result)
}

/// PUT /api/v1/namespace — apply a field-level patch and return the updated info.
pub async fn update_namespace_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Json(request): Json<UpdateNamespaceRequest>,
) -> impl IntoResponse {
    let id = request.id;
    let manager = &instance.namespace_manager;
    let result = manager.update_namespace(request).and_then(|()| {
        manager
            .get_namespace(id)
            .map(|ns| (*ns).clone())
            .ok_or_else(|| FsError::common(format!("namespace {} updated but not found", id)))
    });
    namespace_response(result)
}

/// GET /api/v1/namespace — query by `id or name`, or list all when neither is set.
pub async fn list_or_get_namespace_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Query(params): Query<NamespaceQueryParams>,
) -> impl IntoResponse {
    let manager = &instance.namespace_manager;
    let found = match (params.id, params.name.as_deref()) {
        (Some(id), _) => manager.get_namespace(id),
        (None, Some(name)) => manager.get_namespace_by_name(name),
        (None, None) => return list_all_namespaces(manager),
    };
    match found {
        Some(namespace) => ApiResponse::success(vec![(*namespace).clone()]),
        None => ApiResponse::<Vec<NamespaceInfo>>::success_with_status_code(StatusCode::NOT_FOUND),
    }
}

fn list_all_namespaces(manager: &NamespaceManager) -> ApiResponse<Vec<NamespaceInfo>> {
    let mut namespaces: Vec<NamespaceInfo> = manager
        .list_namespaces()
        .into_iter()
        .map(|ns| (*ns).clone())
        .collect();
    namespaces.sort_by_key(|ns| ns.id);
    ApiResponse::success(namespaces)
}
