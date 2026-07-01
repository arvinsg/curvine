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
use crate::pd::namespace::NamespaceManager;
use axum::{extract::Query, http::StatusCode, response::IntoResponse, Extension, Json};
use curvine_common::error::FsError;
use curvine_common::state::{CreateNamespaceRequest, NamespaceId, NamespaceInfo};
use serde::Deserialize;
use std::sync::Arc;

const NAMESPACE_ERROR: &str = "NAMESPACE_ERROR";

#[derive(Debug, Deserialize)]
pub struct NamespaceQueryParams {
    pub id: Option<NamespaceId>,
    pub name: Option<String>,
}

/// Map a namespace `FsError` to an HTTP response. Kept local (no dedicated
/// NamespaceError type) because the mapping is small and stable.
fn namespace_error_response<T: Default>(e: FsError) -> ApiResponse<T> {
    let status = match &e {
        FsError::NotFound(_) => StatusCode::NOT_FOUND,
        FsError::AlreadyExists(_) | FsError::StaleEntry(_) => StatusCode::CONFLICT,
        FsError::InvalidArgument(_) | FsError::Common(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ApiResponse::<T>::error(NAMESPACE_ERROR.to_string(), e.to_string(), status)
}

/// POST /api/v1/namespace — create a namespace and return the created info.
pub async fn create_namespace_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Json(request): Json<CreateNamespaceRequest>,
) -> impl IntoResponse {
    let name = request.name.clone();
    let manager = &instance.namespace_manager;
    match manager.create_namespace(request) {
        Ok(()) => match manager.get_namespace_by_name(&name) {
            Some(namespace) => ApiResponse::success((*namespace).clone()),
            None => ApiResponse::<NamespaceInfo>::error(
                NAMESPACE_ERROR.to_string(),
                format!("namespace {} created but not found", name),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
        Err(e) => namespace_error_response(e),
    }
}

/// GET /api/v1/namespace — query by `?id=`, `?name=`, or list all when neither is set.
pub async fn list_or_get_namespace_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Query(params): Query<NamespaceQueryParams>,h
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
