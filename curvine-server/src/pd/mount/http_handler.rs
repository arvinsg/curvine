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
use crate::pd::mount::{MountError, MountListResponse};
use axum::{extract::Query, http::StatusCode, response::IntoResponse, Extension, Json};
use curvine_common::fs::Path;
use curvine_common::state::{self, MountInfo};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct CreateMountBody {
    pub cv_path: String,
    pub ufs_path: String,
    #[serde(default)]
    pub properties: std::collections::HashMap<String, String>,
    pub ttl_ms: Option<i64>,
    pub ttl_action: Option<String>,
    pub write_type: Option<String>,
    pub replicas: Option<i32>,
    pub consistency_strategy: Option<String>,
    pub mount_type: Option<String>,
    pub block_size: Option<i64>,
    pub provider: Option<String>,
}

fn str_to_mount_type(s: &str) -> state::MountType {
    match s.to_uppercase().as_str() {
        "ORCH" => state::MountType::Orch,
        _ => state::MountType::Cst,
    }
}

pub async fn create_mount_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Json(body): Json<CreateMountBody>,
) -> impl IntoResponse {
    if body.cv_path.is_empty() || body.ufs_path.is_empty() {
        let err = MountError::cv_ufs_required();
        return ApiResponse::<()>::error(err.code().into(), err.to_string(), err.status_code());
    }

    let mut builder = state::MountOptionsBuilder::new();
    builder = builder.set_properties(body.properties);
    if let Some(t) = body.ttl_ms {
        builder = builder.ttl_ms(t);
    }
    if let Some(ref s) = body.ttl_action {
        if let Ok(ta) = state::TtlAction::try_from(s.as_str()) {
            builder = builder.ttl_action(ta);
        }
    }
    if let Some(ref s) = body.consistency_strategy {
        if let Ok(cs) = state::ConsistencyStrategy::try_from(s.as_str()) {
            builder = builder.consistency_strategy(cs);
        }
    }
    if let Some(b) = body.block_size {
        builder = builder.block_size(b);
    }
    if let Some(r) = body.replicas {
        builder = builder.replicas(r);
    }
    if let Some(ref s) = body.mount_type {
        builder = builder.mount_type(str_to_mount_type(s));
    }
    if let Some(ref s) = body.write_type {
        if let Ok(wt) = state::WriteType::try_from(s.as_str()) {
            builder = builder.write_type(wt);
        }
    }
    if let Some(ref s) = body.provider {
        if let Ok(p) = state::Provider::try_from(s.as_str()) {
            builder = builder.provider(p);
        }
    }
    let mnt_opt = builder.build();

    match instance
        .mount_manager
        .mount(None, &body.cv_path, &body.ufs_path, &mnt_opt)
    {
        Ok(()) => ApiResponse::<()>::success_with_status_code(StatusCode::OK),
        Err(e) => {
            let err = MountError::from_fs_error(e);
            ApiResponse::<()>::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ListMountsParams {
    pub prefix: Option<String>,
    pub limit: Option<u32>,
}

pub async fn list_mounts_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Query(params): Query<ListMountsParams>,
) -> impl IntoResponse {
    match instance.mount_manager.get_mount_table() {
        Ok(table) => {
            let mut mounts = table;
            if let Some(prefix) = &params.prefix {
                if !prefix.is_empty() {
                    mounts.retain(|m| {
                        m.cv_path.starts_with(prefix) || m.ufs_path.starts_with(prefix)
                    });
                }
            }
            let limit = params.limit.unwrap_or(100).min(1000);
            if mounts.len() > limit as usize {
                mounts.truncate(limit as usize);
            }
            ApiResponse::success(MountListResponse::new(mounts))
        }
        Err(e) => {
            let err = MountError::from_fs_error(e);
            ApiResponse::<MountListResponse>::error(
                err.code().into(),
                err.to_string(),
                err.status_code(),
            )
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct GetMountByPathParams {
    pub path: Option<String>,
}

pub async fn get_mount_by_path_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Query(params): Query<GetMountByPathParams>,
) -> impl IntoResponse {
    let cv_path = match &params.path {
        Some(p) if !p.is_empty() => p.clone(),
        _ => {
            let err = MountError::path_query_required();
            return ApiResponse::<MountInfo>::error(
                err.code().into(),
                err.to_string(),
                err.status_code(),
            );
        }
    };

    let path = match Path::from_str(&cv_path) {
        Ok(p) => p,
        Err(e) => {
            let err = MountError::invalid_path(e);
            return ApiResponse::<MountInfo>::error(
                err.code().into(),
                err.to_string(),
                err.status_code(),
            );
        }
    };

    match instance.mount_manager.get_mount_info(&path) {
        Ok(Some(info)) => ApiResponse::success(info),
        Ok(None) => ApiResponse::<MountInfo>::success_with_status_code(StatusCode::NOT_FOUND),
        Err(e) => {
            let err = MountError::from_fs_error(e);
            ApiResponse::<MountInfo>::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeleteMountParams {
    pub cv_path: Option<String>,
}

pub async fn delete_mount_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Query(params): Query<DeleteMountParams>,
) -> impl IntoResponse {
    let cv_path = match &params.cv_path {
        Some(p) if !p.is_empty() => p.clone(),
        _ => {
            let err = MountError::path_query_required();
            return ApiResponse::<()>::error(err.code().into(), err.to_string(), err.status_code());
        }
    };

    match instance.mount_manager.umount(&cv_path) {
        Ok(()) => ApiResponse::<()>::success_with_status_code(StatusCode::NO_CONTENT),
        Err(e) => {
            let err = MountError::from_fs_error(e);
            ApiResponse::<()>::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}
