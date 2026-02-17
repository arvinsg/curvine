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

use crate::pd::config::{ConfigError, ConfigListResponse};
use crate::pd::http::ApiResponse;
use crate::pd::http_handler::PdHttpHandler;
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use curvine_common::proto::{DeleteConfigRequest, GetConfigRequest, ListConfigRequest};
use curvine_common::utils::ProtoUtils;
use serde::Deserialize;
use std::sync::Arc;

pub async fn get_config_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let req = GetConfigRequest { key };
    match instance.config_manager.get_config(req) {
        Ok(resp) => match resp.item {
            Some(item) => ApiResponse::success(ProtoUtils::config_info_from_pb(item)),
            None => ApiResponse::success_with_status_code(StatusCode::NOT_FOUND),
        },
        Err(e) => {
            let err = ConfigError::internal_error(e);
            ApiResponse::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SetConfigBody {
    value: String,
}

#[derive(Debug, Deserialize)]
pub struct SetConfigQueryParams {
    pub key: Option<String>,
    pub value: Option<String>,
}

pub async fn set_config_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Path(key): Path<String>,
    Query(params): Query<SetConfigQueryParams>,
    body: Option<Json<SetConfigBody>>,
) -> impl IntoResponse {
    let value = match body {
        Some(b) => b.value.clone().into_bytes(),
        None => match params.value {
            Some(v) => v.into_bytes(),
            None => {
                let err = ConfigError::missing_value();
                return ApiResponse::error(
                    err.code().into(),
                    err.to_string(),
                    err.status_code(),
                );
            }
        },
    };
    let req = ProtoUtils::set_config_request_from_http(key, value);
    match instance.config_manager.set_config(req) {
        Ok(_) => ApiResponse::success(None),
        Err(e) => {
            let err = ConfigError::internal_error(e);
            ApiResponse::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

pub async fn set_config_by_query_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Query(params): Query<SetConfigQueryParams>,
) -> impl IntoResponse {
    let key = match params.key {
        Some(k) => k,
        None => {
            let err = ConfigError::missing_key();
            return ApiResponse::error(err.code().into(), err.to_string(), err.status_code());
        }
    };
    let value = match params.value {
        Some(v) => v.into_bytes(),
        None => {
            let err = ConfigError::missing_value();
            return ApiResponse::error(err.code().into(), err.to_string(), err.status_code());
        }
    };
    let req = ProtoUtils::set_config_request_from_http(key, value);
    match instance.config_manager.set_config(req) {
        Ok(_) => ApiResponse::success(None),
        Err(e) => {
            let err = ConfigError::internal_error(e);
            ApiResponse::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeleteConfigParams {
    prev_version: Option<u64>,
}

pub async fn delete_config_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Path(key): Path<String>,
    Query(params): Query<DeleteConfigParams>,
) -> impl IntoResponse {
    let req = DeleteConfigRequest {
        key,
        prev_version: params.prev_version,
    };
    match instance.config_manager.delete_config(req) {
        Ok(_) => ApiResponse::success_with_status_code(StatusCode::NO_CONTENT),
        Err(e) => {
            let err = ConfigError::internal_error(e);
            ApiResponse::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ListConfigsParams {
    prefix: Option<String>,
    limit: Option<u32>,
}

pub async fn list_configs_handler(
    Extension(instance): Extension<Arc<PdHttpHandler>>,
    Query(params): Query<ListConfigsParams>,
) -> impl IntoResponse {
    let req = ListConfigRequest {
        prefix: params.prefix.unwrap_or_default(),
        limit: params.limit,
    };
    match instance.config_manager.list_config(req) {
        Ok(resp) => {
            let items = resp
                .items
                .into_iter()
                .map(ProtoUtils::config_info_from_pb)
                .collect();
            ApiResponse::success(ConfigListResponse::new(items))
        }
        Err(e) => {
            let err = ConfigError::internal_error(e);
            ApiResponse::error(err.code().into(), err.to_string(), err.status_code())
        }
    }
}
