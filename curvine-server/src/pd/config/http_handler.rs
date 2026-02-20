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

use super::pb_convert::set_config_request_from_http;
use super::store::ConfigScope;
use super::ConfigManager;
use crate::pd::http::{status_code_for_error, ApiResponse};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use curvine_common::proto::{DeleteConfigRequest, GetConfigRequest, ListConfigRequest};
use serde::Deserialize;
use std::sync::Arc;

pub async fn get_config_handler(
    State(manager): State<Arc<ConfigManager>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let req = GetConfigRequest { key };
    match manager.get_config(req) {
        Ok(resp) => match resp.item {
            Some(item) => ApiResponse::success(item),
            None => ApiResponse::error_with_status(
                "Config not found".to_string(),
                StatusCode::NOT_FOUND,
            ),
        },
        Err(e) => ApiResponse::error_with_status(e.to_string(), status_code_for_error(&e)),
    }
}

#[derive(Debug, Deserialize)]
pub struct SetConfigBody {
    value: String,
    scope: Option<ConfigScope>,
}

#[derive(Debug, Deserialize)]
pub struct SetConfigQueryParams {
    pub key: Option<String>,
    pub value: Option<String>,
    pub scope: Option<String>,
}

fn parse_scope_from_str(s: &str) -> ConfigScope {
    match s.to_lowercase().as_str() {
        "node" => ConfigScope::Node(String::new()),
        _ => ConfigScope::Cluster,
    }
}

pub async fn set_config_handler(
    State(manager): State<Arc<ConfigManager>>,
    Path(key): Path<String>,
    Query(params): Query<SetConfigQueryParams>,
    body: Option<Json<SetConfigBody>>,
) -> impl IntoResponse {
    let (value, scope) = match body {
        Some(b) => (b.value.clone().into_bytes(), b.scope.clone()),
        None => {
            let value = match params.value {
                Some(v) => v.into_bytes(),
                None => {
                    return ApiResponse::error_with_status(
                        "Missing value: use JSON body or query param value=...".to_string(),
                        StatusCode::BAD_REQUEST,
                    )
                }
            };
            let scope = params.scope.map(|s| parse_scope_from_str(&s));
            (value, scope)
        }
    };
    let req = set_config_request_from_http(key, value, scope);
    match manager.set_config(req).await {
        Ok(resp) => ApiResponse::success(resp),
        Err(e) => ApiResponse::error_with_status(e.to_string(), status_code_for_error(&e)),
    }
}

pub async fn set_config_by_query_handler(
    State(manager): State<Arc<ConfigManager>>,
    Query(params): Query<SetConfigQueryParams>,
) -> impl IntoResponse {
    let key = match params.key {
        Some(k) => k,
        None => {
            return ApiResponse::error_with_status(
                "Missing key: use query param key=...".to_string(),
                StatusCode::BAD_REQUEST,
            )
        }
    };
    let value = match params.value {
        Some(v) => v.into_bytes(),
        None => {
            return ApiResponse::error_with_status(
                "Missing value: use query param value=...".to_string(),
                StatusCode::BAD_REQUEST,
            )
        }
    };
    let scope = params.scope.map(|s| parse_scope_from_str(&s));
    let req = set_config_request_from_http(key, value, scope);
    match manager.set_config(req).await {
        Ok(resp) => ApiResponse::success(resp),
        Err(e) => ApiResponse::error_with_status(e.to_string(), status_code_for_error(&e)),
    }
}

#[derive(Debug, Deserialize)]
struct DeleteConfigParams {
    prev_version: Option<u64>,
}

pub async fn delete_config_handler(
    State(manager): State<Arc<ConfigManager>>,
    Path(key): Path<String>,
    Query(params): Query<DeleteConfigParams>,
) -> impl IntoResponse {
    let req = DeleteConfigRequest {
        key,
        prev_version: params.prev_version,
    };
    match manager.delete_config(req).await {
        Ok(resp) => ApiResponse::success(resp),
        Err(e) => ApiResponse::error_with_status(e.to_string(), status_code_for_error(&e)),
    }
}

#[derive(Debug, Deserialize)]
struct ListConfigsParams {
    prefix: Option<String>,
    limit: Option<u32>,
}

pub async fn list_configs_handler(
    State(manager): State<Arc<ConfigManager>>,
    Query(params): Query<ListConfigsParams>,
) -> impl IntoResponse {
    let req = ListConfigRequest {
        prefix: params.prefix.unwrap_or_default(),
        limit: params.limit,
    };
    match manager.list_config(req) {
        Ok(resp) => ApiResponse::success(resp),
        Err(e) => ApiResponse::error_with_status(e.to_string(), status_code_for_error(&e)),
    }
}
