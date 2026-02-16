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

use crate::pd::config_handler::ConfigHandler;
use crate::pd::config_types::*;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{delete, get, put},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct HttpConfigHandler {
    config_handler: Arc<ConfigHandler>,
}

impl HttpConfigHandler {
    pub fn new(config_handler: Arc<ConfigHandler>) -> Self {
        Self { config_handler }
    }

    pub fn routes(&self) -> Router {
        Router::new()
            .route("/api/v1/config/:key", get(get_config_handler))
            .route("/api/v1/config/:key", put(set_config_handler))
            .route("/api/v1/config/:key", delete(delete_config_handler))
            .route("/api/v1/configs", get(list_configs_handler))
            .with_state(self.config_handler.clone())
    }
}

#[derive(Debug, Serialize)]
struct ApiResponse<T> {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip)]
    status_code: StatusCode,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            status_code: StatusCode::OK,
        }
    }

    fn error(message: String) -> Self
    where
        T: Default,
    {
        Self {
            success: false,
            data: None,
            error: Some(message),
            status_code: StatusCode::BAD_REQUEST,
        }
    }

    fn error_with_status(message: String, status_code: StatusCode) -> Self
    where
        T: Default,
    {
        Self {
            success: false,
            data: None,
            error: Some(message),
            status_code,
        }
    }
}

impl<T: Serialize> IntoResponse for ApiResponse<T> {
    fn into_response(self) -> Response {
        (self.status_code, Json(self)).into_response()
    }
}

fn status_code_for_error(err: &curvine_common::error::FsError) -> StatusCode {
    let msg = err.to_string();
    if msg.contains("Version mismatch") {
        StatusCode::CONFLICT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

async fn get_config_handler(
    State(handler): State<Arc<ConfigHandler>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let req = GetConfigRequest { key };

    match handler.get_config(req) {
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
struct SetConfigBody {
    value: String,
    scope: Option<ConfigScope>,
}

async fn set_config_handler(
    State(handler): State<Arc<ConfigHandler>>,
    Path(key): Path<String>,
    Json(body): Json<SetConfigBody>,
) -> impl IntoResponse {
    let req = SetConfigRequest {
        key,
        value: body.value.into_bytes(),
        scope: body.scope,
    };

    match handler.set_config(req).await {
        Ok(resp) => ApiResponse::success(resp),
        Err(e) => ApiResponse::error_with_status(e.to_string(), status_code_for_error(&e)),
    }
}

async fn delete_config_handler(
    State(handler): State<Arc<ConfigHandler>>,
    Path(key): Path<String>,
    Query(params): Query<DeleteConfigParams>,
) -> impl IntoResponse {
    let req = DeleteConfigRequest {
        key,
        prev_version: params.prev_version,
    };

    match handler.delete_config(req).await {
        Ok(resp) => ApiResponse::success(resp),
        Err(e) => ApiResponse::error_with_status(e.to_string(), status_code_for_error(&e)),
    }
}

#[derive(Debug, Deserialize)]
struct DeleteConfigParams {
    prev_version: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ListConfigsParams {
    prefix: Option<String>,
    limit: Option<u32>,
}

async fn list_configs_handler(
    State(handler): State<Arc<ConfigHandler>>,
    Query(params): Query<ListConfigsParams>,
) -> impl IntoResponse {
    let req = ListConfigRequest {
        prefix: params.prefix.unwrap_or_default(),
        limit: params.limit,
    };

    match handler.list_config(req) {
        Ok(resp) => ApiResponse::success(resp),
        Err(e) => ApiResponse::error_with_status(e.to_string(), status_code_for_error(&e)),
    }
}
