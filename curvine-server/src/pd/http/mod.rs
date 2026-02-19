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

// Common HTTP utilities for PD web handlers.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip)]
    pub status_code: StatusCode,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            status_code: StatusCode::OK,
        }
    }

    pub fn success_with_status(data: T, status_code: StatusCode) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            status_code: status_code,
        }
    }

    pub fn error_with_status(message: String, status_code: StatusCode) -> Self
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

/// Maps FsError to HTTP status code (e.g. version mismatch -> CONFLICT).
pub fn status_code_for_error(err: &curvine_common::error::FsError) -> StatusCode {
    let msg = err.to_string();
    if msg.contains("Version mismatch") {
        StatusCode::CONFLICT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}
