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

use axum::http::StatusCode;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum MetaError {
    #[error("path must not be empty")]
    PathEmpty,

    #[error("query parameter path is required")]
    PathRequired,

    #[error("route operation failed: {0}")]
    RouteError(String),

    #[error("meta group {0} not found")]
    GroupNotFound(u64),

    #[error("{0}")]
    InternalError(String),
}

impl MetaError {
    pub fn code(&self) -> &'static str {
        match self {
            MetaError::PathEmpty | MetaError::PathRequired => "INVALID_INPUT",
            MetaError::RouteError(_) => "ROUTE_ERROR",
            MetaError::GroupNotFound(_) => "NOT_FOUND",
            MetaError::InternalError(_) => "INTERNAL_ERROR",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            MetaError::PathEmpty | MetaError::PathRequired => StatusCode::BAD_REQUEST,
            MetaError::RouteError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            MetaError::GroupNotFound(_) => StatusCode::NOT_FOUND,
            MetaError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn path_empty() -> Self {
        MetaError::PathEmpty
    }

    pub fn path_required() -> Self {
        MetaError::PathRequired
    }

    pub fn route_error(e: impl ToString) -> Self {
        MetaError::RouteError(e.to_string())
    }

    pub fn group_not_found(group_id: u64) -> Self {
        MetaError::GroupNotFound(group_id)
    }

    pub fn internal_error(e: impl ToString) -> Self {
        MetaError::InternalError(e.to_string())
    }
}
