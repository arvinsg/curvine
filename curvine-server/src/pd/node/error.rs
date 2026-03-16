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
pub enum NodeError {
    #[error("unknown node type: {0}, expected worker or meta")]
    InvalidNodeType(String),

    #[error("node {0} not found")]
    NodeNotFound(u32),

    #[error("{0}")]
    InternalError(String),
}

impl NodeError {
    pub fn code(&self) -> &'static str {
        match self {
            NodeError::InvalidNodeType(_) => "INVALID_INPUT",
            NodeError::NodeNotFound(_) => "NOT_FOUND",
            NodeError::InternalError(_) => "INTERNAL_ERROR",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            NodeError::InvalidNodeType(_) => StatusCode::BAD_REQUEST,
            NodeError::NodeNotFound(_) => StatusCode::NOT_FOUND,
            NodeError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn invalid_node_type(node_type: impl Into<String>) -> Self {
        NodeError::InvalidNodeType(node_type.into())
    }

    pub fn node_not_found(node_id: u32) -> Self {
        NodeError::NodeNotFound(node_id)
    }

    pub fn internal_error(e: impl ToString) -> Self {
        NodeError::InternalError(e.to_string())
    }
}
