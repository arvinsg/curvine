use axum::http::StatusCode;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum ClusterError {
    #[error("unknown node type: {0}, expected worker or meta")]
    InvalidNodeType(String),

    #[error("node {0} not found")]
    NodeNotFound(u32),

    #[error("pool {0} not found")]
    PoolNotFound(String),

    #[error("{0}")]
    InternalError(String),
}

impl ClusterError {
    pub fn code(&self) -> &'static str {
        match self {
            ClusterError::InvalidNodeType(_) => "INVALID_INPUT",
            ClusterError::NodeNotFound(_) | ClusterError::PoolNotFound(_) => "NOT_FOUND",
            ClusterError::InternalError(_) => "INTERNAL_ERROR",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            ClusterError::InvalidNodeType(_) => StatusCode::BAD_REQUEST,
            ClusterError::NodeNotFound(_) | ClusterError::PoolNotFound(_) => StatusCode::NOT_FOUND,
            ClusterError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn invalid_node_type(node_type: impl Into<String>) -> Self {
        ClusterError::InvalidNodeType(node_type.into())
    }

    pub fn node_not_found(node_id: u32) -> Self {
        ClusterError::NodeNotFound(node_id)
    }

    pub fn pool_not_found(pool: impl Into<String>) -> Self {
        ClusterError::PoolNotFound(pool.into())
    }

    pub fn internal_error(e: impl ToString) -> Self {
        ClusterError::InternalError(e.to_string())
    }
}
