// node_errors.rs

// Ordered imports
use std::fmt::{self, Display};
use std::io;

use super::storage_engine::errors::StorageEngineError;
use gossip::structures::application_state::SchemaError;
use logger::LoggerError;
use native_protocol::errors::NativeError;
use partitioner::errors::PartitionerError;
use query_creator::errors::CQLError; // Importar LoggerError

/// Enum representing the possible errors that can occur within the `Node` and during query execution (`QueryExecution`).
#[derive(Debug)]
pub enum NodeError {
    /// Error related to the `Partitioner`.
    PartitionerError(PartitionerError),
    /// Error related to the query coordinator (`QueryCoordinator`).
    CQLError(CQLError),
    /// Input/output (I/O) error.
    IoError(io::Error),
    /// Error related to lock acquisition.
    LockError,
    /// Error related to keyspace operations.
    KeyspaceError,
    /// Generic error.
    OtherError,
    /// Error related to thread creation or handling.
    ThreadError,
    /// Error related to node-to-node connections.
    InternodeError,
    /// Error related to client interactions.
    ClientError,
    /// Error related to handling open queries.
    OpenQueryError,
    /// Error related to the inter-node communication protocol.
    InternodeProtocolError,
    /// Error related to native protocol operations.
    NativeError(NativeError),
    /// Error related to the storage engine.
    StorageEngineError(StorageEngineError),
    /// Error related to the logger.
    LoggerError(LoggerError),
    /// Error related to the gossip protocol.
    GossipError,
    /// Error related to schema updating.
    SchemaError(SchemaError),
}

impl Display for NodeError {
    /// Implementation of the `fmt` method to convert the error into a readable string.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeError::PartitionerError(e) => write!(f, "Partitioner Error: {}", e),
            NodeError::CQLError(e) => write!(f, "Query Coordinator Error: {}", e),
            NodeError::IoError(e) => write!(f, "I/O Error: {}", e),
            NodeError::LockError => write!(f, "Failed to acquire lock"),
            NodeError::KeyspaceError => write!(f, "Keyspace error"),
            NodeError::OtherError => write!(f, "Other error"),
            NodeError::ThreadError => write!(f, "Thread Error"),
            NodeError::InternodeError => write!(f, "Internode Error"),
            NodeError::ClientError => write!(f, "Client Error"),
            NodeError::OpenQueryError => write!(f, "Open Query Error"),
            NodeError::InternodeProtocolError => write!(f, "Internode Protocol Error"),
            NodeError::NativeError(e) => write!(f, "Native Protocol Error: {}", e),
            NodeError::StorageEngineError(e) => write!(f, "Storage Engine Error: {}", e),
            NodeError::LoggerError(e) => write!(f, "Logger Error: {}", e),
            NodeError::GossipError => write!(f, "Gossip Error"),
            NodeError::SchemaError(e) => write!(f, "Schema Error: {}", e),
        }
    }
}

impl From<PartitionerError> for NodeError {
    /// Conversion from `PartitionerError` to `NodeError`.
    fn from(error: PartitionerError) -> Self {
        NodeError::PartitionerError(error)
    }
}

impl From<CQLError> for NodeError {
    /// Conversion from `CQLError` to `NodeError`.
    fn from(error: CQLError) -> Self {
        NodeError::CQLError(error)
    }
}

impl From<io::Error> for NodeError {
    /// Conversion from `io::Error` to `NodeError`.
    fn from(error: io::Error) -> Self {
        NodeError::IoError(error)
    }
}

impl<T> From<std::sync::PoisonError<T>> for NodeError {
    /// Conversion from a lock error (`PoisonError`) to `NodeError`.
    fn from(_: std::sync::PoisonError<T>) -> Self {
        NodeError::LockError
    }
}

impl From<NativeError> for NodeError {
    /// Conversion from `NativeError` to `NodeError`.
    fn from(error: NativeError) -> Self {
        NodeError::NativeError(error)
    }
}

impl From<StorageEngineError> for NodeError {
    /// Conversion from `StorageEngineError` to `NodeError`.
    fn from(error: StorageEngineError) -> Self {
        NodeError::StorageEngineError(error)
    }
}

impl From<LoggerError> for NodeError {
    /// Conversion from `LoggerError` to `NodeError`.
    fn from(error: LoggerError) -> Self {
        NodeError::LoggerError(error)
    }
}

impl From<SchemaError> for NodeError {
    fn from(_value: SchemaError) -> Self {
        NodeError::OtherError
    }
}
