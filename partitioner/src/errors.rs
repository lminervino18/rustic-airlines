use std::fmt::{self, Display};

/// Enum representing the possible errors that can occur within the `Partitioner` struct.
///
/// The possible errors are:
///
/// - `NodeAlreadyExists`: the IP address is already present in the partitioner.
/// - `NodeNotFound`: the IP address could not be found in the partitioner.
/// - `HashError`: an error occurred while hashing a value.
/// - `EmptyPartitioner`: attempted to retrieve an IP but the partitioner has no nodes.
///
/// These errors allow for more detailed handling and logging of unexpected issues.
#[derive(Debug, PartialEq)]
pub enum PartitionerError {
    NodeAlreadyExists,
    NodeNotFound,
    HashError,
    EmptyPartitioner,
}

impl Display for PartitionerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionerError::NodeAlreadyExists => write!(
                f,
                "[NodeAlreadyExists]: The node is already in the partitioner"
            ),
            PartitionerError::NodeNotFound => {
                write!(f, "[NodeNotFound]: The specified node was not found")
            }
            PartitionerError::HashError => write!(
                f,
                "[HashError]: There was an error computing the hash value"
            ),
            PartitionerError::EmptyPartitioner => write!(
                f,
                "[EmptyPartitioner]: The partitioner has no nodes available"
            ),
        }
    }
}
