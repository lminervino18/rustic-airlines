//! Internode protocol module.
//!
//! This module contains the definitions for the internode protocol messages, queries, and responses.
//!
//! The internode protocol is used to communicate between nodes in the cluster. It is a custom
//! protocol that is used to send queries, responses, and gossip messages between nodes.

use message::InternodeMessageError;

pub mod message;
pub mod query;
pub mod response;

/// The InternodeSerializable trait is used to serialize and deserialize internode protocol messages.\
/// This trait is implemented by all internode protocol messages, queries, and responses.\
pub trait InternodeSerializable {
    /// Serializes the internode protocol message to a byte array.
    fn as_bytes(&self) -> Vec<u8>;

    /// Deserializes the internode protocol message from a byte array.
    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError>
    where
        Self: Sized;
}
