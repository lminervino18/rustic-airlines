//! Query message sent by a coordinator node to other nodes.
//!
//! This module contains the definition of the `InternodeQuery` struct, which represents a query
//! message sent by a coordinator node to other nodes in the cluster. The query message contains
//! information about the query to be executed, such as the query string, the client ID, and the
//! keyspace name.

use std::io::{Cursor, Read};

use super::{message::InternodeMessageError, InternodeSerializable};
use query_creator::{NeedsKeyspace, NeedsTable, QueryCreator};
/// A query sent by a coordinator node to other nodes in the cluster.
///
/// ### Fields
/// - `query_string`: The CQL query string.
/// - `open_query_id`: The `id` of the query to be identified by the open queries handler.
/// - `client_id`: The client that owns the query in this node.
/// - `replication`: This query should be executed over the replications stored by the node.
/// - `keyspace_name`: Keyspace on which the query acts.
/// - `timestamp`: The timestamp when the coordinator node received the query.
#[derive(Debug, PartialEq, Clone)]
pub struct InternodeQuery {
    /// The CQL query string.
    pub query_string: String,
    /// The `id` of the query to be identified by the open queries handler.
    pub open_query_id: u32,
    /// The client that owns the query in this node.
    pub client_id: u32,
    /// This query should be executed over the replications stored by the node,
    /// not over its owned data.
    pub replication: bool,
    /// Keyspace on which the query acts.
    pub keyspace_name: String,
    /// The timestamp when the coordinator node received the query.
    pub timestamp: i64,
}

impl NeedsKeyspace for InternodeQuery {
    fn needs_keyspace(&self) -> bool {
        // Crear una instancia de QueryCreator con el query_string de InternodeQuery
        let query_creator = QueryCreator::new();

        // Manejar el resultado de handle_query (Result<Query, Error>)
        match query_creator.handle_query(self.query_string.clone()) {
            Ok(query) => query.needs_keyspace(), // Llamar al trait NeedsKeyspace implementado para Query
            Err(_) => {
                // En caso de error, se puede asumir que no se necesita keyspace o manejarlo de otro modo
                false
            }
        }
    }
}

impl NeedsTable for InternodeQuery {
    fn needs_table(&self) -> bool {
        // Crear una instancia de QueryCreator con el query_string de InternodeQuery
        let query_creator = QueryCreator::new();

        // Manejar el resultado de handle_query (Result<Query, Error>)
        match query_creator.handle_query(self.query_string.clone()) {
            Ok(query) => query.needs_table(), // Llamar al trait NeedsKeyspace implementado para Query
            Err(_) => {
                // En caso de error, se puede asumir que no se necesita keyspace o manejarlo de otro modo
                false
            }
        }
    }
}

impl InternodeSerializable for InternodeQuery {
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |   open_query_id   |
    /// +----+----+----+----+
    /// |     client_id     |
    /// +----+----+----+----+
    /// |     timestamp     |
    /// +----+----+----+----+
    /// |     timestamp     |
    /// +----+----+----+----+
    /// |rep |     keyspace_
    /// +----+----+----+----+
    /// |len |keyspace_name |
    /// |        ...        |
    /// |   keyspace_name   |
    /// +----+----+----+----+
    /// |    query_length   |
    /// +----+----+----+----+
    /// |    query_string   |
    /// |        ...        |
    /// |    query_string   |
    /// +----+----+----+----+
    /// ```
    /// Serializes the `InternodeQuery` struct into a byte vector.
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend(&self.open_query_id.to_be_bytes());
        bytes.extend(&self.client_id.to_be_bytes());
        bytes.extend(&self.timestamp.to_be_bytes());

        bytes.push(self.replication as u8);

        let keyspace_name_len = self.keyspace_name.len() as u32;
        bytes.extend(&keyspace_name_len.to_be_bytes());
        bytes.extend(self.keyspace_name.as_bytes());

        let query_string_len = self.query_string.len() as u32;
        bytes.extend(&query_string_len.to_be_bytes());
        bytes.extend(self.query_string.as_bytes());

        bytes
    }

    /// Deserializes a byte vector into an `InternodeQuery` struct.
    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(bytes);

        let mut open_query_id_bytes = [0u8; 4];
        cursor
            .read_exact(&mut open_query_id_bytes)
            .map_err(|_| InternodeMessageError)?;
        let open_query_id = u32::from_be_bytes(open_query_id_bytes);

        let mut client_id_bytes = [0u8; 4];
        cursor
            .read_exact(&mut client_id_bytes)
            .map_err(|_| InternodeMessageError)?;
        let client_id = u32::from_be_bytes(client_id_bytes);

        let mut timestamp_bytes = [0u8; 8];
        cursor
            .read_exact(&mut timestamp_bytes)
            .map_err(|_| InternodeMessageError)?;
        let timestamp = i64::from_be_bytes(timestamp_bytes);

        let mut replication_byte = [0u8; 1];
        cursor
            .read_exact(&mut replication_byte)
            .map_err(|_| InternodeMessageError)?;
        let replication = replication_byte[0] != 0;

        let mut keyspace_name_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut keyspace_name_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let keyspace_name_len = u32::from_be_bytes(keyspace_name_len_bytes) as usize;

        let mut keyspace_name_bytes = vec![0u8; keyspace_name_len];
        cursor
            .read_exact(&mut keyspace_name_bytes)
            .map_err(|_| InternodeMessageError)?;
        let keyspace_name =
            String::from_utf8(keyspace_name_bytes).map_err(|_| InternodeMessageError)?;

        let mut query_string_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut query_string_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let query_string_len = u32::from_be_bytes(query_string_len_bytes) as usize;

        let mut query_string_bytes = vec![0u8; query_string_len];
        cursor
            .read_exact(&mut query_string_bytes)
            .map_err(|_| InternodeMessageError)?;
        let query_string =
            String::from_utf8(query_string_bytes).map_err(|_| InternodeMessageError)?;

        Ok(InternodeQuery {
            query_string,
            open_query_id,
            client_id,
            replication,
            keyspace_name,
            timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_to_bytes() {
        let query = InternodeQuery {
            query_string: "SELECT * FROM something".to_string(),
            open_query_id: 1,
            client_id: 1,
            replication: false,
            keyspace_name: "keyspace".to_string(),
            timestamp: 1,
        };

        let query_bytes = query.as_bytes();

        let mut bytes = Vec::new();

        bytes.extend(query.open_query_id.to_be_bytes());
        bytes.extend(query.client_id.to_be_bytes());
        bytes.extend(query.timestamp.to_be_bytes());

        bytes.push(query.replication as u8);

        let keyspace_name_len = query.keyspace_name.len() as u32;
        bytes.extend(&keyspace_name_len.to_be_bytes());
        bytes.extend(query.keyspace_name.as_bytes());

        let query_string_len = query.query_string.len() as u32;
        bytes.extend(&query_string_len.to_be_bytes());
        bytes.extend(query.query_string.as_bytes());

        assert_eq!(query_bytes, bytes);
    }

    #[test]
    fn test_query_from_bytes() {
        let query = InternodeQuery {
            query_string: "SELECT * FROM something".to_string(),
            open_query_id: 1,
            client_id: 1,
            replication: false,
            keyspace_name: "keyspace".to_string(),
            timestamp: 1,
        };

        let query_bytes = query.as_bytes();

        let parsed_query = InternodeQuery::from_bytes(&query_bytes).unwrap();

        assert_eq!(parsed_query, query);
    }
}
