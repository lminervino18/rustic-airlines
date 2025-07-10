use super::{query::InternodeQuery, response::InternodeResponse, InternodeSerializable};
use gossip::messages::GossipMessage;
use std::{
    io::{Cursor, Read},
    net::Ipv4Addr,
};

/// The opcode of an internode message.\
/// The opcode is used to determine the type of message being sent.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Opcode {
    Query = 0x01,
    Response = 0x02,
    Gossip = 0x03,
}

/// The header of an internode message.
///
/// ### Fields
///
/// * `opcode` - The opcode of the message.
/// * `ip` - The IP address of the node that sent the message.
#[derive(Debug, PartialEq)]
struct InternodeHeader {
    opcode: Opcode,
    ip: Ipv4Addr,
    length: u32,
}

const HEADER_SIZE: usize = 9;

impl InternodeSerializable for InternodeHeader {
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |         ip        |
    /// +----+----+----+----+
    /// |  content_length   |
    /// +----+----+----+----+
    /// | op |              |
    /// +----+----+----+----+
    /// ```
    /// Serializes the header into a byte vector.
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.ip.octets());
        bytes.extend_from_slice(&self.length.to_be_bytes());
        bytes.push(self.opcode as u8);

        bytes
    }

    /// Deserializes the header from a byte slice.
    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(bytes);

        let mut ip_bytes = [0u8; 4];
        cursor
            .read_exact(&mut ip_bytes)
            .map_err(|_| InternodeMessageError)?;

        let ip = Ipv4Addr::from(ip_bytes);

        let mut len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut len_bytes)
            .map_err(|_| InternodeMessageError)?;

        let length = u32::from_be_bytes(len_bytes);

        let mut opcode_byte = [0u8; 1];
        cursor
            .read_exact(&mut opcode_byte)
            .map_err(|_| InternodeMessageError)?;

        let opcode = match opcode_byte[0] {
            0x01 => Opcode::Query,
            0x02 => Opcode::Response,
            0x03 => Opcode::Gossip,
            _ => return Err(InternodeMessageError),
        };

        Ok(InternodeHeader { opcode, ip, length })
    }
}

/// The content of an internode message.\
///
/// ### Variants
///
/// * `Query` - A query message.
/// * `Response` - A response message.
/// * `Gossip` - A gossip message.
#[derive(Debug, PartialEq, Clone)]
pub enum InternodeMessageContent {
    Query(InternodeQuery),
    Response(InternodeResponse),
    Gossip(GossipMessage),
}

/// A message transmitted between nodes via the internode protocol.
///
/// ## Fields
///
/// * `from` - The IP address of the node that sent the message.
/// * `content` - The content of the message.
///
#[derive(Debug, PartialEq, Clone)]
pub struct InternodeMessage {
    /// The IP address of the node that sent the message.
    pub from: Ipv4Addr,
    /// The content of the message.
    pub content: InternodeMessageContent,
}

impl InternodeMessage {
    /// Creates a new internode message.
    pub fn new(from: Ipv4Addr, content: InternodeMessageContent) -> Self {
        Self { from, content }
    }
}

/// An error that occurs when serializing or deserializing an internode message.
#[derive(Debug)]
pub struct InternodeMessageError;

impl InternodeSerializable for InternodeMessage {
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |       header      |
    /// +----+----+----+----+
    /// |head|  content...
    /// +----+----+----+----+
    /// ```
    /// Serializes the message into a byte vector.
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let opcode = match self.content {
            InternodeMessageContent::Query(_) => Opcode::Query,
            InternodeMessageContent::Response(_) => Opcode::Response,
            InternodeMessageContent::Gossip(_) => Opcode::Gossip,
        };

        let content_bytes = match &self.content {
            InternodeMessageContent::Query(internode_query) => internode_query.as_bytes(),
            InternodeMessageContent::Response(internode_response) => internode_response.as_bytes(),
            InternodeMessageContent::Gossip(gossip_message) => gossip_message.as_bytes(),
        };

        let header = InternodeHeader {
            ip: self.from,
            opcode,
            length: content_bytes.len() as u32,
        };

        bytes.extend_from_slice(&header.as_bytes());
        bytes.extend_from_slice(&content_bytes);

        bytes
    }

    /// Deserializes the message from a byte slice.
    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError> {
        let mut cursor = Cursor::new(bytes);

        let mut header_bytes = [0u8; HEADER_SIZE];
        cursor
            .read_exact(&mut header_bytes)
            .map_err(|_| InternodeMessageError)?;

        let header =
            InternodeHeader::from_bytes(&header_bytes).map_err(|_| InternodeMessageError)?;
        let mut content_bytes = vec![0u8; header.length as usize];
        cursor
            .read_exact(&mut content_bytes)
            .map_err(|_| InternodeMessageError)?;

        let content = match header.opcode {
            Opcode::Query => InternodeMessageContent::Query(
                InternodeQuery::from_bytes(&content_bytes).map_err(|_| InternodeMessageError)?,
            ),
            Opcode::Response => InternodeMessageContent::Response({
                InternodeResponse::from_bytes(&content_bytes).map_err(|_| InternodeMessageError)?
            }),
            Opcode::Gossip => InternodeMessageContent::Gossip(
                GossipMessage::from_bytes(&content_bytes).map_err(|_| InternodeMessageError)?,
            ),
        };
        let message = InternodeMessage {
            from: header.ip,
            content,
        };

        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use crate::internode_protocol::response::{InternodeResponseContent, InternodeResponseStatus};

    use super::*;

    #[test]
    fn test_message_from_bytes_error() {
        let message_bytes = vec![0, 0, 0, 0, 0];

        let parsed_message = InternodeMessage::from_bytes(&message_bytes);

        assert!(parsed_message.is_err());
    }

    #[test]
    fn test_header_to_bytes() {
        let header = InternodeHeader {
            opcode: Opcode::Query,
            ip: Ipv4Addr::new(127, 0, 0, 1),
            length: 0,
        };

        let header_bytes = header.as_bytes();

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&header.ip.octets());
        bytes.extend_from_slice(&header.length.to_be_bytes());
        bytes.push(header.opcode as u8);

        assert_eq!(header_bytes, bytes);
    }

    #[test]
    fn test_header_from_bytes() {
        let header = InternodeHeader {
            opcode: Opcode::Query,
            ip: Ipv4Addr::new(127, 0, 0, 1),
            length: 0,
        };

        let header_bytes = header.as_bytes();

        let parsed_header = InternodeHeader::from_bytes(&header_bytes).unwrap();

        assert_eq!(parsed_header, header);
    }

    #[test]
    fn test_header_from_bytes_error() {
        let header_bytes = vec![0, 0, 0, 0, 0];

        let parsed_header = InternodeHeader::from_bytes(&header_bytes);

        assert!(parsed_header.is_err());
    }

    #[test]
    fn test_message_to_bytes_query() {
        let query = InternodeQuery {
            query_string: "SELECT * FROM something".to_string(),
            open_query_id: 1,
            client_id: 1,
            replication: false,
            keyspace_name: "keyspace".to_string(),
            timestamp: 1,
        };

        let query_bytes = query.as_bytes();

        let message = InternodeMessage {
            from: Ipv4Addr::new(127, 0, 0, 1),
            content: InternodeMessageContent::Query(query),
        };

        let message_bytes = message.as_bytes();

        let mut bytes = Vec::new();

        let header = InternodeHeader {
            opcode: Opcode::Query,
            ip: Ipv4Addr::new(127, 0, 0, 1),
            length: query_bytes.len() as u32,
        };

        bytes.extend_from_slice(&header.as_bytes());
        bytes.extend_from_slice(&query_bytes);

        assert_eq!(message_bytes, bytes);
    }

    #[test]
    fn test_message_from_bytes_query() {
        let query = InternodeQuery {
            query_string: "SELECT * FROM something".to_string(),
            open_query_id: 1,
            client_id: 1,
            replication: false,
            keyspace_name: "keyspace".to_string(),
            timestamp: 1,
        };

        let message = InternodeMessage {
            from: Ipv4Addr::new(127, 0, 0, 1),
            content: InternodeMessageContent::Query(query),
        };

        let message_bytes = message.as_bytes();

        let parsed_message = InternodeMessage::from_bytes(&message_bytes).unwrap();

        assert_eq!(parsed_message, message);
    }

    #[test]
    fn test_message_to_bytes_response() {
        let response = InternodeResponse {
            open_query_id: 1,
            status: InternodeResponseStatus::Ok,
            content: Some(InternodeResponseContent {
                columns: vec!["column1".to_string(), "column2".to_string()],
                select_columns: vec!["column1".to_string(), "column2".to_string()],
                values: vec![vec!["value1".to_string(), "value2".to_string()]],
            }),
        };

        let response_bytes = response.as_bytes();

        let message = InternodeMessage {
            from: Ipv4Addr::new(127, 0, 0, 1),
            content: InternodeMessageContent::Response(response),
        };

        let message_bytes = message.as_bytes();

        let mut bytes = Vec::new();

        let header = InternodeHeader {
            opcode: Opcode::Response,
            ip: Ipv4Addr::new(127, 0, 0, 1),
            length: response_bytes.len() as u32,
        };

        bytes.extend_from_slice(&header.as_bytes());
        bytes.extend_from_slice(&response_bytes);

        assert_eq!(message_bytes, bytes);
    }

    #[test]
    fn test_message_from_bytes_response() {
        let response = InternodeResponse {
            open_query_id: 1,
            status: InternodeResponseStatus::Ok,
            content: Some(InternodeResponseContent {
                columns: vec!["column1".to_string(), "column2".to_string()],
                select_columns: vec!["column1".to_string(), "column2".to_string()],
                values: vec![vec!["value1".to_string(), "value2".to_string()]],
            }),
        };

        let message = InternodeMessage {
            from: Ipv4Addr::new(127, 0, 0, 1),
            content: InternodeMessageContent::Response(response),
        };

        let message_bytes = message.as_bytes();

        let parsed_message = InternodeMessage::from_bytes(&message_bytes).unwrap();

        assert_eq!(parsed_message, message);
    }
}
