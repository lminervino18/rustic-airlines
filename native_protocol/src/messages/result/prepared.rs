use std::io::Read;

use crate::{errors::NativeError, Serializable};

use super::metadata::Metadata;

#[derive(Debug, PartialEq)]
/// The result to a PREPARE message.
pub struct Prepared {
    id: Vec<u8>,
    metadata: Metadata,
    result_metadata: Metadata,
}

impl Serializable for Prepared {
    fn to_bytes(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&(self.id.len() as u16).to_be_bytes());
        bytes.extend_from_slice(&self.id);

        bytes.extend_from_slice(&self.metadata.to_bytes()?);

        bytes.extend_from_slice(&self.result_metadata.to_bytes()?);

        Ok(bytes)
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, NativeError> {
        let mut cursor = std::io::Cursor::new(bytes);

        let mut id_len_bytes = [0u8; 2];
        cursor
            .read_exact(&mut id_len_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let id_len = u16::from_be_bytes(id_len_bytes) as usize;

        let mut id_bytes = vec![0u8; id_len];
        cursor
            .read_exact(&mut id_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let id = id_bytes;

        let metadata = Metadata::from_bytes(&mut cursor)?;

        let result_metadata = Metadata::from_bytes(&mut cursor)?;

        Ok(Prepared {
            id,
            metadata,
            result_metadata,
        })
    }
}

#[cfg(test)]

mod tests {
    use crate::{
        messages::result::{
            metadata::{ColumnSpec, Metadata, MetadataFlags, TableSpec},
            prepared::Prepared,
            rows::ColumnType,
        },
        Serializable,
    };

    fn mock_metadata() -> Metadata {
        Metadata {
            flags: MetadataFlags {
                global_table_spec: true,
                has_more_pages: false,
                no_metadata: false,
            },
            columns_count: 1,
            global_table_spec: Some(TableSpec {
                keyspace: "test_keyspace".to_string(),
                table_name: "test_table".to_string(),
            }),
            col_spec_i: vec![ColumnSpec {
                keyspace: Some("test_keyspace".to_string()),
                table_name: Some("test_table".to_string()),
                name: "test_column".to_string(),
                type_: ColumnType::Int,
            }],
        }
    }

    #[test]
    fn test_prepared_to_bytes() {
        let prepared = Prepared {
            id: vec![0x01, 0x02, 0x03],
            metadata: mock_metadata(),
            result_metadata: mock_metadata(),
        };

        let bytes = prepared.to_bytes().unwrap();

        let mut expected_bytes = Vec::new();

        expected_bytes.extend_from_slice(&(prepared.id.len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice(&prepared.id);
        expected_bytes.extend_from_slice(&prepared.metadata.to_bytes().unwrap());
        expected_bytes.extend_from_slice(&prepared.result_metadata.to_bytes().unwrap());

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_prepared_from_bytes() {
        let expected_prepared = Prepared {
            id: vec![0x01, 0x02, 0x03],
            metadata: mock_metadata(),
            result_metadata: mock_metadata(),
        };

        let bytes = expected_prepared.to_bytes().unwrap();

        let prepared = Prepared::from_bytes(&bytes).unwrap();

        assert_eq!(expected_prepared, prepared);
    }
}
