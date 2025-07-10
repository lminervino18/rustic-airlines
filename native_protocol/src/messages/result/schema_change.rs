use std::io::Read;

use crate::{errors::NativeError, Serializable};

// Represents the type of change in a schema altering query
#[derive(Debug, PartialEq)]
pub enum ChangeType {
    Created,
    Updated,
    Dropped,
}

// Represents the target of a schema altering query
#[derive(Debug, PartialEq)]
pub enum Target {
    Keyspace,
    Table,
    Type,
}

// If target is Keyspace, name is None and keyspace is the name of the keyspace changed
// If target is Table or Type, name is the name of the table or type changed and keyspace is the name of the keyspace
#[derive(Debug, PartialEq)]
pub struct Options {
    keyspace: String,
    name: Option<String>,
}

impl Options {
    pub fn new(keyspace: String, table: Option<String>) -> Self {
        Options {
            keyspace,
            name: table,
        }
    }
}
#[derive(Debug, PartialEq)]
///  The result to a schema altering query
/// (creation/update/drop of a keyspace/table/index).
pub struct SchemaChange {
    change_type: ChangeType,
    target: Target,
    options: Options,
}

impl SchemaChange {
    pub fn new(change_type: ChangeType, target: Target, options: Options) -> Self {
        SchemaChange {
            change_type,
            target,
            options,
        }
    }
}
impl Serializable for SchemaChange {
    /// Serializes the schema change to bytes.
    fn to_bytes(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        let change_type = match self.change_type {
            ChangeType::Created => "CREATED",
            ChangeType::Updated => "UPDATED",
            ChangeType::Dropped => "DROPPED",
        };

        bytes.extend_from_slice(&(change_type.len() as u16).to_be_bytes());
        bytes.extend_from_slice(change_type.as_bytes());

        let target = match self.target {
            Target::Keyspace => "KEYSPACE",
            Target::Table => "TABLE",
            Target::Type => "TYPE",
        };

        bytes.extend_from_slice(&(target.len() as u16).to_be_bytes());
        bytes.extend_from_slice(target.as_bytes());

        bytes.extend_from_slice(&(self.options.keyspace.len() as u16).to_be_bytes());
        bytes.extend_from_slice(self.options.keyspace.as_bytes());

        if let Some(name) = &self.options.name {
            bytes.extend_from_slice(&(name.len() as u16).to_be_bytes());
            bytes.extend_from_slice(name.as_bytes());
        } else {
            bytes.extend_from_slice([0u8; 2].as_ref());
        }

        Ok(bytes)
    }

    /// Deserializes the schema change from bytes, returning a SchemaChange.
    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, NativeError> {
        let mut cursor = std::io::Cursor::new(bytes);

        // Read change type
        let mut change_type_len_bytes = [0u8; 2];
        cursor
            .read_exact(&mut change_type_len_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let change_type_len = u16::from_be_bytes(change_type_len_bytes) as usize;

        let mut change_type_bytes = vec![0u8; change_type_len];
        cursor
            .read_exact(&mut change_type_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let change_type =
            String::from_utf8(change_type_bytes).map_err(|_| NativeError::DeserializationError)?;

        // Read target
        let mut target_len_bytes = [0u8; 2];
        cursor
            .read_exact(&mut target_len_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let target_len = u16::from_be_bytes(target_len_bytes) as usize;

        let mut target_bytes = vec![0u8; target_len];
        cursor
            .read_exact(&mut target_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let target =
            String::from_utf8(target_bytes).map_err(|_| NativeError::DeserializationError)?;

        // Read keyspace
        let mut keyspace_len_bytes = [0u8; 2];
        cursor
            .read_exact(&mut keyspace_len_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let keyspace_len = u16::from_be_bytes(keyspace_len_bytes) as usize;

        let mut keyspace_bytes = vec![0u8; keyspace_len];
        cursor
            .read_exact(&mut keyspace_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let keyspace =
            String::from_utf8(keyspace_bytes).map_err(|_| NativeError::DeserializationError)?;

        // Read name of the table or type if present
        let name = {
            let mut name_bytes_len = [0u8; 2];
            cursor
                .read_exact(&mut name_bytes_len)
                .map_err(|_| NativeError::CursorError)?;
            let name_len = u16::from_be_bytes(name_bytes_len) as usize;

            if name_len > 0 {
                let mut name_bytes = vec![0u8; name_len];
                cursor
                    .read_exact(&mut name_bytes)
                    .map_err(|_| NativeError::CursorError)?;
                Some(String::from_utf8(name_bytes).map_err(|_| NativeError::DeserializationError)?)
            } else {
                None
            }
        };

        let change_type = match change_type.as_str() {
            "CREATED" => ChangeType::Created,
            "UPDATED" => ChangeType::Updated,
            "DROPPED" => ChangeType::Dropped,
            _ => return Err(NativeError::InvalidVariant),
        };

        let target = match target.as_str() {
            "KEYSPACE" => Target::Keyspace,
            "TABLE" => Target::Table,
            "TYPE" => Target::Type,
            _ => return Err(NativeError::InvalidVariant),
        };

        Ok(SchemaChange {
            change_type,
            target,
            options: Options { keyspace, name },
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::messages::result::result_::{Result, ResultCode};
    use crate::messages::result::schema_change::{ChangeType, Options, Target};

    use super::*;

    #[test]
    fn test_schema_change_to_bytes() {
        let schema_change = Result::SchemaChange(SchemaChange {
            change_type: ChangeType::Created,
            target: Target::Table,
            options: Options {
                keyspace: "my_keyspace".to_string(),
                name: Some("my_table".to_string()),
            },
        });

        let bytes = schema_change.to_bytes().unwrap();

        let mut expected_bytes = Vec::new();
        expected_bytes.extend_from_slice(&(ResultCode::SchemaChange as u32).to_be_bytes());
        expected_bytes.extend_from_slice(&("CREATED".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("CREATED".as_bytes());
        expected_bytes.extend_from_slice(&("TABLE".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("TABLE".as_bytes());
        expected_bytes.extend_from_slice(&("my_keyspace".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("my_keyspace".as_bytes());
        expected_bytes.extend_from_slice(&("my_table".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("my_table".as_bytes());

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_schema_change_to_bytes_with_none() {
        let expected_result = Result::SchemaChange(SchemaChange {
            change_type: ChangeType::Created,
            target: Target::Keyspace,
            options: Options {
                keyspace: "my_keyspace".to_string(),
                name: None,
            },
        });

        let bytes = Result::to_bytes(&expected_result).unwrap();

        let mut expected_bytes = Vec::new();
        expected_bytes.extend_from_slice(&(ResultCode::SchemaChange as u32).to_be_bytes());
        expected_bytes.extend_from_slice(&("CREATED".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("CREATED".as_bytes());
        expected_bytes.extend_from_slice(&("KEYSPACE".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("KEYSPACE".as_bytes());
        expected_bytes.extend_from_slice(&("my_keyspace".len() as u16).to_be_bytes());
        expected_bytes.extend_from_slice("my_keyspace".as_bytes());
        expected_bytes.extend_from_slice([0u8; 2].as_ref());

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_schema_change_from_bytes_with_none() {
        let expected_result = Result::SchemaChange(SchemaChange {
            change_type: ChangeType::Created,
            target: Target::Keyspace,
            options: Options {
                keyspace: "my_keyspace".to_string(),
                name: None,
            },
        });

        let bytes = Result::to_bytes(&expected_result).unwrap();

        let result = Result::from_bytes(&bytes).unwrap();

        assert_eq!(result, expected_result);
    }
}
