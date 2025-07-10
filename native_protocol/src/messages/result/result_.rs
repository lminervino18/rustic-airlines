use std::io::Read;

use crate::{errors::NativeError, Serializable};

use super::{prepared::Prepared, rows::Rows, schema_change::SchemaChange};

pub enum ResultCode {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
}

impl ResultCode {
    pub fn from_bytes(bytes: [u8; 4]) -> std::result::Result<Self, NativeError> {
        let result = match u32::from_be_bytes(bytes) {
            0x0001 => ResultCode::Void,
            0x0002 => ResultCode::Rows,
            0x0003 => ResultCode::SetKeyspace,
            0x0004 => ResultCode::Prepared,
            0x0005 => ResultCode::SchemaChange,
            _ => return Err(NativeError::InvalidCode),
        };

        Ok(result)
    }
}

/// The result to a `use` query.
type SetKeyspace = String;

#[derive(Debug, PartialEq)]
pub enum Result {
    /// For results carrying no information.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |             kind (4 bytes)            |  // 0x0001
    /// +---------+---------+---------+---------+
    /// |             (empty body)              |
    /// +---------+---------+---------+---------+
    Void,
    /// For results to select queries, returning a set of rows.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |             kind (4 bytes)            |  // 0x0002
    /// +---------+---------+---------+---------+__
    /// |            flags (4 bytes)            |  |
    /// +---------+---------+---------+---------+  |
    /// |        columns_count (4 bytes)        |  |
    /// +---------+---------+---------+---------+  |
    /// |        (optional) paging_state        |  |
    /// +---------+---------+---------+---------+  |
    /// |    (optional) global_table_spec       |  | -> Metadata
    /// +---------+---------+---------+---------+  |
    /// |       (optional) col_spec_1           |  |
    /// +---------+---------+---------+---------+  |
    /// |                 ...                   |  |
    /// +---------+---------+---------+---------+  |
    /// |       (optional) col_spec_i           |__|
    /// +---------+---------+---------+---------+
    /// |         rows_count (4 bytes)          |
    /// +---------+---------+---------+---------+
    /// |          row_1 (value bytes)          |
    /// +---------+---------+---------+---------+
    /// |          row_2 (value bytes)          |
    /// +---------+---------+---------+---------+
    /// |                 ...                   |
    /// +---------+---------+---------+---------+
    /// |          row_m (value bytes)          |
    /// +---------+---------+---------+---------+
    Rows(Rows),
    /// The result to a `use` query.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |            kind (4 bytes)             |  // 0x0003
    /// +---------+---------+---------+---------+
    /// |    keyspace name (string + len (2))   |
    /// +---------+---------+---------+---------+
    SetKeyspace(SetKeyspace),
    /// Result to a PREPARE message.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |             kind (4 bytes)            |  // 0x0004
    /// +---------+---------+---------+---------+
    /// |           id (short bytes)            |
    /// +---------+---------+---------+---------+ __
    /// |            flags (4 bytes)            |   |
    /// +---------+---------+---------+---------+   |
    /// |         columns_count (4 bytes)       |   |
    /// +---------+---------+---------+---------+   |
    /// |        (optional) paging_state        |   |
    /// +---------+---------+---------+---------+   | -> Metadata
    /// |    (optional) global_table_spec       |   |
    /// +---------+---------+---------+---------+   |
    /// |      (optional)  col_spec_1           |   |
    /// +---------+---------+---------+---------+   |
    /// |                 ...                   |   |
    /// +---------+---------+---------+---------+   |
    /// |      (optional)  col_spec_i           |_ _|
    /// +---------+---------+---------+---------+
    /// |           result_metadata             | -> Metadata
    /// +---------+---------+---------+---------+
    Prepared(Prepared),
    /// The result to a schema altering query.
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |            kind (4 bytes)             |  // 0x0005
    /// +---------+---------+---------+---------+
    /// |    change_type (string + len (2))     |
    /// +---------+---------+---------+---------+
    /// |       target (string + len (2))       |
    /// +---------+---------+---------+---------+
    /// |       options (string + len (2))      |
    /// +---------+---------+---------+---------+
    SchemaChange(SchemaChange),
}

impl Serializable for Result {
    /// ```md
    /// 0        8        16       24       32
    /// +---------+---------+---------+---------+
    /// |            Kind (4 bytes)             |
    /// +---------+---------+---------+---------+
    /// |             Result Body               |
    /// +                                       +
    /// |                ...                    |
    /// +---------+---------+---------+---------+
    /// ```
    fn to_bytes(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        let code = match self {
            Result::Void => ResultCode::Void,
            Result::Rows(_) => ResultCode::Rows,
            Result::SetKeyspace(_) => ResultCode::SetKeyspace,
            Result::Prepared(_) => ResultCode::Prepared,
            Result::SchemaChange(_) => ResultCode::SchemaChange,
        };

        bytes.extend_from_slice(&(code as u32).to_be_bytes());

        match self {
            Result::Void => {}
            Result::Rows(rows) => {
                bytes.extend_from_slice(&rows.to_bytes()?);
            }
            Result::SetKeyspace(keyspace) => {
                bytes.extend_from_slice(keyspace.as_bytes());
            }
            Result::Prepared(prepared) => {
                bytes.extend_from_slice(&prepared.to_bytes()?);
            }
            Result::SchemaChange(schema_change) => {
                bytes.extend_from_slice(&schema_change.to_bytes()?);
            }
        }

        Ok(bytes)
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Result, NativeError> {
        let mut cursor = std::io::Cursor::new(bytes);
        let mut code_bytes = [0u8; 4];
        cursor
            .read_exact(&mut code_bytes)
            .map_err(|_| NativeError::CursorError)?;

        let code = ResultCode::from_bytes(code_bytes)?;

        match code {
            ResultCode::Void => Ok(Result::Void),
            ResultCode::Rows => {
                let rows = Rows::from_bytes(&bytes[4..])?;
                Ok(Result::Rows(rows))
            }
            ResultCode::SetKeyspace => {
                let mut keyspace = String::new();
                cursor
                    .read_to_string(&mut keyspace)
                    .map_err(|_| NativeError::CursorError)?;
                Ok(Result::SetKeyspace(keyspace))
            }
            ResultCode::Prepared => {
                let prepared = Prepared::from_bytes(&bytes[4..])?;
                Ok(Result::Prepared(prepared))
            }
            ResultCode::SchemaChange => {
                let schema_change = SchemaChange::from_bytes(&bytes[4..])?;
                Ok(Result::SchemaChange(schema_change))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::Serializable;

    use super::Result;
    use super::ResultCode;

    #[test]
    fn test_void_result_to_bytes() {
        let result = Result::Void;

        let bytes = result.to_bytes().unwrap();

        let expected_bytes = [0x00, 0x00, 0x00, 0x01];

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_void_result_from_bytes() {
        let bytes: [u8; 4] = (ResultCode::Void as u32).to_be_bytes();

        let result = Result::from_bytes(&bytes).unwrap();

        assert_eq!(result, Result::Void);
    }

    #[test]
    fn test_set_keyspace_to_bytes() {
        let set_keyspace = Result::SetKeyspace("test_keyspace".to_string());

        let bytes = set_keyspace.to_bytes().unwrap();

        let expected_bytes = [
            0x00, 0x00, 0x00, 0x03, // kind = 0x0003
            0x74, 0x65, 0x73, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65,
        ];

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_set_keyspace_from_bytes() {
        let keyspace = "test_keyspace";

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(ResultCode::SetKeyspace as u32).to_be_bytes());
        bytes.extend_from_slice(keyspace.as_bytes());

        let result = Result::from_bytes(&bytes).unwrap();

        assert_eq!(result, Result::SetKeyspace(keyspace.to_string()));
    }
}
