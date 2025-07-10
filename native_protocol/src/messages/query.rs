use std::io::Read;

use crate::{errors::NativeError, Serializable};

enum ConsistencyCode {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Consistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    Serial,
    LocalSerial,
    LocalOne,
}

impl Consistency {
    pub fn from_string(s: &str) -> Result<Self, NativeError> {
        let consistency = match s.to_lowercase().as_str() {
            "any" => Consistency::Any,
            "one" => Consistency::One,
            "two" => Consistency::Two,
            "three" => Consistency::Three,
            "quorum" => Consistency::Quorum,
            "all" => Consistency::All,
            "local_quorum" => Consistency::LocalQuorum,
            "each_quorum" => Consistency::EachQuorum,
            "serial" => Consistency::Serial,
            "local_serial" => Consistency::LocalSerial,
            "local_one" => Consistency::LocalOne,
            _ => return Err(NativeError::InvalidCode),
        };

        Ok(consistency)
    }

    pub fn to_string(&self) -> &'static str {
        match self {
            Consistency::Any => "ANY",
            Consistency::One => "ONE",
            Consistency::Two => "TWO",
            Consistency::Three => "THREE",
            Consistency::Quorum => "QUORUM",
            Consistency::All => "ALL",
            Consistency::LocalQuorum => "LOCAL_QUORUM",
            Consistency::EachQuorum => "EACH_QUORUM",
            Consistency::Serial => "SERIAL",
            Consistency::LocalSerial => "LOCAL_SERIAL",
            Consistency::LocalOne => "LOCAL_ONE",
        }
    }

    fn to_code(&self) -> Result<ConsistencyCode, NativeError> {
        let consistency_code = match self {
            Consistency::Any => ConsistencyCode::Any,
            Consistency::One => ConsistencyCode::One,
            Consistency::Two => ConsistencyCode::Two,
            Consistency::Three => ConsistencyCode::Three,
            Consistency::Quorum => ConsistencyCode::Quorum,
            Consistency::All => ConsistencyCode::All,
            Consistency::LocalQuorum => ConsistencyCode::LocalQuorum,
            Consistency::EachQuorum => ConsistencyCode::EachQuorum,
            Consistency::Serial => ConsistencyCode::Serial,
            Consistency::LocalSerial => ConsistencyCode::LocalSerial,
            Consistency::LocalOne => ConsistencyCode::LocalOne,
        };

        Ok(consistency_code)
    }

    fn from_code(consistency_code: u16) -> Result<Self, NativeError> {
        let consistency = match consistency_code {
            0x0000 => Consistency::Any,
            0x0001 => Consistency::One,
            0x0002 => Consistency::Two,
            0x0003 => Consistency::Three,
            0x0004 => Consistency::Quorum,
            0x0005 => Consistency::All,
            0x0006 => Consistency::LocalQuorum,
            0x0007 => Consistency::EachQuorum,
            0x0008 => Consistency::Serial,
            0x0009 => Consistency::LocalSerial,
            0x000A => Consistency::LocalOne,
            _ => return Err(NativeError::InvalidCode),
        };

        Ok(consistency)
    }
}

enum FlagCode {
    Values = 0x01,
    SkipMetadata = 0x02,
    PageSize = 0x04,
    WithPagingState = 0x08,
    WithSerialConsistency = 0x10,
    WithDefaultTimestamp = 0x20,
    WithNamesForValues = 0x40,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Flag {
    /// If set, a [short] <n> followed by <n> [value]
    /// values are provided. Those values are used for bound variables in
    /// the query.
    Values,
    /// If set, the Result Set returned as a response
    /// to the query (if any) will have the NO_METADATA flag.
    SkipMetadata,
    /// If set, <result_page_size> is an [int]
    /// controlling the desired page size of the result (in CQL3 rows).
    PageSize,
    /// If set, <paging_state> should be present.
    /// <paging_state> is a [bytes] value that should have been returned
    /// in a result set.
    WithPagingState,
    /// If set, <serial_consistency> should be
    /// present. <serial_consistency> is the [consistency] level for the
    /// serial phase of conditional updates.
    WithSerialConsistency,
    /// If set, <timestamp> should be present.
    /// <timestamp> is a [long] representing the default timestamp for the query
    /// in microseconds (negative values are forbidden). This will
    /// replace the server side assigned timestamp as default timestamp.
    WithDefaultTimestamp,
    /// This only makes sense if the 0x01 flag is set and
    /// is ignored otherwise. If present, the values from the 0x01 flag will
    /// be preceded by a name.
    WithNamesForValues,
}

#[derive(PartialEq, Debug, Clone)]
pub struct QueryParams {
    /// Is the consistency level for the operation.
    consistency: Consistency,
    /// Is a byte whose bits define the options for this query.
    flags: Vec<Flag>, // TODO: should be struct with possible values
}

impl QueryParams {
    pub fn new(consistency: Consistency, flags: Vec<Flag>) -> Self {
        QueryParams { consistency, flags }
    }

    fn flags_to_byte(&self) -> Result<u8, NativeError> {
        let mut flags_byte: u8 = 0;

        for flag in &self.flags {
            flags_byte |= match flag {
                Flag::Values => FlagCode::Values as u8,
                Flag::SkipMetadata => FlagCode::SkipMetadata as u8,
                Flag::PageSize => FlagCode::PageSize as u8,
                Flag::WithPagingState => FlagCode::WithPagingState as u8,
                Flag::WithSerialConsistency => FlagCode::WithSerialConsistency as u8,
                Flag::WithDefaultTimestamp => FlagCode::WithDefaultTimestamp as u8,
                Flag::WithNamesForValues => FlagCode::WithNamesForValues as u8,
            }
        }

        Ok(flags_byte)
    }

    fn byte_to_flags(flags_byte: u8) -> Result<Vec<Flag>, NativeError> {
        let mut flags = Vec::new();

        if flags_byte & FlagCode::Values as u8 != 0 {
            flags.push(Flag::Values);
        }
        if flags_byte & FlagCode::SkipMetadata as u8 != 0 {
            flags.push(Flag::SkipMetadata);
        }
        if flags_byte & FlagCode::PageSize as u8 != 0 {
            flags.push(Flag::PageSize);
        }
        if flags_byte & FlagCode::WithPagingState as u8 != 0 {
            flags.push(Flag::WithPagingState);
        }
        if flags_byte & FlagCode::WithSerialConsistency as u8 != 0 {
            flags.push(Flag::WithSerialConsistency);
        }
        if flags_byte & FlagCode::WithDefaultTimestamp as u8 != 0 {
            flags.push(Flag::WithDefaultTimestamp);
        }
        if flags_byte & FlagCode::WithNamesForValues as u8 != 0 {
            flags.push(Flag::WithNamesForValues);
        }

        Ok(flags)
    }
}

#[derive(PartialEq, Debug)]
pub struct Query {
    pub query: String,
    pub params: QueryParams,
}

impl Query {
    pub fn new(query: String, params: QueryParams) -> Self {
        Query { query, params }
    }

    pub fn get_query(&self) -> &str {
        &self.query
    }

    pub fn get_consistency(&self) -> &str {
        self.params.consistency.to_string()
    }
}

impl Serializable for Query {
    // this is a [long string]
    /// ```md
    /// 0         8        16        24        32
    /// +---------+---------+---------+---------+
    /// |        query length (4 bytes)         |
    /// +---------+---------+---------+---------+
    /// |              query bytes              |
    /// +                                       +
    /// |                 ...                   |
    /// +---------+---------+---------+---------+
    /// |  consistency (2)  | flag (1)|         |
    /// +---------+---------+---------+---------+
    /// |         optional parameters           |
    /// +                                       +
    /// |                 ...                   |
    /// +---------+---------+---------+---------+
    /// ```

    /// Serialize the `Query` struct to a byte vector.
    /// The byte vector will contain the query in the format described above.
    fn to_bytes(&self) -> Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        // Add query string length (4 bytes) and query string
        let query_len = self.query.len() as u32;
        bytes.extend_from_slice(&query_len.to_be_bytes());
        bytes.extend_from_slice(self.query.as_bytes());

        // Add consistency (2 bytes)
        let consistency_code = self.params.consistency.to_code()?;
        bytes.extend_from_slice(&(consistency_code as u16).to_be_bytes());

        // Add flags (1 byte)
        let flags_byte = self.params.flags_to_byte()?;
        bytes.push(flags_byte);

        // TODO: Add optional parameters based on flags.

        Ok(bytes)
    }

    /// Parse a `Query` struct from a byte slice.
    /// The byte slice must contain a query in the format described in `to_bytes`.
    fn from_bytes(bytes: &[u8]) -> Result<Self, NativeError> {
        let mut cursor = std::io::Cursor::new(bytes);

        // Read query length (4 bytes)
        let mut query_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut query_len_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let query_len = u32::from_be_bytes(query_len_bytes) as usize;

        // Read the query string (UTF-8)
        let mut query_bytes = vec![0u8; query_len];
        cursor
            .read_exact(&mut query_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let query =
            String::from_utf8(query_bytes).map_err(|_| NativeError::DeserializationError)?;

        // Read the consistency level (2 bytes)
        let mut consistency_code_bytes = [0u8; 2];
        cursor
            .read_exact(&mut consistency_code_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let consistency_code = u16::from_be_bytes(consistency_code_bytes);

        // Convert the consistency code to the corresponding `Consistency`
        let consistency = Consistency::from_code(consistency_code)?;

        // Read flags (1 byte)
        let mut flags_byte = [0u8; 1];
        cursor
            .read_exact(&mut flags_byte)
            .map_err(|_| NativeError::CursorError)?;
        let flags_byte = flags_byte[0];

        // Convert the flags byte to a vector of `Flag`
        let flags = QueryParams::byte_to_flags(flags_byte)?;

        // Create the `QueryParams` and the `Query` struct
        let params = QueryParams { consistency, flags };

        Ok(Query { query, params })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_to_bytes_ok() {
        let query = "SELECT * FROM users WHERE id = 2".to_string();
        let params = QueryParams {
            consistency: Consistency::Quorum,
            flags: vec![Flag::Values, Flag::PageSize],
        };

        let query_message = Query {
            query: query.to_string(),
            params,
        };

        let actual_bytes = query_message.to_bytes().unwrap();

        let expected_bytes: Vec<u8> = vec![
            // Longitud de la query string (4 bytes)
            0x00, 0x00, 0x00, 0x20,
            // Query string: "SELECT * FROM users WHERE id = 2" en UTF-8
            0x53, 0x45, 0x4C, 0x45, 0x43, 0x54, 0x20, 0x2A, 0x20, 0x46, 0x52, 0x4F, 0x4D, 0x20,
            0x75, 0x73, 0x65, 0x72, 0x73, 0x20, 0x57, 0x48, 0x45, 0x52, 0x45, 0x20, 0x69, 0x64,
            0x20, 0x3D, 0x20, 0x32,
            // Consistency (Quorum = 0x0004 en 2 bytes) -----------
            0x00, 0x04,
            // Flags (1 byte, con Values (0x01) y PageSize (0x04) = 0x05) ----------
            0x05,
        ];

        assert_eq!(actual_bytes, expected_bytes);
    }

    #[test]
    fn test_to_bytes() {
        let query = "SELECT * FROM users WHERE id = 2".to_string();
        let params = QueryParams {
            consistency: Consistency::Quorum,
            flags: vec![Flag::Values, Flag::PageSize],
        };

        let query_len = query.len();

        let query_message = Query { query, params };

        // Serialize to bytes
        let query_bytes = query_message.to_bytes().unwrap();

        // Check the length of the serialized byte array
        // Length of query length (4 bytes) + query string + consistency (2 bytes) + flags (1 byte)
        assert_eq!(query_bytes.len(), 4 + query_len + 2 + 1);

        // Check the query length (first 4 bytes)
        let expected_query_len = query_len as u32;
        assert_eq!(
            u32::from_be_bytes(query_bytes[0..4].try_into().unwrap()),
            expected_query_len
        );

        // Check the consistency code (next 2 bytes)
        let expected_consistency_code = ConsistencyCode::Quorum as u16;
        assert_eq!(
            u16::from_be_bytes(
                query_bytes[query_len + 4..query_len + 6]
                    .try_into()
                    .unwrap()
            ),
            expected_consistency_code
        );

        // Check the flags (next 1 byte)
        let expected_flags = FlagCode::Values as u8 | FlagCode::PageSize as u8;
        assert_eq!(query_bytes[query_len + 6], expected_flags);
    }

    #[test]
    fn test_from_bytes() {
        let original_query = "SELECT * FROM users WHERE id = ?".to_string();
        let params = QueryParams {
            consistency: Consistency::Quorum,
            flags: vec![Flag::Values, Flag::PageSize],
        };

        let expected_query = Query {
            query: original_query,
            params,
        };

        // Serialize to bytes
        let query_bytes = expected_query.to_bytes().unwrap();

        // Deserialize from bytes
        let deserialized_query = Query::from_bytes(&query_bytes).unwrap();

        // Check that the original and deserialized queries are the same
        assert_eq!(expected_query, deserialized_query);
    }
}
