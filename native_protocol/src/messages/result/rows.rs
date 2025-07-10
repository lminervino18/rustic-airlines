use std::collections::BTreeMap;
use std::io::Cursor;
use std::{io::Read, net::IpAddr};

use uuid::Uuid;

use crate::types::FromCursorDeserializable;
use crate::{
    types::{Bytes, CassandraString, Int, OptionBytes, OptionSerializable},
    NativeError, Serializable,
};

use super::metadata::Metadata;

enum ColumnTypeCode {
    Custom = 0x0000,
    Ascii = 0x0001,
    Bigint = 0x0002,
    Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Timestamp = 0x000B,
    Uuid = 0x000C,
    Varchar = 0x000D,
    Varint = 0x000E,
    Timeuuid = 0x000F,
    Inet = 0x0010,
    List = 0x0020,
    Set = 0x0022,
    Tuple = 0x0031,
}

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Custom(String),
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    List(Box<ColumnType>),
    // Map(Box<ColumnType>, Box<ColumnType>),
    Set(Box<ColumnType>),
    /* UDT {
        keyspace: String,
        udt_name: String,
        fields: Vec<(String, ColumnType)>,
    }, */
    Tuple(Vec<ColumnType>),
}

impl OptionSerializable for ColumnType {
    fn serialize_option(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        match self {
            ColumnType::Custom(custom) => {
                bytes.extend_from_slice(&(ColumnTypeCode::Custom as u16).to_be_bytes());
                bytes.extend_from_slice(custom.to_string_bytes()?.as_slice());

                Ok(bytes)
            }
            ColumnType::Ascii => {
                bytes.extend_from_slice(&(ColumnTypeCode::Ascii as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Bigint => {
                bytes.extend_from_slice(&(ColumnTypeCode::Bigint as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Blob => {
                bytes.extend_from_slice(&(ColumnTypeCode::Blob as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Boolean => {
                bytes.extend_from_slice(&(ColumnTypeCode::Boolean as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Counter => {
                bytes.extend_from_slice(&(ColumnTypeCode::Counter as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Decimal => {
                bytes.extend_from_slice(&(ColumnTypeCode::Decimal as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Double => {
                bytes.extend_from_slice(&(ColumnTypeCode::Double as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Float => {
                bytes.extend_from_slice(&(ColumnTypeCode::Float as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Int => {
                bytes.extend_from_slice(&(ColumnTypeCode::Int as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Timestamp => {
                bytes.extend_from_slice(&(ColumnTypeCode::Timestamp as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Uuid => {
                bytes.extend_from_slice(&(ColumnTypeCode::Uuid as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Varchar => {
                bytes.extend_from_slice(&(ColumnTypeCode::Varchar as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Varint => {
                bytes.extend_from_slice(&(ColumnTypeCode::Varint as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Timeuuid => {
                bytes.extend_from_slice(&(ColumnTypeCode::Timeuuid as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::Inet => {
                bytes.extend_from_slice(&(ColumnTypeCode::Inet as u16).to_be_bytes());
                Ok(bytes)
            }
            ColumnType::List(inner_type) => {
                bytes.extend_from_slice(&(ColumnTypeCode::List as u16).to_be_bytes());
                let inner_type_bytes = inner_type.to_option_bytes()?;
                bytes.extend_from_slice(inner_type_bytes.as_slice());

                Ok(bytes)
            }
            ColumnType::Set(inner_type) => {
                bytes.extend_from_slice(&(ColumnTypeCode::Set as u16).to_be_bytes());
                let inner_type_bytes = inner_type.to_option_bytes()?;
                bytes.extend_from_slice(inner_type_bytes.as_slice());

                Ok(bytes)
            }
            ColumnType::Tuple(inner_types) => {
                bytes.extend_from_slice(&(ColumnTypeCode::Tuple as u16).to_be_bytes());
                let inner_types_len = inner_types.len() as u16;
                bytes.extend_from_slice(&inner_types_len.to_be_bytes());
                for inner_type in inner_types {
                    let inner_type_bytes = inner_type.to_option_bytes()?;
                    bytes.extend_from_slice(inner_type_bytes.as_slice());
                }

                Ok(bytes)
            }
        }
    }

    fn deserialize_option(
        option_id: u16,
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> std::result::Result<Self, NativeError> {
        match option_id {
            0x0000 => {
                let custom = String::from_string_bytes(cursor)?;
                Ok(ColumnType::Custom(custom))
            }
            0x0001 => Ok(ColumnType::Ascii),
            0x0002 => Ok(ColumnType::Bigint),
            0x0003 => Ok(ColumnType::Blob),
            0x0004 => Ok(ColumnType::Boolean),
            0x0005 => Ok(ColumnType::Counter),
            0x0006 => Ok(ColumnType::Decimal),
            0x0007 => Ok(ColumnType::Double),
            0x0008 => Ok(ColumnType::Float),
            0x0009 => Ok(ColumnType::Int),
            0x000B => Ok(ColumnType::Timestamp),
            0x000C => Ok(ColumnType::Uuid),
            0x000D => Ok(ColumnType::Varchar),
            0x000E => Ok(ColumnType::Varint),
            0x000F => Ok(ColumnType::Timeuuid),
            0x0010 => Ok(ColumnType::Inet),
            0x0020 => {
                let inner_type = ColumnType::from_option_bytes(cursor)?;
                Ok(ColumnType::List(Box::new(inner_type)))
            }
            0x0021 => {
                todo!()
                // let key_type = ColumnType::from_option_bytes(cursor)?;
                // let value_type = ColumnType::from_option_bytes(cursor)?;
                // Ok(ColumnType::Map(Box::new(key_type), Box::new(value_type)))
            }
            0x0022 => {
                let inner_type = ColumnType::from_option_bytes(cursor)?;
                Ok(ColumnType::Set(Box::new(inner_type)))
            }
            0x0030 => {
                /* let keyspace = String::from_string_bytes(cursor);
                let name = String::from_string_bytes(cursor);

                let mut fields_len_bytes = [0u8; 2];
                cursor.read_exact(&mut fields_len_bytes)?;
                let fields_count = u16::from_be_bytes(fields_len_bytes);
                let mut fields = Vec::new();
                for _ in 0..fields_count {
                    let field_name = String::from_string_bytes(cursor);
                    let field_type = ColumnType::from_option_bytes(cursor)?;
                    fields.push((field_name, field_type));
                }
                Ok(ColumnType::UDT {
                    keyspace,
                    name,
                    fields,
                }) */
                todo!()
            }
            0x0031 => {
                let mut inner_type_len_bytes = [0u8; 2];
                cursor
                    .read_exact(&mut inner_type_len_bytes)
                    .map_err(|_| NativeError::CursorError)?;
                let inner_types_count = u16::from_be_bytes(inner_type_len_bytes);
                let mut inner_types = Vec::new();
                for _ in 0..inner_types_count {
                    let inner_type = ColumnType::from_option_bytes(cursor)?;
                    inner_types.push(inner_type);
                }
                Ok(ColumnType::Tuple(inner_types))
            }
            _ => Err(NativeError::InvalidCode),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ColumnValue {
    Custom(String),
    Ascii(String), // this is actually an ascii string
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    Decimal {
        scale: i32,
        unscaled: Vec<u8>, // Big-endian two's complement representation
    },
    Double(f64),
    Float(f32),
    Int(i32),
    Timestamp(i64), // Milliseconds since epoch
    Uuid(Uuid),
    Varchar(String),
    Varint(Vec<u8>),
    Timeuuid(Uuid),
    Inet(IpAddr),
    List(Vec<ColumnValue>),
    // Map(HashMap<ColumnValue, ColumnValue>),
    Set(Vec<ColumnValue>),
    /* UDT {
        keyspace: String,
        udt_name: String,
        fields: Vec<(String, ColumnType)>,
    }, */
    Tuple(Vec<ColumnValue>),
}

impl ColumnValue {
    pub fn to_bytes(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        match self {
            ColumnValue::Custom(custom) => {
                bytes.extend_from_slice(custom.to_string_bytes()?.as_slice());
            }
            ColumnValue::Ascii(ascii) => {
                bytes.extend_from_slice(ascii.to_string_bytes()?.as_slice());
            }
            ColumnValue::Bigint(bigint) => {
                bytes.extend_from_slice(&bigint.to_be_bytes());
            }
            ColumnValue::Blob(blob) => {
                bytes.extend_from_slice(blob.as_slice());
            }
            ColumnValue::Boolean(boolean) => {
                let byte = if *boolean { 1u8 } else { 0u8 };
                bytes.push(byte);
            }
            ColumnValue::Counter(counter) => {
                bytes.extend_from_slice(&counter.to_be_bytes());
            }
            ColumnValue::Decimal { scale, unscaled } => {
                bytes.extend_from_slice(&scale.to_be_bytes());
                bytes.extend_from_slice(unscaled.as_slice());
            }
            ColumnValue::Double(double) => {
                bytes.extend_from_slice(&double.to_be_bytes());
            }
            ColumnValue::Float(float) => {
                bytes.extend_from_slice(&float.to_be_bytes());
            }
            ColumnValue::Int(int) => {
                bytes.extend_from_slice(&int.to_be_bytes());
            }
            ColumnValue::Timestamp(timestamp) => {
                bytes.extend_from_slice(&timestamp.to_be_bytes());
            }
            ColumnValue::Uuid(uuid) => {
                bytes.extend_from_slice(uuid.as_bytes());
            }
            ColumnValue::Varchar(varchar) => {
                bytes.extend_from_slice(varchar.to_string_bytes()?.as_slice());
            }
            ColumnValue::Varint(varint) => {
                bytes.extend_from_slice(varint.as_slice());
            }
            ColumnValue::Timeuuid(timeuuid) => {
                bytes.extend_from_slice(timeuuid.as_bytes());
            }
            ColumnValue::Inet(inet) => match inet {
                IpAddr::V4(ipv4) => {
                    bytes.extend_from_slice(&ipv4.octets());
                }
                IpAddr::V6(ipv6) => {
                    bytes.extend_from_slice(&ipv6.octets());
                }
            },
            // A [int] n indicating the number of elements in the list, followed
            // by n elements. Each element is [bytes] representing the serialized value.
            ColumnValue::List(inner_value) => {
                let number_of_elements = Int::from(inner_value.len() as i32);
                bytes.extend_from_slice(number_of_elements.to_be_bytes().as_slice());

                for value in inner_value {
                    let value_bytes = Bytes::Vec(value.to_bytes()?).to_bytes()?;
                    bytes.extend_from_slice(&value_bytes);
                }
            }
            /* ColumnValue::Map(key_value, value_value) => {
                todo!()
            } */
            // A [int] n indicating the number of elements in the set, followed by n
            // elements. Each element is [bytes] representing the serialized value.
            ColumnValue::Set(inner_value) => {
                let number_of_elements = Int::from(inner_value.len() as i32);
                bytes.extend_from_slice(number_of_elements.to_be_bytes().as_slice());

                for value in inner_value {
                    let value_bytes = Bytes::Vec(value.to_bytes()?).to_bytes()?;
                    bytes.extend_from_slice(&value_bytes);
                }
            }
            // A UDT value is composed of successive [bytes] values, one for each field of the UDT
            // value (in the order defined by the type).
            // ColumnValue::UDT { .. } => {
            //     todo!()
            // }
            // A sequence of [bytes] values representing the items in a tuple. The encoding
            // of each element depends on the data type for that position in the tuple.
            ColumnValue::Tuple(inner_value) => {
                let number_of_elements = Int::from(inner_value.len() as i32);
                bytes.extend_from_slice(number_of_elements.to_be_bytes().as_slice());

                for value in inner_value {
                    let value_bytes = Bytes::Vec(value.to_bytes()?).to_bytes()?;
                    bytes.extend_from_slice(&value_bytes);
                }
            }
        }
        Ok(bytes)
    }

    pub fn from_bytes(cursor: &mut Cursor<&[u8]>, type_: &ColumnType) -> Result<Self, NativeError> {
        let value = match type_ {
            ColumnType::Custom(_) => {
                let custom = String::from_string_bytes(cursor)?;
                ColumnValue::Custom(custom)
            }
            ColumnType::Ascii => {
                let ascii = String::from_string_bytes(cursor)?;
                ColumnValue::Ascii(ascii)
            }
            ColumnType::Bigint => {
                let mut bigint_bytes = [0u8; 8];
                cursor
                    .read_exact(&mut bigint_bytes)
                    .map_err(|_| NativeError::CursorError)?;
                let bigint = i64::from_be_bytes(bigint_bytes);
                ColumnValue::Bigint(bigint)
            }
            ColumnType::Blob => {
                let mut bytes = vec![];
                cursor
                    .read_to_end(&mut bytes)
                    .map_err(|_| NativeError::CursorError)?;

                ColumnValue::Blob(bytes)
            }
            ColumnType::Boolean => {
                let mut boolean_byte = [0u8; 1];
                cursor
                    .read_exact(&mut boolean_byte)
                    .map_err(|_| NativeError::CursorError)?;
                let boolean = boolean_byte[0] == 1;
                ColumnValue::Boolean(boolean)
            }
            ColumnType::Counter => {
                let mut counter_bytes = [0u8; 8];
                cursor
                    .read_exact(&mut counter_bytes)
                    .map_err(|_| NativeError::CursorError)?;
                let counter = i64::from_be_bytes(counter_bytes);
                ColumnValue::Counter(counter)
            }
            ColumnType::Decimal => {
                let mut scale_bytes = [0u8; 4];
                cursor
                    .read_exact(&mut scale_bytes)
                    .map_err(|_| NativeError::CursorError)?;
                let scale = i32::from_be_bytes(scale_bytes);

                let unscaled = Bytes::from_bytes(cursor)?;
                let unscaled = if let Bytes::Vec(unscaled) = unscaled {
                    unscaled
                } else {
                    vec![]
                };

                ColumnValue::Decimal { scale, unscaled }
            }
            ColumnType::Double => {
                let mut double_bytes = [0u8; 8];
                cursor
                    .read_exact(&mut double_bytes)
                    .map_err(|_| NativeError::CursorError)?;
                let double = f64::from_be_bytes(double_bytes);
                ColumnValue::Double(double)
            }
            ColumnType::Float => {
                let mut float_bytes = [0u8; 4];
                cursor
                    .read_exact(&mut float_bytes)
                    .map_err(|_| NativeError::CursorError)?;
                let float = f32::from_be_bytes(float_bytes);
                ColumnValue::Float(float)
            }
            ColumnType::Int => {
                let mut int_bytes = [0u8; 4];
                cursor
                    .read_exact(&mut int_bytes)
                    .map_err(|_| NativeError::CursorError)?;
                let int = i32::from_be_bytes(int_bytes);
                ColumnValue::Int(int)
            }
            ColumnType::Timestamp => {
                let mut timestamp_bytes = [0u8; 8];
                cursor
                    .read_exact(&mut timestamp_bytes)
                    .map_err(|_| NativeError::CursorError)?;
                let timestamp = i64::from_be_bytes(timestamp_bytes);
                ColumnValue::Timestamp(timestamp)
            }
            ColumnType::Uuid => {
                let mut uuid = [0u8; 16];
                cursor
                    .read_exact(&mut uuid)
                    .map_err(|_| NativeError::CursorError)?;
                ColumnValue::Uuid(Uuid::from_bytes(uuid))
            }
            ColumnType::Varchar => {
                let varchar = String::from_string_bytes(cursor)?;
                ColumnValue::Varchar(varchar)
            }
            ColumnType::Varint => {
                let varint = Bytes::from_bytes(cursor)?;
                let varint = if let Bytes::Vec(varint) = varint {
                    varint
                } else {
                    vec![]
                };
                ColumnValue::Varint(varint)
            }
            ColumnType::Timeuuid => {
                let mut timeuuid = [0u8; 16];
                cursor
                    .read_exact(&mut timeuuid)
                    .map_err(|_| NativeError::CursorError)?;
                ColumnValue::Timeuuid(Uuid::from_bytes(timeuuid))
            }
            ColumnType::Inet => {
                let mut bytes = [0u8; 16];
                let bytes_len = cursor
                    .read(&mut bytes)
                    .map_err(|_| NativeError::CursorError)?;
                let inet = match bytes_len {
                    4 => IpAddr::V4(std::net::Ipv4Addr::new(
                        bytes[0], bytes[1], bytes[2], bytes[3],
                    )),
                    16 => IpAddr::V6(std::net::Ipv6Addr::new(
                        u16::from_be_bytes([bytes[0], bytes[1]]),
                        u16::from_be_bytes([bytes[2], bytes[3]]),
                        u16::from_be_bytes([bytes[4], bytes[5]]),
                        u16::from_be_bytes([bytes[6], bytes[7]]),
                        u16::from_be_bytes([bytes[8], bytes[9]]),
                        u16::from_be_bytes([bytes[10], bytes[11]]),
                        u16::from_be_bytes([bytes[12], bytes[13]]),
                        u16::from_be_bytes([bytes[14], bytes[15]]),
                    )),
                    _ => return Err(NativeError::DeserializationError),
                };
                ColumnValue::Inet(inet)
            }
            ColumnType::List(inner_type) => {
                let list = list_from_cursor(cursor, inner_type)?;
                ColumnValue::List(list)
            }
            ColumnType::Set(_) => {
                todo!()
            }
            ColumnType::Tuple(_) => {
                todo!()
            }
        };
        Ok(value)
    }
}

fn list_from_cursor(
    cursor: &mut std::io::Cursor<&[u8]>,
    col_type: &ColumnType,
) -> Result<Vec<ColumnValue>, NativeError> {
    let number_of_elements = Int::deserialize(cursor)?;

    if number_of_elements < 0 {
        return Ok(vec![]);
    }

    let number_of_elements: u16 = number_of_elements
        .try_into()
        .map_err(|_| NativeError::DeserializationError)?;

    let elements = (0..number_of_elements)
        .map(|_| {
            let bytes = Bytes::from_bytes(cursor)?;

            let inner_bytes = if let Bytes::Vec(inner_bytes) = bytes {
                inner_bytes
            } else {
                vec![]
            };

            let mut in_cursor = Cursor::new(inner_bytes.as_slice());

            ColumnValue::from_bytes(&mut in_cursor, col_type)
        })
        .collect();

    elements
}

// key: column name, value: column value
type Row = BTreeMap<String, ColumnValue>;

#[derive(Debug, PartialEq)]
/// Indicates a set of rows.
pub struct Rows {
    pub metadata: Metadata,
    pub rows_count: Int,
    pub rows_content: Vec<Row>,
}

impl Rows {
    pub fn new(cols: Vec<(String, ColumnType)>, rows: Vec<Row>) -> Rows {
        let metadata = Metadata::new(cols.len() as u32, cols);

        Self {
            metadata,
            rows_count: Int::from(rows.len() as i32),
            rows_content: rows,
        }
    }
}

impl Serializable for Rows {
    fn to_bytes(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.metadata.to_bytes()?);

        bytes.extend_from_slice(&self.rows_count.to_be_bytes());

        for row in &self.rows_content {
            for col in &self.metadata.col_spec_i {
                let col_name = &col.name;

                let value = row.get(col_name).unwrap();
                let value_bytes = Bytes::Vec(value.to_bytes()?).to_bytes()?;

                bytes.extend_from_slice(&value_bytes);
            }
        }

        Ok(bytes)
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, NativeError> {
        let mut cursor = std::io::Cursor::new(bytes);

        let metadata = Metadata::from_bytes(&mut cursor)?;

        let rows_count = Int::deserialize(&mut cursor)?;

        let mut rows_content = Vec::new();
        for _ in 0..rows_count {
            let mut row = BTreeMap::new();
            for col_spec in &metadata.col_spec_i {
                let value_bytes = Bytes::from_bytes(&mut cursor)?;

                let bytes_ = if let Bytes::Vec(bytes) = value_bytes {
                    bytes
                } else {
                    Err(NativeError::DeserializationError)?
                };

                let mut cursor2 = Cursor::new(bytes_.as_slice());

                let value = ColumnValue::from_bytes(&mut cursor2, &col_spec.type_)?;
                row.insert(col_spec.name.clone(), value);
            }
            rows_content.push(row);
        }

        Ok(Rows {
            metadata,
            rows_count,
            rows_content,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use crate::messages::result::metadata::{ColumnSpec, MetadataFlags, TableSpec};
    use crate::messages::result::rows::Rows;
    use crate::Serializable;
    use crate::{
        messages::result::metadata::Metadata,
        types::{Bytes, CassandraString, Int},
    };

    use super::{ColumnType, ColumnValue};

    #[test]
    fn blob_to_column_value() {
        let blob = vec![0x01, 0x02, 0x03, 0x04];

        let value = ColumnValue::Blob(blob.clone()).to_bytes().unwrap();

        assert_eq!(blob, value)
    }

    #[test]
    fn column_value_custom_to_bytes() {
        let col = ColumnValue::Custom("custom".to_string())
            .to_bytes()
            .unwrap();

        assert_eq!(col, "custom".to_string().to_string_bytes().unwrap())
    }

    #[test]
    fn column_value_ascii_to_bytes() {
        let col = ColumnValue::Ascii("ascii".to_string()).to_bytes().unwrap();

        assert_eq!(col, "ascii".to_string().to_string_bytes().unwrap())
    }

    #[test]
    fn column_value_uuid_to_bytes() {
        let uuid = uuid::Uuid::from_u128(0x1234567890abcdef1234567890abcdef);

        let value = ColumnValue::Uuid(uuid).to_bytes().unwrap();

        assert_eq!(value, uuid.as_bytes())
    }

    #[test]
    fn column_value_uuid_from_bytes() {
        let uuid = uuid::Uuid::from_u128(0x1234567890abcdef1234567890abcdef);

        let value = ColumnValue::Uuid(uuid);

        let bytes = value.to_bytes().unwrap();

        assert_eq!(
            ColumnValue::from_bytes(&mut Cursor::new(&bytes), &ColumnType::Uuid).unwrap(),
            value
        )
    }

    #[test]
    fn column_value_list_to_bytes() {
        let blob_1 = vec![0x01u8, 0x02, 0x03, 0x04];
        let blob_2 = vec![0x02, 0x01];

        let col = ColumnValue::List(vec![
            ColumnValue::Blob(blob_1.clone()),
            ColumnValue::Blob(blob_2.clone()),
        ]);

        // expected bytes: length + bytes blob1 + bytes blob2
        let bytes = vec![
            Int::from(2).to_be_bytes().to_vec(), // number of elements
            Bytes::Vec(blob_1).to_bytes().unwrap(),
            Bytes::Vec(blob_2).to_bytes().unwrap(),
        ]
        .concat();

        assert_eq!(col.to_bytes().unwrap(), bytes)
    }

    #[test]
    fn column_value_list_from_bytes() {
        let blob_1 = vec![0x01u8, 0x02, 0x03, 0x04];
        let blob_2 = vec![0x02, 0x01];

        let col = ColumnValue::List(vec![
            ColumnValue::Blob(blob_1.clone()),
            ColumnValue::Blob(blob_2.clone()),
        ]);

        let bytes = col.to_bytes().unwrap();

        assert_eq!(
            ColumnValue::from_bytes(
                &mut Cursor::new(&bytes),
                &ColumnType::List(Box::new(ColumnType::Blob))
            )
            .unwrap(),
            col,
        )
    }

    #[test]
    fn rows_to_bytes() {
        let rows = Rows {
            metadata: Metadata {
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
            },
            rows_count: Int::from(2),
            rows_content: vec![BTreeMap::from([(
                "test_column".to_string(),
                ColumnValue::Int(1),
            )])],
        };

        let bytes = rows.to_bytes().unwrap();

        let mut expected_bytes = Vec::new();
        let metadata_bytes = rows.metadata.to_bytes().unwrap();
        let rows_count_bytes = rows.rows_count.to_be_bytes();

        let row_content_bytes = vec![Bytes::Vec(ColumnValue::Int(1).to_bytes().unwrap())
            .to_bytes()
            .unwrap()]
        .concat();

        expected_bytes.extend_from_slice(&metadata_bytes);
        expected_bytes.extend_from_slice(&rows_count_bytes);
        expected_bytes.extend_from_slice(&row_content_bytes);

        assert_eq!(bytes, expected_bytes)
    }

    #[test]
    fn rows_from_bytes() {
        let expected_rows = Rows {
            metadata: Metadata {
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
            },
            rows_count: Int::from(2),
            rows_content: vec![
                BTreeMap::from([("test_column".to_string(), ColumnValue::Int(1))]),
                BTreeMap::from([("test_column".to_string(), ColumnValue::Int(2))]),
            ],
        };

        let bytes = expected_rows.to_bytes().unwrap();

        let rows = Rows::from_bytes(&bytes).unwrap();

        assert_eq!(rows, expected_rows)
    }

    #[test]
    fn rows_to_bytes_() {
        let rows = Rows {
            metadata: Metadata {
                flags: MetadataFlags {
                    global_table_spec: false,
                    has_more_pages: false,
                    no_metadata: false,
                },
                columns_count: 1,
                global_table_spec: None,
                col_spec_i: vec![ColumnSpec {
                    keyspace: None,
                    table_name: None,
                    name: "test_column".to_string(),
                    type_: ColumnType::Int,
                }],
            },
            rows_count: Int::from(2),
            rows_content: vec![BTreeMap::from([(
                "test_column".to_string(),
                ColumnValue::Int(1),
            )])],
        };

        let bytes = rows.to_bytes().unwrap();

        let mut expected_bytes = Vec::new();
        let metadata_bytes = rows.metadata.to_bytes().unwrap();
        let rows_count_bytes = rows.rows_count.to_be_bytes();

        let row_content_bytes = vec![Bytes::Vec(ColumnValue::Int(1).to_bytes().unwrap())
            .to_bytes()
            .unwrap()]
        .concat();

        expected_bytes.extend_from_slice(&metadata_bytes);
        expected_bytes.extend_from_slice(&rows_count_bytes);
        expected_bytes.extend_from_slice(&row_content_bytes);

        assert_eq!(bytes, expected_bytes)
    }

    #[test]
    fn rows_from_bytes_() {
        let cols = vec![
            ("age".to_string(), ColumnType::Int),
            ("name".to_string(), ColumnType::Varchar),
        ];
        let rows = vec![
            BTreeMap::from([
                ("age".to_string(), ColumnValue::Int(1)),
                ("name".to_string(), ColumnValue::Varchar("John".to_string())),
            ]),
            BTreeMap::from([
                ("age".to_string(), ColumnValue::Int(2)),
                ("name".to_string(), ColumnValue::Varchar("Doe".to_string())),
            ]),
        ];

        let expected_rows = Rows::new(cols, rows);

        let bytes = expected_rows.to_bytes().unwrap();

        let rows = Rows::from_bytes(&bytes).unwrap();

        assert_eq!(rows, expected_rows)
    }

    #[test]
    fn rows_from_bytes_2() {
        let cols = vec![
            ("user_id".to_string(), ColumnType::Uuid),
            ("age".to_string(), ColumnType::Int),
            ("last_name".to_string(), ColumnType::Ascii),
            ("weight".to_string(), ColumnType::Int),
        ];
        let rows = vec![
            BTreeMap::from([
                (
                    "user_id".to_string(),
                    ColumnValue::Uuid(uuid::Uuid::from_u128(0x1234567890abcdef1234567890abcdef)),
                ),
                ("age".to_string(), ColumnValue::Int(1)),
                (
                    "last_name".to_string(),
                    ColumnValue::Ascii("Doe".to_string()),
                ),
                ("weight".to_string(), ColumnValue::Int(70)),
            ]),
            BTreeMap::from([
                (
                    "user_id".to_string(),
                    ColumnValue::Uuid(uuid::Uuid::from_u128(0x1234567890abcdef1234567890cdefab)),
                ),
                ("age".to_string(), ColumnValue::Int(3)),
                (
                    "last_name".to_string(),
                    ColumnValue::Ascii("Smith".to_string()),
                ),
                ("weight".to_string(), ColumnValue::Int(80)),
            ]),
        ];

        let expected_rows = Rows::new(cols, rows);

        let bytes = expected_rows.to_bytes().unwrap();

        let rows = Rows::from_bytes(&bytes).unwrap();

        assert_eq!(rows, expected_rows)
    }

    #[test]
    fn rows_to_from_bytes_with_email() {
        let cols = vec![
            ("age".to_string(), ColumnType::Int),
            ("name".to_string(), ColumnType::Ascii),
            ("email".to_string(), ColumnType::Ascii),
            ("email2".to_string(), ColumnType::Ascii),
        ];
        let rows = vec![
            BTreeMap::from([
                ("age".to_string(), ColumnValue::Int(25)),
                ("name".to_string(), ColumnValue::Ascii("John".to_string())),
                (
                    "email".to_string(),
                    ColumnValue::Ascii("john@gmail.com".to_string()),
                ),
                (
                    "email2".to_string(),
                    ColumnValue::Ascii("marston@hotmail.com".to_string()),
                ),
            ]),
            BTreeMap::from([
                ("age".to_string(), ColumnValue::Int(30)),
                ("name".to_string(), ColumnValue::Ascii("Bill".to_string())),
                (
                    "email".to_string(),
                    ColumnValue::Ascii("bill@gmail.com".to_string()),
                ),
                (
                    "email2".to_string(),
                    ColumnValue::Ascii("williamson@hotmail.com".to_string()),
                ),
            ]),
        ];

        let expected_rows = Rows::new(cols, rows);

        let bytes = expected_rows.to_bytes().unwrap();

        let rows = Rows::from_bytes(&bytes).unwrap();

        assert_eq!(rows, expected_rows)
    }
}
