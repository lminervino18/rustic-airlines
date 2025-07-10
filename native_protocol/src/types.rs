use std::io::{Cursor, Read};

use crate::errors::NativeError;

/// A 2 bytes unsigned integer.
pub type Short = u16;
/// A 4 bytes signed integer.
pub type Int = i32;

pub trait FromCursorDeserializable {
    fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<Self, NativeError>
    where
        Self: Sized;
}

impl FromCursorDeserializable for Int {
    fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<Self, NativeError> {
        let mut bytes = [0u8; 4];
        cursor
            .read_exact(&mut bytes)
            .map_err(|_| NativeError::CursorError)?;

        Ok(Int::from_be_bytes(bytes))
    }
}

pub trait OptionSerializable {
    fn deserialize_option(
        option_id: u16,
        cursor: &mut Cursor<&[u8]>,
    ) -> std::result::Result<Self, NativeError>
    where
        Self: Sized;

    fn serialize_option(&self) -> std::result::Result<Vec<u8>, NativeError>;
}

pub trait OptionBytes: Sized {
    fn from_option_bytes(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> std::result::Result<Self, NativeError>;

    fn to_option_bytes(&self) -> Result<Vec<u8>, NativeError>;
}

impl<T: OptionSerializable> OptionBytes for T {
    fn to_option_bytes(&self) -> Result<Vec<u8>, NativeError> {
        self.serialize_option()
    }

    fn from_option_bytes(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> std::result::Result<Self, NativeError> {
        let mut option_id_bytes = [0u8; 2];
        cursor
            .read_exact(&mut option_id_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let option_id = u16::from_be_bytes(option_id_bytes);

        T::deserialize_option(option_id, cursor)
    }
}

pub trait CassandraString {
    fn from_string_bytes(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> std::result::Result<Self, NativeError>
    where
        Self: Sized;

    fn to_string_bytes(&self) -> std::result::Result<Vec<u8>, NativeError>;
}

impl CassandraString for String {
    fn from_string_bytes(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> std::result::Result<Self, NativeError> {
        let mut len_bytes = [0u8; 2];
        cursor
            .read_exact(&mut len_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let len = u16::from_be_bytes(len_bytes) as usize;

        let mut string_bytes = vec![0u8; len];
        cursor
            .read_exact(&mut string_bytes)
            .map_err(|_| NativeError::CursorError)?;

        let string =
            String::from_utf8(string_bytes).map_err(|_| NativeError::DeserializationError)?;

        Ok(string)
    }

    fn to_string_bytes(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.len() as u16).to_be_bytes());

        bytes.extend_from_slice(self.as_bytes());

        Ok(bytes)
    }
}

#[derive(Debug, PartialEq)]
pub enum Bytes {
    None,
    Vec(Vec<u8>),
}

impl Default for Bytes {
    fn default() -> Self {
        Bytes::None
    }
}

impl Bytes {
    pub fn to_bytes(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        match self {
            Bytes::None => {
                bytes.extend_from_slice(&Int::from(-1).to_be_bytes());
            }
            Bytes::Vec(vec) => {
                bytes.extend_from_slice(&Int::from(vec.len() as i32).to_be_bytes());
                bytes.extend_from_slice(vec.as_slice());
            }
        }
        Ok(bytes)
    }

    pub fn from_bytes(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> std::result::Result<Self, NativeError> {
        let mut bytes_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut bytes_len_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let bytes_len = Int::from_be_bytes(bytes_len_bytes);

        if bytes_len < 0 {
            return Ok(Self::None);
        }

        let mut bytes_bytes = vec![0u8; bytes_len as usize];
        cursor
            .read_exact(&mut bytes_bytes)
            .map_err(|_| NativeError::CursorError)?;

        Ok(Self::Vec(bytes_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes_null() {
        let bytes = Bytes::None;

        let bytes = bytes.to_bytes().unwrap();

        // largo -1 para representar null
        assert_eq!(bytes, [0xFF; 4])
    }

    #[test]
    fn test_to_bytes_vec() {
        let bytes = Bytes::Vec(vec![0x01, 0x02, 0x03, 0x00]);

        let bytes = bytes.to_bytes().unwrap();

        // 4 bytes para el Int + los 4 bytes a transportar
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x00])
    }

    #[test]
    fn test_from_bytes_null() {
        let input = [0xFF, 0xFF, 0xFF, 0xFF].as_slice();
        let mut cursor = std::io::Cursor::new(input);

        let result = Bytes::from_bytes(&mut cursor).unwrap();

        assert_eq!(result, Bytes::None)
    }

    #[test]
    fn test_from_bytes_vec() {
        let input = [0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x00].as_slice();

        let mut cursor = std::io::Cursor::new(input);

        let result = Bytes::from_bytes(&mut cursor).unwrap();

        assert_eq!(result, Bytes::Vec(vec![0x01, 0x02, 0x03, 0x00]));
    }

    #[test]
    fn string_from_to_bytes() {
        let string = "test_column".to_string();
        let bytes = string.to_string_bytes().unwrap();

        let string_ =
            String::from_string_bytes(&mut std::io::Cursor::new(bytes.as_slice())).unwrap();

        assert_eq!(string, string_);
    }

    #[test]
    fn string_from_string_bytes() {
        let input = [0x00, 0x03, 'a' as u8, 'b' as u8, 'c' as u8, 'd' as u8];

        let mut cursor = std::io::Cursor::new(input.as_slice());

        let string = String::from_string_bytes(&mut cursor).unwrap();

        assert_eq!(string, "abc");
    }

    #[test]
    fn option_from_option_bytes() {
        #[derive(PartialEq, Debug)]
        enum Options {
            Something,
            SomethinElse(String),
        }

        impl OptionSerializable for Options {
            fn deserialize_option(
                option_id: u16,
                cursor: &mut Cursor<&[u8]>,
            ) -> std::result::Result<Self, NativeError>
            where
                Self: Sized,
            {
                match option_id {
                    0x0001 => Ok(Options::Something),
                    0x0002 => {
                        let string = String::from_string_bytes(cursor).unwrap();
                        Ok(Options::SomethinElse(string))
                    }
                    _ => unimplemented!(),
                }
            }

            fn serialize_option(&self) -> Result<Vec<u8>, NativeError> {
                todo!()
            }
        }

        let input = [0x00, 0x02, 0x00, 0x03, 'a' as u8, 'b' as u8, 'c' as u8];
        let mut cursor = std::io::Cursor::new(input.as_slice());

        let option = Options::from_option_bytes(&mut cursor).unwrap();

        assert_eq!(option, Options::SomethinElse("abc".to_string()));
    }

    #[test]
    fn option_to_option_bytes() {
        #[derive(PartialEq, Debug)]
        enum Options {
            _Something,
            SomethinElse(String),
        }

        impl OptionSerializable for Options {
            fn deserialize_option(
                _option_id: u16,
                _cursor: &mut Cursor<&[u8]>,
            ) -> std::result::Result<Self, NativeError>
            where
                Self: Sized,
            {
                todo!()
            }

            fn serialize_option(&self) -> Result<Vec<u8>, NativeError> {
                let mut bytes = Vec::new();

                match self {
                    Options::_Something => {
                        bytes.extend_from_slice(&(0x0001 as u16).to_be_bytes());
                        Ok(bytes)
                    }
                    Options::SomethinElse(txt) => {
                        bytes.extend_from_slice(&(0x0002 as u16).to_be_bytes());
                        bytes.extend_from_slice(&txt.to_string_bytes().unwrap());
                        Ok(bytes)
                    }
                }
            }
        }

        let option = Options::SomethinElse("abc".to_string());
        let bytes = option.to_option_bytes().unwrap();

        let expected = vec![0x00, 0x02, 0x00, 0x03, 'a' as u8, 'b' as u8, 'c' as u8];

        assert_eq!(bytes, expected)
    }
}
