use std::io::Read;

use crate::{errors::NativeError, Serializable};

#[derive(Debug, Copy, Clone)]
pub enum ErrorCode {
    ServerError = 0x0000,
    ProtocolError = 0x000A,
    BadCredentials = 0x0100,
    UnavailableException = 0x1000,
    Overloaded = 0x1001,
    IsBootstrapping = 0x1002,
    TruncateError = 0x1003,
    WriteTimeout = 0x1100,
    ReadTimeout = 0x1200,
    SyntaxError = 0x2000,
    Unauthorized = 0x2100,
    Invalid = 0x2200,
    ConfigError = 0x2300,
    AlreadyExists = 0x2400,
    Unprepared = 0x2500,
}

impl ErrorCode {
    pub fn from_u32(value: u32) -> Result<ErrorCode, NativeError> {
        let error = match value {
            0x0000 => ErrorCode::ServerError,
            0x000A => ErrorCode::ProtocolError,
            0x0100 => ErrorCode::BadCredentials,
            0x1000 => ErrorCode::UnavailableException,
            0x1001 => ErrorCode::Overloaded,
            0x1002 => ErrorCode::IsBootstrapping,
            0x1003 => ErrorCode::TruncateError,
            0x1100 => ErrorCode::WriteTimeout,
            0x1200 => ErrorCode::ReadTimeout,
            0x2000 => ErrorCode::SyntaxError,
            0x2100 => ErrorCode::Unauthorized,
            0x2200 => ErrorCode::Invalid,
            0x2300 => ErrorCode::ConfigError,
            0x2400 => ErrorCode::AlreadyExists,
            0x2500 => ErrorCode::Unprepared,
            _ => return Err(NativeError::InvalidCode),
        };

        Ok(error)
    }

    pub fn to_u32(&self) -> u32 {
        *self as u32
    }
}

#[derive(Debug, PartialEq)]
pub struct WriteTimeout;
#[derive(Debug, PartialEq)]
pub struct UnavailableException;

#[derive(Debug, PartialEq)]
pub enum Error {
    /// Something unexpected happened. This indicates a server-side bug.
    ServerError(String),
    /// Timeout exception during a write request.
    WriteTimeout(String, WriteTimeout),
    /// Some client message triggered a protocol violation (for instance
    /// a QUERY message is sent before a STARTUP one has been sent).
    ProtocolError(String),
    /// The request cannot be processed because the coordinator node is
    /// overloaded.
    Overloaded(String),
    /// The request cannot be processed because the coordinator node knows it is not
    /// up-to-date and should not be serving requests.
    UnavailableException(String, UnavailableException),
    /// The request was a read request but the coordinator node is
    /// bootstrapping.
    IsBootstrapping(String),
}

impl Serializable for Error {
    fn to_bytes(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        match self {
            Error::ServerError(message) => {
                bytes.extend_from_slice(&ErrorCode::ServerError.to_u32().to_be_bytes());
                bytes.extend_from_slice(message.as_bytes());
            }
            Error::WriteTimeout(message, _) => {
                bytes.extend_from_slice(&ErrorCode::WriteTimeout.to_u32().to_be_bytes());
                bytes.extend_from_slice(message.as_bytes());
            }
            Error::ProtocolError(message) => {
                bytes.extend_from_slice(&ErrorCode::ProtocolError.to_u32().to_be_bytes());
                bytes.extend_from_slice(message.as_bytes());
            }
            Error::Overloaded(message) => {
                bytes.extend_from_slice(&ErrorCode::Overloaded.to_u32().to_be_bytes());
                bytes.extend_from_slice(message.as_bytes());
            }
            Error::UnavailableException(message, _) => {
                bytes.extend_from_slice(&ErrorCode::UnavailableException.to_u32().to_be_bytes());
                bytes.extend_from_slice(message.as_bytes());
            }
            Error::IsBootstrapping(message) => {
                bytes.extend_from_slice(&ErrorCode::IsBootstrapping.to_u32().to_be_bytes());
                bytes.extend_from_slice(message.as_bytes());
            }
        }

        Ok(bytes)
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Error, NativeError> {
        let mut cursor = std::io::Cursor::new(bytes);
        let mut code_bytes = [0u8; 4];
        cursor
            .read_exact(&mut code_bytes)
            .map_err(|_| NativeError::CursorError)?;

        let code = ErrorCode::from_u32(u32::from_be_bytes(code_bytes))?;

        let mut message_bytes = Vec::new();

        cursor
            .read_to_end(&mut message_bytes)
            .map_err(|_| NativeError::CursorError)?;

        let message =
            String::from_utf8(message_bytes).map_err(|_| NativeError::DeserializationError)?;

        let error = match code {
            ErrorCode::ServerError => Error::ServerError(message),
            ErrorCode::WriteTimeout => Error::WriteTimeout(message, WriteTimeout),
            ErrorCode::ProtocolError => Error::ProtocolError(message),
            ErrorCode::Overloaded => Error::Overloaded(message),
            ErrorCode::UnavailableException => {
                Error::UnavailableException(message, UnavailableException)
            }
            ErrorCode::IsBootstrapping => Error::IsBootstrapping(message),
            _ => return Err(NativeError::InvalidVariant),
        };

        Ok(error)
    }
}

#[cfg(test)]

mod tests {
    use super::*;

    #[test]
    fn test_error_to_bytes() {
        let error = Error::ServerError("Server error".to_string());
        let server_error_bytes = error.to_bytes().unwrap();
        assert_eq!(
            server_error_bytes,
            vec![
                0x00, 0x00, 0x00, 0x00, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x20, 0x65, 0x72, 0x72,
                0x6f, 0x72
            ]
        );

        let error = Error::ProtocolError("Protocol error".to_string());

        let protocol_error_bytes = error.to_bytes().unwrap();

        assert_eq!(
            protocol_error_bytes,
            vec![
                0x00, 0x00, 0x00, 0x0A, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x20, 0x65,
                0x72, 0x72, 0x6f, 0x72
            ]
        );
    }

    #[test]
    fn test_error_from_bytes() {
        let server_error_bytes = vec![
            0x00, 0x00, 0x00, 0x00, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x20, 0x65, 0x72, 0x72,
            0x6f, 0x72,
        ];

        let error = Error::from_bytes(&server_error_bytes).unwrap();

        assert_eq!(error, Error::ServerError("Server error".to_string()));

        let protocol_error_bytes = vec![
            0x00, 0x00, 0x00, 0x0A, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x20, 0x65,
            0x72, 0x72, 0x6f, 0x72,
        ];

        let error = Error::from_bytes(&protocol_error_bytes).unwrap();

        assert_eq!(error, Error::ProtocolError("Protocol error".to_string()));
    }
}
