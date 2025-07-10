use std::fmt;

/// Enum representing errors that can occur within the native protocol.
#[derive(Debug)]
pub enum NativeError {
    SerializationError,
    DeserializationError,
    NotEnoughBytes,
    CursorError,
    InvalidCode,
    InvalidVariant,
}

impl fmt::Display for NativeError {
    /// Implementation of the `fmt` method to convert the error into a readable string.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = match self {
            NativeError::SerializationError => "Serialization error occurred",
            NativeError::DeserializationError => "Deserialization error occurred",
            NativeError::NotEnoughBytes => "Not enough bytes for operation",
            NativeError::CursorError => "Cursor error encountered",
            NativeError::InvalidCode => "Invalid code encountered",
            NativeError::InvalidVariant => "Invalid variant provided",
        };
        write!(f, "{}", description)
    }
}
