use errors::NativeError;

pub mod errors;
pub mod frame;
pub mod header;
pub mod messages;
pub mod types;

pub trait Serializable {
    fn to_bytes(&self) -> std::result::Result<Vec<u8>, NativeError>;

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, NativeError>
    where
        Self: Sized;
}

pub trait ByteSerializable {
    fn to_byte(&self) -> std::result::Result<u8, NativeError>;

    fn from_byte(byte: u8) -> std::result::Result<Self, NativeError>
    where
        Self: Sized;
}
