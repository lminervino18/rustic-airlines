use std::{
    io::{Cursor, Read},
    vec::Vec,
};

use crate::{
    errors::NativeError,
    header::{Flags, FrameHeader, Opcode, Version},
    messages::{
        auth::{AuthChallenge, AuthResponse, AuthSuccess, Authenticate},
        error::Error,
        query::Query,
        result::result_::Result,
    },
    types::{Int, Short},
    ByteSerializable, Serializable,
};

#[derive(Debug)]
pub enum Frame {
    /// Initialize the connection.
    Startup,
    /// Indicates that the server is ready to process queries.
    Ready,
    /// Performs a CQL query.
    Query(Query),
    /// The result to a query.
    Result(Result),
    /// Indicates an error processing a request.
    Error(Error),
    /// Indicates that the server require authentication, and which authentication mechanism to use.
    Authenticate(Authenticate),
    /// Sent by the client as a response to a server authentication challenge or to initiate the authentication exchange.
    AuthResponse(AuthResponse),
    /// Sent by the server to indicate the authentication phase was successful.
    AuthSuccess(AuthSuccess),
    /// Sent by the server to challenge the client during the authentication process.
    AuthChallenge(AuthChallenge),
}

impl Serializable for Frame {
    /// 0         8        16        24        32         40
    /// +---------+---------+---------+---------+---------+
    /// | version |  flags  |      stream       | opcode  |
    /// +---------+---------+---------+---------+---------+
    /// |                length                 |         |
    /// +---------+---------+---------+---------+---------+
    /// |                                                 |
    /// .                ...  body ...                    .
    /// .                                                 .
    /// .                                                 .
    /// +-------------------------------------------------+
    fn to_bytes(&self) -> std::result::Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();

        let version = match self {
            Frame::Startup | Frame::Query(_) | Frame::AuthResponse(_) => Version::RequestV3,
            Frame::Ready
            | Frame::Result(_)
            | Frame::Error(_)
            | Frame::AuthChallenge(_)
            | Frame::AuthSuccess(_)
            | Frame::Authenticate(_) => Version::ResponseV3,
        };

        let opcode = match self {
            Frame::Startup => Opcode::Startup,
            Frame::Ready => Opcode::Ready,
            Frame::Query(_) => Opcode::Query,
            Frame::Result(_) => Opcode::Result,
            Frame::Error(_) => Opcode::Error,
            Frame::AuthChallenge(_) => Opcode::AuthChallenge,
            Frame::AuthSuccess(_) => Opcode::AuthSuccess,
            Frame::Authenticate(_) => Opcode::Authenticate,
            Frame::AuthResponse(_) => Opcode::AuthResponse,
        };

        let flags = Flags {
            compression: false,
            tracing: false,
        };

        let body_bytes = match self {
            Frame::Startup => vec![0x00, 0x00], // View 4.1.1., the startup body is a [string map] of options, but we do not use them. The [string map] requires 2 bytes for the length nonetheless, therefore, the 0x0000.
            Frame::Ready => Vec::new(),
            Frame::Query(query) => query.to_bytes()?,
            Frame::Result(result) => result.to_bytes()?,
            Frame::Error(error) => error.to_bytes()?,
            Frame::AuthChallenge(auth_challenge) => auth_challenge.to_bytes()?,
            Frame::AuthSuccess(auth_success) => auth_success.to_bytes()?,
            Frame::Authenticate(authenticate) => authenticate.to_bytes()?,
            Frame::AuthResponse(auth_response) => auth_response.to_bytes()?,
        };

        let length =
            u32::try_from(body_bytes.len()).map_err(|_| NativeError::SerializationError)?;

        let header = FrameHeader::new(version, flags, 0, opcode, length);

        let header_bytes = header.to_bytes()?;

        bytes.extend_from_slice(&header_bytes);
        bytes.extend_from_slice(&body_bytes);

        Ok(bytes)
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, NativeError> {
        let mut cursor = Cursor::new(bytes);

        // Read version (1 byte)
        let mut version_bytes = [0u8];
        cursor
            .read_exact(&mut version_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let _ = u8::from_be_bytes(version_bytes);

        // Read flags (1 byte)
        let mut flags_bytes = [0u8];
        cursor
            .read_exact(&mut flags_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let _ = Flags::from_byte(flags_bytes[0])?;

        // Read stream (2 bytes)
        let mut stream_bytes = [0u8; 2];
        cursor
            .read_exact(&mut stream_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let _ = Short::from_be_bytes(stream_bytes);

        // Read opcode (2 bytes)
        let mut opcode_bytes = [0u8];
        cursor
            .read_exact(&mut opcode_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let opcode = Opcode::from_byte(opcode_bytes[0])?;

        // Read body length (4 bytes)
        let mut length_bytes = [0u8; 4];
        cursor
            .read_exact(&mut length_bytes)
            .map_err(|_| NativeError::CursorError)?;
        let length = Int::from_be_bytes(length_bytes);

        // Read body
        let mut body = vec![
            0u8;
            length
                .try_into()
                .map_err(|_| NativeError::DeserializationError)?
        ];
        cursor
            .read_exact(&mut body)
            .map_err(|_| NativeError::CursorError)?;

        let frame = match opcode {
            Opcode::Startup => Self::Startup,
            Opcode::Ready => Self::Ready,
            Opcode::Query => Self::Query(Query::from_bytes(&body)?),
            Opcode::Error => Self::Error(Error::from_bytes(&body)?),
            Opcode::Result => Self::Result(Result::from_bytes(&body)?),
            Opcode::AuthChallenge => Self::AuthChallenge(AuthChallenge::from_bytes(&body)?),
            Opcode::AuthSuccess => Self::AuthSuccess(AuthSuccess::from_bytes(&body)?),
            Opcode::Authenticate => Self::Authenticate(Authenticate::from_bytes(&body)?),
            Opcode::AuthResponse => Self::AuthResponse(AuthResponse::from_bytes(&body)?),
            _ => return Err(NativeError::InvalidVariant),
        };

        Ok(frame)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use crate::{
        messages::{
            query::{Consistency, QueryParams},
            result::rows::{ColumnType, ColumnValue, Rows},
        },
        types::Bytes,
    };

    use super::*;

    #[test]
    fn test_frame_to_bytes_startup() {
        let frame = Frame::Startup;
        let bytes = frame.to_bytes().unwrap();

        let expected_bytes = vec![
            0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
        ];

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_frame_to_bytes_ready() {
        let frame = Frame::Ready;
        let bytes = frame.to_bytes().unwrap();

        let expected_bytes = vec![0x83, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00];

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_frame_to_bytes_query() {
        let query_string = "SELECT * FROM table WHERE id = 1".to_string();
        let query_params = QueryParams::new(Consistency::One, vec![]);
        let query = Query::new(query_string, query_params);

        let body_bytes = query.to_bytes().unwrap();
        let frame = Frame::Query(query);

        let body_len = body_bytes.len() as u8;

        let bytes = frame.to_bytes().unwrap();

        let mut expected_bytes: Vec<u8> =
            vec![0x03, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, body_len];

        expected_bytes.extend_from_slice(body_bytes.as_slice());

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_frame_to_bytes_error() {
        let error_message = "Error".to_string();
        let error = Error::ServerError(error_message);

        let body_bytes = error.to_bytes().unwrap();
        let frame = Frame::Error(error);

        let body_len = body_bytes.len() as u8;

        let bytes = frame.to_bytes().unwrap();

        let mut expected_bytes: Vec<u8> =
            vec![0x83, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, body_len];

        expected_bytes.extend_from_slice(body_bytes.as_slice());

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn bytes_to_frame_startup() {
        let bytes = Frame::Startup.to_bytes().unwrap();
        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::Startup))
    }

    #[test]
    fn bytes_to_frame_ready() {
        let bytes = Frame::Ready.to_bytes().unwrap();
        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::Ready))
    }

    #[test]
    fn bytes_to_frame_query() {
        let query_string = "SELECT * FROM table WHERE id = 1".to_string();
        let query_params = QueryParams::new(Consistency::One, vec![]);
        let query = Query::new(query_string.clone(), query_params.clone());
        let bytes = Frame::Query(query).to_bytes().unwrap();

        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::Query(_)));

        let query = match frame {
            Frame::Query(query) => query,
            _ => panic!(),
        };

        assert_eq!(query.query, query_string);
        assert_eq!(query.params, query_params);
    }

    #[test]
    fn bytes_to_frame_result() {
        let cols = vec![
            ("age".to_string(), ColumnType::Int),
            ("name".to_string(), ColumnType::Varchar),
        ];
        let rows_content = vec![
            BTreeMap::from([
                ("age".to_string(), ColumnValue::Int(1)),
                ("name".to_string(), ColumnValue::Varchar("John".to_string())),
            ]),
            BTreeMap::from([
                ("age".to_string(), ColumnValue::Int(2)),
                ("name".to_string(), ColumnValue::Varchar("Doe".to_string())),
            ]),
        ];

        let rows = Rows::new(cols, rows_content);

        let frame_bytes = Frame::Result(Result::Rows(rows)).to_bytes().unwrap();

        let frame = Frame::from_bytes(&frame_bytes).unwrap();

        assert!(matches!(frame, Frame::Result(_)));

        let result = match frame {
            Frame::Result(result) => result,
            _ => panic!(),
        };

        assert!(matches!(result, Result::Rows(_)));
    }

    #[test]
    fn bytes_to_frame_error() {
        let error_message = "Error".to_string();
        let error = Error::ServerError(error_message.clone());
        let bytes = Frame::Error(error).to_bytes().unwrap();

        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::Error(_)));

        let error = match frame {
            Frame::Error(error) => error,
            _ => panic!(),
        };

        assert_eq!(error, Error::ServerError(error_message));
    }

    #[test]
    fn bytes_to_frame_authenticate() {
        let auth = Authenticate {
            authenticator: "auth_mech".to_string(),
        };
        let bytes = Frame::Authenticate(auth).to_bytes().unwrap();

        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::Authenticate(_)));

        let auth = match frame {
            Frame::Authenticate(auth) => auth,
            _ => panic!(),
        };

        assert_eq!(auth.authenticator, "auth_mech");
    }

    #[test]
    fn bytes_to_frame_auth_response() {
        let auth_response = AuthResponse {
            token: Bytes::Vec(vec![0x01, 0x02, 0x03]),
        };
        let bytes = Frame::AuthResponse(auth_response).to_bytes().unwrap();

        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::AuthResponse(_)));

        let new_auth_response = match frame {
            Frame::AuthResponse(auth) => auth,
            _ => panic!(),
        };

        assert_eq!(
            new_auth_response,
            AuthResponse {
                token: Bytes::Vec(vec![0x01, 0x02, 0x03]),
            }
        );
    }

    #[test]
    fn bytes_to_frame_auth_success() {
        let auth_success = AuthSuccess {
            token: Bytes::Vec(vec![0x01, 0x02, 0x03]),
        };
        let bytes = Frame::AuthSuccess(auth_success).to_bytes().unwrap();

        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::AuthSuccess(_)));

        let new_auth_success = match frame {
            Frame::AuthSuccess(auth) => auth,
            _ => panic!(),
        };

        assert_eq!(
            new_auth_success,
            AuthSuccess {
                token: Bytes::Vec(vec![0x01, 0x02, 0x03]),
            }
        );
    }

    #[test]
    fn bytes_to_frame_auth_challenge() {
        let auth_challenge = AuthChallenge {
            token: Bytes::Vec(vec![0x01, 0x02, 0x03]),
        };
        let bytes = Frame::AuthChallenge(auth_challenge).to_bytes().unwrap();

        let frame = Frame::from_bytes(&bytes).unwrap();

        assert!(matches!(frame, Frame::AuthChallenge(_)));

        let new_auth_challenge = match frame {
            Frame::AuthChallenge(auth) => auth,
            _ => panic!(),
        };

        assert_eq!(
            new_auth_challenge,
            AuthChallenge {
                token: Bytes::Vec(vec![0x01, 0x02, 0x03]),
            }
        );
    }
}
