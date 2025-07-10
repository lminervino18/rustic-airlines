use crate::{
    errors::NativeError,
    types::{Bytes, CassandraString},
    Serializable,
};

///  Indicates that the server require authentication, and which authentication mechanism to use.\
///  This message will be sent following a `STARTUP` message if authentication is
/// required and must be answered by a `AUTH_RESPONSE` message from the client.\
/// The exchange ends when the server sends an `AUTH_SUCCESS` message or an `ERROR` message.
///
/// ### Fields
///
/// - `authenticator` - The name of the authentication mechanism to use.
#[derive(Debug, PartialEq, Default)]
pub struct Authenticate {
    pub authenticator: String,
}

impl Serializable for Authenticate {
    /// Converts the `Authenticate` message to bytes.
    fn to_bytes(&self) -> Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.authenticator.to_string_bytes()?);
        Ok(bytes)
    }

    /// Converts bytes to an `Authenticate` message.
    fn from_bytes(bytes: &[u8]) -> Result<Self, NativeError>
    where
        Self: Sized,
    {
        let mut cursor = std::io::Cursor::new(bytes);
        let authenticator = String::from_string_bytes(&mut cursor)?;
        Ok(Authenticate { authenticator })
    }
}

/// Answers a server `AUTH_CHALLENGE` or `AUTHENTICATE` message.\
/// The response to a `AUTH_RESPONSE` is either a follow-up `AUTH_CHALLENGE` message,
/// an `AUTH_SUCCESS` message or an `ERROR` message.
///
/// ### Fields
///
/// - `token` - The authentication token. The details of what this token contains (and when it can be null/empty, if ever) depends on the actual authenticator used.
#[derive(Debug, PartialEq, Default)]
pub struct AuthResponse {
    pub token: Bytes,
}

impl AuthResponse {
    pub fn new(token: Bytes) -> Self {
        Self { token }
    }
}

impl Serializable for AuthResponse {
    /// Converts the `AuthResponse` message to bytes.
    fn to_bytes(&self) -> Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.token.to_bytes()?);
        Ok(bytes)
    }

    /// Converts bytes to an `AuthResponse` message.
    fn from_bytes(bytes: &[u8]) -> Result<Self, NativeError>
    where
        Self: Sized,
    {
        let mut cursor = std::io::Cursor::new(bytes);
        let token = Bytes::from_bytes(&mut cursor)?;
        Ok(AuthResponse { token })
    }
}

/// Sent by the server to indicate that the authentication process has been successfully completed.\
///
/// ### Fields
///
/// - `token` - The authentication token. Holds final information from the server that the client may require to finish the authentication process. What that token contains and whether it can be null depends on the actual authenticator used.
#[derive(Debug, PartialEq, Default)]
pub struct AuthSuccess {
    pub token: Bytes,
}

impl Serializable for AuthSuccess {
    /// Converts the `AuthSuccess` message to bytes.
    fn to_bytes(&self) -> Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.token.to_bytes()?);
        Ok(bytes)
    }

    /// Converts bytes to an `AuthSuccess` message.
    fn from_bytes(bytes: &[u8]) -> Result<Self, NativeError>
    where
        Self: Sized,
    {
        let mut cursor = std::io::Cursor::new(bytes);
        let token = Bytes::from_bytes(&mut cursor)?;
        Ok(AuthSuccess { token })
    }
}

/// Sent by the server to challenge the client during the authentication process.\
/// The client must respond with an `AUTH_RESPONSE` message.
///
/// ### Fields
///
/// - `token` - The authentication token. The details of what this token contains (and when it can be null/empty, if ever) depends on the actual authenticator used.
#[derive(Debug, PartialEq)]
pub struct AuthChallenge {
    pub token: Bytes,
}

impl Serializable for AuthChallenge {
    /// Converts the `AuthChallenge` message to bytes.
    fn to_bytes(&self) -> Result<Vec<u8>, NativeError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.token.to_bytes()?);
        Ok(bytes)
    }

    /// Converts bytes to an `AuthChallenge` message.
    fn from_bytes(bytes: &[u8]) -> Result<Self, NativeError>
    where
        Self: Sized,
    {
        let mut cursor = std::io::Cursor::new(bytes);
        let token = Bytes::from_bytes(&mut cursor)?;
        Ok(AuthChallenge { token })
    }
}

mod tests {
    #[allow(unused_imports)]
    use crate::{
        messages::auth::{AuthChallenge, AuthResponse, AuthSuccess, Authenticate},
        types::Bytes,
        Serializable,
    };

    #[test]
    fn authenticate_to_bytes() {
        let authenticate = Authenticate {
            authenticator: "authenticator".to_string(),
        };
        let bytes = authenticate.to_bytes().unwrap();
        assert_eq!(
            bytes,
            vec![
                (authenticate.authenticator.len() as u16)
                    .to_be_bytes()
                    .to_vec(),
                authenticate.authenticator.as_bytes().to_vec()
            ]
            .concat()
        );
    }

    #[test]
    fn authenticate_from_bytes() {
        let authenticate = Authenticate {
            authenticator: "authenticator".to_string(),
        };

        let bytes = vec![
            (authenticate.authenticator.len() as u16)
                .to_be_bytes()
                .to_vec(),
            authenticate.authenticator.as_bytes().to_vec(),
        ]
        .concat();

        let new_authenticate = Authenticate::from_bytes(&bytes).unwrap();
        assert_eq!(new_authenticate, authenticate);
    }

    #[test]
    fn auth_response_to_bytes() {
        let auth_response = crate::messages::auth::AuthResponse {
            token: Bytes::Vec(vec![1, 2, 3, 4]),
        };

        let bytes = auth_response.to_bytes().unwrap();

        match auth_response.token {
            Bytes::Vec(vec) => {
                assert_eq!(
                    bytes,
                    vec![(vec.len() as i32).to_be_bytes().to_vec(), vec].concat()
                );
            }
            _ => panic!("Expected Bytes::Vec"),
        };
    }

    #[test]
    fn auth_response_from_bytes() {
        let auth_response = AuthResponse {
            token: Bytes::Vec(vec![1, 2, 3, 4]),
        };

        let bytes = match &auth_response.token {
            Bytes::Vec(vec) => {
                vec![(vec.len() as i32).to_be_bytes().to_vec(), vec.to_vec()].concat()
            }
            _ => panic!("Expected Bytes::Vec"),
        };

        let new_auth_response = AuthResponse::from_bytes(&bytes).unwrap();
        assert_eq!(new_auth_response, auth_response);
    }

    #[test]

    fn auth_challenge_to_bytes() {
        let auth_challenge = crate::messages::auth::AuthChallenge {
            token: Bytes::Vec(vec![1, 2, 3, 4]),
        };

        let bytes = auth_challenge.to_bytes().unwrap();

        match auth_challenge.token {
            Bytes::Vec(vec) => {
                assert_eq!(
                    bytes,
                    vec![(vec.len() as i32).to_be_bytes().to_vec(), vec].concat()
                );
            }
            _ => panic!("Expected Bytes::Vec"),
        };
    }

    #[test]
    fn auth_challenge_from_bytes() {
        let auth_challenge = AuthChallenge {
            token: Bytes::Vec(vec![1, 2, 3, 4]),
        };

        let bytes = match &auth_challenge.token {
            Bytes::Vec(vec) => {
                vec![(vec.len() as i32).to_be_bytes().to_vec(), vec.to_vec()].concat()
            }
            _ => panic!("Expected Bytes::Vec"),
        };

        let new_auth_challenge = AuthChallenge::from_bytes(&bytes).unwrap();
        assert_eq!(new_auth_challenge, auth_challenge);
    }

    #[test]
    fn auth_success_to_bytes() {
        let auth_success = crate::messages::auth::AuthSuccess {
            token: Bytes::Vec(vec![1, 2, 3, 4]),
        };

        let bytes = auth_success.to_bytes().unwrap();

        match auth_success.token {
            Bytes::Vec(vec) => {
                assert_eq!(
                    bytes,
                    vec![(vec.len() as i32).to_be_bytes().to_vec(), vec].concat()
                );
            }
            _ => panic!("Expected Bytes::Vec"),
        };
    }

    #[test]
    fn auth_success_from_bytes() {
        let auth_success = AuthSuccess {
            token: Bytes::Vec(vec![1, 2, 3, 4]),
        };

        let bytes = match &auth_success.token {
            Bytes::Vec(vec) => {
                vec![(vec.len() as i32).to_be_bytes().to_vec(), vec.to_vec()].concat()
            }
            _ => panic!("Expected Bytes::Vec"),
        };

        let new_auth_success = AuthSuccess::from_bytes(&bytes).unwrap();
        assert_eq!(new_auth_success, auth_success);
    }
}
