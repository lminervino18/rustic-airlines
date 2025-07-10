use crate::errors::CQLError;

/// Struct that represents the `USE` SQL clause.
/// The `USE` clause is used to specify the keyspace to be used in the current session.
///
/// # Fields
///
/// * `keyspace_name` - A string that contains the name of the keyspace.
///
#[derive(Debug, PartialEq, Clone)]
pub struct Use {
    pub keyspace_name: String,
}

impl Use {
    /// Creates and returns a new `Use` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of strings that contains the tokens to be parsed.
    ///
    /// The tokens should be in the following order: `USE`, `keyspace_name`.
    ///
    /// # Returns
    /// * `Ok(Use)` - A successfully parsed `Use` struct.
    /// * `Err(CQLError::InvalidSyntax)` - If the tokens are invalid or improperly formatted.
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() != 2 || tokens[0].to_uppercase() != "USE" {
            return Err(CQLError::InvalidSyntax);
        }

        Ok(Self {
            keyspace_name: tokens[1].clone(),
        })
    }

    /// Retrieves the name of the keyspace.
    ///
    /// # Returns
    /// A `String` representing the keyspace name.
    pub fn get_name(&self) -> String {
        self.keyspace_name.clone()
    }

    /// Serializes the `Use` struct into a query string representation.
    ///
    /// # Returns
    /// A `String` in the format `USE keyspace_name`
    pub fn serialize(&self) -> String {
        format!("USE {}", self.keyspace_name)
    }

    /// Deserializes a query string into a `Use` struct.
    ///
    /// # Arguments
    ///
    /// * `s` - A string query in the format `USE keyspace_name`.
    ///
    /// # Returns
    /// * `Ok(Use)` - If the string is successfully parsed.
    /// * `Err(CQLError::InvalidSyntax)` - If the string is invalid or improperly formatted.
    pub fn deserialize(s: &str) -> Result<Self, CQLError> {
        let trimmed = s.trim();

        if !trimmed.starts_with("USE ") {
            return Err(CQLError::InvalidSyntax);
        }

        let keyspace_name = trimmed[4..].trim().to_string();

        Ok(Use { keyspace_name })
    }
}

#[cfg(test)]
mod test {
    use crate::{errors::CQLError, Use};

    #[test]
    fn new_invalid_syntax() {
        let tokens = vec![String::from("USE")];
        let result = Use::new_from_tokens(tokens);
        assert_eq!(result, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_valid_syntax() {
        let tokens = vec![String::from("USE"), String::from("my_keyspace")];

        let result = Use::new_from_tokens(tokens).unwrap();
        assert_eq!(
            result,
            Use {
                keyspace_name: String::from("my_keyspace"),
            }
        );
    }

    #[test]
    fn serialize_test() {
        let use_query = Use {
            keyspace_name: String::from("my_keyspace"),
        };

        let serialized = use_query.serialize();
        assert_eq!(serialized, "USE my_keyspace");
    }

    #[test]
    fn deserialize_valid_syntax() {
        let query_string = "USE my_keyspace";
        let use_query = Use::deserialize(query_string).unwrap();

        assert_eq!(
            use_query,
            Use {
                keyspace_name: String::from("my_keyspace"),
            }
        );
    }

    #[test]
    fn deserialize_invalid_syntax() {
        let query_string = "USE";
        let result = Use::deserialize(query_string);
        assert_eq!(result, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn deserialize_invalid_format() {
        let query_string = "SELECT * FROM my_keyspace";
        let result = Use::deserialize(query_string);
        assert_eq!(result, Err(CQLError::InvalidSyntax));
    }
}
