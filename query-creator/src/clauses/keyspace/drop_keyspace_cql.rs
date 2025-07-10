use crate::errors::CQLError;

#[derive(Debug, Clone)]

/// Represents a `DROP KEYSPACE` operation in CQL.
///
/// # Fields
/// - `name: String`
///   - The name of the keyspace to be dropped.
///
/// # Purpose
/// This struct models the `DROP KEYSPACE` operation in CQL, allowing for parsing,
pub struct DropKeyspace {
    name: String,
}

impl DropKeyspace {
    /// Creates a new `DropKeyspace` instance from a vector of query tokens.
    ///
    /// # Parameters
    /// - `query: Vec<String>`:
    ///   - A vector of strings representing the tokens of a CQL `DROP KEYSPACE` query.
    ///
    /// # Returns
    /// - `Ok(DropKeyspace)`:
    ///   - If the query is valid and can be successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    ///
    /// # Validation
    /// - The query must contain exactly 3 tokens.
    /// - The query must begin with `DROP KEYSPACE`.
    ///
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        if query.len() != 3
            || query[0].to_uppercase() != "DROP"
            || query[1].to_uppercase() != "KEYSPACE"
        {
            return Err(CQLError::InvalidSyntax);
        }

        let name = &query[2];

        Ok(Self {
            name: name.to_string(),
        })
    }

    /// Retrieves the name of the keyspace.
    ///
    /// # Returns
    /// - `String`:
    ///   - The name of the keyspace.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Serializes the `DropKeyspace` structure to a CQL query string.
    ///
    /// # Returns
    /// - `String`:
    ///   - A string representing the `DROP KEYSPACE` CQL query in the following format:
    ///     ```sql
    ///     DROP KEYSPACE <keyspace_name>;
    ///     ```
    ///
    pub fn serialize(&self) -> String {
        format!("DROP KEYSPACE {}", self.name)
    }

    /// Deserializes a CQL query string into a `DropKeyspace` structure.
    ///
    /// # Parameters
    /// - `query: &str`:
    ///   - A string representing a CQL `DROP KEYSPACE` query.
    ///
    /// # Returns
    /// - `Ok(DropKeyspace)`:
    ///   - If the query is valid and can be successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    ///
    pub fn deserialize(query: &str) -> Result<Self, CQLError> {
        // Divide la consulta en tokens y convierte a `Vec<String>`
        let tokens = query.split_whitespace().map(|s| s.to_string()).collect();
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CQLError;

    #[test]
    fn test_new_from_tokens_valid() {
        let query = vec![
            "DROP".to_string(),
            "KEYSPACE".to_string(),
            "example_keyspace".to_string(),
        ];
        let drop_keyspace = DropKeyspace::new_from_tokens(query).unwrap();

        assert_eq!(drop_keyspace.get_name(), "example_keyspace".to_string());
    }

    #[test]
    fn test_new_from_tokens_invalid_syntax() {
        // Caso: Tokens insuficientes
        let query = vec!["DROP".to_string(), "KEYSPACE".to_string()];
        assert!(matches!(
            DropKeyspace::new_from_tokens(query),
            Err(CQLError::InvalidSyntax)
        ));

        // Caso: Primer token incorrecto
        let query = vec![
            "DELETE".to_string(),
            "KEYSPACE".to_string(),
            "example_keyspace".to_string(),
        ];
        assert!(matches!(
            DropKeyspace::new_from_tokens(query),
            Err(CQLError::InvalidSyntax)
        ));

        // Caso: Segundo token incorrecto
        let query = vec![
            "DROP".to_string(),
            "DATABASE".to_string(),
            "example_keyspace".to_string(),
        ];
        assert!(matches!(
            DropKeyspace::new_from_tokens(query),
            Err(CQLError::InvalidSyntax)
        ));
    }

    #[test]
    fn test_serialize() {
        let drop_keyspace = DropKeyspace {
            name: "example_keyspace".to_string(),
        };
        let serialized = drop_keyspace.serialize();

        assert_eq!(serialized, "DROP KEYSPACE example_keyspace");
    }

    #[test]
    fn test_deserialize_valid() {
        let query = "DROP KEYSPACE example_keyspace";
        let drop_keyspace = DropKeyspace::deserialize(query).unwrap();

        assert_eq!(drop_keyspace.get_name(), "example_keyspace".to_string());
    }

    #[test]
    fn test_deserialize_invalid_syntax() {
        // Caso: Query incompleta
        let query = "DROP KEYSPACE";
        assert!(matches!(
            DropKeyspace::deserialize(query),
            Err(CQLError::InvalidSyntax)
        ));

        // Caso: Query incorrecta
        let query = "REMOVE KEYSPACE example_keyspace;";
        assert!(matches!(
            DropKeyspace::deserialize(query),
            Err(CQLError::InvalidSyntax)
        ));
    }
}
