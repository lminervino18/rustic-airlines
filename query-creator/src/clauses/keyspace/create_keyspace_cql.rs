use crate::{errors::CQLError, QueryCreator};

#[derive(Debug, Clone, Default)]
/// Represents a `CREATE KEYSPACE` operation in CQL.
///
/// # Fields
/// - `name: String`
///   - The name of the keyspace to be created.
/// - `if_not_exists_clause: bool`
///   - Indicates whether the `IF NOT EXISTS` clause is included.
/// - `replication_class: String`
///   - The replication strategy class for the keyspace (e.g., `SimpleStrategy`).
/// - `replication_factor: u32`
///   - The replication factor for the keyspace.
///
/// # Purpose
/// This struct models the `CREATE KEYSPACE` operation in CQL, enabling parsing, validation, and serialization of such operations.
pub struct CreateKeyspace {
    pub name: String,
    pub if_not_exists_clause: bool,
    pub replication_class: String, // TODO: enum?
    pub replication_factor: u32,
}

impl CreateKeyspace {
    /// Creates a new `CreateKeyspace` instance from a vector of query tokens.
    ///
    /// # Parameters
    /// - `query: Vec<String>`:
    ///   - A vector of strings representing the tokens of a CQL `CREATE KEYSPACE` query.
    ///
    /// # Returns
    /// - `Ok(CreateKeyspace)`:
    ///   - If the query is valid and can be successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    ///
    /// # Validation
    /// - The query must begin with `CREATE KEYSPACE`.
    /// - The query may optionally include `IF NOT EXISTS`.
    /// - The query must include `WITH REPLICATION = { ... }`.
    /// - The replication class must be `SimpleStrategy`.
    /// - The replication factor must be a valid unsigned integer.
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        if query.len() < 10
            || query[0].to_uppercase() != "CREATE"
            || query[1].to_uppercase() != "KEYSPACE"
        {
            return Err(CQLError::InvalidSyntax);
        }

        // Check for IF NOT EXISTS
        let mut index = 2;
        let if_not_exists_clause = if query.len() > 3
            && query[2].to_uppercase() == "IF"
            && query[3].to_uppercase() == "NOT"
            && query[4].to_uppercase() == "EXISTS"
        {
            index += 3; // Skip the "IF NOT EXISTS" part
            true
        } else {
            index += 0; // No change in index
            false
        };

        let keyspace_name = query[index].to_string();

        if query[index + 1].to_uppercase() != "WITH"
            || query[index + 2].to_uppercase() != "REPLICATION"
            || query[index + 3] != "="
        {
            return Err(CQLError::InvalidSyntax);
        }

        let mut replication_class = String::new();
        let mut replication_factor = 0;

        let mut replication_index = index + 4; // Start after "WITH REPLICATION ="
        while replication_index < query.len() {
            match query[replication_index].as_str() {
                "{" => replication_index += 1, // Skip the start of block '{'
                "class" => {
                    replication_class = query[replication_index + 1].to_string();
                    replication_index += 2;
                }
                "replication_factor" => {
                    replication_factor = query[replication_index + 1]
                        .parse::<u32>()
                        .map_err(|_| CQLError::InvalidSyntax)?;
                    replication_index += 2;
                }
                "}" => break, // End when finding '}'
                _ => replication_index += 1,
            }
        }

        if replication_class != "SimpleStrategy" {
            return Err(CQLError::InvalidSyntax);
        }

        Ok(Self {
            name: keyspace_name,
            if_not_exists_clause,
            replication_class,
            replication_factor,
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

    /// Retrieves the replication class of the keyspace.
    ///
    /// # Returns
    /// - `String`:
    ///   - The replication class (e.g., `SimpleStrategy`).
    pub fn get_replication_class(&self) -> String {
        self.replication_class.clone()
    }

    /// Retrieves the replication factor of the keyspace.
    ///
    /// # Returns
    /// - `u32`:
    ///   - The replication factor.
    pub fn get_replication_factor(&self) -> u32 {
        self.replication_factor
    }

    /// Updates the replication class of the keyspace.
    ///
    /// # Parameters
    /// - `replication_class: String`:
    ///   - The new replication class to set.
    pub fn update_replication_class(&mut self, replication_class: String) {
        self.replication_class = replication_class;
    }

    /// Updates the replication factor of the keyspace.
    ///
    /// # Parameters
    /// - `replication_factor: u32`:
    ///   - The new replication factor to set.
    pub fn update_replication_factor(&mut self, replication_factor: u32) {
        self.replication_factor = replication_factor;
    }

    /// Serializes the `CreateKeyspace` structure to a CQL query string.
    ///
    /// # Returns
    /// - `String`:
    ///   - A string representing the `CREATE KEYSPACE` CQL query in the following format:
    ///     ```sql
    ///     CREATE KEYSPACE [IF NOT EXISTS] <keyspace_name> WITH replication = {'class': '<replication_class>', 'replication_factor': <replication_factor>};
    ///     ```
    ///
    pub fn serialize(&self) -> String {
        format!(
            "CREATE KEYSPACE {}{} WITH replication = {{'class': '{}', 'replication_factor': {}}};",
            if self.if_not_exists_clause {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.replication_class,
            self.replication_factor
        )
    }

    /// Deserializes a CQL query string into a `CreateKeyspace` structure.
    ///
    /// # Parameters
    /// - `query: &str`:
    ///   - A string representing a CQL `CREATE KEYSPACE` query.
    ///
    /// # Returns
    /// - `Ok(CreateKeyspace)`:
    ///   - If the query is valid and can be successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    pub fn deserialize(query: &str) -> Result<Self, CQLError> {
        // Divide la consulta en tokens y convierte a `Vec<String>`
        let tokens = QueryCreator::tokens_from_query(query);
        Self::new_from_tokens(tokens)
    }
}

impl PartialEq for CreateKeyspace {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_keyspace_valid_simple_strategy() {
        let query = vec![
            "CREATE".to_string(),
            "KEYSPACE".to_string(),
            "example".to_string(),
            "WITH".to_string(),
            "replication".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "SimpleStrategy".to_string(),
            "replication_factor".to_string(),
            "3".to_string(),
            "}".to_string(),
        ];

        let result = CreateKeyspace::new_from_tokens(query);
        assert!(result.is_ok());

        let create_keyspace = result.unwrap();
        assert_eq!(create_keyspace.name, "example");
        assert_eq!(create_keyspace.replication_class, "SimpleStrategy");
        assert_eq!(create_keyspace.replication_factor, 3);
        assert_eq!(create_keyspace.if_not_exists_clause, false);
    }

    #[test]
    fn test_create_keyspace_invalid_replication_class() {
        let query = vec![
            "CREATE".to_string(),
            "KEYSPACE".to_string(),
            "example".to_string(),
            "WITH".to_string(),
            "replication".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "InvalidStrategy".to_string(),
            "replication_factor".to_string(),
            "3".to_string(),
            "}".to_string(),
        ];

        let result = CreateKeyspace::new_from_tokens(query);
        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_create_keyspace_invalid_replication_factor() {
        let query = vec![
            "CREATE".to_string(),
            "KEYSPACE".to_string(),
            "example".to_string(),
            "WITH".to_string(),
            "replication".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "SimpleStrategy".to_string(),
            "replication_factor".to_string(),
            "three".to_string(),
            "}".to_string(),
        ];

        let result = CreateKeyspace::new_from_tokens(query);
        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_create_keyspace_valid_if_not_exists() {
        let query = vec![
            "CREATE".to_string(),
            "KEYSPACE".to_string(),
            "IF".to_string(),
            "NOT".to_string(),
            "EXISTS".to_string(),
            "example".to_string(),
            "WITH".to_string(),
            "replication".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "SimpleStrategy".to_string(),
            "replication_factor".to_string(),
            "3".to_string(),
            "}".to_string(),
        ];

        let result = CreateKeyspace::new_from_tokens(query);
        assert!(result.is_ok());

        let create_keyspace = result.unwrap();
        assert_eq!(create_keyspace.name, "example");
        assert_eq!(create_keyspace.replication_class, "SimpleStrategy");
        assert_eq!(create_keyspace.replication_factor, 3);
        assert_eq!(create_keyspace.if_not_exists_clause, true)
    }
}
