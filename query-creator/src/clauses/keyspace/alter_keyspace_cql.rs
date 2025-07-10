use crate::errors::CQLError;
use crate::QueryCreator;

#[derive(Debug, Clone)]
pub struct AlterKeyspace {
    name: String,
    replication_class: String,
    replication_factor: u32,
}

impl AlterKeyspace {
    /// Creates a new `AlterKeyspace` instance from a vector of query tokens.
    ///
    /// # Parameters
    /// - `query: Vec<String>`:
    ///   - A vector of strings representing the tokens of a CQL `ALTER KEYSPACE` query.
    ///
    /// # Returns
    /// - `Ok(AlterKeyspace)`:
    ///   - If the query is valid and can be successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    ///
    /// # Validation
    /// - The query must begin with `ALTER KEYSPACE`.
    /// - The query must include `WITH REPLICATION = { ... }`.
    /// - The replication class must be `SimpleStrategy`.
    /// - The replication factor must be a valid unsigned integer.
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        if query.len() < 10
            || query[0].to_uppercase() != "ALTER"
            || query[1].to_uppercase() != "KEYSPACE"
        {
            return Err(CQLError::InvalidSyntax);
        }

        let keyspace_name = query[2].to_string();

        if query[3].to_uppercase() != "WITH"
            || query[4].to_uppercase() != "REPLICATION"
            || query[5] != "="
        {
            return Err(CQLError::InvalidSyntax);
        }

        // Validar apertura y cierre de llaves
        if query[6] != "{" || query[query.len() - 1] != "}" {
            return Err(CQLError::InvalidSyntax);
        }

        let mut replication_class = String::new();
        let mut replication_factor = 0;

        // Iterar sobre los tokens dentro de las llaves, empezando en el índice 7 y terminando en len - 1
        let mut i = 7;
        while i < query.len() - 1 {
            let key = query[i].trim();
            let value = query[i + 1].trim();

            match key {
                "class" => replication_class = value.to_string(),
                "replication_factor" => {
                    replication_factor =
                        value.parse::<u32>().map_err(|_| CQLError::InvalidSyntax)?;
                }
                _ => return Err(CQLError::InvalidSyntax),
            }
            i += 2; // Saltar al siguiente par clave-valor
        }

        // Validar la clase de replicación
        if replication_class != "SimpleStrategy" {
            return Err(CQLError::InvalidSyntax);
        }

        Ok(Self {
            name: keyspace_name,
            replication_class,
            replication_factor,
        })
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

    /// Retrieves the name of the keyspace.
    ///
    /// # Returns
    /// - `String`:
    ///   - The name of the keyspace.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Serializes the `AlterKeyspace` structure to a CQL query string.
    ///
    /// # Purpose
    /// Converts the `AlterKeyspace` instance into its corresponding CQL `ALTER KEYSPACE` statement.
    ///
    /// # Returns
    /// - `String`:
    ///   - A string representing the `ALTER KEYSPACE` CQL query in the following format:
    ///     ```sql
    ///     ALTER KEYSPACE <keyspace_name> WITH REPLICATION = {'class': '<replication_class>', 'replication_factor': <replication_factor>};

    pub fn serialize(&self) -> String {
        format!(
            "ALTER KEYSPACE {} WITH REPLICATION = {{'class': '{}', 'replication_factor': {}}};",
            self.name, self.replication_class, self.replication_factor
        )
    }

    /// Deserializes a CQL query string into an `AlterKeyspace` structure.
    ///
    /// # Parameters
    /// - `query: &str`:
    ///   - A string representing a CQL `ALTER KEYSPACE` query.
    ///
    /// # Returns
    /// - `Ok(AlterKeyspace)`:
    ///   - If the query is valid and can be successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    pub fn deserialize(query: &str) -> Result<Self, CQLError> {
        // Divide la consulta en tokens y convierte a `Vec<String>`
        let tokens: Vec<String> = QueryCreator::tokens_from_query(query);
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alter_keyspace_valid_simple_strategy() {
        let query = vec![
            "ALTER".to_string(),
            "KEYSPACE".to_string(),
            "example".to_string(),
            "WITH".to_string(),
            "REPLICATION".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "SimpleStrategy".to_string(),
            "replication_factor".to_string(),
            "3".to_string(),
            "}".to_string(),
        ];

        let result = AlterKeyspace::new_from_tokens(query);
        assert!(result.is_ok());

        let alter_keyspace = result.unwrap();
        assert_eq!(alter_keyspace.name, "example");
        assert_eq!(alter_keyspace.replication_class, "SimpleStrategy");
        assert_eq!(alter_keyspace.replication_factor, 3);
    }

    #[test]
    fn test_alter_keyspace_invalid_replication_class() {
        let query = vec![
            "ALTER".to_string(),
            "KEYSPACE".to_string(),
            "example".to_string(),
            "WITH".to_string(),
            "REPLICATION".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "InvalidStrategy".to_string(),
            "replication_factor".to_string(),
            "3".to_string(),
            "}".to_string(),
        ];

        let result = AlterKeyspace::new_from_tokens(query);
        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_alter_keyspace_invalid_syntax_missing_with_replication() {
        let query = vec![
            "ALTER".to_string(),
            "KEYSPACE".to_string(),
            "example".to_string(),
            "WITHOUT".to_string(),
            "REPLICATION".to_string(),
            "=".to_string(),
            "{".to_string(),
            "class".to_string(),
            "SimpleStrategy".to_string(),
            "replication_factor".to_string(),
            "3".to_string(),
            "}".to_string(),
        ];

        let result = AlterKeyspace::new_from_tokens(query);
        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }
}
