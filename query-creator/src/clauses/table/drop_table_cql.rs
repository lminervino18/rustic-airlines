use crate::errors::CQLError;

/// Represents a `DROP TABLE` operation in CQL.
///
/// # Fields
/// - `table_name: String`
///   - The name of the table being dropped.
/// - `keyspace_used_name: String`
///   - The keyspace containing the table, if specified.
///
/// # Purpose
/// This struct models the `DROP TABLE` operation in CQL, providing methods for parsing,
/// serialization, and deserialization.
#[derive(Debug, Clone)]
pub struct DropTable {
    table_name: String,
    keyspace_used_name: String,
}

impl DropTable {
    /// Creates a new `DropTable` instance from a vector of query tokens.
    ///
    /// # Parameters
    /// - `query: Vec<String>`:
    ///   - A vector of strings representing the tokens of a `DROP TABLE` query.
    ///
    /// # Returns
    /// - `Ok(DropTable)`:
    ///   - If the query is valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    ///
    /// # Validation
    /// - The query must contain exactly 3 tokens.
    /// - The query must begin with `DROP TABLE`.
    pub fn new_from_tokens(query: Vec<String>) -> Result<Self, CQLError> {
        if query.len() != 3
            || query[0].to_uppercase() != "DROP"
            || query[1].to_uppercase() != "TABLE"
        {
            return Err(CQLError::InvalidSyntax);
        }

        let full_table_name = query[2].to_string();
        let (keyspace_used_name, table_name) = if full_table_name.contains('.') {
            let parts: Vec<&str> = full_table_name.split('.').collect();
            (parts[0].to_string(), parts[1].to_string())
        } else {
            (String::new(), full_table_name.clone())
        };

        Ok(Self {
            table_name,
            keyspace_used_name,
        })
    }

    /// Retrieves the name of the table being dropped.
    ///
    /// # Returns
    /// - `String` containing the table name.
    pub fn get_table_name(&self) -> String {
        self.table_name.clone()
    }

    /// Serializes the `DropTable` instance into a CQL query string.
    ///
    /// # Returns
    /// - `String` representing the `DROP TABLE` query in the following format:
    ///     ```sql
    ///     DROP TABLE [<keyspace_name>.]<table_name>;
    ///    
    pub fn serialize(&self) -> String {
        let table_name_str = if !self.keyspace_used_name.is_empty() {
            format!("{}.{}", self.keyspace_used_name, self.table_name)
        } else {
            self.table_name.clone()
        };

        format!("DROP TABLE {}", table_name_str)
    }

    /// Deserializes a CQL query string into a `DropTable` instance.
    ///
    /// # Parameters
    /// - `serialized: &str`:
    ///   - A string representing a `DROP TABLE` query.
    ///
    /// # Returns
    /// - `Ok(DropTable)`:
    ///   - If the query is valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        let tokens: Vec<String> = serialized
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();
        Self::new_from_tokens(tokens)
    }

    /// Retrieves the keyspace containing the table, if specified.
    ///
    /// # Returns
    /// - `String` containing the keyspace name, or an empty string if not specified.
    pub fn get_used_keyspace(&self) -> String {
        self.keyspace_used_name.clone()
    }
}

// Implementación de `PartialEq` para permitir comparación de `DropTable`
impl PartialEq for DropTable {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
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
            "TABLE".to_string(),
            "test_keyspace.test_table".to_string(),
        ];
        let drop_table = DropTable::new_from_tokens(query);
        assert!(drop_table.is_ok());
        assert_eq!(drop_table.unwrap().get_table_name(), "test_table");
    }

    #[test]
    fn test_new_from_tokens_invalid_syntax() {
        // Caso donde faltan tokens
        let query = vec!["DROP".to_string(), "TABLE".to_string()];
        let drop_table = DropTable::new_from_tokens(query);
        assert_eq!(drop_table, Err(CQLError::InvalidSyntax));

        // Caso donde el primer token es incorrecto
        let query = vec![
            "DELETE".to_string(),
            "TABLE".to_string(),
            "test_table".to_string(),
        ];
        let drop_table = DropTable::new_from_tokens(query);
        assert_eq!(drop_table, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn test_serialize() {
        let drop_table = DropTable {
            table_name: "test_table".to_string(),
            keyspace_used_name: "test_keyspace".to_string(),
        };
        let serialized = drop_table.serialize();
        assert_eq!(serialized, "DROP TABLE test_keyspace.test_table");
    }

    #[test]
    fn test_deserialize_valid() {
        let serialized = "DROP TABLE test_table";
        let drop_table = DropTable::deserialize(serialized);
        assert!(drop_table.is_ok());
        assert_eq!(drop_table.unwrap().get_table_name(), "test_table");
    }

    #[test]
    fn test_deserialize_invalid_syntax() {
        // Caso donde falta el nombre de la tabla
        let serialized = "DROP TABLE";
        let drop_table = DropTable::deserialize(serialized);
        assert_eq!(drop_table, Err(CQLError::InvalidSyntax));

        // Caso donde el comando no es "DROP TABLE"
        let serialized = "DELETE TABLE test_table";
        let drop_table = DropTable::deserialize(serialized);
        assert_eq!(drop_table, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn test_partial_eq() {
        let drop_table1 = DropTable {
            table_name: "test_table".to_string(),
            keyspace_used_name: String::new(),
        };
        let drop_table2 = DropTable {
            table_name: "test_table".to_string(),
            keyspace_used_name: String::new(),
        };
        let drop_table3 = DropTable {
            table_name: "another_table".to_string(),
            keyspace_used_name: String::new(),
        };

        assert_eq!(drop_table1, drop_table2);
        assert_ne!(drop_table1, drop_table3);
    }
}
