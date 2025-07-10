use crate::{errors::CQLError, utils::is_set};

/// Struct representing the `SET` SQL clause.
///
/// The `SET` clause is used in an `UPDATE` statement to set new values to columns.
///
/// # Fields
///
/// * A vector of tuples containing the column name and the new value.
#[derive(PartialEq, Debug, Clone)]
pub struct Set(pub Vec<(String, String)>);

impl Set {
    /// Retrieves a reference to the internal vector of column-value pairs.
    ///
    /// # Returns
    /// - A reference to the vector of `(String, String)` pairs representing the column names and their respective values.
    pub fn get_pairs(&self) -> &Vec<(String, String)> {
        &self.0
    }

    /// Creates and returns a new `Set` instance from a vector of tokens.
    ///
    /// # Parameters
    /// - `tokens: Vec<&str>`:
    ///   - A vector of tokens used to build the `Set` instance.
    ///
    /// # Returns
    /// - `Ok(Set)`:
    ///   - If the tokens are valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the tokens are invalid or improperly formatted.
    ///
    /// # Notes
    /// - The tokens must be in the format: `"SET column = value"`.
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, CQLError> {
        let mut set = Vec::new();
        let mut i = 0;

        if !is_set(tokens[i]) || !tokens.contains(&"=") {
            return Err(CQLError::InvalidSyntax);
        }
        i += 1;

        while i < tokens.len() {
            if tokens[i] == "=" && i + 1 < tokens.len() {
                set.push((tokens[i - 1].to_string(), tokens[i + 1].to_string()));
            }
            i += 1;
        }

        Ok(Self(set))
    }

    /// Serializes the `Set` clause into a CQL string.
    ///
    /// # Returns
    /// - `String`:
    ///   - The serialized string representation of the `SET` clause.
    ///   - Format: `column1 = value1, column2 = value2`.
    ///   - If a value is not numeric, it will be wrapped in single quotes.
    pub fn serialize(&self) -> String {
        self.0
            .iter()
            .map(|(col, val)| {
                let formatted_value = if val.parse::<f64>().is_ok() {
                    val.clone() // Es un número, se deja sin comillas
                } else {
                    format!("'{}'", val) // No es un número, se envuelve entre comillas
                };
                format!("{} = {}", col, formatted_value)
            })
            .collect::<Vec<String>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CQLError;

    #[test]
    fn test_new_from_tokens_single_pair() {
        let tokens = vec!["SET", "age", "=", "18"];
        let set_clause = Set::new_from_tokens(tokens).unwrap();
        assert_eq!(set_clause, Set(vec![("age".to_string(), "18".to_string())]));
    }

    #[test]
    fn test_new_from_tokens_multiple_pairs() {
        let tokens = vec!["SET", "age", "=", "18", "name", "=", "John"];
        let set_clause = Set::new_from_tokens(tokens).unwrap();
        assert_eq!(
            set_clause,
            Set(vec![
                ("age".to_string(), "18".to_string()),
                ("name".to_string(), "John".to_string())
            ])
        );
    }

    #[test]
    fn test_new_from_tokens_missing_equals_sign() {
        let tokens = vec!["SET", "age", "18"];
        let result = Set::new_from_tokens(tokens);
        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_new_from_tokens_missing_set_keyword() {
        let tokens = vec!["age", "=", "18"];
        let result = Set::new_from_tokens(tokens);
        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_serialize_with_numbers() {
        let set_clause = Set(vec![("age".to_string(), "18".to_string())]);
        assert_eq!(set_clause.serialize(), "age = 18");
    }

    #[test]
    fn test_serialize_with_strings() {
        let set_clause = Set(vec![("name".to_string(), "John".to_string())]);
        assert_eq!(set_clause.serialize(), "name = 'John'");
    }

    #[test]
    fn test_serialize_mixed_types() {
        let set_clause = Set(vec![
            ("age".to_string(), "18".to_string()),
            ("name".to_string(), "John".to_string()),
        ]);
        assert_eq!(set_clause.serialize(), "age = 18, name = 'John'");
    }

    #[test]
    fn test_get_pairs() {
        let set_clause = Set(vec![
            ("age".to_string(), "18".to_string()),
            ("name".to_string(), "John".to_string()),
        ]);
        let pairs = set_clause.get_pairs();
        assert_eq!(
            pairs,
            &vec![
                ("age".to_string(), "18".to_string()),
                ("name".to_string(), "John".to_string())
            ]
        );
    }
}
