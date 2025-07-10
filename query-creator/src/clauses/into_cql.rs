use crate::{errors::CQLError, utils::is_into};

/// Represents the `INTO` clause in CQL `INSERT` statements.
///
/// The `INTO` clause specifies the target table and the columns into which data will be inserted.
///
/// # Fields
/// - `table_name: String`
///   - The name of the table where data will be inserted.
/// - `keyspace_used_name: String`
///   - The keyspace containing the table, if specified.
/// - `columns: Vec<String>`
///   - A vector of column names into which data will be inserted.
///
/// # Purpose
/// This struct is used to parse and represent the `INTO` clause in `INSERT` queries.
#[derive(Debug, PartialEq, Clone)]
pub struct Into {
    pub table_name: String,
    pub keyspace_used_name: String,
    pub columns: Vec<String>,
}

impl Into {
    /// Creates a new `Into` instance from a vector of tokens.
    ///
    /// # Parameters
    /// - `tokens: Vec<&str>`:
    ///   - A vector of string tokens representing the `INTO` clause.
    ///
    /// # Returns
    /// - `Ok(Into)`:
    ///   - If the tokens are valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the tokens are invalid or improperly formatted.
    ///
    /// # Notes
    /// - The expected token order is:
    ///   `"INTO", "table_name", "(columns)"`.
    /// - The `columns` should be enclosed in parentheses and separated by commas.
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, CQLError> {
        if tokens.len() < 3 {
            return Err(CQLError::InvalidSyntax);
        }
        let mut i = 0;
        let table_name;
        let keyspace_used_name: String;
        let mut columns: Vec<String> = Vec::new();

        if is_into(tokens[i]) {
            i += 1;
            let full_table_name = tokens[i].to_string();
            (keyspace_used_name, table_name) = if full_table_name.contains('.') {
                let parts: Vec<&str> = full_table_name.split('.').collect();
                (parts[0].to_string(), parts[1].to_string())
            } else {
                (String::new(), full_table_name.clone())
            };
            i += 1;

            let cols: Vec<String> = tokens[i].split(",").map(|c| c.trim().to_string()).collect();

            for col in cols {
                columns.push(col);
            }

            if columns.is_empty() {
                return Err(CQLError::InvalidSyntax);
            }
        } else {
            return Err(CQLError::InvalidSyntax);
        }

        Ok(Self {
            table_name,
            keyspace_used_name,
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CQLError;

    #[test]
    fn test_new_from_tokens_valid_simple() {
        let tokens = vec!["INTO", "users", "id,name,age"];
        let into_clause = Into::new_from_tokens(tokens).unwrap();

        assert_eq!(into_clause.table_name, "users".to_string());
        assert_eq!(
            into_clause.columns,
            vec!["id".to_string(), "name".to_string(), "age".to_string()]
        );
    }

    #[test]
    fn test_new_from_tokens_valid_with_whitespace() {
        let tokens = vec!["INTO", "employees", "id, name , salary"];
        let into_clause = Into::new_from_tokens(tokens).unwrap();

        assert_eq!(into_clause.table_name, "employees".to_string());
        assert_eq!(
            into_clause.columns,
            vec!["id".to_string(), "name".to_string(), "salary".to_string()]
        );
    }

    #[test]
    fn test_new_from_tokens_missing_into_keyword() {
        let tokens = vec!["users", "id,name,age"];
        let result = Into::new_from_tokens(tokens);

        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_new_from_tokens_missing_table_name() {
        let tokens = vec!["INTO", "id,name,age"];
        let result = Into::new_from_tokens(tokens);

        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }
}
