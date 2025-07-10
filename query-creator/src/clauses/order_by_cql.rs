use crate::QueryCreator;
use crate::{
    errors::CQLError,
    utils::{is_by, is_order},
};

/// Represents the `ORDER BY` clause in CQL queries.
///
/// The `ORDER BY` clause is used to sort the results of a query in ascending (`ASC`) or descending (`DESC`) order.
///
/// # Fields
/// - `columns: Vec<String>`
///   - A list of columns by which the result set will be sorted.
/// - `order: String`
///   - The order of sorting. It can be either `"ASC"` for ascending or `"DESC"` for descending.
///
/// # Purpose
/// This struct is used to parse, represent, and serialize the `ORDER BY` clause in queries.
#[derive(Debug, PartialEq, Clone)]
pub struct OrderBy {
    pub columns: Vec<String>,
    pub order: String,
}

impl OrderBy {
    /// Creates a new `OrderBy` instance from tokens.
    ///
    /// # Parameters
    /// - `tokens: Vec<String>`:
    ///   - A vector of strings representing the `ORDER BY` clause.
    ///
    /// # Returns
    /// - `Ok(OrderBy)`:
    ///   - If the tokens are valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the tokens are invalid or improperly formatted.
    ///
    /// # Notes
    /// - The expected token order is:
    ///   `"ORDER", "BY", "columns", "[ASC|DESC]"`.
    /// - The `columns` should be comma-separated.
    /// - If the order (`ASC` or `DESC`) is not specified, it defaults to ascending (`ASC`).
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 3 {
            return Err(CQLError::InvalidSyntax);
        }

        let mut columns = Vec::new();
        let mut order = String::new();
        let mut i = 0;

        if !is_order(&tokens[i]) || !is_by(&tokens[i + 1]) {
            return Err(CQLError::InvalidSyntax);
        }

        i += 2;

        while i < tokens.len() && tokens[i] != "DESC" && tokens[i] != "ASC" {
            columns.push(tokens[i].to_string());
            i += 1;
        }

        if i < tokens.len() {
            order = tokens[i].to_string();
        }

        if order.is_empty() {
            order = "ASC".to_string();
        }

        Ok(Self { columns, order })
    }

    /// Serializes the `OrderBy` instance into a CQL query string.
    ///
    /// # Returns
    /// - `String`:
    ///   - A string representation of the `ORDER BY` clause in the following format:
    ///     ```sql
    ///     ORDER BY column1, column2 ASC|DESC
    ///     ```
    pub fn serialize(&self) -> String {
        let columns_str = self.columns.join(", ");
        if self.order.is_empty() {
            format!("ORDER BY {}", columns_str)
        } else {
            format!("ORDER BY {} {}", columns_str, self.order)
        }
    }

    /// Deserializes a string into an `OrderBy` instance.
    ///
    /// # Parameters
    /// - `query: &str`:
    ///   - A string representing the `ORDER BY` clause.
    ///
    /// # Returns
    /// - `Ok(OrderBy)`:
    ///   - If the string is valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    pub fn deserialize(&mut self, query: &str) -> Result<OrderBy, CQLError> {
        let tokens = QueryCreator::tokens_from_query(query);
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CQLError;

    #[test]
    fn test_new_from_tokens_valid_desc() {
        let tokens = vec![
            "ORDER".to_string(),
            "BY".to_string(),
            "name".to_string(),
            "DESC".to_string(),
        ];
        let order_by = OrderBy::new_from_tokens(tokens).unwrap();

        assert_eq!(
            order_by,
            OrderBy {
                columns: vec!["name".to_string()],
                order: "DESC".to_string(),
            }
        );
    }

    #[test]
    fn test_new_from_tokens_valid_asc() {
        let tokens = vec![
            "ORDER".to_string(),
            "BY".to_string(),
            "age".to_string(),
            "ASC".to_string(),
        ];
        let order_by = OrderBy::new_from_tokens(tokens).unwrap();

        assert_eq!(
            order_by,
            OrderBy {
                columns: vec!["age".to_string()],
                order: "ASC".to_string(),
            }
        );
    }

    #[test]
    fn test_new_from_tokens_multiple_columns() {
        let tokens = vec![
            "ORDER".to_string(),
            "BY".to_string(),
            "age".to_string(),
            "name".to_string(),
            "DESC".to_string(),
        ];
        let order_by = OrderBy::new_from_tokens(tokens).unwrap();

        assert_eq!(
            order_by,
            OrderBy {
                columns: vec!["age".to_string(), "name".to_string()],
                order: "DESC".to_string(),
            }
        );
    }

    #[test]
    fn test_new_from_tokens_no_order_specified() {
        let tokens = vec!["ORDER".to_string(), "BY".to_string(), "name".to_string()];
        let order_by = OrderBy::new_from_tokens(tokens).unwrap();

        assert_eq!(
            order_by,
            OrderBy {
                columns: vec!["name".to_string()],
                order: "ASC".to_string(), // Defaults to ascending order if none is specified
            }
        );
    }

    #[test]
    fn test_new_from_tokens_missing_by_keyword() {
        let tokens = vec!["ORDER".to_string(), "name".to_string(), "ASC".to_string()];
        let result = OrderBy::new_from_tokens(tokens);

        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_new_from_tokens_missing_order_keyword() {
        let tokens = vec!["BY".to_string(), "name".to_string(), "ASC".to_string()];
        let result = OrderBy::new_from_tokens(tokens);

        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }

    #[test]
    fn test_serialize_with_order() {
        let order_by = OrderBy {
            columns: vec!["age".to_string(), "name".to_string()],
            order: "DESC".to_string(),
        };

        assert_eq!(order_by.serialize(), "ORDER BY age, name DESC");
    }

    #[test]
    fn test_serialize_without_order() {
        let order_by = OrderBy {
            columns: vec!["age".to_string()],
            order: "".to_string(),
        };

        assert_eq!(order_by.serialize(), "ORDER BY age");
    }

    #[test]
    fn test_deserialize_valid_query() {
        let mut order_by = OrderBy {
            columns: vec![],
            order: "".to_string(),
        };
        let result = order_by.deserialize("ORDER BY age DESC").unwrap();

        assert_eq!(
            result,
            OrderBy {
                columns: vec!["age".to_string()],
                order: "DESC".to_string(),
            }
        );
    }

    #[test]
    fn test_deserialize_invalid_query() {
        let mut order_by = OrderBy {
            columns: vec![],
            order: "".to_string(),
        };
        let result = order_by.deserialize("ORDER age DESC");
        assert!(matches!(result, Err(CQLError::InvalidSyntax)));
    }
}
