use super::into_cql::Into;
use crate::errors::CQLError;
use crate::utils::{is_insert, is_values};
use crate::QueryCreator;

/// Represents the `INSERT` clause in CQL queries.
///
/// The `INSERT` clause is used to add new records to a table.
///
/// # Fields
/// - `values: Vec<String>`
///   - A vector of values to be inserted into the table.
/// - `into_clause: Into`
///   - An `Into` struct containing the table name and the list of column names.
/// - `if_not_exists: bool`
///   - Indicates whether the `IF NOT EXISTS` clause is included in the query.
///
/// # Purpose
/// This struct encapsulates the functionality for parsing, serializing, and deserializing the `INSERT` clause.
#[derive(Debug, PartialEq, Clone)]
pub struct Insert {
    pub values: Vec<String>,
    pub into_clause: Into,
    pub if_not_exists: bool,
}

impl Insert {
    /// Creates a new `Insert` instance from a vector of tokens.
    ///
    /// # Parameters
    /// - `tokens: Vec<String>`:
    ///   - A vector of strings representing the tokens of an `INSERT` query.
    ///
    /// # Returns
    /// - `Ok(Insert)`:
    ///   - If the tokens are valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the tokens are invalid or improperly formatted.
    ///
    /// # Notes
    /// - The expected token order is:
    ///   `"INSERT", "INTO", "table_name", "columns", "VALUES", "values" [IF NOT EXISTS]`.
    /// - Column names and values should be enclosed in parentheses and separated by commas.
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 6 {
            return Err(CQLError::InvalidSyntax);
        }
        let mut into_tokens: Vec<&str> = Vec::new();
        let mut values: Vec<String> = Vec::new();

        let mut i = 0;

        if is_insert(&tokens[i]) {
            i += 1;
            while !is_values(&tokens[i]) && i < tokens.len() {
                into_tokens.push(tokens[i].as_str());
                i += 1;
            }
        }
        if is_values(&tokens[i]) {
            i += 1;

            let vals: Vec<String> = tokens[i]
                .replace("\'", "")
                .split(",")
                .map(|c| c.trim().to_string())
                .collect();

            for val in vals {
                values.push(val);
            }
            i += 1;
        }

        let mut if_not_exists = false;

        if i < tokens.len()
            && tokens[i] == "IF"
            && tokens[i + 1] == "NOT"
            && tokens[i + 2] == "EXISTS"
        {
            if_not_exists = true;
        }

        if into_tokens.is_empty() || values.is_empty() {
            return Err(CQLError::InvalidSyntax);
        }

        let into_clause = Into::new_from_tokens(into_tokens)?;

        Ok(Self {
            values,
            into_clause,
            if_not_exists,
        })
    }

    /// Serializes the `Insert` instance into a CQL query string.
    ///
    /// # Returns
    /// - `String`:
    ///   - A string representation of the `INSERT` query in the following format:
    ///     ```sql
    ///     INSERT INTO [keyspace.]table_name (columns) VALUES (values) [IF NOT EXISTS];
    ///     `
    pub fn serialize(&self) -> String {
        let columns = self.into_clause.columns.join(", ");
        let values = self.values.join(", ");

        let if_not_exists = if self.if_not_exists {
            " IF NOT EXISTS"
        } else {
            ""
        };

        let table_name_str = if !self.into_clause.keyspace_used_name.is_empty() {
            format!(
                "{}.{}",
                self.into_clause.keyspace_used_name, self.into_clause.table_name
            )
        } else {
            self.into_clause.table_name.clone()
        };

        format!(
            "INSERT INTO {} ({}) VALUES ({}){}",
            table_name_str, columns, values, if_not_exists
        )
    }

    /// Deserializes a CQL query string into an `Insert` instance.
    ///
    /// # Parameters
    /// - `s: &str`:
    ///   - A string representing an `INSERT` query.
    ///
    /// # Returns
    /// - `Ok(Insert)`:
    ///   - If the query is valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    pub fn deserialize(s: &str) -> Result<Self, CQLError> {
        let tokens: Vec<String> = QueryCreator::tokens_from_query(s);
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod test {
    use crate::{clauses::into_cql, errors::CQLError, Insert};

    #[test]
    fn serialize_basic_insert() {
        let insert = Insert {
            values: vec![String::from("Alen"), String::from("25")],
            into_clause: into_cql::Into {
                table_name: String::from("keyspace.table"),
                keyspace_used_name: String::new(),
                columns: vec![String::from("name"), String::from("age")],
            },
            if_not_exists: false,
        };

        let serialized = insert.serialize();
        assert_eq!(
            serialized,
            "INSERT INTO keyspace.table (name, age) VALUES (Alen, 25)"
        );
    }

    #[test]
    fn serialize_insert_if_not_exists() {
        let insert = Insert {
            values: vec![String::from("Alen"), String::from("25")],
            into_clause: into_cql::Into {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                columns: vec![String::from("name"), String::from("age")],
            },
            if_not_exists: true,
        };

        let serialized = insert.serialize();
        assert_eq!(
            serialized,
            "INSERT INTO table (name, age) VALUES (Alen, 25) IF NOT EXISTS"
        );
    }

    #[test]
    fn deserialize_basic_insert() {
        let s = "INSERT INTO table (name, age) VALUES (Alen, 25)";
        let deserialized = Insert::deserialize(s).unwrap();

        assert_eq!(
            deserialized,
            Insert {
                values: vec![String::from("Alen"), String::from("25")],
                into_clause: into_cql::Into {
                    table_name: String::from("table"),
                    keyspace_used_name: String::new(),
                    columns: vec![String::from("name"), String::from("age")],
                },
                if_not_exists: false,
            }
        );
    }

    #[test]
    fn deserialize_insert_if_not_exists() {
        let s = "INSERT INTO table (name, age) VALUES (Alen, 25) IF NOT EXISTS";
        let deserialized = Insert::deserialize(s).unwrap();

        assert_eq!(
            deserialized,
            Insert {
                values: vec![String::from("Alen"), String::from("25")],
                into_clause: into_cql::Into {
                    table_name: String::from("table"),
                    keyspace_used_name: String::new(),
                    columns: vec![String::from("name"), String::from("age")],
                },
                if_not_exists: true,
            }
        );
    }

    #[test]
    fn deserialize_invalid_syntax_missing_values() {
        let s = "INSERT INTO table (name, age)";
        let deserialized = Insert::deserialize(s);
        assert_eq!(deserialized, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn deserialize_invalid_syntax_incorrect_format() {
        let s = "INSERT INTO table VALUES (Alen, 25)";
        let deserialized = Insert::deserialize(s);
        assert_eq!(deserialized, Err(CQLError::InvalidSyntax));
    }
}
