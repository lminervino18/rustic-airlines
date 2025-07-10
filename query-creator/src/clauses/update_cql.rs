use super::if_cql::If;
use super::set_cql::Set;
use super::where_cql::Where;
use crate::errors::CQLError;
use crate::utils::{is_set, is_update, is_where};
use crate::QueryCreator;

/// Struct representing the `UPDATE` SQL clause.
///
/// The `UPDATE` clause is used to modify records in a table.
///
/// # Fields
///
/// * `table_name` - The name of the table to be updated.
/// * `keyspace_used_name` - The keyspace name of the table to be updated.
/// * `set_clause` - The `SET` clause specifying the columns and values to update.
/// * `where_clause` - Optional `WHERE` clause for filtering records to update.
/// * `if_clause` - Optional `IF` clause specifying conditions for the update.
#[derive(PartialEq, Debug, Clone)]
pub struct Update {
    pub table_name: String,
    pub keyspace_used_name: String,
    pub set_clause: Set,
    pub where_clause: Option<Where>,
    pub if_clause: Option<If>,
}

impl Update {
    /// Creates and returns a new `Update` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of `String` tokens representing the `UPDATE` clause.
    ///
    /// The tokens must include the table name, `SET` clause, and optionally `WHERE` and `IF` clauses.
    ///
    /// # Returns
    /// * `Ok(Update)` - A successfully parsed `Update` struct.
    /// * `Err(CQLError::InvalidSyntax)` - If the tokens are invalid or improperly formatted.
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 6 {
            return Err(CQLError::InvalidSyntax);
        }
        let mut where_tokens = Vec::new();
        let mut set_tokens = Vec::new();
        let mut table_name = String::new();
        let mut keyspace_used_name = String::new();
        let mut if_tokens = Vec::new();

        let mut i = 0;

        while i < tokens.len() {
            if i == 0 && !is_update(&tokens[i]) || i == 2 && !is_set(&tokens[i]) {
                return Err(CQLError::InvalidSyntax);
            }

            if i == 0 && is_update(&tokens[i]) && i + 1 < tokens.len() {
                let full_table_name = tokens[i + 1].to_string();
                (keyspace_used_name, table_name) = if full_table_name.contains('.') {
                    let parts: Vec<&str> = full_table_name.split('.').collect();
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    (String::new(), full_table_name.clone())
                };
            }

            if i == 2 && is_set(&tokens[i]) {
                while i < tokens.len() && !is_where(&tokens[i]) {
                    set_tokens.push(tokens[i].as_str());
                    i += 1;
                }
                if i < tokens.len() && is_where(&tokens[i]) {
                    while i < tokens.len() && tokens[i] != "IF" {
                        where_tokens.push(tokens[i].as_str());
                        i += 1;
                    }
                }
                if i < tokens.len() && tokens[i] == "IF" {
                    while i < tokens.len() {
                        if_tokens.push(tokens[i].as_str());
                        i += 1;
                    }
                }
            }
            i += 1;
        }

        if table_name.is_empty() || set_tokens.is_empty() {
            return Err(CQLError::InvalidSyntax);
        }

        let set_clause = Set::new_from_tokens(set_tokens)?;

        let mut where_clause = None;

        if !where_tokens.is_empty() {
            where_clause = Some(Where::new_from_tokens(where_tokens)?);
        }

        let mut if_clause = None;

        if !if_tokens.is_empty() {
            if_clause = Some(If::new_from_tokens(if_tokens)?);
        }

        Ok(Self {
            table_name,
            keyspace_used_name,
            where_clause,
            set_clause,
            if_clause,
        })
    }

    /// Serializes the `Update` struct into a CQL string.
    ///
    /// # Returns
    /// * A `String` representation of the `UPDATE` statement.
    pub fn serialize(&self) -> String {
        let table_name_str = if !self.keyspace_used_name.is_empty() {
            format!("{}.{}", self.keyspace_used_name, self.table_name)
        } else {
            self.table_name.clone()
        };

        let mut result = format!(
            "UPDATE {} SET {}",
            table_name_str,
            self.set_clause.serialize()
        );

        if let Some(where_clause) = &self.where_clause {
            result.push_str(&format!(" WHERE {}", where_clause.serialize()));
        }

        if let Some(if_clause) = &self.if_clause {
            result.push_str(&format!(" IF {}", if_clause.serialize()));
        }

        result
    }

    /// Deserializes a CQL string into an `Update` struct.
    ///
    /// # Arguments
    ///
    /// * `serialized` - A CQL string representing the `UPDATE` clause.
    ///
    /// # Returns
    /// * `Ok(Update)` - If the string is successfully parsed.
    /// * `Err(CQLError::InvalidSyntax)` - If the string is invalid or improperly formatted.
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        let tokens: Vec<String> = QueryCreator::tokens_from_query(serialized);
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        clauses::{
            condition::Condition, if_cql::If, set_cql::Set, update_cql::Update, where_cql::Where,
        },
        errors::CQLError,
        operator::Operator,
    };

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("UPDATE")];
        let update = Update::new_from_tokens(tokens);
        assert_eq!(update, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_3_tokens() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
        ];
        let update = Update::new_from_tokens(tokens);
        assert_eq!(update, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_without_where() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
            String::from("nombre"),
            String::from("="),
            String::from("Alen"),
        ];
        let update = Update::new_from_tokens(tokens).unwrap();
        assert_eq!(
            update,
            Update {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
                where_clause: None,
                if_clause: None,
            }
        );
    }

    #[test]
    fn new_with_keyspace() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("keyspace.table"),
            String::from("SET"),
            String::from("nombre"),
            String::from("="),
            String::from("Alen"),
        ];
        let update = Update::new_from_tokens(tokens).unwrap();
        assert_eq!(
            update,
            Update {
                table_name: String::from("table"),
                keyspace_used_name: String::from("keyspace"),
                set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
                where_clause: None,
                if_clause: None,
            }
        );
    }

    #[test]
    fn new_with_where() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
            String::from("nombre"),
            String::from("="),
            String::from("Alen"),
            String::from("WHERE"),
            String::from("edad"),
            String::from("<"),
            String::from("30"),
        ];
        let update = Update::new_from_tokens(tokens).unwrap();
        assert_eq!(
            update,
            Update {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("edad"),
                        operator: Operator::Lesser,
                        value: String::from("30"),
                    },
                }),
                if_clause: None,
            }
        );
    }

    #[test]
    fn new_with_if() {
        let tokens = vec![
            String::from("UPDATE"),
            String::from("table"),
            String::from("SET"),
            String::from("nombre"),
            String::from("="),
            String::from("Alen"),
            String::from("WHERE"),
            String::from("edad"),
            String::from("="),
            String::from("30"),
            String::from("IF"),
            String::from("name"),
            String::from("="),
            String::from("john"),
        ];
        let update = Update::new_from_tokens(tokens).unwrap();
        assert_eq!(
            update,
            Update {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                set_clause: Set(vec![(String::from("nombre"), String::from("Alen"))]),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("edad"),
                        operator: Operator::Equal,
                        value: String::from("30"),
                    },
                }),
                if_clause: Some(If {
                    condition: Condition::Simple {
                        field: String::from("name"),
                        operator: Operator::Equal,
                        value: String::from("john"),
                    },
                }),
            }
        );
    }
}
