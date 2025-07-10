use super::if_cql::If;
use super::where_cql::Where;
use crate::errors::CQLError;
use crate::utils::{is_delete, is_from, is_where};
use crate::QueryCreator;

/// Represents a `DELETE` SQL clause in CQL.
///
/// # Fields
/// - `table_name: String`
///   - The name of the table from which records will be deleted.
/// - `keyspace_used_name: String`
///   - The keyspace containing the table, if specified.
/// - `columns: Option<Vec<String>>`
///   - An optional list of column names to delete. If `None`, all columns will be considered.
/// - `where_clause: Option<Where>`
///   - An optional `WHERE` clause specifying the condition for deletion.
/// - `if_clause: Option<If>`
///   - An optional `IF` clause specifying a conditional deletion.
/// - `if_exist: bool`
///   - Indicates if the `IF EXISTS` clause is present.
///
/// # Purpose
/// This struct models the `DELETE` clause in CQL, providing methods for parsing, serialization, and deserialization.
#[derive(PartialEq, Debug, Clone)]
pub struct Delete {
    pub table_name: String,
    pub keyspace_used_name: String,
    pub columns: Option<Vec<String>>, // Agregamos un vector opcional para las columnas
    pub where_clause: Option<Where>,
    pub if_clause: Option<If>,
    pub if_exist: bool,
}

impl Delete {
    /// Creates a new `Delete` instance from tokens.
    ///
    /// # Parameters
    /// - `tokens: Vec<String>`:
    ///   - A vector of strings representing the tokens of a `DELETE` clause.
    ///
    /// # Returns
    /// - `Ok(Delete)`:
    ///   - If the tokens are valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the tokens are invalid or improperly formatted.
    ///
    /// # Notes
    /// - The tokens must follow the order:
    ///   `DELETE`, `[column(s)_optional]`, `FROM`, `table_name`, `WHERE`, `condition`, `IF`, `condition`.
    /// - The `WHERE` and `IF` clauses are optional.
    pub fn new_from_tokens(tokens: Vec<String>) -> Result<Self, CQLError> {
        if tokens.len() < 3 {
            return Err(CQLError::InvalidSyntax);
        }

        let mut i = 0;
        let mut columns = None;
        let table_name: String;
        let keyspace_used_name: String;
        let mut where_tokens: Vec<&str> = Vec::new();
        let mut if_tokens: Vec<&str> = Vec::new();

        // Verificamos que la primera palabra sea DELETE
        if !is_delete(&tokens[i]) {
            return Err(CQLError::InvalidSyntax);
        }
        i += 1;

        // Procesamos las columnas opcionales antes de la palabra clave FROM
        if i < tokens.len() && !is_from(&tokens[i]) {
            let mut column_names = Vec::new();
            while i < tokens.len() && !is_from(&tokens[i]) {
                column_names.push(tokens[i].clone());
                i += 1;
            }
            columns = Some(column_names);
        }

        // Verificamos que la palabra clave FROM esté presente y que haya un nombre de tabla después
        if i < tokens.len() && is_from(&tokens[i]) && i + 1 < tokens.len() {
            let full_table_name = &tokens[i + 1];
            (keyspace_used_name, table_name) = if full_table_name.contains('.') {
                let parts: Vec<&str> = full_table_name.split('.').collect();
                (parts[0].to_string(), parts[1].to_string())
            } else {
                (String::new(), full_table_name.clone())
            };
            i += 2;
        } else {
            return Err(CQLError::InvalidSyntax);
        }

        // Procesamos la cláusula WHERE, si está presente
        if i < tokens.len() && is_where(&tokens[i]) {
            while i < tokens.len() && tokens[i] != "IF" {
                where_tokens.push(tokens[i].as_str());
                i += 1;
            }
        }

        let where_clause = if !where_tokens.is_empty() {
            Some(Where::new_from_tokens(where_tokens)?)
        } else {
            None
        };

        // Procesamos la cláusula IF, si está presente
        if i < tokens.len() && tokens[i] == "IF" {
            while i < tokens.len() {
                if_tokens.push(tokens[i].as_str());
                i += 1;
            }
        }

        let mut if_clause = None;

        let mut if_exist = false;

        if !if_tokens.is_empty() {
            if if_tokens[1] == "EXIST" {
                if_exist = true;
            } else if if_tokens.len() > 2 {
                if_clause = Some(If::new_from_tokens(if_tokens)?);
            }
        }

        Ok(Self {
            table_name,
            keyspace_used_name,
            columns,
            where_clause,
            if_clause,
            if_exist,
        })
    }

    /// Serializes the `Delete` instance into a CQL query string.
    ///
    /// # Returns
    /// - `String`:
    ///   - A string representation of the `DELETE` clause in the following format:
    ///     ```sql
    ///     DELETE [columns] FROM [keyspace.]table_name [WHERE condition] [IF condition];
    ///     ```
    pub fn serialize(&self) -> String {
        let mut serialized = String::from("DELETE");

        // Añadimos las columnas si existen
        if let Some(columns) = &self.columns {
            serialized.push_str(&format!(" {}", columns.join(", ")));
        }

        let table_name_str = if !self.keyspace_used_name.is_empty() {
            format!("{}.{}", self.keyspace_used_name, self.table_name)
        } else {
            self.table_name.clone()
        };

        serialized.push_str(&format!(" FROM {}", table_name_str));

        if let Some(where_clause) = &self.where_clause {
            serialized.push_str(&format!(" WHERE {}", where_clause.serialize()));
        }

        if let Some(if_clause) = &self.if_clause {
            serialized.push_str(&format!(" IF {}", if_clause.serialize()));
        }

        serialized
    }

    /// Deserializes a CQL query string into a `Delete` instance.
    ///
    /// # Parameters
    /// - `serialized: &str`:
    ///   - A string representing the `DELETE` clause.
    ///
    /// # Returns
    /// - `Ok(Delete)`:
    ///   - If the query is valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        let tokens: Vec<String> = QueryCreator::tokens_from_query(serialized);
        Self::new_from_tokens(tokens)
    }
}

#[cfg(test)]
mod tests {

    use super::Delete;
    use crate::{
        clauses::{condition::Condition, if_cql::If, where_cql::Where},
        errors::CQLError,
        operator::Operator,
    };

    #[test]
    fn new_1_token() {
        let tokens = vec![String::from("DELETE")];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_2_token() {
        let tokens = vec![String::from("DELETE"), String::from("FROM")];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_without_where() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("table"),
        ];
        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                where_clause: None,
                columns: None,
                if_clause: None,
                if_exist: false,
            }
        );
    }

    #[test]
    fn new_with_keyspace() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("keyspace.table"),
        ];
        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                keyspace_used_name: String::from("keyspace"),
                where_clause: None,
                columns: None,
                if_clause: None,
                if_exist: false,
            }
        );
    }

    #[test]
    fn new_4_tokens() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
        ];
        let delete = Delete::new_from_tokens(tokens);
        assert_eq!(delete, Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn new_with_where() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("cantidad"),
            String::from(">"),
            String::from("1"),
        ];
        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("cantidad"),
                        operator: Operator::Greater,
                        value: String::from("1")
                    }
                }),
                columns: None,
                if_clause: None,
                if_exist: false,
            }
        );
    }

    #[test]
    fn new_with_columns() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("columna_a"),
            String::from("columna_b"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("cantidad"),
            String::from(">"),
            String::from("1"),
        ];
        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                columns: Some(vec![String::from("columna_a"), String::from("columna_b")]),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("cantidad"),
                        operator: Operator::Greater,
                        value: String::from("1")
                    }
                }),
                if_clause: None,
                if_exist: false,
            }
        );
    }

    #[test]
    fn new_with_if() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("columna_a"),
            String::from("columna_b"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("id"),
            String::from("="),
            String::from("1234"),
            String::from("IF"),
            String::from("user"),
            String::from("="),
            String::from("jhon"),
        ];

        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                columns: Some(vec![String::from("columna_a"), String::from("columna_b")]),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("id"),
                        operator: Operator::Equal,
                        value: String::from("1234")
                    }
                }),
                if_clause: Some(If {
                    condition: Condition::Simple {
                        field: String::from("user"),
                        operator: Operator::Equal,
                        value: String::from("jhon")
                    }
                }),
                if_exist: false,
            }
        );
    }

    #[test]
    fn new_with_if_exist() {
        let tokens = vec![
            String::from("DELETE"),
            String::from("columna_a"),
            String::from("columna_b"),
            String::from("FROM"),
            String::from("table"),
            String::from("WHERE"),
            String::from("id"),
            String::from("="),
            String::from("1234"),
            String::from("IF"),
            String::from("EXIST"),
        ];

        let delete = Delete::new_from_tokens(tokens).unwrap();
        assert_eq!(
            delete,
            Delete {
                table_name: String::from("table"),
                keyspace_used_name: String::new(),
                columns: Some(vec![String::from("columna_a"), String::from("columna_b")]),
                where_clause: Some(Where {
                    condition: Condition::Simple {
                        field: String::from("id"),
                        operator: Operator::Equal,
                        value: String::from("1234")
                    }
                }),
                if_clause: None,
                if_exist: true,
            }
        );
    }
}
