use crate::clauses::types::alter_table_op::AlterTableOperation;
use crate::clauses::types::column::Column;
use crate::clauses::types::datatype::DataType;
use crate::errors::CQLError;
use crate::QueryCreator;
use std::cmp::PartialEq;
use std::str::FromStr;

/// Represents an `ALTER TABLE` operation in CQL.
///
/// # Fields
/// - `table_name: String`
///   - The name of the table being altered.
/// - `keyspace_used_name: String`
///   - The keyspace containing the table, if specified.
/// - `operations: Vec<AlterTableOperation>`
///   - A list of operations to be performed on the table (e.g., adding or dropping columns).
///
/// # Purpose
/// This struct models the `ALTER TABLE` operation in CQL, providing methods for parsing,
/// serialization, and deserialization.
#[derive(Debug, Clone)]
pub struct AlterTable {
    table_name: String,
    keyspace_used_name: String,
    operations: Vec<AlterTableOperation>,
}

impl AlterTable {
    /// Creates a new `AlterTable` instance.
    ///
    /// # Parameters
    /// - `table_name: String`:
    ///   - The name of the table being altered.
    /// - `keyspace_used_name: String`:
    ///   - The keyspace containing the table, if applicable.
    /// - `operations: Vec<AlterTableOperation>`:
    ///   - The operations to be performed on the table.
    ///
    /// # Returns
    /// - `AlterTable`:
    ///   - A new instance of the `AlterTable` struct.
    pub fn new(
        table_name: String,
        keyspace_used_name: String,
        operations: Vec<AlterTableOperation>,
    ) -> AlterTable {
        AlterTable {
            table_name,
            keyspace_used_name,
            operations,
        }
    }

    /// Deserializes a CQL query string into an `AlterTable` instance.
    ///
    /// # Parameters
    /// - `serialized: &str`:
    ///   - A string representing an `ALTER TABLE` query.
    ///
    /// # Returns
    /// - `Ok(AlterTable)`:
    ///   - If the query is valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the query is invalid or improperly formatted.
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        let tokens: Vec<String> = QueryCreator::tokens_from_query(serialized);
        Self::new_from_tokens(tokens)
    }

    /// Constructs an `AlterTable` instance from a vector of query tokens.
    ///
    /// # Parameters
    /// - `query: Vec<String>`:
    ///   - A vector of strings representing the tokens of an `ALTER TABLE` query.
    ///
    /// # Returns
    /// - `Ok(AlterTable)`:
    ///   - If the tokens are valid and successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the tokens are invalid or improperly formatted.
    ///
    /// # Validation
    /// - The query must begin with `ALTER TABLE`.
    /// - Operations supported include `ADD`, `DROP`, `MODIFY`, and `RENAME`.
    pub fn new_from_tokens(query: Vec<String>) -> Result<AlterTable, CQLError> {
        if query.len() < 4
            || query[0].to_uppercase() != "ALTER"
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

        let operations = &query[3..];

        let mut ops: Vec<AlterTableOperation> = Vec::new();
        let mut i = 0;

        while i < operations.len() {
            match operations[i].to_uppercase().as_str() {
                "ADD" => {
                    // Soporte para omitir "COLUMN"
                    let mut offset = 1;
                    if i + 2 < operations.len() && operations[i + 1].to_uppercase() == "COLUMN" {
                        offset = 2;
                    }

                    if i + offset + 1 >= operations.len() {
                        return Err(CQLError::InvalidSyntax);
                    }

                    let col_name = operations[i + offset].to_string();
                    let col_type = DataType::from_str(&operations[i + offset + 1])?;

                    let allows_null = if operations.len() > i + offset + 2
                        && operations[i + offset + 2].to_uppercase() == "NOT"
                    {
                        if operations.len() < i + offset + 4
                            || operations[i + offset + 3].to_uppercase() != "NULL"
                        {
                            return Err(CQLError::InvalidSyntax);
                        }
                        false
                    } else {
                        true
                    };

                    ops.push(AlterTableOperation::AddColumn(Column::new(
                        &col_name,
                        col_type,
                        false,
                        allows_null,
                    )));
                    i += offset + 2;
                }
                "DROP" => {
                    let col_name = operations[i + 1].to_string();
                    ops.push(AlterTableOperation::DropColumn(col_name));
                    i += 2;
                }
                "MODIFY" => {
                    let col_name = operations[i + 1].to_string();
                    let col_type = DataType::from_str(&operations[i + 2])?;

                    let allows_null = if operations.len() > i + 3
                        && operations[i + 3].to_uppercase() == "NOT"
                    {
                        if operations.len() < i + 5 || operations[i + 4].to_uppercase() != "NULL" {
                            return Err(CQLError::InvalidSyntax);
                        }
                        false
                    } else {
                        true
                    };

                    ops.push(AlterTableOperation::ModifyColumn(
                        col_name,
                        col_type,
                        allows_null,
                    ));
                    i += 3;
                }
                "RENAME" => {
                    let old_col_name = operations[i + 1].to_string();
                    let new_col_name = operations[i + 3].to_string();
                    ops.push(AlterTableOperation::RenameColumn(
                        old_col_name,
                        new_col_name,
                    ));
                    i += 4;
                }
                _ => return Err(CQLError::InvalidSyntax),
            }
            i += 1;
        }
        Ok(AlterTable::new(table_name, keyspace_used_name, ops))
    }

    /// Serializes an `AlterTable` instance into a CQL query string.
    ///
    /// # Returns
    /// - `String`:
    ///   - A string representing the `ALTER TABLE` query.
    pub fn serialize(&self) -> String {
        let operations_str: Vec<String> = self
            .operations
            .iter()
            .map(|op| match op {
                AlterTableOperation::AddColumn(column) => {
                    let mut op_str =
                        format!("ADD {} {}", column.name, column.data_type.to_string());
                    if !column.allows_null {
                        op_str.push_str(" NOT NULL");
                    }
                    op_str
                }
                AlterTableOperation::DropColumn(column_name) => format!("DROP {}", column_name),
                AlterTableOperation::ModifyColumn(column_name, data_type, allows_null) => {
                    let mut op_str = format!("MODIFY {} {}", column_name, data_type.to_string());
                    if !*allows_null {
                        op_str.push_str(" NOT NULL");
                    }
                    op_str
                }
                AlterTableOperation::RenameColumn(old_name, new_name) => {
                    format!("RENAME {} TO {}", old_name, new_name)
                }
            })
            .collect();

        let table_name_str = if !self.keyspace_used_name.is_empty() {
            format!("{}.{}", self.keyspace_used_name, self.table_name)
        } else {
            self.table_name.clone()
        };

        format!(
            "ALTER TABLE {} {}",
            table_name_str,
            operations_str.join(" ")
        )
    }

    pub fn get_table_name(&self) -> String {
        self.table_name.clone()
    }

    pub fn get_operations(&self) -> Vec<AlterTableOperation> {
        self.operations.clone()
    }

    pub fn get_used_keyspace(&self) -> String {
        self.keyspace_used_name.clone()
    }
}

// Implementación de PartialEq para comparar por `table_name` y `operations`
impl PartialEq for AlterTable {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clauses::types::datatype::DataType;

    #[test]
    fn test_alter_table_add_column() {
        let query = vec![
            "ALTER".to_string(),
            "TABLE".to_string(),
            "sky.airports".to_string(),
            "ADD".to_string(),
            "new_col".to_string(),
            "INT".to_string(),
        ];
        let alter_table = AlterTable::new_from_tokens(query).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(alter_table.keyspace_used_name, "sky");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::AddColumn(Column::new(
                "new_col",
                DataType::Int,
                false,
                true
            ))]
        );
    }

    #[test]
    fn test_alter_table_serialize() {
        let operations = vec![AlterTableOperation::AddColumn(Column::new(
            "new_col",
            DataType::Int,
            false,
            true,
        ))];
        let alter_table =
            AlterTable::new("airports".to_string(), String::new(), operations.clone());
        let serialized = alter_table.serialize();
        assert_eq!(serialized, "ALTER TABLE airports ADD new_col INT");
    }

    #[test]
    fn test_alter_table_deserialize() {
        let serialized = "ALTER TABLE airports ADD new_col INT";
        let alter_table = AlterTable::deserialize(serialized).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::AddColumn(Column::new(
                "new_col",
                DataType::Int,
                false,
                true
            ))]
        );
    }

    #[test]
    fn test_alter_table_equality() {
        let alter_table1 = AlterTable::new("airports".to_string(), String::new(), vec![]);
        let alter_table2 = AlterTable::new("airports".to_string(), String::new(), vec![]);
        assert_eq!(alter_table1, alter_table2);
    }

    #[test]
    fn test_alter_table_drop_column() {
        let query = vec![
            "ALTER".to_string(),
            "TABLE".to_string(),
            "airports".to_string(),
            "DROP".to_string(),
            "old_col".to_string(),
        ];
        let alter_table = AlterTable::new_from_tokens(query).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::DropColumn("old_col".to_string())]
        );
    }

    #[test]
    fn test_alter_table_modify_column() {
        let query = vec![
            "ALTER".to_string(),
            "TABLE".to_string(),
            "airports".to_string(),
            "MODIFY".to_string(),
            "new_col".to_string(),
            "STRING".to_string(),
        ];
        let alter_table = AlterTable::new_from_tokens(query).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::ModifyColumn(
                "new_col".to_string(),
                DataType::String,
                true
            )]
        );
    }

    #[test]
    fn test_alter_table_rename_column() {
        let query = vec![
            "ALTER".to_string(),
            "TABLE".to_string(),
            "airports".to_string(),
            "RENAME".to_string(),
            "old_col".to_string(),
            "TO".to_string(),
            "new_col".to_string(),
        ];
        let alter_table = AlterTable::new_from_tokens(query).unwrap();
        assert_eq!(alter_table.get_table_name(), "airports");
        assert_eq!(
            alter_table.get_operations(),
            vec![AlterTableOperation::RenameColumn(
                "old_col".to_string(),
                "new_col".to_string()
            )]
        );
    }
}
