use super::{condition::Condition, recursive_parser::parse_condition};
use crate::{errors::CQLError, logical_operator::LogicalOperator, operator::Operator};

/// Struct representing the `WHERE` SQL clause.
///
/// The `WHERE` clause is used to filter records that match a certain condition.
///
/// # Fields
///
/// * `condition` - The condition to be evaluated.
///
#[derive(Debug, PartialEq, Clone)]
pub struct Where {
    pub condition: Condition,
}

impl Where {
    /// Creates and returns a new `Where` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of tokens that can be used to build a `Where` instance.
    ///
    /// The tokens should be in the following order: `WHERE`, `column`, `operator`, `value` in the case of a simple condition, and `WHERE`, `condition`, `AND` or `OR`, `condition` for a complex condition.
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, CQLError> {
        if tokens.len() < 4 {
            return Err(CQLError::InvalidSyntax);
        }
        let mut pos = 1;
        let condition = parse_condition(&tokens, &mut pos)?;

        Ok(Self { condition })
    }
    pub fn serialize(&self) -> String {
        self.condition.serialize()
    }
    /// Validates that the conditions in the `WHERE` clause follow the correct structure for
    /// operations like `DELETE` or `UPDATE`. Specifically:
    /// - The first conditions must involve the `partition_key` with an `=` operator.
    /// - Subsequent conditions must involve `clustering_columns` with valid comparison operators (`=`, `<`, `>`).
    ///
    /// # Arguments
    ///
    /// * `partitioner_keys` - A vector of strings containing the names of the primary keys that
    ///   must appear first in the conditions.
    /// * `clustering_columns` - A vector of strings containing the names of the clustering columns
    ///   that must appear in subsequent conditions in their defined order.
    /// * `is_delete` - A boolean indicating whether this is a `DELETE` operation.
    /// * `is_update` - A boolean indicating whether this is an `UPDATE` operation.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the conditions follow the required structure.
    /// * `Err(CQLError::InvalidCondition)` if the conditions are invalid.
    ///
    /// # Rules
    ///
    /// 1. **Partition Key Validation:**
    ///    - The first conditions in the `WHERE` clause must involve the `partition_key` with the `=` operator.
    ///    - Example: `WHERE id = 1`
    ///
    /// 2. **Clustering Column Validation:**
    ///    - Conditions after the `partition_key` must involve the `clustering_columns`.
    ///    - These conditions must use valid comparison operators (`=`, `<`, `>`).
    ///    - Conditions must respect the order of clustering columns as defined in the table schema.
    ///    - Example: `WHERE id = 1 AND age > 25 AND city = 'New York'`
    ///
    /// 3. **For `DELETE` or `UPDATE` operations:**
    ///    - The `partition_key` condition is mandatory.
    ///    - Clustering column conditions are optional but must follow the rules outlined above.
    ///
    /// # Examples
    ///
    /// ## Valid Conditions
    /// ```sql
    /// WHERE id = 1
    /// WHERE id = 1 AND age > 30
    /// WHERE id = 1 AND age = 30 AND city = 'New York'
    /// ```
    ///
    /// ## Invalid Conditions
    /// ```sql
    /// WHERE age = 30             // Missing partition key
    /// WHERE id = 1 AND city = 'New York' // Skipping clustering column `age`
    /// WHERE id > 1               // Invalid operator for partition key
    /// ```
    ///
    /// # Errors
    ///
    /// - `CQLError::InvalidCondition`:
    ///   - If the partition key condition is missing or uses an invalid operator.
    ///   - If clustering column conditions do not respect the defined order or use invalid operators.

    pub fn validate_cql_conditions(
        &self,
        partitioner_keys: &Vec<String>,
        clustering_columns: &Vec<String>,
        _delete_or_select: bool,
        update: bool,
    ) -> Result<(), CQLError> {
        let mut partitioner_key_count = 0;
        let mut partitioner_keys_verified = false;
        let mut clustering_key_count = 0;

        // Valida recursivamente las condiciones
        Self::recursive_validate_conditions(
            &self.condition,
            partitioner_keys,
            clustering_columns,
            &mut partitioner_key_count,
            &mut partitioner_keys_verified,
            &mut clustering_key_count,
            _delete_or_select,
            update,
        )?;

        // En caso de `UPDATE`, verificar que todas las clustering columns hayan sido comparadas
        if update && clustering_key_count != clustering_columns.len() {
            return Err(CQLError::InvalidCondition); // No se han comparado todas las clustering columns
        }
        Ok(())
    }

    // Método recursivo para validar las condiciones de las claves primarias y de clustering.
    #[allow(clippy::too_many_arguments)]
    fn recursive_validate_conditions(
        condition: &Condition,
        partitioner_keys: &Vec<String>,
        clustering_columns: &Vec<String>,
        partitioner_key_count: &mut usize,
        partitioner_keys_verified: &mut bool,
        clustering_key_count: &mut usize,
        _delete_or_select: bool,
        update: bool,
    ) -> Result<(), CQLError> {
        match condition {
            Condition::Simple {
                field, operator, ..
            } => {
                // Si no hemos verificado todas las partitioner keys, verificamos solo claves primarias con `=`
                if !*partitioner_keys_verified {
                    if partitioner_keys.contains(field) && *operator == Operator::Equal {
                        *partitioner_key_count += 1;
                        if *partitioner_key_count == partitioner_keys.len() {
                            *partitioner_keys_verified = true; // Todas las claves primarias han sido verificadas
                        }
                    } else {
                        return Err(CQLError::InvalidCondition); // La clave no es de partición o el operador no es `=`
                    }
                } else {
                    // Si ya verificamos las partitioner keys, ahora validamos clustering columns
                    if !clustering_columns.contains(field) {
                        return Err(CQLError::InvalidCondition); // No es una clustering column válida
                    }
                    // En caso de `UPDATE`, verificamos que todas las clustering columns se comparen
                    if update {
                        if *operator != Operator::Equal {
                            return Err(CQLError::InvalidCondition); // Las clustering columns deben compararse con `=`
                        }
                        *clustering_key_count += 1;
                    }
                }
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => {
                // Verificar que el operador sea `AND` si aún estamos verificando partitioner keys
                if !*partitioner_keys_verified && *operator != LogicalOperator::And {
                    return Err(CQLError::InvalidCondition); // Solo se permite `AND` para las partitioner keys
                }

                // Si es un `UPDATE`, después de verificar las partitioner keys, solo permitimos `AND` para clustering columns
                if update && *partitioner_keys_verified && *operator != LogicalOperator::And {
                    return Err(CQLError::InvalidCondition); // Solo se permite `AND` para las clustering columns en `UPDATE`
                }

                // Verificación recursiva en las condiciones anidadas
                if let Some(left_condition) = left.as_ref() {
                    Self::recursive_validate_conditions(
                        left_condition,
                        partitioner_keys,
                        clustering_columns,
                        partitioner_key_count,
                        partitioner_keys_verified,
                        clustering_key_count,
                        _delete_or_select,
                        update,
                    )?;
                }

                Self::recursive_validate_conditions(
                    right,
                    partitioner_keys,
                    clustering_columns,
                    partitioner_key_count,
                    partitioner_keys_verified,
                    clustering_key_count,
                    _delete_or_select,
                    update,
                )?;
            }
        }

        Ok(())
    }

    /// Retrieves the values for the `partition_key` conditions in the `WHERE` clause.
    ///
    /// # Arguments
    ///
    /// * `partitioner_keys` - A vector containing the names of the partition keys that
    ///   must match the condition.
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<String>)` - A vector containing the values associated with the `partitioner_keys`
    ///   in the condition, ordered according to the keys in `partitioner_keys`.
    /// * `Err(CQLError::InvalidColumn)` - If no conditions match the provided `partitioner_keys`.
    ///
    /// # Description
    ///
    /// - This function checks the `condition` field to identify conditions related to
    ///   the `partitioner_keys`.
    /// - For simple conditions (`field = value`), it validates if the `field` belongs
    ///   to `partitioner_keys` and has the `=` operator. If valid, the `value` is added to the result.
    /// - For complex conditions (e.g., `AND` or `OR`), it recursively evaluates the left
    ///   and right conditions to collect matching partition key values.
    ///
    /// # Behavior
    ///
    /// - **Simple Conditions**: Only `=` operators for fields in `partitioner_keys` are considered.
    /// - **Complex Conditions**: Combines results from left and right subconditions.
    /// - Returns an error if no valid conditions for `partitioner_keys` are found.
    pub fn get_value_partitioner_key_condition(
        &self,
        partitioner_keys: Vec<String>,
    ) -> Result<Vec<String>, CQLError> {
        let mut result = vec![];

        match &self.condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                // Si es una condición simple y la clave está en partitioner_keys y el operador es `=`
                if partitioner_keys.contains(field) && *operator == Operator::Equal {
                    result.push(value.clone());
                }
            }
            Condition::Complex { left, right, .. } => {
                // Recorremos la condición izquierda
                if let Some(left_condition) = left.as_ref() {
                    Self::collect_partitioner_key_values(
                        left_condition,
                        &partitioner_keys,
                        &mut result,
                    );
                }
                Self::collect_partitioner_key_values(right, &partitioner_keys, &mut result);
            }
        }

        if result.is_empty() {
            Err(CQLError::InvalidColumn)
        } else {
            Ok(result)
        }
    }

    // Método auxiliar para recorrer las condiciones y recolectar los valores de las partitioner keys.
    fn collect_partitioner_key_values(
        condition: &Condition,
        partitioner_keys: &[String],
        result: &mut Vec<String>,
    ) {
        match condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                // Si la condición simple corresponde a una partitioner key
                if partitioner_keys.contains(field) && *operator == Operator::Equal {
                    result.push(value.clone());
                }
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => {
                // Solo procesar si es un operador lógico AND
                if *operator == LogicalOperator::And {
                    if let Some(left_condition) = left.as_ref() {
                        Self::collect_partitioner_key_values(
                            left_condition,
                            partitioner_keys,
                            result,
                        );
                    }
                    Self::collect_partitioner_key_values(right, partitioner_keys, result);
                }
            }
        }
    }

    /// Collects values associated with `partition_key` conditions in the `WHERE` clause.
    ///
    /// # Arguments
    ///
    /// * `partitioner_keys` - A vector containing the names of the partition keys to match.
    /// * `condition` - A reference to the current condition being evaluated.
    /// * `result` - A mutable vector where matched values for partition keys are stored.
    ///
    /// # Behavior
    ///
    /// - **Simple Conditions**:
    ///   - If the condition is `field = value`, checks if the `field` belongs to `partitioner_keys`.
    ///   - If the `field` is in `partitioner_keys` and the operator is `=`, appends the `value` to `result`.
    ///
    /// - **Complex Conditions**:
    ///   - Evaluates both left and right subconditions recursively.
    ///   - Collects results from valid subconditions into `result`.
    ///
    /// - Ignores conditions that are not relevant to the `partitioner_keys`.
    ///
    /// # Usage
    ///
    /// This function is typically used as a helper for recursive evaluation of conditions
    /// in a query, enabling extraction of partition key values from a complex `WHERE` clause.

    pub fn get_value_clustering_column_condition(
        &self,
        clustering_columns: Vec<String>,
    ) -> Vec<Option<String>> {
        // Inicializamos el resultado con None para cada columna de clustering
        let mut result = vec![None; clustering_columns.len()];

        match &self.condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                // Si es una condición simple y la clave está en clustering_columns y el operador es `=`
                if let Some(index) = clustering_columns.iter().position(|col| col == field) {
                    if *operator == Operator::Equal {
                        result[index] = Some(value.clone());
                    }
                }
            }
            Condition::Complex { left, right, .. } => {
                // Recorremos la condición izquierda
                if let Some(left_condition) = left.as_ref() {
                    Self::collect_clustering_column_values(
                        left_condition,
                        &clustering_columns,
                        &mut result,
                    );
                }
                Self::collect_clustering_column_values(right, &clustering_columns, &mut result);
            }
        }

        result
    }

    #[allow(clippy::only_used_in_recursion)]
    fn collect_clustering_column_values(
        condition: &Condition,
        clustering_columns: &[String],
        result: &mut Vec<Option<String>>,
    ) {
        match condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                // Si la clave está en clustering_columns y el operador es `=`, actualizamos el resultado
                if let Some(index) = clustering_columns.iter().position(|col| col == field) {
                    if *operator == Operator::Equal {
                        result[index] = Some(value.clone());
                    }
                }
            }
            Condition::Complex { left, right, .. } => {
                // Recursivamente verificamos las condiciones izquierda y derecha
                if let Some(left_condition) = left.as_ref() {
                    Self::collect_clustering_column_values(
                        left_condition,
                        clustering_columns,
                        result,
                    );
                }
                Self::collect_clustering_column_values(right, clustering_columns, result);
            }
        }
    }

    /// Retrieves the value of a clustering column if there is a condition with the `=` operator.
    ///
    /// # Arguments
    ///
    /// * `clustering_column` - The name of the clustering column for which the value is to be retrieved.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(String))` - If a condition with the `=` operator is found.
    /// * `Ok(None)` - If no condition with the `=` operator exists.
    /// * `Err(CQLError)` - If a validation error occurs.

    pub fn get_value_for_clustering_column(&self, clustering_column: &str) -> Option<String> {
        Self::recursive_find_equal_condition(&self.condition, clustering_column)
    }

    /// Método recursivo para buscar condiciones `=` para una clustering column específica.
    fn recursive_find_equal_condition(
        condition: &Condition,
        clustering_column: &str,
    ) -> Option<String> {
        match condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                if field == clustering_column && *operator == Operator::Equal {
                    return Some(value.clone());
                }
                None
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => {
                // Solo procesar condiciones unidas por `AND`
                if *operator == LogicalOperator::And {
                    if let Some(left_condition) = left {
                        if let Some(value) =
                            Self::recursive_find_equal_condition(left_condition, clustering_column)
                        {
                            return Some(value);
                        }
                    }
                    Self::recursive_find_equal_condition(right, clustering_column)
                } else {
                    None // Ignorar condiciones con operadores no válidos
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{errors::CQLError, logical_operator::LogicalOperator, operator::Operator};

    #[test]
    fn test_new_from_tokens_simple_condition() {
        let tokens = vec!["WHERE", "age", ">", "18"];
        let where_clause = Where::new_from_tokens(tokens).unwrap();
        assert_eq!(
            where_clause,
            Where {
                condition: Condition::Simple {
                    field: "age".to_string(),
                    operator: Operator::Greater,
                    value: "18".to_string(),
                },
            }
        );
    }

    #[test]
    fn test_new_from_tokens_complex_condition() {
        let tokens = vec!["WHERE", "age", "=", "18", "AND", "name", "=", "John"];
        let where_clause = Where::new_from_tokens(tokens).unwrap();
        assert_eq!(
            where_clause,
            Where {
                condition: Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: "age".to_string(),
                        operator: Operator::Equal,
                        value: "18".to_string(),
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Simple {
                        field: "name".to_string(),
                        operator: Operator::Equal,
                        value: "John".to_string(),
                    }),
                }
            }
        );
    }

    // #[test]
    // fn test_validate_cql_conditions_valid_update() {
    //     let partitioner_keys = vec!["id".to_string()];
    //     let clustering_columns = vec!["age".to_string(), "name".to_string()];
    //     let condition = Condition::Complex {
    //         left: Some(Box::new(Condition::Simple {
    //             field: "id".to_string(),
    //             operator: Operator::Equal,
    //             value: "1".to_string(),
    //         })),
    //         operator: LogicalOperator::And,
    //         right: Box::new(Condition::Simple {
    //             field: "age".to_string(),
    //             operator: Operator::Equal,
    //             value: "30".to_string(),
    //         }),
    //     };

    //     let where_clause = Where { condition };
    //     assert!(where_clause
    //         .validate_cql_conditions(&partitioner_keys, &clustering_columns, false, true)
    //         .is_ok());
    // }

    #[test]
    fn test_validate_cql_conditions_invalid_update_missing_clustering_column() {
        let partitioner_keys = vec!["id".to_string()];
        let clustering_columns = vec!["age".to_string(), "name".to_string()];
        let condition = Condition::Simple {
            field: "id".to_string(),
            operator: Operator::Equal,
            value: "1".to_string(),
        };

        let where_clause = Where { condition };
        assert_eq!(
            where_clause.validate_cql_conditions(
                &partitioner_keys,
                &clustering_columns,
                false,
                true
            ),
            Err(CQLError::InvalidCondition)
        );
    }

    #[test]
    fn test_get_value_partitioner_key_condition_single_key() {
        let partitioner_keys = vec!["id".to_string()];
        let condition = Condition::Simple {
            field: "id".to_string(),
            operator: Operator::Equal,
            value: "123".to_string(),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_partitioner_key_condition(partitioner_keys);
        assert_eq!(result, Ok(vec!["123".to_string()]));
    }

    #[test]
    fn test_get_value_partitioner_key_condition_multiple_keys() {
        let partitioner_keys = vec!["id".to_string(), "key".to_string()];
        let condition = Condition::Complex {
            left: Some(Box::new(Condition::Simple {
                field: "id".to_string(),
                operator: Operator::Equal,
                value: "123".to_string(),
            })),
            operator: LogicalOperator::And,
            right: Box::new(Condition::Simple {
                field: "key".to_string(),
                operator: Operator::Equal,
                value: "abc".to_string(),
            }),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_partitioner_key_condition(partitioner_keys);
        assert_eq!(result, Ok(vec!["123".to_string(), "abc".to_string()]));
    }

    #[test]
    fn test_get_value_clustering_column_condition_single_column() {
        let clustering_columns = vec!["age".to_string()];
        let condition = Condition::Simple {
            field: "age".to_string(),
            operator: Operator::Equal,
            value: "25".to_string(),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_clustering_column_condition(clustering_columns);
        assert_eq!(result, (vec![Some("25".to_string())]));
    }

    #[test]
    fn test_get_value_clustering_column_condition_multiple_columns() {
        let clustering_columns = vec!["age".to_string(), "name".to_string()];
        let condition = Condition::Complex {
            left: Some(Box::new(Condition::Simple {
                field: "age".to_string(),
                operator: Operator::Equal,
                value: "25".to_string(),
            })),
            operator: LogicalOperator::And,
            right: Box::new(Condition::Simple {
                field: "name".to_string(),
                operator: Operator::Equal,
                value: "John".to_string(),
            }),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_clustering_column_condition(clustering_columns);
        assert_eq!(
            result,
            (vec![Some("25".to_string()), Some("John".to_string())])
        );
    }

    #[test]
    fn test_get_value_partitioner_key_condition_no_match() {
        let partitioner_keys = vec!["id".to_string()];
        let condition = Condition::Simple {
            field: "age".to_string(),
            operator: Operator::Equal,
            value: "30".to_string(),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_partitioner_key_condition(partitioner_keys);
        assert_eq!(result, Err(CQLError::InvalidColumn));
    }

    #[test]
    fn test_get_value_clustering_column_condition_no_match() {
        let clustering_columns = vec!["age".to_string()];
        let condition = Condition::Simple {
            field: "name".to_string(),
            operator: Operator::Equal,
            value: "Alice".to_string(),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_clustering_column_condition(clustering_columns);
        assert_eq!(result, vec![None]);
    }

    #[test]
    fn test_simple_condition_equal() {
        let where_clause = Where {
            condition: Condition::Simple {
                field: "value1".to_string(),
                operator: Operator::Equal,
                value: "150".to_string(),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, Some("150".to_string()));
    }

    #[test]
    fn test_simple_condition_non_equal() {
        let where_clause = Where {
            condition: Condition::Simple {
                field: "value1".to_string(),
                operator: Operator::Greater,
                value: "300".to_string(),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);
    }

    #[test]
    fn test_simple_condition_different_column() {
        let where_clause = Where {
            condition: Condition::Simple {
                field: "value2".to_string(),
                operator: Operator::Equal,
                value: "500".to_string(),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);
    }

    #[test]
    fn test_complex_condition_with_and_equal() {
        let where_clause = Where {
            condition: Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: "value1".to_string(),
                    operator: Operator::Equal,
                    value: "150".to_string(),
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Simple {
                    field: "value2".to_string(),
                    operator: Operator::Greater,
                    value: "300".to_string(),
                }),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, Some("150".to_string()));

        let result = where_clause.get_value_for_clustering_column("value2");
        assert_eq!(result, None);
    }

    #[test]
    fn test_complex_condition_with_multiple_and() {
        let where_clause = Where {
            condition: Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: "value1".to_string(),
                    operator: Operator::Equal,
                    value: "150".to_string(),
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: "value2".to_string(),
                        operator: Operator::Equal,
                        value: "500".to_string(),
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Simple {
                        field: "value3".to_string(),
                        operator: Operator::Greater,
                        value: "40".to_string(),
                    }),
                }),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, Some("150".to_string()));

        let result = where_clause.get_value_for_clustering_column("value2");
        assert_eq!(result, Some("500".to_string()));

        let result = where_clause.get_value_for_clustering_column("value3");
        assert_eq!(result, None);
    }

    #[test]
    fn test_complex_condition_with_or() {
        let where_clause = Where {
            condition: Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: "value1".to_string(),
                    operator: Operator::Equal,
                    value: "150".to_string(),
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Condition::Simple {
                    field: "value2".to_string(),
                    operator: Operator::Equal,
                    value: "500".to_string(),
                }),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);

        let result = where_clause.get_value_for_clustering_column("value2");
        assert_eq!(result, None);
    }

    #[test]
    fn test_no_conditions() {
        let where_clause = Where {
            condition: Condition::Complex {
                left: None,
                operator: LogicalOperator::And,
                right: Box::new(Condition::Simple {
                    field: "value1".to_string(),
                    operator: Operator::Greater,
                    value: "150".to_string(),
                }),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);
    }

    #[test]
    fn test_invalid_condition_for_column() {
        let where_clause = Where {
            condition: Condition::Simple {
                field: "value4".to_string(),
                operator: Operator::Equal,
                value: "999".to_string(),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);
    }
}
