use super::{condition::Condition, recursive_parser::parse_condition};
use crate::{errors::CQLError, logical_operator::LogicalOperator, operator::Operator};

/// Represents the `IF` clause in CQL queries.
///
/// The `IF` clause is used to add conditions that must be met for a query to execute.
///
/// # Fields
/// - `condition: Condition`
///   - The condition associated with the `IF` clause.
///
/// # Purpose
/// This struct encapsulates the functionality for parsing, validating, and serializing the `IF` clause.
#[derive(Debug, PartialEq, Clone)]
pub struct If {
    pub condition: Condition,
}

impl If {
    /// Creates a new `If` instance from tokens.
    ///
    /// # Parameters
    /// - `tokens: Vec<&str>`:
    ///   - A vector of string tokens representing the `IF` clause.
    ///
    /// # Returns
    /// - `Ok(If)`:
    ///   - If the tokens are valid and the condition is successfully parsed.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the tokens are invalid or improperly formatted.
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, CQLError> {
        if tokens.len() < 4 {
            return Err(CQLError::InvalidSyntax);
        }
        let mut pos = 1;
        let condition = parse_condition(&tokens, &mut pos)?;

        Ok(Self { condition })
    }

    /// Serializes the `If` instance into a string.
    ///
    /// # Returns
    /// - `String`:
    ///   - A string representation of the `IF` clause, suitable for use in a CQL query.
    pub fn serialize(&self) -> String {
        self.condition.serialize()
    }

    /// Validates that none of the conditions in the `IF` clause involve partition or clustering keys.
    ///
    /// # Parameters
    /// - `partitioner_keys: &Vec<String>`:
    ///   - A vector containing the names of the partition keys.
    /// - `clustering_columns: &Vec<String>`:
    ///   - A vector containing the names of the clustering columns.
    ///
    /// # Returns
    /// - `Ok(())`:
    ///   - If the conditions meet the requirements.
    /// - `Err(CQLError::InvalidCondition)`:
    ///   - If any condition violates the validation rules.
    pub fn validate_cql_conditions(
        self,
        partitioner_keys: &Vec<String>,
        clustering_columns: &Vec<String>,
    ) -> Result<(), CQLError> {
        Self::recursive_validate_no_partition_clustering(
            &self.condition,
            partitioner_keys,
            clustering_columns,
        )
    }

    // Recursive method to validate that conditions do not include partition or clustering keys.
    fn recursive_validate_no_partition_clustering(
        condition: &Condition,
        partitioner_keys: &Vec<String>,
        clustering_columns: &Vec<String>,
    ) -> Result<(), CQLError> {
        match condition {
            Condition::Simple { field, .. } => {
                // Check if the field is a partition or clustering key
                if partitioner_keys.contains(field) || clustering_columns.contains(field) {
                    return Err(CQLError::InvalidCondition);
                }
            }
            Condition::Complex { left, right, .. } => {
                // Validate recursively for both left and right conditions
                if let Some(left_condition) = left.as_ref() {
                    Self::recursive_validate_no_partition_clustering(
                        left_condition,
                        partitioner_keys,
                        clustering_columns,
                    )?;
                }
                Self::recursive_validate_no_partition_clustering(
                    right,
                    partitioner_keys,
                    clustering_columns,
                )?;
            }
        }
        Ok(())
    }

    /// Returns the values of the partitioner keys in the `IF` clause conditions.
    ///
    /// # Parameters
    /// - `partitioner_keys: Vec<String>`:
    ///   - A vector containing the names of the partition keys.
    ///
    /// # Returns
    /// - `Ok(Vec<String>)`:
    ///   - A vector of values for the partitioner keys.
    /// - `Err(CQLError::InvalidColumn)`:
    ///   - If no valid partitioner key conditions are found.
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
                // If it is a simple condition and the key is in partitioner_keys and the operator is `=`
                if partitioner_keys.contains(field) && *operator == Operator::Equal {
                    result.push(value.clone());
                }
            }
            Condition::Complex { left, .. } => {
                // Traverse the left condition
                if let Some(left_condition) = left.as_ref() {
                    Self::collect_partitioner_key_values(
                        left_condition,
                        &partitioner_keys,
                        &mut result,
                    );
                }
            }
        }

        if result.is_empty() {
            Err(CQLError::InvalidColumn)
        } else {
            Ok(result)
        }
    }

    // Helper method to traverse conditions and collect values of partitioner keys.
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
                // If the simple condition corresponds to a partitioner key
                if partitioner_keys.contains(field) && *operator == Operator::Equal {
                    result.push(value.clone());
                }
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => {
                // Only process if it is a logical AND operator
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::Operator;

    #[test]
    fn test_new_from_tokens_simple_condition() {
        let tokens = vec!["IF", "age", "=", "18"];
        let if_clause = If::new_from_tokens(tokens).unwrap();

        let expected_condition = Condition::Simple {
            field: "age".to_string(),
            operator: Operator::Equal,
            value: "18".to_string(),
        };

        assert_eq!(if_clause.condition, expected_condition);
    }

    #[test]
    fn test_validate_cql_conditions_no_partition_clustering() {
        let if_clause = If {
            condition: Condition::Simple {
                field: "non_key".to_string(),
                operator: Operator::Equal,
                value: "value".to_string(),
            },
        };

        let partitioner_keys = vec!["partition_key".to_string()];
        let clustering_columns = vec!["clustering_key".to_string()];

        assert!(if_clause
            .validate_cql_conditions(&partitioner_keys, &clustering_columns)
            .is_ok());
    }

    #[test]
    fn test_validate_cql_conditions_with_partition_key() {
        let if_clause = If {
            condition: Condition::Simple {
                field: "partition_key".to_string(),
                operator: Operator::Equal,
                value: "value".to_string(),
            },
        };

        let partitioner_keys = vec!["partition_key".to_string()];
        let clustering_columns = vec!["clustering_key".to_string()];

        assert_eq!(
            if_clause.validate_cql_conditions(&partitioner_keys, &clustering_columns),
            Err(CQLError::InvalidCondition)
        );
    }

    #[test]
    fn test_get_value_partitioner_key_condition_with_partition_key() {
        let if_clause = If {
            condition: Condition::Simple {
                field: "partition_key".to_string(),
                operator: Operator::Equal,
                value: "42".to_string(),
            },
        };

        let partitioner_keys = vec!["partition_key".to_string()];
        let values = if_clause.get_value_partitioner_key_condition(partitioner_keys);

        assert_eq!(values.unwrap(), vec!["42".to_string()]);
    }

    #[test]
    fn test_get_value_partitioner_key_condition_no_match() {
        let if_clause = If {
            condition: Condition::Simple {
                field: "non_key".to_string(),
                operator: Operator::Equal,
                value: "value".to_string(),
            },
        };

        let partitioner_keys = vec!["partition_key".to_string()];
        let values = if_clause.get_value_partitioner_key_condition(partitioner_keys);

        assert_eq!(values, Err(CQLError::InvalidColumn));
    }
}
