use crate::{errors::CQLError, logical_operator::LogicalOperator, operator::Operator};
use std::collections::HashMap;

use super::types::column::Column;

/// Represents a condition in a `WHERE` clause of a CQL query.
///
/// # Variants
/// - `Simple`
///   - Represents a basic condition with a field, operator, and value.
/// - `Complex`
///   - Represents a composite condition with logical operators (e.g., `AND`, `OR`, `NOT`)
///     and nested conditions.
///
/// # Purpose
/// This enum provides a flexible representation for query conditions, supporting
/// both simple field-value comparisons and complex logical expressions.
#[derive(Debug, PartialEq, Clone)]
pub enum Condition {
    Simple {
        field: String,
        operator: Operator,
        value: String,
    },
    Complex {
        left: Option<Box<Condition>>, // Opcional para el caso de 'Not'
        operator: LogicalOperator,
        right: Box<Condition>,
    },
}

impl Condition {
    /// Creates a new `Simple` condition from tokens.
    ///
    /// # Parameters
    /// - `tokens: &[&str]`:
    ///   - A slice of tokens representing the condition.
    /// - `pos: &mut usize`:
    ///   - The current position in the tokens, which is updated as tokens are consumed.
    ///
    /// # Returns
    /// - `Ok(Condition)`:
    ///   - If the tokens represent a valid simple condition.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the tokens are invalid or improperly formatted.
    pub fn new_simple_from_tokens(tokens: &[&str], pos: &mut usize) -> Result<Self, CQLError> {
        if let Some(field) = tokens.get(*pos) {
            *pos += 1;

            if let Some(operator) = tokens.get(*pos) {
                *pos += 1;

                if let Some(value) = tokens.get(*pos) {
                    *pos += 1;
                    Ok(Condition::new_simple(field, operator, value)?)
                } else {
                    Err(CQLError::InvalidSyntax)
                }
            } else {
                Err(CQLError::InvalidSyntax)
            }
        } else {
            Err(CQLError::InvalidSyntax)
        }
    }

    fn new_simple(field: &str, operator: &str, value: &str) -> Result<Self, CQLError> {
        let op = match operator {
            "=" => Operator::Equal,
            ">" => Operator::Greater,
            "<" => Operator::Lesser,
            _ => return Err(CQLError::InvalidSyntax),
        };

        Ok(Condition::Simple {
            field: field.to_string(),
            operator: op,
            value: value.to_string(),
        })
    }

    /// Creates a new `Complex` condition.
    ///
    /// # Parameters
    /// - `left: Option<Condition>`:
    ///   - The left condition (optional for `NOT` operator).
    /// - `operator: LogicalOperator`:
    ///   - The logical operator (`AND`, `OR`, `NOT`).
    /// - `right: Condition`:
    ///   - The right condition.
    ///
    /// # Returns
    /// - `Condition::Complex` instance.
    pub fn new_complex(
        left: Option<Condition>,
        operator: LogicalOperator,
        right: Condition,
    ) -> Self {
        Condition::Complex {
            left: left.map(Box::new),
            operator,
            right: Box::new(right),
        }
    }

    /// Executes the condition on a given register.
    ///
    /// # Parameters
    /// - `register: &HashMap<String, String>`:
    ///   - A map representing the record to evaluate.
    /// - `columns: Vec<Column>`:
    ///   - The schema of the table being queried.
    ///
    /// # Returns
    /// - `Ok(bool)`:
    ///   - `true` if the condition evaluates to `true`.
    ///   - `false` otherwise.
    /// - `Err(CQLError)`:
    ///   - If the condition cannot be evaluated due to invalid types or missing fields.
    pub fn execute(
        &self,
        register: &HashMap<String, String>,
        columns: Vec<Column>,
    ) -> Result<bool, CQLError> {
        let op_result: Result<bool, CQLError> = match &self {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                let y = value;
                if let Some(x) = register.get(field) {
                    let col = columns
                        .iter()
                        .find(|col| &col.name == field)
                        .ok_or(CQLError::Error)?;
                    let col_type = &col.data_type;
                    if col_type.is_valid_value(value) {
                        let comparison = col_type.compare(x, y, operator)?;
                        return Ok(comparison);
                    } else {
                        return Err(CQLError::InvalidSyntax);
                    }
                } else {
                    Err(CQLError::Error)
                }
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => match operator {
                LogicalOperator::Not => {
                    let result = right.execute(register, columns)?;
                    Ok(!result)
                }
                LogicalOperator::Or => {
                    if let Some(left) = left {
                        let left_result = left.execute(register, columns.clone())?;
                        let right_result = right.execute(register, columns.clone())?;
                        Ok(left_result || right_result)
                    } else {
                        Err(CQLError::Error)
                    }
                }
                LogicalOperator::And => {
                    if let Some(left) = left {
                        let left_result = left.execute(register, columns.clone())?;
                        let right_result = right.execute(register, columns.clone())?;
                        Ok(left_result && right_result)
                    } else {
                        Err(CQLError::Error)
                    }
                }
            },
        };
        op_result
    }

    /// Serializes the condition into a string.
    ///
    /// # Returns
    /// - `String`:
    ///   - A string representation of the condition, suitable for use in a CQL query.
    pub fn serialize(&self) -> String {
        match self {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                format!("{} {} {}", field, operator.serialize(), value)
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => match operator {
                LogicalOperator::Not => format!("{} {}", operator.serialize(), right.serialize()),
                _ => format!(
                    "{} {} {}",
                    left.as_ref().unwrap().serialize(),
                    operator.serialize(),
                    right.serialize()
                ),
            },
        }
    }

    /// Deserializes a string into a `Condition`.
    ///
    /// # Parameters
    /// - `serialized: &str`:
    ///   - A string representing the condition.
    ///
    /// # Returns
    /// - `Ok(Condition)`:
    ///   - If the string is successfully parsed into a valid condition.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the string is invalid or improperly formatted.
    pub fn deserialize(serialized: &str) -> Result<Self, CQLError> {
        let tokens: Vec<&str> = serialized.split_whitespace().collect();
        Self::parse_tokens(&tokens, 0, tokens.len())
    }

    /// Parses tokens to create a `Condition`.
    ///
    /// # Parameters
    /// - `tokens: &[&str]`:
    ///   - A slice of tokens representing the condition.
    /// - `start: usize`:
    ///   - The starting position in the token slice.
    /// - `end: usize`:
    ///   - The ending position in the token slice.
    ///
    /// # Returns
    /// - `Ok(Condition)`:
    ///   - If the tokens represent a valid condition.
    /// - `Err(CQLError::InvalidSyntax)`:
    ///   - If the tokens are invalid or improperly formatted.
    fn parse_tokens(tokens: &[&str], mut start: usize, end: usize) -> Result<Self, CQLError> {
        // Si solo tiene 3 tokens, es una condición simple (e.g., `field = value`)
        if end - start == 3 {
            return Self::new_simple_from_tokens(tokens, &mut start);
        }

        // Si contiene un operador lógico en el centro, entonces es una condición compleja
        let mut i = start;
        while i < end {
            match tokens[i] {
                "AND" | "OR" | "NOT" => {
                    let operator = LogicalOperator::deserialize(tokens[i])?;
                    if operator == LogicalOperator::Not {
                        let right = Self::parse_tokens(tokens, i + 1, end)?;
                        return Ok(Condition::Complex {
                            left: None,
                            operator,
                            right: Box::new(right),
                        });
                    } else {
                        let left = Self::parse_tokens(tokens, start, i)?;
                        let right = Self::parse_tokens(tokens, i + 1, end)?;
                        return Ok(Condition::Complex {
                            left: Some(Box::new(left)),
                            operator,
                            right: Box::new(right),
                        });
                    }
                }
                _ => i += 1,
            }
        }
        Err(CQLError::InvalidSyntax)
    }
}

#[cfg(test)]
mod tests {
    use super::Condition;
    use crate::clauses::{
        condition::{LogicalOperator, Operator},
        types::{column::Column, datatype::DataType},
    };
    use std::collections::HashMap;

    #[test]
    fn create_simple() {
        let condition = Condition::new_simple("age", ">", "18").unwrap();
        assert_eq!(
            condition,
            Condition::Simple {
                field: String::from("age"),
                operator: Operator::Greater,
                value: String::from("18")
            }
        )
    }

    #[test]
    fn create_simple_from_tokens() {
        let tokens = vec!["age", ">", "18"];
        let mut pos = 0;
        let condition = Condition::new_simple_from_tokens(&tokens, &mut pos).unwrap();

        assert_eq!(
            condition,
            Condition::Simple {
                field: String::from("age"),
                operator: Operator::Greater,
                value: String::from("18")
            }
        )
    }

    #[test]
    fn create_complex_with_left() {
        let left = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("18"),
        };

        let right = Condition::Simple {
            field: String::from("city"),
            operator: Operator::Equal,
            value: String::from("Gaiman"),
        };

        let complex = Condition::new_complex(Some(left), LogicalOperator::And, right);

        assert_eq!(
            complex,
            Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("age"),
                    operator: Operator::Greater,
                    value: String::from("18"),
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman"),
                })
            }
        )
    }

    #[test]
    fn create_complex_without_left() {
        let right = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Alen"),
        };

        let complex = Condition::new_complex(None, LogicalOperator::Not, right);

        assert_eq!(
            complex,
            Condition::Complex {
                left: None,
                operator: LogicalOperator::Not,
                right: Box::new(Condition::Simple {
                    field: String::from("name"),
                    operator: Operator::Equal,
                    value: String::from("Alen"),
                })
            }
        )
    }

    #[test]
    fn execute_simple() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));

        let condition_true = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("18"),
        };

        let condition_false = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("40"),
        };

        let columns: Vec<Column> = vec![
            Column::new("name", DataType::String, false, false),
            Column::new("lastname", DataType::String, false, false),
            Column::new("age", DataType::Int, false, false),
        ];

        let result_true = condition_true.execute(&register, columns.clone()).unwrap();
        let result_false = condition_false.execute(&register, columns.clone()).unwrap();

        assert_eq!(result_true, true);

        assert_eq!(result_false, false);
    }

    #[test]
    fn execute_and() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));

        let left = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("18"),
        };
        let right = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Alen"),
        };

        let condition = Condition::Complex {
            left: Some(Box::new(left)),
            operator: LogicalOperator::And,
            right: Box::new(right),
        };

        let columns: Vec<Column> = vec![
            Column::new("name", DataType::String, false, false),
            Column::new("lastname", DataType::String, false, false),
            Column::new("age", DataType::Int, false, false),
        ];

        let result = condition.execute(&register, columns).unwrap();

        assert_eq!(result, true)
    }

    #[test]
    fn execute_or() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));

        let left = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("40"),
        };
        let right = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Emily"),
        };

        let condition = Condition::Complex {
            left: Some(Box::new(left)),
            operator: LogicalOperator::Or,
            right: Box::new(right),
        };

        let columns: Vec<Column> = vec![
            Column::new("name", DataType::String, false, false),
            Column::new("lastname", DataType::String, false, false),
            Column::new("age", DataType::Int, false, false),
        ];

        let result = condition.execute(&register, columns).unwrap();

        assert_eq!(result, false)
    }

    #[test]
    fn execute_not() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));

        let right = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Emily"),
        };

        let condition = Condition::Complex {
            left: None,
            operator: LogicalOperator::Not,
            right: Box::new(right),
        };

        let columns: Vec<Column> = vec![
            Column::new("name", DataType::String, false, false),
            Column::new("lastname", DataType::String, false, false),
            Column::new("age", DataType::Int, false, false),
        ];
        let result = condition.execute(&register, columns).unwrap();

        assert_eq!(result, true)
    }

    #[test]
    fn execute_and_or() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));
        register.insert(String::from("city"), String::from("Gaiman"));

        let left = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("40"),
        };
        let right1 = Condition::Simple {
            field: String::from("name"),
            operator: Operator::Equal,
            value: String::from("Alen"),
        };

        let or = Condition::Complex {
            left: Some(Box::new(left)),
            operator: LogicalOperator::Or,
            right: Box::new(right1),
        };

        let right2 = Condition::Simple {
            field: String::from("city"),
            operator: Operator::Equal,
            value: String::from("Trelew"),
        };

        let and = Condition::Complex {
            left: Some(Box::new(or)),
            operator: LogicalOperator::And,
            right: Box::new(right2),
        };

        let columns: Vec<Column> = vec![
            Column::new("name", DataType::String, false, false),
            Column::new("lastname", DataType::String, false, false),
            Column::new("age", DataType::Int, false, false),
            Column::new("city", DataType::String, false, false),
        ];

        let result = and.execute(&register, columns).unwrap();

        assert_eq!(result, false)
    }

    #[test]
    fn execute_not_and() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));
        register.insert(String::from("city"), String::from("Gaiman"));

        let right1 = Condition::Simple {
            field: String::from("age"),
            operator: Operator::Greater,
            value: String::from("40"),
        };

        let not = Condition::Complex {
            left: None,
            operator: LogicalOperator::Not,
            right: Box::new(right1),
        };

        let right2 = Condition::Simple {
            field: String::from("city"),
            operator: Operator::Equal,
            value: String::from("Gaiman"),
        };

        let and = Condition::Complex {
            left: Some(Box::new(not)),
            operator: LogicalOperator::And,
            right: Box::new(right2),
        };

        let columns: Vec<Column> = vec![
            Column::new("name", DataType::String, false, false),
            Column::new("lastname", DataType::String, false, false),
            Column::new("age", DataType::Int, false, false),
            Column::new("city", DataType::String, false, false),
        ];

        let result = and.execute(&register, columns).unwrap();

        assert_eq!(result, true)
    }

    #[test]
    fn execute_not_and_or_with_paren() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));
        register.insert(String::from("city"), String::from("Gaiman"));

        // NOT (city = Gaiman AND (age > 18 OR lastname = Davies))

        let condition = Condition::Complex {
            left: None,
            operator: LogicalOperator::Not,
            right: Box::new(Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman"),
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: String::from("age"),
                        operator: Operator::Greater,
                        value: String::from("18"),
                    })),
                    operator: LogicalOperator::Or,
                    right: Box::new(Condition::Simple {
                        field: String::from("lastname"),
                        operator: Operator::Equal,
                        value: String::from("Davies"),
                    }),
                }),
            }),
        };

        let columns: Vec<Column> = vec![
            Column::new("name", DataType::String, false, false),
            Column::new("lastname", DataType::String, false, false),
            Column::new("age", DataType::Int, false, false),
            Column::new("city", DataType::String, false, false),
        ];

        let result = condition.execute(&register, columns).unwrap();

        assert_eq!(result, false)
    }

    #[test]

    fn execute_and_or_with_paren2() {
        let mut register = HashMap::new();
        register.insert(String::from("name"), String::from("Alen"));
        register.insert(String::from("lastname"), String::from("Davies"));
        register.insert(String::from("age"), String::from("24"));
        register.insert(String::from("city"), String::from("Gaiman"));

        // city = Gaiman AND (age > 30 OR lastname = Davies)

        let condition = Condition::Complex {
            left: Some(Box::new(Condition::Simple {
                field: String::from("city"),
                operator: Operator::Equal,
                value: String::from("Gaiman"),
            })),
            operator: LogicalOperator::And,
            right: Box::new(Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("age"),
                    operator: Operator::Greater,
                    value: String::from("30"),
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Condition::Simple {
                    field: String::from("lastname"),
                    operator: Operator::Equal,
                    value: String::from("Davies"),
                }),
            }),
        };

        let columns: Vec<Column> = vec![
            Column::new("name", DataType::String, false, false),
            Column::new("lastname", DataType::String, false, false),
            Column::new("age", DataType::Int, false, false),
            Column::new("city", DataType::String, false, false),
        ];

        let result = condition.execute(&register, columns).unwrap();

        assert_eq!(result, true);
    }
}
