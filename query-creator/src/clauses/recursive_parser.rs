use super::condition::Condition;
use crate::{
    errors::CQLError,
    logical_operator::LogicalOperator,
    utils::{is_and, is_left_paren, is_not, is_or, is_right_paren},
};

/// Parses a condition from a vector of tokens.
///
/// The condition can be a simple condition or a complex condition:
/// - A simple condition contains a field, an operator, and a value.
/// - A complex condition contains a left condition, a logical operator (e.g., AND, OR, NOT), and a right condition.
///
/// # Arguments
/// - `tokens: &Vec<&str>`
///   - A vector of tokens representing the condition to be parsed.
/// - `pos: &mut usize`
///   - A mutable reference to the current position in the tokens vector.
///
/// # Returns
/// - `Ok(Condition)`:
///   - The parsed `Condition` instance.
/// - `Err(CQLError)`:
///   - If the tokens cannot be parsed into a valid condition.
pub fn parse_condition(tokens: &Vec<&str>, pos: &mut usize) -> Result<Condition, CQLError> {
    let mut left = parse_or(tokens, pos)?;

    while let Some(token) = tokens.get(*pos) {
        if is_or(token) {
            *pos += 1;
            let right = parse_or(tokens, pos)?;
            left = Condition::new_complex(Some(left), LogicalOperator::Or, right);
        } else {
            break;
        }
    }
    Ok(left)
}

fn parse_or(tokens: &Vec<&str>, pos: &mut usize) -> Result<Condition, CQLError> {
    let mut left = parse_and(tokens, pos)?;

    while let Some(token) = tokens.get(*pos) {
        if is_and(token) {
            *pos += 1;
            let right = parse_and(tokens, pos)?;
            left = Condition::new_complex(Some(left), LogicalOperator::And, right);
        } else {
            break;
        }
    }
    Ok(left)
}

fn parse_and(tokens: &Vec<&str>, pos: &mut usize) -> Result<Condition, CQLError> {
    if let Some(token) = tokens.get(*pos) {
        if is_not(token) {
            *pos += 1;
            let expr = parse_and(tokens, pos)?;
            Ok(Condition::new_complex(None, LogicalOperator::Not, expr))
        } else {
            parse_base(tokens, pos)
        }
    } else {
        parse_base(tokens, pos)
    }
}

fn parse_base(tokens: &Vec<&str>, pos: &mut usize) -> Result<Condition, CQLError> {
    if let Some(token) = tokens.get(*pos) {
        if is_left_paren(token) {
            *pos += 1;
            let expr = parse_condition(tokens, pos)?;
            let next_token = tokens.get(*pos).ok_or(CQLError::Error)?;
            if is_right_paren(next_token) {
                *pos += 1;
                Ok(expr)
            } else {
                Err(CQLError::Error)
            }
        } else {
            let simple_condition = Condition::new_simple_from_tokens(tokens, pos)?;
            Ok(simple_condition)
        }
    } else {
        Err(CQLError::Error)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        clauses::recursive_parser::{parse_condition, Condition},
        logical_operator::LogicalOperator,
        operator::Operator,
    };

    #[test]
    fn simple_conditions() {
        let tokens1 = vec!["city", "=", "Gaiman"];
        let tokens2 = vec!["age", "<", "30"];
        let tokens3 = vec!["age", ">", "18"];
        let mut pos = 0;
        let condition1 = parse_condition(&tokens1, &mut pos).unwrap();
        pos = 0;
        let condition2 = parse_condition(&tokens2, &mut pos).unwrap();
        pos = 0;
        let condition3 = parse_condition(&tokens3, &mut pos).unwrap();

        assert_eq!(
            condition1,
            Condition::Simple {
                field: String::from("city"),
                operator: Operator::Equal,
                value: String::from("Gaiman"),
            }
        );
        assert_eq!(
            condition2,
            Condition::Simple {
                field: String::from("age"),
                operator: Operator::Lesser,
                value: String::from("30"),
            }
        );
        assert_eq!(
            condition3,
            Condition::Simple {
                field: String::from("age"),
                operator: Operator::Greater,
                value: String::from("18"),
            }
        );
    }

    #[test]
    fn not() {
        let tokens = vec!["NOT", "city", "=", "Gaiman"];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: None,
                operator: LogicalOperator::Not,
                right: Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman")
                })
            }
        )
    }

    #[test]
    fn one_or() {
        let tokens = vec!["city", "=", "Gaiman", "OR", "age", "<", "30"];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman")
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Condition::Simple {
                    field: String::from("age"),
                    operator: Operator::Lesser,
                    value: String::from("30")
                })
            }
        )
    }

    #[test]
    fn two_or() {
        let tokens = vec![
            "city", "=", "Gaiman", "OR", "age", "<", "30", "OR", "lastname", "=", "Davies",
        ];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: Some(Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: String::from("city"),
                        operator: Operator::Equal,
                        value: String::from("Gaiman")
                    })),
                    operator: LogicalOperator::Or,
                    right: Box::new(Condition::Simple {
                        field: String::from("age"),
                        operator: Operator::Lesser,
                        value: String::from("30")
                    })
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Condition::Simple {
                    field: String::from("lastname"),
                    operator: Operator::Equal,
                    value: String::from("Davies")
                })
            }
        )
    }

    #[test]
    fn one_and() {
        let tokens = vec!["city", "=", "Gaiman", "AND", "age", "<", "30"];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman")
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Simple {
                    field: String::from("age"),
                    operator: Operator::Lesser,
                    value: String::from("30")
                })
            }
        )
    }

    #[test]
    fn two_and() {
        let tokens = vec![
            "city", "=", "Gaiman", "AND", "age", "<", "30", "AND", "lastname", "=", "Davies",
        ];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: Some(Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: String::from("city"),
                        operator: Operator::Equal,
                        value: String::from("Gaiman")
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Simple {
                        field: String::from("age"),
                        operator: Operator::Lesser,
                        value: String::from("30")
                    })
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Simple {
                    field: String::from("lastname"),
                    operator: Operator::Equal,
                    value: String::from("Davies")
                })
            }
        )
    }

    #[test]
    fn and_or() {
        let tokens = vec![
            "city", "=", "Gaiman", "AND", "age", ">", "18", "OR", "lastname", "=", "Davies",
        ];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: Some(Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: String::from("city"),
                        operator: Operator::Equal,
                        value: String::from("Gaiman")
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Simple {
                        field: String::from("age"),
                        operator: Operator::Greater,
                        value: String::from("18")
                    })
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Condition::Simple {
                    field: String::from("lastname"),
                    operator: Operator::Equal,
                    value: String::from("Davies")
                })
            }
        )
    }

    #[test]
    fn not_and_or() {
        let tokens = vec![
            "NOT", "city", "=", "Gaiman", "AND", "age", ">", "18", "OR", "lastname", "=", "Davies",
        ];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: Some(Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Complex {
                        left: None,
                        operator: LogicalOperator::Not,
                        right: Box::new(Condition::Simple {
                            field: String::from("city"),
                            operator: Operator::Equal,
                            value: String::from("Gaiman")
                        })
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Simple {
                        field: String::from("age"),
                        operator: Operator::Greater,
                        value: String::from("18")
                    })
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Condition::Simple {
                    field: String::from("lastname"),
                    operator: Operator::Equal,
                    value: String::from("Davies")
                })
            }
        )
    }

    #[test]
    fn and_not() {
        let tokens = vec!["city", "=", "Gaiman", "AND", "NOT", "age", ">", "18"];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman")
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Complex {
                    left: None,
                    operator: LogicalOperator::Not,
                    right: Box::new(Condition::Simple {
                        field: String::from("age"),
                        operator: Operator::Greater,
                        value: String::from("18")
                    })
                })
            }
        )
    }

    #[test]
    fn or_not() {
        let tokens = vec!["city", "=", "Gaiman", "OR", "NOT", "age", ">", "18"];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman")
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Condition::Complex {
                    left: None,
                    operator: LogicalOperator::Not,
                    right: Box::new(Condition::Simple {
                        field: String::from("age"),
                        operator: Operator::Greater,
                        value: String::from("18")
                    })
                })
            }
        )
    }

    #[test]
    fn and_or_with_paren() {
        let tokens = vec![
            "city", "=", "Gaiman", "AND", "(", "age", ">", "18", "OR", "lastname", "=", "Davies",
            ")",
        ];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: String::from("city"),
                    operator: Operator::Equal,
                    value: String::from("Gaiman")
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: String::from("age"),
                        operator: Operator::Greater,
                        value: String::from("18")
                    })),
                    operator: LogicalOperator::Or,
                    right: Box::new(Condition::Simple {
                        field: String::from("lastname"),
                        operator: Operator::Equal,
                        value: String::from("Davies")
                    })
                })
            }
        )
    }

    #[test]
    fn not_and_or_with_paren() {
        let tokens = vec![
            "NOT", "(", "city", "=", "Gaiman", "AND", "(", "age", ">", "18", "OR", "lastname", "=",
            "Davies", ")", ")",
        ];
        let mut pos = 0;
        let condition = parse_condition(&tokens, &mut pos).unwrap();
        assert_eq!(
            condition,
            Condition::Complex {
                left: None,
                operator: LogicalOperator::Not,
                right: Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: String::from("city"),
                        operator: Operator::Equal,
                        value: String::from("Gaiman")
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Complex {
                        left: Some(Box::new(Condition::Simple {
                            field: String::from("age"),
                            operator: Operator::Greater,
                            value: String::from("18")
                        })),
                        operator: LogicalOperator::Or,
                        right: Box::new(Condition::Simple {
                            field: String::from("lastname"),
                            operator: Operator::Equal,
                            value: String::from("Davies")
                        })
                    })
                })
            }
        )
    }
}
