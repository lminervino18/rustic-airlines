/// Logical operators used in the `WHERE` clause.
/// - `And`: Logical AND operator
/// - `Or`: Logical OR operator
/// - `Not`: Logical NOT operator
///
///
use crate::CQLError;
#[derive(Debug, PartialEq, Clone)]

/// Represents logical operators used in SQL-like queries.
///
/// # Variants
/// - `And`
///   - Represents the logical `AND` operator.
/// - `Or`
///   - Represents the logical `OR` operator.
/// - `Not`
///   - Represents the logical `NOT` operator.
///
/// # Purpose
/// The `LogicalOperator` enum is designed to encapsulate logical operations commonly used in SQL-like query conditions.
/// It provides methods for serialization and deserialization to and from their string representations.
///
/// # Methods
/// ## `serialize`
/// Converts the `LogicalOperator` to its SQL string representation.
///
/// - **Returns**:
///   - A string slice representing the logical operator (`"AND"`, `"OR"`, `"NOT"`).
///
/// ## `deserialize`
/// Converts a string to its corresponding `LogicalOperator`.
///
/// - **Parameters**:
///   - `op_str: &str`:
///     - A string representing a logical operator (e.g., `"AND"`, `"OR"`, `"NOT"`).
///
/// - **Returns**:
///   - `Ok(LogicalOperator)` if the string matches a known operator.
///   - `Err(CQLError::InvalidSyntax)` if the string does not match any valid operator.
///
pub enum LogicalOperator {
    And,
    Or,
    Not,
}

impl LogicalOperator {
    /// Serializes the `LogicalOperator` to its SQL string representation.
    ///
    /// # Purpose
    /// Converts a `LogicalOperator` enum variant to its corresponding SQL keyword,
    /// enabling seamless integration with SQL-like query serialization.
    ///
    /// # Returns
    /// - `&str`:
    ///   - A string slice representing the logical operator:
    ///     - `"AND"` for `LogicalOperator::And`.
    ///     - `"OR"` for `LogicalOperator::Or`.
    ///     - `"NOT"` for `LogicalOperator::Not`.

    pub fn serialize(&self) -> &str {
        match self {
            LogicalOperator::And => "AND",
            LogicalOperator::Or => "OR",
            LogicalOperator::Not => "NOT",
        }
    }

    /// Deserializes a string to a `LogicalOperator`.
    ///
    /// # Purpose
    /// Converts a string representation of a logical operator (e.g., `"AND"`, `"OR"`, `"NOT"`)
    /// into the corresponding `LogicalOperator` enum variant.
    ///
    /// # Parameters
    /// - `op_str: &str`:
    ///   - A string slice representing a logical operator.
    ///     - Valid inputs: `"AND"`, `"OR"`, `"NOT"`.
    ///
    /// # Returns
    /// - `Result<LogicalOperator, CQLError>`:
    ///   - `Ok(LogicalOperator)` if the input matches a valid logical operator.
    ///   - `Err(CQLError::InvalidSyntax)` if the input does not match any valid operator.

    pub fn deserialize(op_str: &str) -> Result<Self, CQLError> {
        match op_str {
            "AND" => Ok(LogicalOperator::And),
            "OR" => Ok(LogicalOperator::Or),
            "NOT" => Ok(LogicalOperator::Not),
            _ => Err(CQLError::InvalidSyntax),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize() {
        // Test serialization of LogicalOperator variants
        assert_eq!(LogicalOperator::And.serialize(), "AND");
        assert_eq!(LogicalOperator::Or.serialize(), "OR");
        assert_eq!(LogicalOperator::Not.serialize(), "NOT");
    }

    #[test]
    fn test_deserialize_valid() {
        // Test valid deserialization of logical operators
        assert_eq!(
            LogicalOperator::deserialize("AND"),
            Ok(LogicalOperator::And)
        );
        assert_eq!(LogicalOperator::deserialize("OR"), Ok(LogicalOperator::Or));
        assert_eq!(
            LogicalOperator::deserialize("NOT"),
            Ok(LogicalOperator::Not)
        );
    }

    #[test]
    fn test_deserialize_invalid() {
        // Test invalid deserialization cases
        assert_eq!(
            LogicalOperator::deserialize("INVALID"),
            Err(CQLError::InvalidSyntax)
        );
        assert_eq!(
            LogicalOperator::deserialize(""),
            Err(CQLError::InvalidSyntax)
        );
        assert_eq!(
            LogicalOperator::deserialize("and"), // Case sensitivity test
            Err(CQLError::InvalidSyntax)
        );
    }

    #[test]
    fn test_serialize_and_deserialize_roundtrip() {
        // Test that serialization and deserialization are inverses
        let operators = vec![
            LogicalOperator::And,
            LogicalOperator::Or,
            LogicalOperator::Not,
        ];

        for op in operators {
            let serialized = op.serialize();
            let deserialized = LogicalOperator::deserialize(serialized).unwrap();
            assert_eq!(op, deserialized);
        }
    }
}
