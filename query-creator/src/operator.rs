/// Enum for the operators used in the queries.
/// - `Equal`: Equal operator
/// - `Greater`: Greater than operator
/// - `Lesser`: Lesser than operator
///
///
///
use crate::CQLError;
#[derive(Debug, PartialEq, Clone)]

/// Represents comparison operators used in queries.
///
/// # Variants
/// - `Equal`
///   - Represents the equality (`=`) operator.
/// - `Greater`
///   - Represents the greater than (`>`) operator.
/// - `Lesser`
///   - Represents the lesser than (`<`) operator.
///
/// # Purpose
/// The `Operator` enum encapsulates comparison operators commonly used in SQL-like query conditions. It provides methods to serialize these operators to their string representations and deserialize them back into enum variants.
///
/// # Use Cases
/// - Parsing query strings to extract comparison operators.
/// - Generating query strings from parsed conditions.

pub enum Operator {
    Equal,
    Greater,
    Lesser,
}

impl Operator {
    /// Serializes the `Operator` to its SQL string representation.
    ///
    /// # Purpose
    /// Converts an `Operator` enum variant to its corresponding SQL keyword, enabling seamless integration with SQL-like query serialization.
    ///
    /// # Returns
    /// - `&str`:
    ///   - A string slice representing the comparison operator:
    ///     - `"="` for `Operator::Equal`.
    ///     - `">"` for `Operator::Greater`.
    ///     - `"<"` for `Operator::Lesser`.

    pub fn serialize(&self) -> &str {
        match self {
            Operator::Equal => "=",
            Operator::Greater => ">",
            Operator::Lesser => "<",
        }
    }

    /// Deserializes a string to an `Operator`.
    ///
    /// # Purpose
    /// Converts a string representation of a comparison operator (e.g., `"="`, `">"`, `"<"`)
    /// into the corresponding `Operator` enum variant.
    ///
    /// # Parameters
    /// - `op_str: &str`:
    ///   - A string slice representing a comparison operator.
    ///     - Valid inputs: `"="`, `">"`, `"<"`.
    ///
    /// # Returns
    /// - `Result<Operator, CQLError>`:
    ///   - `Ok(Operator)` if the input matches a valid comparison operator.
    ///   - `Err(CQLError::InvalidSyntax)` if the input does not match any valid operator.

    pub fn deserialize(op_str: &str) -> Result<Self, CQLError> {
        match op_str {
            "=" => Ok(Operator::Equal),
            ">" => Ok(Operator::Greater),
            "<" => Ok(Operator::Lesser),
            _ => Err(CQLError::InvalidSyntax),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CQLError;

    #[test]
    fn test_serialize() {
        // Test serialization of Operator variants
        assert_eq!(Operator::Equal.serialize(), "=");
        assert_eq!(Operator::Greater.serialize(), ">");
        assert_eq!(Operator::Lesser.serialize(), "<");
    }

    #[test]
    fn test_deserialize_valid() {
        // Test valid deserialization of comparison operators
        assert_eq!(Operator::deserialize("="), Ok(Operator::Equal));
        assert_eq!(Operator::deserialize(">"), Ok(Operator::Greater));
        assert_eq!(Operator::deserialize("<"), Ok(Operator::Lesser));
    }

    #[test]
    fn test_deserialize_invalid() {
        // Test invalid deserialization cases
        assert_eq!(
            Operator::deserialize("INVALID"),
            Err(CQLError::InvalidSyntax)
        );
        assert_eq!(Operator::deserialize(""), Err(CQLError::InvalidSyntax));
    }

    #[test]
    fn test_serialize_and_deserialize_roundtrip() {
        // Test that serialization and deserialization are inverses
        let operators = vec![Operator::Equal, Operator::Greater, Operator::Lesser];

        for op in operators {
            let serialized = op.serialize();
            let deserialized = Operator::deserialize(serialized).unwrap();
            assert_eq!(op, deserialized);
        }
    }
}
