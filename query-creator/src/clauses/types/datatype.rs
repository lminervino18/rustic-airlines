use crate::{errors::CQLError, operator::Operator};
use uuid::Uuid;

/// Enum that represents different data types supported in CQL (Cassandra Query Language).
/// Each variant corresponds to a data type in CQL and is associated with a specific integer value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum DataType {
    /// Represents an integer (CQL `INT`).
    Int = 0x00,

    /// Represents a string (CQL `TEXT` or `STRING`).
    String = 0x01,

    /// Represents a boolean (CQL `BOOLEAN`).
    Boolean = 0x02,

    /// Represents a float (CQL `FLOAT`).
    Float = 0x03,

    /// Represents a double (CQL `DOUBLE`).
    Double = 0x04,

    /// Represents a timestamp (CQL `TIMESTAMP`).
    Timestamp = 0x05,

    /// Represents a UUID (CQL `UUID`).
    Uuid = 0x06,
}

impl std::str::FromStr for DataType {
    type Err = CQLError;

    /// Converts a string representation of a CQL data type to a `DataType` enum.
    ///
    /// # Arguments
    ///
    /// * `s` - The string representation of a CQL data type (e.g., "INT", "TEXT").
    ///
    /// # Returns
    ///
    /// A `Result` containing the corresponding `DataType` variant if the string is valid,
    /// or a `CQLError::InvalidSyntax` error if the string doesn't match any valid type.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INT" => Ok(DataType::Int),
            "TEXT" | "STRING" => Ok(DataType::String),
            "BOOLEAN" => Ok(DataType::Boolean),
            "FLOAT" => Ok(DataType::Float),
            "DOUBLE" => Ok(DataType::Double),
            "TIMESTAMP" => Ok(DataType::Timestamp),
            "UUID" => Ok(DataType::Uuid),
            _ => Err(CQLError::InvalidSyntax),
        }
    }
}

impl DataType {
    /// Returns the string representation of the data type (CQL syntax).
    ///
    /// # Returns
    ///
    /// A string representing the CQL data type (e.g., `"INT"`, `"TEXT"`).
    pub fn to_string(&self) -> &str {
        match self {
            DataType::Int => "INT",
            DataType::String => "TEXT",
            DataType::Boolean => "BOOLEAN",
            DataType::Float => "FLOAT",
            DataType::Double => "DOUBLE",
            DataType::Timestamp => "TIMESTAMP",
            DataType::Uuid => "UUID",
        }
    }

    /// Compares two values (as strings) of the current `DataType` with a specified operator (e.g., `=`, `>`, `<`).
    ///
    /// # Arguments
    ///
    /// * `x` - The first value to compare (as a string).
    /// * `y` - The second value to compare (as a string).
    /// * `operator` - The comparison operator (e.g., `Equal`, `Greater`, `Lesser`).
    ///
    /// # Returns
    ///
    /// A `Result<bool, CQLError>`, where `Ok(true)` or `Ok(false)` indicates whether the comparison is true or false,
    /// and `Err(CQLError::InvalidCondition)` indicates that the values could not be parsed for comparison.
    pub fn compare(&self, x: &str, y: &str, operator: &Operator) -> Result<bool, CQLError> {
        match self {
            DataType::Int => {
                let x = x.parse::<i32>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<i32>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::String => {
                let x = x
                    .parse::<String>()
                    .map_err(|_| CQLError::InvalidCondition)?;
                let y = y
                    .parse::<String>()
                    .map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::Boolean => {
                let x = x.parse::<bool>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<bool>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x & !y),
                    Operator::Lesser => Ok(!x & y),
                }
            }
            DataType::Float => {
                let x = x.parse::<f32>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<f32>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::Double => {
                let x = x.parse::<f64>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<f64>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::Timestamp => {
                let x = x.parse::<i64>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<i64>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
            DataType::Uuid => {
                let x = x.parse::<Uuid>().map_err(|_| CQLError::InvalidCondition)?;
                let y = y.parse::<Uuid>().map_err(|_| CQLError::InvalidCondition)?;
                match operator {
                    Operator::Equal => Ok(x == y),
                    Operator::Greater => Ok(x > y),
                    Operator::Lesser => Ok(x < y),
                }
            }
        }
    }

    /// Checks if a given string value is valid for the specified `DataType`.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to check, represented as a string.
    ///
    /// # Returns
    ///
    /// A boolean indicating whether the value is valid for the specified data type.
    pub fn is_valid_value(&self, value: &str) -> bool {
        match self {
            DataType::Int => value.parse::<i32>().is_ok(),
            DataType::String => true,
            DataType::Boolean => {
                value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false")
            }
            DataType::Float => value.parse::<f32>().is_ok(),
            DataType::Double => value.parse::<f64>().is_ok(),
            DataType::Timestamp => self.is_valid_timestamp(value),
            DataType::Uuid => value.parse::<Uuid>().is_ok(),
        }
    }

    fn is_valid_timestamp(&self, value: &str) -> bool {
        chrono::DateTime::parse_from_rfc3339(value).is_ok() || value.parse::<i64>().is_ok()
    }
}
