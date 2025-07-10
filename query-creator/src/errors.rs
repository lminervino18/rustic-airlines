use std::fmt::Display;

/// Enum representing the possible errors that can occur when processing SQL queries.
///
/// The possible errors are:
///
/// - `InvalidTable`: related to problems with the processing of tables.
/// - `InvalidColumn`: related to problems with the processing of columns.
/// - `InvalidSyntax`: related to problems with the processing of queries.
/// - `Error`: generic type for other possible errors detected.
///
#[derive(Debug, PartialEq)]
pub enum CQLError {
    InvalidTable,
    InvalidColumn,
    InvalidSyntax,
    NoActualKeyspaceError,
    TableAlreadyExist,
    NoWhereCondition,
    MissingPartitionOrClusteringColumns,
    InvalidCondition,
    Error,
}

impl Display for CQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CQLError::InvalidTable => write!(f, "[InvalidTable]: [Error to process table]"),
            CQLError::InvalidColumn => write!(f, "[InvalidColumn]: [Error to process column]"),
            CQLError::InvalidSyntax => write!(f, "[InvalidSyntax]: [Error to process query]"),
            CQLError::NoActualKeyspaceError => {
                write!(f, "[NoActualKeyspace]: [There is no current keyspace]")
            }
            CQLError::TableAlreadyExist => {
                write!(f, "[TableAlreadyExist]: [The table already exists]")
            }
            CQLError::NoWhereCondition => {
                write!(
                    f,
                    "[NoWhereCondition]: [The query requires a WHERE clause, but none was found]"
                )
            }
            CQLError::MissingPartitionOrClusteringColumns => {
                write!(f, "[MissingPartitionOrClusteringColumns]: [The query is missing required partition or clustering columns]")
            }
            CQLError::InvalidCondition => {
                write!(
                    f,
                    "[InvalidCondition]: [The condition in the query is invalid]"
                )
            }
            CQLError::Error => write!(f, "[Error]: [An unspecified error occurred]"),
        }
    }
}
