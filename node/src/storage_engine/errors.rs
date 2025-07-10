/// Enumeration of possible errors that can be returned by the `StorageEngine`.
///
/// This enum represents various types of errors that can occur during operations
/// within the storage engine, including file and directory operations, query validation,
/// and key management.
#[derive(Debug)]
pub enum StorageEngineError {
    /// Error related to input/output operations.
    ///
    /// This error occurs when an I/O operation fails, such as reading or writing files.
    IoError,

    /// Error when creating a temporary file fails.
    ///
    /// This error occurs when the system is unable to create a temporary file
    /// used during certain operations, such as writing data atomically.
    TempFileCreationFailed,

    /// Error when attempting to write to a file fails.
    ///
    /// This error indicates that a file write operation was unsuccessful.
    FileWriteFailed,

    /// Error when attempting to read from a file fails.
    ///
    /// This error occurs when the system is unable to read data from a file.
    FileReadFailed,

    /// Error when attempting to delete a file fails.
    ///
    /// This error indicates that a file could not be deleted, potentially due to
    /// permissions or the file being in use.
    FileDeletionFailed,

    /// Error when a directory creation operation fails.
    ///
    /// This error occurs when the system cannot create the required directory structure.
    DirectoryCreationFailed,

    /// Error when a file is not found.
    ///
    /// This error is returned when a file expected to be present cannot be located.
    FileNotFound,

    /// Error when replacing a file fails.
    ///
    /// This error occurs when an operation attempting to replace an existing file fails.
    FileReplacementFailed,

    /// Error due to an invalid query.
    ///
    /// This error is returned when a query does not conform to the expected syntax or semantics.
    InvalidQuery,

    /// Error when attempting to update or modify a primary key.
    ///
    /// This error indicates that an operation attempted to change a primary key value,
    /// which is not allowed in the storage engine.
    PrimaryKeyModificationNotAllowed,

    /// Error when a required column is missing.
    ///
    /// This error occurs when a column specified in an operation cannot be found in the schema.
    ColumnNotFound,

    /// Error when the WHERE clause is missing or invalid.
    ///
    /// This error indicates that a query lacks a required WHERE clause or that the clause
    /// is invalid.
    MissingWhereClause,

    /// Error when partition key values are incomplete or mismatched.
    ///
    /// This error occurs when the partition key values provided in an operation do not
    /// match the schema or are incomplete.
    PartitionKeyMismatch,

    /// Error when clustering key values are incomplete or mismatched.
    ///
    /// This error is returned when the clustering key values in an operation are incomplete
    /// or do not match the schema.
    ClusteringKeyMismatch,

    /// General error for unsupported operations.
    ///
    /// This error is returned when an operation is attempted that is not supported
    /// by the storage engine.
    UnsupportedOperation,
}

impl std::fmt::Display for StorageEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageEngineError::IoError => write!(f, "I/O operation failed."),
            StorageEngineError::TempFileCreationFailed => {
                write!(f, "Failed to create a temporary file.")
            }
            StorageEngineError::FileWriteFailed => write!(f, "Failed to write to the file."),
            StorageEngineError::FileReadFailed => write!(f, "Failed to read from the file."),
            StorageEngineError::FileDeletionFailed => write!(f, "Failed to delete the file."),
            StorageEngineError::DirectoryCreationFailed => {
                write!(f, "Failed to create the directory.")
            }
            StorageEngineError::FileNotFound => write!(f, "File not found."),
            StorageEngineError::FileReplacementFailed => {
                write!(f, "Failed to replace the original file.")
            }
            StorageEngineError::InvalidQuery => write!(f, "The query is invalid."),
            StorageEngineError::PrimaryKeyModificationNotAllowed => {
                write!(f, "Modification of primary keys is not allowed.")
            }
            StorageEngineError::ColumnNotFound => write!(f, "Specified column not found."),
            StorageEngineError::MissingWhereClause => {
                write!(f, "The WHERE clause is missing or invalid.")
            }
            StorageEngineError::PartitionKeyMismatch => {
                write!(f, "Partition key values are incomplete or mismatched.")
            }
            StorageEngineError::ClusteringKeyMismatch => {
                write!(f, "Clustering key values are incomplete or mismatched.")
            }
            StorageEngineError::UnsupportedOperation => write!(f, "This operation is unsupported."),
        }
    }
}

impl std::error::Error for StorageEngineError {}

impl From<std::io::Error> for StorageEngineError {
    fn from(_: std::io::Error) -> Self {
        StorageEngineError::IoError
    }
}
