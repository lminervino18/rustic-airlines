use super::datatype::DataType;

/// Represents a column in a database table schema.
/// This struct contains information about the column's name, data type, primary key status, nullability, and clustering/partitioning properties.
#[derive(Debug, Clone, Eq)]
pub struct Column {
    /// The name of the column.
    pub name: String,

    /// The data type of the column (e.g., `INT`, `TEXT`, etc.).
    pub data_type: DataType,

    /// Whether the column is a primary key in the table.
    pub is_primary_key: bool,

    /// Whether the column allows null values (`true` if it allows null, `false` if it does not).
    pub allows_null: bool,

    /// Whether the column is part of the clustering key in the table.
    pub is_clustering_column: bool,

    /// Whether the column is a partition key for the table.
    pub is_partition_key: bool,

    /// The order of the clustering column (e.g., `ASC` for ascending, `DESC` for descending).
    /// This could potentially be represented as an enum, e.g., `ClusteringOrder::Asc` or `ClusteringOrder::Desc`.
    pub clustering_order: String, // TODO: enum? Is it ASC/DESC?
}

impl Column {
    /// Creates a new `Column` instance.
    /// This method initializes the column with a given name, data type, primary key status, and nullability. Other properties are set to their default values.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the column.
    /// * `data_type` - The data type of the column.
    /// * `is_primary_key` - Whether the column is a primary key.
    /// * `allows_null` - Whether the column allows null values.
    ///
    /// # Returns
    ///
    /// A `Column` instance with the specified properties and default values for clustering and partition keys.
    pub fn new(name: &str, data_type: DataType, is_primary_key: bool, allows_null: bool) -> Column {
        Column {
            name: name.to_string(),
            data_type,
            is_primary_key,
            allows_null,
            is_clustering_column: false,
            is_partition_key: false,
            clustering_order: String::new(),
        }
    }

    /// Returns the clustering order of the column as a `String`.
    ///
    /// # Returns
    ///
    /// The clustering order of the column (e.g., `"ASC"` or `"DESC"`).
    pub fn get_clustering_order(&self) -> String {
        self.clustering_order.clone()
    }
}

// Implementation of the `PartialEq` trait to compare `Column` instances by their name only.
impl PartialEq for Column {
    /// Compares two `Column` instances for equality.
    ///
    /// # Arguments
    ///
    /// * `other` - The other `Column` instance to compare with the current instance.
    ///
    /// # Returns
    ///
    /// `true` if the names of both columns are equal, otherwise `false`.
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
