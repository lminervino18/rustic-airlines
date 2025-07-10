use crate::messages::MessageError;
use query_creator::clauses::{
    keyspace::create_keyspace_cql::CreateKeyspace,
    table::create_table_cql::CreateTable,
    types::{column::Column, datatype::DataType},
};
use std::{
    collections::HashMap,
    fmt::{self, Display},
    io::{Cursor, Read},
};

pub trait CursorSerializable {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(cursor: &mut Cursor<&[u8]>) -> std::result::Result<Self, MessageError>
    where
        Self: Sized;
}

#[derive(Clone, PartialEq)]
pub struct TableSchema {
    pub inner: CreateTable,
}

impl TableSchema {}

/// Implements `fmt::Debug` for `Table` to provide human-readable information for debugging.
impl fmt::Debug for TableSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table: {}", self.get_name())
    }
}

impl TableSchema {
    pub fn new(inner: CreateTable) -> Self {
        TableSchema { inner }
    }

    /// Gets the name of the table.
    ///
    /// # Returns
    /// The table name as a `String`.
    pub fn get_name(&self) -> String {
        self.inner.get_name()
    }

    /// Retrieves all columns in the table.
    ///
    /// # Returns
    /// A `Vec<Column>` containing the columns.
    pub fn get_columns(&self) -> Vec<Column> {
        self.inner.get_columns()
    }

    /// Gets the index of a column by its name.
    ///
    /// # Parameters
    /// - `column_name`: The name of the column.
    ///
    /// # Returns
    /// The index of the column, or `None` if the column is not found.
    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.get_columns()
            .iter()
            .position(|col| col.name == column_name)
    }

    /// Checks if a specific column is the primary key.
    ///
    /// # Parameters
    /// - `column_name`: The name of the column to check.
    ///
    /// # Returns
    /// `Ok(true)` if the column is the primary key, `Ok(false)` otherwise, or an error if the column is not found.
    pub fn is_primary_key(&self, column_name: &str) -> Result<bool, SchemaError> {
        let column_index = self
            .get_column_index(column_name)
            .ok_or(SchemaError::NoSuchColumn(column_name.to_string()))?;

        let columns = self.inner.get_columns();
        let column = columns
            .get(column_index)
            .ok_or(SchemaError::NoSuchColumn(column_name.to_string()))?;

        Ok(column.is_primary_key)
    }

    /// Gets the name of the primary key column.
    ///
    /// # Returns
    /// The name of the primary key column as a `String`, or an error if no primary key is found.
    pub fn get_partition_keys(&self) -> Result<Vec<String>, SchemaError> {
        let mut partitioner_keys: Vec<String> = vec![];
        let columns = self.get_columns();

        for column in columns {
            if column.is_partition_key {
                partitioner_keys.push(column.name.clone());
            }
        }

        assert!(!partitioner_keys.is_empty()); // TODO: there MUST be at least a partition key. Enforce in type system

        Ok(partitioner_keys)
    }

    /// Gets the name of the primary key column.
    ///
    /// # Returns
    /// The name of the primary key column as a `String`, or an error if no primary key is found.
    pub fn get_clustering_columns(&self) -> Result<Vec<String>, SchemaError> {
        let mut clustering_columns: Vec<String> = vec![];
        let columns = self.get_columns();

        for column in columns {
            if column.is_clustering_column {
                clustering_columns.push(column.name.clone());
            }
        }

        Ok(clustering_columns)
    }

    pub fn get_clustering_column_in_order(&self) -> Vec<String> {
        self.inner.get_clustering_column_in_order()
    }
}

impl CursorSerializable for Column {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let name_len_bytes = (self.name.len() as u32).to_be_bytes();
        bytes.extend_from_slice(&name_len_bytes);
        let name_bytes = self.name.as_bytes();
        bytes.extend_from_slice(name_bytes);

        let data_type_byte = self.data_type as u8;
        bytes.push(data_type_byte);

        let is_primary_key = self.is_primary_key as u8;
        bytes.push(is_primary_key);

        let allows_null = self.allows_null as u8;
        bytes.push(allows_null);

        let is_clustering_column = self.is_clustering_column as u8;
        bytes.push(is_clustering_column);

        let is_partition_key = self.is_partition_key as u8;
        bytes.push(is_partition_key);

        let clustering_order_len = self.clustering_order.len() as u32;
        bytes.extend_from_slice(&clustering_order_len.to_be_bytes());
        let clustering_order_bytes = self.clustering_order.as_bytes();
        bytes.extend_from_slice(clustering_order_bytes);

        bytes
    }

    fn from_bytes(cursor: &mut Cursor<&[u8]>) -> std::result::Result<Self, MessageError>
    where
        Self: Sized,
    {
        let mut name_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut name_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let name_len = u32::from_be_bytes(name_len_bytes);

        let mut name_bytes = vec![0u8; name_len as usize];
        cursor
            .read_exact(&mut name_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let name = String::from_utf8(name_bytes).map_err(|_| MessageError::CursorError)?;

        let mut data_type_byte = [0u8; 1];
        cursor
            .read_exact(&mut data_type_byte)
            .map_err(|_| MessageError::CursorError)?;

        let data_type = match data_type_byte[0] {
            0 => DataType::Int,
            1 => DataType::String,
            2 => DataType::Boolean,
            3 => DataType::Float,
            4 => DataType::Double,
            5 => DataType::Timestamp,
            6 => DataType::Uuid,
            _ => {
                return Err(MessageError::InvalidValue(format!(
                    "Invalid DataType value: {}",
                    data_type_byte[0]
                )))
            }
        };

        let mut is_primary_key_bytes = [0u8; 1];
        cursor
            .read_exact(&mut is_primary_key_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let is_primary_key = is_primary_key_bytes[0] == 1;

        let mut allows_null_bytes = [0u8; 1];
        cursor
            .read_exact(&mut allows_null_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let allows_null = allows_null_bytes[0] == 1;

        let mut is_clustering_column_bytes = [0u8; 1];
        cursor
            .read_exact(&mut is_clustering_column_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let is_clustering_column = is_clustering_column_bytes[0] == 1;

        let mut is_partition_key_bytes = [0u8; 1];
        cursor
            .read_exact(&mut is_partition_key_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let is_partition_key = is_partition_key_bytes[0] == 1;

        let mut clustering_order_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut clustering_order_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let clustering_order_len = u32::from_be_bytes(clustering_order_len_bytes);

        let mut clustering_order_bytes = vec![0u8; clustering_order_len as usize];
        cursor
            .read_exact(&mut clustering_order_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let clustering_order =
            String::from_utf8(clustering_order_bytes).map_err(|_| MessageError::CursorError)?;

        Ok(Column {
            name,
            data_type,
            is_clustering_column,
            is_partition_key,
            is_primary_key,
            allows_null,
            clustering_order,
        })
    }
}

impl CursorSerializable for CreateTable {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let name_len_bytes = (self.name.len() as u32).to_be_bytes();
        let name_bytes = self.name.as_bytes();
        bytes.extend_from_slice(&name_len_bytes);
        bytes.extend_from_slice(name_bytes);

        let keyspace_len_bytes = (self.keyspace_used_name.len() as u32).to_be_bytes();
        let keyspace_bytes = self.keyspace_used_name.as_bytes();
        bytes.extend_from_slice(&keyspace_len_bytes);
        bytes.extend_from_slice(keyspace_bytes);

        let if_not_exists = self.if_not_exists_clause as u8;
        bytes.push(if_not_exists);

        let columns_len = self.columns.len() as u32;
        bytes.extend_from_slice(&columns_len.to_be_bytes());
        let mut columns_bytes = Vec::new();

        for column in &self.columns {
            columns_bytes.extend_from_slice(&column.to_bytes());
        }

        bytes.extend_from_slice(&columns_bytes);

        let clustering_columns_len = self.clustering_columns_in_order.len() as u32;
        bytes.extend_from_slice(&clustering_columns_len.to_be_bytes());
        let mut clustering_columns_bytes = Vec::new();

        for column in &self.clustering_columns_in_order {
            clustering_columns_bytes.extend_from_slice(&(column.len() as u32).to_be_bytes());
            clustering_columns_bytes.extend_from_slice(column.as_bytes());
        }

        bytes.extend_from_slice(&clustering_columns_bytes);

        bytes
    }

    fn from_bytes(cursor: &mut Cursor<&[u8]>) -> std::result::Result<Self, MessageError>
    where
        Self: Sized,
    {
        let mut name_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut name_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let name_len = u32::from_be_bytes(name_len_bytes);

        let mut name_bytes = vec![0u8; name_len as usize];
        cursor
            .read_exact(&mut name_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let name = String::from_utf8(name_bytes).map_err(|_| MessageError::CursorError)?;

        let mut keyspace_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut keyspace_len_bytes)
            .map_err(|_| MessageError::CursorError)?;

        let keyspace_len = u32::from_be_bytes(keyspace_len_bytes);

        let mut keyspace_bytes = vec![0u8; keyspace_len as usize];
        cursor
            .read_exact(&mut keyspace_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let keyspace = String::from_utf8(keyspace_bytes).map_err(|_| MessageError::CursorError)?;

        let mut if_not_exists_bytes = [0u8; 1];
        cursor
            .read_exact(&mut if_not_exists_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let if_not_exists = if_not_exists_bytes[0] == 1;

        let mut columns_len_bytes = [0u8; 4];

        cursor
            .read_exact(&mut columns_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let columns_len = u32::from_be_bytes(columns_len_bytes);

        let mut columns = Vec::new();

        for _ in 0..columns_len {
            let column = Column::from_bytes(cursor).map_err(|_| MessageError::CursorError)?;
            columns.push(column);
        }

        let mut clustering_columns_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut clustering_columns_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let clustering_columns_len = u32::from_be_bytes(clustering_columns_len_bytes);

        let mut clustering_columns = Vec::new();

        for _ in 0..clustering_columns_len {
            let mut column_len_bytes = [0u8; 4];
            cursor
                .read_exact(&mut column_len_bytes)
                .map_err(|_| MessageError::CursorError)?;
            let column_len = u32::from_be_bytes(column_len_bytes);

            let mut column_bytes = vec![0u8; column_len as usize];

            cursor
                .read_exact(&mut column_bytes)
                .map_err(|_| MessageError::CursorError)?;

            let column = String::from_utf8(column_bytes).map_err(|_| MessageError::CursorError)?;

            clustering_columns.push(column);
        }

        Ok(CreateTable {
            name,
            keyspace_used_name: keyspace,
            if_not_exists_clause: if_not_exists,
            columns,
            clustering_columns_in_order: clustering_columns,
        })
    }
}

impl CursorSerializable for TableSchema {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let inner_bytes = self.inner.to_bytes();

        bytes.extend_from_slice(&inner_bytes);

        bytes
    }

    fn from_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Self, MessageError> {
        let inner = CreateTable::from_bytes(cursor).map_err(|_| MessageError::CursorError)?;

        Ok(TableSchema { inner })
    }
}

#[derive(Clone, PartialEq, Debug, Default)]
pub struct Schema {
    pub timestamp: i64,
    // no puedo usar Keyspace porque sino tengo una
    // dependencia circular entre node y gossip
    pub keyspaces: HashMap<String, KeyspaceSchema>,
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            timestamp: 0,
            keyspaces: HashMap::new(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let timestamp_bytes = self.timestamp.to_be_bytes();
        bytes.extend_from_slice(&timestamp_bytes);

        let keyspaces_len = self.keyspaces.len() as u32;
        bytes.extend_from_slice(&keyspaces_len.to_be_bytes());

        for (keyspace_name, keyspace_schema) in &self.keyspaces {
            let keyspace_name_len_bytes = (keyspace_name.len() as u32).to_be_bytes();
            let keyspace_name_bytes = keyspace_name.as_bytes();
            bytes.extend_from_slice(&keyspace_name_len_bytes);
            bytes.extend_from_slice(keyspace_name_bytes);

            let keyspace_schema_bytes = keyspace_schema.to_bytes();
            bytes.extend_from_slice(&keyspace_schema_bytes);
        }

        bytes
    }

    pub fn from_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Self, MessageError> {
        let mut timestamp_bytes = [0u8; 8];
        cursor
            .read_exact(&mut timestamp_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let timestamp = i64::from_be_bytes(timestamp_bytes);

        let mut keyspaces_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut keyspaces_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let keyspaces_len = u32::from_be_bytes(keyspaces_len_bytes);

        let mut keyspaces = HashMap::new();

        for _ in 0..keyspaces_len {
            let mut keyspace_name_len_bytes = [0u8; 4];
            cursor
                .read_exact(&mut keyspace_name_len_bytes)
                .map_err(|_| MessageError::CursorError)?;
            let keyspace_name_len = u32::from_be_bytes(keyspace_name_len_bytes);

            let mut keyspace_name_bytes = vec![0u8; keyspace_name_len as usize];
            cursor
                .read_exact(&mut keyspace_name_bytes)
                .map_err(|_| MessageError::CursorError)?;
            let keyspace_name =
                String::from_utf8(keyspace_name_bytes).map_err(|_| MessageError::CursorError)?;

            let keyspace_schema =
                KeyspaceSchema::from_bytes(cursor).map_err(|_| MessageError::CursorError)?;

            keyspaces.insert(keyspace_name, keyspace_schema);
        }

        Ok(Schema {
            keyspaces,
            timestamp,
        })
    }
}

#[derive(Clone, PartialEq, Debug, Default)]
/// Represents the application state of the endpoint in the cluster at a given point in time.
///
/// ### Fields
/// - `status`: The status of the node.
/// - `version`: The version of the ApplicationState.
/// - `schema`: The schema of the cluster.
pub struct ApplicationState {
    pub status: NodeStatus,
    pub version: u32,
    pub schema: Schema,
}

/// Represents the schema of the keyspace.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct KeyspaceSchema {
    pub inner: CreateKeyspace,
    pub tables: Vec<TableSchema>,
}

#[derive(Debug)]
pub enum SchemaError {
    InvalidTable(String),
    Other,
    NoSuchColumn(String),
}

impl Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaError::InvalidTable(e) => write!(f, "Invalid table: {:?}", e),
            SchemaError::Other => write!(f, "Other schema error"),
            SchemaError::NoSuchColumn(e) => write!(f, "No such column: {:?}", e),
        }
    }
}

impl KeyspaceSchema {
    // /// Creates a new instance of `Keyspace` from a `CreateKeyspace`.
    // ///
    // /// # Arguments
    // ///
    // /// * `create_keyspace` - The keyspace definition to create the instance.
    // ///
    // /// # Returns
    // /// Returns a new instance of `Keyspace`.
    // pub fn new(create_keyspace: CreateKeyspace) -> Self {
    //     Self {
    //         inner: create_keyspace,
    //         tables: vec![],
    //     }
    // }

    /// Gets the name of the keyspace.
    ///
    /// # Returns
    /// Returns the keyspace name as a `String`.
    pub fn get_name(&self) -> String {
        self.inner.get_name()
    }

    /// Retrieves all tables associated with this keyspace.
    ///
    /// # Returns
    /// Returns a vector of tables (`Vec<Table>`).
    pub fn get_tables(&self) -> Vec<TableSchema> {
        self.tables.clone()
    }

    /// Gets the replication class of the keyspace.
    ///
    /// # Returns
    /// Returns the replication class as a `String`.
    pub fn get_replication_class(&self) -> String {
        self.inner.get_replication_class()
    }

    /// Gets the replication factor of the keyspace.
    ///
    /// # Returns
    /// Returns the replication factor as `u32`.
    pub fn get_replication_factor(&self) -> u32 {
        self.inner.get_replication_factor()
    }

    /// Updates the replication class of the keyspace.
    ///
    /// # Arguments
    ///
    /// * `replication_class` - The new replication class.
    pub fn update_replication_class(&mut self, replication_class: String) {
        self.inner.update_replication_class(replication_class);
    }

    /// Updates the replication factor of the keyspace.
    ///
    /// # Arguments
    ///
    /// * `replication_factor` - The new replication factor.
    pub fn update_replication_factor(&mut self, replication_factor: u32) {
        self.inner.update_replication_factor(replication_factor)
    }

    /// Adds a new table to the keyspace.
    ///
    /// # Arguments
    ///
    /// * `new_table` - The table to add.
    ///
    /// # Returns
    /// Returns `Ok(())` if the table was successfully added, or a `NodeError` if the table already exists.
    pub fn add_table(&mut self, new_table: TableSchema) -> Result<(), SchemaError> {
        if self.tables.contains(&new_table) {
            return Err(SchemaError::InvalidTable(new_table.get_name()));
        }

        self.tables.push(new_table);

        Ok(())
    }

    /// Retrieves a table by its name.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to search for.
    ///
    /// # Returns
    /// Returns the found table or a `NodeError` if not found.
    pub fn get_table(&self, table_name: &str) -> Result<TableSchema, SchemaError> {
        let table = self
            .tables
            .iter()
            .find(|table| table.get_name() == *table_name)
            .cloned();

        if let Some(table) = table {
            Ok(table)
        } else {
            Err(SchemaError::InvalidTable(table_name.to_string()))
        }
    }

    /// Removes a table by its name.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to remove.
    ///
    /// # Returns
    /// Returns `Ok(())` if the table was successfully removed or a `NodeError` if not found.
    pub fn remove_table(&mut self, table_name: &str) -> Result<(), SchemaError> {
        let index = self
            .tables
            .iter()
            .position(|table| table.get_name() == table_name)
            .ok_or(SchemaError::InvalidTable(table_name.to_string()))?;

        self.tables.remove(index);
        Ok(())
    }
}

impl CursorSerializable for CreateKeyspace {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let name_len = self.name.len() as u32;
        let name_bytes = self.name.as_bytes();
        bytes.extend_from_slice(&name_len.to_be_bytes());
        bytes.extend_from_slice(name_bytes);

        let if_not_exists = self.if_not_exists_clause as u8;
        bytes.push(if_not_exists);

        let replication_class_len = self.replication_class.len() as u32;
        let replication_class_bytes = self.replication_class.as_bytes();
        bytes.extend_from_slice(&replication_class_len.to_be_bytes());
        bytes.extend_from_slice(replication_class_bytes);

        bytes.extend_from_slice(&self.replication_factor.to_be_bytes());

        bytes
    }

    fn from_bytes(cursor: &mut Cursor<&[u8]>) -> std::result::Result<Self, MessageError>
    where
        Self: Sized,
    {
        let mut name_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut name_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let name_len = u32::from_be_bytes(name_len_bytes);

        let mut name_bytes = vec![0u8; name_len as usize];
        cursor
            .read_exact(&mut name_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let name = String::from_utf8(name_bytes).map_err(|_| MessageError::CursorError)?;

        let mut if_not_exists_bytes = [0u8; 1];
        cursor
            .read_exact(&mut if_not_exists_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let if_not_exists = if_not_exists_bytes[0] == 1;

        let mut replication_class_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut replication_class_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let replication_class_len = u32::from_be_bytes(replication_class_len_bytes);

        let mut replication_class_bytes = vec![0u8; replication_class_len as usize];
        cursor
            .read_exact(&mut replication_class_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let replication_class =
            String::from_utf8(replication_class_bytes).map_err(|_| MessageError::CursorError)?;

        let mut replication_factor_bytes = [0u8; 4];
        cursor
            .read_exact(&mut replication_factor_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let replication_factor = u32::from_be_bytes(replication_factor_bytes);

        Ok(CreateKeyspace {
            name,
            if_not_exists_clause: if_not_exists,
            replication_class,
            replication_factor,
        })
    }
}

impl KeyspaceSchema {
    pub fn new(keyspace: CreateKeyspace, tables: Vec<TableSchema>) -> Self {
        KeyspaceSchema {
            inner: keyspace,
            tables,
        }
    }
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |      keyspace     |
    /// |        ...        |
    /// +----+----+----+----+
    /// |      tables       |
    /// |        ...        |
    /// +----+----+----+----+
    /// ```
    /// Convert the `KeyspaceSchema` to a byte vector.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let keyspace_bytes = self.inner.to_bytes();
        bytes.extend_from_slice(&keyspace_bytes);

        let tables_len = self.tables.len() as u32;
        bytes.extend_from_slice(&tables_len.to_be_bytes());

        let mut tables_bytes = vec![];

        for table in &self.tables {
            tables_bytes.extend_from_slice(&table.to_bytes());
        }

        bytes.extend_from_slice(&tables_bytes);

        bytes
    }

    /// Create a `KeyspaceSchema` from bytes.
    pub fn from_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Self, MessageError> {
        let keyspace = CreateKeyspace::from_bytes(cursor).map_err(|_| MessageError::CursorError)?;

        let mut tables_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut tables_len_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let tables_len = u32::from_be_bytes(tables_len_bytes);

        let mut tables = Vec::new();

        for _ in 0..tables_len {
            let table = TableSchema::from_bytes(cursor).map_err(|_| MessageError::CursorError)?;
            tables.push(table);
        }

        Ok(KeyspaceSchema {
            inner: keyspace,
            tables,
        })
    }
}

impl ApplicationState {
    /// Create a new `ApplicationState` message.
    pub fn new(status: NodeStatus, version: u32, schema: Schema) -> Self {
        ApplicationState {
            status,
            version,
            schema,
        }
    }

    pub fn set_schema(&mut self, schema: Schema) {
        self.schema = schema;
        self.version += 1;
    }

    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |  status |   0x00  |
    /// +----+----+----+----+
    /// |      version      |
    /// +----+----+----+----+
    /// |       schema      |
    /// |        ...        |
    /// +----+----+----+----+
    /// ```
    /// Convert the `ApplicationState` message to a byte slice.
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let status_bytes = (self.status as u16).to_be_bytes();
        let version_bytes = self.version.to_be_bytes();

        bytes.extend_from_slice(&status_bytes);
        bytes.extend_from_slice(&version_bytes);

        let schemas_bytes = self.schema.to_bytes();

        bytes.extend_from_slice(&schemas_bytes);

        bytes
    }

    /// Create an `ApplicationState` message from a byte slice.
    pub fn from_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Self, MessageError> {
        let mut status_bytes = [0u8; 2];
        cursor
            .read_exact(&mut status_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let status_value = u16::from_be_bytes(status_bytes);

        let mut version_bytes = [0u8; 4];
        cursor
            .read_exact(&mut version_bytes)
            .map_err(|_| MessageError::CursorError)?;
        let version = u32::from_be_bytes(version_bytes);

        let status = match status_value {
            0 => NodeStatus::Bootstrap,
            1 => NodeStatus::Normal,
            2 => NodeStatus::Leaving,
            3 => NodeStatus::Removing,
            4 => NodeStatus::Dead,
            _ => {
                return Err(MessageError::InvalidValue(format!(
                    "Invalid NodeStatus value: {}",
                    status_value
                )))
            }
        };

        let schema = Schema::from_bytes(cursor)?;

        Ok(ApplicationState {
            status,
            version,
            schema,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Debug, Default)]
/// Represents the status of the node in the cluster.
/// - `Bootstrap`: The node is bootstrapping.
/// - `Normal`: The node is in the cluster.
/// - `Leaving`: The node is leaving the cluster.
/// - `Removing`: The node is being removed from the cluster.
/// - `Dead`: The node is dead.
pub enum NodeStatus {
    #[default]
    /// The node is in the process of joining the cluster.
    Bootstrap = 0x0,
    /// The node is in the cluster, and is fully operational.
    Normal = 0x1,
    /// The node is in the process of leaving the cluster.
    Leaving = 0x2,
    /// The node is in the process of being removed from the cluster.
    Removing = 0x3,
    /// The node is dead. Rip.
    Dead = 0x4,
}

impl NodeStatus {
    pub fn is_dead(&self) -> bool {
        matches!(self, NodeStatus::Dead)
    }

    pub fn is_normal(&self) -> bool {
        matches!(self, NodeStatus::Normal)
    }

    pub fn is_leaving(&self) -> bool {
        matches!(self, NodeStatus::Leaving)
    }

    pub fn is_starting(&self) -> bool {
        matches!(self, NodeStatus::Bootstrap)
    }

    pub fn is_removing(&self) -> bool {
        matches!(self, NodeStatus::Removing)
    }

    pub fn is_alive(&self) -> bool {
        !self.is_dead()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use query_creator::clauses::{
        keyspace::create_keyspace_cql::CreateKeyspace,
        table::create_table_cql::CreateTable,
        types::{column::Column, datatype::DataType},
    };

    use crate::structures::application_state::{
        ApplicationState, CursorSerializable, KeyspaceSchema, NodeStatus, Schema, TableSchema,
    };

    #[test]
    fn app_state_to_from_bytes() {
        let app_state = ApplicationState::new(NodeStatus::Bootstrap, 1, Schema::new());

        let bytes = app_state.as_bytes();

        let mut cursor = std::io::Cursor::new(bytes.as_slice());

        let app_state = ApplicationState::from_bytes(&mut cursor).unwrap();

        assert_eq!(app_state.status, NodeStatus::Bootstrap);
        assert_eq!(app_state.version, 1);
    }

    #[test]
    fn column_to_from_bytes() {
        let expected_column = Column {
            name: "table".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            allows_null: false,
            is_clustering_column: false,
            is_partition_key: false,
            clustering_order: "asc".to_string(),
        };

        let bytes = expected_column.to_bytes();

        let mut cursor = std::io::Cursor::new(bytes.as_slice());

        let column = Column::from_bytes(&mut cursor).unwrap();

        assert_eq!(expected_column, column);
    }

    #[test]
    fn create_table_to_from_bytes() {
        let expected_table = CreateTable {
            name: "table".to_string(),
            keyspace_used_name: "keyspace".to_string(),
            if_not_exists_clause: false,
            columns: vec![Column {
                name: "table".to_string(),
                data_type: DataType::String,
                is_primary_key: false,
                allows_null: false,
                is_clustering_column: false,
                is_partition_key: false,
                clustering_order: "asc".to_string(),
            }],
            clustering_columns_in_order: vec![],
        };

        let bytes = expected_table.to_bytes();

        let mut cursor = std::io::Cursor::new(bytes.as_slice());

        let table = CreateTable::from_bytes(&mut cursor).unwrap();

        assert_eq!(table, expected_table);
    }

    #[test]
    fn table_schema_to_from_bytes() {
        let table_schema = TableSchema {
            inner: CreateTable {
                name: "table".to_string(),
                keyspace_used_name: "keyspace".to_string(),
                if_not_exists_clause: false,
                columns: vec![Column {
                    name: "table".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    allows_null: false,
                    is_clustering_column: false,
                    is_partition_key: false,
                    clustering_order: "asc".to_string(),
                }],
                clustering_columns_in_order: vec![],
            },
        };

        let bytes = table_schema.to_bytes();

        let mut cursor = std::io::Cursor::new(bytes.as_slice());

        let table = TableSchema::from_bytes(&mut cursor).unwrap();

        assert_eq!(table_schema, table);
    }

    #[test]
    fn keyspace_to_from_bytes() {
        let expected_keyspace = KeyspaceSchema {
            inner: CreateKeyspace::default(),
            tables: vec![TableSchema {
                inner: CreateTable {
                    name: "table".to_string(),
                    keyspace_used_name: "keyspace".to_string(),
                    if_not_exists_clause: false,
                    columns: vec![Column {
                        name: "table".to_string(),
                        data_type: DataType::String,
                        is_primary_key: false,
                        allows_null: false,
                        is_clustering_column: false,
                        is_partition_key: false,
                        clustering_order: "asc".to_string(),
                    }],
                    clustering_columns_in_order: vec![],
                },
            }],
        };

        let bytes = expected_keyspace.to_bytes();

        let mut cursor = std::io::Cursor::new(bytes.as_slice());

        let keyspace = KeyspaceSchema::from_bytes(&mut cursor).unwrap();

        assert_eq!(keyspace, expected_keyspace);
    }

    #[test]
    fn schema_to_from_bytes() {
        let expected_schema = Schema {
            timestamp: 100,
            keyspaces: HashMap::from([(
                "keyspace".to_string(),
                KeyspaceSchema {
                    inner: CreateKeyspace::default(),
                    tables: vec![
                        TableSchema {
                            inner: CreateTable {
                                name: "table".to_string(),
                                keyspace_used_name: "keyspace".to_string(),
                                if_not_exists_clause: false,
                                columns: vec![],
                                clustering_columns_in_order: vec![],
                            },
                        },
                        TableSchema {
                            inner: CreateTable {
                                name: "table2".to_string(),
                                keyspace_used_name: "keyspace".to_string(),
                                if_not_exists_clause: false,
                                columns: vec![],
                                clustering_columns_in_order: vec![],
                            },
                        },
                    ],
                },
            )]),
        };

        let bytes = expected_schema.to_bytes();

        let mut cursor = std::io::Cursor::new(bytes.as_slice());

        let schema = Schema::from_bytes(&mut cursor).unwrap();

        assert_eq!(expected_schema, schema);
    }
}
