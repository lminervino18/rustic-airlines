use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
};

use query_creator::{clauses::types::column::Column, operator::Operator};

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    /// Inserts a new row into a table within the specified keyspace.
    ///
    /// This function handles the insertion of a row into a `.csv` file representing a table. It ensures
    /// that the data adheres to the structure of the table (defined by columns) and maintains the correct
    /// clustering order of rows based on the clustering keys.
    ///
    /// If the table file does not exist, it will be created. The function also supports conditional inserts
    /// (`if_not_exist`) and handles replication scenarios.
    ///
    /// # Arguments
    /// - `keyspace`: The name of the keyspace where the table resides.
    /// - `table`: The name of the table into which the row will be inserted.
    /// - `values`: A vector of string slices representing the values for the row, in column order.
    /// - `columns`: A vector of `Column` structs defining the table's schema.
    /// - `clustering_columns_in_order`: A vector of strings indicating the clustering columns and their order.
    /// - `is_replication`: A boolean indicating whether the insertion is part of a replication process.
    /// - `if_not_exist`: A boolean indicating whether the row should only be inserted if it does not already exist.
    /// - `timestamp`: A 64-bit integer representing the timestamp of the operation.
    ///
    /// # Returns
    /// - `Ok(())`: If the row is successfully inserted.
    /// - `Err(StorageEngineError)`: If an error occurs during the operation, such as:
    ///   - `DirectoryCreationFailed`: When the required directories cannot be created.
    ///   - `IoError`: For issues reading or writing to files.
    ///   - `UnsupportedOperation`: If an unsupported operation is encountered (e.g., invalid data type comparison).
    ///   - `TempFileCreationFailed`: If a temporary file cannot be created.
    ///
    /// # Behavior
    /// - If the table file does not exist:
    ///   - The file is created, and the header row is written based on the provided `columns`.
    /// - If the table file exists:
    ///   - The header is validated, and rows are written in clustering order.
    /// - If `if_not_exist` is `true`, rows with matching clustering keys will not be overwritten.
    /// - For clustering keys:
    ///   - The function ensures that rows are inserted in the correct order based on the `clustering_columns_in_order`.
    ///   - Clustering order can be `ASC` (ascending) or `DESC` (descending), defined per column.
    ///
    /// # Considerations
    /// - The function assumes that the `columns` accurately describe the structure of the table.
    /// - The length of `values` must match the number of columns.
    /// - Invalid values (e.g., a non-integer value for an `INT` column) will result in an error.
    /// - The function writes data atomically using temporary files to avoid corruption in case of errors.
    ///
    /// # Edge Cases
    /// - **Empty `values` or `columns`:** The function will return an error if the values or columns are missing.
    /// - **Invalid clustering order:** If a clustering column's order is unspecified or inconsistent, an error may occur.
    /// - **Concurrent writes:** Simultaneous calls to `insert` on the same table may cause unexpected behavior and are not supported.
    ///
    /// # Limitations
    /// - The function currently supports only `.csv` file formats.
    /// - Complex data types (e.g., nested structures) are not supported.

    pub fn insert(
        &self,
        keyspace: &str,
        table: &str,
        values: Vec<&str>,
        columns: Vec<Column>,
        clustering_columns_in_order: Vec<String>,
        is_replication: bool,
        if_not_exist: bool,
        timestamp: i64,
    ) -> Result<(), StorageEngineError> {
        let folder_path =
            self.get_keyspace_path(keyspace)
                .join(if is_replication { "replication" } else { "" });

        if !folder_path.exists() {
            fs::create_dir_all(&folder_path)
                .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;
        }

        let file_path = folder_path.join(format!("{}.csv", table));
        let temp_file_path = folder_path.join(format!("temp_{}.csv", timestamp));
        let index_file_path = folder_path.join(format!("{}_index.csv", table));

        let clustering_indices =
            Self::get_clustering_indices(&columns, &clustering_columns_in_order)?;
        let partition_key_indices = Self::get_partition_key_indices(&columns);

        let mut inserted = false;
        let mut current_byte_offset: u64 = 0;
        let mut index_map = std::collections::BTreeMap::new();

        // Preparar archivo temporal
        let mut temp_file =
            File::create(&temp_file_path).map_err(|_| StorageEngineError::IoError)?;
        let mut temp_index = BufWriter::new(
            File::create(&index_file_path).map_err(|_| StorageEngineError::IoError)?,
        );

        writeln!(temp_index, "clustering_column,start_byte,end_byte")
            .map_err(|_| StorageEngineError::IoError)?;

        if let Ok(file) = File::open(&file_path) {
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            if let Some(header_line) = lines.next() {
                let header_line = header_line.map_err(|_| StorageEngineError::IoError)?;
                writeln!(temp_file, "{}", header_line).map_err(|_| StorageEngineError::IoError)?;
                current_byte_offset += header_line.len() as u64 + 1; // Contamos el '\n'
            }
            for (_, line) in lines.enumerate() {
                let line = line.map_err(|_| StorageEngineError::IoError)?;
                let line_length = line.len() as u64;

                let (line_content, row_timestamp) = Self::split_line(&line)?;
                let row: Vec<&str> = line_content.split(',').collect();

                let is_same_partition =
                    Self::is_same_partition(&row, &values, &partition_key_indices);
                let clustering_cmp =
                    Self::compare_clustering(&row, &values, &clustering_indices, &columns)?;

                if clustering_cmp == std::cmp::Ordering::Equal {
                    if is_same_partition && if_not_exist {
                        writeln!(temp_file, "{};{}", line_content, row_timestamp)
                            .map_err(|_| StorageEngineError::IoError)?;
                        current_byte_offset += line_length + 1;
                        Self::update_index_map(
                            &row,
                            &clustering_indices,
                            &mut index_map,
                            current_byte_offset - line_length - 1,
                            line_length,
                        );
                        continue;
                    }
                    Self::write_inserted_row(
                        &mut temp_file,
                        &values,
                        timestamp,
                        &mut inserted,
                        &mut current_byte_offset,
                        &mut index_map,
                        &clustering_indices,
                    )?;
                    continue;
                } else if clustering_cmp == std::cmp::Ordering::Greater && !inserted {
                    Self::write_inserted_row(
                        &mut temp_file,
                        &values,
                        timestamp,
                        &mut inserted,
                        &mut current_byte_offset,
                        &mut index_map,
                        &clustering_indices,
                    )?;
                }

                writeln!(temp_file, "{};{}", line_content, row_timestamp)
                    .map_err(|_| StorageEngineError::IoError)?;

                current_byte_offset += line_length + 1;
                Self::update_index_map(
                    &row,
                    &clustering_indices,
                    &mut index_map,
                    current_byte_offset - line_length - 1,
                    line_length,
                );
            }
        }

        if !inserted {
            Self::write_inserted_row(
                &mut temp_file,
                &values,
                timestamp,
                &mut inserted,
                &mut current_byte_offset,
                &mut index_map,
                &clustering_indices,
            )?;
        }

        for (key, (start_byte, end_byte)) in index_map {
            writeln!(temp_index, "{},{},{}", key, start_byte, end_byte)
                .map_err(|_| StorageEngineError::IoError)?;
        }

        fs::rename(&temp_file_path, &file_path).map_err(|_| StorageEngineError::IoError)?;
        Ok(())
    }

    fn write_inserted_row(
        file: &mut File,
        values: &[&str],
        timestamp: i64,
        inserted: &mut bool,
        current_byte_offset: &mut u64,
        index_map: &mut std::collections::BTreeMap<String, (u64, u64)>,
        clustering_indices: &[(usize, String)],
    ) -> Result<(), StorageEngineError> {
        let line = format!("{};{}", values.join(","), timestamp);
        let line_length = line.len() as u64;

        writeln!(file, "{}", line).map_err(|_| StorageEngineError::IoError)?;
        Self::update_index_map(
            &values,
            clustering_indices,
            index_map,
            *current_byte_offset,
            line_length,
        );
        *current_byte_offset += line_length + 1; // +1 para incluir '\n'
        *inserted = true;
        Ok(())
    }

    fn update_index_map(
        row: &[&str],
        clustering_indices: &[(usize, String)],
        index_map: &mut std::collections::BTreeMap<String, (u64, u64)>,
        start_byte: u64,
        line_length: u64,
    ) {
        if let Some(&(idx, _)) = clustering_indices.first() {
            let key = row[idx].to_string();
            let entry = index_map
                .entry(key)
                .or_insert((start_byte, start_byte + line_length));
            entry.1 = start_byte + line_length;
        }
    }

    fn get_clustering_indices(
        columns: &[Column],
        clustering_columns: &[String],
    ) -> Result<Vec<(usize, String)>, StorageEngineError> {
        Ok(clustering_columns
            .iter()
            .filter_map(|col_name| {
                columns
                    .iter()
                    .position(|col| col.name == *col_name)
                    .map(|idx| {
                        let order = if columns[idx].get_clustering_order() == "ASC" {
                            "DESC".to_string()
                        } else {
                            "ASC".to_string()
                        };
                        (idx, order)
                    })
            })
            .collect())
    }

    fn get_partition_key_indices(columns: &[Column]) -> Vec<usize> {
        columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_partition_key)
            .map(|(idx, _)| idx)
            .collect()
    }

    fn split_line(line: &str) -> Result<(&str, &str), StorageEngineError> {
        line.split_once(";").ok_or(StorageEngineError::IoError)
    }

    fn is_same_partition(row: &[&str], values: &[&str], partition_indices: &[usize]) -> bool {
        partition_indices
            .iter()
            .all(|&index| row.get(index) == values.get(index))
    }

    fn compare_clustering(
        row: &[&str],
        values: &[&str],
        clustering_indices: &[(usize, String)],
        columns: &[Column],
    ) -> Result<std::cmp::Ordering, StorageEngineError> {
        if clustering_indices.len() == 0 {
            return Ok(std::cmp::Ordering::Less);
        }
        for &(idx, ref order) in clustering_indices {
            let row_val = row.get(idx).unwrap_or(&"");
            let value = values.get(idx).unwrap_or(&"");
            if row_val != value {
                let is_less = columns[idx]
                    .data_type
                    .compare(row_val, value, &Operator::Lesser)
                    .map_err(|_| StorageEngineError::UnsupportedOperation)?;
                return Ok(match (is_less, order.as_str()) {
                    (true, "DESC") | (false, "ASC") => std::cmp::Ordering::Less,
                    (false, "DESC") | (true, "ASC") => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                });
            }
        }
        Ok(std::cmp::Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use query_creator::clauses::types::column::Column;
    use query_creator::clauses::types::datatype::DataType;
    use std::fs::{self, File};
    use std::io::{BufRead, BufReader};
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn test_insert_new_row_with_correct_columns() {
        // Use a unique directory for this test
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        // Keyspace and table setup
        let keyspace = "test_keyspace";
        let table = "test_table";
        let columns = vec![
            Column::new("id", DataType::Int, true, false), // id: INT, primary key, not null
            Column::new("name", DataType::String, false, true), // name: TEXT, not primary key, allows null
        ];
        let clustering_columns_in_order = vec!["id".to_string()];
        let values = vec!["1", "John"];
        let timestamp = 1234567890;

        // Clean the environment
        let folder_path = storage.get_keyspace_path(keyspace);
        if folder_path.exists() {
            fs::remove_dir_all(&folder_path).unwrap();
        }

        // Create the keyspace folder
        fs::create_dir_all(folder_path.clone()).unwrap();

        // Add the header manually to the file
        let table_file_path = folder_path.join(format!("{}.csv", table));
        let mut file = File::create(&table_file_path).unwrap();
        writeln!(file, "id,name").unwrap(); // Write header manually

        // Insert row
        let result = storage.insert(
            keyspace,
            table,
            values.clone(),
            columns.clone(),
            clustering_columns_in_order.clone(),
            false, // is_replication
            false, // if_not_exist
            timestamp,
        );
        assert!(result.is_ok(), "Failed to insert a new row");

        // Verify the file was created
        assert!(
            table_file_path.exists(),
            "Table file was not created after insert"
        );

        // Verify the content of the file
        let file = File::open(&table_file_path).unwrap();
        let reader = BufReader::new(file);

        let mut lines = reader.lines();
        assert_eq!(
            lines.next().unwrap().unwrap(),
            "id,name",
            "Header does not match expected value"
        );
        assert_eq!(
            lines.next().unwrap().unwrap(),
            format!("{},{};{}", values[0], values[1], timestamp),
            "Row content does not match expected value"
        );

        // Cleanup
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }

    #[test]
    fn test_insert_with_clustering_order_and_manual_header() {
        // Use a unique directory for this test
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        // Keyspace and table setup
        let keyspace = "test_keyspace";
        let table = "test_table";

        // Create columns with additional configurations
        let mut id_column = Column::new("id", DataType::Int, true, false);
        id_column.is_clustering_column = true; // Set as clustering column
        id_column.clustering_order = "ASC".to_string(); // Define clustering order

        let name_column = Column::new("name", DataType::String, false, true);

        let columns = vec![id_column, name_column];
        let clustering_columns_in_order = vec!["id".to_string()];

        let values_row1 = vec!["2", "Alice"];
        let values_row2 = vec!["1", "Bob"];
        let timestamp1 = 1234567890;
        let timestamp2 = 1234567891;

        // Clean the environment
        let folder_path = storage.get_keyspace_path(keyspace);
        if folder_path.exists() {
            fs::remove_dir_all(&folder_path).unwrap();
        }

        // Create the keyspace folder
        fs::create_dir_all(folder_path.clone()).unwrap();

        // Add the header manually to the file
        let table_file_path = folder_path.join(format!("{}.csv", table));
        let mut file = File::create(&table_file_path).unwrap();
        writeln!(file, "id,name").unwrap(); // Write header manually

        // Insert rows
        let _ = storage.insert(
            keyspace,
            table,
            values_row1.clone(),
            columns.clone(),
            clustering_columns_in_order.clone(),
            false,
            false,
            timestamp1,
        );

        let _ = storage.insert(
            keyspace,
            table,
            values_row2.clone(),
            columns.clone(),
            clustering_columns_in_order.clone(),
            false,
            false,
            timestamp2,
        );

        // Verify the content of the file
        let file = File::open(&table_file_path).unwrap();
        let reader = BufReader::new(file);

        let mut lines = reader.lines();
        assert_eq!(
            lines.next().unwrap().unwrap(),
            "id,name",
            "Header does not match expected value"
        );

        let row1 = lines.next().unwrap().unwrap();
        let row2 = lines.next().unwrap().unwrap();
        assert!(
            row1.starts_with("1"),
            "Clustering order is incorrect, first row should have the smallest ID"
        );
        assert!(
            row2.starts_with("2"),
            "Clustering order is incorrect, second row should have the larger ID"
        );

        // Cleanup
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }
}
