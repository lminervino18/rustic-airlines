use super::{errors::StorageEngineError, StorageEngine};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};

impl StorageEngine {
    /// Creates a new table in the given keyspace.
    ///
    /// # Parameters
    ///
    /// * `keyspace`: The name of the keyspace where the table will be stored.
    /// * `table`: The name of the table to create.
    /// * `columns`: A vector of strings representing the names of the table columns.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the table is created successfully, or an error if it fails.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `StorageEngineError::DirectoryCreationFailed` if the directory for the table cannot be created.
    /// * `StorageEngineError::FileWriteFailed` if writing to the table or replication files fails.
    /// * `StorageEngineError::IoError` if an I/O error occurs while renaming files.
    pub fn create_table(
        &self,
        keyspace: &str,
        table: &str,
        columns: Vec<&str>,
    ) -> Result<(), StorageEngineError> {
        // Generate the folder name where the keyspace will be stored
        let keyspace_path = self.get_keyspace_path(keyspace);
        let replication_path = keyspace_path.join("replication");

        let primary_file_path = keyspace_path.join(format!("{}.csv", table));
        let replication_file_path = replication_path.join(format!("{}.csv", table));

        // Create the keyspace and replication folders if they don't exist
        std::fs::create_dir_all(&keyspace_path)
            .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;
        std::fs::create_dir_all(&replication_path)
            .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;

        // Create the file in the primary folder and write the columns as the header
        let mut primary_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&primary_file_path)
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        let header: Vec<String> = columns.iter().map(|col| col.to_string()).collect();
        writeln!(primary_file, "{}", header.join(","))
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        // Create the same file in the replication folder and write the columns as the header
        let mut replication_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&replication_file_path)
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        writeln!(replication_file, "{}", header.join(","))
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        // Create the index file in the primary folder
        let index_file_path = keyspace_path.join(format!("{}_index.csv", table));
        let mut index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&index_file_path)
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        writeln!(index_file, "clustering_column,start_byte,end_byte")
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        // Create the index file in the replication folder
        let replication_index_file_path = replication_path.join(format!("{}_index.csv", table));
        let mut replication_index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&replication_index_file_path)
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        writeln!(
            replication_index_file,
            "clustering_column,first_byte,last_byte"
        )
        .map_err(|_| StorageEngineError::FileWriteFailed)?;

        Ok(())
    }

    /// Drops a table from storage.
    ///
    /// # Parameters
    ///
    /// * `keyspace`: The name of the keyspace that contains the table.
    /// * `table`: The name of the table to drop.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the table is successfully dropped, or an error if it fails.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `StorageEngineError::FileDeletionFailed` if the table or replication files cannot be deleted.
    pub fn drop_table(&self, keyspace: &str, table: &str) -> Result<(), StorageEngineError> {
        let keyspace_path = self.get_keyspace_path(keyspace);
        let replication_path = keyspace_path.join("replication");

        // Paths for primary and replication files and index files
        let primary_file_path = keyspace_path.join(format!("{}.csv", table));
        let replication_file_path = replication_path.join(format!("{}.csv", table));
        let primary_index_path = keyspace_path.join(format!("{}_index.csv", table));
        let replication_index_path = replication_path.join(format!("{}_index.csv", table));

        // Remove the primary and replication files
        if let Err(_) = std::fs::remove_file(&primary_file_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        if let Err(_) = std::fs::remove_file(&replication_file_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        // Remove the primary and replication index files
        if let Err(_) = std::fs::remove_file(&primary_index_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        if let Err(_) = std::fs::remove_file(&replication_index_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        Ok(())
    }

    /// Adds a new column to a table in the specified keyspace.
    ///
    /// # Parameters
    ///
    /// * `keyspace`: The name of the keyspace that contains the table.
    /// * `table`: The name of the table where the column will be added.
    /// * `column`: The name of the new column to add.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the column is added successfully, or an error if it fails.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `StorageEngineError::IoError` if an I/O error occurs when adding the column to the file.
    pub fn add_column_to_table(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Result<(), StorageEngineError> {
        let keyspace_path = self.get_keyspace_path(keyspace);
        let file_path = keyspace_path.join(format!("{}.csv", table));
        let replica_path = keyspace_path
            .join("replication")
            .join(format!("{}.csv", table));

        Self::add_column_to_file(file_path.to_str().unwrap(), column)?;
        Self::add_column_to_file(replica_path.to_str().unwrap(), column)?;

        Ok(())
    }

    /// Removes a column from a table in the specified keyspace.
    ///
    /// # Parameters
    ///
    /// * `keyspace`: The name of the keyspace that contains the table.
    /// * `table`: The name of the table from which the column will be removed.
    /// * `column`: The name of the column to remove.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the column is removed successfully, or an error if it fails.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `StorageEngineError::UnsupportedOperation` if the column does not exist or cannot be removed.
    /// * `StorageEngineError::IoError` if an I/O error occurs when removing the column from the file.
    pub fn remove_column_from_table(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Result<(), StorageEngineError> {
        let keyspace_path = self.get_keyspace_path(keyspace);
        let file_path = keyspace_path.join(format!("{}.csv", table));
        let replica_path = keyspace_path
            .join("replication")
            .join(format!("{}.csv", table));

        Self::remove_column_from_file(file_path.to_str().unwrap(), column)?;
        Self::remove_column_from_file(replica_path.to_str().unwrap(), column)?;

        Ok(())
    }

    // Renames a column in a table of the specified keyspace.
    ///
    /// # Parameters
    ///
    /// * `keyspace`: The name of the keyspace that contains the table.
    /// * `table`: The name of the table where the column will be renamed.
    /// * `column`: The current name of the column to rename.
    /// * `new_column`: The new name for the column.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the column is renamed successfully, or an error if it fails.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `StorageEngineError::IoError` if an I/O error occurs when renaming the column in the file.
    pub fn rename_column_from_table(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
        new_column: &str,
    ) -> Result<(), StorageEngineError> {
        let keyspace_path = self.get_keyspace_path(keyspace);
        let file_path = keyspace_path.join(format!("{}.csv", table));
        let replica_path = keyspace_path
            .join("replication")
            .join(format!("{}.csv", table));

        Self::rename_column_in_file(file_path.to_str().unwrap(), column, new_column)?;
        Self::rename_column_in_file(replica_path.to_str().unwrap(), column, new_column)?;

        Ok(())
    }

    pub(crate) fn add_column_to_file(
        file_path: &str,
        column_name: &str,
    ) -> Result<(), StorageEngineError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = BufReader::new(file);
        let mut first_line = true;

        for line in reader.lines() {
            let mut line = line?;
            if first_line {
                line.push_str(&format!(",{}", column_name));
                first_line = false;
            } else {
                line.push_str(","); // Append an empty cell for the new column in each row
            }
            writeln!(temp_file, "{}", line)?;
        }

        fs::rename(temp_path, file_path).map_err(|_| StorageEngineError::IoError)
    }

    pub(crate) fn remove_column_from_file(
        file_path: &str,
        column_name: &str,
    ) -> Result<(), StorageEngineError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = BufReader::new(file);
        let mut col_index: Option<usize> = None;

        for line in reader.lines() {
            let line = line?;
            let cells: Vec<&str> = line.split(',').collect();

            if col_index.is_none() {
                col_index = cells.iter().position(|&col| col == column_name);
                if col_index.is_none() {
                    return Err(StorageEngineError::UnsupportedOperation);
                }
            }

            let filtered_line: Vec<&str> = cells
                .iter()
                .enumerate()
                .filter(|&(i, _)| Some(i) != col_index)
                .map(|(_, &cell)| cell)
                .collect();

            writeln!(temp_file, "{}", filtered_line.join(","))?;
        }

        fs::rename(temp_path, file_path).map_err(|_| StorageEngineError::IoError)
    }

    pub(crate) fn rename_column_in_file(
        file_path: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), StorageEngineError> {
        let temp_path = format!("{}.temp", file_path);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&temp_path)?;

        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = BufReader::new(file);

        for (i, line) in reader.lines().enumerate() {
            let mut line = line?;
            if i == 0 {
                line = line.replace(old_name, new_name); // Rename in the header
            }
            writeln!(temp_file, "{}", line)?;
        }

        fs::rename(temp_path, file_path).map_err(|_| StorageEngineError::IoError)
    }
}

#[cfg(test)]
mod tests {
    use super::StorageEngine;
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn test_create_table() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let columns = vec!["id", "name", "age"];

        let storage = StorageEngine::new(root.clone(), "127.0.0.1".to_string());

        // Ejecutar create_table
        let result = storage.create_table(keyspace, table_name, columns);
        assert!(result.is_ok(), "Failed to create table");

        let keyspace_path = root.join(format!("keyspaces_of_127_0_0_1")).join(keyspace);
        let primary_file_path = keyspace_path.join(format!("{}.csv", table_name));

        // Verificar que el archivo de la tabla ha sido creado
        assert!(primary_file_path.exists(), "Table file not created");

        // Verificar que el archivo de replicación también ha sido creado
        let replication_path = keyspace_path.join("replication");
        let replication_file_path = replication_path.join(format!("{}.csv", table_name));
        assert!(
            replication_file_path.exists(),
            "Replication file not created"
        );

        // Verificar que el archivo de índices ha sido creado
        let index_file_path = keyspace_path.join(format!("{}_index.csv", table_name));
        assert!(index_file_path.exists(), "Index file not created");
    }

    #[test]
    fn test_drop_table() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let columns = vec!["id", "name", "age"];

        let storage = StorageEngine::new(root.clone(), "127.0.0.1".to_string());

        // Crear la tabla primero
        let result = storage.create_table(keyspace, table_name, columns);
        assert!(result.is_ok(), "Failed to create table");

        // Ejecutar drop_table
        let result = storage.drop_table(keyspace, table_name);
        assert!(result.is_ok(), "Failed to drop table");

        let keyspace_path = root.join(format!("keyspaces_of_127_0_0_1")).join(keyspace);
        let primary_file_path = keyspace_path.join(format!("{}.csv", table_name));

        // Verificar que el archivo de la tabla ha sido eliminado
        assert!(!primary_file_path.exists(), "Table file not deleted");

        // Verificar que el archivo de replicación también ha sido eliminado
        let replication_file_path = keyspace_path
            .join("replication")
            .join(format!("{}.csv", table_name));
        assert!(
            !replication_file_path.exists(),
            "Replication file not deleted"
        );

        // Verificar que el archivo de índices ha sido eliminado
        let index_file_path = keyspace_path.join(format!("{}_index.csv", table_name));
        assert!(!index_file_path.exists(), "Index file not deleted");
    }

    #[test]
    fn test_add_column_to_table() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let columns = vec!["id", "name", "age"];

        let storage = StorageEngine::new(root.clone(), "127.0.0.1".to_string());

        // Crear la tabla primero
        let result = storage.create_table(keyspace, table_name, columns);
        assert!(result.is_ok(), "Failed to create table");

        // Agregar una columna a la tabla
        let result = storage.add_column_to_table(keyspace, table_name, "email");
        assert!(result.is_ok(), "Failed to add column");

        let keyspace_path = root.join(format!("keyspaces_of_127_0_0_1")).join(keyspace);
        let file_path = keyspace_path.join(format!("{}.csv", table_name));
        let file = File::open(file_path).expect("Failed to open table file");
        let reader = BufReader::new(file);
        let header = reader
            .lines()
            .next()
            .expect("Failed to read header")
            .unwrap();

        // Verificar que la nueva columna ha sido añadida al encabezado
        assert!(header.contains("email"), "Column not added");
    }

    #[test]
    fn test_remove_column_from_table() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let columns = vec!["id", "name", "age"];

        let storage = StorageEngine::new(root.clone(), "127.0.0.1".to_string());

        // Crear la tabla primero
        let result = storage.create_table(keyspace, table_name, columns);
        assert!(result.is_ok(), "Failed to create table");

        // Eliminar una columna de la tabla
        let result = storage.remove_column_from_table(keyspace, table_name, "age");
        assert!(result.is_ok(), "Failed to remove column");

        let keyspace_path = root.join(format!("keyspaces_of_127_0_0_1")).join(keyspace);
        let file_path = keyspace_path.join(format!("{}.csv", table_name));
        let file = File::open(file_path).expect("Failed to open table file");
        let reader = BufReader::new(file);
        let header = reader
            .lines()
            .next()
            .expect("Failed to read header")
            .unwrap();

        // Verificar que la columna "age" ha sido eliminada del encabezado
        assert!(!header.contains("age"), "Column not removed");
    }

    #[test]
    fn test_rename_column_from_table() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let columns = vec!["id", "name", "age"];

        let storage = StorageEngine::new(root.clone(), "127.0.0.1".to_string());

        // Crear la tabla primero
        let result = storage.create_table(keyspace, table_name, columns);
        assert!(result.is_ok(), "Failed to create table");

        // Renombrar una columna de la tabla
        let result = storage.rename_column_from_table(keyspace, table_name, "age", "years");
        assert!(result.is_ok(), "Failed to rename column");

        let keyspace_path = root.join(format!("keyspaces_of_127_0_0_1")).join(keyspace);
        let file_path = keyspace_path.join(format!("{}.csv", table_name));
        let file = File::open(file_path).expect("Failed to open table file");
        let reader = BufReader::new(file);
        let header = reader
            .lines()
            .next()
            .expect("Failed to read header")
            .unwrap();

        // Verificar que la columna "age" ha sido renombrada a "years"
        assert!(header.contains("years"), "Column not renamed");
    }
}
