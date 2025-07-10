use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Seek},
};

use gossip::structures::application_state::TableSchema;
use query_creator::clauses::select_cql::Select;

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    /// Executes a `SELECT` query on a table stored as CSV files, returning rows that match the given conditions.
    ///
    /// # Parameters
    ///
    /// - `select_query`:
    ///   An instance of the `Select` struct representing the SQL-like `SELECT` query.
    ///   Includes details such as selected columns, `WHERE` conditions, `ORDER BY` clause, and `LIMIT`.
    ///
    /// - `table`:
    ///   The `Table` instance containing metadata for the target table, including column definitions,
    ///   clustering columns, and primary keys.
    ///
    /// - `is_replication`:
    ///   A boolean indicating whether the query targets replicated data (`true`) or the main data (`false`).
    ///
    /// - `keyspace`:
    ///   The name of the keyspace containing the table. Used to locate the file paths for the table.
    ///
    /// # Returns
    ///
    /// - `Ok(Vec<String>)`:
    ///   A vector of strings representing the selected rows. The first two entries include:
    ///     - **Complete column names**: Header with all columns in the table.
    ///     - **Selected columns**: Header with the columns specified in the `SELECT` query.
    ///   The remaining entries are rows from the table matching the conditions in the `WHERE` clause,
    ///   formatted as strings with values separated by commas.
    ///
    /// - `Err(StorageEngineError)`:
    ///   If an error occurs during the query execution, such as missing files, invalid syntax,
    ///   or failure to apply conditions.
    ///
    /// # Detailed Steps
    ///
    /// 1. **Determine File Paths**:
    ///    - Constructs the folder and file paths for the table and its associated index files based on the `keyspace` and `is_replication`.
    ///
    /// 2. **Check for File Existence**:
    ///    - Ensures that the target directory exists. Creates it if missing.
    ///    - Opens the table's CSV file for reading the data and the index file for locating clustering keys.
    ///
    /// 3. **Index Processing**:
    ///    - Reads the index file to determine the byte range for rows matching the first clustering column in the `WHERE` clause.
    ///    - If a match is found, sets `start_byte` and `end_byte` to limit the data search within the file.
    ///
    /// 4. **Header Preparation**:
    ///    - Adds the complete column list (all table columns) and the selected column list (columns in the `SELECT` query) as the first two rows of the result.
    ///
    /// 5. **Row Filtering**:
    ///    - Reads rows within the specified byte range (or the entire file if no clustering column is specified).
    ///    - Evaluates each row against the `WHERE` clause conditions using the `line_matches_where_clause` helper function.
    ///    - Adds rows matching the conditions to the result vector.
    ///
    /// 6. **Apply `LIMIT`**:
    ///    - Truncates the results to include only the specified number of rows if a `LIMIT` clause is present.
    ///
    /// 7. **Apply `ORDER BY`**:
    ///    - Sorts the results based on a single column and order (ascending or descending) if specified in the `ORDER BY` clause.
    ///    - Uses the `sort_results_single_column` helper function for sorting.
    ///
    /// 8. **Return Results**:
    ///    - Returns the vector of rows as `Ok(Vec<String>)`.
    ///    - If no rows match the conditions, the result includes only the headers.
    ///
    /// # Helper Functions
    ///
    /// - `line_matches_where_clause`:
    ///   Validates if a row matches the `WHERE` clause conditions. Converts the row into a key-value map of column names to values
    ///   and evaluates the conditions in the query.
    ///
    /// - `sort_results_single_column`:
    ///   Sorts the result rows based on a single column specified in the `ORDER BY` clause. Supports ascending (`ASC`) and descending (`DESC`) orders.
    ///
    /// # Errors
    ///
    /// - **`StorageEngineError::DirectoryCreationFailed`**:
    ///   If the directory for the keyspace or replication files cannot be created.
    ///
    /// - **`StorageEngineError::MissingWhereClause`**:
    ///   If the `WHERE` clause is missing and is required for clustering column evaluation.
    ///
    /// - **`StorageEngineError::IoError`**:
    ///   For general input/output issues during file reading or seeking.
    ///
    /// - **`StorageEngineError::InvalidSyntax`**:
    ///   If the query syntax is invalid or unsupported.
    ///
    /// - **`StorageEngineError::IndexFileNotFound`**:
    ///   If the index file is missing or cannot be read while clustering column evaluation is required.

    pub fn select(
        &self,
        select_query: Select,
        table: TableSchema,
        is_replication: bool,
        keyspace: &str,
    ) -> Result<Vec<String>, StorageEngineError> {
        let table_name = table.get_name();
        let base_folder_path = self.get_keyspace_path(keyspace);

        // Construcción de la ruta de la carpeta según si es replicación o no
        let folder_path = if is_replication {
            base_folder_path.join("replication")
        } else {
            base_folder_path
        };

        // Crear la carpeta si no existe
        if !folder_path.exists() {
            fs::create_dir_all(&folder_path)
                .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;
        }

        // Rutas para los archivos de datos e índices
        let file_path = folder_path.join(format!("{}.csv", table_name));
        let index_file_path = folder_path.join(format!("{}_index.csv", table_name));

        let file = OpenOptions::new().read(true).open(&file_path)?;
        let index_file = OpenOptions::new().read(true).open(&index_file_path)?;
        let mut reader = BufReader::new(file);

        // Leer los índices
        let index_reader = BufReader::new(index_file);
        let mut start_byte = 0;
        let mut end_byte = u64::MAX;

        // Obtener la primera columna de clustering y sus valores
        if let Some(first_clustering_column) = table.get_clustering_column_in_order().get(0) {
            let clustering_value = select_query
                .clone()
                .where_clause
                .ok_or(StorageEngineError::MissingWhereClause)?
                .get_value_for_clustering_column(&first_clustering_column);

            if let Some(clustering_column_value) = clustering_value {
                for (i, line) in index_reader.lines().enumerate() {
                    if i == 0 {
                        // Saltar el header del archivo de índices
                        continue;
                    }
                    let line = line?;
                    let parts: Vec<&str> = line.split(',').collect();
                    if parts.len() == 3 && parts[0] == clustering_column_value {
                        start_byte = parts[1].parse::<u64>().unwrap_or(0);
                        end_byte = parts[2].parse::<u64>().unwrap_or(u64::MAX);
                        break;
                    }
                }
            }
        }

        // Posicionar el lector en el rango de bytes
        if start_byte > 0 {
            reader.seek(std::io::SeekFrom::Start(start_byte))?;
        } else {
            // Si no se encontró la clustering column, saltar el header manualmente
            let mut buffer = String::new();
            reader.read_line(&mut buffer)?; // Leer y descartar el header
        }

        let mut results = Vec::new();
        let complete_columns: Vec<String> =
            table.get_columns().iter().map(|c| c.name.clone()).collect();
        results.push(complete_columns.join(","));
        results.push(select_query.columns.join(","));

        // Leer las líneas del rango especificado
        let mut current_byte_offset = start_byte;

        while current_byte_offset < end_byte {
            let mut buffer = String::new();
            let bytes_read = reader.read_line(&mut buffer)?;
            if bytes_read == 0 {
                break; // Fin del archivo
            }
            current_byte_offset += bytes_read as u64;
            let (line, _) = buffer
                .trim_end()
                .split_once(";")
                .ok_or(StorageEngineError::IoError)?;
            if self.line_matches_where_clause(&line, &table, &select_query)? {
                results.push(buffer.trim_end().to_string());
            }
        }

        // Aplicar `LIMIT` si está presente
        if let Some(limit) = select_query.limit {
            if limit < results.len() - 2 {
                results = results[..limit + 2].to_vec();
            }
        }

        // Ordenar los resultados si hay cláusula `ORDER BY`
        if let Some(order_by) = select_query.orderby_clause {
            self.sort_results_single_column(&mut results, &order_by.columns[0], &order_by.order)?
        }

        Ok(results)
    }

    fn sort_results_single_column(
        &self,
        results: &mut Vec<String>,
        order_by_column: &str,
        order: &str, // Either "ASC" or "DESC"
    ) -> Result<(), StorageEngineError> {
        if results.len() <= 3 {
            // No sorting needed if only headers or very few rows
            return Ok(());
        }

        // Separate the two headers
        let header1 = results[0].clone();
        let header2 = results[1].clone();
        let rows = &mut results[2..];

        // Get the index of the column specified in order_by_column
        let header_columns: Vec<&str> = header1.split(',').collect();
        let col_index = header_columns
            .iter()
            .position(|&col| col == order_by_column);

        if let Some(col_index) = col_index {
            // Define sort closure based on order
            rows.sort_by(|a, b| {
                let a_val = a.split(',').nth(col_index).unwrap_or("");
                let b_val = b.split(',').nth(col_index).unwrap_or("");
                let cmp = a_val.cmp(b_val);

                match order {
                    "ASC" => cmp,
                    "DESC" => cmp.reverse(),
                    _ => std::cmp::Ordering::Equal, // Ignore invalid order specifiers
                }
            });
        }

        // Restore headers
        results[0] = header1;
        results[1] = header2;
        Ok(())
    }

    fn line_matches_where_clause(
        &self,
        line: &str,
        table: &TableSchema,
        select_query: &Select,
    ) -> Result<bool, StorageEngineError> {
        // Convert the line into a map of column to value

        let values: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &values, false);

        let columns = table.get_columns();
        // Check the WHERE clause condition in the SELECT query
        if let Some(where_clause) = &select_query.where_clause {
            Ok(where_clause
                .condition
                .execute(&column_value_map, columns)
                .map_err(|_| StorageEngineError::MissingWhereClause)?)
        } else {
            Ok(true) // If no WHERE clause, consider the line as matching
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageEngine;
    use query_creator::clauses::table::create_table_cql::CreateTable;
    use query_creator::clauses::types::column::Column;
    use query_creator::clauses::types::datatype::DataType;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn test_select_existing_rows_with_where() {
        // Configuración de entorno único para la prueba
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        // Configuración de keyspace y tabla
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let columns = vec![
            Column::new("id", DataType::Int, true, false), // id: INT, primary key, not null
            Column::new("name", DataType::String, false, false), // name: TEXT, not primary key, allows null
        ];
        let clustering_columns_in_order = vec!["id".to_string()];
        let values_row1 = vec!["1", "John"];
        let values_row2 = vec!["2", "Jane"];
        let timestamp = 1234567890;

        // Limpiar el entorno
        let folder_path = storage.get_keyspace_path(keyspace);
        if folder_path.exists() {
            fs::remove_dir_all(&folder_path).unwrap();
        }

        // Crear el directorio del keyspace
        fs::create_dir_all(folder_path.clone()).unwrap();

        // Crear archivo de tabla y agregar la cabecera manualmente
        let table_file_path = folder_path.join(format!("{}.csv", table_name));
        let mut file = File::create(&table_file_path).unwrap();
        writeln!(file, "id,name").unwrap(); // Escribir cabecera manualmente

        // Insertar filas
        storage
            .insert(
                keyspace,
                table_name,
                values_row1.clone(),
                columns.clone(),
                clustering_columns_in_order.clone(),
                false, // no es replicación
                false, // no es si no existe
                timestamp,
            )
            .unwrap();

        storage
            .insert(
                keyspace,
                table_name,
                values_row2.clone(),
                columns.clone(),
                clustering_columns_in_order.clone(),
                false,
                false,
                timestamp,
            )
            .unwrap();

        // Crear la instancia de `Table` para el SELECT
        let create_table = CreateTable::new_from_tokens(vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "test_keyspace.test_table".to_string(),
            "id INT PRIMARY KEY, name TEXT".to_string(),
        ])
        .unwrap();
        let table = TableSchema::new(create_table.clone());
        // Crear consulta SELECT con `WHERE id = 1`
        let select_tokens = vec![
            "SELECT".to_string(),
            "id,name".to_string(),
            "FROM".to_string(),
            "test_keyspace.test_table".to_string(),
            "WHERE".to_string(),
            "id".to_string(),
            "=".to_string(),
            "1".to_string(),
        ];
        let select_query = Select::new_from_tokens(select_tokens).unwrap();
        // Ejecutar SELECT
        let result = storage.select(select_query, table, false, keyspace);
        assert!(result.is_ok(), "Error al ejecutar SELECT");
        let result_rows = result.unwrap();

        // Validar resultado
        assert_eq!(result_rows.len(), 3); // Cabecera + 2 filas (incluyendo SELECT)
        assert_eq!(result_rows[0], "id,name", "Cabecera incorrecta");
        assert_eq!(
            result_rows[1], "id,name",
            "Columnas seleccionadas incorrectas"
        );
        assert_eq!(
            result_rows[2], "1,John;1234567890",
            "Fila no coincide con el resultado esperado"
        );

        // Cleanup
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }

    #[test]
    fn test_select_with_limit() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let mut name_column = Column::new("name", DataType::String, false, false);
        name_column.is_clustering_column = true;
        let columns = vec![
            Column::new("id", DataType::Int, true, false),
            name_column,
            Column::new("age", DataType::Int, false, false),
        ];
        let clustering_columns_in_order = vec!["age".to_string()];
        let values_row1 = vec!["1", "John", "18"];
        let values_row2 = vec!["1", "Jaz", "19"];
        let values_row3 = vec!["1", "Jol", "20"];
        let timestamp = 1234567890;

        let folder_path = storage.get_keyspace_path(keyspace);
        if folder_path.exists() {
            fs::remove_dir_all(&folder_path).unwrap();
        }

        fs::create_dir_all(folder_path.clone()).unwrap();

        let table_file_path = folder_path.join(format!("{}.csv", table_name));
        let mut file = File::create(&table_file_path).unwrap();
        writeln!(file, "id,name").unwrap();

        storage
            .insert(
                keyspace,
                table_name,
                values_row1.clone(),
                columns.clone(),
                clustering_columns_in_order.clone(),
                false,
                false,
                timestamp,
            )
            .unwrap();

        storage
            .insert(
                keyspace,
                table_name,
                values_row2.clone(),
                columns.clone(),
                clustering_columns_in_order.clone(),
                false,
                false,
                timestamp,
            )
            .unwrap();

        storage
            .insert(
                keyspace,
                table_name,
                values_row3.clone(),
                columns.clone(),
                clustering_columns_in_order.clone(),
                false,
                false,
                timestamp,
            )
            .unwrap();

        let create_table = CreateTable::new_from_tokens(vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "test_keyspace.test_table".to_string(),
            "id INT , name TEXT, age INT, PRIMARY KEY (id, name)".to_string(),
        ])
        .unwrap();
        let table = TableSchema::new(create_table.clone());

        let select_tokens = vec![
            "SELECT".to_string(),
            "id,name".to_string(),
            "FROM".to_string(),
            "test_keyspace.test_table".to_string(),
            "WHERE".to_string(),
            "id".to_string(),
            "=".to_string(),
            "1".to_string(),
            "LIMIT".to_string(),
            "2".to_string(),
        ];

        let select_query = Select::new_from_tokens(select_tokens).unwrap();
        let result = storage.select(select_query, table, false, keyspace);
        assert!(result.is_ok(), "Error executing SELECT with LIMIT");
        let result_rows = result.unwrap();
        assert_eq!(result_rows.len(), 4); // Header + 2 rows use native_protocol::messages::result::schema_change::SchemaChange;
        assert_eq!(result_rows[0], "id,name,age", "Header mismatch");
        assert_eq!(result_rows[1], "id,name", "Selected columns mismatch");
        assert!(result_rows.contains(&"1,Jol,20;1234567890".to_string()));
        assert!(result_rows.contains(&"1,Jaz,19;1234567890".to_string()));

        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }

    #[test]
    fn test_select_with_not_matching_where() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let columns = vec![
            Column::new("id", DataType::Int, true, false),
            Column::new("name", DataType::String, false, false),
            Column::new("age", DataType::Int, false, false),
        ];
        let clustering_columns_in_order = vec!["id".to_string()];
        let values_row1 = vec!["1", "John", "18"];
        let values_row2 = vec!["1", "John", "19"];
        let values_row3 = vec!["3", "John", "20"];
        let timestamp = 1234567890;

        let folder_path = storage.get_keyspace_path(keyspace);
        if folder_path.exists() {
            fs::remove_dir_all(&folder_path).unwrap();
        }

        fs::create_dir_all(folder_path.clone()).unwrap();

        let table_file_path = folder_path.join(format!("{}.csv", table_name));
        let mut file = File::create(&table_file_path).unwrap();
        writeln!(file, "id,name").unwrap();

        storage
            .insert(
                keyspace,
                table_name,
                values_row1.clone(),
                columns.clone(),
                clustering_columns_in_order.clone(),
                false,
                false,
                timestamp,
            )
            .unwrap();

        storage
            .insert(
                keyspace,
                table_name,
                values_row2.clone(),
                columns.clone(),
                clustering_columns_in_order.clone(),
                false,
                false,
                timestamp,
            )
            .unwrap();

        storage
            .insert(
                keyspace,
                table_name,
                values_row3.clone(),
                columns.clone(),
                clustering_columns_in_order.clone(),
                false,
                false,
                timestamp,
            )
            .unwrap();

        let create_table = CreateTable::new_from_tokens(vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "test_keyspace.test_table".to_string(),
            "id INT PRIMARY KEY, name TEXT, age INT".to_string(),
        ])
        .unwrap();
        let table = TableSchema::new(create_table.clone());

        let select_tokens = vec![
            "SELECT".to_string(),
            "id,name".to_string(),
            "FROM".to_string(),
            "test_keyspace.test_table".to_string(),
            "WHERE".to_string(),
            "id".to_string(),
            "=".to_string(),
            "1".to_string(),
            "AND".to_string(),
            "name".to_string(),
            "=".to_string(),
            "Maca".to_string(),
        ];

        let select_query = Select::new_from_tokens(select_tokens).unwrap();
        let result = storage.select(select_query, table, false, keyspace);
        assert!(result.is_ok(), "Error executing SELECT with LIMIT");
        let result_rows = result.unwrap();
        assert_eq!(result_rows.len(), 2); // Header + 2 rows
        assert_eq!(result_rows[0], "id,name,age", "Header mismatch");
        assert_eq!(result_rows[1], "id,name", "Selected columns mismatch");

        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }
}
