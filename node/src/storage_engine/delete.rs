use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use gossip::structures::application_state::TableSchema;
use query_creator::clauses::delete_cql::Delete;

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    /// Deletes rows or specific column values from a table within the specified keyspace.
    ///
    /// This function performs deletion operations on a `.csv` file representing a table.
    /// The deletion can be targeted to specific rows or specific columns within a row based
    /// on the provided `WHERE` conditions in the `Delete` query.
    ///
    /// The function ensures that deletions respect clustering order and creates temporary files
    /// during operations to maintain atomicity.
    ///
    /// # Arguments
    ///
    /// - `delete_query`: A `Delete` struct that defines the delete operation, including conditions
    ///   (`WHERE`) and specific columns to delete.
    /// - `table`: A `Table` struct representing the schema of the table where the delete operation
    ///   will occur.
    /// - `keyspace`: A `&str` that specifies the keyspace containing the table.
    /// - `is_replication`: A `bool` indicating whether the operation is part of replication.
    /// - `timestamp`: A `i64` representing the timestamp of the operation, used for tracking updates.
    ///
    /// # Returns
    ///
    /// - `Ok(())`: If the delete operation is successfully completed.
    /// - `Err(StorageEngineError)`: If an error occurs during the operation, such as:
    ///   - `DirectoryCreationFailed`: When required directories cannot be created.
    ///   - `FileNotFound`: When the target file for deletion does not exist.
    ///   - `IoError`: For issues reading or writing to files.
    ///   - `TempFileCreationFailed`: If a temporary file cannot be created.
    ///   - `InvalidQuery`: If the provided query lacks required clauses or is invalid.
    ///
    /// # Behavior
    ///
    /// - If specific columns are specified in the `Delete` query:
    ///   - Only the specified columns will be cleared in rows that meet the `WHERE` condition.
    /// - If no columns are specified:
    ///   - Entire rows that meet the `WHERE` condition will be deleted.
    /// - If the table file does not exist:
    ///   - An error (`FileNotFound`) is returned.
    /// - Temporary files are created during the operation to avoid corruption of the original file.
    ///
    /// # Edge Cases
    ///
    /// - **No `WHERE` Clause:** If the `WHERE` clause is missing in the `Delete` query,
    ///   the function returns an `InvalidQuery` error.
    /// - **Non-Existing Columns:** If the specified columns do not exist in the table,
    ///   they are ignored.
    /// - **Concurrent Writes:** Simultaneous delete operations on the same table may cause
    ///   unexpected behavior and are not supported.
    ///
    /// # Limitations
    ///
    /// - The function operates only on `.csv` file formats.
    /// - The `IF` clause in the `Delete` query is currently ignored and must be set to `None`.
    /// - Complex conditions are supported via the `WHERE` clause but may require careful schema validation.

    pub fn delete(
        &self,
        delete_query: Delete,
        table: TableSchema,
        keyspace: &str,
        is_replication: bool,
        timestamp: i64,
    ) -> Result<(), StorageEngineError> {
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

        // Rutas para los archivos de datos y de índices
        let file_path = folder_path.join(format!("{}.csv", table_name));
        let temp_file_path = folder_path.join(format!(
            "{}.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| StorageEngineError::TempFileCreationFailed)?
                .as_nanos()
        ));
        let index_file_path = folder_path.join(format!("{}_index.csv", table_name));
        let temp_index_file_path = folder_path.join(format!(
            "{}_index.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| StorageEngineError::TempFileCreationFailed)?
                .as_nanos()
        ));

        // Abrir el archivo original, si no existe retornar error
        let file = OpenOptions::new()
            .read(true)
            .open(&file_path)
            .map_err(|_| StorageEngineError::FileNotFound)?;
        let reader = BufReader::new(file);

        // Crear los archivos temporales para datos y para índices
        let mut temp_file = File::create(&temp_file_path)
            .map_err(|_| StorageEngineError::TempFileCreationFailed)?;
        let mut temp_index_file = File::create(&temp_index_file_path)
            .map_err(|_| StorageEngineError::TempFileCreationFailed)?;

        // Escribir el encabezado en el archivo temporal de índices
        writeln!(temp_index_file, "clustering_column,start_byte,end_byte")
            .map_err(|_| StorageEngineError::FileWriteFailed)?;

        // Variables para manejar índices
        let mut current_byte_offset: u64 = 0;
        let mut index_map: Vec<(String, (u64, u64))> = Vec::new();

        // Obtener los nombres y órdenes de las columnas de clustering
        let clustering_key_order: Vec<(usize, String)> = table
            .get_clustering_column_in_order()
            .iter()
            .filter_map(|col_name| {
                table.get_column_index(col_name).map(|idx| {
                    let order = table
                        .get_columns()
                        .iter()
                        .find(|col| &col.name == col_name)
                        .map(|col| col.clustering_order.clone()) // Suponiendo que `order` es un String en la columna
                        .unwrap_or_else(|| "ASC".to_string()); // Predeterminado a ASC si no se encuentra
                    (idx, order)
                })
            })
            .collect();

        // Iterar sobre cada línea del archivo original
        for (i, line) in reader.lines().enumerate() {
            let line = line.map_err(|_| StorageEngineError::IoError)?;
            let line_length = line.len() as u64;

            if i == 0 {
                current_byte_offset += line_length + 1;
                writeln!(temp_file, "{}", line)?;
                continue;
            }

            let (line, time_of_row) = line.split_once(";").ok_or(StorageEngineError::IoError)?;
            let mut columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();

            let mut write_line = true; // Flag para determinar si la línea debe ser escrita
            let mut changed_line = false;
            if let Some(columns_to_delete) = &delete_query.columns {
                // Si hay columnas específicas para eliminar, borra esos valores
                if self.should_delete_line(&table, &delete_query, &line)? {
                    for column_name in columns_to_delete {
                        if let Some(index) = table.get_column_index(column_name) {
                            columns[index] = "".to_string(); // Vaciar el valor de la columna específica
                        }
                    }
                } else {
                    // Si se debe borrar toda la fila, no la escribimos
                    write_line = true;
                    changed_line = true;
                }
            } else {
                // Si no hay columnas específicas, elimina la fila si se cumplen las condiciones
                if self.should_delete_line(&table, &delete_query, &line)? {
                    write_line = false;
                }
            }

            // Si la línea no debe ser eliminada, escribirla en el archivo temporal
            if write_line {
                let time_to_write = if changed_line {
                    &timestamp.to_string()
                } else {
                    time_of_row
                };
                writeln!(temp_file, "{};{}", columns.join(","), time_to_write)?;
                if let Some(&(idx, _)) = clustering_key_order.first() {
                    if let Some(key) = columns.get(idx) {
                        let entry = (
                            key.clone(),
                            (current_byte_offset, current_byte_offset + line_length),
                        );
                        index_map.push(entry);
                    }
                }
                current_byte_offset += line_length + 1;
            }
        }

        // Ordenar el archivo de índices según el orden de las clustering columns
        for (_, order) in &clustering_key_order {
            if order == "ASC" {
                index_map.sort_by(|a, b| a.0.cmp(&b.0));
            } else {
                index_map.sort_by(|a, b| b.0.cmp(&a.0));
            }
        }

        // Escribir el archivo de índices actualizado
        for (key, (start_byte, end_byte)) in index_map {
            writeln!(temp_index_file, "{},{},{}", key, start_byte, end_byte)
                .map_err(|_| StorageEngineError::FileWriteFailed)?;
        }

        // Reemplazar los archivos originales con los temporales
        fs::rename(&temp_file_path, &file_path)
            .map_err(|_| StorageEngineError::FileReplacementFailed)?;
        fs::rename(&temp_index_file_path, &index_file_path)
            .map_err(|_| StorageEngineError::FileReplacementFailed)?;

        Ok(())
    }

    /// Verifica si una línea cumple las condiciones para ser eliminada
    fn should_delete_line(
        &self,
        table: &TableSchema,
        delete_query: &Delete,
        line: &str,
    ) -> Result<bool, StorageEngineError> {
        let columns: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let column_value_map = self.create_column_value_map(table, &columns, false);

        let columns = table.get_columns();

        // Verificar la cláusula `WHERE`
        if let Some(where_clause) = &delete_query.where_clause {
            if where_clause
                .condition
                .execute(&column_value_map, columns.clone())
                .unwrap_or(false)
            {
                // Si la cláusula `IF` está presente, comprobarla
                if let Some(if_clause) = &delete_query.if_clause {
                    if !if_clause
                        .condition
                        .execute(&column_value_map, columns.clone())
                        .unwrap_or(false)
                    {
                        // Si la cláusula `IF` no coincide, no eliminar
                        return Ok(false);
                    }
                }
                // Si `WHERE` se cumple y (si existe) `IF` también, eliminar
                return Ok(true);
            } else {
                // Si `WHERE` no se cumple, no eliminar
                return Ok(false);
            }
        } else {
            // Si falta la cláusula `WHERE`, devolver un error
            return Err(StorageEngineError::InvalidQuery);
        }
    }
}

#[cfg(test)]
mod tests {
    use query_creator::clauses::where_cql::Where;

    use super::*;
    use query_creator::clauses::condition::Condition;
    use query_creator::clauses::delete_cql::Delete;
    use query_creator::clauses::table::create_table_cql::CreateTable;
    use query_creator::logical_operator::LogicalOperator;
    use query_creator::operator::Operator;
    use std::path::PathBuf;

    #[test]
    fn test_delete_row_with_table_inner_using_create_table_tokens() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", uuid::Uuid::new_v4()));
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let storage = StorageEngine::new(root.clone(), "127.0.0.1".to_string());

        let table_path = storage
            .get_keyspace_path(keyspace)
            .join(format!("{}.csv", table_name));
        fs::create_dir_all(table_path.parent().unwrap()).unwrap();

        // Crear archivo de prueba con contenido inicial
        let mut file = File::create(&table_path).unwrap();
        writeln!(file, "id,name,age;1234567890").unwrap();
        writeln!(file, "1,John,30;1234567890").unwrap();
        writeln!(file, "2,Alice,25;1234567890").unwrap();

        // Crear los tokens para `CreateTable`
        let tokens = vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            format!("{}.{}", keyspace, table_name),
            "id INT, name TEXT, age INT, PRIMARY KEY (id)".to_string(),
        ];

        // Usar `new_from_tokens` para crear el `CreateTable`
        let create_table = CreateTable::new_from_tokens(tokens).unwrap();

        // Crear el `Table` utilizando el `CreateTable`
        let table = TableSchema {
            inner: create_table,
        };

        // Crear el `Delete` query
        let delete_query = Delete {
            table_name: table_name.to_string(),
            keyspace_used_name: keyspace.to_string(),
            columns: None,
            where_clause: Some(Where {
                condition: Condition::Simple {
                    field: "id".to_string(),
                    operator: Operator::Equal,
                    value: "2".to_string(),
                },
            }),
            if_clause: None,
            if_exist: false,
        };

        // Ejecutar el `delete`
        let result = storage.delete(delete_query, table, keyspace, false, 1234567890);
        assert!(result.is_ok(), "Delete operation failed");

        // Verificar el contenido del archivo después de la operación
        let file = File::open(&table_path).unwrap();
        let reader = BufReader::new(file);
        let lines: Vec<_> = reader.lines().map(|l| l.unwrap()).collect();

        // La fila con id=2 debería haber sido eliminada
        assert_eq!(lines.len(), 2); // Header + 1 row
        assert_eq!(lines[1], "1,John,30;1234567890");
    }

    #[test]
    fn test_delete_row_with_multiple_conditions() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", uuid::Uuid::new_v4()));
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let storage = StorageEngine::new(root.clone(), "127.0.0.1".to_string());

        let table_path = storage
            .get_keyspace_path(keyspace)
            .join(format!("{}.csv", table_name));
        fs::create_dir_all(table_path.parent().unwrap()).unwrap();

        // Crear archivo de prueba con contenido inicial
        let mut file = File::create(&table_path).unwrap();
        writeln!(file, "id,name,age;1234567890").unwrap();
        writeln!(file, "1,John,30;1234567890").unwrap();
        writeln!(file, "2,Alice,25;1234567890").unwrap();
        writeln!(file, "3,Bob,40;1234567890").unwrap();

        // Crear los tokens para `CreateTable`
        let tokens = vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            format!("{}.{}", keyspace, table_name),
            "id INT, name TEXT, age INT, PRIMARY KEY (id, name, age)".to_string(),
        ];

        // Usar `new_from_tokens` para crear el `CreateTable`
        let create_table = CreateTable::new_from_tokens(tokens).unwrap();

        // Crear el `Table` utilizando el `CreateTable`
        let table = TableSchema {
            inner: create_table,
        };

        // Crear el `Delete` query con múltiples condiciones
        let delete_query = Delete {
            table_name: table_name.to_string(),
            keyspace_used_name: keyspace.to_string(),
            columns: None,
            where_clause: Some(Where {
                condition: Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: "name".to_string(),
                        operator: Operator::Equal,
                        value: "Alice".to_string(),
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Simple {
                        field: "age".to_string(),
                        operator: Operator::Equal,
                        value: "25".to_string(),
                    }),
                },
            }),
            if_clause: None,
            if_exist: false,
        };

        // Ejecutar el `delete`
        let result = storage.delete(delete_query, table, keyspace, false, 1234567890);
        assert!(result.is_ok(), "Delete operation failed");

        // Verificar el contenido del archivo después de la operación
        let file = File::open(&table_path).unwrap();
        let reader = BufReader::new(file);
        let lines: Vec<_> = reader.lines().map(|l| l.unwrap()).collect();

        // La fila con name=Alice y age=25 debería haber sido eliminada
        assert_eq!(lines.len(), 3); // Header + 2 rows
        assert_eq!(lines[1], "1,John,30;1234567890");
        assert_eq!(lines[2], "3,Bob,40;1234567890");
    }

    #[test]
    fn test_delete_non_existing_row() {
        let root = PathBuf::from(format!("/tmp/storage_test_{}", uuid::Uuid::new_v4()));
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let storage = StorageEngine::new(root.clone(), "127.0.0.1".to_string());

        let table_path = storage
            .get_keyspace_path(keyspace)
            .join(format!("{}.csv", table_name));
        fs::create_dir_all(table_path.parent().unwrap()).unwrap();

        // Crear archivo de prueba con contenido inicial
        let mut file = File::create(&table_path).unwrap();
        writeln!(file, "id,name,age;1234567890").unwrap();
        writeln!(file, "1,John,30;1234567890").unwrap();

        // Crear los tokens para `CreateTable`
        let tokens = vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            format!("{}.{}", keyspace, table_name),
            "id INT, name TEXT, age INT, PRIMARY KEY (id)".to_string(),
        ];

        // Usar `new_from_tokens` para crear el `CreateTable`
        let create_table = CreateTable::new_from_tokens(tokens).unwrap();

        // Crear el `Table` utilizando el `CreateTable`
        let table = TableSchema {
            inner: create_table,
        };

        // Crear el `Delete` query para una fila que no existe
        let delete_query = Delete {
            table_name: table_name.to_string(),
            keyspace_used_name: keyspace.to_string(),
            columns: None,
            where_clause: Some(Where {
                condition: Condition::Simple {
                    field: "id".to_string(),
                    operator: Operator::Equal,
                    value: "99".to_string(),
                },
            }),
            if_clause: None,
            if_exist: false,
        };

        // Ejecutar el `delete`
        let result = storage.delete(delete_query, table, keyspace, false, 1234567890);
        assert!(result.is_ok(), "Delete operation failed");

        // Verificar el contenido del archivo después de la operación
        let file = File::open(&table_path).unwrap();
        let reader = BufReader::new(file);
        let lines: Vec<_> = reader.lines().map(|l| l.unwrap()).collect();

        // La fila no debería haberse modificado
        assert_eq!(lines.len(), 2); // Header + 1 row
        assert_eq!(lines[1], "1,John,30;1234567890");
    }
}
