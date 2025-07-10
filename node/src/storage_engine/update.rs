use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use gossip::structures::application_state::TableSchema;
use query_creator::clauses::update_cql::Update;

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    /// Performs an update on rows in a table by applying an `UPDATE` query to the records
    /// stored in CSV files. The update may be part of a replication process and will modify
    /// the files corresponding to the specified table.
    ///
    /// # Parameters
    ///
    /// * `update_query` - The `UPDATE` query containing the `SET`, `WHERE`, and optionally
    ///   the `IF` clauses. It defines which columns should be updated and under which conditions.
    ///
    /// * `table` - The table on which the update query is executed. The table must contain
    ///   the necessary columns and keys to identify rows and their indices.
    ///
    /// * `is_replication` - A boolean indicating whether the update is part of a replication process.
    ///   If `true`, the changes will also be reflected in replication files.
    ///
    /// * `keyspace` - The name of the keyspace containing the table. It is used to determine the path of the table's
    ///   files in the filesystem.
    ///
    /// * `timestamp` - An `i64` value representing the timestamp associated with the update.
    ///   This value will be included in the updated rows to track when the modification occurred.
    ///
    /// # Returns
    ///
    /// Returns a `Result<(), StorageEngineError>`:
    ///
    /// - `Ok(())` if the update is successful.
    /// - `Err(StorageEngineError)` if an error occurs during the update process, such as file creation failures,
    ///   I/O errors, or violation of constraints (like modifying primary keys).
    ///
    /// # Description
    ///
    /// This function performs the following steps to execute the update:
    ///
    /// 1. **Determine file paths**: It calculates the file paths of the table in the filesystem
    ///    based on the keyspace name and whether the update is part of replication.
    ///
    /// 2. **Create storage folder**: If the folder containing the table files doesn't exist, it will be created.
    ///
    /// 3. **File handling**: It opens the table CSV files and index files. If the table file doesn't exist,
    ///    a new empty one is created. It also creates a temporary file to store the changes without affecting
    ///    the original file.
    ///
    /// 4. **Read and write data**: It reads the table file line by line, evaluates the conditions in the update query
    ///    (such as the `WHERE` and `IF` clauses), and updates the rows that match. If no matching rows are found,
    ///    new rows are added.
    ///
    /// 5. **Update indices**: If a row is updated, the corresponding indices in the index file are adjusted.
    ///
    /// 6. **Replace original file**: The original table file is replaced with the temporary file containing the updated rows.
    ///
    /// 7. **Replication**: If the update is part of a replication process, it ensures the changes are reflected
    ///    in the corresponding replication files.
    ///
    /// 8. **Common errors**:
    ///    - `DirectoryCreationFailed`: Occurs if the folder for the files cannot be created.
    ///    - `TempFileCreationFailed`: Occurs if the temporary file cannot be created.
    ///    - `IoError`: Error during file reading or writing.
    ///    - `PartitionKeyMismatch`: If the partition key values do not match.
    ///    - `ClusteringKeyMismatch`: If there is a mismatch in the clustering keys.
    ///    - `ColumnNotFound`: If a non-existent column is specified.
    ///    - `PrimaryKeyModificationNotAllowed`: If a primary key column is attempted to be modified.
    ///    - `FileWriteFailed`: Error writing to a temporary file.
    ///
    /// ```
    pub fn update(
        &self,
        update_query: Update,
        table: TableSchema,
        is_replication: bool,
        keyspace: &str,
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

        // Rutas para el archivo original y el archivo temporal
        let file_path = folder_path.join(format!("{}.csv", table_name));
        let index_file_path = folder_path.join(format!("{}_index.csv", table.get_name()));
        let temp_file_path = folder_path.join(format!(
            "{}.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| StorageEngineError::TempFileCreationFailed)?
                .as_nanos()
        ));
        let mut temp_index = BufWriter::new(
            File::create(&index_file_path).map_err(|_| StorageEngineError::IoError)?,
        );

        writeln!(temp_index, "clustering_column,start_byte,end_byte")
            .map_err(|_| StorageEngineError::IoError)?;

        let columns = table.get_columns();
        let clustering_key_index =
            table
                .get_clustering_column_in_order()
                .get(0)
                .and_then(|col_name| {
                    columns
                        .iter()
                        .position(|col| col.name == *col_name && col.is_clustering_column)
                });

        let mut current_byte_offset: u64 = 0;

        let mut index_map: std::collections::BTreeMap<String, (u64, u64)> =
            std::collections::BTreeMap::new();

        // Abrir el archivo original, si existe, o crear un nuevo archivo vacío
        let file = if file_path.exists() {
            OpenOptions::new()
                .read(true)
                .open(&file_path)
                .map_err(|_| StorageEngineError::DirectoryCreationFailed)?
        } else {
            File::create(&file_path).map_err(|_| StorageEngineError::DirectoryCreationFailed)?
        };
        let mut reader = BufReader::new(file);

        // Crear el archivo temporal
        let mut temp_file = File::create(&temp_file_path)
            .map_err(|_| StorageEngineError::TempFileCreationFailed)?;

        // Leer el encabezado sin consumir el iterador
        let mut header_line = String::new();
        reader
            .read_line(&mut header_line)
            .map_err(|_| StorageEngineError::IoError)?;

        // Escribir el encabezado en el archivo temporal
        writeln!(temp_file, "{}", header_line.trim_end()) // Eliminar \n innecesario
            .map_err(|_| StorageEngineError::FileWriteFailed)?;
        current_byte_offset += header_line.len() as u64; // Contar el tamaño del encabezado

        let mut _found_match = false;

        // Iterar sobre las líneas del archivo original y aplicar la actualización
        for line in reader.lines() {
            let line = line?;
            _found_match |= self.update_or_write_line(
                &table,
                &update_query,
                &line,
                &mut temp_file,
                &mut index_map,
                clustering_key_index,
                &mut current_byte_offset,
                timestamp,
            )?;
        }

        // Reemplazar el archivo original con el actualizado
        fs::rename(&temp_file_path, &file_path)
            .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;

        // Actualizar el archivo de índices
        for (key, (start_byte, end_byte)) in index_map {
            writeln!(temp_index, "{},{},{}", key, start_byte, end_byte)
                .map_err(|_| StorageEngineError::IoError)?;
        }

        std::mem::drop(temp_index);
        // Si no se encontró ninguna fila que coincida, agregar una nueva
        /*if !found_match {
            self.add_new_row_in_update(&table, &update_query, keyspace, is_replication, timestamp)?;
        }*/

        Ok(())
    }

    /// Crea un mapa de valores de columna para una fila dada.
    pub fn create_column_value_map(
        &self,
        table: &TableSchema,
        columns: &[String],
        only_partitioner_key: bool,
    ) -> HashMap<String, String> {
        let mut column_value_map: HashMap<String, String> = HashMap::new();
        for (i, column) in table.get_columns().iter().enumerate() {
            if let Some(value) = columns.get(i) {
                if column.is_partition_key || column.is_clustering_column || !only_partitioner_key {
                    column_value_map.insert(column.name.clone(), value.clone());
                }
            }
        }

        column_value_map
    }

    fn update_or_write_line(
        &self,
        table: &TableSchema,
        update_query: &Update,
        line: &str,
        temp_file: &mut File,
        index_map: &mut std::collections::BTreeMap<String, (u64, u64)>,
        clustering_key_index: Option<usize>,
        current_byte_offset: &mut u64,
        timestamp: i64,
    ) -> Result<bool, StorageEngineError> {
        // Dividir la línea en contenido y timestamp
        let (line_content, time_of_row) =
            line.split_once(";").ok_or(StorageEngineError::IoError)?;
        let mut columns: Vec<String> = line_content
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        let column_value_map = self.create_column_value_map(table, &columns, false);

        let columns_schema = table.get_columns();

        let mut replaced = false;
        let mut line_length;

        // Evaluar la cláusula WHERE
        if let Some(where_clause) = &update_query.where_clause {
            if where_clause
                .condition
                .execute(&column_value_map, columns_schema.clone())
                .unwrap_or(false)
            {
                // Evaluar la cláusula IF, si está presente
                if let Some(if_clause) = &update_query.if_clause {
                    if !if_clause
                        .condition
                        .execute(&column_value_map, columns_schema.clone())
                        .unwrap_or(false)
                    {
                        // Si la cláusula IF no se cumple, escribir la línea original
                        writeln!(temp_file, "{};{}", line_content, time_of_row)?;
                        line_length = line.len() as u64 + 1; // Contar '\n'
                        Self::update_index_map_update(
                            &columns,
                            clustering_key_index,
                            index_map,
                            *current_byte_offset,
                            line_length,
                        );
                        *current_byte_offset += line_length;
                        return Ok(false);
                    }
                }

                // Aplicar SET para actualizar columnas
                for (column, new_value) in update_query.clone().set_clause.get_pairs() {
                    if table
                        .is_primary_key(&column)
                        .map_err(|_| StorageEngineError::ColumnNotFound)?
                    {
                        return Err(StorageEngineError::PrimaryKeyModificationNotAllowed);
                    }
                    let index = table
                        .get_column_index(&column)
                        .ok_or(StorageEngineError::ColumnNotFound)?;
                    columns[index] = new_value.clone();
                }

                // Crear línea actualizada con el nuevo timestamp
                let updated_line = format!("{};{}", columns.join(","), timestamp);
                line_length = updated_line.len() as u64 + 1; // Contar '\n'
                writeln!(temp_file, "{}", updated_line)?;

                // Actualizar el índice si corresponde a la clustering key
                Self::update_index_map_update(
                    &columns,
                    clustering_key_index,
                    index_map,
                    *current_byte_offset,
                    line_length,
                );

                *current_byte_offset += line_length;
                replaced = true;
            }
        }

        if !replaced {
            // No se cumple la cláusula WHERE, escribir la línea original
            writeln!(temp_file, "{};{}", line_content, time_of_row)?;
            line_length = line.len() as u64 + 1; // Contar '\n'

            // Actualizar el índice para la línea original
            Self::update_index_map_update(
                &columns,
                clustering_key_index,
                index_map,
                *current_byte_offset,
                line_length,
            );
            *current_byte_offset += line_length;
        }

        Ok(replaced)
    }

    fn update_index_map_update(
        row: &[String],
        clustering_key_index: Option<usize>,
        index_map: &mut std::collections::BTreeMap<String, (u64, u64)>,
        start_byte: u64,
        line_length: u64,
    ) {
        if let Some(idx) = clustering_key_index {
            let clustering_key = row[idx].clone();
            let entry = index_map
                .entry(clustering_key)
                .or_insert((start_byte, start_byte + line_length));
            entry.1 = start_byte + line_length;
        }
    }

    fn _add_new_row_in_update(
        &self,
        table: &TableSchema,
        update_query: &Update,
        keyspace: &str,
        is_replication: bool,
        timestamp: i64,
    ) -> Result<(), StorageEngineError> {
        let mut new_row: Vec<String> = vec!["".to_string(); table.get_columns().len()];

        let primary_keys = table
            .get_partition_keys()
            .map_err(|_| StorageEngineError::PartitionKeyMismatch)?;

        let primary_key_values = update_query
            .where_clause
            .as_ref()
            .map(|where_clause| {
                where_clause.get_value_partitioner_key_condition(primary_keys.clone())
            })
            .ok_or(StorageEngineError::MissingWhereClause)?
            .map_err(|_| StorageEngineError::PartitionKeyMismatch)?;

        if primary_key_values.len() != primary_keys.len() {
            return Err(StorageEngineError::PartitionKeyMismatch);
        }

        for (i, primary_key) in primary_keys.iter().enumerate() {
            let primary_key_index = table
                .get_column_index(primary_key)
                .ok_or(StorageEngineError::ColumnNotFound)?;
            new_row[primary_key_index] = primary_key_values[i].clone();
        }

        let clustering_keys = table
            .get_clustering_columns()
            .map_err(|_| StorageEngineError::ClusteringKeyMismatch)?;

        let clustering_key_values = update_query
            .where_clause
            .as_ref()
            .map(|where_clause| {
                where_clause.get_value_clustering_column_condition(clustering_keys.clone())
            })
            .ok_or(StorageEngineError::MissingWhereClause)?;

        for (i, value) in clustering_key_values.iter().enumerate() {
            if let Some(val) = value {
                let index = table.get_column_index(&clustering_keys[i]);

                if let Some(i) = index {
                    new_row[i] = val.clone();
                }
            }
        }

        for (column, new_value) in update_query.set_clause.get_pairs() {
            if table
                .is_primary_key(&column)
                .map_err(|_| StorageEngineError::ColumnNotFound)?
            {
                return Err(StorageEngineError::PrimaryKeyModificationNotAllowed);
            }
            let index = table
                .get_column_index(&column)
                .ok_or(StorageEngineError::ColumnNotFound)?;

            new_row[index] = new_value.clone();
        }

        let values: Vec<&str> = new_row.iter().map(|v| v.as_str()).collect();

        self.insert(
            keyspace,
            &table.get_name(),
            values,
            table.get_columns(),
            table.get_clustering_column_in_order(),
            is_replication,
            true,
            timestamp,
        )?;

        Ok(())
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
    use std::io::{BufRead, BufReader};
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn test_update_existing_row() {
        // Usamos un directorio único para esta prueba
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        // Setup de keyspace y tabla
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let columns = vec![
            Column::new("id", DataType::Int, true, false), // id: INT, primary key, not null
            Column::new("name", DataType::String, false, true), // name: TEXT, not primary key, allows null
        ];
        let clustering_columns_in_order = vec!["id".to_string()];
        let values = vec!["1", "John"];
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

        // Insertar una fila
        let result = storage.insert(
            keyspace,
            table_name,
            values.clone(),
            columns.clone(),
            clustering_columns_in_order.clone(),
            false, // no es replicación
            false, // no es si no existe
            timestamp,
        );
        assert!(result.is_ok(), "No se pudo insertar una nueva fila");

        // Verificar que el archivo fue creado
        assert!(
            table_file_path.exists(),
            "El archivo de la tabla no fue creado después de la inserción"
        );

        // Crear la instancia de `Table` para el UPDATE
        let create_table = CreateTable::new_from_tokens(vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "test_keyspace.test_table".to_string(),
            "id INT PRIMARY KEY, name TEXT".to_string(),
        ])
        .unwrap();

        let table = TableSchema::new(create_table.clone());

        let tokens = vec![
            "UPDATE".to_string(),
            "test_keyspace.test_table".to_string(),
            "SET".to_string(),
            "name".to_string(),
            "=".to_string(),
            "Jane".to_string(),
            "WHERE".to_string(),
            "id".to_string(),
            "=".to_string(),
            "1".to_string(),
        ];

        let update_query = Update::new_from_tokens(tokens).unwrap();
        let result = storage.update(update_query, table, false, keyspace, 1234567890);
        assert!(result.is_ok(), "No se pudo actualizar la fila");

        // Verificar el contenido del archivo después del UPDATE
        let file = File::open(&table_file_path).unwrap();
        let reader = BufReader::new(file);

        let mut lines = reader.lines();
        assert_eq!(
            lines.next().unwrap().unwrap(),
            "id,name",
            "La cabecera no coincide con el valor esperado"
        );
        assert_eq!(
            lines.next().unwrap().unwrap(),
            "1,Jane;1234567890", // El valor 'name' debería haberse actualizado a 'Jane'
            "El contenido de la fila no coincide con el valor esperado"
        );

        // Cleanup
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }

    #[test]
    fn test_update_non_existent_row() {
        // Usamos un directorio único para esta prueba
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        // Setup de keyspace y tabla
        let keyspace = "test_keyspace";
        let table_name = "test_table";
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

        // Crear la instancia de `Table` para el UPDATE
        let create_table = CreateTable::new_from_tokens(vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "test_keyspace.test_table".to_string(),
            "id INT PRIMARY KEY, name TEXT".to_string(),
        ])
        .unwrap();

        let table = TableSchema::new(create_table);

        // Intentar actualizar una fila inexistente (esto debería añadirla)
        let tokens = vec![
            "UPDATE".to_string(),
            "test_keyspace.test_table".to_string(),
            "SET".to_string(),
            "name".to_string(),
            "=".to_string(),
            "Jane".to_string(),
            "WHERE".to_string(),
            "id".to_string(),
            "=".to_string(),
            "2".to_string(),
        ];

        let update_query = Update::new_from_tokens(tokens).unwrap();
        let result = storage.update(update_query, table, false, keyspace, timestamp);
        assert!(result.is_ok(), "No se pudo agregar una fila nueva");

        // Verificar el contenido del archivo después del UPDATE
        let file = File::open(&table_file_path).unwrap();
        let reader = BufReader::new(file);

        let mut lines = reader.lines();
        assert_eq!(
            lines.next().unwrap().unwrap(),
            "id,name",
            "La cabecera no coincide con el valor esperado"
        );
        assert!(
            lines.next().is_none(),
            "Se esperaba que no hubiera más líneas, pero se encontró un valor"
        );

        // Cleanup
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }
    #[test]
    fn test_update_where_condition_not_met() {
        // Usamos un directorio único para esta prueba
        let root = PathBuf::from(format!("/tmp/storage_test_{}", Uuid::new_v4()));
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        // Setup de keyspace y tabla
        let keyspace = "test_keyspace";
        let table_name = "test_table";
        let columns = vec![
            Column::new("id", DataType::Int, true, false), // id: INT, primary key, not null
            Column::new("name", DataType::String, false, true), // name: TEXT, not primary key, allows null
        ];
        let clustering_columns_in_order = vec!["id".to_string()];
        let values = vec!["1", "John"];
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

        // Insertar una fila
        let result = storage.insert(
            keyspace,
            table_name,
            values.clone(),
            columns.clone(),
            clustering_columns_in_order.clone(),
            false, // no es replicación
            false, // no es si no existe
            timestamp,
        );
        assert!(result.is_ok(), "No se pudo insertar una nueva fila");

        // Crear la instancia de `Table` para el UPDATE
        let create_table = CreateTable::new_from_tokens(vec![
            "CREATE".to_string(),
            "TABLE".to_string(),
            "test_keyspace.test_table".to_string(),
            "id INT PRIMARY KEY, name TEXT".to_string(),
        ])
        .unwrap();

        let table = TableSchema::new(create_table);

        // Intentar actualizar una fila con una condición `WHERE` que no coincide
        let tokens = vec![
            "UPDATE".to_string(),
            "test_keyspace.test_table".to_string(),
            "SET".to_string(),
            "name".to_string(),
            "=".to_string(),
            "Jane".to_string(),
            "WHERE".to_string(),
            "id".to_string(),
            "=".to_string(),
            "999".to_string(), // Esta fila no existe
        ];

        let update_query = Update::new_from_tokens(tokens).unwrap();
        let result = storage.update(update_query, table, false, keyspace, timestamp);
        assert!(
            result.is_ok(),
            "La actualización falló aunque no debería cambiar nada"
        );

        // Verificar que la fila original no se haya modificado
        let file = File::open(&table_file_path).unwrap();
        let reader = BufReader::new(file);

        let mut lines = reader.lines();
        assert_eq!(
            lines.next().unwrap().unwrap(),
            "id,name",
            "La cabecera no coincide con el valor esperado"
        );

        assert_eq!(
            lines.next().unwrap().unwrap(),
            "1,John;1234567890", // La fila original debería mantenerse igual
            "El contenido de la fila no coincide con el valor esperado"
        );
        // assert_eq!(
        //     lines.next().unwrap().unwrap(),
        //     "999,Jane;1234567890", // La fila original debería mantenerse igual
        //     "El contenido de la fila no coincide con el valor esperado"
        // );

        // Cleanup
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }
}
