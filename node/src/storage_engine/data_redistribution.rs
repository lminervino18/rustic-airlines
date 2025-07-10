use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    net::{Ipv4Addr, TcpStream},
    sync::{Arc, Mutex},
    // thread::{self},
    // time::Duration,
};

use gossip::structures::application_state::{KeyspaceSchema, TableSchema};
use logger::{Color, Logger};
use partitioner::Partitioner;

use crate::{
    internode_protocol::{
        message::{InternodeMessage, InternodeMessageContent},
        query::InternodeQuery,
    },
    utils::connect_and_send_message,
    INTERNODE_PORT,
};

use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    /// Redistributes data across nodes for the specified keyspaces.
    ///
    /// This function processes the data files associated with the given keyspaces
    /// and redistributes them across the cluster based on the partitioner. It ensures
    /// that each node holds the appropriate data based on the partitioning logic,
    /// and handles both normal and replication data files.
    ///
    /// # Arguments
    ///
    /// * `keyspaces` - A vector of keyspace schemas to process and redistribute.
    /// * `partitioner` - The partitioner responsible for determining the ownership of data.
    /// * `logger` - The logger instance for recording progress and errors.
    /// * `connections` - A shared map of connections to other nodes in the cluster.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the redistribution completes successfully.
    /// * `Err(StorageEngineError)` if any error occurs during redistribution.
    ///
    /// # Errors
    ///
    /// This function can return `StorageEngineError` for various reasons, such as:
    /// * IO errors while reading or writing data files.
    /// * Errors in parsing or serializing data.
    /// * Issues with internode communication or connection handling.
    pub fn redistribute_data(
        &self,
        keyspaces: Vec<KeyspaceSchema>,
        partitioner: &Partitioner,
        logger: Logger,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), StorageEngineError> {
        for keyspace in keyspaces {
            let tables = keyspace.clone().get_tables();

            for table in tables {
                // Rutas de archivos
                let base_folder_path = self.get_keyspace_path(&keyspace.clone().get_name());
                let normal_file_path = base_folder_path.join(format!("{}.csv", table.get_name()));
                let replication_file_path = base_folder_path
                    .join("replication")
                    .join(format!("{}.csv", table.get_name()));

                // Procesar archivo normal
                if normal_file_path.exists() {
                    self.process_file(
                        &normal_file_path,
                        &partitioner,
                        logger.clone(),
                        keyspace.clone(),
                        table.clone(),
                        false,
                        self.ip.clone(),
                        connections.clone(),
                    )?;
                }

                // Procesar archivo de replicación
                if replication_file_path.exists() {
                    self.process_file(
                        &replication_file_path,
                        &partitioner,
                        logger.clone(),
                        keyspace.clone(),
                        table.clone(),
                        true,
                        self.ip.clone(),
                        connections.clone(),
                    )?;
                }
            }
        }

        Ok(())
    }

    fn process_file(
        &self,
        file_path: &std::path::Path,
        partitioner: &Partitioner,
        logger: Logger,
        keyspace: KeyspaceSchema,
        table: TableSchema,
        is_replication: bool,
        self_ip: String,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), StorageEngineError> {
        let self_ip: Ipv4Addr = self_ip
            .parse()
            .map_err(|_| StorageEngineError::UnsupportedOperation)?;

        let columns: Vec<String> = table.get_columns().iter().map(|c| c.name.clone()).collect();

        let temp_file_path = file_path.with_extension("tmp");

        // Crear el archivo de índice con el formato `{nombre_archivo}_index.csv`
        let file_name = file_path.file_stem().ok_or(StorageEngineError::IoError)?;
        let index_file_path =
            file_path.with_file_name(format!("{}_index.csv", file_name.to_string_lossy()));

        let mut temp_file =
            BufWriter::new(File::create(&temp_file_path).map_err(|_| StorageEngineError::IoError)?);
        let mut index_file = BufWriter::new(
            File::create(&index_file_path).map_err(|_| StorageEngineError::IoError)?,
        );

        // Escribir el encabezado en el archivo de índice
        writeln!(index_file, "clustering_column,start_byte,end_byte")
            .map_err(|_| StorageEngineError::IoError)?;

        let file = File::open(file_path).map_err(|_| StorageEngineError::IoError)?;
        let reader = BufReader::new(file);

        let mut current_byte_offset: u64 = 0;
        let mut index_map: std::collections::BTreeMap<String, (u64, u64)> =
            std::collections::BTreeMap::new();

        let partition_key_indices: Vec<usize> = table
            .get_columns()
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_partition_key)
            .map(|(idx, _)| idx)
            .collect();

        let clustering_key_indices: Vec<(usize, String)> = table
            .get_clustering_column_in_order()
            .iter()
            .filter_map(|col_name| {
                table
                    .get_columns()
                    .iter()
                    .position(|col| col.name == *col_name && col.is_clustering_column)
                    .map(|idx| {
                        let inverted_order =
                            if table.get_columns()[idx].get_clustering_order() == "ASC" {
                                "DESC".to_string()
                            } else {
                                "ASC".to_string()
                            };
                        (idx, inverted_order)
                    })
            })
            .collect();

        for (i, line) in reader.lines().enumerate() {
            let line = line.map_err(|_| StorageEngineError::IoError)?;
            let line_length = line.len() as u64;

            // Escribir encabezado del archivo original
            if i == 0 {
                writeln!(temp_file, "{}", line).map_err(|_| StorageEngineError::IoError)?;
                current_byte_offset += line_length + 1;
                continue;
            }

            // Procesar línea de datos
            if let Some((data, timestamp)) = line.split_once(";") {
                let row: Vec<&str> = data.split(',').collect();

                // Construir la clave de partición
                let mut partition_key = String::new();
                for partition_key_index in &partition_key_indices {
                    partition_key.push_str(row[*partition_key_index]);
                }

                // Determinar el nodo actual para la clave de partición
                let current_node = partitioner
                    .get_ip(partition_key.clone())
                    .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                if current_node == self_ip {
                    if !is_replication {
                        // Si el nodo actual es el dueño de la clave
                        writeln!(temp_file, "{};{}", data, timestamp)
                            .map_err(|_| StorageEngineError::IoError)?;

                        // Actualizar índice
                        if let Some(&(idx, _)) = clustering_key_indices.first() {
                            let key = row[idx].to_string();
                            index_map.insert(
                                key,
                                (current_byte_offset, current_byte_offset + line_length),
                            );
                        }
                        current_byte_offset += line_length + 1;
                    } else {
                        let timest: i64 = timestamp
                            .parse()
                            .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                        self.insert(
                            &keyspace.get_name(),
                            &table.get_name(),
                            row.clone(),
                            table.get_columns(),
                            table.get_clustering_column_in_order(),
                            true,
                            false,
                            timest,
                        )?;
                    }
                } else {
                    // Reubicar la fila al nodo correspondiente
                    let insert_string = Self::create_cql_insert(
                        &keyspace.get_name(),
                        &table.get_name(),
                        columns.clone(),
                        row.clone(),
                    )?;

                    let timestamp_n: i64 = timestamp
                        .parse()
                        .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                    Self::create_and_send_internode_message(
                        self_ip,
                        current_node,
                        &keyspace.get_name(),
                        &insert_string,
                        timestamp_n,
                        false,
                        connections.clone(),
                        logger.clone(),
                    );
                }

                // Manejo de réplicas
                let successors = partitioner
                    .get_n_successors(current_node, keyspace.get_replication_factor() as usize - 1)
                    .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                for rep_ip in successors {
                    if rep_ip == self_ip {
                        if is_replication {
                            writeln!(temp_file, "{};{}", data, timestamp)
                                .map_err(|_| StorageEngineError::IoError)?;

                            if let Some(&(idx, _)) = clustering_key_indices.first() {
                                let key = row[idx].to_string();
                                index_map.insert(
                                    key,
                                    (current_byte_offset, current_byte_offset + line_length),
                                );
                            }
                            current_byte_offset += line_length + 1;
                        } else {
                            let timest: i64 = timestamp
                                .parse()
                                .map_err(|_| StorageEngineError::UnsupportedOperation)?;
                            self.insert(
                                &keyspace.get_name(),
                                &table.get_name(),
                                row.clone(),
                                table.get_columns(),
                                table.get_clustering_column_in_order(),
                                true,
                                false,
                                timest,
                            )?;
                        }
                    } else {
                        let insert_string = Self::create_cql_insert(
                            &keyspace.get_name(),
                            &table.get_name(),
                            columns.clone(),
                            row.clone(),
                        )?;
                        let timestamp_n: i64 = timestamp
                            .parse()
                            .map_err(|_| StorageEngineError::UnsupportedOperation)?;

                        Self::create_and_send_internode_message(
                            self_ip,
                            rep_ip,
                            &keyspace.get_name(),
                            &insert_string,
                            timestamp_n,
                            true,
                            connections.clone(),
                            logger.clone(),
                        );
                    }
                }
            }
        }

        // Escribir archivo de índice
        let mut sorted_indices: Vec<_> = index_map.into_iter().collect();
        for &(_, ref order) in &clustering_key_indices {
            if order == "ASC" {
                sorted_indices.sort_by(|a, b| a.0.cmp(&b.0));
            } else {
                sorted_indices.sort_by(|a, b| b.0.cmp(&a.0));
            }
        }

        for (key, (start_byte, end_byte)) in sorted_indices {
            writeln!(index_file, "{},{},{}", key, start_byte, end_byte)
                .map_err(|_| StorageEngineError::IoError)?;
        }

        fs::rename(&temp_file_path, file_path).map_err(|_| StorageEngineError::IoError)?;

        Ok(())
    }

    fn create_and_send_internode_message(
        self_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
        keyspace_name: &str,
        serialized_message: &str,
        timestamp: i64,
        is_replication: bool,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>, // Ajusta el tipo si es necesario
        logger: Logger,
    ) {
        // Crear el mensaje de internodo
        let message = InternodeMessage::new(
            self_ip,
            InternodeMessageContent::Query(InternodeQuery {
                query_string: serialized_message.to_string(),
                open_query_id: 0,
                client_id: 0,
                replication: is_replication,
                keyspace_name: keyspace_name.to_string(),
                timestamp,
            }),
        );
        // Enviar el mensaje al nodo objetivo
        let rep = if is_replication {
            "AS REPLICATION "
        } else {
            ""
        };

        logger
            .info(
                &format!(
                    "INTERNODE (REDISTRIBUTION): I SENT {:?}{:?} to {:?}",
                    rep,
                    serialized_message.to_string(),
                    target_ip
                ),
                Color::Cyan,
                true,
            )
            .ok();
        //thread::sleep(Duration::from_millis(300));
        let result = connect_and_send_message(target_ip, INTERNODE_PORT, connections, message);
        // Manejar errores o resultados
        _ = result;
    }

    fn create_cql_insert(
        keyspace: &str,
        table: &str,
        columns: Vec<String>,
        values: Vec<&str>,
    ) -> Result<String, StorageEngineError> {
        if columns.len() != values.len() {
            return Err(StorageEngineError::UnsupportedOperation);
        }

        // Generar la lista de columnas separadas por comas
        let columns_string = columns.join(", ");

        // Escapar valores si es necesario, rodeándolos con comillas simples
        let values_string = values
            .iter()
            .map(|value| format!("'{}'", value.replace("'", "''"))) // Escapar comillas simples en los valores
            .collect::<Vec<String>>()
            .join(",");

        // Construir la sentencia CQL
        let cql = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            keyspace, table, columns_string, values_string
        );

        Ok(cql)
    }
}
