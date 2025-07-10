// Exportar todos los elementos del módulo query_execution

use crate::internode_protocol::message::{InternodeMessage, InternodeMessageContent};
use crate::internode_protocol::query::InternodeQuery;
use crate::internode_protocol::response::{InternodeResponse, InternodeResponseStatus};
use crate::open_query_handler::OpenQueryHandler;
use crate::utils::{check_keyspace, check_table, connect_and_send_message};
use crate::{storage_engine, Node, NodeError, Query, QueryExecution, INTERNODE_PORT};
use chrono::Utc;
use gossip::messages::GossipMessage;
use gossip::structures::application_state::TableSchema;
use logger::{Color, Logger};
use native_protocol::frame::Frame;
use native_protocol::messages::error;
use partitioner::Partitioner;
use query_creator::clauses::keyspace::{
    alter_keyspace_cql::AlterKeyspace, create_keyspace_cql::CreateKeyspace,
    drop_keyspace_cql::DropKeyspace,
};
use query_creator::clauses::table::{
    alter_table_cql::AlterTable, create_table_cql::CreateTable, drop_table_cql::DropTable,
};
use query_creator::clauses::types::column::Column;
use query_creator::clauses::use_cql::Use;
use query_creator::clauses::{
    delete_cql::Delete, insert_cql::Insert, select_cql::Select, update_cql::Update,
};
use query_creator::{CreateClientResponse, NeedsKeyspace, NeedsTable, QueryCreator};
use std::collections::HashMap;
use std::net::{Ipv4Addr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Struct that represents the handler for internode communication protocol.
pub struct InternodeProtocolHandler;

impl InternodeProtocolHandler {
    /// Creates a new `InternodeProtocolHandler` for handling internode commands
    /// and responses between nodes in a distributed setting.
    pub fn new() -> Self {
        InternodeProtocolHandler
    }

    /// Handles incoming commands sent to the node in a distributed database system.
    ///
    /// # Purpose
    /// Processes various types of internode messages, including queries, responses, and gossip messages.
    /// Delegates the handling of each message type to specialized helper functions to ensure efficient
    /// and accurate execution.
    ///
    /// # Parameters
    /// - `node: &Arc<Mutex<Node>>`
    ///   - A thread-safe, shared reference to the node handling the command.
    ///   - This ensures safe access and modifications during message processing.
    /// - `message: InternodeMessage`
    ///   - The received message containing:
    ///     - `content`: The type of message, which may be:
    ///       - `InternodeMessageContent::Query`: Represents a query to be executed on this node.
    ///       - `InternodeMessageContent::Response`: Represents a response to a previously issued query.
    ///       - `InternodeMessageContent::Gossip`: Represents a gossip protocol message for cluster state sharing.
    ///     - `from`: The identifier of the node that sent the message.
    /// - `connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>`
    ///   - A thread-safe map of active connections to other nodes in the cluster.
    ///   - Keys are node addresses (as strings), and values are thread-safe `TcpStream` objects for communication.
    ///
    /// # Returns
    /// - `Result<(), NodeError>`
    ///   - On success:
    ///     - Returns `Ok(())`, indicating that the command was successfully handled.
    ///   - On failure:
    ///     - Returns `Err(NodeError)` with details about the error encountered.
    ///
    /// # Behavior
    /// 1. **Query Handling**:
    ///    - If the message content is `InternodeMessageContent::Query`, calls `handle_query_command`.
    ///    - Executes the query on the local node and manages communication with other nodes if necessary.
    /// 2. **Response Handling**:
    ///    - If the message content is `InternodeMessageContent::Response`, calls `handle_response_command`.
    ///    - Processes the response for a previously issued query or command.
    /// 3. **Gossip Handling**:
    ///    - If the message content is `InternodeMessageContent::Gossip`, calls `handle_gossip_command`.
    ///    - Updates the node's internal state based on the gossip protocol message.
    /// 4. **Error Handling**:
    ///    - Any errors encountered during the handling of commands are returned as `NodeError`.
    ///
    /// # Message Types
    /// - `InternodeMessageContent::Query`:
    ///   - Represents a database query to be executed (e.g., SELECT, INSERT, UPDATE, DELETE, or schema operations).
    /// - `InternodeMessageContent::Response`:
    ///   - Represents the result of a previously executed query or command.
    /// - `InternodeMessageContent::Gossip`:
    ///   - Represents messages exchanged between nodes to share cluster state and maintain consistency.
    ///
    /// # Errors
    /// - Returns `NodeError` in the following cases:
    ///   - Failure in handling a query command (e.g., invalid query syntax or execution issues).
    ///   - Failure in processing a response command (e.g., unexpected or malformed response).
    ///   - Errors during gossip command processing (e.g., communication or state update issues).
    ///
    /// # Notes
    /// - This function relies on `handle_query_command`, `handle_response_command`, and `handle_gossip_command` for specific operations.
    /// - Assumes the `message` is valid and well-formed; malformed messages may result in `NodeError`.
    /// - The function is part of the internode communication layer in a distributed database system.

    pub fn handle_command(
        &self,
        node: &Arc<Mutex<Node>>,
        message: InternodeMessage,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let log = { node.lock()?.get_logger() };
        match message.clone().content {
            InternodeMessageContent::Query(query) => {
                let (open_query_id_str, color) = if query.open_query_id == 0 {
                    ("REDISTRIBUTION".to_string(), Color::Cyan)
                } else {
                    (format!("Query: {:?}", query.open_query_id), Color::Blue)
                };
                log.info(
                    &format!(
                        "INTERNODE ({}): I RECEIVED {:?} from {:?}",
                        open_query_id_str, query.query_string, message.from
                    ),
                    color,
                    true,
                )?;
                self.handle_query_command(node, query, connections, message.clone().from)?;
                Ok(())
            }
            InternodeMessageContent::Response(response) => {
                self.handle_response_command(node, &response, message.from, connections)?;

                Ok(())
            }
            InternodeMessageContent::Gossip(message) => {
                self.handle_gossip_command(node, &message, connections)?;
                Ok(())
            }
        }
    }

    /// Adds an OK response to an open query, determines if the query is complete, and sends the final response to the client.
    ///
    /// # Purpose
    /// This function handles the final stages of processing for a distributed query in a database cluster.
    /// It collects responses from multiple nodes, performs consistency checks (e.g., read repair), and
    /// sends the final result back to the client if all responses have been received.
    ///
    /// # Parameters
    /// - `query_handler: &mut OpenQueryHandler`
    ///   - A mutable reference to the `OpenQueryHandler`, which tracks ongoing queries and their associated responses.
    /// - `response: &InternodeResponse`
    ///   - The response received from another node, containing query results or status.
    /// - `open_query_id: i32`
    ///   - The unique identifier of the open query being processed.
    /// - `keyspace_name: String`
    ///   - The name of the keyspace associated with the query.
    /// - `table: Option<TableSchema>`
    ///   - An optional table schema that defines the structure of the table involved in the query.
    /// - `columns: Vec<Column>`
    ///   - A vector of column metadata associated with the query, used for filtering and organizing results.
    /// - `self_ip: Ipv4Addr`
    ///   - The IP address of the current node processing the query.
    /// - `from: Ipv4Addr`
    ///   - The IP address of the node that sent the response.
    /// - `connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>`
    ///   - A thread-safe map of connections to other nodes in the cluster.
    ///   - Keys are node addresses as strings, and values are `TcpStream` objects for internode communication.
    /// - `partitioner: Partitioner`
    ///   - The partitioner used to distribute and retrieve data within the cluster.
    /// - `storage_path: PathBuf`
    ///   - The file system path for accessing local storage.
    ///
    /// # Returns
    /// - `Result<(), NodeError>`
    ///   - On success:
    ///     - Returns `Ok(())`, indicating that the response was successfully added and processed.
    ///   - On failure:
    ///     - Returns `Err(NodeError)` with details about the error encountered during processing.
    ///
    /// # Behavior
    /// 1. **Add OK Response**:
    ///    - Adds the given `response` to the open query identified by `open_query_id` using the `query_handler`.
    ///    - Determines if the query has been completed (i.e., all required responses have been received).
    /// 2. **Read Repair**:
    ///    - If the query is complete:
    ///      - Collects the responses from all involved nodes using `get_acumulated_responses`.
    ///      - Performs a read repair operation to ensure consistency across nodes:
    ///        - Identifies the most up-to-date row based on the responses.
    ///        - Updates inconsistent nodes to align with the most recent data.
    /// 3. **Filter and Join Columns**:
    ///    - Filters and organizes the rows based on the query's select columns and metadata from the response.
    /// 4. **Create Client Response**:
    ///    - Constructs the response frame for the client using the query's metadata and the final row set.
    /// 5. **Send Response**:
    ///    - Sends the response frame to the client and ensures the connection is flushed.
    ///
    /// # Errors
    /// - Returns `NodeError` in the following cases:
    ///   - Issues during read repair or row consistency checks.
    ///   - Errors in constructing or sending the client response frame.
    ///   - Connection write or flush failures.
    ///
    /// # Notes
    /// - This function is part of the internode query processing system, ensuring data consistency and client response delivery.
    /// - Read repair is a key feature of distributed databases, maintaining data consistency across nodes.
    /// - The function assumes the query is valid and that the `query_handler` has been correctly initialized and managed.

    pub fn add_ok_response_to_open_query_and_send_response_if_closed(
        query_handler: &mut OpenQueryHandler,
        response: &InternodeResponse,
        open_query_id: i32,
        keyspace_name: String,
        table: Option<TableSchema>,
        columns: Vec<Column>,
        self_ip: Ipv4Addr,
        from: Ipv4Addr,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        partitioner: Partitioner,
        storage_path: PathBuf,
        logger: Logger,
    ) -> Result<(), NodeError> {
        if let Some(open_query) =
            query_handler.add_ok_response_and_get_if_closed(open_query_id, response.clone(), from)
        {
            let contents_of_different_nodes = open_query.get_acumulated_responses();
            //here we have to determinated the more new row
            // and do READ REPAIR

            let mut rows = vec![];
            if let Some(table) = table {
                rows = Self::read_repair(
                    contents_of_different_nodes,
                    columns.clone(),
                    self_ip,
                    keyspace_name.clone(),
                    table.clone(),
                    connections,
                    partitioner,
                    storage_path,
                )?;

                rows = if let Some(content) = &response.content {
                    Self::filter_and_join_columns(
                        rows,
                        content.select_columns.clone(),
                        content.columns.clone(),
                    )
                } else {
                    vec![]
                };
            };

            let connection = open_query.get_connection();
            let frame =
                open_query
                    .get_query()
                    .create_client_response(columns, keyspace_name, rows)?;

            logger.info(
                &format!("NATIVE: I sent FRAME RESPONSE to client",),
                Color::Yellow,
                true,
            )?;

            connection.send(frame).map_err(|_| NodeError::OtherError)?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Performs a read repair operation to ensure data consistency across nodes in a distributed database system.
    ///
    /// # Purpose
    /// Read repair is a fundamental mechanism in distributed databases to ensure eventual consistency.
    /// When data is read from multiple nodes, inconsistencies may arise due to network delays, partial failures,
    /// or outdated replicas. This function identifies the most recent version of data for each key, updates
    /// outdated nodes with the correct version, and returns the latest consistent data to the caller.
    ///
    /// # Parameters
    /// - `contents_of_different_nodes: Vec<(Ipv4Addr, InternodeResponse)>`
    ///   - A collection of responses from different nodes. Each response includes:
    ///     - The IP address of the responding node.
    ///     - An `InternodeResponse` containing query results and metadata.
    /// - `columns: Vec<Column>`
    ///   - A vector of column metadata that defines the structure of the table. This includes information about
    ///     primary keys and clustering columns used to identify and order rows.
    /// - `self_ip: Ipv4Addr`
    ///   - The IP address of the current node performing the read repair.
    /// - `keyspace_name: String`
    ///   - The name of the keyspace associated with the table being queried.
    /// - `table: TableSchema`
    ///   - The schema of the table being queried. This includes details about columns, keys, and clustering order.
    /// - `connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>`
    ///   - A thread-safe map of active connections to other nodes in the cluster.
    ///     - Keys are node addresses as strings.
    ///     - Values are thread-safe `TcpStream` objects for internode communication.
    /// - `partitioner: Partitioner`
    ///   - The partitioner responsible for determining the placement of data in the cluster based on primary keys.
    /// - `storage_path: PathBuf`
    ///   - The file system path for accessing local storage.
    ///
    /// # Returns
    /// - `Result<Vec<String>, NodeError>`
    ///   - On success:
    ///     - Returns a `Vec<String>` containing the rows of the latest consistent data, formatted as strings.
    ///   - On failure:
    ///     - Returns `Err(NodeError)` if an error occurs during the repair process or node communication.
    ///
    /// # Behavior
    /// 1. **Key Index Extraction**:
    ///    - Extracts indices for primary key and clustering columns from the `columns` metadata to identify rows uniquely.
    /// 2. **Identify Latest Versions**:
    ///    - Compares responses from all nodes to determine the most recent version of each row based on timestamps.
    ///    - Uses the `find_latest_versions` helper function to construct a mapping of keys to their latest values.
    /// 3. **Repair Outdated Nodes**:
    ///    - Updates nodes with outdated data by sending the latest version of inconsistent rows:
    ///      - If the outdated node is not the current node (`self_ip`), sends an update query to the affected node.
    ///      - If the outdated node is the current node, applies the update locally using the storage engine.
    ///    - Uses the `repair_nodes` helper function for this step.
    /// 4. **Return Consistent Data**:
    ///    - Returns the rows corresponding to the latest consistent data after performing repairs.
    ///
    /// # Key Internal Logic
    /// - **Primary and Clustering Keys**:
    ///   - Primary keys are used to determine data partitioning.
    ///   - Clustering columns define the order of rows within a partition.
    /// - **Timestamp Comparison**:
    ///   - Timestamps are used to identify the most recent version of a row.
    ///   - Rows with older timestamps are considered outdated and are repaired.
    /// - **Node Communication**:
    ///   - Uses internode communication to propagate updates to other nodes as part of the repair process.
    ///
    /// # Notes
    /// - This function is not public but is a cornerstone of maintaining consistency in the system.
    /// - It assumes that `contents_of_different_nodes` contains well-formed responses from participating nodes.
    /// - Read repair can introduce additional network traffic and disk I/O, but it ensures that the database converges
    ///   towards a consistent state.
    ///
    /// # Errors
    /// - Returns `NodeError` in the following scenarios:
    ///   - Failures in node communication while sending updates.
    ///   - Issues in constructing or applying updates on local or remote nodes.
    ///   - Errors in parsing or processing timestamps or values.
    ///
    /// # Importance
    /// Read repair is critical for ensuring that distributed databases provide accurate and consistent results to users.
    /// While it is an internal mechanism, its role in reconciling inconsistencies makes it essential for achieving
    /// the system's eventual consistency guarantees.

    fn read_repair(
        contents_of_different_nodes: Vec<(Ipv4Addr, InternodeResponse)>,
        columns: Vec<Column>,
        self_ip: Ipv4Addr,
        keyspace_name: String,
        table: TableSchema,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        partitioner: Partitioner,
        storage_path: PathBuf,
    ) -> Result<Vec<String>, NodeError> {
        let primary_key_indices = Self::get_key_indices(&columns, true);
        let clustering_column_indices = Self::get_key_indices(&columns, false);

        let latest_versions = Self::find_latest_versions(
            &contents_of_different_nodes,
            &primary_key_indices,
            &clustering_column_indices,
        );

        let updated_rows = Self::repair_nodes(
            contents_of_different_nodes,
            &columns,
            &primary_key_indices,
            &clustering_column_indices,
            latest_versions,
            &self_ip,
            &keyspace_name,
            table,
            &connections,
            &partitioner,
            storage_path,
        )?;

        Ok(updated_rows)
    }

    fn get_key_indices(columns: &[Column], is_partition_key: bool) -> Vec<usize> {
        columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if is_partition_key && column.is_partition_key {
                    Some(index)
                } else if !is_partition_key && column.is_clustering_column {
                    Some(index)
                } else {
                    None
                }
            })
            .collect()
    }

    fn find_latest_versions(
        contents_of_different_nodes: &[(Ipv4Addr, InternodeResponse)],
        primary_key_indices: &[usize],
        clustering_column_indices: &[usize],
    ) -> HashMap<String, (Ipv4Addr, i64, Vec<String>)> {
        let mut latest_versions: HashMap<String, (Ipv4Addr, i64, Vec<String>)> = HashMap::new();

        for (node_ip, response) in contents_of_different_nodes {
            if let Some(content) = &response.content {
                for value in &content.values {
                    let key =
                        Self::build_key(value, primary_key_indices, clustering_column_indices);
                    let current_timestamp = Self::get_timestamp(value);

                    if let Some((_, latest_timestamp, _)) = latest_versions.get(&key) {
                        if *latest_timestamp < current_timestamp {
                            latest_versions
                                .insert(key, (*node_ip, current_timestamp, value.clone()));
                        }
                    } else {
                        latest_versions.insert(key, (*node_ip, current_timestamp, value.clone()));
                    }
                }
            }
        }

        latest_versions
    }

    fn build_key(
        value: &[String],
        primary_key_indices: &[usize],
        clustering_column_indices: &[usize],
    ) -> String {
        let mut key_components: Vec<String> = Vec::new();

        for &index in primary_key_indices {
            key_components.push(value[index].clone());
        }
        for &index in clustering_column_indices {
            key_components.push(value[index].clone());
        }

        key_components.join("|")
    }

    fn get_timestamp(value: &[String]) -> i64 {
        let timestamp_index = value.len() - 1;
        value[timestamp_index].parse::<i64>().unwrap_or(0)
    }

    fn repair_nodes(
        contents_of_different_nodes: Vec<(Ipv4Addr, InternodeResponse)>,
        columns: &[Column],
        primary_key_indices: &[usize],
        clustering_column_indices: &[usize],
        latest_versions: HashMap<String, (Ipv4Addr, i64, Vec<String>)>,
        self_ip: &Ipv4Addr,
        keyspace_name: &String,
        table: TableSchema,
        connections: &Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        partitioner: &Partitioner,
        storage_path: PathBuf,
    ) -> Result<Vec<String>, NodeError> {
        let mut updated_rows: Vec<String> = Vec::new();
        let table_name = &table.get_name();
        for (node_ip, response) in &contents_of_different_nodes {
            if let Some(content) = &response.content {
                for value in &content.values {
                    let key =
                        Self::build_key(value, primary_key_indices, clustering_column_indices);

                    if let Some((latest_ip, latest_timestamp, latest_value)) =
                        latest_versions.get(&key)
                    {
                        let current_timestamp = Self::get_timestamp(value);

                        if node_ip != latest_ip && current_timestamp < *latest_timestamp {
                            let insert_query = Self::generate_insert_query(
                                keyspace_name,
                                table_name,
                                columns,
                                latest_value,
                            );

                            let replication = Self::get_is_replication(
                                latest_value,
                                primary_key_indices,
                                partitioner,
                                node_ip,
                            )?;

                            if node_ip != self_ip {
                                Self::send_update_to_node(
                                    *node_ip,
                                    connections,
                                    insert_query,
                                    self_ip,
                                    keyspace_name,
                                    replication,
                                )?;
                            } else {
                                let latest_values = latest_value
                                    .iter()
                                    .map(|v| v.as_str())
                                    .take(latest_value.len() - 1)
                                    .collect();

                                Self::update_this_node(
                                    self_ip,
                                    keyspace_name,
                                    replication,
                                    table_name,
                                    latest_values,
                                    table.get_clustering_column_in_order(),
                                    columns,
                                    storage_path.clone(),
                                )?;
                                // Opcional: manejar lógica para actualizar el propio nodo si es necesario
                            }
                        }
                    }
                }
            }
        }

        updated_rows.extend(
            latest_versions
                .into_iter()
                .map(|(_, (_, _, value))| value.join(",")),
        );

        Ok(updated_rows)
    }

    fn get_is_replication(
        latest_value: &[String],
        primary_key_indices: &[usize],
        partitioner: &Partitioner,
        node_ip: &Ipv4Addr,
    ) -> Result<bool, NodeError> {
        // Construir la clave particionada a partir de los valores de las claves primarias
        let value_partitioner_key: Vec<String> = primary_key_indices
            .iter()
            .map(|&index| latest_value[index].clone())
            .collect();

        let value_to_hash = value_partitioner_key.join("");

        // Determinar si el nodo necesita replicación
        let is_replication = partitioner.get_ip(value_to_hash)? != *node_ip;

        Ok(is_replication)
    }

    fn generate_insert_query(
        keyspace_name: &String,
        table_name: &String,
        columns: &[Column],
        latest_value: &[String],
    ) -> String {
        let mut insert_query = format!("INSERT INTO {}.{} (", keyspace_name, table_name);

        insert_query.push_str(
            &columns
                .iter()
                .map(|col| col.name.clone())
                .collect::<Vec<String>>()
                .join(","),
        );
        insert_query.push_str(") VALUES (");

        insert_query.push_str(
            &latest_value
                .iter()
                .take(latest_value.len().saturating_sub(1))
                .map(|val| format!("'{}'", val))
                .collect::<Vec<String>>()
                .join(","),
        );
        insert_query.push_str(");");

        insert_query
    }

    fn send_update_to_node(
        node_ip: Ipv4Addr,
        connections: &Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        query: String,
        self_ip: &Ipv4Addr,
        keyspace_name: &String,
        replication: bool,
    ) -> Result<(), NodeError> {
        let message = InternodeMessage::new(
            *self_ip,
            InternodeMessageContent::Query(InternodeQuery {
                query_string: query,
                open_query_id: 0,
                client_id: 0,
                replication: replication,
                keyspace_name: keyspace_name.clone(),
                timestamp: Utc::now().timestamp(),
            }),
        );

        connect_and_send_message(node_ip, INTERNODE_PORT, connections.clone(), message)?;
        Ok(())
    }

    fn update_this_node(
        self_ip: &Ipv4Addr,
        keyspace_name: &String,
        replication: bool,
        table_name: &String,
        values: Vec<&str>,
        clustering_columns_in_order: Vec<String>,
        columns: &[Column],
        path: PathBuf,
    ) -> Result<(), NodeError> {
        storage_engine::StorageEngine::new(path, self_ip.to_string()).insert(
            &keyspace_name,
            &table_name,
            values,
            columns.to_vec(),
            clustering_columns_in_order,
            replication,
            false,
            Utc::now().timestamp(),
        )?;
        Ok(())
    }

    fn filter_and_join_columns(
        rows: Vec<String>,
        select_columns: Vec<String>,
        columns: Vec<String>,
    ) -> Vec<String> {
        // Crear el encabezado con las columnas seleccionadas
        let mut result = vec![select_columns.join(",")];

        // Obtener los índices de las columnas seleccionadas
        let selected_indices: Vec<usize> = select_columns
            .iter()
            .filter_map(|col| columns.iter().position(|c| c == col))
            .collect();

        // Procesar cada fila de valores
        let filtered_rows: Vec<String> = rows
            .iter()
            .map(|row| {
                // Dividir la fila en sus componentes (se asume que están separadas por comas)
                let row_values: Vec<&str> = row.split(',').collect();

                // Seleccionar solo los valores correspondientes a los índices de las columnas seleccionadas
                selected_indices
                    .iter()
                    .map(|&i| row_values.get(i).unwrap_or(&"").to_string()) // Crear copias de los valores
                    .collect::<Vec<String>>()
                    .join(",")
            })
            .collect();

        // Agregar los valores procesados al resultado
        result.extend(filtered_rows);

        result
    }

    /// Adds an error response to an open query and sends the final error response to the client if the query is complete.
    ///
    /// # Purpose
    /// This function handles error scenarios for distributed queries. It marks an open query with an error response
    /// and checks if all required responses for the query have been received. If the query is complete, it constructs
    /// an error frame and sends it back to the client.
    ///
    /// # Parameters
    /// - `query_handler: &mut OpenQueryHandler`
    ///   - A mutable reference to the `OpenQueryHandler`, which tracks ongoing queries and their associated responses.
    ///   - This handler is used to mark the query with an error response and check if all responses have been received.
    /// - `open_query_id: i32`
    ///   - The unique identifier of the open query being processed.
    ///
    /// # Returns
    /// - `Result<(), NodeError>`
    ///   - On success:
    ///     - Returns `Ok(())`, indicating that the error response was successfully added and processed.
    ///   - On failure:
    ///     - Returns `Err(NodeError)` if there is an issue writing the error response to the client or flushing the connection.
    ///
    /// # Behavior
    /// 1. **Add Error Response**:
    ///    - Marks the query identified by `open_query_id` as having encountered an error using the `query_handler`.
    ///    - Determines if the query is complete (i.e., all responses, including the error response, have been received).
    /// 2. **Construct Error Frame**:
    ///    - If the query is complete:
    ///      - Creates an error response frame using the `Frame::Error` constructor with a `ServerError` message.
    /// 3. **Send Error Response**:
    ///    - Sends the error response frame to the client over the connection associated with the query.
    ///    - Ensures the connection is flushed to deliver the response promptly.
    ///
    /// # Errors
    /// - Returns `NodeError` in the following cases:
    ///   - Failure to write the error response frame to the client connection.
    ///   - Failure to flush the connection after writing the error frame.
    ///
    /// # Notes
    /// - This function is part of the error handling mechanism for distributed queries in a database cluster.
    /// - It ensures that the client is notified of errors in a timely and structured manner.
    /// - The `query_handler` must be properly initialized and managed to ensure consistent query tracking.
    ///
    /// # Example Workflow
    /// - A query encounters an error on one or more nodes.
    /// - This function is called to mark the query as having an error response.
    /// - If all responses for the query have been received, it constructs an error frame and sends it to the client.
    /// - If the query is not yet complete, no action is taken beyond marking the error response.

    pub fn add_error_response_to_open_query_and_send_response_if_closed(
        query_handler: &mut OpenQueryHandler,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        if let Some(open_query) = query_handler.add_error_response_and_get_if_closed(open_query_id)
        {
            let connection = open_query.get_connection();

            let error_frame = Frame::Error(error::Error::ServerError(".".to_string()));

            connection
                .send(error_frame)
                .map_err(|_| NodeError::OtherError)?;
            Ok(())
        } else {
            Ok(())
        }
    }

    // Handles a query command received from another node.
    fn handle_query_command(
        &self,
        node: &Arc<Mutex<Node>>,
        query: InternodeQuery,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        node_ip: Ipv4Addr,
    ) -> Result<(), NodeError> {
        if query.needs_keyspace() {
            let q = QueryCreator::new().handle_query(query.query_string.clone())?;
            check_keyspace(node, &q, query.client_id as i32, 6)?;
        }

        if query.needs_table() {
            let q = QueryCreator::new().handle_query(query.query_string.clone())?;
            check_table(node, &q, query.client_id as i32, 6)?;
        }

        if query.keyspace_name != "None" {
            {
                let mut guard_node = node.lock()?;
                let k = guard_node.get_keyspace(query.keyspace_name.as_str())?;
                guard_node.get_open_handle_query().set_keyspace_of_query(
                    query.open_query_id as i32,
                    k.ok_or(NodeError::KeyspaceError)?,
                );
            }
        }

        let self_ip;
        let logger;
        {
            let guard_node = node.lock()?;
            self_ip = guard_node.get_ip();
            logger = guard_node.get_logger();
        };
        let query_split: Vec<&str> = query.query_string.split_whitespace().collect();
        let result: Result<Option<((i32, i32), InternodeResponse)>, NodeError> =
            match query_split[0] {
                "CREATE" => match query_split[1] {
                    "TABLE" => Self::handle_create_table_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    "KEYSPACE" => Self::handle_create_keyspace_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    _ => Err(NodeError::InternodeProtocolError),
                },
                "DROP" => match query_split[1] {
                    "TABLE" => Self::handle_drop_table_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    "KEYSPACE" => Self::handle_drop_keyspace_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    _ => Err(NodeError::InternodeProtocolError),
                },
                "ALTER" => match query_split[1] {
                    "TABLE" => Self::handle_alter_table_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    "KEYSPACE" => Self::handle_alter_keyspace_command(
                        node,
                        &query.query_string,
                        connections.clone(),
                        true,
                        query.open_query_id as i32,
                        query.client_id as i32,
                    ),
                    _ => Err(NodeError::InternodeProtocolError),
                },
                "INSERT" => Self::handle_insert_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.replication,
                    query.open_query_id as i32,
                    query.client_id as i32,
                    query.timestamp,
                ),
                "UPDATE" => Self::handle_update_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.replication,
                    query.open_query_id as i32,
                    query.client_id as i32,
                    query.timestamp,
                ),
                "DELETE" => Self::handle_delete_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.replication,
                    query.open_query_id as i32,
                    query.client_id as i32,
                    query.timestamp,
                ),
                "SELECT" => Self::handle_select_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.replication,
                    query.open_query_id as i32,
                    query.client_id as i32,
                ),
                "USE" => Self::handle_use_command(
                    node,
                    &query.query_string,
                    connections.clone(),
                    true,
                    query.open_query_id as i32,
                    query.client_id as i32,
                ),
                _ => Err(NodeError::InternodeProtocolError),
            };

        let response: Option<((i32, i32), InternodeResponse)> = result?;

        if let Some(responses) = response {
            let (_, value): ((i32, i32), InternodeResponse) = responses.clone();

            if query.open_query_id != 0 {
                logger.info(
                    &format!(
                        "INTERNODE (Query: {:?}): I SENT OK to coordinator node: {:?}",
                        query.open_query_id, node_ip
                    ),
                    Color::Green,
                    true,
                )?;

                connect_and_send_message(
                    node_ip,
                    INTERNODE_PORT,
                    connections,
                    InternodeMessage {
                        from: self_ip,
                        content: InternodeMessageContent::Response(value),
                    },
                )?;
            }
        }

        Ok(())
    }

    // Handles a response command from another node.
    fn handle_response_command(
        &self,
        node: &Arc<Mutex<Node>>,
        response: &InternodeResponse,
        from: Ipv4Addr,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let self_ip;
        let partitioner;
        let storage_path;
        let logger;
        {
            let guard_node = node.lock()?;
            self_ip = guard_node.get_ip();
            partitioner = guard_node.get_partitioner();
            storage_path = guard_node.storage_path.clone();
            logger = guard_node.get_logger();
        }
        let mut guard_node = node.lock()?;

        let query_handler = guard_node.get_open_handle_query();

        let keyspace = query_handler.get_keyspace_of_query(response.open_query_id as i32)?;

        let keyspace_name = if let Some(value) = keyspace {
            value.get_name()
        } else {
            "".to_string()
        };

        match response.status {
            InternodeResponseStatus::Ok => {
                logger.info(
                    &format!(
                        "INTERNODE (Query: {}): I RECEIVED OK RESPONSE {:?} from {:?}",
                        response.open_query_id, response.status, from
                    ),
                    Color::Blue,
                    true,
                )?;

                self.process_ok_response(
                    query_handler,
                    response,
                    response.open_query_id as i32,
                    keyspace_name,
                    self_ip,
                    from,
                    connections,
                    partitioner,
                    storage_path.clone(),
                    logger,
                )?;
            }
            InternodeResponseStatus::Error => {
                logger.info(
                    &format!(
                        "INTERNODE (Query: {}): I RECEIVED OK RESPONSE {:?} from {:?}",
                        response.open_query_id, response.status, from
                    ),
                    Color::Red,
                    true,
                )?;
                self.process_error_response(query_handler, response.open_query_id as i32)?;
            }
        }

        Ok(())
    }

    // Handles a gossip command from another node.
    // This function is responsible for processing the gossip message and responding accordingly.
    fn handle_gossip_command(
        &self,
        node: &Arc<Mutex<Node>>,
        gossip_message: &GossipMessage,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let mut guard_node = node.lock()?;

        match &gossip_message.payload {
            gossip::messages::Payload::Syn(syn) => {
                let ack = guard_node.gossiper.handle_syn(syn);

                let msg =
                    GossipMessage::new(guard_node.get_ip(), gossip::messages::Payload::Ack(ack));

                let result = connect_and_send_message(
                    gossip_message.from,
                    INTERNODE_PORT,
                    connections,
                    InternodeMessage::new(
                        guard_node.get_ip(),
                        InternodeMessageContent::Gossip(msg),
                    ),
                );

                if result.is_err() {
                    guard_node.gossiper.kill(gossip_message.from).ok();
                }
            }
            gossip::messages::Payload::Ack(ack) => {
                let ack2 = guard_node.gossiper.handle_ack(ack);

                let msg =
                    GossipMessage::new(guard_node.get_ip(), gossip::messages::Payload::Ack2(ack2));

                let result = connect_and_send_message(
                    gossip_message.from,
                    INTERNODE_PORT,
                    connections,
                    InternodeMessage::new(
                        guard_node.get_ip(),
                        InternodeMessageContent::Gossip(msg),
                    ),
                );

                if result.is_err() {
                    //println!("Node is dead: {:?}", gossip_message.from);
                    guard_node.gossiper.kill(gossip_message.from).ok();
                }
            }

            gossip::messages::Payload::Ack2(ack2) => {
                guard_node.gossiper.handle_ack2(ack2);
            }
        };

        Ok(())
    }

    // Procesa la respuesta cuando el estado es "OK"
    fn process_ok_response(
        &self,
        query_handler: &mut OpenQueryHandler,
        response: &InternodeResponse,
        open_query_id: i32,
        keyspace_name: String,
        self_ip: Ipv4Addr,
        from: Ipv4Addr,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        partitioner: Partitioner,
        storage_path: PathBuf,
        logger: Logger,
    ) -> Result<(), NodeError> {
        // Obtener la consulta abierta

        let columns;
        let table;
        {
            let open_query = if let Some(value) = query_handler.get_query_mut(&open_query_id) {
                value
            } else {
                return Ok(());
            };

            table = open_query.get_table();
            // Copiar los valores necesarios para evitar el uso de `open_query` posteriormente
            columns = open_query
                .get_table()
                .map_or_else(Vec::new, |table| table.get_columns());
        }
        // Llamar a la función con los valores copiados, sin `open_query` en uso
        Self::add_ok_response_to_open_query_and_send_response_if_closed(
            query_handler,
            response,
            open_query_id,
            keyspace_name,
            table,
            columns,
            self_ip,
            from,
            connections,
            partitioner,
            storage_path,
            logger,
        )?;

        Ok(())
    }

    // Procesa la respuesta cuando el estado es "OK"
    fn process_error_response(
        &self,
        query_handler: &mut OpenQueryHandler,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        Self::add_error_response_to_open_query_and_send_response_if_closed(
            query_handler,
            open_query_id,
        )?;

        Ok(())
    }

    // Handles an `INSERT` command.
    fn handle_insert_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        replication: bool,
        open_query_id: i32,
        client_id: i32,
        timestamp: i64,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Insert::deserialize(structure).map_err(NodeError::CQLError)?;
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::Insert(query),
            internode,
            replication,
            open_query_id,
            client_id,
            Some(timestamp),
        )
    }

    // Handles a `CREATE_TABLE` command.
    fn handle_create_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = CreateTable::deserialize(structure).map_err(NodeError::CQLError)?;

        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::CreateTable(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
        )
    }

    // Handles a `DROP_TABLE` command.
    fn handle_drop_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = DropTable::deserialize(structure).map_err(NodeError::CQLError)?;
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::DropTable(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
        )
    }

    // Handles an `ALTER_TABLE` command.
    fn handle_alter_table_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = AlterTable::deserialize(structure).map_err(NodeError::CQLError)?;
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::AlterTable(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
        )
    }

    // Handles a `CREATE_KEYSPACE` command.
    fn handle_create_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let storage_path = { node.lock()?.storage_path.clone() };
        let query = CreateKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::CreateKeyspace(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
        )
    }

    // Handles a `DROP_KEYSPACE` command.
    fn handle_drop_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = DropKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::DropKeyspace(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
        )
    }

    // Handles an `ALTER_KEYSPACE` command.
    fn handle_alter_keyspace_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = AlterKeyspace::deserialize(structure).map_err(NodeError::CQLError)?;
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::AlterKeyspace(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
        )
    }

    // Handles an `UPDATE` command.
    fn handle_update_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        replication: bool,
        open_query_id: i32,
        client_id: i32,
        timestamp: i64,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Update::deserialize(structure).map_err(NodeError::CQLError)?;
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::Update(query),
            internode,
            replication,
            open_query_id,
            client_id,
            Some(timestamp),
        )
    }

    // Handles a `DELETE` command.
    fn handle_delete_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        replication: bool,
        open_query_id: i32,
        client_id: i32,
        timestamp: i64,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Delete::deserialize(structure).map_err(NodeError::CQLError)?;
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::Delete(query),
            internode,
            replication,
            open_query_id,
            client_id,
            Some(timestamp),
        )
    }

    // Handles a `SELECT` command.
    fn handle_select_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        replication: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Select::deserialize(structure).map_err(NodeError::CQLError)?;
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::Select(query),
            internode,
            replication,
            open_query_id,
            client_id,
            None,
        )
    }

    // Handles an `INSERT` command.
    fn handle_use_command(
        node: &Arc<Mutex<Node>>,
        structure: &str,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let query = Use::deserialize(structure).map_err(NodeError::CQLError)?;
        let storage_path = { node.lock()?.storage_path.clone() };
        QueryExecution::new(node.clone(), connections, storage_path)?.execute(
            Query::Use(query),
            internode,
            false,
            open_query_id,
            client_id,
            None,
        )
    }
}
