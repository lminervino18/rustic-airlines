use crate::internode_protocol::message::{InternodeMessage, InternodeMessageContent};
use crate::internode_protocol::query::InternodeQuery;
use crate::internode_protocol::response::{
    InternodeResponse, InternodeResponseContent, InternodeResponseStatus,
};
use crate::utils::connect_and_send_message;
use crate::NodeError;
use crate::{Node, INTERNODE_PORT};
use logger::{Color, Logger};
use query_creator::clauses::types::column::Column;

pub mod alter_keyspace;
pub mod alter_table;
pub mod create_keyspace;
pub mod create_table;
pub mod delete;
pub mod drop_keyspace;
pub mod drop_table;
pub mod insert;
pub mod select;
pub mod update;
pub mod use_cql;
use super::storage_engine::StorageEngine;
use query_creator::errors::CQLError;
use query_creator::Query;
use std::collections::HashMap;
use std::net::{Ipv4Addr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
// Si `node` es el módulo raíz

/// Struct for executing various database queries across nodes with support
/// for distributed communication and replication.
pub struct QueryExecution {
    node_that_execute: Arc<Mutex<Node>>,
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    execution_finished_itself: bool,
    execution_replicate_itself: bool,
    how_many_nodes_failed: i32,
    storage_engine: StorageEngine,
}

impl QueryExecution {
    /// Creates a new instance of `QueryExecution`.
    ///
    /// # Purpose
    /// This function initializes a `QueryExecution` object that manages query execution on a specific node
    /// in a distributed database system. It sets up the required components such as the `StorageEngine`
    /// and establishes the execution context.
    ///
    /// # Parameters
    /// - `node_that_execute: Arc<Mutex<Node>>`
    ///   - A shared, thread-safe reference to the node responsible for executing queries.
    ///   - The node is locked during initialization to retrieve its IP address and other details.
    /// - `connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>`
    ///   - A shared, thread-safe map of active connections to other nodes in the cluster.
    ///   - The key is a string representing the node address, and the value is a thread-safe `TcpStream`
    ///     for communication with the corresponding node.
    /// - `storage_path: PathBuf`
    ///   - A file system path to the storage directory used by the `StorageEngine`.
    ///   - This defines where local data for the node is stored.
    ///
    /// # Returns
    /// - `Result<QueryExecution, NodeError>`
    ///   - On success:
    ///     - Returns an `Ok(QueryExecution)` instance configured with the provided parameters.
    ///   - On failure:
    ///     - Returns an `Err(NodeError)` if there is an issue accessing the node or initializing the storage engine.
    ///
    /// # Behavior
    /// 1. **Node Access**:
    ///    - Locks the `node_that_execute` mutex to safely access the node's details.
    ///    - Retrieves the IP address of the node using `get_ip_string()`.
    /// 2. **Storage Engine Initialization**:
    ///    - Creates a new instance of `StorageEngine` using the provided `storage_path` and the node's IP address.
    /// 3. **QueryExecution Initialization**:
    ///    - Sets default values for execution-related flags:
    ///      - `execution_finished_itself`: `false` (indicates whether the execution is complete).
    ///      - `execution_replicate_itself`: `false` (indicates whether replication is complete).
    ///      - `how_many_nodes_failed`: `0` (initializes the failure counter for nodes).
    ///    - Assigns the `node_that_execute`, `connections`, and `storage_engine` to the `QueryExecution` object.
    ///
    /// # Errors
    /// - Returns `NodeError` in the following cases:
    ///   - Failure to lock the `node_that_execute` mutex.
    ///   - Failure to initialize the `StorageEngine` (e.g., invalid `storage_path` or node IP issues).
    ///
    /// # Notes
    /// - This function is designed to be thread-safe, utilizing `Arc` and `Mutex` for shared resources.
    /// - Ensure that the `storage_path` is valid and accessible to avoid initialization errors.

    pub fn new(
        node_that_execute: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        storage_path: PathBuf,
    ) -> Result<QueryExecution, NodeError> {
        let ip = { node_that_execute.lock()?.get_ip_string() };

        let storage_engine = StorageEngine::new(storage_path, ip);
        Ok(QueryExecution {
            node_that_execute,
            connections,
            execution_finished_itself: false,
            execution_replicate_itself: false,
            how_many_nodes_failed: 0,
            storage_engine: storage_engine,
        })
    }

    /// Executes a query against the database, with support for various query types
    /// (e.g., SELECT, INSERT, UPDATE, DELETE, etc.) and internode communication.
    ///
    /// # Parameters
    /// - `query: Query`
    ///   - The query to be executed. This can be any of the following:
    ///     - `Query::Select` for SELECT queries.
    ///     - `Query::Insert` for INSERT queries.
    ///     - `Query::Update` for UPDATE queries.
    ///     - `Query::Delete` for DELETE queries.
    ///     - `Query::CreateTable`, `Query::DropTable`, `Query::AlterTable` for table management.
    ///     - `Query::CreateKeyspace`, `Query::DropKeyspace`, `Query::AlterKeyspace` for keyspace management.
    ///     - `Query::Use` for switching keyspaces.
    /// - `internode: bool`
    ///   - If `true`, enables internode communication for the query, involving other nodes in the cluster.
    /// - `replication: bool`
    ///   - Specifies whether the query should trigger data replication across nodes.
    /// - `open_query_id: i32`
    ///   - A unique identifier for the query being executed. This is used to track the query across nodes.
    /// - `client_id: i32`
    ///   - The identifier of the client initiating the query. Useful for audit trails or debugging.
    /// - `timestap: Option<i64>`
    ///   - Optional timestamp for the query. If `None`, certain queries (e.g., INSERT, UPDATE, DELETE) will fail with a `NodeError::InternodeProtocolError`.
    ///
    /// # Returns
    /// - `Result<Option<((i32, i32), InternodeResponse)>, NodeError>`
    ///   - On success:
    ///     - Returns `Ok(Some(((execution_status, node_failures), response)))` where:
    ///       - `execution_status`: Indicates how many internode queries finished successfully:
    ///         - `2` for both local and replicated execution completed.
    ///         - `1` for either local or replicated execution completed.
    ///         - `0` for neither completed.
    ///       - `node_failures`: Number of nodes that failed during execution.
    ///       - `response`: An `InternodeResponse` containing the query results or status.
    ///     - For internode communication, it may return a simpler response with metadata.
    ///   - On failure:
    ///     - Returns `Err(NodeError)` containing details of the error.
    ///
    /// # Query Execution Process
    /// - **SELECT Queries**:
    ///   - Executes `execute_select` to fetch rows from the database.
    ///   - Processes the results into columns, select columns, and row values.
    ///   - Constructs an `InternodeResponseContent` object with the query results.
    /// - **INSERT Queries**:
    ///   - Requires a valid timestamp (`timestap` parameter).
    ///   - Validates the target table within the context of the query's keyspace.
    ///   - Calls `execute_insert` to perform the operation.
    /// - **UPDATE Queries**:
    ///   - Similar to INSERT but updates rows in the database.
    /// - **DELETE Queries**:
    ///   - Removes rows based on the conditions specified in the query.
    /// - **Table and Keyspace Management**:
    ///   - Handles `CREATE`, `DROP`, and `ALTER` operations for tables and keyspaces.
    ///   - Operations are forwarded to specific handlers like `execute_create_table`.
    /// - **USE Queries**:
    ///   - Switches the keyspace context for subsequent queries.
    ///
    /// # Internode Communication
    /// - If `internode` is enabled, the function constructs an `InternodeResponse` object:
    ///   - `Ok`: Indicates the query succeeded.
    ///   - `Error`: Captures failures, logs the error, and updates the response status.
    /// - Non-internode queries return execution status and failure counts directly.
    ///
    /// # Error Handling
    /// - Returns a `NodeError` in case of failures, which could include:
    ///   - `InternodeProtocolError`: Missing timestamp for queries requiring one.
    ///   - `CQLError`: Specific errors during query execution, such as missing keyspace or table.
    ///   - `Other`: Errors encountered during query execution.
    ///
    pub fn execute(
        &mut self,
        query: Query,
        internode: bool,
        replication: bool,
        open_query_id: i32,
        client_id: i32,
        timestap: Option<i64>,
    ) -> Result<Option<((i32, i32), InternodeResponse)>, NodeError> {
        let mut response: InternodeResponse = InternodeResponse {
            open_query_id: open_query_id as u32,
            status: InternodeResponseStatus::Ok,
            content: None,
        };

        let query_result = {
            match query.clone() {
                Query::Select(select_query) => {
                    match self.execute_select(
                        select_query,
                        internode,
                        replication,
                        open_query_id,
                        client_id,
                    ) {
                        Ok(select_querys) => {
                            let columns: Vec<String> = select_querys
                                .get(0)
                                .map(|s| s.split(',').map(String::from).collect())
                                .unwrap_or_default();

                            let select_columns: Vec<String> = select_querys
                                .get(1)
                                .map(|s| s.split(',').map(String::from).collect())
                                .unwrap_or_default();

                            let values: Vec<Vec<String>> = if select_querys.len() > 2 {
                                let result: Vec<Vec<String>> = select_querys[2..]
                                    .iter()
                                    .map(|s| {
                                        // Dividir en dos partes por ";"
                                        if let Some((first_part, second_part)) = s.split_once(';') {
                                            // Dividir la primera parte por "," y agregar la segunda parte
                                            let mut combined: Vec<String> =
                                                first_part.split(',').map(String::from).collect();
                                            combined.push(second_part.to_string()); // Añadir la parte después de ";"
                                            combined
                                        } else {
                                            // Si no hay ";", considerar todo como un único valor
                                            vec![s.to_string()]
                                        }
                                    })
                                    .collect();
                                result
                            } else {
                                Vec::new()
                            };

                            response.content = Some(InternodeResponseContent {
                                columns: columns,
                                select_columns: select_columns,
                                values: values,
                            });
                            Ok(())
                        }
                        Err(e) => {
                            // Aquí podrías mapear a un error específico de `NodeError`
                            Err(e)
                        }
                    }
                }
                Query::Insert(insert_query) => {
                    let timestamp_n;
                    if let Some(t) = timestap {
                        timestamp_n = t;
                    } else {
                        return Err(NodeError::InternodeProtocolError);
                    }
                    let table;
                    {
                        let table_name = insert_query.into_clause.table_name.clone();
                        let mut guard_node = self.node_that_execute.lock()?;
                        let keyspace = guard_node
                            .get_open_handle_query()
                            .get_keyspace_of_query(open_query_id)?
                            .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;

                        table = guard_node.get_table(table_name, keyspace)?;
                    }
                    self.execute_insert(
                        insert_query,
                        table,
                        internode,
                        replication,
                        open_query_id,
                        client_id,
                        timestamp_n,
                    )
                }
                Query::Update(update_query) => {
                    let timestamp_n;
                    if let Some(t) = timestap {
                        timestamp_n = t;
                    } else {
                        return Err(NodeError::InternodeProtocolError);
                    }
                    self.execute_update(
                        update_query,
                        internode,
                        replication,
                        open_query_id,
                        client_id,
                        timestamp_n,
                    )
                }
                Query::Delete(delete_query) => {
                    let timestamp_n;
                    if let Some(t) = timestap {
                        timestamp_n = t;
                    } else {
                        return Err(NodeError::InternodeProtocolError);
                    }
                    self.execute_delete(
                        delete_query,
                        internode,
                        replication,
                        open_query_id,
                        client_id,
                        timestamp_n,
                    )
                }
                Query::CreateTable(create_table) => {
                    self.execute_create_table(create_table, open_query_id)
                }
                Query::DropTable(drop_table) => self.execute_drop_table(drop_table, open_query_id),
                Query::AlterTable(alter_table) => {
                    self.execute_alter_table(alter_table, open_query_id)
                }
                Query::CreateKeyspace(create_keyspace) => {
                    self.execute_create_keyspace(create_keyspace)
                }
                Query::DropKeyspace(drop_keyspace) => self.execute_drop_keyspace(drop_keyspace),
                Query::AlterKeyspace(alter_keyspace) => self.execute_alter_keyspace(alter_keyspace),
                Query::Use(_) => {
                    return Err(NodeError::OtherError);
                    //self.execute_use(use_cql, internode, open_query_id, client_id)
                }
            }
        };

        if internode {
            let response = {
                match query_result {
                    Ok(_) => response,

                    Err(_) => {
                        eprintln!(
                            "el error en este nodo es {:?} de la query {:?}",
                            query_result, query
                        );
                        InternodeResponse {
                            open_query_id: open_query_id as u32,
                            status: InternodeResponseStatus::Error,
                            content: None,
                        }
                    }
                }
            };
            Ok(Some(((0, 0), response)))
        } else {
            match query_result {
                Ok(_) => {
                    let how_many_internode_query_has_finish = match (
                        self.execution_finished_itself,
                        self.execution_replicate_itself,
                    ) {
                        (true, true) => 2,
                        (true, false) | (false, true) => 1,
                        (false, false) => 0,
                    };

                    return Ok(Some((
                        (
                            how_many_internode_query_has_finish,
                            self.how_many_nodes_failed,
                        ),
                        response,
                    )));
                }
                Err(e) => return Err(e),
            }
        }
    }

    // Función auxiliar para enviar un mensaje a todos los nodos en el partitioner
    fn _send_to_other_nodes(
        &self,
        local_node: MutexGuard<'_, Node>,
        serialized_message: &str,
        open_query_id: i32,
        client_id: i32,
        keyspace_name: &str,
        timestap: i64,
    ) -> Result<i32, NodeError> {
        let current_ip = local_node.get_ip();
        let message = InternodeMessage::new(
            current_ip,
            InternodeMessageContent::Query(InternodeQuery {
                query_string: serialized_message.to_string(),
                open_query_id: open_query_id as u32,
                client_id: client_id as u32,
                replication: false,
                keyspace_name: keyspace_name.to_string(),
                timestamp: timestap,
            }),
        );

        let mut failed_nodes = 0;

        for ip in local_node.get_partitioner().get_nodes() {
            if ip != current_ip {
                let result = connect_and_send_message(
                    ip,
                    INTERNODE_PORT,
                    self.connections.clone(),
                    message.clone(),
                );
                if result.is_err() {
                    failed_nodes += 1;
                }
            }
        }
        Ok(failed_nodes)
    }

    // Función auxiliar para enviar un mensaje a un nodo específico en el partitioner
    fn send_to_single_node(
        &self,
        self_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
        serialized_message: &str,
        open_query_id: i32,
        client_id: i32,
        keyspace_name: &str,
        timestap: i64,
        logger: Logger,
    ) -> Result<i32, NodeError> {
        let message = InternodeMessage::new(
            self_ip,
            InternodeMessageContent::Query(InternodeQuery {
                query_string: serialized_message.to_string(),
                open_query_id: open_query_id as u32,
                client_id: client_id as u32,
                replication: false,
                keyspace_name: keyspace_name.to_string(),
                timestamp: timestap,
            }),
        );

        logger.info(
            &format!(
                "INTERNODE (Query: {:?}): I SENT {:?} to {:?}",
                open_query_id, serialized_message, target_ip
            ),
            Color::Green,
            true,
        )?;

        let result = connect_and_send_message(
            target_ip,
            INTERNODE_PORT,
            self.connections.clone(),
            message.clone(),
        );

        if result.is_err() {
            return Ok(1);
        }

        Ok(0)
    }

    // Función auxiliar para enviar un mensaje a todos los nodos en el partitioner con replicación
    fn send_to_replication_nodes(
        &self,
        mut local_node: MutexGuard<'_, Node>,
        node_to_get_succesor: Ipv4Addr,
        serialized_message: &str,
        open_query_id: i32,
        client_id: i32,
        keyspace_name: &str,
        timestap: i64,
        logger: Logger,
    ) -> Result<(i32, bool), NodeError> {
        // Serializa el objeto que se quiere enviar

        // Bloquea el nodo para obtener el partitioner y la IP
        let current_ip = local_node.get_ip();

        let message = InternodeMessage::new(
            current_ip,
            InternodeMessageContent::Query(InternodeQuery {
                query_string: serialized_message.to_string(),
                open_query_id: open_query_id as u32,
                client_id: client_id as u32,
                replication: true,
                keyspace_name: keyspace_name.to_string(),
                timestamp: timestap,
            }),
        );

        let replication_factor = local_node
            .get_open_handle_query()
            .get_keyspace_of_query(open_query_id)?
            .ok_or(NodeError::KeyspaceError)?
            .get_replication_factor();

        let n_succesors = local_node
            .get_partitioner()
            .get_n_successors(node_to_get_succesor, (replication_factor - 1) as usize)?;

        let mut failed_nodes = 0;
        let mut the_node_has_to_replicate = false;

        // Recorre los nodos del partitioner y envía el mensaje a cada nodo excepto el actual
        for ip in n_succesors {
            if ip != current_ip {
                logger.info(
                    &format!(
                        "INTERNODE (Query: {:?}): I SENT as REPLICATION {:?} to {:?}",
                        open_query_id, serialized_message, ip
                    ),
                    Color::Green,
                    true,
                )?;

                let result = connect_and_send_message(
                    ip,
                    INTERNODE_PORT,
                    self.connections.clone(),
                    message.clone(),
                );
                if result.is_err() {
                    failed_nodes += 1;
                }
            } else {
                the_node_has_to_replicate = true;
            }
        }
        Ok((failed_nodes, the_node_has_to_replicate))
    }

    fn validate_values(&self, columns: Vec<Column>, values: &[String]) -> Result<(), CQLError> {
        if values.len() != columns.len() {
            return Err(CQLError::InvalidSyntax);
        }

        for (column, value) in columns.iter().zip(values) {
            if value == "" {
                continue;
            }
            if !column.data_type.is_valid_value(value) {
                return Err(CQLError::InvalidSyntax);
            }
        }
        Ok(())
    }
}
