// Local modules firstsrc/lib
mod errors;
mod internode_protocol;
mod internode_protocol_handler;
mod open_query_handler;
mod query_execution;
pub mod storage_engine;
mod utils;

// Standard libraries
use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;
use std::{env, thread, vec};

// External libraries
use chrono::Utc;
use driver::server::{handle_client_request, Request};
use errors::NodeError;
use gossip::structures::application_state::{KeyspaceSchema, NodeStatus, Schema, TableSchema};
use gossip::Gossiper;
use internode_protocol::message::{InternodeMessage, InternodeMessageContent};
use internode_protocol::response::{
    InternodeResponse, InternodeResponseContent, InternodeResponseStatus,
};
use internode_protocol::InternodeSerializable;
use internode_protocol_handler::InternodeProtocolHandler;
// use keyspace::Keyspace;
use logger::{Color, Logger};
use native_protocol::frame::Frame;
use native_protocol::messages::auth::{AuthSuccess, Authenticate};
use native_protocol::messages::error;
use native_protocol::Serializable;
use open_query_handler::OpenQueryHandler;
use partitioner::Partitioner;
use query_creator::clauses::keyspace::create_keyspace_cql::CreateKeyspace;
use query_creator::clauses::table::create_table_cql::CreateTable;
use query_creator::clauses::types::column::Column;
use query_creator::errors::CQLError;
use query_creator::{GetTableName, GetUsedKeyspace, NeedsKeyspace, NeedsTable, Query};
use query_creator::{NeededResponses, QueryCreator};
use query_execution::QueryExecution;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use storage_engine::StorageEngine;
use utils::{check_keyspace, check_table, connect_and_send_message};

const CLIENT_NODE_PORT: u16 = 0x4645; // Hexadecimal of "FE" (FERRUM) = 17989
const INTERNODE_PORT: u16 = 0x554D; // Hexadecimal of "UM" (FERRUM) = 21837

/// Represents a node within the distributed network.
/// The node can manage keyspaces, tables, and handle connections between nodes and clients.
///
pub struct Node {
    ip: Ipv4Addr,
    partitioner: Partitioner,
    open_query_handler: OpenQueryHandler,
    clients_keyspace: HashMap<i32, Option<String>>,
    last_client_id: i32,
    gossiper: Gossiper,
    storage_path: PathBuf,
    logger: Logger,
    /// Represents the latest known schema of the cluster.
    schema: Schema,
}

impl Node {
    /// Creates a new instance of a `Node` in a distributed database system.
    ///
    /// # Purpose
    /// This function initializes a node, sets up its partitioner, storage engine, and other essential components.
    /// Nodes are fundamental building blocks of the distributed system, responsible for storing, processing,
    /// and communicating data.
    ///
    /// # Parameters
    /// - `ip: Ipv4Addr`
    ///   - The IP address of the node being initialized. This address is used for communication and data partitioning.
    /// - `seeds_nodes: Vec<Ipv4Addr>`
    ///   - A list of IP addresses representing seed nodes in the cluster. These nodes are used to initialize the
    ///     partitioner and gossip protocol for cluster membership and state sharing.
    /// - `storage_path: PathBuf`
    ///   - The file system path where the node's storage engine will manage data and metadata.
    ///
    /// # Returns
    /// - `Result<Node, NodeError>`
    ///   - On success:
    ///     - Returns `Ok(Node)` with a fully initialized node instance.
    ///   - On failure:
    ///     - Returns `Err(NodeError)` if there is an issue with partitioner initialization, storage setup, or other components.
    ///
    /// # Behavior
    /// 1. **Partitioner Initialization**:
    ///    - Creates a new `Partitioner` instance and adds the current node's `ip` to the partition map.
    ///    - Iterates over the `seeds_nodes` list to add additional nodes to the partitioner, excluding the current node.
    /// 2. **Storage Engine Setup**:
    ///    - Initializes a `StorageEngine` with the provided `storage_path` and node's IP address.
    ///    - Resets storage folders to ensure a clean state for the node.
    /// 3. **Node Components**:
    ///    - Creates and configures the following components for the node:
    ///      - `OpenQueryHandler`: Manages queries currently being processed by the node.
    ///      - `clients_keyspace`: Tracks keyspaces for clients connected to the node.
    ///      - `last_client_id`: Initializes the client ID counter to zero.
    ///      - `gossiper`: Initializes the gossip protocol with the node's endpoint state and seed nodes.
    ///      - `schema`: Manages the database schema (e.g., keyspaces and tables).
    ///
    /// # Notes
    /// - **Seed Nodes**:
    ///   - Seed nodes are critical for the initial discovery of other nodes in the cluster.
    ///   - The current node (`ip`) is excluded from being added as its own seed.
    /// - **Storage Engine Reset**:
    ///   - Resetting storage folders ensures no residual data interferes with the node's operation.
    ///   - This operation should be used with caution in production environments to avoid unintended data loss.
    ///
    /// # Errors
    /// - Returns `NodeError` in the following scenarios:
    ///   - Failure to initialize or add nodes to the partitioner.
    ///   - Issues resetting storage folders during storage engine initialization.
    ///   - General failures in setting up the node's components.
    ///
    /// # Importance
    /// This function is the entry point for creating a node in the cluster. It ensures that the node is ready to
    /// participate in data storage, query processing, and cluster communication. Proper initialization of nodes
    /// is critical for maintaining the stability and reliability of the distributed system.

    pub fn new(
        ip: Ipv4Addr,
        seeds_nodes: Vec<Ipv4Addr>,
        storage_path: PathBuf,
    ) -> Result<Node, NodeError> {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(ip)?;

        let storage_engine = StorageEngine::new(storage_path.clone(), ip.to_string());
        storage_engine.reset_folders()?;

        for seed_ip in seeds_nodes.clone() {
            if seed_ip != ip {
                partitioner.add_node(seed_ip)?;
            }
        }

        Ok(Node {
            ip,
            partitioner,
            open_query_handler: OpenQueryHandler::new(),
            clients_keyspace: HashMap::new(),
            last_client_id: 0,
            storage_path: storage_path.clone(),
            gossiper: Gossiper::new()
                .with_endpoint_state(ip)
                .with_seeds(seeds_nodes),
            logger: Logger::new(&storage_path, &ip.to_string())?,
            schema: Schema::new(),
        })
    }

    /// Starts the gossip protocol for the node, enabling cluster membership and state sharing.
    ///
    /// # Purpose
    /// Gossip is a critical component in distributed databases for maintaining cluster membership,
    /// state synchronization, and fault detection. This function initiates a background thread that
    /// continuously executes the gossip protocol, exchanging state information with other nodes and
    /// ensuring that the cluster remains consistent and operational.
    ///
    /// # Parameters
    /// - `node: Arc<Mutex<Node>>`
    ///   - A thread-safe reference to the `Node` that will participate in the gossip protocol.
    ///   - The `Node` contains information about its state, schema, and connections to the cluster.
    /// - `connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>`
    ///   - A thread-safe map of active connections to other nodes in the cluster.
    ///     - Keys are node addresses as strings.
    ///     - Values are thread-safe `TcpStream` objects for internode communication.
    ///
    /// # Returns
    /// - `Result<(), NodeError>`
    ///   - On success:
    ///     - Returns `Ok(())` indicating that the gossip protocol started successfully.
    ///   - On failure:
    ///     - Returns `Err(NodeError)` if the initialization of the gossip thread fails.
    ///
    /// # Behavior
    /// 1. **Gossip Protocol Initialization**:
    ///    - Launches a background thread that executes the gossip protocol in a loop.
    ///    - Updates the node's status to `Normal` after an initial period (e.g., 1500ms).
    ///    - Sends periodic heartbeat messages to indicate the node is alive.
    ///
    /// 2. **Cluster Communication**:
    ///    - Picks target nodes for gossip communication using the `pick_ips` function from the `Gossiper`.
    ///    - Sends `SYN` messages to target nodes, carrying the node's state information.
    ///    - Handles node failures by marking unreachable nodes as `Dead` and triggering redistributions if necessary.
    ///    - Adds newly discovered nodes to the partitioner and integrates them into the cluster.
    ///
    /// 3. **Schema Updates**:
    ///    - Synchronizes the node's schema with the most recent schema available from the gossip protocol.
    ///    - Updates the node's schema metadata to reflect changes in the cluster (e.g., new tables or keyspaces).
    ///
    /// 4. **Partitioner Updates**:
    ///    - Adjusts the partitioner when nodes join or leave the cluster.
    ///    - Redistributes data across the cluster when changes in membership occur.
    ///
    /// 5. **Fault Tolerance**:
    ///    - Detects dead nodes and removes them from the partitioner to avoid stale data.
    ///    - Adds new nodes to the partitioner and redistributes data to maintain consistency.
    ///
    /// # Thread Execution
    /// - The gossip protocol runs indefinitely in a loop with a sleep interval of 1200ms between iterations.
    /// - Within each iteration:
    ///   - The node sends and receives gossip messages.
    ///   - Updates its internal state, schema, and partitioner as needed.
    ///
    /// # Notes
    /// - This function is critical for maintaining the health and consistency of the cluster.
    /// - The gossip thread runs in the background and continuously monitors the state of the cluster.
    /// - Redistributing data is a resource-intensive operation and should be handled carefully in large clusters.
    ///
    /// # Errors
    /// - Returns `NodeError` in the following scenarios:
    ///   - Failure to communicate with other nodes (e.g., network issues).
    ///   - Errors in partitioner operations, such as adding or removing nodes.
    ///   - Locking issues while accessing the `Node` or `connections`.
    ///
    /// # Importance
    /// The gossip protocol ensures that all nodes in the cluster have a consistent view of the system's state.
    /// It handles node failures, joins, and leaves dynamically, enabling the cluster to adapt to changes
    /// without manual intervention. This function is foundational for ensuring high availability and fault tolerance.
    ///
    /// # Example Workflow
    /// - A node starts the gossip protocol using `start_gossip`.
    /// - The gossip thread continuously communicates with other nodes, exchanging state information.
    /// - When a node joins or leaves, the partitioner adjusts and redistributes data as necessary.
    /// - The cluster remains consistent, and client queries are routed accurately based on the partitioner.

    pub fn start_gossip(
        node: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let _ = thread::spawn(move || {
            let initial_gossip = Instant::now();
            let mut log;
            loop {
                {
                    {
                        let mut node_guard = match node.lock() {
                            Ok(guard) => guard,
                            Err(_) => return NodeError::LockError,
                        };

                        let ip = node_guard.ip;
                        log = node_guard.get_logger();
                        if initial_gossip.elapsed().as_millis() > 3000 {
                            node_guard
                                .gossiper
                                .change_status(ip, NodeStatus::Normal)
                                .ok();
                        }
                        let _ = node_guard.gossiper.heartbeat(ip);
                    }

                    let ips: Vec<Ipv4Addr>;
                    let syn;
                    {
                        let node_guard = match node.lock() {
                            Ok(guard) => guard,
                            Err(_) => return NodeError::LockError,
                        };
                        ips = node_guard
                            .gossiper
                            .pick_ips(node_guard.get_ip())
                            .iter()
                            .map(|x| **x)
                            .collect();
                        syn = node_guard.gossiper.create_syn(node_guard.ip);
                    }

                    let mut node_guard = match node.lock() {
                        Ok(guard) => guard,
                        Err(_) => return NodeError::LockError,
                    };

                    for ip in ips {
                        let connections_clone = Arc::clone(&connections);
                        let msg = InternodeMessage::new(
                            ip.clone(),
                            InternodeMessageContent::Gossip(syn.clone()),
                        );

                        if connect_and_send_message(ip, INTERNODE_PORT, connections_clone, msg)
                            .is_err()
                        {
                            node_guard.gossiper.kill(ip).ok();
                        }
                    }
                }

                // After each gossip round, update the schema of the node
                {
                    let mut node_guard = match node.lock() {
                        Ok(guard) => guard,
                        Err(_) => return NodeError::LockError,
                    };

                    let ip = node_guard.ip;

                    // Sets the schema of the current node to the most updated schema
                    if let Some(schema) = node_guard.gossiper.get_most_updated_schema() {
                        if let Some(endpoint_state) =
                            node_guard.gossiper.endpoints_state.get_mut(&ip)
                        {
                            endpoint_state.application_state.set_schema(schema);
                        } else {
                            return NodeError::GossipError;
                        }
                    }

                    // Updates the latest schema from the gossiper
                    if let Err(e) = node_guard.set_latest_schema_from_gossiper() {
                        return e;
                    };
                }

                // After each gossip round, update the partitioner
                {
                    // Bloqueo del mutex solo para extraer lo necesario
                    let (storage_path, self_ip, keyspaces, logger) = {
                        let node_guard = match node.lock() {
                            Ok(guard) => guard,
                            Err(_) => return NodeError::LockError,
                        };

                        (
                            node_guard.storage_path.clone(), // Clonar el path de almacenamiento
                            node_guard.get_ip().to_string(), // Clonar el IP
                            node_guard.schema.keyspaces.clone(),
                            node_guard.get_logger(), // Clonar los keyspaces desde el guard     // Referencia mutable al particionador
                        )
                    };
                    let mut node_guard = match node.lock() {
                        Ok(guard) => guard,
                        Err(_) => return NodeError::LockError,
                    };
                    let endpoints_states = &node_guard.gossiper.endpoints_state.clone();
                    let partitioner = &mut node_guard.partitioner;
                    let mut needs_to_redistribute = false;

                    for (ip, state) in endpoints_states {
                        let is_in_partitioner: bool;
                        let result = partitioner.node_already_in_partitioner(ip);
                        if let Ok(is_in) = result {
                            is_in_partitioner = is_in;
                        } else {
                            return NodeError::PartitionerError(
                                partitioner::errors::PartitionerError::HashError,
                            );
                        }

                        if state.application_state.status.is_dead() {
                            if is_in_partitioner {
                                needs_to_redistribute = true;
                                partitioner.remove_node(*ip).ok();
                                let _ = log.info(
                                    &format!(
                                        "NODE {:?} IS DEAD .. New Ring: {:?}",
                                        ip, partitioner
                                    ),
                                    Color::Red,
                                    true,
                                );
                            }
                        } else {
                            if !is_in_partitioner {
                                //println!("se acaba de unir un nodo, redistribuyo");
                                needs_to_redistribute = true;
                                partitioner.add_node(*ip).ok();
                                let _ = log.info(
                                    &format!("NEW NODE {:?} .. New Ring: {:?}", ip, partitioner),
                                    Color::Green,
                                    true,
                                );
                            }
                        }
                    }

                    if needs_to_redistribute {
                        let _ = logger.info("START REDISTRIBUTION...", Color::Cyan, true);

                        // Clonar las variables necesarias para el nuevo hilo
                        let storage_path = storage_path.clone();
                        let self_ip = self_ip.clone();
                        let partitioner = partitioner.clone();
                        let logger = logger.clone();
                        let connections = connections.clone();
                        let keyspaces: Vec<KeyspaceSchema> = keyspaces.values().cloned().collect();

                        let redistribution_result =
                            storage_engine::StorageEngine::new(storage_path, self_ip)
                                .redistribute_data(
                                    keyspaces,
                                    &partitioner,
                                    logger.clone(),
                                    connections,
                                );

                        match redistribution_result {
                            Ok(_) => {
                                let _ =
                                    logger
                                        .clone()
                                        .info("END REDISTRIBUTION...", Color::Cyan, true);
                            }
                            Err(e) => {
                                let _ = logger
                                    .clone()
                                    .error(&format!("REDISTRIBUTION FAILED! {:?}", e), true);
                            }
                        }
                    }
                }
                let gossip_logger = log.clone();
                let _ = gossip_logger
                    .clone()
                    .info("GOSSIP: New Gossip Round", Color::White, true);
                thread::sleep(std::time::Duration::from_millis(1000));
            }
        });
        Ok(())
    }

    /// Adds a new open query in the node, initializing its tracking and determining the required responses.
    ///
    /// # Purpose
    /// This function sets up a new query for execution, associating it with the client connection
    /// and tracking its progress. It calculates the required number of responses based on the query type,
    /// replication factor, and cluster size.
    ///
    /// # Arguments
    /// - `query: Query`
    ///   - The query object representing the operation to be executed (e.g., SELECT, INSERT, UPDATE, DELETE).
    /// - `consistency_level: &str`
    ///   - The desired consistency level for the query (e.g., `ONE`, `QUORUM`, `ALL`).
    ///   - Determines the number of nodes that must respond successfully for the query to be considered successful.
    /// - `connection: TcpStream`
    ///   - The TCP connection to the client issuing the query. This is used to send the query's result or error back to the client.
    /// - `table: Option<TableSchema>`
    ///   - The schema of the table associated with the query, if applicable.
    /// - `keyspace: Option<KeyspaceSchema>`
    ///   - The schema of the keyspace associated with the query, if applicable.
    ///
    /// # Returns
    /// - `Result<i32, NodeError>`
    ///   - On success:
    ///     - Returns the unique ID of the newly opened query.
    ///   - On failure:
    ///     - Returns a `NodeError` if there is an issue during query initialization or schema access.
    ///
    /// # Behavior
    /// 1. **Cluster Information Retrieval**:
    ///    - Determines the total number of nodes in the cluster using `self.get_how_many_nodes_i_know`.
    /// 2. **Replication Factor Determination**:
    ///    - Retrieves the replication factor from the `keyspace` schema if provided.
    ///    - Defaults to a replication factor of `1` if the keyspace is not specified.
    /// 3. **Response Calculation**:
    ///    - Determines the number of responses required for the query to satisfy the consistency level:
    ///      - For `NeededResponseCount::One`, requires one response.
    ///      - For `NeededResponseCount::Specific`, calculates the responses based on the query's specified requirement
    ///        and the replication factor, but caps it at the total number of nodes in the cluster.
    /// 4. **Open Query Initialization**:
    ///    - Registers the query with the specified parameters, including the number of required responses,
    ///      client connection, query details, and associated schema, using `self.open_query_handler.new_open_query`.
    ///
    /// # Notes
    /// - **Replication Factor**:
    ///   - The replication factor determines how many copies of the data exist in the cluster and influences the consistency guarantees.
    /// - **Consistency Level**:
    ///   - Directly affects the `needed_responses` calculation, determining how strictly the cluster adheres to the query's requirements.
    ///
    /// # Errors
    /// - Returns a `NodeError` in the following scenarios:
    ///   - Issues accessing or cloning the keyspace or table schema.
    ///   - Errors in initializing the query in the `open_query_handler`.
    ///
    /// # Importance
    /// This function is essential for managing distributed queries in the cluster. It ensures that queries are
    /// properly initialized, tracks their progress, and enforces consistency requirements based on the cluster's
    /// configuration and the client's desired guarantees.

    pub fn add_open_query(
        &mut self,
        query: Query,
        consistency_level: &str,
        tx_reply: Sender<Frame>,
        table: Option<TableSchema>,
        keyspace: Option<KeyspaceSchema>,
    ) -> Result<i32, NodeError> {
        let all_nodes = self.get_how_many_nodes_i_know();

        let replication_factor = {
            if let Some(value) = keyspace.clone() {
                value.get_replication_factor()
            } else {
                1
            }
        };

        let needed_responses = match query.needed_responses() {
            query_creator::NeededResponseCount::One => 1,
            query_creator::NeededResponseCount::ReplicationFactor => {
                let calculated_responses = replication_factor as usize;
                if calculated_responses > all_nodes {
                    all_nodes
                } else {
                    calculated_responses
                }
            }
        };

        Ok(self.open_query_handler.new_open_query(
            needed_responses as i32,
            tx_reply,
            query,
            consistency_level,
            table,
            keyspace,
        ))
    }

    fn get_ip(&self) -> Ipv4Addr {
        self.ip
    }

    pub fn get_logger(&self) -> Logger {
        self.logger.clone()
    }
    fn get_ip_string(&self) -> String {
        self.ip.to_string()
    }

    fn get_how_many_nodes_i_know(&self) -> usize {
        self.partitioner.get_nodes().len() - 1
    }

    fn get_partitioner(&self) -> Partitioner {
        self.partitioner.clone()
    }

    fn get_open_handle_query(&mut self) -> &mut OpenQueryHandler {
        &mut self.open_query_handler
    }

    fn generate_client_id(&mut self) -> i32 {
        self.last_client_id += 1;
        self.clients_keyspace.insert(self.last_client_id, None);
        self.last_client_id
    }

    fn update_schema_in_storage(&self, old_schema: Schema) -> Result<(), NodeError> {
        let storage = StorageEngine::new(self.storage_path.clone(), self.ip.to_string());

        // Process new or updated keyspaces
        for (keyspace_name, keyspace) in self.schema.keyspaces.clone() {
            if !old_schema.keyspaces.contains_key(&keyspace_name) {
                // Create a new keyspace
                storage.create_keyspace(&keyspace_name)?;
            }

            let old_tables = old_schema
                .keyspaces
                .get(&keyspace_name)
                .map(|keyspace| keyspace.tables.clone())
                .unwrap_or_else(Vec::new);

            // Update existing keyspace
            self.update_keyspace_tables(&storage, &keyspace_name, old_tables, keyspace.tables)?
        }

        // Process deleted keyspaces
        for (keyspace_name, keyspace) in old_schema.clone().keyspaces {
            if !self.schema.keyspaces.contains_key(&keyspace_name) {
                // Drop keyspace
                storage.drop_keyspace(&keyspace_name, &self.ip.to_string())?;
            } else {
                // Drop tables from existing keyspace

                let new_tables = self
                    .schema
                    .keyspaces
                    .get(&keyspace_name)
                    .map(|keyspace| keyspace.tables.clone())
                    .unwrap_or_else(Vec::new);

                self.remove_obsolete_tables(&storage, &keyspace_name, keyspace.tables, new_tables)?;
            }
        }
        Ok(())
    }

    // Updates tables in an existing keyspace by creating new tables if they don't exist.
    fn update_keyspace_tables(
        &self,
        storage: &StorageEngine,
        keyspace_name: &str,
        old_tables: Vec<TableSchema>,
        new_tables: Vec<TableSchema>,
    ) -> Result<(), NodeError> {
        for table in new_tables {
            if old_tables
                .iter()
                .find(|old_table| old_table.get_name() == table.get_name())
                .is_none()
            {
                // Create a new table
                let cols = table.get_columns();
                let col_names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();

                storage.create_table(keyspace_name, &table.get_name(), col_names)?
            }
        }
        Ok(())
    }

    // Removes tables from an existing keyspace that are no longer present in the updated schema.
    fn remove_obsolete_tables(
        &self,
        storage: &StorageEngine,
        keyspace_name: &str,
        old_tables: Vec<TableSchema>,
        new_tables: Vec<TableSchema>,
    ) -> Result<(), NodeError> {
        for table in old_tables {
            if new_tables
                .iter()
                .find(|new_table| new_table.get_name() == table.get_name())
                .is_none()
            {
                // Drop table
                storage.drop_table(keyspace_name, &table.get_name())?;
            }
        }
        Ok(())
    }

    fn set_latest_schema_from_gossiper(&mut self) -> Result<(), NodeError> {
        let old_schema = self.schema.clone();

        // acá se actualiza el schema del nodo
        self.schema = match self.gossiper.endpoints_state.get(&self.ip) {
            Some(endpoint_state) => endpoint_state.application_state.schema.clone(),
            None => return Err(NodeError::LockError),
        };

        self.update_schema_in_storage(old_schema)?;
        //println!("Schema updated: {:?}", self.schema);
        Ok(())
    }

    fn add_keyspace(&mut self, new_keyspace: CreateKeyspace) -> Result<(), NodeError> {
        self.gossiper
            .add_keyspace(self.ip, new_keyspace)
            .map_err(|_| NodeError::KeyspaceError)?;

        // We manually update the latest schema right after modification so
        // we don't have to wait for the next gossip round.
        self.set_latest_schema_from_gossiper()?;

        Ok(())
    }

    fn remove_keyspace(&mut self, keyspace_name: String) -> Result<(), NodeError> {
        self.gossiper
            .remove_keyspace(self.ip, &keyspace_name)
            .map_err(|_| NodeError::KeyspaceError)?;

        // Recorre los clients_keyspace para encontrar y actualizar keyspaces coincidentes
        for (_, client_keyspace) in self.clients_keyspace.iter_mut() {
            if let Some(ref keyspace) = client_keyspace {
                if keyspace == &keyspace_name {
                    *client_keyspace = None;
                }
            }
        }

        // We manually update the latest schema right after modification so
        // we don't have to wait for the next gossip round.
        self.set_latest_schema_from_gossiper()?;

        Ok(())
    }

    fn _set_actual_keyspace(
        &mut self,
        keyspace_name: String,
        client_id: i32,
    ) -> Result<(), NodeError> {
        // Configurar el keyspace actual del cliente usando el índice encontrado
        self.clients_keyspace.insert(client_id, Some(keyspace_name));

        Ok(())
    }

    fn _update_keyspace(&mut self, client_id: i32, new_keyspace: KeyspaceSchema) {
        let new_key_name = new_keyspace.clone().get_name().clone();
        self.clients_keyspace
            .insert(client_id, Some(new_key_name.clone()));

        for (_, (kespace_name, _)) in self.schema.keyspaces.clone().iter().enumerate() {
            if kespace_name == &new_key_name {
                self.schema
                    .keyspaces
                    .insert(new_key_name.clone(), new_keyspace.clone());
            }
            // if new_key_name == keyspace.get_name() {
            //     self.keyspaces[i] = new_keyspace.clone();
            // }
        }
        // unimplemented!()
    }

    fn add_table(&mut self, new_table: CreateTable, keyspace_name: &str) -> Result<(), NodeError> {
        self.gossiper
            .add_table(self.ip, new_table, keyspace_name)
            .map_err(|_| NodeError::GossipError)?;

        // We manually update the latest schema right after modification so
        // we don't have to wait for the next gossip round.
        self.set_latest_schema_from_gossiper()?;

        Ok(())
    }

    fn get_table(
        &self,
        table_name: String,
        client_keyspace: KeyspaceSchema,
    ) -> Result<TableSchema, NodeError> {
        // Busca y devuelve la tabla solicitada
        // TODO: acá buscar en schema, no en client_keyspace ???

        client_keyspace
            .get_table(&table_name)
            .map_err(|_| NodeError::CQLError(CQLError::InvalidTable))
    }

    fn remove_table(&mut self, table_name: String, open_query_id: i32) -> Result<(), NodeError> {
        // Obtiene el keyspace actual del cliente
        let keyspace_name = self
            .get_open_handle_query()
            .get_keyspace_of_query(open_query_id)?
            .ok_or(NodeError::KeyspaceError)?
            .get_name();

        self.gossiper
            .remove_table(self.ip, &keyspace_name, &table_name)
            .map_err(|_| NodeError::KeyspaceError)?;

        // We manually update the latest schema right after modification so
        // we don't have to wait for the next gossip round.
        self.set_latest_schema_from_gossiper()?;

        Ok(())
    }

    fn update_table(
        &mut self,
        _keyspace_name: &str,
        _new_table: CreateTable,
    ) -> Result<(), NodeError> {
        // // Encuentra el índice del Keyspace en el Vec
        // if let Some(index) = self
        //     .keyspaces
        //     .iter()
        //     .position(|k| k.get_name() == keyspace_name)
        // {
        //     // Obtenemos una referencia mutable al Keyspace en el índice encontrado
        //     let keyspace = &mut self.keyspaces[index];

        //     // Encuentra la posición de la tabla a actualizar en el keyspace
        //     let table_index = keyspace
        //         .tables
        //         .iter()
        //         .position(|table| table.get_name() == new_table.get_name())
        //         .ok_or(NodeError::CQLError(CQLError::InvalidTable))?;

        //     // Reemplaza la tabla existente con la nueva en el Keyspace
        //     keyspace.tables[table_index] = Table::new(new_table);
        //     Ok(())
        // } else {
        //     // Retorna un error si el Keyspace no se encuentra
        //     Err(NodeError::KeyspaceError)
        // }
        unimplemented!()
    }

    fn table_already_exist(
        &mut self,
        table_name: String,
        keyspace_name: String,
    ) -> Result<bool, NodeError> {
        let keyspace = self
            .get_keyspace(&keyspace_name)?
            .ok_or(NodeError::KeyspaceError)?;

        // Verifica si la tabla ya existe en el keyspace del cliente
        for table in keyspace.get_tables() {
            if table.get_name() == table_name {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn get_client_keyspace(&self, client_id: i32) -> Result<Option<KeyspaceSchema>, NodeError> {
        let keyspace_name = self
            .clients_keyspace
            .get(&client_id)
            .ok_or(NodeError::InternodeProtocolError)
            .cloned()?;

        if let Some(value) = keyspace_name {
            Ok(self.schema.keyspaces.get(&value).cloned())
        } else {
            Ok(None)
        }
    }

    fn get_keyspace(&self, keyspace_name: &str) -> Result<Option<KeyspaceSchema>, NodeError> {
        Ok(self.schema.keyspaces.get(keyspace_name).cloned())
    }

    /// Starts the node's core functionalities, including internode connections, gossip, and client connections.
    ///
    /// # Purpose
    /// This function is responsible for initializing and starting the main operational threads of a node
    /// in a distributed database system. It ensures that the node can handle internode communication,
    /// participate in the gossip protocol, and serve client requests simultaneously.
    ///
    /// # Parameters
    /// - `node: Arc<Mutex<Node>>`
    ///   - A thread-safe reference to the `Node` instance being started.
    ///   - Contains the node's state, schema, partitioner, and other critical components.
    /// - `connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>`
    ///   - A thread-safe map of active TCP connections to other nodes and clients.
    ///     - Keys are addresses (as strings), and values are `TcpStream` objects for communication.
    ///
    /// # Returns
    /// - `Result<(), NodeError>`
    ///   - On success:
    ///     - Returns `Ok(())`, indicating that all threads started successfully and are running.
    ///   - On failure:
    ///     - Returns `Err(NodeError)` if any thread fails to start or encounters an unrecoverable error.
    ///
    /// # Behavior
    /// 1. **Retrieve Node IP**:
    ///    - Extracts the node's IP address by locking the `node` reference.
    ///
    /// 2. **Thread for Internode Connections**:
    ///    - Creates a thread to handle connections between nodes in the cluster.
    ///    - Uses the `handle_node_connections` function to manage internode communication and synchronize state.
    ///
    /// 3. **Thread for Gossip Protocol**:
    ///    - Starts a background thread for the gossip protocol using `start_gossip`.
    ///    - Gossip ensures cluster membership, state sharing, and failure detection.
    ///
    /// 4. **Thread for Client Connections**:
    ///    - Creates a thread to handle incoming client connections and requests.
    ///    - Uses the `handle_client_connections` function to manage client queries and responses.
    ///
    /// 5. **Thread Joining**:
    ///    - Waits for the threads handling internode connections and client connections to complete using `join`.
    ///    - Propagates errors if any thread encounters a failure or panic.
    ///
    /// # Error Handling
    /// - Errors are logged for each thread individually using `unwrap_or_else` to ensure independent thread robustness.
    /// - If any thread panics or fails during execution:
    ///   - The error is captured and returned as a `NodeError`.
    ///
    /// # Notes
    /// - **Thread-Safe Design**:
    ///   - The function leverages `Arc<Mutex<T>>` to ensure safe concurrent access to shared resources (e.g., `node` and `connections`).
    /// - **Critical Functionality**:
    ///   - This function is a key entry point for activating a node in the cluster. Without it, the node cannot
    ///     participate in cluster operations or serve clients.
    ///
    /// # Errors
    /// - Returns `NodeError::InternodeError` if the internode connections thread fails.
    /// - Returns `NodeError::ClientError` if the client connections thread fails.
    /// - Errors during gossip are logged but do not cause the `start` function to fail.
    ///
    /// # Importance
    /// This function encapsulates the primary operational lifecycle of a node in the system. By starting the gossip protocol,
    /// managing internode connections, and handling client queries, it ensures the node's integration into the cluster
    /// and its ability to serve requests efficiently.

    pub fn start(
        node: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        let self_ip;
        let log;
        {
            let node_guard = node.lock()?;
            self_ip = node_guard.get_ip();
            log = node_guard.get_logger().clone();
        }

        let log_gossip = log.clone();
        // Creates a thread to handle gossip
        let gossip_connections = Arc::clone(&connections);
        let node_gossip = Arc::clone(&node);
        Self::start_gossip(node_gossip, gossip_connections).unwrap_or_else(|err| {
            let message = format!("ERROR in GOSSIP: {:?}", err);
            log_gossip.clone().error(&message, true).ok(); // Or handle the error as needed
        });

        //thread::sleep(Duration::from_secs(2));
        // Creates a thread to handle client connections
        let client_connections_node = Arc::clone(&node);
        let client_connections = Arc::clone(&connections);
        let self_ip_client = self_ip;

        let log_client = log.clone();
        let handle_client_thread = thread::spawn(move || {
            Self::handle_client_connections(
                client_connections_node,
                client_connections,
                self_ip_client,
            )
            .unwrap_or_else(|e| {
                let message = format!("ERROR in CLIENT CONNECTIONS: {:?}", e);
                log_client.clone().error(&message, true).ok();
            });
        });

        // Creates a thread to handle node connections
        let node_connections_node = Arc::clone(&node);
        let node_connections = Arc::clone(&connections);
        let self_ip_node = self_ip.clone();
        let log_internode = log.clone();
        let handle_node_thread = thread::spawn(move || {
            Self::handle_node_connections(node_connections_node, node_connections, self_ip_node)
                .unwrap_or_else(|err| {
                    let message = format!("ERROR in INTERNODE CONNECTIONS: {:?}", err);
                    log_internode.error(&message, true).ok(); // Or handle the error as needed
                });
        });

        handle_node_thread
            .join()
            .map_err(|_| NodeError::InternodeError)?;
        handle_client_thread
            .join()
            .map_err(|_| NodeError::ClientError)?;

        Ok(())
    }

    fn handle_node_connections(
        node: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        self_ip: std::net::Ipv4Addr,
    ) -> Result<(), NodeError> {
        let socket = SocketAddrV4::new(self_ip, INTERNODE_PORT);
        let listener = TcpListener::bind(socket)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let node_clone = Arc::clone(&node);
                    let stream = Arc::new(Mutex::new(stream)); // Encapsulates the stream in Arc<Mutex<TcpStream>>
                    let connections_clone = Arc::clone(&connections);

                    thread::spawn(move || {
                        if let Err(e) = Node::handle_incoming_internode_messages(
                            node_clone,
                            stream,
                            connections_clone,
                        ) {
                            eprintln!("{:?}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Error accepting internode connection: {:?}", e);
                }
            }
        }

        Ok(())
    }

    fn handle_client_connections(
        node: Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        self_ip: std::net::Ipv4Addr,
    ) -> Result<(), NodeError> {
        // Cargar configuración TLS
        let project_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        let path_certs = Path::new(&project_dir)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("certs");

        // Cargar configuración TLS
        let certs = CertificateDer::pem_file_iter(path_certs.join("cert.crt"))
            .unwrap()
            .map(|cert| cert.unwrap())
            .collect();
        let private_key = PrivateKeyDer::from_pem_file(path_certs.join("cert.key")).unwrap();

        match rustls::crypto::aws_lc_rs::default_provider().install_default() {
            Ok(_) => {}
            Err(err) => {
                eprintln!("Failed to install CryptoProvider: {:?}", err);
            }
        }

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)
            .unwrap();

        let socket = SocketAddrV4::new(self_ip, CLIENT_NODE_PORT); // Specific port for clients
        let listener = TcpListener::bind(socket)?;

        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    // Crear una conexión TLS para el stream TCP
                    let mut conn = ServerConnection::new(Arc::new(config.clone()))
                        .expect("No se pudo crear la conexión TLS");

                    conn.complete_io(&mut stream).unwrap();
                    let connections_clone = Arc::clone(&connections);

                    let stream = StreamOwned::new(conn, stream);

                    let node_clone = Arc::clone(&node);
                    thread::spawn(move || {
                        Node::handle_incoming_client_messages(node_clone, stream, connections_clone)
                    });
                }
                Err(e) => {
                    eprintln!("Error accepting client connection: {:?}", e);
                }
            }
        }

        Ok(())
    }

    // Receives packets from the client
    fn handle_incoming_client_messages(
        node: Arc<Mutex<Node>>,
        mut stream: StreamOwned<ServerConnection, TcpStream>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        // Clone the stream under Mutex protection and create the reader

        let client_id;
        let log;

        {
            let mut guard_node = node.lock()?;
            client_id = guard_node.generate_client_id();
            log = guard_node.get_logger();
        };

        let mut is_authenticated = false;

        loop {
            // Clean the buffer

            let mut buffer = [0; 2048];

            // Execute initial inserts if necessary

            // Try to read a line
            let bytes_read = stream.read(&mut buffer);

            match bytes_read {
                Ok(0) => {
                    // Connection closed
                    break;
                }
                Ok(_) => {
                    let request = handle_client_request(&buffer).unwrap();

                    match request {
                        Request::Startup => {
                            let auth = Frame::Authenticate(Authenticate::default()).to_bytes()?;
                            stream.write(auth.as_slice())?;
                            stream.flush()?;
                        }
                        Request::AuthResponse(password) => {
                            let response = if password == "admin" {
                                is_authenticated = true;
                                Frame::AuthSuccess(AuthSuccess::default()).to_bytes()?
                            } else {
                                Frame::Authenticate(Authenticate::default()).to_bytes()?
                            };

                            stream.write(response.as_slice())?;
                            stream.flush()?;
                        }
                        Request::Query(query) => {
                            if !is_authenticated {
                                let auth =
                                    Frame::Authenticate(Authenticate::default()).to_bytes()?;
                                stream.write(auth.as_slice())?;
                                stream.flush()?;
                                continue;
                            }
                            // Handle the query
                            let query_str = query.get_query();
                            let query_consistency_level: &str = &query.get_consistency();
                            log.info(
                                &format!(
                                    "NATIVE: I RECEIVED {} whit CL: {} from CLIENT",
                                    query_str.replace("\n", ""),
                                    query_consistency_level,
                                ),
                                Color::Yellow,
                                true,
                            )?;

                            let (tx_reply, rx_reply) = mpsc::channel();

                            let result = Node::handle_query_execution(
                                query_str,
                                query_consistency_level,
                                &node,
                                connections.clone(),
                                tx_reply,
                                client_id,
                            );

                            if let Err(e) = result {
                                let frame = Frame::Error(error::Error::ServerError(e.to_string()));

                                let frame_bytes_result = &frame.to_bytes();
                                let mut frame_bytes = &vec![];
                                if let Ok(value) = frame_bytes_result {
                                    frame_bytes = value;
                                }
                                stream.write(&frame_bytes)?;
                                stream.flush()?;
                            } else {
                                // await resolution of the query
                                let reply = rx_reply.recv().map_err(|_| NodeError::OtherError)?;
                                stream.write(&reply.to_bytes()?)?;
                            }
                        }
                    };
                }
                Err(_) => {
                    // Another type of error
                    return Err(NodeError::OtherError);
                }
            }
        }

        Ok(())
    }

    fn handle_incoming_internode_messages(
        node: Arc<Mutex<Node>>,
        stream: Arc<Mutex<TcpStream>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    ) -> Result<(), NodeError> {
        // Clone the stream under Mutex protection and create the reader
        let mut reader = {
            let stream_guard = stream.lock()?;
            BufReader::new(stream_guard.try_clone().map_err(NodeError::IoError)?)
        };

        let internode_protocol_handler = InternodeProtocolHandler::new();

        loop {
            // Clean the buffer
            let mut buffer = [0u8; 850000];

            // Execute initial inserts if necessary

            // Self::execute_querys(&node, connections.clone())?;

            // Try to read a line
            let bytes_read = reader.read(&mut buffer);
            let result = InternodeMessage::from_bytes(&buffer);

            let message;

            match result {
                Ok(value) => {
                    message = value;
                }
                Err(_) => {
                    //println!("error al procesar mensaje internodo");
                    // println!("Error al crear los bytes: {:?}", e);

                    continue;
                }
            }

            match bytes_read {
                Ok(0) => {
                    // Connection closed
                    break;
                }
                Ok(_) => {
                    // Process the command with the protocol, passing the buffer and the necessary parameters
                    let result = internode_protocol_handler.handle_command(
                        &node,
                        message.clone(),
                        connections.clone(),
                    );

                    // If there's an error handling the command, exit the loop
                    if let Err(e) = result {
                        eprintln!("{:?} when other node sent me {:?}", e, message);
                        break;
                    }
                }
                Err(_) => {
                    // Another type of error
                    return Err(NodeError::OtherError);
                }
            }
        }

        Ok(())
    }

    fn current_timestamp() -> i64 {
        Utc::now().timestamp()
    }

    fn handle_query_execution(
        query_str: &str,
        consistency_level: &str,
        node: &Arc<Mutex<Node>>,
        connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
        tx_reply: Sender<Frame>,
        client_id: i32,
    ) -> Result<(), NodeError> {
        let query = QueryCreator::new()
            .handle_query(query_str.to_string())
            .map_err(NodeError::CQLError)?;

        if query.needs_keyspace() {
            //println!("esta query: {:?} necesita un keyspace", query_str);
            check_keyspace(node, &query, client_id, 6)?;
        }

        if query.needs_table() {
            //println!("esta query: {:?} necesita una tabla", query_str);
            check_table(node, &query, client_id, 6)?;
        }

        let open_query_id;
        let self_ip: Ipv4Addr;
        let storage_path;
        let logger;
        {
            let mut guard_node = node.lock()?;
            let keyspace;
            // Obtener el keyspace especificado o el actual del cliente
            if let Some(keyspace_name) = query.get_used_keyspace() {
                keyspace = guard_node.get_keyspace(&keyspace_name)?
            } else {
                keyspace = guard_node.get_client_keyspace(client_id)?;
            }

            // Intentar obtener el nombre de la tabla y buscar la tabla correspondiente en el keyspace
            let table = query.get_table_name().and_then(|table_name| {
                keyspace
                    .clone()
                    .and_then(|k| guard_node.get_table(table_name, k).ok())
            });

            // Agregar la consulta abierta
            open_query_id = guard_node.add_open_query(
                query.clone(),
                consistency_level,
                tx_reply,
                table,
                keyspace,
            )?;
            self_ip = guard_node.get_ip();
            storage_path = guard_node.storage_path.clone();
            logger = guard_node.get_logger();
        }
        let timestamp = Self::current_timestamp();

        let response =
            QueryExecution::new(node.clone(), connections.clone(), storage_path.clone())?.execute(
                query.clone(),
                false,
                false,
                open_query_id,
                client_id,
                Some(timestamp),
            )?;

        if let Some(((finished_responses, failed_nodes), content)) = response {
            let mut guard_node = node.lock()?;
            // Obtener el keyspace especificado o el actual del cliente

            let keyspace = guard_node
                .get_open_handle_query()
                .get_keyspace_of_query(open_query_id)?
                .clone();

            // Intentar obtener el nombre de la tabla y buscar la tabla correspondiente en el keyspace
            let table = query.get_table_name().and_then(|table_name| {
                keyspace
                    .clone()
                    .and_then(|k| guard_node.get_table(table_name, k).ok())
            });
            let columns: Vec<Column> = {
                if let Some(table) = table.clone() {
                    table.get_columns()
                } else {
                    vec![]
                }
            };

            let keyspace_name: String = if let Some(key) = keyspace.clone() {
                key.get_name()
            } else {
                "".to_string()
            };

            let partitioner = guard_node.get_partitioner();
            let query_handler = guard_node.get_open_handle_query();

            for _ in 0..finished_responses {
                let mut select_columns: Vec<String> = vec![];
                let mut values: Vec<Vec<String>> = vec![];
                let mut complete_columns: Vec<String> = vec![];
                if let Some(cont) = content.content.clone() {
                    complete_columns = cont.columns.clone();
                    select_columns = cont.select_columns.clone();
                    values = cont.values.clone();
                }

                InternodeProtocolHandler::add_ok_response_to_open_query_and_send_response_if_closed(
                    query_handler,
                    // TODO: convertir el content al content de la response
                    &InternodeResponse::new(open_query_id as u32, InternodeResponseStatus::Ok, Some(InternodeResponseContent{
                        columns: complete_columns,
                        select_columns:  select_columns,
                        values: values,
                    })),
                    open_query_id,
                    keyspace_name.clone(),
                    table.clone(),
                    columns.clone(),
                    self_ip,
                    self_ip,
                    connections.clone(),
                    partitioner.clone(),
                    storage_path.clone(),
                    logger.clone(),
                )?;
            }
            for _ in 0..failed_nodes {
                InternodeProtocolHandler::add_error_response_to_open_query_and_send_response_if_closed(
                    query_handler,
                    open_query_id,

                )?;
            }
        }

        Ok(())
    }
}
