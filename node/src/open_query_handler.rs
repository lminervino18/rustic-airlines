use crate::errors::NodeError;
use crate::internode_protocol::response::InternodeResponse;
use gossip::structures::application_state::{KeyspaceSchema, TableSchema};
use native_protocol::frame::Frame;
use query_creator::Query;
use std::collections::HashMap;
use std::fmt;
use std::net::Ipv4Addr;
use std::sync::mpsc::Sender;

#[derive(Debug, PartialEq)]

/// Represents the consistency levels available for queries in a distributed database.
///
/// # Purpose
/// Consistency levels define the guarantees about how many replicas must acknowledge a read or write operation
/// for it to be considered successful. They provide a balance between data consistency, availability, and performance
/// in a distributed database system.
///
/// # Variants
/// - `Any`
///   - The write operation is considered successful once it is accepted by at least one node, including a hinted handoff node.
///   - Offers the weakest consistency but maximizes availability.
///   - Typically used for writes where availability is prioritized over strict consistency.
/// - `One`
///   - The operation is considered successful if at least one replica responds.
///   - Provides minimal consistency while ensuring low latency.
/// - `Two`
///   - The operation is considered successful if at least two replicas respond.
///   - Offers a stronger consistency guarantee than `One` but requires more replicas to participate.
/// - `Three`
///   - The operation is considered successful if at least three replicas respond.
///   - Further increases consistency compared to `Two`, at the cost of higher latency and reduced availability.
/// - `Quorum`
///   - The operation is considered successful if a majority (quorum) of replicas respond.
///   - Balances consistency and availability, commonly used for both reads and writes in distributed databases.
///   - Ensures that a read after a write will see the most recent value as long as the write was acknowledged by a quorum.
/// - `All`
///   - The operation is considered successful only if all replicas respond.
///   - Provides the highest level of consistency but sacrifices availability and increases latency.
///   - Typically used when strict consistency is critical.
///
/// # Usage
/// - The choice of consistency level depends on the application's requirements for consistency, availability, and latency.
/// - Lower consistency levels (`Any`, `One`) prioritize availability and performance.
/// - Higher consistency levels (`Quorum`, `All`) prioritize strict consistency but may reduce availability in case of node failures.

pub enum ConsistencyLevel {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
}

impl ConsistencyLevel {
    /// Creates a `ConsistencyLevel` instance from a string representation.
    ///
    /// # Arguments
    /// - `s: &str`
    ///   - The string representation of the consistency level.
    ///     Valid values are `"any"`, `"one"`, `"two"`, `"three"`, `"quorum"`, and `"all"`.
    ///
    /// # Returns
    /// - A `ConsistencyLevel` corresponding to the input string.
    /// - Defaults to `ConsistencyLevel::All` if the input string does not match any known level.
    ///
    /// # Behavior
    /// - The function is case-insensitive, handling both uppercase and lowercase inputs.
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "any" => ConsistencyLevel::Any,
            "one" => ConsistencyLevel::One,
            "two" => ConsistencyLevel::Two,
            "three" => ConsistencyLevel::Three,
            "quorum" => ConsistencyLevel::Quorum,
            "all" => ConsistencyLevel::All,
            _ => ConsistencyLevel::All,
        }
    }

    /// Checks if a query is ready based on the number of responses received and the required responses.
    ///
    /// # Arguments
    /// - `responses_received: usize`
    ///   - The number of successful responses received so far.
    /// - `responses_needed: usize`
    ///   - The total number of responses required to satisfy the consistency level.
    ///
    /// # Returns
    /// - `true` if the consistency level requirements are met based on the responses received.
    /// - `false` otherwise.
    ///
    /// # Behavior
    /// - The required number of responses varies depending on the `ConsistencyLevel`:
    ///   - `Any`, `One`: Requires at least one response.
    ///   - `Two`, `Three`: Requires two and three responses, respectively.
    ///   - `Quorum`: Requires more than half of the required responses.
    ///   - `All`: Requires all responses.
    pub fn is_query_ready(&self, responses_received: usize, responses_needed: usize) -> bool {
        match self {
            ConsistencyLevel::Any => responses_received >= 1,
            ConsistencyLevel::One => responses_received >= 1,
            ConsistencyLevel::Two => responses_received >= 2,
            ConsistencyLevel::Three => responses_received >= 3,
            ConsistencyLevel::Quorum => responses_received >= (responses_needed / 2 + 1),
            ConsistencyLevel::All => responses_received >= responses_needed,
        }
    }

    /// Calculates the number of OK responses required to satisfy the consistency level.
    ///
    /// # Arguments
    /// - `responses_needed: usize`
    ///   - The total number of responses required to satisfy the consistency level.
    ///
    /// # Returns
    /// - The minimum number of OK responses needed to satisfy the `ConsistencyLevel`.
    ///
    /// # Behavior
    /// - The required number of responses varies depending on the `ConsistencyLevel`:
    ///   - `Any`, `One`: Requires one response.
    ///   - `Two`, `Three`: Requires two and three responses, respectively.
    ///   - `Quorum`: Requires more than half of the required responses.
    ///   - `All`: Requires all responses.
    pub fn required_oks(&self, responses_needed: usize) -> usize {
        match self {
            ConsistencyLevel::Any => 1,
            ConsistencyLevel::One => 1,
            ConsistencyLevel::Two => 2,
            ConsistencyLevel::Three => 3,
            ConsistencyLevel::Quorum => responses_needed / 2 + 1,
            ConsistencyLevel::All => responses_needed,
        }
    }
}

/// Represents an open query being processed in the distributed database system.
///
/// # Purpose
/// The `OpenQuery` structure is used to track the state of a query as it is processed.
/// It manages responses, tracks the required consistency level, and accumulates results from different nodes
/// until the query is complete.
///
/// # Fields
/// - `needed_responses: i32`
///   - The total number of responses required to satisfy the consistency level of the query.
///   - This value is determined based on the consistency level and the replication factor of the keyspace.
/// - `ok_responses: i32`
///   - The number of successful responses (`OK`) received so far.
///   - Incremented each time a node responds successfully.
/// - `error_responses: i32`
///   - The number of error responses received so far.
///   - Incremented each time a node responds with an error.
/// - `acumulated_ok_responses: Vec<(Ipv4Addr, InternodeResponse)>`
///   - A vector containing successful responses from nodes.
///   - Each entry includes:
///     - The IP address of the responding node.
///     - The corresponding `InternodeResponse` containing query results or metadata.
/// - `connection: TcpStream`
///   - The TCP connection to the client that issued the query.
///   - Used to send the final result or error back to the client once the query is complete.
/// - `query: Query`
///   - The query object representing the operation being executed (e.g., SELECT, INSERT, UPDATE, DELETE).
/// - `consistency_level: ConsistencyLevel`
///   - The consistency level required for the query (e.g., `ONE`, `QUORUM`, `ALL`).
///   - Determines the number of responses needed for the query to be considered successful.
/// - `table: Option<TableSchema>`
///   - An optional schema of the table associated with the query.
///   - Used to validate and process the query's structure and data.
///
/// # Usage
/// - `OpenQuery` is created when a new query is initiated by a client.
/// - It tracks the state of the query as responses are received from nodes in the cluster.
/// - Once all required responses are collected (or an error threshold is reached), the query is completed,
///   and the result is sent back to the client.
///
/// # Notes
/// - **Consistency Levels**:
///   - The `needed_responses` field is derived from the `consistency_level` and the cluster configuration.
/// - **State Management**:
///   - The `ok_responses` and `error_responses` fields are incremented dynamically as nodes respond.
/// - **Accumulated Responses**:
///   - The `acumulated_ok_responses` vector collects responses for operations like read repair and result aggregation.
///
/// # Example
/// An example use case is tracking a query requiring `QUORUM` consistency in a cluster:
/// - `needed_responses` is calculated as a majority of the cluster nodes.
/// - As nodes respond successfully, their results are added to `acumulated_ok_responses`.
/// - If the number of `ok_responses` meets or exceeds `needed_responses`, the query is considered successful.
/// - If the number of `error_responses` exceeds a threshold, the query fails.

#[derive(Debug)]
pub struct OpenQuery {
    needed_responses: i32,
    ok_responses: i32,
    error_responses: i32,
    acumulated_ok_responses: Vec<(Ipv4Addr, InternodeResponse)>,
    tx_reply: Sender<Frame>,
    query: Query,
    consistency_level: ConsistencyLevel,
    table: Option<TableSchema>,
}

impl OpenQuery {
    fn new(
        needed_responses: i32,
        tx_reply: Sender<Frame>,
        query: Query,
        consistencty: &str,
        table: Option<TableSchema>,
    ) -> Self {
        Self {
            needed_responses,
            ok_responses: 0,
            error_responses: 0,
            acumulated_ok_responses: vec![],
            tx_reply,
            query,
            consistency_level: ConsistencyLevel::from_str(consistencty),
            table,
        }
    }

    // Adds a response to the query and increments the count of actual responses.
    //
    // # Parameters
    // - `response`: The response to be added.
    fn add_ok_response(&mut self, response: InternodeResponse, from: Ipv4Addr) {
        self.acumulated_ok_responses.push((from, response));
        self.ok_responses += 1;
    }

    // Adds a response to the query and increments the count of actual responses.
    //
    // # Parameters
    // - `response`: The response to be added.
    fn add_error_response(&mut self) {
        self.error_responses += 1;
    }

    // Checks if the query has received all needed responses.
    //
    // # Returns
    /// `true` if the query is closed (i.e., all responses have been received), `false` otherwise.
    fn is_close(&self) -> bool {
        self.consistency_level
            .is_query_ready(self.ok_responses as usize, self.needed_responses as usize)
            || !self.can_still_achieve_required_ok(
                self.needed_responses,
                self.error_responses,
                self.consistency_level
                    .required_oks(self.needed_responses as usize) as i32,
            )
    }

    fn can_still_achieve_required_ok(
        &self,
        total_responses: i32,
        error_responses: i32,
        required_ok: i32,
    ) -> bool {
        total_responses - error_responses >= required_ok
        //total rta - errores - ok >= oks necesarios - oks
    }

    /// Gets the TCP connection associated with this query.
    ///
    /// # Returns
    /// A reference to the `TcpStream` used by this query.
    /// Returns a reference to the TCP connection associated with the query.
    ///
    /// # Purpose
    /// Provides access to the client's TCP connection, allowing the caller to send results or errors back to the client.
    ///
    /// # Returns
    /// - `&TcpStream`: A reference to the TCP connection used by the client that initiated the query.
    ///
    /// # Notes
    /// - This method returns a reference, so the caller must not drop or close the connection.
    pub fn get_connection(&self) -> Sender<Frame> {
        self.tx_reply.clone()
    }

    /// Returns a clone of the query associated with the `OpenQuery`.
    ///
    /// # Purpose
    /// Allows access to the query object being processed.
    ///
    /// # Returns
    /// - `Query`: A clone of the query object.
    ///
    /// # Notes
    /// - The query is cloned to ensure that the original query in `OpenQuery` remains unaltered.
    /// - Useful for inspecting or re-executing the query.
    pub fn get_query(&self) -> Query {
        self.query.clone()
    }

    /// Returns a clone of the table schema associated with the query, if available.
    ///
    /// # Purpose
    /// Provides access to the schema of the table being queried or modified.
    ///
    /// # Returns
    /// - `Option<TableSchema>`:
    ///   - `Some(TableSchema)` if a table schema is associated with the query.
    ///   - `None` if the query does not involve a specific table.
    ///
    /// # Notes
    /// - The schema is cloned to avoid unintended modifications to the original.
    pub fn get_table(&self) -> Option<TableSchema> {
        self.table.clone()
    }

    /// Returns a clone of the accumulated successful responses from nodes.
    ///
    /// # Purpose
    /// Provides access to the list of responses received from nodes that successfully processed the query.
    ///
    /// # Returns
    /// - `Vec<(Ipv4Addr, InternodeResponse)>`:
    ///   - A vector of tuples where each tuple contains:
    ///     - `Ipv4Addr`: The IP address of the responding node.
    ///     - `InternodeResponse`: The response received from the node.
    ///
    /// # Notes
    /// - The vector is cloned to ensure the original data remains intact.
    /// - Useful for tasks like read repair or aggregating results for the client.
    pub fn get_acumulated_responses(&self) -> Vec<(Ipv4Addr, InternodeResponse)> {
        self.acumulated_ok_responses.clone()
    }
}

/// Implements `fmt::Display` for `OpenQuery` to provide human-readable formatting for query status.
impl fmt::Display for OpenQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ID: con {:?} (Ok responses) y {:?} (Error responses) of {:?} needed responses",
            self.ok_responses, self.error_responses, self.needed_responses
        )
    }
}

/// Manages multiple `OpenQuery` instances, each identified by a unique ID.
///
/// # Purpose
/// The `OpenQueryHandler` is responsible for managing the lifecycle of open queries in a distributed database system.
/// It keeps track of active queries, their associated keyspaces, and assigns unique identifiers to new queries.
///
/// # Fields
/// - `queries: HashMap<i32, OpenQuery>`
///   - A map of query IDs to their corresponding `OpenQuery` instances.
///   - Each entry represents an active query being tracked by the handler.
/// - `keyspaces_queries: HashMap<i32, Option<KeyspaceSchema>>`
///   - A map of query IDs to their associated keyspace schemas.
///   - Allows quick lookup of the keyspace context for a given query.
///   - The value is `None` if the query is not tied to a specific keyspace.
/// - `next_id: i32`
///   - A counter for generating unique IDs for new queries.
///   - Increments with each new query added to ensure unique identification.
///
/// # Usage
/// - The `OpenQueryHandler` is used to add, retrieve, and manage queries during their execution lifecycle.
/// - Provides functionalities to track the progress of queries, manage responses, and handle consistency checks.
///
/// # Notes
/// - **Thread Safety**:
///   - While this structure is not inherently thread-safe, it is typically used in conjunction with synchronization
///     mechanisms (e.g., `Arc<Mutex<OpenQueryHandler>>`) in multi-threaded environments.
/// - **Keyspace Context**:
///   - The `keyspaces_queries` map helps associate queries with their respective keyspaces, which is crucial
///     for enforcing schema constraints and managing data distribution.
///
/// # Example
/// A typical use case involves:
/// 1. Adding a new query with a unique ID.
/// 2. Tracking the progress of the query through its responses.
/// 3. Removing the query once it is complete.
///
/// # Importance
/// This structure is integral to managing the state of distributed queries, ensuring that responses from
/// nodes are appropriately tracked and queries are executed within the correct schema context.

pub struct OpenQueryHandler {
    queries: HashMap<i32, OpenQuery>,
    keyspaces_queries: HashMap<i32, Option<KeyspaceSchema>>,
    next_id: i32,
}

impl OpenQueryHandler {
    /// Creates a new instance of `OpenQueryHandler`.
    ///
    /// # Purpose
    /// This method initializes a new `OpenQueryHandler` with empty query maps and a starting ID for queries.
    /// It sets up the internal state required to manage and track multiple open queries in a distributed system.
    ///
    /// # Returns
    /// - A new `OpenQueryHandler` instance with:
    ///   - An empty `queries` map to store active queries.
    ///   - An empty `keyspaces_queries` map to associate queries with keyspace schemas.
    ///   - The `next_id` field initialized to `1`, ensuring unique identification for newly added queries.
    ///
    /// # Behavior
    /// - The `queries` and `keyspaces_queries` fields are initialized as empty hash maps.
    /// - The `next_id` field is set to `1`, indicating that the first query added will have an ID of `1`.
    ///
    /// # Notes
    /// - This method is typically called when setting up a node or a component that needs to manage multiple open queries.
    /// - The returned instance can be used to add and manage queries immediately.
    ///
    pub fn new() -> Self {
        Self {
            queries: HashMap::new(),
            keyspaces_queries: HashMap::new(),
            next_id: 1,
        }
    }

    /// Creates and registers a new open query with a unique ID.
    ///
    /// # Purpose
    /// This method initializes a new `OpenQuery`, assigns it a unique identifier, and stores it in the handler.
    /// It tracks the query's progress and associates it with the provided connection, consistency level,
    /// and optional table and keyspace schemas.
    ///
    /// # Arguments
    /// - `needed_responses: i32`
    ///   - The number of responses required to satisfy the query's consistency level.
    /// - `connection: TcpStream`
    ///   - The TCP connection to the client that initiated the query.
    ///   - Used to send responses or errors back to the client.
    /// - `query: Query`
    ///   - The query to be executed (e.g., SELECT, INSERT, UPDATE, DELETE).
    /// - `consistency_level: &str`
    ///   - The desired consistency level for the query (e.g., `ONE`, `QUORUM`, `ALL`).
    /// - `table: Option<TableSchema>`
    ///   - An optional table schema associated with the query.
    ///   - Used for operations involving specific tables.
    /// - `keyspace: Option<KeyspaceSchema>`
    ///   - An optional keyspace schema associated with the query.
    ///   - Used to validate the query's context within the keyspace.
    ///
    /// # Returns
    /// - `i32`: The unique ID assigned to the new query.
    ///
    /// # Behavior
    /// 1. **ID Generation**:
    ///    - Assigns the current value of `self.next_id` as the ID for the new query.
    ///    - Increments `self.next_id` to ensure the next query receives a unique ID.
    /// 2. **Query Initialization**:
    ///    - Creates a new `OpenQuery` using the provided arguments.
    ///    - Populates the query with details like the number of needed responses, client connection, query, and schema.
    /// 3. **Query Registration**:
    ///    - Adds the new `OpenQuery` to the `queries` map, associating it with the generated ID.
    ///    - If a keyspace is provided, associates it with the query in the `keyspaces_queries` map.
    ///
    /// # Notes
    /// - **Thread Safety**:
    ///   - This method is not inherently thread-safe. Synchronization mechanisms (e.g., `Mutex`) must be used
    ///     if the `OpenQueryHandler` is shared across threads.
    /// - **Keyspace and Table Context**:
    ///   - If `keyspace` or `table` is `None`, the query is considered independent of a specific schema context.
    ///
    /// # Errors
    /// - The method does not directly return errors but relies on the validity of the provided arguments.
    /// - Invalid arguments may lead to runtime issues during query execution.
    ///
    /// # Importance
    /// This method is crucial for initiating and managing distributed queries. It ensures that each query
    /// is uniquely identified and associated with the required metadata, allowing the system to track its
    /// progress and enforce consistency guarantees.
    pub fn new_open_query(
        &mut self,
        needed_responses: i32,
        tx_reply: Sender<Frame>,
        query: Query,
        consistency_level: &str,
        table: Option<TableSchema>,
        keyspace: Option<KeyspaceSchema>,
    ) -> i32 {
        let new_id = self.next_id;
        self.next_id += 1;
        let query = OpenQuery::new(needed_responses, tx_reply, query, consistency_level, table);
        self.queries.insert(new_id, query);
        self.keyspaces_queries.insert(new_id, keyspace);
        new_id
    }

    /// Retrieves a mutable reference to an `OpenQuery` identified by its unique ID.
    ///
    /// # Purpose
    /// This method allows access to an `OpenQuery` for modification. It is useful for updating
    /// the state of a query as responses are received or other actions are required during its execution.
    ///
    /// # Arguments
    /// - `id: &i32`
    ///   - A reference to the unique ID of the `OpenQuery` to retrieve.
    ///
    /// # Returns
    /// - `Option<&mut OpenQuery>`:
    ///   - Returns `Some(&mut OpenQuery)` if a query with the given ID exists.
    ///   - Returns `None` if no query with the given ID is found.
    ///
    /// # Behavior
    /// - Searches the `queries` map for the specified `id`.
    /// - If the query exists, returns a mutable reference to it.
    /// - If the query does not exist, returns `None`.
    ///
    /// # Notes
    /// - **Thread Safety**:
    ///   - This method is not inherently thread-safe. When used in a multi-threaded context,
    ///     ensure the `OpenQueryHandler` is protected by synchronization primitives like `Mutex`.
    /// - **Use Cases**:
    ///   - Modifying the state of an open query, such as incrementing response counts or adding accumulated responses.
    ///
    /// # Errors
    /// - The method itself does not produce errors but returns `None` if the query ID is invalid or not found.

    pub fn get_query_mut(&mut self, id: &i32) -> Option<&mut OpenQuery> {
        self.queries.get_mut(id)
    }

    /// Retrieves the keyspace schema associated with a specific query ID.
    ///
    /// # Purpose
    /// This method allows access to the keyspace schema linked to a given open query, if available.
    /// It is useful for ensuring that operations are executed within the correct keyspace context.
    ///
    /// # Arguments
    /// - `open_query_id: i32`
    ///   - The unique ID of the open query whose keyspace schema is to be retrieved.
    ///
    /// # Returns
    /// - `Result<Option<KeyspaceSchema>, NodeError>`:
    ///   - Returns `Ok(Some(KeyspaceSchema))` if the keyspace schema is found.
    ///   - Returns `Ok(None)` if the query ID exists but is not associated with a specific keyspace.
    ///   - Returns `Err(NodeError::InternodeProtocolError)` if the query ID is not found in the `keyspaces_queries` map.
    ///
    /// # Behavior
    /// - Searches the `keyspaces_queries` map for the specified query ID.
    /// - If the query ID exists, returns the associated `KeyspaceSchema` or `None`.
    /// - If the query ID does not exist, returns an error of type `NodeError::InternodeProtocolError`.
    ///
    /// # Notes
    /// - **Thread Safety**:
    ///   - This method is not inherently thread-safe. When used in a multi-threaded context,
    ///     ensure the `OpenQueryHandler` is protected by synchronization primitives like `Mutex`.
    /// - **Use Cases**:
    ///   - Retrieving the keyspace context for schema validation or query execution.
    ///
    /// # Errors
    /// - Returns `NodeError::InternodeProtocolError` if the query ID is not found in the `keyspaces_queries` map.

    pub fn get_keyspace_of_query(
        &self,
        open_query_id: i32,
    ) -> Result<Option<KeyspaceSchema>, NodeError> {
        self.keyspaces_queries
            .get(&open_query_id)
            .ok_or(NodeError::InternodeProtocolError)
            .cloned()
    }

    /// Updates a table schema in a specific keyspace, or adds it if it doesn't exist.
    ///
    /// # Purpose
    /// This method modifies the schema of a table within a keyspace. If the table exists, it is updated;
    /// otherwise, it is added to the keyspace. This ensures that keyspace definitions remain accurate and up-to-date.
    ///
    /// # Arguments
    /// - `keyspace_name: &str`
    ///   - The name of the keyspace where the table schema needs to be updated or added.
    /// - `new_table: TableSchema`
    ///   - The new or updated schema of the table to be applied in the keyspace.
    ///
    /// # Returns
    /// - `Result<(), NodeError>`:
    ///   - Returns `Ok(())` if the table is successfully updated or added.
    ///   - Returns an appropriate `NodeError` if an issue arises during the operation.
    ///
    /// # Behavior
    /// 1. **Keyspace Lookup**:
    ///    - Iterates through the `keyspaces_queries` map to find the keyspace matching `keyspace_name`.
    /// 2. **Table Update or Addition**:
    ///    - Checks if the table exists in the keyspace:
    ///      - If it exists, updates its schema in place.
    ///      - If it does not exist, adds the new table schema to the keyspace.
    /// 3. **Operation Completion**:
    ///    - Ensures that the keyspace schema is modified correctly and returns success.
    ///
    /// # Notes
    /// - **Thread Safety**:
    ///   - This method is not inherently thread-safe. Ensure synchronization using `Mutex` when accessed concurrently.
    /// - **Use Cases**:
    ///   - Updating table definitions after schema changes.
    ///   - Adding new tables to an existing keyspace dynamically.
    ///
    /// # Errors
    /// - The method currently does not return explicit errors but assumes the keyspace and table operations are valid.
    /// - Ensure that the provided keyspace name and table schema are consistent with the system's state to avoid runtime issues.

    pub fn update_table_in_keyspace(
        &mut self,
        keyspace_name: &str,
        new_table: TableSchema,
    ) -> Result<(), NodeError> {
        for (_, keyspace) in &mut self.keyspaces_queries {
            if let Some(key) = keyspace {
                if key.get_name() == keyspace_name {
                    let mut find = false;
                    for (i, table) in key.get_tables().iter_mut().enumerate() {
                        if table.get_name() == new_table.clone().get_name() {
                            key.tables[i] = new_table.clone();
                            find = true;
                        }
                    }
                    if !find {
                        key.add_table(new_table.clone())?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Sets or updates the keyspace schema associated with a specific open query.
    ///
    /// # Purpose
    /// This method associates a `KeyspaceSchema` with an open query identified by its unique ID.
    /// It is useful for ensuring that the query operates within the correct keyspace context.
    ///
    /// # Arguments
    /// - `open_query_id: i32`
    ///   - The unique ID of the open query for which the keyspace is to be set or updated.
    /// - `keyspace: KeyspaceSchema`
    ///   - The keyspace schema to associate with the query.
    ///
    /// # Behavior
    /// - Inserts the provided `KeyspaceSchema` into the `keyspaces_queries` map for the given query ID.
    /// - If an entry for the query ID already exists, it is overwritten with the new keyspace schema.
    ///
    /// # Notes
    /// - **Thread Safety**:
    ///   - This method is not inherently thread-safe. Use synchronization mechanisms like `Mutex` when accessed concurrently.
    /// - **Use Cases**:
    ///   - Associating a keyspace context with a query to ensure correct schema validation.
    ///   - Dynamically updating the keyspace of a query during execution.
    ///
    /// # Example
    /// - A query requiring a specific keyspace is assigned its schema using this method before execution begins.
    ///
    /// # Errors
    /// - This method does not directly return errors. However, ensure that the provided keyspace schema is valid
    ///   and consistent with the system's state to avoid downstream issues.

    pub fn set_keyspace_of_query(&mut self, open_query_id: i32, keyspace: KeyspaceSchema) {
        self.keyspaces_queries.insert(open_query_id, Some(keyspace));
    }

    /// Adds a successful response to the `OpenQuery` with the specified ID and checks if it is closed.
    ///
    /// # Purpose
    /// This method handles the addition of a successful (`OK`) response to an `OpenQuery`, identified by its unique ID.
    /// It checks if the query has gathered enough responses to meet its consistency requirements and returns
    /// the query if it is closed.
    ///
    /// # Parameters
    /// - `open_query_id: i32`
    ///   - The unique ID of the `OpenQuery` to which the response is to be added.
    /// - `response: InternodeResponse`
    ///   - The response object containing the result or status of the query from a node.
    /// - `from: Ipv4Addr`
    ///   - The IP address of the node that sent the response.
    ///
    /// # Returns
    /// - `Option<OpenQuery>`:
    ///   - Returns `Some(OpenQuery)` if the query is closed after adding the response.
    ///   - Returns `None` if the query remains open.
    ///
    /// # Behavior
    /// 1. **Query Retrieval**:
    ///    - Looks up the `OpenQuery` using the `open_query_id`.
    ///    - If the query does not exist, returns `None`.
    /// 2. **Response Addition**:
    ///    - Invokes `add_ok_response` on the query to update its state with the new successful response.
    /// 3. **Closure Check**:
    ///    - Calls `is_close` to determine if the query has received enough successful responses to meet its consistency level.
    ///    - If closed, removes the query from the handler and returns it.
    ///    - If not closed, the query remains in the handler, and `None` is returned.
    ///
    /// # Notes
    /// - **Thread Safety**:
    ///   - Ensure this method is used with synchronization mechanisms (e.g., `Mutex`) in multi-threaded environments.
    /// - **Consistency Requirements**:
    ///   - The query's closure depends on its `ConsistencyLevel` and the number of successful responses received.
    ///
    /// # Logging
    /// - Logs a message indicating the query's status when it is closed, showing the number of successful responses (`OKs`) received.
    ///
    /// # Use Cases
    /// - Collecting responses for distributed queries and determining when they are complete.
    /// - Triggering post-query processing (e.g., sending results to clients) once the query is closed.
    ///
    /// # Errors
    /// - This method does not directly return errors. However, ensure the query ID and response are valid to avoid inconsistencies.

    pub fn add_ok_response_and_get_if_closed(
        &mut self,
        open_query_id: i32,
        response: InternodeResponse,
        from: Ipv4Addr,
    ) -> Option<OpenQuery> {
        match self.get_query_mut(&open_query_id) {
            Some(query) => {
                query.add_ok_response(response, from);
                if query.is_close() {
                    // println!(
                    //     "con {:?} / {:?} OKS la query se cerro",
                    //     query.ok_responses, query.needed_responses
                    // );

                    self.queries.remove(&open_query_id)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Adds an error response to the `OpenQuery` with the specified ID and checks if it is closed.
    ///
    /// # Purpose
    /// This method updates an `OpenQuery` with an error response and determines if the query is considered closed
    /// based on the accumulated responses (both successful and error responses) relative to its consistency requirements.
    ///
    /// # Parameters
    /// - `open_query_id: i32`
    ///   - The unique ID of the `OpenQuery` to which the error response is to be added.
    ///
    /// # Returns
    /// - `Option<OpenQuery>`:
    ///   - Returns `Some(OpenQuery)` if the query is closed after adding the error response.
    ///   - Returns `None` if the query remains open.
    ///
    /// # Behavior
    /// 1. **Query Retrieval**:
    ///    - Attempts to retrieve the `OpenQuery` associated with the provided `open_query_id`.
    ///    - If the query does not exist, returns `None`.
    /// 2. **Error Response Addition**:
    ///    - Calls `add_error_response` on the retrieved query to increment its error response count.
    /// 3. **Closure Check**:
    ///    - Evaluates whether the query has gathered enough responses (successful or errors) to meet its closure condition.
    ///    - If the query is closed, it is removed from the `queries` map and returned.
    ///    - If the query remains open, it is left in the handler, and `None` is returned.
    ///
    /// # Logging
    /// - Logs a message when the query is closed, showing the number of successful (`OK`) responses compared to the required responses.
    ///
    /// # Notes
    /// - **Thread Safety**:
    ///   - This method is not inherently thread-safe. Use synchronization mechanisms like `Mutex` in multi-threaded environments.
    /// - **Error Handling**:
    ///   - This method does not propagate errors directly. Ensure the query ID is valid to avoid inconsistencies.
    /// - **Use Cases**:
    ///   - Managing queries that receive partial or unsuccessful responses from nodes.
    ///   - Triggering cleanup or error propagation once the query is closed due to error thresholds.
    ///
    /// # Example Workflow
    /// - A query accumulates error responses from nodes.
    /// - When the accumulated responses meet the query's closure condition, it is removed and returned for further handling.

    pub fn add_error_response_and_get_if_closed(
        &mut self,
        open_query_id: i32,
    ) -> Option<OpenQuery> {
        match self.get_query_mut(&open_query_id) {
            Some(query) => {
                query.add_error_response();

                if query.is_close() {
                    // println!(
                    //     "con {:?} / {:?} ERRORES la query se cerro",
                    //     query.ok_responses, query.needed_responses
                    // );
                    self.queries.remove(&open_query_id)
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

impl fmt::Display for OpenQueryHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Open Queries:\n")?;
        for (id, query) in &self.queries {
            writeln!(
                f,
                "Query ID {}: {} OKs, {} Errors, {} Needed",
                id, query.ok_responses, query.error_responses, query.needed_responses
            )?;
        }
        Ok(())
    }
}
