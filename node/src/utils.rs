use gossip::structures::application_state::{KeyspaceSchema, TableSchema};
use query_creator::errors::CQLError;
use query_creator::{GetTableName, GetUsedKeyspace, Query};

use crate::errors::NodeError;
use crate::internode_protocol::message::InternodeMessage;
use crate::internode_protocol::InternodeSerializable;
use crate::Node;
use std::collections::HashMap;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Attempts to connect to a peer and send a message over the `TcpStream`.
///
/// # Purpose
/// This function manages communication with a peer node in a distributed system.
/// It reuses existing connections when available, attempts to reconnect if a connection is broken,
/// and ensures thread-safe access to the shared connections map while sending the message.
///
/// # Parameters
/// - `peer_id: Ipv4Addr`
///   - The IPv4 address of the peer to connect to.
/// - `port: u16`
///   - The port number on which the peer is listening for incoming connections.
/// - `connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>`
///   - A thread-safe map of active TCP connections to other nodes.
///     - Keys are peer addresses (in `String` format).
///     - Values are `Arc<Mutex<TcpStream>>`, allowing thread-safe access and sharing of streams.
/// - `message: InternodeMessage`
///   - The message to send to the peer, serialized using the `InternodeSerializable` trait.
///
/// # Returns
/// - `Result<(), NodeError>`:
///   - Returns `Ok(())` on successful connection and message transmission.
///   - Returns `Err(NodeError)` if an error occurs during connection or message handling.
///
/// # Behavior
/// 1. **Existing Connection Handling**:
///    - Checks if a connection to the peer already exists in the `connections` map.
///    - If an existing connection is found:
///      - Acquires a lock on the `TcpStream` and attempts to send the message.
///      - Ensures the stream is flushed after writing.
///      - Returns `Err(NodeError::IoError)` if any errors occur during this process.
/// 2. **New Connection Handling**:
///    - If no existing connection is found, attempts to establish a new `TcpStream` connection to the peer.
///    - Adds the new connection to the `connections` map for future reuse.
///    - Sends the message through the newly established connection and ensures the stream is flushed.
/// 3. **Thread Safety**:
///    - Uses `Mutex` locks to ensure safe access to the shared `connections` map and individual streams.
///
/// # Errors
/// - Returns `NodeError::LockError` if the `Mutex` lock on the `connections` map or a `TcpStream` fails.
/// - Returns `NodeError::IoError` for I/O errors during connection, writing, or flushing operations.
///
/// # Notes
/// - **Efficient Reuse**:
///   - This function optimizes network usage by reusing existing connections where possible.
/// - **Logging**:
///   - Logs errors to `stderr` for debugging purposes but does not expose them in the return type.
/// - **Thread-Safe Design**:
///   - The function ensures thread safety for shared resources, making it suitable for concurrent environments.
///
/// # Importance
/// This function is critical for maintaining efficient and reliable communication between nodes in a distributed system.
/// By managing connections dynamically and reusing streams, it minimizes overhead and improves resilience to network issues.

pub fn connect_and_send_message(
    peer_id: Ipv4Addr,
    port: u16,
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
    message: InternodeMessage,
) -> Result<(), NodeError> {
    let peer_socket = SocketAddrV4::new(peer_id, port);
    let peer_addr = peer_socket.to_string();

    // Intentar reutilizar una conexión existente
    if let Some(existing_stream) = {
        let connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
        connections_guard.get(&peer_addr).cloned()
    } {
        let mut stream_guard = existing_stream.lock().map_err(|_| NodeError::LockError)?;
        if stream_guard.write_all(&message.as_bytes()).is_err() {
            return Err(NodeError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Error al escribir en el stream",
            )));
        }
        if stream_guard.flush().is_err() {
            return Err(NodeError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Error al hacer flush en el stream",
            )));
        }
        return Ok(());
    }

    // Si no hay conexión, intentar conectar una vez
    let stream = TcpStream::connect((peer_id, port))
        .map_err(|e| {
            eprintln!("Error al intentar conectar con {:?}: {:?}", peer_addr, e);
            NodeError::IoError(e)
        })
        .map_err(|e| e)?;

    let stream = Arc::new(Mutex::new(stream));

    // Añadir la nueva conexión al HashMap
    {
        let mut connections_guard = connections.lock().map_err(|_| NodeError::LockError)?;
        connections_guard.insert(peer_addr.clone(), Arc::clone(&stream));
    }

    // Intentar enviar el mensaje a través de la nueva conexión
    {
        let mut stream_guard = stream.lock().map_err(|_| NodeError::LockError)?;
        stream_guard.write_all(&message.as_bytes()).map_err(|e| {
            eprintln!("Error al escribir en el stream: {:?}", e);
            NodeError::IoError(e)
        })?;
        stream_guard.flush().map_err(|e| {
            eprintln!("Error al hacer flush en el stream: {:?}", e);
            NodeError::IoError(e)
        })?;
    }
    Ok(())
}

/// Checks if a keyspace exists for the given query and client ID.
///
/// This function attempts to retrieve a keyspace associated with a query.
/// If the query specifies a keyspace name, it retrieves it directly; otherwise,
/// it uses the client ID to fetch the associated keyspace. The function retries
/// a specified number of times before returning an error if the keyspace is not found.
///
/// # Arguments
///
/// * `node` - A shared reference to the node, protected by a mutex.
/// * `query` - The query containing the keyspace or table information.
/// * `client_id` - The ID of the client for keyspace lookup.
/// * `max_retries` - Maximum number of retries to check for the keyspace.
///
/// # Returns
///
/// * `Ok(Some(KeyspaceSchema))` if the keyspace is found.
/// * `Ok(None)` if the keyspace is not found (though it shouldn't occur with retries).
/// * `Err(NodeError)` if the keyspace cannot be retrieved after retries.
///
/// # Errors
///
/// Returns an error of type `NodeError::CQLError` with `InvalidSyntax` if the keyspace is not found
/// after the maximum number of retries.
///
pub fn check_keyspace(
    node: &Arc<Mutex<Node>>,
    query: &Query,
    client_id: i32,
    max_retries: usize,
) -> Result<Option<KeyspaceSchema>, NodeError> {
    let mut attempts = 0;

    while attempts < max_retries {
        if attempts != 0 {
            thread::sleep(Duration::from_millis(3000));
        }

        // Bloquear el nodo temporalmente para obtener el keyspace
        let keyspace = {
            let guard_node = node.lock()?;

            if let Some(keyspace_name) = query.get_used_keyspace() {
                guard_node.get_keyspace(&keyspace_name)?
            } else {
                guard_node.get_client_keyspace(client_id)?
            }
        };

        // Si se encuentra el keyspace, retornar
        if keyspace.is_some() {
            return Ok(keyspace);
        }

        // Incrementar el contador de intentos
        attempts += 1;
    }

    // Si no se encuentra el keyspace después de los intentos, retornar error
    Err(NodeError::CQLError(CQLError::InvalidSyntax))
}

/// Checks if a table exists in the keyspace for the given query and client ID.
///
/// This function attempts to retrieve a table associated with a query. It first ensures
/// that a valid keyspace is available. If the keyspace exists, it then attempts to find
/// the table specified in the query. The function retries a specified number of times
/// before returning an error if the table is not found.
///
/// # Arguments
///
/// * `node` - A shared reference to the node, protected by a mutex.
/// * `query` - The query containing the keyspace and table information.
/// * `client_id` - The ID of the client for keyspace lookup.
/// * `max_retries` - Maximum number of retries to check for the table.
///
/// # Returns
///
/// * `Ok(Some(TableSchema))` if the table is found.
/// * `Ok(None)` if the table is not found (though it shouldn't occur with retries).
/// * `Err(NodeError)` if the table cannot be retrieved after retries.
///
/// # Errors
///
/// Returns an error of type `NodeError::CQLError` with `InvalidSyntax` if the table is not found
/// after the maximum number of retries, or if no keyspace is available.
///
pub fn check_table(
    node: &Arc<Mutex<Node>>,
    query: &Query,
    client_id: i32,
    max_retries: usize,
) -> Result<Option<TableSchema>, NodeError> {
    let mut attempts = 0;

    while attempts < max_retries {
        if attempts != 0 {
            thread::sleep(Duration::from_millis(3000));
        }

        // Variables locales para almacenar resultados
        let (_, table): (Option<KeyspaceSchema>, Option<TableSchema>) = {
            // Bloquear el nodo temporalmente
            let guard_node = node.lock()?;

            // Intentar obtener el keyspace
            let keyspace = if let Some(keyspace_name) = query.get_used_keyspace() {
                guard_node.get_keyspace(&keyspace_name)?
            } else {
                guard_node.get_client_keyspace(client_id)?
            };

            // Si no se encuentra el keyspace, retornar un error
            if keyspace.is_none() {
                return Err(NodeError::CQLError(CQLError::InvalidSyntax)); // Keyspace no encontrado
            }

            // Si se obtiene el keyspace, intentar obtener la tabla
            let table = if let Some(ref k) = keyspace {
                if let Some(table_name) = query.get_table_name() {
                    guard_node.get_table(table_name, k.clone()).ok()
                } else {
                    None
                }
            } else {
                None
            };

            // Devolver keyspace y tabla para su uso posterior
            (keyspace, table)
        };

        // Si se encuentra la tabla, retornar
        if table.is_some() {
            return Ok(table);
        }

        // Incrementar el contador de intentos
        attempts += 1;
    }

    // Si no se encuentra la tabla después de los intentos, retornar error
    Err(NodeError::CQLError(CQLError::InvalidSyntax)) // Tabla no encontrada
}
