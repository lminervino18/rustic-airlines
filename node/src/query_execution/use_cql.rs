// Ordered imports
use crate::NodeError;
use gossip::structures::application_state::KeyspaceSchema;
use query_creator::clauses::use_cql::Use;

use super::QueryExecution;

impl QueryExecution {
    /// Executee the selection of what keyspace use. This function is public only for internal use
    /// within the library (defined as `pub(crate)`).
    pub(crate) fn _execute_use(
        &self,
        use_keyspace: Use,
        internode: bool,
        open_query_id: i32,
        client_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        // Get the name of the keyspace to use
        let keyspace_name = use_keyspace.get_name();

        // Set the current keyspace in the node
        node._set_actual_keyspace(keyspace_name.clone(), client_id)?;

        let keyspaces = node.schema.keyspaces.clone();

        if let Some(keyspace) = keyspaces.get(&keyspace_name) {
            node.get_open_handle_query().set_keyspace_of_query(
                open_query_id,
                KeyspaceSchema::new(keyspace.inner.clone(), vec![]),
            );
        } else {
            return Err(NodeError::KeyspaceError); // O usa otro error adecuado para este contexto
        }

        // If this is not an internode operation, communicate the change to other nodes
        if !internode {
            // Serialize the `UseKeyspace` into a simple message
            let serialized_use_keyspace = use_keyspace.serialize();
            self._send_to_other_nodes(
                node,
                &serialized_use_keyspace,
                open_query_id,
                client_id,
                &keyspace_name,
                0,
            )?;
        }

        Ok(())
    }
}
