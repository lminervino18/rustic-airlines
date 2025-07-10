// Ordered imports
use crate::NodeError;
use query_creator::clauses::keyspace::drop_keyspace_cql::DropKeyspace;

use super::QueryExecution;

/// Executes the deletion of a keyspace. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_drop_keyspace(
        &mut self,
        drop_keyspace: DropKeyspace,
    ) -> Result<(), NodeError> {
        // Get the name of the keyspace to delete
        let keyspace_name = drop_keyspace.get_name().clone();

        // Lock the node and remove the keyspace from the internal structure
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        node.remove_keyspace(keyspace_name.clone())?;

        self.execution_finished_itself = true;
        Ok(())
    }
}
