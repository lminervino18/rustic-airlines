// Ordered imports
use crate::NodeError;
use query_creator::clauses::keyspace::alter_keyspace_cql::AlterKeyspace;

use super::QueryExecution;

/// Executes the alteration of a keyspace. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_alter_keyspace(
        &mut self,
        _alter_keyspace: AlterKeyspace,
    ) -> Result<(), NodeError> {
        todo!()
        // // Look for the keyspace in the list of keyspaces
        // let mut node = self.node_that_execute.lock()?;

        // let mut keyspace = node
        //     .keyspaces
        //     .iter()
        //     .find(|k| k.get_name() == alter_keyspace.get_name())
        //     .ok_or(NodeError::KeyspaceError)?
        //     .clone();

        // // Validate if the replication class and factor are the same to avoid unnecessary operations
        // if keyspace.get_replication_class() == alter_keyspace.get_replication_class()
        //     && keyspace.get_replication_factor() == alter_keyspace.get_replication_factor()
        // {
        //     return Ok(()); // No changes, nothing to execute
        // }

        // // Update the replication class and factor in the keyspace
        // keyspace.update_replication_class(alter_keyspace.get_replication_class());
        // keyspace.update_replication_factor(alter_keyspace.get_replication_factor());
        // node.update_keyspace(client_id, keyspace);

        // Ok(())
    }
}
