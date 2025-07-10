// Ordered imports
use crate::NodeError;
use gossip::structures::application_state::TableSchema;
use query_creator::clauses::table::create_table_cql::CreateTable;
use query_creator::errors::CQLError;

use super::QueryExecution;

/// Executes the creation of a table. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_create_table(
        &mut self,
        create_table: CreateTable,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        // Add the table to the node
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        let client_keyspace = node
            .get_open_handle_query()
            .get_keyspace_of_query(open_query_id)?
            .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;

        if let Err(e) = node.add_table(create_table.clone(), &client_keyspace.get_name()) {
            if !create_table.get_if_not_exists_clause() {
                return Err(e);
            }
        }

        node.get_open_handle_query().update_table_in_keyspace(
            &client_keyspace.get_name(),
            TableSchema::new(create_table.clone()),
        )?;

        self.execution_finished_itself = true;

        Ok(())
    }
}
