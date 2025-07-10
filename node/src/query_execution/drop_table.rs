// Ordered imports
use super::QueryExecution;
use crate::NodeError;
use query_creator::clauses::table::drop_table_cql::DropTable;

/// Executes the deletion of a table. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_drop_table(
        &mut self,
        drop_table: DropTable,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        // Get the name of the table to delete
        let table_name = drop_table.get_table_name();

        // Lock the node and remove the table from the internal list
        node.remove_table(table_name.clone(), open_query_id)?;

        self.execution_finished_itself = true;

        Ok(())
    }
}
