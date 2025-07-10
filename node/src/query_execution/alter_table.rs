// Ordered imports
use crate::NodeError;
use query_creator::clauses::table::alter_table_cql::AlterTable;
use query_creator::clauses::types::alter_table_op::AlterTableOperation;
use query_creator::errors::CQLError;

use super::QueryExecution;

/// Executes the alteration of a table. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_alter_table(
        &mut self,
        alter_table: AlterTable,
        open_query_id: i32,
    ) -> Result<(), NodeError> {
        let mut node = self
            .node_that_execute
            .lock()
            .map_err(|_| NodeError::LockError)?;

        let client_keyspace = node
            .get_open_handle_query()
            .get_keyspace_of_query(open_query_id)?
            .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;

        // Get the table name and lock access to it
        let table_name = alter_table.get_table_name();

        let mut table = node
            .get_table(table_name.clone(), client_keyspace.clone())?
            .inner;

        // Apply each alteration operation
        for operation in alter_table.get_operations() {
            match operation {
                AlterTableOperation::AddColumn(column) => {
                    table.add_column(column.clone())?;
                    self.storage_engine.add_column_to_table(
                        &client_keyspace.get_name(),
                        &table_name,
                        &column.name,
                    )?;
                }
                AlterTableOperation::DropColumn(column_name) => {
                    table.remove_column(&column_name)?;
                    self.storage_engine.remove_column_from_table(
                        &client_keyspace.get_name(),
                        &table_name,
                        &column_name,
                    )?;
                }
                AlterTableOperation::ModifyColumn(_column_name, _new_data_type, _allows_null) => {
                    return Err(NodeError::CQLError(CQLError::InvalidSyntax));
                }
                AlterTableOperation::RenameColumn(old_name, new_name) => {
                    table.rename_column(&old_name, &new_name)?;
                    self.storage_engine.rename_column_from_table(
                        &client_keyspace.get_name(),
                        &table_name,
                        &old_name,
                        &new_name,
                    )?;
                }
            }
        }

        // Save the updated table structure to the node
        node.update_table(&client_keyspace.get_name(), table)?;

        self.execution_finished_itself = true;
        Ok(())
    }
}
