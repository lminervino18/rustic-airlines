// Ordered imports
use super::QueryExecution;
use crate::CQLError;
use crate::NodeError;
use query_creator::clauses::delete_cql::Delete;

/// Executes the delete of row/rows. This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_delete(
        &mut self,
        delete_query: Delete,
        internode: bool,
        mut replication: bool,
        open_query_id: i32,
        client_id: i32,
        timestamp: i64,
    ) -> Result<(), NodeError> {
        let table;
        let mut do_in_this_node = true;
        let mut failed_nodes = 0;
        let mut internode_failed_nodes = 0;

        let client_keyspace;
        {
            // Get the table name and reference the node
            let table_name = delete_query.table_name.clone();
            let mut node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;

            client_keyspace = node
                .get_open_handle_query()
                .get_keyspace_of_query(open_query_id)?
                .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;
            // Retrieve the table and replication factor
            table = node.get_table(table_name.clone(), client_keyspace.clone())?;

            // Validate the primary and clustering keys
            let partition_keys = table.get_partition_keys().unwrap();
            let clustering_columns = table.get_clustering_columns().unwrap();

            // Check if columns in DELETE conflict with primary or clustering keys
            if let Some(columns) = delete_query.columns.clone() {
                for column in columns {
                    if partition_keys.contains(&column) || clustering_columns.contains(&column) {
                        return Err(NodeError::CQLError(CQLError::InvalidColumn));
                    }
                }
            }
            // Validate WHERE clause
            let where_clause = delete_query
                .clone()
                .where_clause
                .ok_or(NodeError::CQLError(CQLError::NoWhereCondition))?;

            where_clause.validate_cql_conditions(
                &partition_keys,
                &clustering_columns,
                true,
                false,
            )?;

            // Determine the node responsible for deletion based on hashed partition key values
            let value_to_hash = where_clause
                .get_value_partitioner_key_condition(partition_keys)?
                .join("");
            let node_to_delete = node.partitioner.get_ip(value_to_hash.clone())?;
            let self_ip = node.get_ip().clone();
            let logger = node.get_logger();
            // Forward the DELETE operation if the responsible node is different and not an internode operation
            if !internode && node_to_delete != self_ip {
                let serialized_delete = delete_query.serialize();
                failed_nodes = self.send_to_single_node(
                    node.get_ip(),
                    node_to_delete,
                    &serialized_delete,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                    timestamp,
                    logger.clone(),
                )?;
                do_in_this_node = false;
            }

            // Send DELETE to replication nodes if required
            if !internode {
                let serialized_delete = delete_query.serialize();
                (internode_failed_nodes, replication) = self.send_to_replication_nodes(
                    node,
                    node_to_delete,
                    &serialized_delete,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                    timestamp,
                    logger,
                )?;
            }

            // Set execution_finished_itself if this node is the primary and replication is not needed
            if !internode && node_to_delete == self_ip {
                self.execution_finished_itself = true;
            }
        }

        failed_nodes += internode_failed_nodes;
        self.how_many_nodes_failed = failed_nodes;

        // Early return if no local execution or replication is needed
        if !do_in_this_node && !replication {
            return Ok(());
        }

        // Set the replication flag if this node should replicate the operation
        if replication {
            self.execution_replicate_itself = true;
        }

        self.storage_engine.delete(
            delete_query,
            table,
            &client_keyspace.get_name(),
            replication,
            timestamp,
        )?;
        Ok(())
    }
}
