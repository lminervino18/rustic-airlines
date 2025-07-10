// Ordered imports
use super::QueryExecution;
use crate::NodeError;
use query_creator::clauses::set_cql::Set;
use query_creator::clauses::types::column::Column;
use query_creator::clauses::update_cql::Update;
use query_creator::errors::CQLError;

impl QueryExecution {
    /// Executes the update of row (or insert if not exist). This function is public only for internal use
    /// within the library (defined as `pub(crate)`).
    pub(crate) fn execute_update(
        &mut self,
        update_query: Update,
        internode: bool,
        mut replication: bool,
        open_query_id: i32,
        client_id: i32,
        timestamp: i64,
    ) -> Result<(), NodeError> {
        let table;
        let mut do_in_this_node = true;
        let client_keyspace;
        let mut failed_nodes = 0;
        let mut internode_failed_nodes = 0;
        {
            // Get the table name and reference the node
            let table_name = update_query.table_name.clone();
            let mut node = self
                .node_that_execute
                .lock()
                .map_err(|_| NodeError::LockError)?;

            client_keyspace = node
                .get_open_handle_query()
                .get_keyspace_of_query(open_query_id)?
                .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;

            // Get the table and replication factor
            table = node.get_table(table_name.clone(), client_keyspace.clone())?;

            // Validate primary key and where clause
            let partition_keys = table.get_partition_keys()?;
            let clustering_columns = table.get_clustering_columns()?;

            let where_clause = update_query
                .clone()
                .where_clause
                .ok_or(NodeError::CQLError(CQLError::NoWhereCondition))?;

            where_clause.validate_cql_conditions(
                &partition_keys,
                &clustering_columns,
                false,
                true,
            )?;

            // Validate `IF` clause conditions, if any
            if let Some(if_clause) = update_query.clone().if_clause {
                if_clause.validate_cql_conditions(&partition_keys, &clustering_columns)?;
            }

            // Get the value to hash and determine the node responsible for handling the update
            let value_to_hash = where_clause
                .get_value_partitioner_key_condition(partition_keys)?
                .join("");

            let node_to_update = node.partitioner.get_ip(value_to_hash.clone())?;
            let self_ip = node.get_ip().clone();
            let logger = node.get_logger();
            // If not an internode operation and the target node differs, forward the update
            if !internode && node_to_update != self_ip {
                let serialized_update = update_query.serialize();
                failed_nodes = self.send_to_single_node(
                    node.get_ip(),
                    node_to_update,
                    &serialized_update,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                    timestamp,
                    logger.clone(),
                )?;
                do_in_this_node = false;
            }

            // Send update to replication nodes if needed
            if !internode {
                let serialized_update = update_query.serialize();
                (internode_failed_nodes, replication) = self.send_to_replication_nodes(
                    node,
                    node_to_update,
                    &serialized_update,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                    timestamp,
                    logger.clone(),
                )?;
            }

            // Set execution finished if this node is the primary and no replication is needed
            if !internode && node_to_update == self_ip {
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

        // Validate the update types
        Self::validate_update_types(update_query.clone().set_clause, table.get_columns())?;

        self.storage_engine.update(
            update_query,
            table,
            replication,
            &client_keyspace.get_name(),
            timestamp,
        )?;
        Ok(())
    }

    /// Validates the types of the `SET` clause against the columns of the table
    pub(crate) fn validate_update_types(
        set_clause: Set,
        columns: Vec<Column>,
    ) -> Result<(), NodeError> {
        for (column_name, value) in set_clause.get_pairs() {
            for column in &columns {
                if *column_name == column.name {
                    if column.is_partition_key || column.is_clustering_column {
                        return Err(NodeError::CQLError(CQLError::InvalidCondition));
                    }
                    if !column.data_type.is_valid_value(value) {
                        return Err(NodeError::CQLError(CQLError::InvalidCondition));
                    }
                }
            }
        }
        Ok(())
    }
}
