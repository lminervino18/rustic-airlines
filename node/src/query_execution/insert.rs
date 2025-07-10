// Ordered imports
// use crate::table::Table;
use crate::NodeError;
use gossip::structures::application_state::TableSchema;
use query_creator::clauses::insert_cql::Insert;
use query_creator::clauses::types::column::Column;
use query_creator::errors::CQLError;
use uuid;

use super::QueryExecution;

/// Executes the insert of a row (o update if exist). This function is public only for internal use
/// within the library (defined as `pub(crate)`).
impl QueryExecution {
    pub(crate) fn execute_insert(
        &mut self,
        insert_query: Insert,
        table_to_insert: TableSchema,
        internode: bool,
        mut replication: bool,
        open_query_id: i32,
        client_id: i32,
        timestap: i64,
    ) -> Result<(), NodeError> {
        let mut failed_nodes = 0;
        let mut internode_failed_nodes = 0;
        let mut node = self.node_that_execute.lock()?;

        let mut do_in_this_node = true;

        let client_keyspace = node
            .get_open_handle_query()
            .get_keyspace_of_query(open_query_id)?
            .ok_or(NodeError::CQLError(CQLError::NoActualKeyspaceError))?;

        if !node.table_already_exist(table_to_insert.get_name(), client_keyspace.get_name())? {
            return Err(NodeError::CQLError(CQLError::TableAlreadyExist));
        }

        // Retrieve columns and the partition keys
        let columns = table_to_insert.get_columns();

        let mut keys_index: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if column.is_partition_key {
                    Some(index)
                } else {
                    None
                }
            })
            .collect();

        let clustering_columns_index: Vec<usize> = columns
            .clone()
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if column.is_clustering_column {
                    Some(index)
                } else {
                    None
                }
            })
            .collect();

        // Check if there's at least one partition key
        if keys_index.is_empty() {
            return Err(NodeError::CQLError(CQLError::Error));
        }

        // Clone values from the insert query
        let mut values = insert_query.values.clone();

        // Concatenate the partition key column values to generate the hash
        let value_to_hash = keys_index
            .iter()
            .map(|&index| values[index].clone())
            .collect::<Vec<String>>()
            .join("");

        // Validate and complete row values
        values = self.complete_row(
            columns.clone(),
            insert_query.clone().into_clause.columns,
            values,
        )?;

        let mut new_insert = insert_query.clone();
        let new_values: Vec<String> = values.iter().filter(|v| !v.is_empty()).cloned().collect();
        new_insert.values = new_values;
        self.validate_values(columns.clone(), &values)?;

        // Deterclient_keyspacemine the node responsible for the insert
        let node_to_insert = node.get_partitioner().get_ip(value_to_hash.clone())?;
        let self_ip = node.get_ip().clone();
        let keyspace_name = client_keyspace.get_name();
        let logger = node.get_logger();
        // If not internode and the target IP differs, forward the insert
        if !internode {
            if node_to_insert != self_ip {
                let serialized_insert = new_insert.serialize();
                failed_nodes = self.send_to_single_node(
                    node.get_ip(),
                    node_to_insert,
                    &serialized_insert,
                    open_query_id,
                    client_id,
                    &client_keyspace.get_name(),
                    timestap,
                    logger.clone(),
                )?;
                do_in_this_node = false; // The actual insert will be done by another node
            } else {
                self.execution_finished_itself = true; // Insert will be done by this node
            }

            // Send the insert to replication nodes
            let serialized_insert = new_insert.serialize();
            (internode_failed_nodes, replication) = self.send_to_replication_nodes(
                node,
                node_to_insert,
                &serialized_insert,
                open_query_id,
                client_id,
                &client_keyspace.get_name(),
                timestap,
                logger,
            )?;
            if replication {
                self.execution_replicate_itself = true; // This node will replicate the insert
            }
        }

        failed_nodes += internode_failed_nodes;
        self.how_many_nodes_failed = failed_nodes;

        // If the node itself is the target and no further replication is required, finish here
        if !do_in_this_node && !replication {
            return Ok(());
        }

        // If this node is responsible for the insert, execute it here
        keys_index.extend(&clustering_columns_index);

        self.storage_engine.insert(
            &keyspace_name,
            &insert_query.into_clause.table_name,
            values.iter().map(|s| s.as_str()).collect(),
            columns,
            table_to_insert.get_clustering_column_in_order(),
            replication,
            insert_query.if_not_exists,
            timestap,
        )?;
        Ok(())
    }

    fn complete_row(
        &self,
        columns: Vec<Column>,
        specified_columns: Vec<String>,
        values: Vec<String>,
    ) -> Result<Vec<String>, NodeError> {
        let mut complete_row = vec!["".to_string(); columns.len()];
        let mut specified_keys = 0;

        for (i, column) in columns.iter().enumerate() {
            if let Some(pos) = specified_columns.iter().position(|c| c == &column.name) {
                // Generar UUID si el valor especificado es "uuid()"
                let value = if values[pos] == "uuid()" {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    values[pos].clone()
                };

                complete_row[i] = value.clone();

                // Incrementar contador de claves especificadas si es clave de partición o clustering
                if column.is_partition_key || column.is_clustering_column {
                    specified_keys += 1;
                }
            }
        }

        // Verificar que se hayan especificado todas las claves de partición y clustering
        let total_keys = columns
            .iter()
            .filter(|c| c.is_partition_key || c.is_clustering_column)
            .count();

        if specified_keys != total_keys {
            return Err(NodeError::CQLError(
                CQLError::MissingPartitionOrClusteringColumns,
            ));
        }

        Ok(complete_row)
    }
}
