use driver::{CassandraClient, QueryResult};
use native_protocol::messages::error::Error;
use native_protocol::messages::result::result_::Result;
use native_protocol::messages::result::rows::ColumnValue;
use native_protocol::messages::result::schema_change;
use native_protocol::messages::result::schema_change::SchemaChange;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{fs, thread};
use std::{net::Ipv4Addr, str::FromStr};

// Function to launch a node with a given IP
fn launch_node(ip: &str) -> Child {
    Command::new("cargo")
        .arg("run")
        .current_dir("node_launcher") // Switch to the correct node_launcher directory
        .arg("--")
        .arg(ip)
        .spawn()
        .expect("Failed to launch node")
}

// Execute a query and verify the result type
fn execute_and_verify(
    client: &mut CassandraClient,
    query: &str,
    expected_result: QueryResult,
) -> bool {
    match client.execute(query, "quorum") {
        Ok(query_result) => match (&expected_result, &query_result) {
            (
                QueryResult::Result(Result::SchemaChange(_)),
                QueryResult::Result(Result::SchemaChange(_)),
            )
            | (QueryResult::Result(Result::Void), QueryResult::Result(Result::Void))
            | (
                QueryResult::Result(Result::SetKeyspace(_)),
                QueryResult::Result(Result::SetKeyspace(_)),
            )
            | (QueryResult::Error(_), QueryResult::Error(_)) => true,
            _ => false,
        },
        Err(e) => {
            eprintln!("Error executing query: {}\nError: {:?}", query, e);
            false
        }
    }
}

// Function to delete folders created by nodes based on IP
fn delete_node_directories(ip_addresses: Vec<&str>) {
    for ip in ip_addresses {
        let folder_name = format!("node_launcher/keyspaces_{}", ip.replace(".", "_"));
        let folder_path = Path::new(&folder_name);

        if folder_path.exists() {
            fs::remove_dir_all(folder_path).expect("Failed to delete node directory");
            println!("Deleted directory: {}", folder_name);
        } else {
            println!("Directory not found: {}", folder_name);
        }
    }
}

fn execute_and_verify_select(
    client: &mut CassandraClient,
    query: &str,
    expected_values: Vec<String>,
) -> bool {
    match client.execute(query, "all") {
        Ok(query_result) => match query_result {
            QueryResult::Result(Result::Rows(rows)) => {
                if rows.rows_content.is_empty() {
                    return expected_values.is_empty();
                }

                let row = &rows.rows_content[0];
                let actual_values: Vec<String> = row
                    .values()
                    .map(|column_value| match column_value {
                        ColumnValue::Ascii(val) | ColumnValue::Varchar(val) => val.clone(),
                        ColumnValue::Int(val) => val.to_string(),
                        ColumnValue::Double(val) => val.to_string(),
                        ColumnValue::Boolean(val) => val.to_string(),
                        ColumnValue::Timestamp(val) => val.to_string(),
                        _ => "".to_string(), // Devuelve cadena vacía para tipos no esperados
                    })
                    .collect();

                println!(
                    "Expected values: {:?}, Actual values: {:?}",
                    expected_values, actual_values
                );

                expected_values
                    .iter()
                    .all(|value| actual_values.contains(value))
            }
            QueryResult::Error(e) => {
                eprintln!("Error in query result: {:?}", e);
                false
            }
            _ => {
                eprintln!("Unexpected query result type: {:?}", query_result);
                false
            }
        },
        Err(e) => {
            eprintln!("Error executing query: {}\nError: {:?}", query, e);
            false
        }
    }
}

fn setup_keyspace_queries(client: &mut CassandraClient) {
    // Create keyspace with replication_factor = 3
    let query = "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";
    let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
        schema_change::ChangeType::Created,
        schema_change::Target::Keyspace,
        schema_change::Options::new("test_keyspace".to_string(), None),
    )));
    assert!(
        execute_and_verify(client, query, expected_result),
        "Failed keyspace creation: {}",
        query
    );
    println!("Keyspace creation succeeded: {}", query);

    // // Alter keyspace replication factor to 2
    // let query = "ALTER KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}";
    // let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
    //     schema_change::ChangeType::Updated,
    //     schema_change::Target::Keyspace,
    //     schema_change::Options::new("test_keyspace".to_string(), None),
    // )));
    // assert!(
    //     execute_and_verify(client, query, expected_result),
    //     "Failed keyspace alteration: {}",
    //     query
    // );
    // println!("Keyspace alteration succeeded: {}", query);
}

fn setup_table_queries(client: &mut CassandraClient) {
    // Create table "test_table"
    let query = "CREATE TABLE test_keyspace.test_table (id INT, name TEXT, last_name TEXT, PRIMARY KEY (id, name))";
    let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
        schema_change::ChangeType::Created,
        schema_change::Target::Table,
        schema_change::Options::new("test_table".to_string(), None),
    )));
    assert!(
        execute_and_verify(client, query, expected_result),
        "Failed table creation: {}",
        query
    );
    println!("Table creation succeeded: {}", query);

    // // Create table "test_table"
    // let query = "USE test_keyspace";
    // let expected_result = QueryResult::Result(Result::SetKeyspace("".to_string()));
    // assert!(
    //     execute_and_verify(client, query, expected_result),
    //     "set keyspace faildes: {}",
    //     query
    // );
    // println!("Set keyspace succeeded: {}", query);

    // // Alter table "test_table" to add a new column
    // let query = "ALTER TABLE test_table ADD last_name TEXT";
    // let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
    //     schema_change::ChangeType::Updated,
    //     schema_change::Target::Table,
    //     schema_change::Options::new("test_table".to_string(), None),
    // )));
    // assert!(
    //     execute_and_verify(client, query, expected_result),
    //     "Failed table alteration: {}",
    //     query
    // );
    // println!("Table alteration succeeded: {}", query);
}

fn execute_insert_queries(client: &mut CassandraClient) {
    let query =
        "INSERT INTO test_keyspace.test_table (id, name, last_name) VALUES (1, 'Alice', 'David')";
    assert!(
        execute_and_verify(client, query, QueryResult::Result(Result::Void)),
        "Full insert failed"
    );
    println!("Full insert query executed successfully: {}", query);

    // Verificar que el registro fue insertado
    let select_query = "SELECT id, name, last_name FROM test_keyspace.test_table WHERE id = 1";

    let expected_values = vec!["1".to_string(), "Alice".to_string(), "David".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of full insert failed"
    );

    //7. Inserción parcial (solo columna obligatoria `id` y `name`)
    let query = "INSERT INTO test_keyspace.test_table (id, name) VALUES (2, 'Bob')";
    assert!(
        execute_and_verify(client, query, QueryResult::Result(Result::Void)),
        "Partial insert failed"
    );
    println!("Partial insert query executed successfully: {}", query);

    //Verificar que el registro fue insertado con valores nulos en las columnas no especificadas
    let select_query = "SELECT id, name, last_name FROM test_keyspace.test_table WHERE id = 2";

    let expected_values = vec!["2".to_string(), "Bob".to_string(), "".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of partial insert failed"
    );

    // 8. Inserción sin `PRIMARY KEY` (debe fallar)
    let query = "INSERT INTO test_keyspace.test_table (name, last_name) VALUES ('Bob', 'Martinez')";
    assert!(
        execute_and_verify(
            client,
            query,
            QueryResult::Error(Error::ServerError("".to_string()))
        ),
        "Insert without primary key should fail"
    );
    println!(
        "Insert without primary key query executed with expected failure: {}",
        query
    );

    // 9. Inserción con `IF NOT EXISTS` cuando la fila no existe
    let query =
        "INSERT INTO test_keyspace.test_table (id, name, last_name) VALUES (3, 'Charlie', 'Cox') IF NOT EXISTS";
    assert!(
        execute_and_verify(client, query, QueryResult::Result(Result::Void)),
        "Insert with IF NOT EXISTS failed (when row does not exist)"
    );
    println!(
        "Insert with IF NOT EXISTS query executed successfully: {}",
        query
    );

    // Verificar que el registro fue insertado
    let select_query = "SELECT id, name, last_name FROM test_keyspace.test_table WHERE id = 3";

    let expected_values = vec!["3".to_string(), "Charlie".to_string(), "Cox".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of insert with IF NOT EXISTS failed"
    );

    // 10. Inserción con `IF NOT EXISTS` cuando la fila ya existe
    let query =
        "INSERT INTO test_keyspace.test_table (id, name, last_name) VALUES (3, 'Charlie', 'Bet') IF NOT EXISTS";
    assert!(
        execute_and_verify(client, query, QueryResult::Result(Result::Void)),
        "Insert with IF NOT EXISTS should not insert when row exists"
    );
    println!(
        "Insert with IF NOT EXISTS query executed successfully (no insert expected): {}",
        query
    );

    // Verificar que el registro no fue modificado
    let select_query = "SELECT id, name, last_name FROM test_keyspace.test_table WHERE id = 3";

    let expected_values = vec!["3".to_string(), "Charlie".to_string(), "Cox".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of no change with IF NOT EXISTS failed"
    );

    // 10. Inserción con columnas invalidas
    let query = "INSERT INTO test_keyspace.test_table (name, last_name) VALUES ('Charlie', 'charlie@example.com') IF NOT EXISTS";
    assert!(
        execute_and_verify(
            client,
            query,
            QueryResult::Error(Error::ServerError("".to_string()))
        ),
        "Insert with invalid column"
    );
    println!("Insert with invalid column: {}", query);
}

fn execute_update_queries(client: &mut CassandraClient) {
    let update_query =
        "UPDATE test_keyspace.test_table SET last_name = 'Rake' WHERE id = 1 AND name = 'Alice'";
    assert!(
        execute_and_verify(client, update_query, QueryResult::Result(Result::Void)),
        "Update without IF failed"
    );
    println!("Update without IF condition executed successfully");

    // Verificar la actualización
    let select_query =
        "SELECT last_name FROM test_keyspace.test_table WHERE id = 1 AND name = 'Alice'";
    let expected_values = vec!["Rake".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of update without IF failed"
    );

    // 2. Actualización con condición IF que cumple
    let update_query = "UPDATE test_keyspace.test_table SET last_name = 'Chap' WHERE id = 1 AND name = 'Alice' IF last_name = 'Rake'";
    assert!(
        execute_and_verify(client, update_query, QueryResult::Result(Result::Void)),
        "Update with IF condition (matching) failed"
    );
    println!("Update with IF condition (matching) executed successfully");

    // Verificar la actualización
    let select_query = "SELECT last_name FROM test_keyspace.test_table WHERE id = 1";
    let expected_values = vec!["Chap".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of update with matching IF condition failed"
    );

    // 3. Actualización con condición IF que no cumple
    let update_query =
        "UPDATE test_keyspace.test_table SET last_name = 'Sax' WHERE id = 1 IF last_name = 'Tok'";
    assert!(
        !execute_and_verify(client, update_query, QueryResult::Result(Result::Void)),
        "Update with non-matching IF condition should fail"
    );
    println!("Update with non-matching IF condition executed successfully");

    // Verificar que el last_name no haya cambiado
    let select_query =
        "SELECT last_name FROM test_keyspace.test_table WHERE id = 1 AND name = 'Alice'";
    let expected_values = vec!["Chap".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of update with non-matching IF condition failed (last_name changed)"
    );

    let update_query =
        "UPDATE test_keyspace.test_table SET last_name = 'Max' WHERE id = 2 AND name = 'Bob'";
    assert!(
        execute_and_verify(client, update_query, QueryResult::Result(Result::Void)),
        "Multi-condition update without IF failed"
    );
    println!("Multi-condition update without IF executed successfully");

    // Verificar la actualización
    let select_query =
        "SELECT last_name FROM test_keyspace.test_table WHERE id = 2 AND name = 'Bob'";
    let expected_values = vec!["Max".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of multi-condition update failed"
    );

    // 5. Actualización con condición IF y WHERE no cumplida
    let update_query =
        "UPDATE test_keyspace.test_table SET last_name = 'Tel' WHERE id = 2 AND name = 'Bob' IF last_name = 'Prin'";
    assert!(
        execute_and_verify(client, update_query, QueryResult::Result(Result::Void)),
        "Update with non-matching IF and WHERE should do nothing"
    );
    println!("Update with non-matching IF and WHERE condition executed successfully");

    //Verificar que el last_name no haya cambiado
    let select_query =
        "SELECT last_name FROM test_keyspace.test_table WHERE id = 2 AND name = 'Bob'";
    let expected_values = vec!["Max".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of no update with non-matching IF and WHERE failed (last_name changed)"
    );
}

fn execute_delete_queries(client: &mut CassandraClient) {
    // DELETE con WHERE sin IF
    let delete_query = "DELETE FROM test_keyspace.test_table WHERE id = 3 AND name = 'Charlie'";
    assert!(
        execute_and_verify(client, delete_query, QueryResult::Result(Result::Void)),
        "Delete without IF failed"
    );
    println!("Delete without IF executed successfully");

    // Verificar que el registro fue eliminado
    let select_query = "SELECT id FROM test_keyspace.test_table WHERE id = 3 AND name = 'Charlie'";
    let expected_values: Vec<String> = vec![];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of delete without IF failed (row still exists)"
    );

    // DELETE con IF y condición que cumple
    let delete_query =
        "DELETE FROM test_keyspace.test_table WHERE id = 1 AND name = 'Alice' IF last_name = 'Chap'";
    assert!(
        execute_and_verify(client, delete_query, QueryResult::Result(Result::Void)),
        "Delete with matching IF condition failed"
    );
    println!("Delete with matching IF condition executed successfully");

    // Verificar que el registro fue eliminado
    let select_query = "SELECT id FROM test_keyspace.test_table WHERE id = 1 AND name = 'Alice'";
    let expected_values: Vec<String> = vec![];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of delete with matching IF condition failed (row still exists)"
    );

    // DELETE con IF y condición que no cumple
    let delete_query =
        "DELETE FROM test_keyspace.test_table WHERE id = 2 AND name = 'Bob' IF last_name = 'NonExistingLastName'";
    assert!(
        execute_and_verify(client, delete_query, QueryResult::Result(Result::Void)),
        "Delete with non-matching IF condition should fail (row should not be deleted)"
    );
    println!("Delete with non-matching IF condition executed successfully");

    // Verificar que el registro no fue eliminado
    let select_query =
        "SELECT id, name, last_name FROM test_keyspace.test_table WHERE id = 2 AND name = 'Bob'";
    let expected_values = vec!["2".to_string(), "Bob".to_string(), "Max".to_string()];
    assert!(
        execute_and_verify_select(client, select_query, expected_values),
        "Verification of delete with non-matching IF condition failed (row was deleted)"
    );
}

fn teardown_keyspace_queries(client: &mut CassandraClient) {
    // DROP keyspace
    let query = "DROP KEYSPACE test_keyspace";
    let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
        schema_change::ChangeType::Dropped,
        schema_change::Target::Keyspace,
        schema_change::Options::new("test_keyspace".to_string(), None),
    )));
    assert!(
        execute_and_verify(client, query, expected_result),
        "Failed keyspace deletion: {}",
        query
    );
    println!("Keyspace deletion succeeded: {}", query);
}

fn teardown_table_queries(client: &mut CassandraClient) {
    // DROP table
    let query = "DROP TABLE test_keyspace.test_table";
    let expected_result = QueryResult::Result(Result::SchemaChange(SchemaChange::new(
        schema_change::ChangeType::Dropped,
        schema_change::Target::Table,
        schema_change::Options::new("test_table".to_string(), Some("test_keyspace".to_string())),
    )));
    assert!(
        execute_and_verify(client, query, expected_result),
        "Failed table deletion: {}",
        query
    );
    println!("Table deletion succeeded: {}", query);
}

#[test]
fn test_integration_with_multiple_nodes() {
    let timeout_duration = Duration::from_secs(60);
    let start_time = Instant::now();
    let is_completed = Arc::new(Mutex::new(false));
    let is_completed_clone = Arc::clone(&is_completed);

    thread::spawn(move || {
        thread::sleep(timeout_duration);
        let completed = is_completed_clone.lock().unwrap();
        if !*completed {
            panic!("Test exceeded 1-minute timeout");
        }
    });

    let ips = vec![
        "127.0.0.1",
        "127.0.0.2",
        "127.0.0.3",
        "127.0.0.4",
        "127.0.0.5",
    ];
    let mut children = vec![];

    for ip in &ips {
        thread::sleep(Duration::from_secs(2));
        let child = launch_node(ip);
        children.push(child);
        println!("Node with IP {} started", ip);
    }
    thread::sleep(Duration::from_secs(5));

    println!(
        "Current working directory: {:?}",
        std::env::current_dir().unwrap()
    );

    let server_ip = "127.0.0.1";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();
    let mut client = CassandraClient::connect(ip).expect("Failed to connect to Cassandra client");
    client.startup().expect("Failed to start Cassandra client");

    setup_keyspace_queries(&mut client);
    setup_table_queries(&mut client);
    thread::sleep(Duration::from_secs(2));
    execute_insert_queries(&mut client);
    execute_update_queries(&mut client);
    execute_delete_queries(&mut client);
    teardown_table_queries(&mut client);
    teardown_keyspace_queries(&mut client);

    delete_node_directories(ips);

    for mut child in children {
        let _ = child.kill();
        let _ = child.wait();
    }

    *is_completed.lock().unwrap() = true;

    let elapsed = start_time.elapsed();
    println!("Test completed in {:?}", elapsed);
}
