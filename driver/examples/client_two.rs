use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr, thread, time::Duration};

/// Example Rust program to interact with a Cassandra server.
/// This program demonstrates inserting a large number of rows into a Cassandra table.
fn main() {
    // Reemplaza con la dirección IP y puerto correctos del servidor
    let server_ip = "127.0.0.1";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    // Crear un keyspace y tabla si no existen
    let setup_queries = vec![
        "CREATE KEYSPACE bulk_insert WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};".to_string(),
        "CREATE TABLE bulk_insert.data (
            id UUID PRIMARY KEY,
            name TEXT,
            value INT
        );".to_string(),
    ];

    for query in setup_queries {
        client.execute(&query, "quorum").unwrap();
    }

    thread::sleep(Duration::from_secs(2));
    // Realizar 10,000 inserciones
    let start = std::time::Instant::now();
    for i in 0..10_000 {
        let insert_query = format!(
            "INSERT INTO bulk_insert.data (id, name, value) VALUES (uuid(), 'name_{}', {});",
            i, i
        );

        //thread::sleep(Duration::from_secs(1));
        match client.execute(&insert_query, "quorum") {
            Ok(_) => {
                println!("Se ineserto la query {:?}", i);
            }
            Err(e) => eprintln!("Error al insertar la fila {}: {:?}", i, e),
        }

        // Simular una pausa opcional si se desea controlar la velocidad de inserción
        // thread::sleep(Duration::from_millis(1));
    }

    let duration = start.elapsed();
    println!(
        "Finalizadas 10,000 inserciones en {:?} segundos.",
        duration.as_secs_f64()
    );
}
