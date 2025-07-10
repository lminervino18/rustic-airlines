use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr, thread, time::Duration};

/// Example Rust program to interact with a Cassandra server.
/// This program demonstrates:
/// - Creating a keyspace in Cassandra.
/// - Creating a table within that keyspace.
/// - Inserting multiple rows of data into the table.
/// - Querying the table to retrieve data.
///
/// The code uses the `CassandraClient` to establish a connection and execute queries.
///
/// Note: Ensure the server IP and port match your Cassandra setup before running this code.
fn main() {
    // Reemplaza con la dirección IP y puerto correctos del servidor
    let server_ip = "127.0.0.1";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    // Conectarse al servidor Cassandra
    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    let create = vec![ // Crear un keyspace
    "CREATE KEYSPACE people_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 4};".to_string(),

    // Crear una tabla
    "CREATE TABLE  people_data.persons (
        partition_key TEXT,
        clustering_key TEXT,
        name TEXT,
        age INT,
        email TEXT,
        phone TEXT,
        PRIMARY KEY (partition_key, clustering_key, name)
    );".to_string(),];

    let queries = vec![

    // Insertar datos para SEXO = 'Masculino' con clustering_key 'Argentina'
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 30, 'juan.perez@example.com', '+5491123456789');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 35, 'carlos.gomez@example.com', '+5491145678901');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 40, 'luis.martinez@example.com', '+5491167890123');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 29, 'pedro.diaz@example.com', '+5491189012345');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 34, 'miguel.castro@example.com', '+5491111234567');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 33, 'luis.romero@example.com', '+5491122345678');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 27, 'federico.sanchez@example.com', '+5491156789012');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 26, 'ricardo.torres@example.com', '+5491178901234');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 31, 'gustavo.diaz@example.com', '+5491190123456');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Argentina', 'a', 36, 'marcelo.gutierrez@example.com', '+5491987654321');".to_string(),

    // Insertar datos para SEXO = 'Masculino' con clustering_key 'Brasil'
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'João Silva', 31, 'joao.silva@example.com', '+5521987654321');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'Carlos Santos', 37, 'carlos.santos@example.com', '+5521912345678');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'Pedro Almeida', 38, 'pedro.almeida@example.com', '+5521934567890');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'Miguel Rocha', 39, 'miguel.rocha@example.com', '+5521956789012');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'Luis Ribeiro', 33, 'luis.ribeiro@example.com', '+5521978901234');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'Marcelo Carvalho', 40, 'marcelo.carvalho@example.com', '+5521989012345');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'Felipe Lima', 29, 'felipe.lima@example.com', '+5521923456789');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'Ricardo Souza', 35, 'ricardo.souza@example.com', '+5521945678901');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'André Nunes', 34, 'andre.nunes@example.com', '+5521967890123');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Brasil', 'Fernando Santos', 30, 'fernando.santos@example.com', '+5521998765432');".to_string(),

    // Insertar datos para SEXO = 'Masculino' con clustering_key 'Chile'
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Juan García', 28, 'juan.garcia@example.com', '+56212345678');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Carlos Vega', 32, 'carlos.vega@example.com', '+56234567890');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Luis Morales', 34, 'luis.morales@example.com', '+56256789012');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Miguel Herrera', 27, 'miguel.herrera@example.com', '+56278901234');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Pedro Araya', 33, 'pedro.araya@example.com', '+56289012345');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Federico Navarro', 29, 'federico.navarro@example.com', '+56267890123');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Ricardo Ortiz', 31, 'ricardo.ortiz@example.com', '+56223456789');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Gustavo Fuentes', 36, 'gustavo.fuentes@example.com', '+56212345678');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Fernando Navarro', 30, 'fernando.navarro@example.com', '+56234567890');".to_string(),
    "INSERT INTO people_data.persons (partition_key, clustering_key, name, age, email, phone) VALUES ('Masculino', 'Chile', 'Marcelo Gutiérrez', 35, 'marcelo.gutierrez@example.com', '+56245678901');".to_string(),

    // Consulta los datos
    "SELECT * FROM people_data.persons WHERE partition_key = 'Masculino';".to_string(),
];

    // Ejecutar cada consulta en un loop
    let mut contador = 0;
    let len = queries.len() + create.len();

    for (_, query) in create.iter().enumerate() {
        match client.execute(&query, "all") {
            Ok(query_result) => {
                match query_result {
                    driver::QueryResult::Result(result) => {
                        contador += 1;
                        println!(
                            "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                            query, result
                        );
                    }
                    driver::QueryResult::Error(error) => {
                        println!("La query: {:?} fallo con el error {:?}", query, error);
                    }
                }
                println!("exitosas {:?}/{:?}", contador, len)
            }
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }

    thread::sleep(Duration::from_secs(2));
    let len = queries.len();
    for (_, query) in queries.iter().enumerate() {
        // if i == 2 {
        //     thread::sleep(Duration::from_secs(2));
        // }
        match client.execute(&query, "all") {
            Ok(query_result) => {
                match query_result {
                    driver::QueryResult::Result(result) => {
                        contador += 1;
                        println!(
                            "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                            query, result
                        );
                    }
                    driver::QueryResult::Error(error) => {
                        println!("La query: {:?} fallo con el error {:?}", query, error);
                    }
                }
                println!("exitosas {:?}/{:?}", contador, len)
            }
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }
}
