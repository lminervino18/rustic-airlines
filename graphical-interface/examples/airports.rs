use driver::CassandraClient;
use std::{net::Ipv4Addr, str::FromStr, thread};

fn main() {
    let server_ip = "127.0.0.2";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    let now = chrono::Utc::now().timestamp();

    let meta_queries = vec![
        "CREATE KEYSPACE sky WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}",
        "CREATE TABLE sky.airports (
            iata TEXT,
            country TEXT,
            name TEXT,
            lat DOUBLE,
            lon DOUBLE,
            PRIMARY KEY (country, iata)
        )",
        "CREATE TABLE sky.flights (
            number TEXT,
            status TEXT,
            lat DOUBLE,
            lon DOUBLE,
            angle FLOAT,
            departure_time TIMESTAMP,
            arrival_time TIMESTAMP,
            airport TEXT,
            direction TEXT,
            PRIMARY KEY (airport, direction, departure_time, arrival_time, number)
        )",

        "CREATE TABLE sky.flight_info (
            number TEXT,
            fuel DOUBLE,
            height INT,
            speed INT,
            origin TEXT,
            destination TEXT,
            PRIMARY KEY (number)
        )",
    ];

    let queries = vec![
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('AEP', 'ARG', 'Aeroparque Jorge Newbery', -34.5592, -58.4156)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('EZE', 'ARG', 'Ministro Pistarini', -34.8222, -58.5358)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('COR', 'ARG', 'Ingeniero Ambrosio Taravella', -31.3236, -64.2080)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('MDZ', 'ARG', 'El Plumerillo', -32.8328, -68.7928)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('ROS', 'ARG', 'Islas Malvinas', -32.9036, -60.7850)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('SLA', 'ARG', 'Martín Miguel de Güemes', -24.8425, -65.4861)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('IGR', 'ARG', 'Cataratas del Iguazú', -25.7373, -54.4734)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('BRC', 'ARG', 'Teniente Luis Candelaria', -41.9629, -71.5332)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('USH', 'ARG', 'Malvinas Argentinas', -54.8433, -68.2958)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('TUC', 'ARG', 'Teniente General Benjamín Matienzo', -26.8409, -65.1048)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('AFA', 'ARG', 'Suboficial Ayudante Santiago Germano', -34.5883, -68.4039)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('CRD', 'ARG', 'General Enrique Mosconi', -45.7853, -67.4655)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('CNQ', 'ARG', 'Doctor Fernando Piragine Niveyro', -27.4455, -58.7619)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('EHL', 'ARG', 'Aeropuerto El Bolsón', -41.9432, -71.5327)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('EPA', 'ARG', 'El Palomar', -34.6099, -58.6126)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('EQS', 'ARG', 'Brigadier General Antonio Parodi', -42.9080, -71.1395)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('FMA', 'ARG', 'Formosa', -26.2127, -58.2281)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('GGS', 'ARG', 'Gobernador Gregores', -48.7831, -70.1500)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('GPO', 'ARG', 'General Pico', -35.6962, -63.7580)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('JUJ', 'ARG', 'Horacio Guzmán', -24.3928, -65.0978)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('LGS', 'ARG', 'Comodoro D. Ricardo Salomón', -35.4936, -69.5747)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('LAP', 'ARG', 'Comodoro Arturo Merino Benítez', -32.85, -68.86)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('PMQ', 'ARG', 'Perito Moreno', -46.5361, -70.9787)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('PRQ', 'ARG', 'Presidente Roque Sáenz Peña', -26.7564, -60.4922)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('REL', 'ARG', 'Almirante Zar', -43.2105, -65.2703)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('RCQ', 'ARG', 'General Justo José de Urquiza', -31.7948, -60.4804)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('RGL', 'ARG', 'Piloto Civil Norberto Fernández', -51.6089, -69.3126)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('RSA', 'ARG', 'Santa Rosa', -36.5883, -64.2757)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('VDM', 'ARG', 'Gobernador Castello', -40.8692, -63.0004)",
        "INSERT INTO sky.airports (iata, country, name, lat, lon) VALUES ('BHI', 'ARG', 'Comandante Espora', -38.7242, -62.1693)",
    ];

    let flight_queries = vec![
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR101', 'on time', -34.5592, -58.4156, 125.3, '{}', '{}', 'AEP', 'departure')", now, now + 3600),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR101', 'on time', -34.5592, -58.4156, 125.3, '{}', '{}', 'BRC', 'arrival')", now, now + 3600),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR101', 92.0, 11000, 540, 'AEP', 'BRC')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR001', 'on time', -34.8222, -58.5358, 239.5, '{}', '{}', 'AEP', 'departure')", now + 120, now + 4000),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR001', 'on time', -34.8222, -58.5358, 239.5, '{}', '{}', 'EZE', 'arrival')", now + 120, now + 4000),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR001',  95.0, 10000, 550, 'AEP', 'EZE')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR102', 'delayed', -31.3236, -64.2080, 178.6, '{}', '{}', 'COR', 'departure')", now + 240, now + 4200),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR102', 'delayed', -31.3236, -64.2080, 178.6, '{}', '{}', 'USH', 'arrival')", now + 240, now + 4200),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR102', 88.5, 12000, 530, 'COR', 'USH')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR103', 'on time', -32.8328, -68.7928, 245.7, '{}', '{}', 'MDZ', 'departure')", now, now + 4000),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR103', 'on time', -32.8328, -68.7928, 245.7, '{}', '{}', 'SLA', 'arrival')", now, now + 4000),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR103', 85.0, 11500, 545, 'MDZ', 'SLA')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR104', 'cancelled', -25.7373, -54.4734, 90.2, '{}', '{}', 'IGR', 'departure')", now, now + 4000),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR104', 'cancelled', -25.7373, -54.4734, 90.2, '{}', '{}', 'EZE', 'arrival')", now, now + 4000),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR104', 94.0, 10500, 535, 'IGR', 'EZE')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR105', 'on time', -32.9036, -60.7850, 156.8, '{}', '{}', 'ROS', 'departure')", now, now + 4000),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR105', 'on time', -32.9036, -60.7850, 156.8, '{}', '{}', 'TUC', 'arrival')", now, now + 4000),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR105', 87.5, 11000, 525, 'ROS', 'TUC')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR106', 'delayed', -38.7242, -62.1693, 212.4, '{}', '{}', 'BHI', 'departure')", now, now + 4000),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR106', 'delayed', -38.7242, -62.1693, 212.4, '{}', '{}', 'MDZ', 'arrival')", now, now + 4000),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR106', 89.0, 10000, 520, 'BHI', 'MDZ')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR107', 'on time', -24.3928, -65.0978, 278.9, '{}', '{}', 'JUJ', 'departure')", now, now + 4000),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR107', 'on time', -24.3928, -65.0978, 278.9, '{}', '{}', 'ROS', 'arrival')", now, now + 4000),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR107', 91.5, 11500, 540, 'JUJ', 'ROS')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR108', 'on time', -51.6089, -69.3126, 34.6, '{}', '{}', 'RGL', 'departure')", now, now + 4000),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR108', 'on time', -51.6089, -69.3126, 34.6, '{}', '{}', 'AEP', 'arrival')", now, now + 4000),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR108', 93.0, 12000, 550, 'RGL', 'AEP')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR109', 'delayed', -27.4455, -58.7619, 145.7, '{}', '{}', 'CNQ', 'departure')", now, now + 4000),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR109', 'delayed', -27.4455, -58.7619, 145.7, '{}', '{}', 'COR', 'arrival')", now, now + 4000),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR109', 86.0, 10500, 530, 'CNQ', 'COR')"),

        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR110', 'on time', -36.5883, -64.2757, 198.3, '{}', '{}', 'RSA', 'departure')", now, now + 4000),
        format!("INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('AR110', 'on time', -36.5883, -64.2757, 198.3, '{}', '{}', 'BRC', 'arrival')", now, now + 4000),
        format!("INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('AR110', 90.5, 11000, 535, 'RSA', 'BRC')"), 

        format!("SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = 'AEP' AND direction = 'departure' AND departure_time > 1733011200"),
        //format!("UPDATE sky.flights SET status = 'on time' WHERE airport = 'EZE' AND direction = 'arrival' AND departure_time = 1733017729 AND arrival_time = 1733021609 AND number = AR001"),
        //format!("SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = 'AEP' AND direction = 'departure' AND departure_time > 1733011200"),
    ];

    let mut contador = 0;
    let len = queries.len() + meta_queries.len() + flight_queries.len();

    for query in meta_queries {
        match client.execute(&query, "all") {
            Ok(query_result) => {
                match query_result {
                    driver::QueryResult::Result(_) => {
                        contador += 1;
                        println!(
                            "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                            query, query_result
                        );
                    }
                    driver::QueryResult::Error(error) => {
                        println!("Error en la consulta: {:?}", error);
                    }
                }
                println!("exitosas {:?}/{:?}", contador, len)
            }
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }

    thread::sleep(std::time::Duration::from_secs(3));

    for query in queries {
        match client.execute(&query, "all") {
            Ok(query_result) => {
                match query_result {
                    driver::QueryResult::Result(_) => {
                        contador += 1;
                        println!(
                            "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                            query, query_result
                        );
                    }
                    driver::QueryResult::Error(error) => {
                        println!("Error en la consulta: {:?}", error);
                    }
                }
                println!("exitosas {:?}/{:?}", contador, len)
            }
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }

    thread::sleep(std::time::Duration::from_secs(3));

    for query in flight_queries {
        match client.execute(&query, "all") {
            Ok(query_result) => {
                match query_result {
                    driver::QueryResult::Result(_) => {
                        contador += 1;
                        println!(
                            "Consulta ejecutada exitosamente: {} y el resultado fue {:?}",
                            query, query_result
                        );
                    }
                    driver::QueryResult::Error(error) => {
                        println!("Error en la consulta: {:?}", error);
                    }
                }
                println!("exitosas {:?}/{:?}", contador, len)
            }
            Err(e) => eprintln!("Error al ejecutar la consulta: {}\nError: {:?}", query, e),
        }
    }
}
