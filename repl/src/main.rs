use std::{
    io::{self, Write},
    net::Ipv4Addr,
    str::FromStr,
};

use driver::CassandraClient;

const IP: &str = "127.0.0.2";

fn main() {
    let mut client = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();

    if client.startup().is_err() {
        eprintln!("Failed to connect to the node at {}", IP);
        return;
    }

    loop {
        print!("> "); // Prompt symbol
        io::stdout().flush().unwrap(); // Ensure the prompt is displayed immediately

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap(); // Read user input

        let trimmed = input.trim(); // Remove trailing newline and whitespace

        match client.execute(&trimmed, "all") {
            Ok(result) => println!("{:?}", result),
            Err(error) => eprintln!("{:?}", error),
        }
    }
}
