use std::{net::Ipv4Addr, str::FromStr};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use driver::{self, CassandraClient, QueryResult};
use native_protocol::messages::result::{result_, rows};
use walkers::Position;

use crate::types::{Airport, Flight, FlightInfo, FlightStatus};

#[derive(Debug, Clone)]
pub struct DBError;

const IP: &str = "127.0.0.1";

/// A trait that defines the required methods for a provider to manage flight
/// and airport data. This trait is implemented by any structure that interacts
/// with the underlying database to fetch and manipulate flight and airport information.
pub trait Provider {
    fn get_airports_by_country(&mut self, country: &str) -> Result<Vec<Airport>, DBError>;

    fn get_departure_flights(
        &mut self,
        airport: &str,
        date: NaiveDate,
    ) -> Result<Vec<Flight>, DBError>;

    fn get_arrival_flights(
        &mut self,
        airport: &str,
        date: NaiveDate,
    ) -> Result<Vec<Flight>, DBError>;

    fn get_flight_info(&mut self, number: &str) -> Result<FlightInfo, DBError>;

    fn get_flights_by_airport(&mut self, airport: &str) -> Result<Vec<Flight>, DBError>;

    fn get_airports(&mut self) -> Result<Vec<Airport>, DBError>;

    fn add_flight(&mut self, flight: Flight) -> Result<(), DBError>;

    fn update_state(&mut self, flight: Flight, direction: &str) -> Result<(), DBError>;
}

/// A structure representing the database connection for managing flight and airport data.
///
/// The `Db` struct is responsible for connecting to a Cassandra database and
/// executing queries required by the graphical interface of the flight simulator.
pub struct Db {
    driver: CassandraClient,
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

impl Db {
    /// Creates a new instance of the `Db` struct, establishing a connection to the database.
    pub fn new() -> Self {
        let mut driver = CassandraClient::connect(Ipv4Addr::from_str(IP).unwrap()).unwrap();
        driver.startup().unwrap();
        Self { driver: driver }
    }

    fn execute_query(&mut self, query: &str, consistency: &str) -> Result<QueryResult, DBError> {
        self.driver.execute(query, consistency).map_err(|_| DBError)
    }
}

impl Provider for Db {
    /// Get the airports from a country from the database to show them in the graphical interface.
    fn get_airports_by_country(
        &mut self,
        country: &str,
    ) -> std::result::Result<Vec<Airport>, DBError> {
        let query = "SELECT * FROM sky.airports WHERE country = 'ARG'".to_string();

        let result = self
            .execute_query(query.as_str(), "quorum")
            .map_err(|_| DBError)?;

        let mut airports: Vec<Airport> = Vec::new();
        if let QueryResult::Result(result_::Result::Rows(res)) = result {
            for row in res.rows_content {
                let mut airport = Airport {
                    name: String::new(),
                    iata: String::new(),
                    position: Position::from_lat_lon(0.0, 0.0),
                    country: String::from(country),
                };

                if let Some(iata) = row.get("iata") {
                    if let rows::ColumnValue::Ascii(iata) = iata {
                        airport.iata = iata.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(name) = row.get("name") {
                    if let rows::ColumnValue::Ascii(name) = name {
                        airport.name = name.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let (Some(lat), Some(lon)) = (row.get("lat"), row.get("lon")) {
                    if let (
                        rows::ColumnValue::Double(latitud),
                        rows::ColumnValue::Double(longitud),
                    ) = (lat, lon)
                    {
                        airport.position = Position::from_lat_lon(*latitud, *longitud);
                    }
                } else {
                    return Err(DBError);
                }

                airports.push(airport);
            }
        }

        Ok(airports)
    }

    fn get_departure_flights(
        &mut self,
        airport: &str,
        date: NaiveDate,
    ) -> std::result::Result<Vec<Flight>, DBError> {
        let from = NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| DBError)?;
        let from = NaiveDateTime::new(date, from).and_utc().timestamp();

        let query = format!(
            "SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = '{airport}' AND direction = 'departure' AND departure_time > {from}"
        );

        let result = self
            .execute_query(query.as_str(), "quorum")
            .map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        if let QueryResult::Result(result_::Result::Rows(res)) = result {
            for row in res.rows_content {
                let mut flight = Flight {
                    number: String::new(),
                    status: String::new(),
                    position: Position::from_lat_lon(0.0, 0.0),
                    heading: 0.0,
                    departure_time: 0,
                    arrival_time: 0,
                    airport: String::new(),
                    direction: String::new(),
                    info: None,
                };

                if let Some(number) = row.get("number") {
                    if let rows::ColumnValue::Ascii(number) = number {
                        flight.number = number.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(status) = row.get("status") {
                    if let rows::ColumnValue::Ascii(status) = status {
                        flight.status = status.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(departure_time) = row.get("departure_time") {
                    if let rows::ColumnValue::Timestamp(departure_time) = departure_time {
                        flight.departure_time = *departure_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(arrival_time) = row.get("arrival_time") {
                    if let rows::ColumnValue::Timestamp(arrival_time) = arrival_time {
                        flight.arrival_time = *arrival_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(airport) = row.get("airport") {
                    if let rows::ColumnValue::Ascii(airport) = airport {
                        flight.airport = airport.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(direction) = row.get("direction") {
                    if let rows::ColumnValue::Ascii(direction) = direction {
                        flight.direction = direction.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                flights.push(flight);
            }
        }

        Ok(flights)
    }

    fn get_arrival_flights(
        &mut self,
        airport: &str,
        date: NaiveDate,
    ) -> std::result::Result<Vec<Flight>, DBError> {
        let from = NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| DBError)?;
        let from = NaiveDateTime::new(date, from).and_utc().timestamp();

        let to = NaiveTime::from_hms_opt(23, 59, 59).ok_or_else(|| DBError)?;
        let to = NaiveDateTime::new(date, to).and_utc().timestamp();

        let query = format!(
            "SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = '{airport}' AND direction = 'arrival' AND arrival_time > {from} AND arrival_time < {to}"
        );

        let result = self
            .execute_query(query.as_str(), "quorum")
            .map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        if let QueryResult::Result(result_::Result::Rows(res)) = result {
            for row in res.rows_content {
                let mut flight = Flight {
                    number: String::new(),
                    status: String::new(),
                    position: Position::from_lat_lon(0.0, 0.0),
                    heading: 0.0,
                    departure_time: 0,
                    arrival_time: 0,
                    airport: String::new(),
                    direction: String::new(),
                    info: None,
                };

                if let Some(number) = row.get("number") {
                    if let rows::ColumnValue::Ascii(number) = number {
                        flight.number = number.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(status) = row.get("status") {
                    if let rows::ColumnValue::Ascii(status) = status {
                        flight.status = status.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(departure_time) = row.get("departure_time") {
                    if let rows::ColumnValue::Timestamp(departure_time) = departure_time {
                        flight.departure_time = *departure_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(arrival_time) = row.get("arrival_time") {
                    if let rows::ColumnValue::Timestamp(arrival_time) = arrival_time {
                        flight.arrival_time = *arrival_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(airport) = row.get("airport") {
                    if let rows::ColumnValue::Ascii(airport) = airport {
                        flight.airport = airport.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(direction) = row.get("direction") {
                    if let rows::ColumnValue::Ascii(direction) = direction {
                        flight.direction = direction.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                flights.push(flight);
            }
        }

        Ok(flights)
    }

    fn get_flight_info(&mut self, number: &str) -> std::result::Result<FlightInfo, DBError> {
        let query = format!(
            "SELECT number, fuel, height, speed, origin, destination FROM sky.flight_info WHERE number = '{number}'"
        );

        let result = self
            .execute_query(query.as_str(), "one")
            .map_err(|_| DBError)?;

        let mut flight_info = FlightInfo {
            number: String::new(),
            fuel: 0.0,
            height: 0,
            speed: 0,
            origin: Default::default(),
            destination: Default::default(),
        };

        if let QueryResult::Result(result_::Result::Rows(res)) = result {
            for row in res.rows_content {
                if let Some(number) = row.get("number") {
                    if let rows::ColumnValue::Ascii(number) = number {
                        flight_info.number = number.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(fuel) = row.get("fuel") {
                    if let rows::ColumnValue::Double(fuel) = fuel {
                        flight_info.fuel = *fuel;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(height) = row.get("height") {
                    if let rows::ColumnValue::Int(height) = height {
                        flight_info.height = *height;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(speed) = row.get("speed") {
                    if let rows::ColumnValue::Int(speed) = speed {
                        flight_info.speed = *speed;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(origin) = row.get("origin") {
                    if let rows::ColumnValue::Ascii(origin) = origin {
                        flight_info.origin = origin.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(destination) = row.get("destination") {
                    if let rows::ColumnValue::Ascii(destination) = destination {
                        flight_info.destination = destination.to_string();
                    }
                } else {
                    return Err(DBError);
                }
            }
        }

        Ok(flight_info)
    }

    fn get_flights_by_airport(&mut self, airport: &str) -> Result<Vec<Flight>, DBError> {
        let today = Utc::now().date_naive();
        let from = NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| DBError)?;
        let from = NaiveDateTime::new(today, from).and_utc().timestamp();

        let query = format!(
            "SELECT number, status, lat, lon, angle, departure_time, arrival_time, airport, direction FROM sky.flights WHERE airport = '{airport}' AND departure_time > {from}"
        );

        let result = self
            .execute_query(query.as_str(), "one")
            .map_err(|_| DBError)?;

        let mut flights: Vec<Flight> = Vec::new();

        if let QueryResult::Result(result_::Result::Rows(res)) = result {
            for row in res.rows_content {
                let mut flight = Flight {
                    number: String::new(),
                    status: String::new(),
                    position: Position::from_lat_lon(0.0, 0.0),
                    heading: 0.0,
                    departure_time: 0,
                    arrival_time: 0,
                    airport: String::new(),
                    direction: String::new(),
                    info: None,
                };

                if let Some(number) = row.get("number") {
                    if let rows::ColumnValue::Ascii(number) = number {
                        flight.number = number.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(status) = row.get("status") {
                    if let rows::ColumnValue::Ascii(status) = status {
                        flight.status = status.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(departure_time) = row.get("departure_time") {
                    if let rows::ColumnValue::Timestamp(departure_time) = departure_time {
                        flight.departure_time = *departure_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(arrival_time) = row.get("arrival_time") {
                    if let rows::ColumnValue::Timestamp(arrival_time) = arrival_time {
                        flight.arrival_time = *arrival_time;
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(airport) = row.get("airport") {
                    if let rows::ColumnValue::Ascii(airport) = airport {
                        flight.airport = airport.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(direction) = row.get("direction") {
                    if let rows::ColumnValue::Ascii(direction) = direction {
                        flight.direction = direction.to_string();
                    }
                } else {
                    return Err(DBError);
                }

                if let (Some(lat), Some(lon)) = (row.get("lat"), row.get("lon")) {
                    if let (
                        rows::ColumnValue::Double(latitud),
                        rows::ColumnValue::Double(longitud),
                    ) = (lat, lon)
                    {
                        flight.position = Position::from_lat_lon(*latitud, *longitud);
                    }
                } else {
                    return Err(DBError);
                }

                if let Some(angle) = row.get("angle") {
                    if let rows::ColumnValue::Float(angle) = angle {
                        flight.heading = *angle;
                    }
                } else {
                    return Err(DBError);
                }

                if flight.status == FlightStatus::OnTime.as_str()
                    || flight.status == FlightStatus::Delayed.as_str()
                {
                    flights.push(flight);
                }
            }
        }

        Ok(flights)
    }

    fn add_flight(&mut self, flight: Flight) -> Result<(), DBError> {
        let query_check = format!(
            "SELECT number FROM sky.flight_info WHERE number = '{}';",
            flight.number
        );

        let result_check = self
            .execute_query(query_check.as_str(), "quorum")
            .map_err(|_| DBError)?;

        if let QueryResult::Result(result_::Result::Rows(res)) = result_check {
            if !res.rows_content.is_empty() {
                return Err(DBError);
            }
        }

        let flight_info = match flight.info {
            Some(data) => data,
            None => return Err(DBError),
        };

        let insert_departure_query = format!(
            "INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('{}', '{}', {}, {}, {}, {}, {}, '{}', 'departure');",
            flight.number,
            flight.status.as_str(),
            flight.position.lat(),
            flight.position.lon(),
            flight.heading,
            flight.departure_time,
            flight.arrival_time,
            flight_info.origin
        );

        let insert_arrival_query = format!(
            "INSERT INTO sky.flights (number, status, lat, lon, angle, departure_time, arrival_time, airport, direction) VALUES ('{}', '{}', {}, {}, {}, {}, {}, '{}', 'arrival');",
            flight.number,
            flight.status.as_str(),
            flight.position.lat(),
            flight.position.lon(),
            flight.heading,
            flight.departure_time,
            flight.arrival_time,
            flight_info.destination
        );

        // Inserción en la tabla flight_info con la información del vuelo
        let insert_flight_info_query = format!(
            "INSERT INTO sky.flight_info (number, fuel, height, speed, origin, destination) VALUES ('{}', {}, {}, {}, '{}', '{}');",
            flight_info.number,
            flight_info.fuel,
            flight_info.height,
            flight_info.speed,
            flight_info.origin,
            flight_info.destination
        );

        // Ejecución de las consultas en Cassandra
        self.execute_query(insert_departure_query.as_str(), "quorum")
            .map_err(|_| DBError)?;
        self.execute_query(insert_arrival_query.as_str(), "quorum")
            .map_err(|_| DBError)?;
        self.execute_query(insert_flight_info_query.as_str(), "quorum")
            .map_err(|_| DBError)?;

        Ok(())
    }

    fn update_state(&mut self, flight: Flight, direction: &str) -> Result<(), DBError> {
        let info = self.get_flight_info(&flight.number)?;

        let (other_airport, other_direction) = match direction {
            "ARRIVAL" => (&info.origin, "DEPARTURE"),
            "DEPARTURE" => (&info.destination, "ARRIVAL"),
            _ => return Err(DBError),
        };

        let update_query_status_departure = format!(
            "UPDATE sky.flights SET status = '{}' WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
            flight.status.as_str(),
            flight.airport,
            &direction.to_lowercase(),
            flight.departure_time,
            flight.arrival_time,
            flight.number
        );

        self.execute_query(&update_query_status_departure, "quorum")
            .map_err(|_| DBError)?;

        let update_query_status_arrival = format!(
                "UPDATE sky.flights SET status = '{}' WHERE airport = '{}' AND direction = '{}' AND departure_time = {} AND arrival_time = {} AND number = {};",
                flight.status.as_str(),
                other_airport,
                other_direction.to_lowercase(),
                flight.departure_time,
                flight.arrival_time,
                flight.number
            );

        self.execute_query(&update_query_status_arrival, "quorum")
            .map_err(|_| DBError)?;

        Ok(())
    }

    fn get_airports(&mut self) -> Result<Vec<Airport>, DBError> {
        self.get_airports_by_country("ARG")
    }
}
