use std::{collections::HashMap, f64::consts::PI, sync::RwLockReadGuard};

use chrono::NaiveDateTime;

use super::{airport::Airport, flight_status::FlightStatus, sim_error::SimError};

/// Represents a flight in the simulator, including its status, route, position,
/// and additional metadata for simulation.
pub struct Flight {
    pub flight_number: String,
    pub status: FlightStatus,
    pub departure_time: NaiveDateTime,
    pub arrival_time: NaiveDateTime,
    pub origin: Airport,
    pub destination: Airport,
    pub latitude: f64,
    pub longitude: f64,
    pub angle: f32,
    pub altitude: i32,
    pub fuel_level: f64,
    pub total_distance: f64,
    pub distance_traveled: f64,
    pub average_speed: i32,
}

const EARTH_RADIUS_KM: f64 = 6371.0;

impl Flight {
    /// Creates a new flight from the information given from the console interface.
    pub fn new_from_console(
        airports: RwLockReadGuard<HashMap<String, Airport>>,
        flight_number: &str,
        origin_code: &str,
        destination_code: &str,
        departure_time_str: &str,
        arrival_time_str: &str,
        average_speed: i32,
    ) -> Result<Self, SimError> {
        let origin = airports
            .get(origin_code)
            .ok_or_else(|| SimError::AirportNotFound(origin_code.to_string()))?
            .clone();

        let destination = airports
            .get(destination_code)
            .ok_or_else(|| SimError::AirportNotFound(destination_code.to_string()))?
            .clone();

        let departure_time = parse_datetime(departure_time_str)?;
        let arrival_time = parse_datetime(arrival_time_str)?;

        if arrival_time <= departure_time || average_speed <= 0 {
            return Err(SimError::InvalidInput);
        }

        let starting_latitude = origin.latitude;
        let starting_longitude = origin.longitude;

        let total_distance = haversine_distance(
            origin.latitude,
            origin.longitude,
            destination.latitude,
            destination.longitude,
        );

        let mut flight = Flight {
            flight_number: flight_number.to_string(),
            status: FlightStatus::Scheduled,
            departure_time,
            arrival_time,
            origin,
            destination,
            latitude: starting_latitude,
            longitude: starting_longitude,
            angle: 0.0,
            altitude: 35000,
            fuel_level: 100.0,
            total_distance,
            distance_traveled: 0.0,
            average_speed,
        };

        flight.angle = flight.calculate_bearing() as f32;

        Ok(flight)
    }

    fn calculate_bearing(&self) -> f64 {
        let lat1 = self.latitude.to_radians();
        let lon1 = self.longitude.to_radians();
        let lat2 = self.destination.latitude.to_radians();
        let lon2 = self.destination.longitude.to_radians();

        let delta_lon = lon2 - lon1;

        // Fórmula para calcular el bearing (rumbo)
        let y = delta_lon.sin() * lat2.cos();
        let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * delta_lon.cos();
        let bearing = y.atan2(x).to_degrees();

        // Asegúrate de que el ángulo está en el rango [0, 360)
        (bearing - 90.0 + 360.0) % 360.0
    }

    fn calculate_position(&mut self, current_time: NaiveDateTime) {
        let elapsed_hours = current_time
            .signed_duration_since(self.departure_time)
            .num_seconds() as f64
            / 3600.0;

        // Calculate traveled distance and update position
        self.distance_traveled =
            (self.average_speed as f64 * elapsed_hours).min(self.total_distance);
        let progress_ratio = self.distance_traveled / self.total_distance;
        self.latitude = self.origin.latitude
            + progress_ratio * (self.destination.latitude - self.origin.latitude);
        self.longitude = self.origin.longitude
            + progress_ratio * (self.destination.longitude - self.origin.longitude);
        self.fuel_level = (100.0 - elapsed_hours * 5.0).max(0.0); // Burn fuel over time

        // Update altitude when approaching the destination
        self.altitude = if self.distance_traveled >= self.total_distance * 0.95 {
            let altitude = self.altitude - 500;
            if altitude < 0 {
                0
            } else {
                altitude
            }
        } else {
            self.altitude
        };
    }

    /// Update the position of the flight and its fuel level based on the current time
    pub fn check_states_and_update_flight(&mut self, current_time: NaiveDateTime) -> bool {
        let mut new_status: bool = false;

        match self.status {
            FlightStatus::Scheduled => {
                if current_time >= self.departure_time {
                    if self.altitude == 0 {
                        self.altitude = 10000
                    }; // Default plane altitude in case it wasn't specified.
                    self.status = FlightStatus::OnTime;
                    new_status = true;
                }
            }
            FlightStatus::OnTime => {
                self.calculate_position(current_time);
                if current_time >= self.arrival_time {
                    self.status = FlightStatus::Delayed;
                    new_status = true;
                }
                if self.distance_traveled >= self.total_distance {
                    self.land();
                    self.status = FlightStatus::Finished;
                    new_status = true;
                }
            }
            FlightStatus::Delayed => {
                self.calculate_position(current_time);
                if self.distance_traveled >= self.total_distance {
                    self.land();
                    self.status = FlightStatus::Finished;
                    new_status = true;
                }
            }
            FlightStatus::Canceled => {
                if self.altitude != 0 {
                    self.land();
                    self.latitude = self.origin.latitude;
                    self.longitude = self.origin.longitude;
                }
            }
            FlightStatus::Finished => {
                if self.altitude > 0 {
                    self.land();
                    self.latitude = self.destination.latitude;
                    self.longitude = self.destination.longitude;
                }
            }
        }

        new_status
    }

    // Land the flight
    fn land(&mut self) {
        self.fuel_level = 0.0;
        self.altitude = 0;
    }
}

fn parse_datetime(datetime_str: &str) -> Result<NaiveDateTime, SimError> {
    let format = "%d-%m-%Y %H:%M:%S"; // The expected format for the date input
    NaiveDateTime::parse_from_str(datetime_str, format)
        .map_err(|_| SimError::InvalidDateFormat(datetime_str.to_string()))
}

fn haversine_distance(origin_lat: f64, origin_lon: f64, dest_lat: f64, dest_lon: f64) -> f64 {
    let origin_lat_rad = origin_lat * PI / 180.0;
    let origin_lon_rad = origin_lon * PI / 180.0;
    let dest_lat_rad = dest_lat * PI / 180.0;
    let dest_lon_rad = dest_lon * PI / 180.0;

    let delta_lat = dest_lat_rad - origin_lat_rad;
    let delta_lon = dest_lon_rad - origin_lon_rad;

    // Haversine formula
    let a = (delta_lat / 2.0).sin().powi(2)
        + origin_lat_rad.cos() * dest_lat_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_KM * c
}
