#[derive(Debug, Clone, PartialEq)]

/// Represents additional information from the flight to be displayed.
pub struct FlightInfo {
    pub number: String,
    pub fuel: f64,
    pub height: i32,
    pub speed: i32,
    pub origin: String,
    pub destination: String,
}

impl Default for FlightInfo {
    fn default() -> Self {
        FlightInfo {
            number: String::from("flight"),
            fuel: 0.0,
            height: 0,
            speed: 0,
            origin: String::from("XXX"),
            destination: String::from("XXX"),
        }
    }
}
