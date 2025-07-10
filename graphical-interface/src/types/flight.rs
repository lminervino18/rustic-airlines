use walkers::Position;

use super::FlightInfo;

/// Represents a flight in the simulator, including its status, route, position,
/// and additional metadata for display.

#[derive(Debug, Clone, PartialEq)]
pub struct Flight {
    pub number: String,
    pub status: String,
    pub position: Position,
    pub heading: f32,
    pub departure_time: i64,
    pub arrival_time: i64,
    pub airport: String,
    pub direction: String,
    pub info: Option<FlightInfo>,
}
