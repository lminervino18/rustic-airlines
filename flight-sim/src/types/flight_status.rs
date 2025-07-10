use super::sim_error::SimError;

/// Represents the various statuses a flight can have.

#[derive(Debug, PartialEq, Clone)]
pub enum FlightStatus {
    Scheduled,
    OnTime,
    Delayed,
    Finished,
    Canceled,
}

impl FlightStatus {
    /// Converts the `FlightStatus` variant to its corresponding string representation.
    pub fn as_str(&self) -> &str {
        match self {
            FlightStatus::Scheduled => "scheduled",
            FlightStatus::OnTime => "on time",
            FlightStatus::Delayed => "delayed",
            FlightStatus::Finished => "finished",
            FlightStatus::Canceled => "canceled",
        }
    }
    /// Creates a `FlightStatus` variant from a string slice.
    pub fn from_str(status: &str) -> Result<FlightStatus, SimError> {
        match status.to_lowercase().as_str() {
            "scheduled" => Ok(FlightStatus::Scheduled),
            "on time" => Ok(FlightStatus::OnTime),
            "delayed" => Ok(FlightStatus::Delayed),
            "finished" => Ok(FlightStatus::Finished),
            "canceled" => Ok(FlightStatus::Canceled),
            _ => Err(SimError::Other("Invalid flight status".to_string())),
        }
    }
}
