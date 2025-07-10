use std::fmt;

/// Represents errors that can occur in the flight simulator application.
#[derive(Debug)]
pub enum SimError {
    InvalidInput,
    InvalidFlight(String), // For invalid flight details (e.g., wrong date format)
    AirportNotFound(String), // If airport can't be found
    InvalidDateFormat(String), // When the date format is incorrect
    TimerLockError(String), // Para errores de bloqueo del Timer
    TimerStartError(String), // Para errores al iniciar el Timer
    InvalidDuration(String), // Cuando se pasa una duración inválida
    Other(String),         // Generic error case with a custom message
    ClientError,           // If something went wrong with the client
}

/// Implement the Display trait for user-friendly error messages
impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimError::InvalidInput => {
                write!(f, "Invalid input. Please check your input and try again.")
            }
            SimError::InvalidFlight(ref flight) => write!(f, "Invalid flight details: {}", flight),
            SimError::AirportNotFound(ref iata_code) => {
                write!(f, "Airport not found: {}", iata_code)
            }
            SimError::InvalidDateFormat(ref date_str) => {
                write!(f, "Invalid date format: {}", date_str)
            }
            SimError::TimerLockError(msg) => write!(f, "Timer lock error: {}", msg),
            SimError::TimerStartError(msg) => write!(f, "Timer start error: {}", msg),
            SimError::InvalidDuration(msg) => write!(f, "Invalid duration: {}", msg),
            SimError::Other(ref message) => write!(f, "Error: {}", message),
            SimError::ClientError => write!(f, "Something went wrong with the client"),
        }
    }
}
