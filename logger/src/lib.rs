use chrono::Utc;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
enum LogLevel {
    Info(Color),
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy)]
pub enum Color {
    Red,
    Green,
    Blue,
    Yellow,
    Cyan,
    Magenta,
    White,
}

impl Color {
    fn to_ansi_code(self) -> &'static str {
        match self {
            Color::Red => "\x1b[31m",
            Color::Green => "\x1b[32m",
            Color::Blue => "\x1b[34m",
            Color::Yellow => "\x1b[33m",
            Color::Cyan => "\x1b[36m",
            Color::Magenta => "\x1b[35m",
            Color::White => "\x1b[37m",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Logger {
    log_file: PathBuf,
}

impl Logger {
    /// Creates a new `Logger` instance.
    ///
    /// # Parameters
    /// - `log_dir`: Path to the directory where the log file should be created.
    /// - `ip`: The IP address to include in the log file name.
    ///
    /// # Returns
    /// A new `Logger` instance.
    pub fn new(log_dir: &Path, ip: &str) -> Result<Self, LoggerError> {
        // Asegurarse de que el directorio existe
        if log_dir.is_dir() {
            std::fs::create_dir_all(log_dir).map_err(LoggerError::from)?;
        } else {
            return Err(LoggerError::InvalidPath(
                "Provided path is not a directory.".into(),
            ));
        }

        // Crear el archivo node_{ip}.log dentro del directorio
        let sanitized_ip = ip.replace(":", "_"); // Reemplaza ":" para evitar problemas en nombres de archivo
        let log_file = log_dir.join(format!("node_{}.log", sanitized_ip));

        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true) // Sobrescribe el archivo si ya existe
            .open(&log_file)
            .map_err(LoggerError::from)?;

        Ok(Logger { log_file })
    }

    // Generic method for writing log messages
    fn log(&self, level: LogLevel, message: &str, to_console: bool) -> Result<(), LoggerError> {
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let log_message = match &level {
            LogLevel::Info(_) => format!("[INFO] [{}]: {}\n", timestamp, message),
            LogLevel::Warn => format!("[WARN] [{}]: {}\n", timestamp, message),
            LogLevel::Error => format!("[ERROR] [{}]: {}\n", timestamp, message),
        };

        // If logging to console, apply colors
        if to_console {
            let colored_message = match &level {
                LogLevel::Info(color) => format!("{}{}\x1b[0m", color.to_ansi_code(), log_message),
                LogLevel::Warn => format!("\x1b[93m{}\x1b[0m", log_message), // Bright Yellow
                LogLevel::Error => format!("\x1b[91m{}\x1b[0m", log_message), // Bright Red
            };
            print!("{}", colored_message);
            io::stdout().flush().map_err(LoggerError::from)?;
        }

        // Open the file, write the log message, and close the file
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file)
            .map_err(LoggerError::from)?;
        file.write_all(log_message.as_bytes())
            .map_err(LoggerError::from)?;
        file.flush().map_err(LoggerError::from)?;

        Ok(())
    }

    /// Logs an informational message.
    ///
    /// # Parameters
    /// - `message`: The informational message to log.
    /// - `color`: The color to use for the console output.
    /// - `to_console`: Whether to log the message to the console as well.
    pub fn info(&self, message: &str, color: Color, to_console: bool) -> Result<(), LoggerError> {
        self.log(LogLevel::Info(color), message, to_console)
    }

    /// Logs a warning message.
    ///
    /// # Parameters
    /// - `message`: The warning message to log.
    /// - `to_console`: Whether to log the message to the console as well.
    pub fn warn(&self, message: &str, to_console: bool) -> Result<(), LoggerError> {
        self.log(LogLevel::Warn, message, to_console)
    }

    /// Logs an error message.
    ///
    /// # Parameters
    /// - `message`: The error message to log.
    /// - `to_console`: Whether to log the message to the console as well.
    pub fn error(&self, message: &str, to_console: bool) -> Result<(), LoggerError> {
        self.log(LogLevel::Error, message, to_console)
    }
}

#[derive(Debug)]
pub enum LoggerError {
    IoError(std::io::Error),
    InvalidPath(String), // Nueva variante para manejar rutas inválidas
}

impl std::fmt::Display for LoggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoggerError::IoError(e) => write!(f, "I/O Error: {}", e),
            LoggerError::InvalidPath(msg) => write!(f, "Invalid Path: {}", msg),
        }
    }
}

impl std::error::Error for LoggerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LoggerError::IoError(e) => Some(e),
            LoggerError::InvalidPath(_) => None, // Las rutas inválidas no tienen una fuente de error adicional
        }
    }
}

impl From<std::io::Error> for LoggerError {
    fn from(err: std::io::Error) -> Self {
        LoggerError::IoError(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_logger_creation_and_logging() {
        // Usar un directorio temporal en /tmp
        let log_dir = Path::new("/tmp/test_logs");
        fs::create_dir_all(log_dir).expect("Failed to create test directory");

        let ip = "127.0.0.1";
        let logger = Logger::new(log_dir, ip).expect("Failed to create logger");

        let message = "Test log message.";
        logger
            .info(message, Color::Green, false)
            .expect("Failed to log message");

        let log_file_path = log_dir.join(format!("node_{}.log", ip.replace(":", "_")));
        let log_contents = fs::read_to_string(&log_file_path).expect("Failed to read log file");

        assert!(log_contents.contains("[INFO]"), "INFO level missing in log");
        assert!(log_contents.contains(message), "Logged message missing");

        // Limpieza
        fs::remove_dir_all(log_dir).expect("Failed to remove test directory");
    }

    #[test]
    fn test_invalid_path() {
        let invalid_path = Path::new("/invalid/path");
        let ip = "127.0.0.1";

        let result = Logger::new(invalid_path, ip);
        assert!(result.is_err(), "Logger should fail with an invalid path");
    }
}
