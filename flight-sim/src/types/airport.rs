/// Represents an airport with its name, IATA code, geographical position, and country.

#[derive(Clone, Debug)]
pub struct Airport {
    pub iata_code: String,
    pub country: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
}

impl Airport {
    pub fn new(
        iata_code: String,
        country: String,
        name: String,
        latitude: f64,
        longitude: f64,
    ) -> Self {
        Airport {
            iata_code,
            country,
            name,
            latitude,
            longitude,
        }
    }
}

impl Default for Airport {
    fn default() -> Self {
        Airport {
            iata_code: "XXX".to_string(), // Código IATA de aeropuerto ficticio
            country: "XXX".to_string(),
            name: "Default Airport".to_string(),
            latitude: 0.0,
            longitude: 0.0,
        }
    }
}
