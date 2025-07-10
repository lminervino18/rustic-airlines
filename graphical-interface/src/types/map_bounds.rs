use walkers::Position;

/// Represents the geographical boundaries of a map view, defined by minimum
/// and maximum latitude and longitude.
pub struct _MapBounds {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

impl _MapBounds {
    /// Checks whether a given position is within the map bounds.
    pub fn _is_within_bounds(&self, pos: &Position) -> bool {
        pos.lat() >= self.min_lat
            && pos.lat() <= self.max_lat
            && pos.lon() >= self.min_lon
            && pos.lon() <= self.max_lon
    }
}
