#[derive(Debug, Clone, Copy, PartialEq, Ord, PartialOrd, Eq, Default)]
/// The ordering of `HeartbeatState` is lexicographical based on the `generation` first and then `version`. `Ord` does this.
/// Represents the heartbeat state of the endpoint in the cluster at a given point in time.
///
/// ### Fields
/// - `generation`: The generation of the node.
/// - `version`: The version of the node.
pub struct HeartbeatState {
    pub generation: u128,
    pub version: u32,
}

impl HeartbeatState {
    /// Creates a new `HeartbeatState` with the given `generation` and `version`.
    pub fn new(generation: u128, version: u32) -> Self {
        Self {
            generation,
            version,
        }
    }

    /// Increments the version of the `HeartbeatState`.
    pub fn inc_version(&mut self) {
        self.version += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::HeartbeatState;

    #[test]
    fn heartbeat_state_ordering() {
        let heartbeat_state_1 = HeartbeatState::new(1, 1);
        let heartbeat_state_2 = HeartbeatState::new(2, 1);
        let heartbeat_state_3 = HeartbeatState::new(1, 2);
        let heartbeat_state_4 = HeartbeatState::new(2, 2);

        assert!(heartbeat_state_1 < heartbeat_state_2);
        assert!(heartbeat_state_1 < heartbeat_state_3);
        assert!(heartbeat_state_1 < heartbeat_state_4);
        assert!(heartbeat_state_2 > heartbeat_state_1);
        assert!(heartbeat_state_2 > heartbeat_state_3);
        assert!(heartbeat_state_2 < heartbeat_state_4);
        assert!(heartbeat_state_3 > heartbeat_state_1);
        assert!(heartbeat_state_3 < heartbeat_state_2);
        assert!(heartbeat_state_3 < heartbeat_state_4);
        assert!(heartbeat_state_4 > heartbeat_state_1);
        assert!(heartbeat_state_4 > heartbeat_state_2);
        assert!(heartbeat_state_4 > heartbeat_state_3);
    }
}
