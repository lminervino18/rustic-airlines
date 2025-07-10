use super::{application_state::ApplicationState, heartbeat_state::HeartbeatState};

#[derive(Debug, Clone, PartialEq, Default)]
/// Represents the state of the endpoint in the cluster at a given point in time.
///
/// ### Fields
/// - `heartbeat_state`: The heartbeat state of the endpoint.
/// - `application_state`: The application state of the endpoint.
pub struct EndpointState {
    pub heartbeat_state: HeartbeatState,
    pub application_state: ApplicationState,
}

impl EndpointState {
    /// Creates a new `EndpointState` with the given `application_state` and `heartbeat_state`.
    pub fn new(application_state: ApplicationState, heartbeat_state: HeartbeatState) -> Self {
        Self {
            application_state,
            heartbeat_state,
        }
    }
}
