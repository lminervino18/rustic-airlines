//! This module contains the implementation of the gossip protocol.
//! TODO: complete

use chrono::{self, Utc};

use messages::{Ack, Ack2, Digest, GossipMessage, Syn};
use query_creator::clauses::{
    keyspace::create_keyspace_cql::CreateKeyspace, table::create_table_cql::CreateTable,
};
use rand::{seq::IteratorRandom, thread_rng};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    net::Ipv4Addr,
};
use structures::{
    application_state::{KeyspaceSchema, NodeStatus, Schema, TableSchema},
    endpoint_state::EndpointState,
    heartbeat_state::HeartbeatState,
};
pub mod messages;
pub mod structures;

/// Struct to represent the gossiper node.
///
/// ### Fields
/// - `endpoints_state`: HashMap containing the state of all the endpoints that the gossiper knows about.
#[derive(Clone)]
pub struct Gossiper {
    pub endpoints_state: HashMap<Ipv4Addr, EndpointState>,
}

#[derive(Debug)]
/// Enum to represent the different errors that can occur during the gossip protocol.
pub enum GossipError {
    SynError,
    NoEndpointStateForIp,
    NoSuchKeyspace,
    KeyspaceAlreadyExists,
    TableAlreadyExists,
}

impl fmt::Display for GossipError {
    /// Implementation of the `fmt` method to convert the error into a readable string.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = match self {
            GossipError::SynError => "Syn error occurred",
            GossipError::NoEndpointStateForIp => "There is no endpoint state for the given ip",
            GossipError::NoSuchKeyspace => "The given keyspace does not exist",
            GossipError::KeyspaceAlreadyExists => "The given keyspace already exists",
            GossipError::TableAlreadyExists => "The given table already exists",
        };
        write!(f, "{}", description)
    }
}

impl Default for Gossiper {
    fn default() -> Self {
        Self::new()
    }
}

impl Gossiper {
    /// Create a new Gossiper instance with an empty state.
    pub fn new() -> Self {
        Self {
            endpoints_state: HashMap::new(),
        }
    }

    /// Increment the version of the heartbeat state of the endpoint with the given ip.
    pub fn heartbeat(&mut self, ip: Ipv4Addr) -> Result<(), GossipError> {
        self.endpoints_state
            .get_mut(&ip)
            .ok_or(GossipError::NoEndpointStateForIp)?
            .heartbeat_state
            .inc_version();

        Ok(())
    }

    /// Set the application state of the endpoint with the given ip.
    pub fn with_endpoint_state(mut self, ip: Ipv4Addr) -> Self {
        self.endpoints_state.insert(ip, EndpointState::default());
        self
    }

    /// Inserts the given ip with a default state into the gossiper.
    pub fn with_seeds(mut self, seeds_ip: Vec<Ipv4Addr>) -> Self {
        for ip in seeds_ip {
            self.endpoints_state.insert(ip, EndpointState::default());
        }
        self
    }

    /// Changes the status of the application state of the endpoint with the given ip.
    pub fn change_status(&mut self, ip: Ipv4Addr, status: NodeStatus) -> Result<(), GossipError> {
        let app_state = &mut self
            .endpoints_state
            .get_mut(&ip)
            .ok_or(GossipError::NoEndpointStateForIp)?
            .application_state;

        app_state.status = status;
        app_state.version += 1;

        Ok(())
    }

    /// Returns a copy of the application state of the endpoint with the given ip.
    pub fn get_status(&self, ip: Ipv4Addr) -> Result<NodeStatus, GossipError> {
        let app_state = self
            .endpoints_state
            .get(&ip)
            .ok_or(GossipError::NoEndpointStateForIp)?
            .application_state
            .status;

        Ok(app_state)
    }

    /// Returns the schema with the largest timestamp from the known application states.
    pub fn get_most_updated_schema(&self) -> Option<Schema> {
        let mut most_updated_schema = None;
        let mut most_updated_timestamp = 0;

        for state in self.endpoints_state.values() {
            if state.application_state.schema.timestamp > most_updated_timestamp {
                most_updated_schema = Some(&state.application_state.schema);
                most_updated_timestamp = state.application_state.schema.timestamp;
            }
        }

        most_updated_schema.cloned()
    }

    /// Removes the keyspace from the application state of the endpoint with the given ip.
    pub fn remove_keyspace(&mut self, ip: Ipv4Addr, keyspace: &str) -> Result<(), GossipError> {
        // Find the app state of the given ip
        let app_state = &mut self
            .endpoints_state
            .get_mut(&ip)
            .ok_or(GossipError::NoEndpointStateForIp)?
            .application_state;

        // TODO: make it an app state or schema method which also alters the timestamp
        app_state.schema.keyspaces.remove(keyspace);

        app_state.version += 1;
        app_state.schema.timestamp = Utc::now().timestamp_millis();

        Ok(())
    }

    /// Adds the keyspace to the application state of the endpoint with the given ip.
    pub fn add_keyspace(
        &mut self,
        ip: Ipv4Addr,
        keyspace: CreateKeyspace,
    ) -> Result<(), GossipError> {
        // Find the app state of the given ip
        let app_state = &mut self
            .endpoints_state
            .get_mut(&ip)
            .ok_or(GossipError::NoEndpointStateForIp)?
            .application_state;

        // Add the keyspace to the schema
        if !app_state
            .schema
            .keyspaces
            .keys()
            .any(|k| *k == keyspace.get_name())
        {
            app_state.schema.keyspaces.insert(
                keyspace.get_name(),
                KeyspaceSchema {
                    inner: keyspace,
                    tables: Vec::new(),
                },
            );
        } else {
            return Err(GossipError::KeyspaceAlreadyExists);
        }

        app_state.version += 1;
        app_state.schema.timestamp = Utc::now().timestamp_millis();

        Ok(())
    }

    /// Add the table to the keyspace of the application state of the endpoint with the given ip.
    pub fn add_table(
        &mut self,
        ip: Ipv4Addr,
        table: CreateTable,
        kesyapce_name: &str,
    ) -> Result<(), GossipError> {
        // Find the app state of the given ip
        let app_state = &mut self
            .endpoints_state
            .get_mut(&ip)
            .ok_or(GossipError::NoEndpointStateForIp)?
            .application_state;

        // Test if the keyspace of the table exists
        let keyspace_exists = app_state
            .schema
            .keyspaces
            .keys()
            .any(|k| *k == kesyapce_name);

        if keyspace_exists {
            let keyspace = app_state
                .schema
                .keyspaces
                .get_mut(kesyapce_name)
                .ok_or(GossipError::NoSuchKeyspace)?;

            // Check if the table already exists
            for t in keyspace.tables.iter() {
                if t.inner.get_name() == table.get_name() {
                    return Err(GossipError::TableAlreadyExists);
                }
            }

            let table_schema = TableSchema::new(table);

            keyspace.tables.push(table_schema);
        } else {
            return Err(GossipError::NoSuchKeyspace);
        }

        app_state.version += 1;
        app_state.schema.timestamp = Utc::now().timestamp_millis();

        Ok(())
    }

    /// Removes the table from the keyspace of the application state of the endpoint with the given ip.
    pub fn remove_table(
        &mut self,
        ip: Ipv4Addr,
        keyspace: &str,
        table: &str,
    ) -> Result<(), GossipError> {
        // Find the app state of the given ip
        let app_state = &mut self
            .endpoints_state
            .get_mut(&ip)
            .ok_or(GossipError::NoEndpointStateForIp)?
            .application_state;

        // Find the given keyspace in the schema
        let k = app_state
            .schema
            .keyspaces
            .iter_mut()
            .find(|(keyspace_name, _)| *keyspace_name == keyspace);

        // If the keyspace exists, remove the table from it
        if let Some((_, k_schema)) = k {
            k_schema.tables.retain(|t| t.inner.get_name() != table);
            app_state.version += 1;
            app_state.schema.timestamp = Utc::now().timestamp_millis();

            Ok(())
        } else {
            Err(GossipError::NoSuchKeyspace)
        }
    }

    /// Marks the endpoint with the given ip as dead.
    pub fn kill(&mut self, ip: Ipv4Addr) -> Result<(), GossipError> {
        self.change_status(ip, NodeStatus::Dead)
    }

    /// Picks 3 random ips from the gossiper state, excluding the given ip.
    pub fn pick_ips(&self, exclude: Ipv4Addr) -> Vec<&Ipv4Addr> {
        let mut rng = thread_rng();
        let ips: Vec<&Ipv4Addr> = self
            .endpoints_state
            .iter()
            .filter(|(&ip, state)| {
                ip != exclude && state.application_state.status != NodeStatus::Dead
            })
            .map(|(ip, _)| ip)
            .choose_multiple(&mut rng, 3);
        ips
    }

    /// Creates a Syn message with the digests of the endpoints in the gossiper state.
    pub fn create_syn(&self, from: Ipv4Addr) -> GossipMessage {
        let digests: Vec<Digest> = self
            .endpoints_state
            .iter()
            .map(|(k, v)| Digest::from_heartbeat_state(*k, &v.heartbeat_state))
            .collect();

        let syn = Syn::new(digests);

        GossipMessage {
            from,
            payload: messages::Payload::Syn(syn),
        }
    }

    /// Handles a Syn message and returns the corresponding Ack message.
    pub fn handle_syn(&self, syn: &Syn) -> Ack {
        let mut stale_digests = Vec::new();
        let mut updated_info = BTreeMap::new();

        for digest in &syn.digests {
            if let Some(my_state) = self.endpoints_state.get(&digest.address) {
                let my_digest =
                    Digest::from_heartbeat_state(digest.address, &my_state.heartbeat_state);

                if digest.generation == my_digest.generation && digest.version == my_digest.version
                {
                    continue;
                }

                match digest
                    .get_heartbeat_state()
                    .cmp(&my_digest.get_heartbeat_state())
                {
                    std::cmp::Ordering::Less => {
                        // Si el de él está desactualizado, le mando la info para que lo actualice
                        updated_info.insert(my_digest, my_state.application_state.clone());
                    }
                    std::cmp::Ordering::Greater => {
                        // Si el mío está desactualizado, le mando mi digest
                        stale_digests.push(my_digest);
                    }
                    std::cmp::Ordering::Equal => continue,
                }
            } else {
                // si no tengo info de ese nodo, entonces mi digest está desactualizado
                // le mando el digest correspondiente a ese nodo con version y generacion en 0
                stale_digests.push(Digest::from_heartbeat_state(
                    digest.address,
                    &HeartbeatState::new(0, 0),
                ));
            }
        }

        Ack {
            stale_digests,
            updated_info,
        }
    }

    /// Handles an Ack message and returns the corresponding Ack2 message.
    pub fn handle_ack(&mut self, ack: &Ack) -> Ack2 {
        let mut updated_info = BTreeMap::new();

        for digest in &ack.stale_digests {
            let my_state = self
                .endpoints_state
                .get(&digest.address)
                .expect("There MUST be an endpoint state for an IP received in an ACK.");

            let my_digest = Digest::from_heartbeat_state(digest.address, &my_state.heartbeat_state);

            if digest.generation == my_digest.generation && digest.version == my_digest.version {
                continue;
            }

            match digest
                .get_heartbeat_state()
                .cmp(&my_digest.get_heartbeat_state())
            {
                std::cmp::Ordering::Less => {
                    // Si el de él está desactualizado, le mando la info para que lo actualice
                    updated_info.insert(my_digest, my_state.application_state.clone());
                }
                std::cmp::Ordering::Greater => {
                    // Si el mío está desactualizado, hubo un problema, se debería haber mandado
                    // el digest en el Syn
                    panic!("Something went wrong, a digest incoming in an ACK should never be greater than the local state");
                }
                std::cmp::Ordering::Equal => continue,
            }
        }

        for (digest, info) in &ack.updated_info {
            let _my_state = self
                .endpoints_state
                .get(&digest.address)
                .expect("There MUST be an endpoint state for an IP received in an ACK.");

            // El ACK debe contener info más actualizada que la mía
            //assert!(digest.get_heartbeat_state() > my_state.heartbeat_state);

            // la actualizo
            self.endpoints_state.insert(
                digest.address,
                EndpointState::new(
                    info.clone(),
                    HeartbeatState::new(digest.generation, digest.version),
                ),
            );
        }

        Ack2 { updated_info }
    }

    /// Handles an Ack2 message and updates the local state.
    pub fn handle_ack2(&mut self, ack2: &Ack2) {
        for (digest, info) in &ack2.updated_info {
            if let Some(_my_state) = self.endpoints_state.get(&digest.address) {
                // El ACK2 debe contener info más actualizada que la mía
                //assert!(digest.get_heartbeat_state() > my_state.heartbeat_state);

                self.endpoints_state.insert(
                    digest.address,
                    EndpointState::new(
                        info.clone(),
                        HeartbeatState::new(digest.generation, digest.version),
                    ),
                );
            } else {
                self.endpoints_state.insert(
                    digest.address,
                    EndpointState::new(
                        info.clone(),
                        HeartbeatState::new(digest.generation, digest.version),
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use messages::Payload;
    use std::str::FromStr;
    use structures::application_state::ApplicationState;

    #[test]
    fn incoming_syn_same_generation_lower_version() {
        // if the incoming version is lower, the returned ack
        // should contain the updated info
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 3, 2)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                HeartbeatState::new(3, 3),
            ),
        )]);

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert!(ack.stale_digests.is_empty());
        assert_eq!(
            ack.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        );
    }

    #[test]
    fn incoming_syn_lower_generation() {
        // if the incoming generation is lower, the returned ack
        // shold containe the updated info
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 2, 5)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                HeartbeatState::new(3, 3),
            ),
        )]);

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert!(ack.stale_digests.is_empty());
        assert_eq!(
            ack.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        );
    }

    #[test]
    fn incoming_syn_higher_generation() {
        // if the incoming generation is higher, the return ack
        // should contain the local stale digest
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 7, 3)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                HeartbeatState::new(4, 8),
            ),
        )]);

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert_eq!(
            ack.stale_digests,
            vec![Digest::from_heartbeat_state(
                ip,
                &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
            )]
        );
        assert!(ack.updated_info.is_empty());
    }

    #[test]
    fn incoming_syn_higher_version_same_generation() {
        // if the incoming digest version is higher, the return ack
        // should contain the local stale digest
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let syn = Syn::new(vec![Digest::new(ip, 7, 3)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert_eq!(
            ack.stale_digests,
            vec![Digest::from_heartbeat_state(
                ip,
                &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
            )]
        );
        assert!(ack.updated_info.is_empty(),);
    }

    #[test]
    fn incoming_ack_stale_digest_lower_generation() {
        // if there is incoming stale digest in the ack, the returned ack2 should
        // contain the updated state
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(vec![Digest::new(ip, 6, 2)], BTreeMap::new());

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        )
    }

    #[test]
    fn incoming_ack_stale_digest_same_generation_lower_version() {
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(vec![Digest::new(ip, 7, 2)], BTreeMap::new());

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                HeartbeatState::new(7, 3),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        )
    }

    #[test]
    fn incoming_ack_stale_digest_lower_generation_greater_version() {
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(vec![Digest::new(ip, 6, 1)], BTreeMap::new());

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                HeartbeatState::new(7, 3),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip,
                    &gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        )
    }

    #[test]
    fn incoming_ack_updated_info_higher_generation_higher_version() {
        // if there is incoming updated info in the ack, the local state
        // should be updated
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(
            Vec::new(),
            BTreeMap::from([(
                Digest::new(ip, 8, 7),
                ApplicationState::new(NodeStatus::Leaving, 9, Schema::default()),
            )]),
        );

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert!(ack2.updated_info.is_empty());
        assert_eq!(
            gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state,
            HeartbeatState::new(8, 7)
        );
        assert_eq!(
            gossiper.endpoints_state.get(&ip).unwrap().application_state,
            ApplicationState::new(NodeStatus::Leaving, 9, Schema::default())
        );
    }

    #[test]
    fn incoming_ack_updated_info_same_generation_higher_version() {
        // if there is incoming updated info in the ack, the local state
        // should be updated
        let ip = Ipv4Addr::from_str("127.0.0.2").unwrap();

        let ack = Ack::new(
            Vec::new(),
            BTreeMap::from([(
                Digest::new(ip, 7, 7),
                ApplicationState::new(NodeStatus::Leaving, 9, Schema::default()),
            )]),
        );

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([(
            ip,
            EndpointState::new(
                ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                HeartbeatState::new(7, 2),
            ),
        )]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        assert!(ack2.updated_info.is_empty());
        assert_eq!(
            gossiper.endpoints_state.get(&ip).unwrap().heartbeat_state,
            HeartbeatState::new(7, 7)
        );
        assert_eq!(
            gossiper.endpoints_state.get(&ip).unwrap().application_state,
            ApplicationState::new(NodeStatus::Leaving, 9, Schema::default())
        );
    }

    #[test]
    fn incoming_ack_updated_info_and_stale_digest() {
        let ip_1 = Ipv4Addr::from_str("127.0.0.2").unwrap();
        let ip_2 = Ipv4Addr::from_str("127.0.0.7").unwrap();

        // ack with one stale digest (ip_1) and one updated info (ip_2)
        let ack = Ack::new(
            vec![Digest::new(ip_1, 6, 1)],
            BTreeMap::from([(
                Digest::new(ip_2, 8, 7),
                ApplicationState::new(NodeStatus::Removing, 9, Schema::default()),
            )]),
        );

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
            (
                ip_1,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            ),
            (
                ip_2,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            ),
        ]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack2 = gossiper.handle_ack(&ack);

        // the ack2 should contain the updated info for ip_1
        assert_eq!(
            ack2.updated_info,
            BTreeMap::from([(
                Digest::from_heartbeat_state(
                    ip_1,
                    &gossiper.endpoints_state.get(&ip_1).unwrap().heartbeat_state
                ),
                gossiper
                    .endpoints_state
                    .get(&ip_1)
                    .unwrap()
                    .application_state
                    .clone()
            )])
        );
        // the local_state should be updated for ip_2
        assert_eq!(
            gossiper.endpoints_state.get(&ip_2).unwrap().heartbeat_state,
            HeartbeatState::new(8, 7)
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip_2)
                .unwrap()
                .application_state,
            ApplicationState::new(NodeStatus::Removing, 9, Schema::default())
        );
    }

    #[test]
    fn incoming_ack2_updated_info() {
        let ip_1 = Ipv4Addr::from_str("127.0.0.2").unwrap();
        let ip_2 = Ipv4Addr::from_str("127.0.0.7").unwrap();

        let ack2 = Ack2::new(BTreeMap::from([
            (
                Digest::new(ip_1, 7, 6),
                ApplicationState::new(NodeStatus::Normal, 7, Schema::default()),
            ),
            (
                Digest::new(ip_2, 8, 7),
                ApplicationState::new(NodeStatus::Removing, 9, Schema::default()),
            ),
        ]));

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
            (
                ip_1,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            ),
            (
                ip_2,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            ),
        ]);

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        gossiper.handle_ack2(&ack2);

        // the local_state should be updated for both ips
        assert_eq!(
            gossiper.endpoints_state.get(&ip_1).unwrap().heartbeat_state,
            HeartbeatState::new(7, 6)
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip_1)
                .unwrap()
                .application_state,
            ApplicationState::new(NodeStatus::Normal, 7, Schema::default())
        );
        assert_eq!(
            gossiper.endpoints_state.get(&ip_2).unwrap().heartbeat_state,
            HeartbeatState::new(8, 7)
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip_2)
                .unwrap()
                .application_state,
            ApplicationState::new(NodeStatus::Removing, 9, Schema::default())
        );
    }

    #[test]
    fn new_digest_in_syn() {
        let new_ip = Ipv4Addr::from_str("127.0.0.7").unwrap();

        let syn = Syn::new(vec![Digest::new(new_ip, 1, 1)]);

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::new();

        let gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let ack = gossiper.handle_syn(&syn);

        assert_eq!(ack.stale_digests, vec![Digest::new(new_ip, 0, 0)]);
        assert!(ack.updated_info.is_empty(),);
    }

    #[test]
    fn new_state_in_ack() {
        let new_ip = Ipv4Addr::from_str("127.0.0.7").unwrap();

        let ack = Ack2::new(BTreeMap::from([(
            Digest::new(new_ip, 1, 1),
            ApplicationState::new(NodeStatus::Bootstrap, 1, Schema::default()),
        )]));

        let local_state: HashMap<Ipv4Addr, EndpointState> = HashMap::new();

        let mut gossiper = Gossiper {
            endpoints_state: local_state.clone(),
        };

        let _ = gossiper.handle_ack2(&ack);

        assert_eq!(
            gossiper
                .endpoints_state
                .get(&new_ip)
                .unwrap()
                .heartbeat_state,
            HeartbeatState::new(1, 1)
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&new_ip)
                .unwrap()
                .application_state,
            ApplicationState::new(NodeStatus::Bootstrap, 1, Schema::default())
        );
    }

    #[test]
    fn test_gossip_flow() {
        let client_ip = Ipv4Addr::from_str("127.0.0.1").unwrap();
        let server_ip = Ipv4Addr::from_str("127.0.0.2").unwrap();
        let another_ip = Ipv4Addr::from_str("127.0.0.3").unwrap();
        let new_ip = Ipv4Addr::from_str("127.0.0.4").unwrap();

        let client_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
            (
                client_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            ),
            (
                server_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 6, Schema::default()),
                    HeartbeatState::new(8, 3),
                ),
            ),
            (
                another_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 8, Schema::default()),
                    HeartbeatState::new(4, 1),
                ),
            ),
            (
                new_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 1, Schema::default()),
                    HeartbeatState::new(1, 1),
                ),
            ),
        ]);

        let server_state: HashMap<Ipv4Addr, EndpointState> = HashMap::from([
            (
                client_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default()),
                    HeartbeatState::new(6, 1),
                ),
            ),
            (
                server_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 7, Schema::default()),
                    HeartbeatState::new(8, 8),
                ),
            ),
            (
                another_ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Normal, 8, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            ),
        ]);

        // client sends syn to server
        let syn = Syn::new(vec![
            Digest::new(client_ip, 7, 2),
            Digest::new(server_ip, 8, 3),
            Digest::new(another_ip, 4, 1),
            Digest::new(new_ip, 1, 1),
        ]);

        let mut gossiper_server = Gossiper {
            endpoints_state: server_state.clone(),
        };

        // server handles syn and sends ack to client
        let ack = gossiper_server.handle_syn(&syn);

        assert_eq!(
            ack,
            Ack::new(
                vec![Digest::new(client_ip, 6, 1), Digest::new(new_ip, 0, 0)],
                BTreeMap::from([
                    (
                        Digest::new(server_ip, 8, 8),
                        ApplicationState::new(NodeStatus::Normal, 7, Schema::default())
                    ),
                    (
                        Digest::new(another_ip, 7, 2),
                        ApplicationState::new(NodeStatus::Normal, 8, Schema::default())
                    ),
                ])
            )
        );

        let mut gossiper_client = Gossiper {
            endpoints_state: client_state.clone(),
        };

        // client handles ack, updates its state and sends ack2 to server
        let ack2 = gossiper_client.handle_ack(&ack);

        assert_eq!(
            ack2,
            Ack2::new(BTreeMap::from([
                (
                    Digest::new(client_ip, 7, 2),
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default())
                ),
                (
                    Digest::new(new_ip, 1, 1),
                    ApplicationState::new(NodeStatus::Bootstrap, 1, Schema::default())
                ),
            ]))
        );

        // server handles ack2 and updates its state
        gossiper_server.handle_ack2(&ack2);

        assert_eq!(
            gossiper_server.endpoints_state,
            gossiper_client.endpoints_state
        );
    }

    #[test]
    fn string_as_bytes() {
        let syn = Syn {
            digests: vec![
                Digest::new(Ipv4Addr::new(127, 0, 0, 1), 1, 15),
                Digest::new(Ipv4Addr::new(127, 0, 0, 2), 10, 15),
                Digest::new(Ipv4Addr::new(127, 0, 0, 3), 3, 15),
            ],
        };

        let gossip_msg = GossipMessage {
            from: Ipv4Addr::new(127, 0, 0, 1),
            payload: Payload::Syn(syn),
        };

        let syn_bytes = gossip_msg.as_bytes();
        let string = format!("{}", std::str::from_utf8(syn_bytes.as_slice()).unwrap());
        let string_bytes = string.as_bytes();

        assert_eq!(syn_bytes, string_bytes);
    }

    #[test]
    fn change_status() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::from([(
                ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default()),
                    HeartbeatState::default(),
                ),
            )]),
        };

        gossiper.change_status(ip, NodeStatus::Normal).unwrap();

        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip)
                .unwrap()
                .application_state
                .status,
            NodeStatus::Normal
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip)
                .unwrap()
                .application_state
                .version,
            3
        );
    }

    #[test]
    fn change_status_non_existent() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::new(),
        };

        let result = gossiper.change_status(ip, NodeStatus::Normal);

        assert!(matches!(result, Err(GossipError::NoEndpointStateForIp)));
    }

    #[test]
    fn remove_keyspace() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::from([(
                ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            )]),
        };

        gossiper.remove_keyspace(ip, "keyspace").unwrap();

        assert!(gossiper
            .endpoints_state
            .get(&ip)
            .unwrap()
            .application_state
            .schema
            .keyspaces
            .is_empty());

        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip)
                .unwrap()
                .application_state
                .version,
            3
        );
    }

    #[test]
    fn remove_keyspace_non_existent_ip() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::new(),
        };

        let result = gossiper.remove_keyspace(ip, "keyspace");

        assert!(matches!(result, Err(GossipError::NoEndpointStateForIp)));
    }

    #[test]
    fn add_keyspace() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::from([(
                ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            )]),
        };

        gossiper
            .add_keyspace(
                ip,
                CreateKeyspace {
                    name: "keyspace".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip)
                .unwrap()
                .application_state
                .schema
                .keyspaces,
            HashMap::from([(
                "keyspace".to_string(),
                KeyspaceSchema::new(
                    CreateKeyspace {
                        name: "keyspace".to_string(),
                        ..Default::default()
                    },
                    Vec::new()
                )
            )])
        );
        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip)
                .unwrap()
                .application_state
                .version,
            3
        );
    }

    #[test]
    fn add_keyspace_non_existent_ip() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::new(),
        };

        let result = gossiper.add_keyspace(ip, CreateKeyspace::default());

        assert!(matches!(result, Err(GossipError::NoEndpointStateForIp)));
    }

    #[test]
    fn remove_table() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::from([(
                ip,
                EndpointState::new(
                    ApplicationState::new(
                        NodeStatus::Bootstrap,
                        2,
                        Schema {
                            keyspaces: HashMap::from([(
                                "keyspace".to_string(),
                                KeyspaceSchema {
                                    inner: CreateKeyspace {
                                        name: "keyspace".to_string(),
                                        ..Default::default()
                                    },
                                    tables: vec![TableSchema {
                                        inner: CreateTable {
                                            name: "table1".to_string(),
                                            keyspace_used_name: "keyspace".to_string(),
                                            ..Default::default()
                                        },
                                    }],
                                },
                            )]),
                            ..Default::default()
                        },
                    ),
                    HeartbeatState::new(7, 2),
                ),
            )]),
        };

        gossiper.remove_table(ip, "keyspace", "table1").unwrap();

        assert!(gossiper
            .endpoints_state
            .get(&ip)
            .unwrap()
            .application_state
            .schema
            .keyspaces
            .get("keyspace")
            .unwrap()
            .tables
            .is_empty());

        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip)
                .unwrap()
                .application_state
                .version,
            3
        );
    }

    #[test]
    fn remove_table_non_existent_ip() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::new(),
        };

        let result = gossiper.remove_table(ip, "keyspace", "table1");

        assert!(matches!(result, Err(GossipError::NoEndpointStateForIp)));
    }

    #[test]
    fn remove_table_non_existent_keyspace() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::from([(
                ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            )]),
        };

        let result = gossiper.remove_table(ip, "keyspace", "table1");

        assert!(matches!(result, Err(GossipError::NoSuchKeyspace)));
    }

    #[test]
    fn add_table() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::from([(
                ip,
                EndpointState::new(
                    ApplicationState::new(
                        NodeStatus::Bootstrap,
                        2,
                        Schema {
                            keyspaces: HashMap::from([(
                                "keyspace".to_string(),
                                KeyspaceSchema {
                                    inner: CreateKeyspace {
                                        name: "keyspace".to_string(),
                                        ..Default::default()
                                    },
                                    tables: Vec::new(),
                                },
                            )]),
                            timestamp: 0,
                        },
                    ),
                    HeartbeatState::new(7, 2),
                ),
            )]),
        };

        gossiper
            .add_table(
                ip,
                CreateTable {
                    name: "table".to_string(),
                    keyspace_used_name: "keyspace".to_string(),
                    if_not_exists_clause: false,
                    columns: Vec::new(),
                    clustering_columns_in_order: Vec::new(),
                },
                "keyspace",
            )
            .unwrap();

        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip)
                .unwrap()
                .application_state
                .schema,
            Schema {
                keyspaces: HashMap::from([(
                    "keyspace".to_string(),
                    KeyspaceSchema {
                        inner: CreateKeyspace {
                            name: "keyspace".to_string(),
                            ..Default::default()
                        },
                        tables: vec![TableSchema {
                            inner: CreateTable {
                                name: "table".to_string(),
                                keyspace_used_name: "keyspace".to_string(),
                                if_not_exists_clause: false,
                                columns: Vec::new(),
                                clustering_columns_in_order: Vec::new(),
                            },
                        }],
                    }
                )]),
                // copy timestamp from insertion
                timestamp: gossiper
                    .endpoints_state
                    .get(&ip)
                    .unwrap()
                    .application_state
                    .schema
                    .timestamp
            }
        );

        assert_eq!(
            gossiper
                .endpoints_state
                .get(&ip)
                .unwrap()
                .application_state
                .version,
            3
        );
    }

    #[test]
    fn add_table_non_existent_ip() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::new(),
        };

        let result = gossiper.add_table(
            ip,
            CreateTable {
                name: "table".to_string(),
                keyspace_used_name: "keyspace".to_string(),
                if_not_exists_clause: false,
                columns: Vec::new(),
                clustering_columns_in_order: Vec::new(),
            },
            "keyspace",
        );

        assert!(matches!(result, Err(GossipError::NoEndpointStateForIp)));
    }

    #[test]
    fn add_table_non_existent_keyspace() {
        let ip = Ipv4Addr::new(127, 0, 0, 1);

        let mut gossiper = Gossiper {
            endpoints_state: HashMap::from([(
                ip,
                EndpointState::new(
                    ApplicationState::new(NodeStatus::Bootstrap, 2, Schema::default()),
                    HeartbeatState::new(7, 2),
                ),
            )]),
        };

        let result = gossiper.add_table(
            ip,
            CreateTable {
                name: "table".to_string(),
                keyspace_used_name: "keyspace".to_string(),
                if_not_exists_clause: false,
                columns: Vec::new(),
                clustering_columns_in_order: Vec::new(),
            },
            "keyspace",
        );

        assert!(matches!(result, Err(GossipError::NoSuchKeyspace)));
    }
}
