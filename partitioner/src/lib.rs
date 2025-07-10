use errors::PartitionerError;
use murmur3::murmur3_32;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::net::Ipv4Addr;
pub mod errors;

#[derive(Clone)]
pub struct Partitioner {
    nodes: BTreeMap<u64, Ipv4Addr>,
}

impl Default for Partitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner {
    /// Creates a new, empty `Partitioner`.
    ///
    /// # Returns
    /// * `Partitioner` - An instance of `Partitioner` with no nodes initially.
    pub fn new() -> Self {
        Partitioner {
            nodes: BTreeMap::new(),
        }
    }

    /// Hashes a value using the `murmur3_32` algorithm and returns the hash as a `u64`.
    ///
    /// # Parameters
    /// - `value`: The value to hash, implemented as a reference to an array of bytes.
    ///
    /// # Returns
    /// * `Result<u64, PartitionerError>` - Returns the hash value as `u64` on success, or `PartitionerError::HashError` on failure.
    fn hash_value<T: AsRef<[u8]>>(value: T) -> Result<u64, PartitionerError> {
        let mut hasher = Cursor::new(value);
        murmur3_32(&mut hasher, 0)
            .map(|hash| hash as u64)
            .map_err(|_| PartitionerError::HashError)
    }

    /// Adds a new node to the partitioner using its IP address.
    ///
    /// # Parameters
    /// - `ip`: The IP address of the node to add.
    ///
    /// # Returns
    /// * `Result<(), PartitionerError>` - Returns `Ok(())` if the node is successfully added, or
    ///   `PartitionerError::NodeAlreadyExists` if the node already exists in the partitioner.
    ///
    /// # Errors
    /// - `PartitionerError::HashError` - If there is an issue hashing the IP address.
    /// - `PartitionerError::NodeAlreadyExists` - If the node's hash already exists in the partitioner.
    pub fn add_node(&mut self, ip: Ipv4Addr) -> Result<(), PartitionerError> {
        let hash = Self::hash_value(ip.to_string())?;
        if self.nodes.contains_key(&hash) {
            return Err(PartitionerError::NodeAlreadyExists);
        }
        self.nodes.insert(hash, ip);

        Ok(())
    }

    /// Removes a node from the partitioner based on its IP address.
    ///
    /// # Parameters
    /// - `ip`: The IP address of the node to remove.
    ///
    /// # Returns
    /// * `Result<Ipv4Addr, PartitionerError>` - Returns the IP address of the removed node,
    ///   or `PartitionerError::NodeNotFound` if the node does not exist.
    ///
    /// # Errors
    /// - `PartitionerError::HashError` - If there is an issue hashing the IP address.
    /// - `PartitionerError::NodeNotFound` - If the node is not found in the partitioner.
    pub fn remove_node(&mut self, ip: Ipv4Addr) -> Result<Ipv4Addr, PartitionerError> {
        let hash = Self::hash_value(ip.to_string())?;

        self.nodes
            .remove(&hash)
            .ok_or(PartitionerError::NodeNotFound)
    }

    pub fn node_already_in_partitioner(&mut self, ip: &Ipv4Addr) -> Result<bool, PartitionerError> {
        let hash = Self::hash_value(ip.to_string())?;

        if self.nodes.contains_key(&hash) {
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Retrieves the IP address of the node responsible for a given value.
    ///
    /// # Parameters
    /// - `value`: The value used to determine the responsible node.
    ///
    /// # Returns
    /// * `Result<Ipv4Addr, PartitionerError>` - Returns the IP address of the node responsible
    ///   for the given value, or `PartitionerError::EmptyPartitioner` if no nodes are present.
    ///
    /// # Errors
    /// - `PartitionerError::HashError` - If there is an issue hashing the value.
    /// - `PartitionerError::EmptyPartitioner` - If the partitioner contains no nodes.
    pub fn get_ip<T: AsRef<[u8]>>(&self, value: T) -> Result<Ipv4Addr, PartitionerError> {
        let hash = Self::hash_value(value)?;
        if self.nodes.is_empty() {
            return Err(PartitionerError::EmptyPartitioner);
        }

        match self.nodes.range(hash..).next() {
            Some((_key, addr)) => Ok(*addr),
            None => self
                .nodes
                .values()
                .next()
                .cloned()
                .ok_or(PartitionerError::EmptyPartitioner),
        }
    }

    /// Returns a list of all nodes' IP addresses within the partitioner.
    ///
    /// # Returns
    /// * `Vec<Ipv4Addr>` - A vector of IP addresses of all nodes.
    pub fn get_nodes(&self) -> Vec<Ipv4Addr> {
        self.nodes.values().cloned().collect()
    }

    /// Checks if a node with the given IP address exists in the partitioner.
    ///
    /// # Parameters
    /// - `ip`: The IP address to check for existence in the partitioner.
    ///
    /// # Returns
    /// * `bool` - Returns `true` if the node exists, `false` otherwise.
    pub fn contains_node(&self, ip: &Ipv4Addr) -> bool {
        let hash = Self::hash_value(ip.to_string()).unwrap_or_default();
        self.nodes.contains_key(&hash)
    }

    /// Retrieves the IP addresses of the next `n` successor nodes in the partitioner,
    /// starting from a given IP address and skipping the starting IP address.
    ///
    /// # Parameters
    /// - `ip`: The starting IP address.
    /// - `n`: The number of successors to retrieve.
    ///
    /// # Returns
    /// * `Result<Vec<Ipv4Addr>, PartitionerError>` - Returns a vector of successor IP addresses.
    ///
    /// # Errors
    /// - `PartitionerError::EmptyPartitioner` - If there are no nodes in the partitioner.
    /// - `PartitionerError::HashError` - If there is an issue hashing the starting IP address.
    pub fn get_n_successors(
        &self,
        ip: Ipv4Addr,
        n: usize,
    ) -> Result<Vec<Ipv4Addr>, PartitionerError> {
        if self.nodes.is_empty() {
            return Err(PartitionerError::EmptyPartitioner);
        }

        let hash = Self::hash_value(ip.to_string())?;
        let mut successors = Vec::new();

        for (_key, addr) in self.nodes.range(hash..) {
            if successors.len() == n {
                break;
            }
            if *addr != ip {
                successors.push(*addr);
            }
        }

        if successors.len() < n {
            for (_key, addr) in self.nodes.iter() {
                if successors.len() == n {
                    break;
                }
                if *addr != ip && !successors.contains(addr) {
                    successors.push(*addr);
                }
            }
        }
        Ok(successors)
    }
}

impl fmt::Debug for Partitioner {
    /// Custom `Debug` implementation to display partitioner's nodes in a `->` format.
    ///
    /// # Examples
    /// * For a partitioner with nodes "192.168.0.1" and "192.168.0.2", the debug output
    ///   will display as `"192.168.0.1 -> 192.168.0.2"`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let addresses: Vec<String> = self.nodes.values().map(|addr| addr.to_string()).collect();
        if !addresses.is_empty() {
            write!(f, "{}", addresses.join(" -> "))
        } else {
            write!(f, "No nodes available")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_add_and_get_nodes() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();

        let nodes = partitioner.get_nodes();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&Ipv4Addr::new(192, 168, 0, 1)));
        assert!(nodes.contains(&Ipv4Addr::new(192, 168, 0, 2)));
    }

    #[test]
    fn test_get_n_successors_no_duplicates_skip_current() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 3)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 4)).unwrap();

        let starting_ip = Ipv4Addr::new(192, 168, 0, 2);
        let successors = partitioner.get_n_successors(starting_ip, 2).unwrap();
        let unique_successors: std::collections::HashSet<_> = successors.iter().collect();

        assert_eq!(
            unique_successors.len(),
            successors.len(),
            "Expected unique successors without duplicates, got {:?}",
            successors
        );
        assert!(
            !successors.contains(&starting_ip),
            "Expected successors to skip the starting node, but it was included"
        );
        assert!(
            successors.len() <= 2,
            "Expected at most 2 successors, but got {:?}",
            successors
        );
    }

    #[test]
    fn test_debug_trait() {
        let mut partitioner = Partitioner::new();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 1)).unwrap();
        partitioner.add_node(Ipv4Addr::new(192, 168, 0, 2)).unwrap();

        let debug_string = format!("{:?}", partitioner);
        assert!(
            debug_string.contains("192.168.0.1 -> 192.168.0.2")
                || debug_string.contains("192.168.0.2 -> 192.168.0.1"),
            "Debug output mismatch: got {}",
            debug_string
        );
    }
}
