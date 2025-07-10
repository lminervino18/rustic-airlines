use super::{errors::StorageEngineError, StorageEngine};

impl StorageEngine {
    /// Creates a keyspace in the storage location.
    ///
    /// This function creates a directory for the specified keyspace name, as well as
    /// a `replication` subdirectory within it.
    ///
    /// # Arguments
    /// - `name`: The name of the keyspace to create.
    ///
    /// # Returns
    /// - `Ok(())` if the keyspace and its subdirectory are successfully created.
    /// - `Err(StorageEngineError::DirectoryCreationFailed)` if there is an issue creating the directories.
    ///
    /// # Errors
    /// This function will return an error if the directory or any subdirectory cannot be created.

    pub fn create_keyspace(&self, name: &str) -> Result<(), StorageEngineError> {
        // Generate the folder name where the keyspace will be stored
        let ip_str = self.ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = self.root.join(&keyspace_folder).join(name);

        // Create the keyspace folder if it doesn't exist
        if let Err(_) = std::fs::create_dir_all(&keyspace_path) {
            return Err(StorageEngineError::DirectoryCreationFailed);
        }

        // Create the replication folder inside the keyspace folder
        let replication_path = keyspace_path.join("replication");
        if let Err(_) = std::fs::create_dir_all(&replication_path) {
            return Err(StorageEngineError::DirectoryCreationFailed);
        }

        Ok(())
    }

    /// Drops a keyspace from the storage location.
    ///
    /// This function removes the directory associated with the specified keyspace name.
    ///
    /// # Arguments
    /// - `name`: The name of the keyspace to delete.
    /// - `ip`: The IP address used to locate the keyspace folder.
    ///
    /// # Returns
    /// - `Ok(())` if the keyspace directory is successfully removed.
    /// - `Err(StorageEngineError::FileDeletionFailed)` if there is an issue deleting the directory.
    ///
    /// # Errors
    /// This function will return an error if the keyspace directory cannot be removed.

    pub fn drop_keyspace(&self, name: &str, ip: &str) -> Result<(), StorageEngineError> {
        // Generate the folder name where the keyspace is stored
        let ip_str = ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = self.root.join(&keyspace_folder).join(name);

        // Remove the keyspace folder
        if let Err(_) = std::fs::remove_dir_all(&keyspace_path) {
            return Err(StorageEngineError::FileDeletionFailed);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_create_keyspace() {
        let root = PathBuf::from("/tmp/storage_test");
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());
        let keyspace_name = "test_keyspace";

        // Ensure the environment is clean
        let ip_str = ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = root.join(&keyspace_folder).join(keyspace_name);

        if keyspace_path.exists() {
            fs::remove_dir_all(&keyspace_path).unwrap();
        }

        // Call the function
        let result = storage.create_keyspace(keyspace_name);
        assert!(result.is_ok(), "Failed to create keyspace");

        // Check that the keyspace directory and replication folder were created
        assert!(keyspace_path.exists(), "Keyspace directory was not created");
        assert!(
            keyspace_path.join("replication").exists(),
            "Replication folder was not created"
        );

        // Clean up after the test
        fs::remove_dir_all(&keyspace_path).unwrap();
    }

    #[test]
    fn test_drop_keyspace() {
        let root = PathBuf::from("/tmp/storage_test");
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());
        let keyspace_name = "test_keyspace";

        // Prepare the environment by creating a keyspace
        let ip_str = ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = root.join(&keyspace_folder).join(keyspace_name);

        fs::create_dir_all(&keyspace_path).unwrap();
        assert!(
            keyspace_path.exists(),
            "Failed to set up test environment: keyspace directory does not exist"
        );

        // Call the function
        let result = storage.drop_keyspace(keyspace_name, &ip);
        assert!(result.is_ok(), "Failed to drop keyspace");

        // Check that the keyspace directory was deleted
        assert!(
            !keyspace_path.exists(),
            "Keyspace directory was not deleted"
        );

        // Clean up the environment
        if root.join(&keyspace_folder).exists() {
            fs::remove_dir_all(root.join(&keyspace_folder)).unwrap();
        }
    }
}
