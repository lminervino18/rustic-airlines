use std::fs::{self};
use std::path::PathBuf;

pub mod data_redistribution;
pub mod delete;
pub mod errors;
pub mod insert;
pub mod keyspace_operations;
pub mod select;
pub mod table_operations;
pub mod update;
use errors::StorageEngineError;

pub struct StorageEngine {
    root: PathBuf,
    ip: String,
}

impl StorageEngine {
    /// Creates a new instance of `StorageEngine`.
    ///
    /// # Arguments
    /// - `root`: The base path where directories will be managed.
    /// - `ip`: The IP address used to generate unique identifiers for keyspace directories
    ///

    pub fn new(root: PathBuf, ip: String) -> Self {
        Self { root, ip }
    }

    /// Resets the keyspace directories associated with the storage engine.
    ///
    /// If the directory for keyspaces already exists, it will be completely deleted
    /// and recreated. If it does not exist, it will be created.
    ///
    /// # Returns
    /// - `Ok(())` on success.
    /// - `Err(StorageEngineError)` if there is an issue deleting or creating the directories.
    /// # Note
    /// - Deleted directories cannot be recovered.

    pub fn reset_folders(&self) -> Result<(), StorageEngineError> {
        let ip_str = self.ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        let keyspace_path = self.root.join(&keyspace_folder);

        // Check if the folder exists and delete it if it does
        if keyspace_path.exists() {
            fs::remove_dir_all(&keyspace_path)
                .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;
        }

        // Create the folder
        fs::create_dir_all(&keyspace_path)
            .map_err(|_| StorageEngineError::DirectoryCreationFailed)?;

        Ok(())
    }

    fn get_keyspace_path(&self, keyspace: &str) -> PathBuf {
        let ip_str = self.ip.replace(".", "_");
        let keyspace_folder = format!("keyspaces_of_{}", ip_str);
        self.root.join(&keyspace_folder).join(keyspace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_storage_engine_new() {
        let root = PathBuf::from("/tmp/storage");
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        assert_eq!(storage.root, root);
        assert_eq!(storage.ip, ip);
    }

    #[test]
    fn test_reset_folders() {
        let root = PathBuf::from("/tmp/storage_test");
        let ip = "127.0.0.1".to_string();
        let storage = StorageEngine::new(root.clone(), ip.clone());

        // Asegurarse de que el directorio inicial no existe
        let keyspace_folder = format!("keyspaces_of_{}", ip.replace(".", "_"));
        let keyspace_path = root.join(&keyspace_folder);

        if keyspace_path.exists() {
            fs::remove_dir_all(&keyspace_path).unwrap();
        }

        // Ejecutar la función de reinicio
        let result = storage.reset_folders();
        assert!(result.is_ok());

        // Verificar que la carpeta fue creada correctamente
        assert!(keyspace_path.exists());

        // Limpiar después de la prueba
        fs::remove_dir_all(&keyspace_path).unwrap();
    }
}
