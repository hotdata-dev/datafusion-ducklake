//! Encryption support for reading encrypted Parquet files in DuckLake.
//!
//! This module provides integration with Parquet Modular Encryption (PME) using
//! encryption keys stored in the DuckLake catalog.

use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "encryption")]
use async_trait::async_trait;
#[cfg(feature = "encryption")]
use arrow::datatypes::SchemaRef;
#[cfg(feature = "encryption")]
use datafusion::common::config::EncryptionFactoryOptions;
#[cfg(feature = "encryption")]
use datafusion::error::Result;
#[cfg(feature = "encryption")]
use datafusion::execution::parquet_encryption::EncryptionFactory;
#[cfg(feature = "encryption")]
use object_store::path::Path;
#[cfg(feature = "encryption")]
use parquet::encryption::decrypt::FileDecryptionProperties;
#[cfg(feature = "encryption")]
use parquet::encryption::encrypt::FileEncryptionProperties;

/// DuckLake encryption factory that provides decryption keys from the catalog.
///
/// This factory maintains a mapping of file paths to their encryption keys,
/// populated from the DuckLake catalog metadata.
#[derive(Debug, Clone)]
pub struct DuckLakeEncryptionFactory {
    /// Map of file paths to their encryption keys (base64 or hex encoded)
    file_keys: Arc<HashMap<String, String>>,
}

impl DuckLakeEncryptionFactory {
    /// Create a new encryption factory with the given file-to-key mapping.
    ///
    /// # Arguments
    /// * `file_keys` - Map of file paths to encryption keys
    pub fn new(file_keys: HashMap<String, String>) -> Self {
        Self {
            file_keys: Arc::new(file_keys),
        }
    }

    /// Create an empty encryption factory (for tables with no encrypted files).
    pub fn empty() -> Self {
        Self {
            file_keys: Arc::new(HashMap::new()),
        }
    }

    /// Check if any files have encryption keys.
    pub fn has_encrypted_files(&self) -> bool {
        !self.file_keys.is_empty()
    }

    /// Decode an encryption key from its stored format.
    ///
    /// DuckLake stores keys as strings - this function handles decoding.
    /// Currently assumes base64 encoding, but can be extended for other formats.
    #[cfg(feature = "encryption")]
    fn decode_key(key: &str) -> Result<Vec<u8>> {
        use base64::Engine;
        use datafusion::error::DataFusionError;

        // Try base64 first (most common for DuckLake)
        if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(key) {
            return Ok(decoded);
        }

        // Try hex encoding as fallback
        if let Ok(decoded) = hex::decode(key) {
            return Ok(decoded);
        }

        // If it's exactly 16 or 32 bytes, treat as raw key
        let bytes = key.as_bytes();
        if bytes.len() == 16 || bytes.len() == 32 {
            return Ok(bytes.to_vec());
        }

        Err(DataFusionError::Execution(format!(
            "Unable to decode encryption key: not valid base64, hex, or raw key format"
        )))
    }
}

#[cfg(feature = "encryption")]
#[async_trait]
impl EncryptionFactory for DuckLakeEncryptionFactory {
    async fn get_file_encryption_properties(
        &self,
        _config: &EncryptionFactoryOptions,
        _schema: &SchemaRef,
        _file_path: &Path,
    ) -> Result<Option<FileEncryptionProperties>> {
        // DuckLake is read-only for now, so we don't support writing encrypted files
        Ok(None)
    }

    async fn get_file_decryption_properties(
        &self,
        _config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> Result<Option<FileDecryptionProperties>> {
        let path_str = file_path.to_string();

        // Try to find the key for this file path
        // The path might be stored with or without leading slash, so try multiple formats:
        // 1. Exact match
        // 2. With leading slash added
        // 3. With leading slash removed
        // 4. Normalized comparison (both without leading slash)
        let key = self
            .file_keys
            .get(&path_str)
            .or_else(|| self.file_keys.get(&format!("/{}", path_str)))
            .or_else(|| self.file_keys.get(path_str.trim_start_matches('/')))
            .or_else(|| {
                // Try matching by normalized paths - useful when absolute paths differ
                // e.g., stored as "/Users/x/data/file.parquet" but received as "Users/x/data/file.parquet"
                self.file_keys.iter().find_map(|(stored_path, key)| {
                    let stored_normalized = stored_path.trim_start_matches('/');
                    let path_normalized = path_str.trim_start_matches('/');
                    if stored_normalized == path_normalized {
                        Some(key)
                    } else {
                        None
                    }
                })
            });

        match key {
            Some(encoded_key) => {
                let key_bytes = Self::decode_key(encoded_key)?;

                // Create decryption properties with the footer key
                // DuckLake uses uniform encryption (same key for footer and all columns)
                let props = FileDecryptionProperties::builder(key_bytes)
                    .build()
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to create decryption properties: {}",
                            e
                        ))
                    })?;

                Ok(Some(props))
            }
            None => {
                // No encryption key for this file - it's not encrypted
                Ok(None)
            }
        }
    }
}

/// Builder for creating a DuckLakeEncryptionFactory from file metadata.
#[derive(Debug, Default)]
pub struct EncryptionFactoryBuilder {
    file_keys: HashMap<String, String>,
}

impl EncryptionFactoryBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an encryption key for a file.
    ///
    /// # Arguments
    /// * `file_path` - The path to the file
    /// * `encryption_key` - The encryption key (if Some)
    pub fn add_file(&mut self, file_path: &str, encryption_key: Option<&str>) -> &mut Self {
        if let Some(key) = encryption_key {
            if !key.is_empty() {
                self.file_keys.insert(file_path.to_string(), key.to_string());
            }
        }
        self
    }

    /// Build the encryption factory.
    pub fn build(self) -> DuckLakeEncryptionFactory {
        DuckLakeEncryptionFactory::new(self.file_keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_factory() {
        let factory = DuckLakeEncryptionFactory::empty();
        assert!(!factory.has_encrypted_files());
    }

    #[test]
    fn test_factory_with_keys() {
        let mut keys = HashMap::new();
        keys.insert("file1.parquet".to_string(), "key1".to_string());
        keys.insert("file2.parquet".to_string(), "key2".to_string());

        let factory = DuckLakeEncryptionFactory::new(keys);
        assert!(factory.has_encrypted_files());
    }

    #[test]
    fn test_builder() {
        let mut builder = EncryptionFactoryBuilder::new();
        builder.add_file("file1.parquet", Some("key1"));
        builder.add_file("file2.parquet", None);
        builder.add_file("file3.parquet", Some(""));
        let factory = builder.build();

        assert!(factory.has_encrypted_files());
        assert_eq!(factory.file_keys.len(), 1);
    }
}
