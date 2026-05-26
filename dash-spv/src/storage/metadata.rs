use std::path::PathBuf;

use crate::{
    error::StorageResult,
    storage::{io::atomic_write, PersistentStorage},
    StorageError,
};
use async_trait::async_trait;
use dashcore::prelude::CoreBlockHeight;

/// Metadata key for persisting the best known peer height.
const LAST_TARGET_HEIGHT_KEY: &str = "last_target_height";

/// Filename of the reorg-in-progress sentinel inside the metadata folder.
pub(crate) const REORG_SENTINEL_FILE: &str = "reorg_in_progress.dat";

#[async_trait]
pub trait MetadataStorage: Send + Sync + 'static {
    async fn store_metadata(&mut self, key: &str, value: &[u8]) -> StorageResult<()>;

    async fn load_metadata(&self, key: &str) -> StorageResult<Option<Vec<u8>>>;

    /// Remove a stored metadata entry. A no-op when the key has not been
    /// persisted.
    async fn delete_metadata(&mut self, key: &str) -> StorageResult<()>;

    /// Persist the last target height to metadata storage.
    async fn store_last_target_height(&mut self, height: CoreBlockHeight) -> StorageResult<()>;
    /// Load the last target height from metadata storage.
    async fn load_last_target_height(&self) -> StorageResult<CoreBlockHeight>;

    /// Create the reorg-in-progress sentinel marker on disk. Written
    /// immediately before the truncation cascade so a crash mid-cascade is
    /// detectable on the next startup.
    async fn write_reorg_sentinel(&mut self) -> StorageResult<()>;

    /// Remove the reorg-in-progress sentinel marker. Called after the
    /// cascade's last truncation completes successfully.
    async fn clear_reorg_sentinel(&mut self) -> StorageResult<()>;

    /// `true` when the reorg-in-progress sentinel marker is present on disk.
    fn is_reorg_sentinel_set(&self) -> bool;
}

pub struct PersistentMetadataStorage {
    storage_path: PathBuf,
}

impl PersistentMetadataStorage {
    pub(crate) const FOLDER_NAME: &str = "metadata";

    fn metadata_folder(&self) -> PathBuf {
        self.storage_path.join(Self::FOLDER_NAME)
    }

    fn reorg_sentinel_path(&self) -> PathBuf {
        self.metadata_folder().join(REORG_SENTINEL_FILE)
    }
}

#[async_trait]
impl PersistentStorage for PersistentMetadataStorage {
    async fn open(storage_path: impl Into<PathBuf> + Send) -> StorageResult<Self> {
        Ok(PersistentMetadataStorage {
            storage_path: storage_path.into(),
        })
    }

    async fn persist(&mut self, _storage_path: impl Into<PathBuf> + Send) -> StorageResult<()> {
        // Current implementation persists data everytime data is stored
        Ok(())
    }
}

#[async_trait]
impl MetadataStorage for PersistentMetadataStorage {
    async fn store_metadata(&mut self, key: &str, value: &[u8]) -> StorageResult<()> {
        let metadata_folder = self.metadata_folder();
        let path = metadata_folder.join(format!("{key}.dat"));

        tokio::fs::create_dir_all(metadata_folder).await?;

        atomic_write(&path, value).await?;

        Ok(())
    }

    async fn load_metadata(&self, key: &str) -> StorageResult<Option<Vec<u8>>> {
        let path = self.metadata_folder().join(format!("{key}.dat"));

        if !path.exists() {
            return Ok(None);
        }

        let data = tokio::fs::read(path).await?;
        Ok(Some(data))
    }

    async fn delete_metadata(&mut self, key: &str) -> StorageResult<()> {
        let path = self.metadata_folder().join(format!("{key}.dat"));
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    /// Persist the last target height to metadata storage.
    async fn store_last_target_height(&mut self, height: CoreBlockHeight) -> StorageResult<()> {
        match serde_json::to_vec(&height) {
            Ok(converted) => self.store_metadata(LAST_TARGET_HEIGHT_KEY, &converted).await,
            Err(e) => {
                let error = format!("Failed to serialize last target height: {}", e);
                tracing::warn!(error);
                Err(StorageError::Serialization(error))
            }
        }
    }

    /// Load the last target height from metadata storage. Used by the block headers manager to
    /// restore progress after restart.
    async fn load_last_target_height(&self) -> StorageResult<CoreBlockHeight> {
        match self.load_metadata(LAST_TARGET_HEIGHT_KEY).await {
            Ok(Some(bytes)) => match serde_json::from_slice::<CoreBlockHeight>(&bytes) {
                Ok(last_target_height) => {
                    tracing::debug!("Restored last target height {}", last_target_height);
                    Ok(last_target_height)
                }
                Err(e) => {
                    let error = format!("Failed to deserialize last target height: {}", e);
                    tracing::warn!(error);
                    Err(StorageError::Serialization(error))
                }
            },
            Ok(None) => {
                let error = "No last target height found (fresh start)".to_string();
                tracing::debug!(error);
                Err(StorageError::NotFound(error))
            }
            Err(e) => {
                let error = format!("Failed to load last target height: {}", e);
                tracing::warn!(error);
                Err(StorageError::Corruption(error))
            }
        }
    }

    async fn write_reorg_sentinel(&mut self) -> StorageResult<()> {
        tokio::fs::create_dir_all(self.metadata_folder()).await?;
        atomic_write(&self.reorg_sentinel_path(), &[]).await
    }

    async fn clear_reorg_sentinel(&mut self) -> StorageResult<()> {
        let path = self.reorg_sentinel_path();
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    fn is_reorg_sentinel_set(&self) -> bool {
        self.reorg_sentinel_path().exists()
    }
}
