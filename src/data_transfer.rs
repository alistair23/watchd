//! Data Transfer Handler for managing large data transfers to Garmin devices
//!
//! When HTTP responses or other data exceed the protobuf message size limit (~2-4KB),
//! the data must be transferred via the DataTransfer protocol. This module provides
//! a registry to store data and serve it in chunks upon request from the watch.
//!
//! Reference: GadgetBridge's DataTransferHandler.java

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Maximum number of active transfers to keep in memory
const MAX_ACTIVE_TRANSFERS: usize = 100;

/// Time to keep transfers in registry before auto-cleanup
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

/// Transfer metadata
#[derive(Debug, Clone)]
struct TransferInfo {
    data: Vec<u8>,
    created_at: Instant,
    last_accessed: Instant,
}

/// DataTransferHandler manages large data transfers
///
/// When data is too large to fit in a single protobuf message, it can be registered
/// with this handler to get a transfer ID. The watch can then request chunks of the
/// data via the DataTransfer protocol.
#[derive(Clone)]
pub struct DataTransferHandler {
    registry: Arc<Mutex<HashMap<u32, TransferInfo>>>,
    next_id: Arc<AtomicU32>,
}

impl DataTransferHandler {
    /// Create a new DataTransferHandler
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU32::new(1)), // Start at 1, 0 is invalid
        }
    }

    /// Register data for transfer and return a unique transfer ID
    ///
    /// # Arguments
    /// * `data` - The data to register for transfer
    ///
    /// # Returns
    /// A unique transfer ID that can be used to fetch chunks
    ///
    /// # Example
    /// ```
    /// let handler = DataTransferHandler::new();
    /// let data = vec![0u8; 10000]; // 10KB of data
    /// let transfer_id = handler.register(data);
    /// println!("Transfer ID: {}", transfer_id);
    /// ```
    pub fn register(&self, data: Vec<u8>) -> u32 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let info = TransferInfo {
            data,
            created_at: Instant::now(),
            last_accessed: Instant::now(),
        };

        let mut registry = self.registry.lock().unwrap();

        // Cleanup old transfers if we're at the limit
        if registry.len() >= MAX_ACTIVE_TRANSFERS {
            self.cleanup_old_transfers_locked(&mut registry);
        }

        registry.insert(id, info);

        log::info!(
            "Registered data transfer: id={}, size={} bytes",
            id,
            registry.get(&id).map(|i| i.data.len()).unwrap_or(0)
        );

        id
    }

    /// Get the total size of a registered transfer
    ///
    /// # Arguments
    /// * `id` - The transfer ID
    ///
    /// # Returns
    /// The total size in bytes, or None if the transfer doesn't exist
    pub fn get_size(&self, id: u32) -> Option<usize> {
        let mut registry = self.registry.lock().unwrap();
        if let Some(info) = registry.get_mut(&id) {
            info.last_accessed = Instant::now();
            Some(info.data.len())
        } else {
            None
        }
    }

    /// Get a chunk of data from a registered transfer
    ///
    /// # Arguments
    /// * `id` - The transfer ID
    /// * `offset` - Byte offset to start reading from
    /// * `length` - Number of bytes to read
    ///
    /// # Returns
    /// A vector containing the requested chunk, or None if the transfer doesn't exist
    /// or the offset is out of bounds
    ///
    /// # Example
    /// ```
    /// let handler = DataTransferHandler::new();
    /// let id = handler.register(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    ///
    /// // Get bytes 2-5
    /// let chunk = handler.get_chunk(id, 2, 4);
    /// assert_eq!(chunk, Some(vec![3, 4, 5, 6]));
    /// ```
    pub fn get_chunk(&self, id: u32, offset: usize, length: usize) -> Option<Vec<u8>> {
        let mut registry = self.registry.lock().unwrap();

        if let Some(info) = registry.get_mut(&id) {
            info.last_accessed = Instant::now();

            if offset >= info.data.len() {
                log::warn!(
                    "Transfer {}: offset {} out of bounds (size: {})",
                    id,
                    offset,
                    info.data.len()
                );
                return None;
            }

            let end = (offset + length).min(info.data.len());
            let chunk = info.data[offset..end].to_vec();

            log::debug!(
                "Transfer {}: serving chunk at offset {} (requested: {}, actual: {} bytes)",
                id,
                offset,
                length,
                chunk.len()
            );

            Some(chunk)
        } else {
            log::warn!("Transfer {} not found in registry", id);
            None
        }
    }

    /// Release a transfer from the registry
    ///
    /// This should be called when the transfer is complete to free memory.
    ///
    /// # Arguments
    /// * `id` - The transfer ID to release
    ///
    /// # Returns
    /// true if the transfer was found and released, false otherwise
    pub fn release(&self, id: u32) -> bool {
        let mut registry = self.registry.lock().unwrap();
        if let Some(info) = registry.remove(&id) {
            log::info!(
                "Released data transfer: id={}, size={} bytes",
                id,
                info.data.len()
            );
            true
        } else {
            log::warn!("Attempted to release non-existent transfer: id={}", id);
            false
        }
    }

    /// Check if a transfer exists
    ///
    /// # Arguments
    /// * `id` - The transfer ID to check
    ///
    /// # Returns
    /// true if the transfer exists
    pub fn exists(&self, id: u32) -> bool {
        self.registry.lock().unwrap().contains_key(&id)
    }

    /// Get the number of active transfers
    pub fn active_count(&self) -> usize {
        self.registry.lock().unwrap().len()
    }

    /// Cleanup old transfers that haven't been accessed recently
    ///
    /// This is called automatically when the registry reaches MAX_ACTIVE_TRANSFERS,
    /// but can also be called manually for maintenance.
    pub fn cleanup_old_transfers(&self) {
        let mut registry = self.registry.lock().unwrap();
        self.cleanup_old_transfers_locked(&mut registry);
    }

    /// Internal cleanup implementation (requires lock already held)
    fn cleanup_old_transfers_locked(&self, registry: &mut HashMap<u32, TransferInfo>) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for (&id, info) in registry.iter() {
            if now.duration_since(info.last_accessed) > TRANSFER_TIMEOUT {
                to_remove.push(id);
            }
        }

        for id in to_remove {
            if let Some(info) = registry.remove(&id) {
                log::info!(
                    "Cleaned up stale transfer: id={}, size={} bytes, age={:?}",
                    id,
                    info.data.len(),
                    now.duration_since(info.created_at)
                );
            }
        }
    }

    /// Get statistics about the data transfer registry
    pub fn get_stats(&self) -> TransferStats {
        let registry = self.registry.lock().unwrap();
        let now = Instant::now();

        let mut stats = TransferStats {
            active_transfers: registry.len(),
            total_bytes: 0,
            oldest_transfer_age: None,
        };

        for info in registry.values() {
            stats.total_bytes += info.data.len();
            let age = now.duration_since(info.created_at);

            stats.oldest_transfer_age = Some(match stats.oldest_transfer_age {
                Some(current) => current.max(age),
                None => age,
            });
        }

        stats
    }
}

impl Default for DataTransferHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the data transfer registry
#[derive(Debug, Clone)]
pub struct TransferStats {
    pub active_transfers: usize,
    pub total_bytes: usize,
    pub oldest_transfer_age: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get_size() {
        let handler = DataTransferHandler::new();
        let data = vec![1u8, 2, 3, 4, 5];
        let id = handler.register(data.clone());

        assert_eq!(handler.get_size(id), Some(5));
    }

    #[test]
    fn test_get_chunk() {
        let handler = DataTransferHandler::new();
        let data = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let id = handler.register(data);

        // Get middle chunk
        let chunk = handler.get_chunk(id, 2, 4);
        assert_eq!(chunk, Some(vec![2, 3, 4, 5]));

        // Get chunk at start
        let chunk = handler.get_chunk(id, 0, 3);
        assert_eq!(chunk, Some(vec![0, 1, 2]));

        // Get chunk at end
        let chunk = handler.get_chunk(id, 7, 10);
        assert_eq!(chunk, Some(vec![7, 8, 9])); // Truncated to available data
    }

    #[test]
    fn test_get_chunk_out_of_bounds() {
        let handler = DataTransferHandler::new();
        let data = vec![1u8, 2, 3];
        let id = handler.register(data);

        // Offset beyond data
        let chunk = handler.get_chunk(id, 10, 5);
        assert_eq!(chunk, None);
    }

    #[test]
    fn test_release() {
        let handler = DataTransferHandler::new();
        let data = vec![1u8, 2, 3];
        let id = handler.register(data);

        assert!(handler.exists(id));
        assert!(handler.release(id));
        assert!(!handler.exists(id));

        // Second release should return false
        assert!(!handler.release(id));
    }

    #[test]
    fn test_multiple_transfers() {
        let handler = DataTransferHandler::new();

        let id1 = handler.register(vec![1u8; 100]);
        let id2 = handler.register(vec![2u8; 200]);
        let id3 = handler.register(vec![3u8; 300]);

        assert_eq!(handler.active_count(), 3);
        assert_eq!(handler.get_size(id1), Some(100));
        assert_eq!(handler.get_size(id2), Some(200));
        assert_eq!(handler.get_size(id3), Some(300));

        handler.release(id2);
        assert_eq!(handler.active_count(), 2);
        assert!(handler.exists(id1));
        assert!(!handler.exists(id2));
        assert!(handler.exists(id3));
    }

    #[test]
    fn test_unique_ids() {
        let handler = DataTransferHandler::new();

        let id1 = handler.register(vec![1u8]);
        let id2 = handler.register(vec![2u8]);
        let id3 = handler.register(vec![3u8]);

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_stats() {
        let handler = DataTransferHandler::new();

        handler.register(vec![1u8; 100]);
        handler.register(vec![2u8; 200]);
        handler.register(vec![3u8; 300]);

        let stats = handler.get_stats();
        assert_eq!(stats.active_transfers, 3);
        assert_eq!(stats.total_bytes, 600);
        assert!(stats.oldest_transfer_age.is_some());
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;

        let handler = DataTransferHandler::new();
        let handler_clone = handler.clone();

        let handle = thread::spawn(move || {
            for i in 0..10 {
                let id = handler_clone.register(vec![i; 100]);
                assert!(handler_clone.exists(id));
            }
        });

        for i in 0..10 {
            let id = handler.register(vec![i; 200]);
            assert!(handler.exists(id));
        }

        handle.join().unwrap();
        assert_eq!(handler.active_count(), 20);
    }
}
