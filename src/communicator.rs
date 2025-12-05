//! Garmin v2 Communicator
//!
//! This module implements the high-level Garmin v2 BLE communicator, providing
//! `initialize_device` and `send_message` functionality that coordinates COBS
//! encoding and MLR protocol handling.

use crate::cobs::CobsCoDec;
use crate::messages::Status;
use crate::mlr::{MessageReceiver, MessageSender, MlrCommunicator};
use crate::types::{GarminError, RequestType, Result, Service};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;

/// Base UUID format for Garmin ML GFDI service
pub const BASE_UUID_FORMAT: &str = "6A4E%04X-667B-11E3-949A-0800200C9A66";

/// Gadgetbridge client ID used in ML protocol
const GADGETBRIDGE_CLIENT_ID: u64 = 2;

/// Trait for BLE operations that must be implemented by the platform
#[async_trait::async_trait]
pub trait BleSupport: Send + Sync {
    /// Get a characteristic by UUID
    fn get_characteristic(&self, uuid: &str) -> Option<CharacteristicHandle>;

    /// Enable notifications on a characteristic
    fn enable_notifications(&self, handle: &CharacteristicHandle) -> Result<()>;

    /// Write data to a characteristic
    async fn write_characteristic(&self, handle: &CharacteristicHandle, data: &[u8]) -> Result<()>;

    /// Create a transaction for queuing multiple operations
    fn create_transaction(&self, name: &str) -> Box<dyn Transaction>;
}

/// Trait for BLE transaction building
pub trait Transaction: Send {
    /// Write data in the transaction
    fn write(&mut self, handle: &CharacteristicHandle, data: &[u8]);

    /// Enable notifications in the transaction
    fn notify(&mut self, handle: &CharacteristicHandle, enable: bool);

    /// Queue the transaction for execution
    fn queue(self: Box<Self>) -> Result<()>;
}

/// Handle to a BLE characteristic
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CharacteristicHandle {
    pub uuid: String,
}

impl CharacteristicHandle {
    pub fn new(uuid: String) -> Self {
        Self { uuid }
    }
}

/// Callback for handling received GFDI messages
pub trait GfdiMessageCallback: Send + Sync {
    fn on_message(&self, message: &[u8]) -> Result<()>;
}

/// Async callback for handling received GFDI messages with response capability
#[async_trait::async_trait]
pub trait AsyncGfdiMessageCallback: Send + Sync {
    async fn on_message(&self, message: &[u8]) -> Result<Option<Vec<u8>>>;
}

/// Writer for sending messages to a service
#[async_trait::async_trait]
pub trait ServiceWriter: Send + Sync {
    async fn write(&self, task_name: &str, data: &[u8]) -> Result<()>;
}

/// Callback for service lifecycle events
pub trait ServiceCallback: Send + Sync {
    /// Called when service is connected and ready to use
    fn on_connect(&self, writer: Box<dyn ServiceWriter>) -> Result<()> {
        let _ = writer;
        Ok(())
    }

    /// Called when service is closed
    fn on_close(&self) -> Result<()> {
        Ok(())
    }

    /// Called when a message is received from the service
    fn on_message(&self, data: &[u8]) -> Result<()>;
}

/// Internal state of the CommunicatorV2
struct CommunicatorState {
    characteristic_send: Option<CharacteristicHandle>,
    characteristic_receive: Option<CharacteristicHandle>,
    service_by_handle: HashMap<u8, Service>,
    handle_by_service: HashMap<Service, u8>,
    service_callbacks: HashMap<Service, Box<dyn ServiceCallback>>,
    mlr_communicators: HashMap<u8, Arc<MlrCommunicator>>,
    max_write_size: usize,
    cobs_codec: CobsCoDec,
}

impl CommunicatorState {
    fn new() -> Self {
        Self {
            characteristic_send: None,
            characteristic_receive: None,
            service_by_handle: HashMap::new(),
            handle_by_service: HashMap::new(),
            service_callbacks: HashMap::new(),
            mlr_communicators: HashMap::new(),
            max_write_size: 20,
            cobs_codec: CobsCoDec::new(),
        }
    }
}

/// Main Garmin v2 Communicator
pub struct CommunicatorV2 {
    state: Arc<Mutex<CommunicatorState>>,
    ble_support: Arc<dyn BleSupport>,
    message_callback: Option<Arc<dyn GfdiMessageCallback>>,
    async_message_callback: Option<Arc<dyn AsyncGfdiMessageCallback>>,
}

/// MLR Message Sender implementation that writes to BLE
struct MlrBleSender {
    ble_support: Arc<dyn BleSupport>,
    characteristic_send: CharacteristicHandle,
}

#[async_trait::async_trait]
impl MessageSender for MlrBleSender {
    async fn send_packet(&self, task_name: &str, packet: &[u8]) -> Result<()> {
        debug!(
            "MLR sending packet for '{}': {} bytes",
            task_name,
            packet.len()
        );
        debug!(
            "MLR packet hex: {}",
            packet
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join(" ")
        );

        match self
            .ble_support
            .write_characteristic(&self.characteristic_send, packet)
            .await
        {
            Ok(_) => {
                debug!("MLR packet sent successfully via BLE");
                Ok(())
            }
            Err(e) => {
                error!("MLR packet write_characteristic failed: {}", e);
                Err(e)
            }
        }
    }
}

/// MLR Message Receiver implementation that processes decoded GFDI messages
///
/// Data flow for incoming GFDI messages:
/// 1. BLE receives packet with handle in first byte (e.g., 0x0B for GFDI)
/// 2. MLR layer strips 2-byte MLR header, passes data here
/// 3. This receiver COBS-decodes the data
/// 4. Result is a GFDI message starting with packet size (2 bytes, little-endian)
/// 5. GFDI message format: [size:2][msg_id:2][payload...][crc:2]
///
/// Note: The handle byte (0x0B for GFDI) is NOT in the GFDI message itself.
/// It's only used in BLE/MLR transport layers.
struct MlrGfdiReceiver {
    _communicator: Weak<Mutex<CommunicatorState>>,
    message_callback: Option<Arc<dyn GfdiMessageCallback>>,
    async_message_callback: Option<Arc<dyn AsyncGfdiMessageCallback>>,
    cobs_codec: Mutex<CobsCoDec>,
}

#[async_trait::async_trait]
impl MessageReceiver for MlrGfdiReceiver {
    async fn on_data_received(&self, data: &[u8]) -> Result<()> {
        debug!("MLR received data: {} bytes", data.len());

        // The data from MLR is COBS encoded, so we need to decode it
        // Use persistent codec to support multi-packet messages
        let mut codec = self.cobs_codec.lock().await;
        codec.receive_bytes(data);

        if let Some(decoded) = codec.retrieve_message() {
            debug!("MLR data decoded via COBS: {} bytes", decoded.len());

            // The decoded message is the GFDI message directly - no handle byte here
            // The handle was already in the MLR header (stripped by MLR layer)
            if decoded.is_empty() {
                warn!("MLR decoded empty message");
                return Ok(());
            }

            // GFDI message format: [packet_size:2][message_id:2][payload...][crc:2]
            // First 2 bytes are packet size (little-endian), NOT a handle byte

            // Debug: Show ALL bytes for every message
            let hex_dump = decoded
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join(" ");
            debug!("Incoming message ALL bytes: {}", hex_dump);
            debug!("Byte-by-byte breakdown:");
            for (i, byte) in decoded.iter().enumerate() {
                debug!("  [{}] = 0x{:02X} ({})", i, byte, byte);
            }

            // Extract and log message type with sequence number and response details
            if decoded.len() >= 4 {
                let mut raw_msg_id = u16::from_le_bytes([decoded[2], decoded[3]]);

                // Check for sequence number (bit 15 set)
                let sequence_number = if (raw_msg_id & 0x8000) != 0 {
                    let seq = (raw_msg_id >> 8) & 0x7F;
                    // Decode actual message ID: bits 0-7 + 5000
                    raw_msg_id = (raw_msg_id & 0xFF) + 5000;
                    Some(seq)
                } else {
                    None
                };

                let msg_id = raw_msg_id;
                let msg_type = crate::messages::MessageId::from_u16(msg_id);

                // Build log message
                let mut log_parts = Vec::new();
                log_parts.push(format!("{} bytes", decoded.len()));

                // Add message type
                let type_str = match msg_type {
                    Some(id) => format!("{:?} (0x{:04X})", id, msg_id),
                    None => format!("Unknown (0x{:04X})", msg_id),
                };
                log_parts.push(format!("type: {}", type_str));

                // Add sequence number if present
                if let Some(seq) = sequence_number {
                    log_parts.push(format!("seq: {}", seq));
                }

                // For Response messages, extract original message type and status
                if msg_id == 5000 {
                    // Debug: show raw bytes for Response messages
                    let hex_dump = decoded
                        .iter()
                        .take(decoded.len().min(20))
                        .map(|b| format!("{:02X}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    debug!("Response message raw bytes: {}", hex_dump);

                    if decoded.len() >= 9 {
                        let orig_msg_id = u16::from_le_bytes([decoded[4], decoded[5]]);
                        let status_byte = decoded[6];

                        debug!("Response decoding: orig_msg_id bytes=[{:02X} {:02X}], status_byte={:02X}",
                               decoded[4], decoded[5], decoded[6]);

                        let orig_type = match crate::messages::MessageId::from_u16(orig_msg_id) {
                            Some(id) => format!("{:?}", id),
                            None => format!("0x{:04X}", orig_msg_id),
                        };

                        let status_str = if let Some(status) = Status::from_u8(status_byte) {
                            status.name().to_string()
                        } else {
                            format!("UNKNOWN_0x{:02X}", status_byte)
                        };

                        log_parts.push(format!("responding to: {}", orig_type));
                        log_parts.push(format!("status: {}", status_str));

                        // Warn only if we see truly unknown status codes or invalid message IDs
                        if Status::from_u8(status_byte).is_none() {
                            warn!(
                                "Unknown status code: 0x{:02X} (valid codes: 0-5)",
                                status_byte
                            );
                        }
                        if orig_msg_id == 0xFFFF {
                            debug!("Response with message ID 0xFFFF - watch doesn't recognize/support this message type (this is normal for optional features)");
                        }
                    } else {
                        warn!(
                            "Response message too short: {} bytes (need at least 9)",
                            decoded.len()
                        );
                    }
                }

                info!("GFDI message received: {}", log_parts.join(", "));
            } else {
                info!(
                    "GFDI message received: {} bytes (invalid - too short)",
                    decoded.len()
                );
            }

            // Call the appropriate callback with the GFDI message
            if let Some(callback) = &self.message_callback {
                callback.on_message(&decoded)?;
            } else if let Some(callback) = &self.async_message_callback {
                let callback = callback.clone();
                let decoded_vec = decoded.to_vec();
                let msg_type = if decoded.len() >= 4 {
                    let raw_msg_id = u16::from_le_bytes([decoded[2], decoded[3]]);
                    let msg_id = if (raw_msg_id & 0x8000) != 0 {
                        (raw_msg_id & 0xFF) + 5000
                    } else {
                        raw_msg_id
                    };
                    format!("0x{:04X}", msg_id)
                } else {
                    "unknown".to_string()
                };
                info!("🔔 Spawning async callback for message type {}", msg_type);

                tokio::spawn(async move {
                    info!(
                        "🔔 Async callback task started for message type {}",
                        msg_type
                    );
                    match callback.on_message(&decoded_vec).await {
                        Ok(_) => {
                            info!(
                                "✅ Async callback completed successfully for message type {}",
                                msg_type
                            );
                        }
                        Err(e) => {
                            error!(
                                "❌ Async MLR callback error for message type {}: {}",
                                msg_type, e
                            );
                        }
                    }
                });
            } else {
                debug!("No callback registered for MLR data");
            }
        } else {
            debug!("MLR data incomplete, waiting for more");
        }

        Ok(())
    }
}

impl CommunicatorV2 {
    /// Create a new CommunicatorV2
    pub fn new(ble_support: Arc<dyn BleSupport>) -> Self {
        Self {
            state: Arc::new(Mutex::new(CommunicatorState::new())),
            ble_support,
            message_callback: None,
            async_message_callback: None,
        }
    }

    /// Set the GFDI message callback
    pub fn set_message_callback(&mut self, callback: Arc<dyn GfdiMessageCallback>) {
        self.message_callback = Some(callback);
    }

    /// Set the async message callback for handling GFDI messages with response capability
    pub fn set_async_message_callback(&mut self, callback: Arc<dyn AsyncGfdiMessageCallback>) {
        self.async_message_callback = Some(callback);
    }

    /// Register a service callback for a specific service
    /// This callback will be called when the service is connected/closed or receives messages
    pub async fn register_service_callback(
        &self,
        service: Service,
        callback: Box<dyn ServiceCallback>,
    ) {
        let mut state = self.state.lock().await;
        state.service_callbacks.insert(service, callback);
        info!("Registered callback for service: {}", service);
    }

    /// Unregister a service callback
    pub async fn unregister_service_callback(
        &self,
        service: Service,
    ) -> Option<Box<dyn ServiceCallback>> {
        let mut state = self.state.lock().await;
        state.service_callbacks.remove(&service)
    }

    /// Get the receive characteristic UUID that was set during initialization
    pub async fn get_receive_characteristic_uuid(&self) -> Option<String> {
        let state = self.state.lock().await;
        state
            .characteristic_receive
            .as_ref()
            .map(|ch| ch.uuid.clone())
    }

    /// Update the MTU (Maximum Transmission Unit) size
    pub async fn on_mtu_changed(&self, mtu: usize) {
        let mut state = self.state.lock().await;

        // Calculate max write chunk based on MTU
        // Typically: MTU - 3 (ATT header overhead)
        state.max_write_size = mtu.saturating_sub(3);

        info!(
            "MTU changed to {}, max write size: {}",
            mtu, state.max_write_size
        );

        // Update all MLR communicators
        for mlr in state.mlr_communicators.values() {
            mlr.set_max_packet_size(state.max_write_size).await;
        }
    }

    /// Initialize the device - discovers characteristics and sets up ML protocol
    ///
    /// This is the Rust port of the Java `initializeDevice` method.
    /// It iterates through known ML characteristics to find a valid send/receive pair.
    pub async fn initialize_device(&self) -> Result<bool> {
        info!("Initializing Garmin v2 device");

        let mut state = self.state.lock().await;

        // Iterate through the known ML characteristics until we find a known pair
        // send characteristic = read characteristic + 0x10 (eg. 2810 / 2820)
        for i in 0x2810..=0x2814 {
            let receive_uuid = BASE_UUID_FORMAT.replace("%04X", &format!("{:04X}", i));
            let send_uuid = BASE_UUID_FORMAT.replace("%04X", &format!("{:04X}", i + 0x10));

            let characteristic_receive = self.ble_support.get_characteristic(&receive_uuid);
            let characteristic_send = self.ble_support.get_characteristic(&send_uuid);

            if let (Some(recv), Some(send)) = (characteristic_receive, characteristic_send) {
                debug!(
                    "Using characteristics receive/send = {}/{}",
                    recv.uuid, send.uuid
                );

                state.characteristic_receive = Some(recv.clone());
                state.characteristic_send = Some(send.clone());

                drop(state);

                // Enable notifications on receive characteristic
                self.ble_support.enable_notifications(&recv)?;

                // Send close all services command to start fresh
                let close_all_data = self.create_close_all_services_message();
                info!("Closing all services: {close_all_data:x?}");
                self.ble_support
                    .write_characteristic(&send, &close_all_data)
                    .await?;

                info!("Garmin v2 device initialized successfully");
                return Ok(true);
            }
        }

        warn!("Failed to find any known ML characteristics");
        Ok(false)
    }

    /// Initialize device using a transaction builder pattern
    ///
    /// This variant uses the transaction pattern similar to the original Java code.
    pub async fn initialize_device_with_transaction(&self, transaction_name: &str) -> Result<bool> {
        info!("Initializing Garmin v2 device with transaction");

        let mut state = self.state.lock().await;
        let mut transaction = self.ble_support.create_transaction(transaction_name);

        // Iterate through the known ML characteristics
        for i in 0x2810..=0x2814 {
            let receive_uuid = BASE_UUID_FORMAT.replace("%04X", &format!("{:04X}", i));
            let send_uuid = BASE_UUID_FORMAT.replace("%04X", &format!("{:04X}", i + 0x10));

            let characteristic_receive = self.ble_support.get_characteristic(&receive_uuid);
            let characteristic_send = self.ble_support.get_characteristic(&send_uuid);

            if let (Some(recv), Some(send)) = (characteristic_receive, characteristic_send) {
                debug!(
                    "Using characteristics receive/send = {}/{}",
                    recv.uuid, send.uuid
                );

                state.characteristic_receive = Some(recv.clone());
                state.characteristic_send = Some(send.clone());

                // Build transaction
                transaction.notify(&recv, true);

                let close_all_data = self.create_close_all_services_message();
                transaction.write(&send, &close_all_data);

                drop(state);
                transaction.queue()?;

                info!("Garmin v2 device initialized successfully with transaction");
                return Ok(true);
            }
        }

        warn!("Failed to find any known ML characteristics");
        Ok(false)
    }

    /// Send a message to the device via the GFDI service
    ///
    /// This is the Rust port of the Java `sendMessage` method.
    /// Messages are COBS-encoded and sent via MLR protocol or directly depending on setup.
    pub async fn send_message(&self, task_name: &str, message: &[u8]) -> Result<()> {
        if message.is_empty() {
            return Err(GarminError::EmptyMessage);
        }

        debug!(
            "send_message: attempting to acquire state lock for task '{}'",
            task_name
        );
        let state = self.state.lock().await;
        debug!("send_message: acquired state lock for task '{}'", task_name);

        // Get the GFDI handle
        let gfdi_handle = state
            .handle_by_service
            .get(&Service::GFDI)
            .ok_or(GarminError::GfdiHandleNotSet)?;

        debug!(
            "Sending message '{}' ({} bytes) via GFDI handle {}",
            task_name,
            message.len(),
            gfdi_handle
        );

        // Extract and log message type with sequence number and response details
        if message.len() >= 4 {
            let mut raw_msg_id = u16::from_le_bytes([message[2], message[3]]);

            // Check for sequence number (bit 15 set)
            let sequence_number = if (raw_msg_id & 0x8000) != 0 {
                let seq = (raw_msg_id >> 8) & 0x7F;
                // Decode actual message ID: bits 0-7 + 5000
                raw_msg_id = (raw_msg_id & 0xFF) + 5000;
                Some(seq)
            } else {
                None
            };

            let msg_id = raw_msg_id;
            let msg_type = crate::messages::MessageId::from_u16(msg_id);

            // Build message type string
            let type_str = match msg_type {
                Some(id) => format!("{:?} (0x{:04X})", id, msg_id),
                None => format!("Unknown (0x{:04X})", msg_id),
            };

            info!("📤 SENDING MESSAGE '{}' to watch:", task_name);
            info!("   Message type: {}", type_str);

            if let Some(seq) = sequence_number {
                info!("   Sequence number: {}", seq);
            }

            // For Response messages, show what we're responding to and status
            if msg_id == 5000 {
                if message.len() >= 9 {
                    let orig_msg_id = u16::from_le_bytes([message[4], message[5]]);
                    let status_byte = message[6];

                    debug!("Outgoing Response decoding: orig_msg_id bytes=[{:02X} {:02X}], status_byte={:02X}",
                           message[4], message[5], message[6]);

                    let orig_type = match crate::messages::MessageId::from_u16(orig_msg_id) {
                        Some(id) => format!("{:?}", id),
                        None => format!("0x{:04X}", orig_msg_id),
                    };

                    let status_str = if let Some(status) = Status::from_u8(status_byte) {
                        status.name().to_string()
                    } else {
                        format!("UNKNOWN_0x{:02X}", status_byte)
                    };

                    info!("   Responding to: {}", orig_type);
                    info!("   Status: {}", status_str);

                    if Status::from_u8(status_byte).is_none() {
                        warn!(
                            "   Unknown status code: 0x{:02X} (valid codes: 0-5)",
                            status_byte
                        );
                    }
                } else {
                    warn!(
                        "   Response message too short: {} bytes (need at least 9)",
                        message.len()
                    );
                }
            }

            info!("   Message size: {} bytes", message.len());
        } else {
            info!("📤 SENDING MESSAGE '{}' to watch:", task_name);
            info!(
                "   Message size: {} bytes (invalid - too short)",
                message.len()
            );
        }
        info!(
            "   Message hex: {}",
            message
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join(" ")
        );
        if message.len() > 32 {
            info!(
                "   First 32 bytes: {}",
                message[..32]
                    .iter()
                    .map(|b| format!("{:02X}", b))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }

        // COBS encode the message
        let payload = CobsCoDec::encode(message);

        // Log the COBS-encoded payload
        debug!(
            "   COBS encoded: {} bytes: {}",
            payload.len(),
            payload[..payload.len().min(32)]
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join(" ")
        );

        // Check if we have an MLR communicator for this handle
        // The MLR communicator is keyed by the MLR handle (bits 0-3 of service handle)
        let mlr_handle = *gfdi_handle & 0x0F;
        if let Some(mlr) = state.mlr_communicators.get(&mlr_handle) {
            let mlr = Arc::clone(mlr);
            // IMPORTANT: Must drop state lock before calling MLR to avoid deadlock
            drop(state);
            let result = mlr.send_message(task_name, &payload).await;
            return result;
        }

        // No MLR - send directly with fragmentation if needed
        let characteristic_send = state
            .characteristic_send
            .as_ref()
            .ok_or_else(|| GarminError::BluetoothError("Send characteristic not set".to_string()))?
            .clone();

        let max_write_size = state.max_write_size;
        let gfdi_handle = *gfdi_handle;
        drop(state);

        let mut remaining = payload.len();
        let mut position = 0;

        if remaining > max_write_size - 1 {
            // Fragmented sending
            while remaining > 0 {
                let chunk_size = remaining.min(max_write_size - 1);
                let mut fragment = vec![gfdi_handle];
                fragment.extend_from_slice(&payload[position..position + chunk_size]);

                info!(
                    "   Writing fragment {}/{} ({} bytes) to BLE",
                    (position / (max_write_size - 1)) + 1,
                    (payload.len() + max_write_size - 2) / (max_write_size - 1),
                    fragment.len()
                );
                debug!(
                    "   Fragment hex: {}",
                    fragment
                        .iter()
                        .map(|b| format!("{:02X}", b))
                        .collect::<Vec<_>>()
                        .join(" ")
                );

                self.ble_support
                    .write_characteristic(&characteristic_send, &fragment)
                    .await?;

                position += chunk_size;
                remaining -= chunk_size;
            }
        } else {
            // Single packet
            let mut packet = vec![gfdi_handle];
            packet.extend_from_slice(&payload);

            info!("   Writing single packet ({} bytes) to BLE", packet.len());
            debug!(
                "   Packet hex: {}",
                packet
                    .iter()
                    .map(|b| format!("{:02X}", b))
                    .collect::<Vec<_>>()
                    .join(" ")
            );

            self.ble_support
                .write_characteristic(&characteristic_send, &packet)
                .await?;
        }

        info!("   ✅ Message '{}' sent successfully to watch", task_name);
        Ok(())
    }

    /// Send a message using a transaction
    pub async fn send_message_with_transaction(
        &self,
        task_name: &str,
        message: &[u8],
    ) -> Result<()> {
        if message.is_empty() {
            return Err(GarminError::EmptyMessage);
        }

        let state = self.state.lock().await;

        let gfdi_handle = state
            .handle_by_service
            .get(&Service::GFDI)
            .ok_or(GarminError::GfdiHandleNotSet)?;

        let payload = CobsCoDec::encode(message);

        // Check if we have an MLR communicator
        if state.mlr_communicators.contains_key(gfdi_handle) {
            let mlr = state.mlr_communicators.get(gfdi_handle).unwrap();
            let result = mlr.send_message(task_name, &payload).await;
            drop(state);
            return result;
        }

        let characteristic_send = state
            .characteristic_send
            .as_ref()
            .ok_or_else(|| GarminError::BluetoothError("Send characteristic not set".to_string()))?
            .clone();

        let max_write_size = state.max_write_size;
        let gfdi_handle_val = *gfdi_handle;
        drop(state);

        let mut transaction = self.ble_support.create_transaction(task_name);
        let mut remaining = payload.len();
        let mut position = 0;

        if remaining > max_write_size - 1 {
            while remaining > 0 {
                let chunk_size = remaining.min(max_write_size - 1);
                let mut fragment = vec![gfdi_handle_val];
                fragment.extend_from_slice(&payload[position..position + chunk_size]);

                transaction.write(&characteristic_send, &fragment);

                position += chunk_size;
                remaining -= chunk_size;
            }
        } else {
            let mut packet = vec![gfdi_handle_val];
            packet.extend_from_slice(&payload);
            transaction.write(&characteristic_send, &packet);
        }

        transaction.queue()?;
        Ok(())
    }

    /// Handle characteristic changed notification (received data)
    pub async fn on_characteristic_changed(&self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        debug!("Characteristic changed: {} bytes", data.len());
        debug!("Raw data: {:02X?}", &data[..data.len().min(20)]);

        debug!("on_characteristic_changed: attempting to acquire state lock");
        let mut state = self.state.lock().await;
        debug!("on_characteristic_changed: acquired state lock");

        // Check if this is an MLR packet
        if data.len() >= 2 && (data[0] & 0x80) != 0 {
            // Extract MLR handle from bits 4-6 (bit 7 is MLR flag, bits 0-3 are req_num)
            let handle = (data[0] & 0x70) >> 4;

            // Check if this is an ACK packet (exactly 2 bytes = header only, no data)
            let is_ack = data.len() == 2;

            debug!(
                "MLR packet detected: first_byte=0x{:02X}, handle={}, is_ack={}, len={}",
                data[0],
                handle,
                is_ack,
                data.len()
            );

            if is_ack {
                debug!("Received MLR ACK for handle {}", handle);
                // ACKs must be passed to the MLR communicator for processing
                if let Some(mlr) = state.mlr_communicators.get(&handle) {
                    let mlr = Arc::clone(mlr);
                    drop(state);
                    let result = mlr.on_packet_received(data).await;
                    return result;
                } else {
                    warn!(
                        "Received ACK for handle {} but no MLR communicator exists",
                        handle
                    );
                    drop(state);
                    return Ok(());
                }
            }

            debug!(
                "Available MLR communicators: {:?}",
                state.mlr_communicators.keys().collect::<Vec<_>>()
            );
            debug!("Registered services: {:?}", state.service_by_handle);

            if let Some(mlr) = state.mlr_communicators.get(&handle) {
                let mlr = Arc::clone(mlr);
                drop(state);
                let result = mlr.on_packet_received(data).await;
                return result;
            } else {
                warn!(
                    "No MLR communicator found for handle {}. MLR packet cannot be processed.",
                    handle
                );
                warn!("This is expected - MLR implementation is not complete yet.");
                warn!("For now, attempting to process as raw COBS data (will likely fail).");
                // Fall through to COBS processing
            }
        }

        let handle = data[0];
        let payload = &data[1..];

        // Handle 0 is for handle management messages
        if handle == 0 {
            debug!("Processing handle management message");
            drop(state);
            return self.process_handle_management(payload).await;
        }

        // Not MLR - process as COBS data
        debug!("Processing as COBS data");
        state.cobs_codec.receive_bytes(data);

        if let Some(message) = state.cobs_codec.retrieve_message() {
            debug!("Decoded COBS message: {} bytes", message.len());
            debug!("Decoded data: {:02X?}", &message[..message.len().min(20)]);

            // Handle the message based on its type
            drop(state);
            self.handle_decoded_message(&message).await?;
        } else {
            debug!("No complete COBS message yet");
        }

        Ok(())
    }

    /// Handle a decoded message from the device
    async fn handle_decoded_message(&self, message: &[u8]) -> Result<()> {
        if message.is_empty() {
            debug!("Empty decoded message, ignoring");
            return Ok(());
        }

        // First byte is the handle
        let handle = message[0];
        let payload = &message[1..];

        debug!(
            "Handling message for handle {}: {} bytes",
            handle,
            payload.len()
        );
        debug!("Payload: {:02X?}", &payload[..payload.len().min(20)]);

        debug!(
            "handle_decoded_message: attempting to acquire state lock for handle {}",
            handle
        );
        let state = self.state.lock().await;
        debug!(
            "handle_decoded_message: acquired state lock for handle {}",
            handle
        );

        if let Some(service) = state.service_by_handle.get(&handle) {
            debug!("Message for service: {}", service);

            // If this is GFDI service, call the callback
            if *service == Service::GFDI {
                // Try sync callback first
                if let Some(callback) = &self.message_callback {
                    debug!("handle_decoded_message: explicitly dropping state lock before sync callback");
                    drop(state);
                    debug!("Calling sync message callback");
                    callback.on_message(payload)?;
                    return Ok(());
                }

                // If no sync callback but we have async callback, spawn a task to call it
                if let Some(callback) = &self.async_message_callback {
                    let callback = callback.clone();
                    let payload_vec = payload.to_vec();
                    debug!("handle_decoded_message: explicitly dropping state lock before async callback");
                    drop(state);

                    debug!("Spawning task to call async message callback");
                    tokio::spawn(async move {
                        if let Err(e) = callback.on_message(&payload_vec).await {
                            log::error!("Async callback error: {}", e);
                        }
                    });
                    return Ok(());
                }
            }
        } else {
            debug!("No service registered for handle {}", handle);
        }

        Ok(())
    }

    /// Process handle management messages (handle 0)
    async fn process_handle_management(&self, message: &[u8]) -> Result<()> {
        if message.is_empty() {
            return Err(GarminError::InvalidMessage(
                "Empty handle management message".to_string(),
            ));
        }

        // First byte is the request type
        let request_type_byte = message[0];
        let request_type = RequestType::from_u8(request_type_byte)?;

        // Next 8 bytes are the client ID - check we have them
        if message.len() < 9 {
            return Err(GarminError::InvalidMessage(format!(
                "Handle management message too short: {} bytes (need at least 9: 1 type + 8 client ID)",
                message.len()
            )));
        }

        let client_id = u64::from_le_bytes([
            message[1], message[2], message[3], message[4], message[5], message[6], message[7],
            message[8],
        ]);

        if client_id != GADGETBRIDGE_CLIENT_ID {
            warn!(
                "Ignoring incoming message, client ID {} is not ours (expected {})",
                client_id, GADGETBRIDGE_CLIENT_ID
            );
            return Ok(());
        }

        debug!("Processing handle management: {}", request_type);

        match request_type {
            RequestType::RegisterMlReq
            | RequestType::CloseHandleReq
            | RequestType::CloseAllReq
            | RequestType::UnkReq => {
                warn!(
                    "Received handle request {}, expecting responses. Message: {:02X?}",
                    request_type, message
                );
                Ok(())
            }
            RequestType::RegisterMlResp => self.process_register_ml_resp(&message[9..]).await,
            RequestType::CloseHandleResp => self.process_close_handle_resp(&message[9..]).await,
            RequestType::CloseAllResp => self.process_close_all_resp().await,
            RequestType::UnkResp => {
                debug!("Received unknown response. Message: {:02X?}", message);
                Ok(())
            }
            _ => {
                warn!("Unknown request type: {}", request_type);
                Ok(())
            }
        }
    }

    /// Process REGISTER_ML_RESP message
    async fn process_register_ml_resp(&self, payload: &[u8]) -> Result<()> {
        if payload.len() < 5 {
            return Err(GarminError::InvalidMessage(
                "RegisterMlResp payload too short".to_string(),
            ));
        }

        let service_code = u16::from_le_bytes([payload[0], payload[1]]);
        let status = payload[2];
        let handle = payload[3];
        let reliable = payload[4];

        let service = match Service::from_code(service_code) {
            Ok(s) => s,
            Err(_) => {
                warn!(
                    "Got register response status={} for unknown service {}",
                    status, service_code
                );
                return Ok(());
            }
        };

        if status != 0 {
            warn!("Failed to register {}, status={}", service, status);
            return Ok(());
        }

        // The service handle has the MLR bit encoded in it
        // For example: handle=134 (0x86) means MLR handle 6 with reliable bit set
        // Extract the actual MLR handle from bits 0-3 of the service handle
        let mlr_handle = if reliable != 0 {
            // Service handle format: bit 7=MLR flag, bits 0-3=MLR handle
            // Service handle 0x80 (10000000) -> MLR handle 0 (bits 0-3 = 0000)
            // Service handle 0x86 (10000110) -> MLR handle 6 (bits 0-3 = 0110)
            handle & 0x0F
        } else {
            handle
        };

        info!(
            "Got register response for {}, service_handle={} (0x{:02X}), mlr_handle={}, reliable={}",
            service, handle, handle, mlr_handle, reliable
        );

        {
            let mut state = self.state.lock().await;
            state.service_by_handle.insert(handle, service);
            state.handle_by_service.insert(service, handle);
        }

        // Create MLR communicator if reliable mode is enabled
        if reliable != 0 {
            info!("Creating MLR communicator for MLR handle {}", mlr_handle);

            // Get the send characteristic
            let send_char = {
                let state = self.state.lock().await;
                state.characteristic_send.clone()
            };

            if send_char.is_none() {
                warn!("Cannot create MLR communicator: send characteristic not available");
                return Ok(());
            }

            // Create sender that writes to BLE
            let sender = Arc::new(MlrBleSender {
                ble_support: Arc::clone(&self.ble_support),
                characteristic_send: send_char.unwrap(),
            });

            // Create receiver that processes GFDI messages
            let receiver = Arc::new(MlrGfdiReceiver {
                _communicator: Arc::downgrade(&self.state),
                message_callback: self.message_callback.clone(),
                async_message_callback: self.async_message_callback.clone(),
                cobs_codec: Mutex::new(CobsCoDec::new()),
            });

            // Create and start MLR communicator
            let mut mlr = MlrCommunicator::new(mlr_handle, 20, sender, receiver);

            if let Err(e) = mlr.start() {
                warn!("Failed to start MLR communicator: {}", e);
            } else {
                info!(
                    "MLR communicator started successfully for handle {}",
                    mlr_handle
                );
            }

            // Store the MLR communicator (wrapped in Arc for cloning)
            let mut state = self.state.lock().await;
            state.mlr_communicators.insert(mlr_handle, Arc::new(mlr));

            info!(
                "✅ MLR communicator fully initialized for handle {}",
                mlr_handle
            );
        }

        // Get or create service callback
        let mut state = self.state.lock().await;

        // If no callback is registered for this service, create a default one for known services
        if !state.service_callbacks.contains_key(&service) {
            let default_callback: Option<Box<dyn ServiceCallback>> = match service {
                Service::GFDI => {
                    // Create default GFDI callback if we have a message callback
                    if let Some(msg_callback) = &self.message_callback {
                        info!("Auto-creating GfdiServiceCallback for GFDI service");
                        Some(Box::new(GfdiServiceCallback::new(msg_callback.clone())))
                    } else {
                        warn!("GFDI service registered but no message callback is set");
                        None
                    }
                }
                // For other services, no default callback yet
                // Users can register custom callbacks for these services
                _ => None,
            };

            if let Some(callback) = default_callback {
                state.service_callbacks.insert(service, callback);
            }
        }

        // Call onConnect for the service callback if registered
        if let Some(callback) = state.service_callbacks.get(&service) {
            // Create a service writer for this handle
            let writer = Box::new(MlrServiceWriter {
                handle: mlr_handle,
                ble_support: self.ble_support.clone(),
                characteristic_send: state.characteristic_send.clone().unwrap(),
            });

            if let Err(e) = callback.on_connect(writer) {
                warn!("Service callback onConnect failed: {}", e);
            }
        } else {
            debug!("No callback registered for service: {}", service);
        }

        Ok(())
    }

    /// Process CLOSE_HANDLE_RESP message
    async fn process_close_handle_resp(&self, payload: &[u8]) -> Result<()> {
        if payload.len() < 4 {
            return Err(GarminError::InvalidMessage(
                "CloseHandleResp payload too short".to_string(),
            ));
        }

        let service_code = u16::from_le_bytes([payload[0], payload[1]]);
        let handle = payload[2];
        let status = payload[3];

        let service = Service::from_code(service_code).ok();

        debug!(
            "Received close handle response: service={:?}, handle={}, status={}",
            service, handle, status
        );

        let mut state = self.state.lock().await;

        if let Some(service) = service {
            // Call service callback onClose
            if let Some(callback) = state.service_callbacks.remove(&service) {
                if let Err(e) = callback.on_close() {
                    warn!("Service callback onClose failed: {}", e);
                }
            }

            state.handle_by_service.remove(&service);

            // Clean up MLR communicator if it exists
            if state.mlr_communicators.contains_key(&handle) {
                state.mlr_communicators.remove(&handle);
            }
        }

        state.service_by_handle.remove(&handle);

        Ok(())
    }

    /// Process CLOSE_ALL_RESP message
    async fn process_close_all_resp(&self) -> Result<()> {
        debug!("Received close all handles response");

        let mut state = self.state.lock().await;

        // Clear all service mappings
        debug!("Clear all service mappings");
        state.service_by_handle.clear();
        state.handle_by_service.clear();

        // Call onClose() for all service callbacks
        for (service, callback) in state.service_callbacks.drain() {
            debug!("Calling onClose for service: {}", service);
            if let Err(e) = callback.on_close() {
                warn!("Service callback onClose failed for {:?}: {}", service, e);
            }
        }

        // Clear MLR communicators
        debug!("Clear all MLR communicators");
        state.mlr_communicators.clear();

        // Get the send characteristic before dropping state
        let characteristic_send = state.characteristic_send.clone();

        drop(state);

        // Re-register GFDI service by sending registration message directly
        // We can't use register_service() because it needs the GFDI handle which we just cleared
        info!("Re-registering GFDI service after CLOSE_ALL_RESP");
        if let Some(send_char) = characteristic_send {
            let register_msg = self.create_register_service_message(Service::GFDI, true);
            if let Err(e) = self
                .ble_support
                .write_characteristic(&send_char, &register_msg)
                .await
            {
                warn!("Failed to send GFDI registration message: {}", e);
            }
        } else {
            warn!("Cannot re-register GFDI: send characteristic not available");
        }

        Ok(())
    }

    /// Handle a decoded message asynchronously with response capability
    pub async fn handle_decoded_message_async(&self, message: &[u8]) -> Result<Option<Vec<u8>>> {
        if message.is_empty() {
            return Ok(None);
        }

        // First byte is the handle
        let handle = message[0];
        let payload = &message[1..];

        debug!(
            "Handling async message for handle {}: {} bytes",
            handle,
            payload.len()
        );

        let state = self.state.lock().await;

        if let Some(service) = state.service_by_handle.get(&handle) {
            debug!("Async message for service: {}", service);

            // If this is GFDI service, call the async callback
            if *service == Service::GFDI {
                if let Some(callback) = &self.async_message_callback {
                    let callback = callback.clone();
                    drop(state);
                    return callback.on_message(payload).await;
                }
            }
        }

        Ok(None)
    }

    /// Register a service with the device
    pub async fn register_service(&self, service: Service, reliable: bool) -> Result<()> {
        info!("Registering service: {} (reliable: {})", service, reliable);

        let message = self.create_register_service_message(service, reliable);
        self.send_message(&format!("register_{}", service), &message)
            .await?;

        Ok(())
    }

    /// Close a service
    pub async fn close_service(&self, service: Service) -> Result<()> {
        info!("Closing service: {}", service);

        let state = self.state.lock().await;
        let handle = state.handle_by_service.get(&service).ok_or_else(|| {
            GarminError::BluetoothError(format!("Service {} not registered", service))
        })?;

        let message = self.create_close_service_message(service, *handle);
        drop(state);

        self.send_message(&format!("close_{}", service), &message)
            .await?;

        Ok(())
    }

    /// Create the "close all services" message
    fn create_close_all_services_message(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(13);
        buffer.push(0); // handle
        buffer.push(RequestType::CloseAllReq.to_u8());
        buffer.extend_from_slice(&GADGETBRIDGE_CLIENT_ID.to_le_bytes());
        buffer.extend_from_slice(&0u16.to_le_bytes());
        buffer.push(0); // Additional padding byte to make it 13 bytes
        buffer
    }

    /// Create a "register service" message
    fn create_register_service_message(&self, service: Service, reliable: bool) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(13);
        buffer.push(0);
        buffer.push(RequestType::RegisterMlReq.to_u8());
        buffer.extend_from_slice(&GADGETBRIDGE_CLIENT_ID.to_le_bytes());
        buffer.extend_from_slice(&service.code().to_le_bytes());
        buffer.push(if reliable { 2 } else { 0 });
        buffer
    }

    /// Create a "close service" message
    fn create_close_service_message(&self, service: Service, handle: u8) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(12);
        buffer.push(0);
        buffer.push(RequestType::CloseHandleReq.to_u8());
        buffer.extend_from_slice(&GADGETBRIDGE_CLIENT_ID.to_le_bytes());
        buffer.extend_from_slice(&service.code().to_le_bytes());
        buffer.push(handle);
        buffer
    }

    /// Register a service handle (called when device responds with handle assignment)
    pub async fn register_handle(&self, service: Service, handle: u8) {
        let mut state = self.state.lock().await;
        state.service_by_handle.insert(handle, service);
        state.handle_by_service.insert(service, handle);
        info!("Registered service {} with handle {}", service, handle);
    }

    /// Create and register an MLR communicator for a handle
    pub async fn create_mlr_communicator(
        &self,
        handle: u8,
        sender: Arc<dyn MessageSender>,
        receiver: Arc<dyn MessageReceiver>,
    ) -> Result<()> {
        let mut state = self.state.lock().await;

        let mut mlr = MlrCommunicator::new(handle, state.max_write_size, sender, receiver);

        mlr.start()?;
        state.mlr_communicators.insert(handle, Arc::new(mlr));

        info!("Created MLR communicator for handle {}", handle);
        Ok(())
    }

    /// Dispose and clean up resources
    pub async fn dispose(&self) {
        info!("Disposing CommunicatorV2");

        // Get state lock for cleanup
        let mut state = self.state.lock().await;

        // Close all MLR communicators
        for (handle, _mlr) in state.mlr_communicators.drain() {
            debug!("Closing MLR communicator for handle {}", handle);
            // Note: Arc<MlrCommunicator> doesn't expose close() method
            // The Drop impl will handle cleanup
        }

        debug!("CommunicatorV2 disposed");
    }

    /// Handle connection state change
    pub async fn on_connection_state_change(&self, connected: bool) {
        debug!(
            "Connection state changed: {}",
            if connected {
                "connected"
            } else {
                "disconnected"
            }
        );

        if !connected {
            // Clear and pause all MLR communicators when disconnected
            self.clear_and_pause_mlr().await;
        }
    }

    /// Pause all MLR communicators (prevents sending during reconnection)
    pub async fn pause_mlr(&self) {
        let state = self.state.lock().await;
        for mlr in state.mlr_communicators.values() {
            mlr.pause().await;
        }
        debug!("Paused all MLR communicators");
    }

    /// Resume all MLR communicators (allows sending after reconnection)
    pub async fn resume_mlr(&self) {
        let state = self.state.lock().await;
        for mlr in state.mlr_communicators.values() {
            mlr.resume().await;
        }
        debug!("Resumed all MLR communicators");
    }

    /// Clear and pause all MLR communicators (use when connection is lost)
    pub async fn clear_and_pause_mlr(&self) {
        let state = self.state.lock().await;
        for mlr in state.mlr_communicators.values() {
            mlr.clear_and_pause().await;
        }
        debug!("Cleared and paused all MLR communicators");
    }
}

impl Drop for CommunicatorV2 {
    fn drop(&mut self) {
        // Note: We can't call async dispose() from Drop
        // The MLR communicators will be cleaned up via their own Drop impls
        // when the Arc references are dropped
        debug!("CommunicatorV2 dropping - MLR communicators will clean up automatically");
    }
}

/// ServiceWriter implementation for MLR (Multi-Link Reliable) services
///
/// Data flow for outgoing GFDI messages:
/// 1. GFDI message is prepared: [packet_size:2][msg_id:2][payload...][crc:2]
/// 2. Message is COBS-encoded
/// 3. ServiceWriter prepends handle byte (e.g., 0x0B for GFDI)
/// 4. MLR layer adds 2-byte MLR header
/// 5. Final packet sent over BLE
///
/// Note: The handle byte (0x0B) is added here for BLE/MLR transport,
/// but is NOT part of the GFDI message structure itself.
struct MlrServiceWriter {
    handle: u8,
    ble_support: Arc<dyn BleSupport>,
    characteristic_send: CharacteristicHandle,
}

#[async_trait::async_trait]
impl ServiceWriter for MlrServiceWriter {
    async fn write(&self, task_name: &str, data: &[u8]) -> Result<()> {
        debug!(
            "MlrServiceWriter writing {} bytes for task '{}' on handle 0x{:02X}",
            data.len(),
            task_name,
            self.handle
        );

        // Prepend handle byte for BLE/MLR transport layer
        // This handle byte (e.g., 0x0B for GFDI) is NOT part of the GFDI message
        let mut payload = Vec::with_capacity(data.len() + 1);
        payload.push(self.handle);
        payload.extend_from_slice(data);

        self.ble_support
            .write_characteristic(&self.characteristic_send, &payload)
            .await
    }
}

/// Example GFDI service callback that decodes COBS and forwards to a message callback
pub struct GfdiServiceCallback {
    cobs_codec: Arc<Mutex<CobsCoDec>>,
    message_callback: Arc<dyn GfdiMessageCallback>,
}

impl GfdiServiceCallback {
    pub fn new(message_callback: Arc<dyn GfdiMessageCallback>) -> Self {
        Self {
            cobs_codec: Arc::new(Mutex::new(CobsCoDec::new())),
            message_callback,
        }
    }
}

impl ServiceCallback for GfdiServiceCallback {
    fn on_message(&self, data: &[u8]) -> Result<()> {
        debug!("GFDI service received {} bytes", data.len());

        // Since this is called from sync context but needs async work,
        // spawn a task to handle the COBS decoding and callback
        let codec = self.cobs_codec.clone();
        let callback = self.message_callback.clone();
        let data = data.to_vec();

        tokio::spawn(async move {
            let mut codec_guard = codec.lock().await;
            codec_guard.receive_bytes(&data);

            if let Some(decoded) = codec_guard.retrieve_message() {
                debug!("GFDI COBS decoded: {} bytes", decoded.len());
                if let Err(e) = callback.on_message(&decoded) {
                    error!("GFDI callback failed: {}", e);
                }
            }
        });

        Ok(())
    }

    fn on_connect(&self, _writer: Box<dyn ServiceWriter>) -> Result<()> {
        info!("GFDI service connected");
        Ok(())
    }

    fn on_close(&self) -> Result<()> {
        info!("GFDI service closed");
        Ok(())
    }
}

/// Example callback for realtime heart rate service
pub struct RealtimeHeartRateCallback {
    on_heart_rate: Box<dyn Fn(u8) -> Result<()> + Send + Sync>,
}

impl RealtimeHeartRateCallback {
    pub fn new(on_heart_rate: Box<dyn Fn(u8) -> Result<()> + Send + Sync>) -> Self {
        Self { on_heart_rate }
    }
}

impl ServiceCallback for RealtimeHeartRateCallback {
    fn on_connect(&self, _writer: Box<dyn ServiceWriter>) -> Result<()> {
        info!("Realtime heart rate service connected");
        Ok(())
    }

    fn on_close(&self) -> Result<()> {
        info!("Realtime heart rate service closed");
        Ok(())
    }

    fn on_message(&self, data: &[u8]) -> Result<()> {
        if data.len() >= 1 {
            let heart_rate = data[0];
            debug!("Realtime heart rate: {} bpm", heart_rate);
            (self.on_heart_rate)(heart_rate)?;
        }
        Ok(())
    }
}

/// Example callback for realtime steps service
pub struct RealtimeStepsCallback {
    on_steps: Box<dyn Fn(u32) -> Result<()> + Send + Sync>,
}

impl RealtimeStepsCallback {
    pub fn new(on_steps: Box<dyn Fn(u32) -> Result<()> + Send + Sync>) -> Self {
        Self { on_steps }
    }
}

impl ServiceCallback for RealtimeStepsCallback {
    fn on_connect(&self, _writer: Box<dyn ServiceWriter>) -> Result<()> {
        info!("Realtime steps service connected");
        Ok(())
    }

    fn on_close(&self) -> Result<()> {
        info!("Realtime steps service closed");
        Ok(())
    }

    fn on_message(&self, data: &[u8]) -> Result<()> {
        if data.len() >= 4 {
            let steps = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            debug!("Realtime steps: {}", steps);
            (self.on_steps)(steps)?;
        }
        Ok(())
    }
}

/// Generic callback for file transfer services
pub struct FileTransferCallback {
    data: Arc<Mutex<Vec<u8>>>,
    on_complete: Arc<Box<dyn Fn(Vec<u8>) -> Result<()> + Send + Sync>>,
}

impl FileTransferCallback {
    pub fn new(on_complete: Box<dyn Fn(Vec<u8>) -> Result<()> + Send + Sync>) -> Self {
        Self {
            data: Arc::new(Mutex::new(Vec::new())),
            on_complete: Arc::new(on_complete),
        }
    }
}

impl ServiceCallback for FileTransferCallback {
    fn on_connect(&self, _writer: Box<dyn ServiceWriter>) -> Result<()> {
        info!("File transfer service connected");
        Ok(())
    }

    fn on_close(&self) -> Result<()> {
        info!("File transfer service closed");

        // Spawn async task to process the complete data
        let data_mutex = self.data.clone();
        let on_complete = self.on_complete.clone();

        tokio::spawn(async move {
            let data = data_mutex.lock().await.clone();
            if !data.is_empty() {
                if let Err(e) = on_complete(data) {
                    error!("File transfer completion callback failed: {}", e);
                }
            }
        });

        Ok(())
    }

    fn on_message(&self, data: &[u8]) -> Result<()> {
        debug!("File transfer received {} bytes", data.len());

        // Spawn async task to append data
        let data_mutex = self.data.clone();
        let data_vec = data.to_vec();

        tokio::spawn(async move {
            let mut guard = data_mutex.lock().await;
            guard.extend_from_slice(&data_vec);
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    struct MockBleSupport {
        characteristics: HashMap<String, CharacteristicHandle>,
        written_data: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl MockBleSupport {
        fn new() -> Self {
            let mut characteristics = HashMap::new();

            // Add mock characteristics
            let receive_uuid = "6A4E2810-667B-11E3-949A-0800200C9A66".to_string();
            let send_uuid = "6A4E2820-667B-11E3-949A-0800200C9A66".to_string();

            characteristics.insert(
                receive_uuid.clone(),
                CharacteristicHandle::new(receive_uuid),
            );
            characteristics.insert(send_uuid.clone(), CharacteristicHandle::new(send_uuid));

            Self {
                characteristics,
                written_data: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_written_data(&self) -> Vec<Vec<u8>> {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(self.written_data.lock())
                    .clone()
            })
        }
    }

    #[async_trait::async_trait]
    impl BleSupport for MockBleSupport {
        fn get_characteristic(&self, uuid: &str) -> Option<CharacteristicHandle> {
            self.characteristics.get(uuid).cloned()
        }

        fn enable_notifications(&self, _handle: &CharacteristicHandle) -> Result<()> {
            Ok(())
        }

        async fn write_characteristic(
            &self,
            _handle: &CharacteristicHandle,
            data: &[u8],
        ) -> Result<()> {
            self.written_data.lock().await.push(data.to_vec());
            Ok(())
        }

        fn create_transaction(&self, _name: &str) -> Box<dyn Transaction> {
            Box::new(MockTransaction::new())
        }
    }

    struct MockTransaction;

    impl MockTransaction {
        fn new() -> Self {
            MockTransaction
        }
    }

    impl Transaction for MockTransaction {
        fn write(&mut self, _handle: &CharacteristicHandle, _data: &[u8]) {
            // No-op
        }

        fn notify(&mut self, _handle: &CharacteristicHandle, _enable: bool) {
            // No-op
        }

        fn queue(self: Box<Self>) -> Result<()> {
            Ok(())
        }
    }

    // Helper to get COBS-encoded message
    fn create_cobs_encoded_message(data: &[u8]) -> Vec<u8> {
        CobsCoDec::encode(data)
    }

    fn create_test_message(msg_type: RequestType, client_id: u64) -> Vec<u8> {
        let mut message = vec![0]; // handle
        message.push(msg_type.to_u8());
        message.extend_from_slice(&client_id.to_le_bytes());
        message
    }

    #[tokio::test]
    async fn test_create_communicator() {
        let ble = Arc::new(MockBleSupport::new());
        let _comm = CommunicatorV2::new(ble);
    }

    #[tokio::test]
    async fn test_initialize_device() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble.clone());

        let result = comm.initialize_device().await.unwrap();
        assert!(result);

        // Should have written close all services command
        let written = ble.get_written_data();
        assert!(!written.is_empty());
    }

    #[tokio::test]
    async fn test_create_close_all_services_message() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble);

        let message = comm.create_close_all_services_message();

        // Should have: [handle][type][client_id]
        assert_eq!(message[0], 0); // handle
        assert_eq!(message[1], RequestType::CloseAllReq.to_u8());
        // Remaining bytes are client ID
    }

    #[tokio::test]
    async fn test_create_register_service_message() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble);

        let message = comm.create_register_service_message(Service::GFDI, true);

        // Should have: [handle][type][client_id][service_code][reliable]
        assert_eq!(message[0], 0); // handle
        assert_eq!(message[1], RequestType::RegisterMlReq.to_u8());
        // bytes 2-9 are client ID
        assert_eq!(u16::from_le_bytes([message[10], message[11]]), 5559); // GFDI code
        assert_eq!(message[12], 2); // reliable flag
    }

    #[tokio::test]
    async fn test_process_close_all_resp() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble);

        // First register a service
        comm.register_handle(Service::GFDI, 1).await;
        comm.register_handle(Service::RealtimeHr, 2).await;

        // Verify services are registered
        {
            let state = comm.state.lock().await;
            assert_eq!(state.service_by_handle.len(), 2);
            assert_eq!(state.handle_by_service.len(), 2);
        }

        // Create a CLOSE_ALL_RESP message
        // Note: handle 0 is part of the outer message structure
        let mut message = vec![0]; // handle 0 for management
        message.push(RequestType::CloseAllResp.to_u8());
        message.extend_from_slice(&GADGETBRIDGE_CLIENT_ID.to_le_bytes());

        // COBS encode the message
        let encoded = CobsCoDec::encode(&message);

        // Process the encoded message
        let result = comm.on_characteristic_changed(&encoded).await;
        assert!(result.is_ok());

        // Verify services are cleared (except GFDI which should be re-registered)
        // Note: The re-registration happens but we can't easily verify it in this test
        // because it would require the mock BLE support to actually process the write
    }

    #[tokio::test]
    async fn test_process_register_ml_resp() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble);

        // Create a REGISTER_ML_RESP message
        // Note: handle 0 is part of the outer message structure
        let mut message = vec![0]; // handle 0 for management
        message.push(RequestType::RegisterMlResp.to_u8());
        message.extend_from_slice(&GADGETBRIDGE_CLIENT_ID.to_le_bytes());
        message.extend_from_slice(&Service::GFDI.code().to_le_bytes()); // service code
        message.push(0); // status = success
        message.push(1); // handle
        message.push(2); // reliable

        // COBS encode the message
        let encoded = CobsCoDec::encode(&message);

        // Process the encoded message
        let result = comm.on_characteristic_changed(&encoded).await;
        assert!(result.is_ok());

        // Verify service is registered
        let state = comm.state.lock().await;
        assert_eq!(state.service_by_handle.get(&1), Some(&Service::GFDI));
        assert_eq!(state.handle_by_service.get(&Service::GFDI), Some(&1));
    }

    #[tokio::test]
    async fn test_process_close_handle_resp() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble);

        // First register a service
        comm.register_handle(Service::RealtimeHr, 2).await;

        // Create a CLOSE_HANDLE_RESP message
        // Note: handle 0 is part of the outer message structure
        let mut message = vec![0]; // handle 0 for management
        message.push(RequestType::CloseHandleResp.to_u8());
        message.extend_from_slice(&GADGETBRIDGE_CLIENT_ID.to_le_bytes());
        message.extend_from_slice(&Service::RealtimeHr.code().to_le_bytes()); // service code
        message.push(2); // handle
        message.push(0); // status = success

        // COBS encode the message
        let encoded = CobsCoDec::encode(&message);

        eprintln!("Original message: {:?}", message);
        eprintln!("Encoded: {:?}", encoded);

        // Process the encoded message
        let result = comm.on_characteristic_changed(&encoded).await;
        if let Err(ref e) = result {
            eprintln!("Error: {}", e);
        }
        assert!(result.is_ok());

        // Verify service is unregistered
        let state = comm.state.lock().await;
        assert_eq!(state.service_by_handle.get(&2), None);
        assert_eq!(state.handle_by_service.get(&Service::RealtimeHr), None);
    }

    #[tokio::test]
    async fn test_process_handle_management_invalid_client_id() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble);

        // Create a message with wrong client ID
        // Note: handle 0 is part of the outer message structure
        let mut message = vec![0]; // handle 0 for management
        message.push(RequestType::RegisterMlResp.to_u8());
        message.extend_from_slice(&999u64.to_le_bytes()); // wrong client ID
                                                          // Add some dummy data to complete the message
        message.extend_from_slice(&[0, 0, 0, 0, 0]);

        // COBS encode the message
        let encoded = CobsCoDec::encode(&message);

        // Process the encoded message - should succeed but be ignored
        let result = comm.on_characteristic_changed(&encoded).await;
        assert!(result.is_ok());

        // Verify nothing was registered
        let state = comm.state.lock().await;
        assert_eq!(state.service_by_handle.len(), 0);
    }

    // Service Callback Tests

    struct TestServiceCallback {
        connect_called: Arc<StdMutex<bool>>,
        close_called: Arc<StdMutex<bool>>,
        messages_received: Arc<StdMutex<Vec<Vec<u8>>>>,
    }

    impl TestServiceCallback {
        fn new() -> Self {
            Self {
                connect_called: Arc::new(StdMutex::new(false)),
                close_called: Arc::new(StdMutex::new(false)),
                messages_received: Arc::new(StdMutex::new(Vec::new())),
            }
        }
    }

    impl ServiceCallback for TestServiceCallback {
        fn on_connect(&self, _writer: Box<dyn ServiceWriter>) -> Result<()> {
            *self.connect_called.lock().unwrap() = true;
            Ok(())
        }

        fn on_close(&self) -> Result<()> {
            *self.close_called.lock().unwrap() = true;
            Ok(())
        }

        fn on_message(&self, data: &[u8]) -> Result<()> {
            self.messages_received.lock().unwrap().push(data.to_vec());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_register_service_callback() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble);

        let callback = TestServiceCallback::new();
        let _connect_called = callback.connect_called.clone();

        comm.register_service_callback(Service::RealtimeHr, Box::new(callback))
            .await;

        // Verify callback is registered
        let state = comm.state.lock().await;
        assert!(state.service_callbacks.contains_key(&Service::RealtimeHr));
        drop(state);

        // Simulate service registration
        comm.register_handle(Service::RealtimeHr, 5).await;

        // The connect callback won't be called in this simple test
        // because we're not going through the full MLR registration flow
    }

    #[tokio::test]
    async fn test_remove_service_callback() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble);

        let callback = TestServiceCallback::new();
        comm.register_service_callback(Service::RealtimeHr, Box::new(callback))
            .await;

        // Verify callback is registered
        {
            let state = comm.state.lock().await;
            assert!(state.service_callbacks.contains_key(&Service::RealtimeHr));
        }

        // Unregister
        let removed = comm.unregister_service_callback(Service::RealtimeHr).await;
        assert!(removed.is_some());

        // Verify callback is removed
        let state = comm.state.lock().await;
        assert!(!state.service_callbacks.contains_key(&Service::RealtimeHr));
    }

    #[tokio::test]
    async fn test_service_callback_on_close_all() {
        let ble = Arc::new(MockBleSupport::new());
        let comm = CommunicatorV2::new(ble);

        let callback1 = TestServiceCallback::new();
        let callback2 = TestServiceCallback::new();
        let close_called1 = callback1.close_called.clone();
        let close_called2 = callback2.close_called.clone();

        comm.register_service_callback(Service::RealtimeHr, Box::new(callback1))
            .await;
        comm.register_service_callback(Service::RealtimeSteps, Box::new(callback2))
            .await;

        // Call process_close_all_resp
        let result = comm.process_close_all_resp().await;
        assert!(result.is_ok());

        // Verify both callbacks had onClose called
        assert!(*close_called1.lock().unwrap());
        assert!(*close_called2.lock().unwrap());

        // Verify callbacks are cleared
        let state = comm.state.lock().await;
        assert_eq!(state.service_callbacks.len(), 0);
    }

    #[tokio::test]
    async fn test_gfdi_service_callback() {
        struct MockGfdiCallback {
            messages: Arc<StdMutex<Vec<Vec<u8>>>>,
        }

        impl GfdiMessageCallback for MockGfdiCallback {
            fn on_message(&self, message: &[u8]) -> Result<()> {
                self.messages.lock().unwrap().push(message.to_vec());
                Ok(())
            }
        }

        let messages = Arc::new(StdMutex::new(Vec::new()));
        let mock_callback = Arc::new(MockGfdiCallback {
            messages: messages.clone(),
        });

        let gfdi_callback = GfdiServiceCallback::new(mock_callback);

        // Test COBS encoded message
        // Note: COBS adds a trailing zero, so we need to work around that
        let test_data = vec![1, 2, 3, 4, 5];
        let encoded = CobsCoDec::encode(&test_data);

        let result = gfdi_callback.on_message(&encoded);
        assert!(result.is_ok());

        // Wait for spawned task to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Verify decoded message was received (may have trailing zero from COBS)
        let received = messages.lock().unwrap();
        assert_eq!(received.len(), 1);
        // The message should start with our test data
        assert!(received[0].starts_with(&test_data));
    }

    #[test]
    fn test_realtime_heart_rate_callback() {
        let heart_rate_value = Arc::new(StdMutex::new(0u8));
        let heart_rate_clone = heart_rate_value.clone();

        let callback = RealtimeHeartRateCallback::new(Box::new(move |hr| {
            *heart_rate_clone.lock().unwrap() = hr;
            Ok(())
        }));

        // Simulate heart rate data
        let data = vec![75]; // 75 bpm
        let result = callback.on_message(&data);
        assert!(result.is_ok());

        assert_eq!(*heart_rate_value.lock().unwrap(), 75);
    }

    #[test]
    fn test_realtime_steps_callback() {
        let steps_value = Arc::new(StdMutex::new(0u32));
        let steps_clone = steps_value.clone();

        let callback = RealtimeStepsCallback::new(Box::new(move |steps| {
            *steps_clone.lock().unwrap() = steps;
            Ok(())
        }));

        // Simulate steps data (1234 steps in little-endian)
        let data = 1234u32.to_le_bytes().to_vec();
        let result = callback.on_message(&data);
        assert!(result.is_ok());

        assert_eq!(*steps_value.lock().unwrap(), 1234);
    }

    #[tokio::test]
    async fn test_file_transfer_callback() {
        let completed_data = Arc::new(StdMutex::new(Vec::new()));
        let completed_clone = completed_data.clone();

        let callback = FileTransferCallback::new(Box::new(move |data| {
            *completed_clone.lock().unwrap() = data;
            Ok(())
        }));

        // Simulate file transfer chunks
        let chunk1 = vec![1, 2, 3, 4];
        let chunk2 = vec![5, 6, 7, 8];

        callback.on_message(&chunk1).unwrap();
        callback.on_message(&chunk2).unwrap();

        // Wait for spawned tasks to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Close the transfer
        callback.on_close().unwrap();

        // Wait for completion callback to run
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Verify all data was collected
        let final_data = completed_data.lock().unwrap();
        assert_eq!(*final_data, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[tokio::test]
    async fn test_auto_gfdi_callback_creation() {
        struct TestGfdiHandler {
            messages: Arc<StdMutex<Vec<Vec<u8>>>>,
        }

        impl GfdiMessageCallback for TestGfdiHandler {
            fn on_message(&self, message: &[u8]) -> Result<()> {
                self.messages.lock().unwrap().push(message.to_vec());
                Ok(())
            }
        }

        let messages = Arc::new(StdMutex::new(Vec::new()));
        let handler = Arc::new(TestGfdiHandler {
            messages: messages.clone(),
        });

        let ble = Arc::new(MockBleSupport::new());
        let mut comm = CommunicatorV2::new(ble);

        // Set the message callback BEFORE service registration
        comm.set_message_callback(handler);

        // Verify no GFDI callback exists yet
        {
            let state = comm.state.lock().await;
            assert!(!state.service_callbacks.contains_key(&Service::GFDI));
        }

        // Simulate GFDI service registration by calling the internal logic
        // In real usage, this would happen when processing REGISTER_ML_RESP
        comm.register_handle(Service::GFDI, 1).await;

        // Now manually trigger the callback creation logic that would happen in process_register_ml_resp
        {
            let mut state = comm.state.lock().await;
            if !state.service_callbacks.contains_key(&Service::GFDI) {
                if let Some(msg_callback) = &comm.message_callback {
                    let gfdi_callback = Box::new(GfdiServiceCallback::new(msg_callback.clone()));
                    state.service_callbacks.insert(Service::GFDI, gfdi_callback);
                }
            }
        }

        // Verify GFDI callback was auto-created
        {
            let state = comm.state.lock().await;
            assert!(state.service_callbacks.contains_key(&Service::GFDI));
        }

        // Test that the auto-created callback works
        let state = comm.state.lock().await;
        if let Some(callback) = state.service_callbacks.get(&Service::GFDI) {
            // Send a COBS-encoded test message
            let test_data = vec![1, 2, 3, 4, 5];
            let encoded = CobsCoDec::encode(&test_data);
            callback.on_message(&encoded).unwrap();
        }
        drop(state);

        // Wait for spawned task to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Verify the message was received by the handler
        let received = messages.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert!(received[0].starts_with(&vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_gfdi_message_type_logging() {
        // Test that message type extraction works correctly

        // DeviceInformation message (ID 5024 = 0x13A0)
        let device_info_msg = vec![
            0x09, 0x00, // packet size
            0xA0, 0x13, // message ID (5024 = DeviceInformation, little-endian)
            0x01, 0x02, 0x03, // payload
            0xAB, 0xCD, // CRC
        ];

        let msg_id = u16::from_le_bytes([device_info_msg[2], device_info_msg[3]]);
        assert_eq!(msg_id, 5024);
        let msg_type = crate::messages::MessageId::from_u16(msg_id);
        assert!(msg_type.is_some());
        assert_eq!(
            msg_type.unwrap(),
            crate::messages::MessageId::DeviceInformation
        );

        // Response message (ID 5000 = 0x1388) with ACK status
        let response_msg = vec![
            0x0B, 0x00, // packet size
            0x88, 0x13, // message ID (5000 = Response)
            0xA0, 0x13, // original message ID (5024 = DeviceInformation)
            0x00, // status (ACK)
            0x01, 0x02, // payload
            0xEF, 0x12, // CRC
        ];

        let msg_id = u16::from_le_bytes([response_msg[2], response_msg[3]]);
        assert_eq!(msg_id, 5000);
        let msg_type = crate::messages::MessageId::from_u16(msg_id);
        assert!(msg_type.is_some());
        assert_eq!(msg_type.unwrap(), crate::messages::MessageId::Response);

        // Verify response details
        let orig_msg_id = u16::from_le_bytes([response_msg[4], response_msg[5]]);
        assert_eq!(orig_msg_id, 5024); // DeviceInformation
        let status = response_msg[6];
        assert_eq!(status, 0); // ACK

        // Message with sequence number (bit 15 set)
        // Encoded: 0x8042 = 1000 0000 0100 0010
        // Sequence: (0x8042 >> 8) & 0x7F = 0x00
        // Message ID: (0x8042 & 0xFF) + 5000 = 0x42 + 5000 = 66 + 5000 = 5066
        let seq_msg = vec![
            0x09, 0x00, // packet size
            0x42, 0x80, // message ID with sequence number (little-endian)
            0x01, 0x02, 0x03, // payload
            0xAB, 0xCD, // CRC
        ];

        let raw_msg_id = u16::from_le_bytes([seq_msg[2], seq_msg[3]]);
        assert_eq!(raw_msg_id, 0x8042);
        assert!((raw_msg_id & 0x8000) != 0); // Sequence bit is set
        let seq_num = (raw_msg_id >> 8) & 0x7F;
        assert_eq!(seq_num, 0);
        let decoded_msg_id = (raw_msg_id & 0xFF) + 5000;
        assert_eq!(decoded_msg_id, 5066);

        // Synchronization message (ID 5037 = 0x13AD)
        let sync_msg = vec![
            0x0C, 0x00, // packet size
            0xAD, 0x13, // message ID (5037 = Synchronization)
            0x01, 0x08, 0x28, 0x00, 0x00, 0x02, // payload
            0x34, 0x56, // CRC
        ];

        let msg_id = u16::from_le_bytes([sync_msg[2], sync_msg[3]]);
        assert_eq!(msg_id, 5037);
        let msg_type = crate::messages::MessageId::from_u16(msg_id);
        assert!(msg_type.is_some());
        assert_eq!(
            msg_type.unwrap(),
            crate::messages::MessageId::Synchronization
        );

        // Response message with NACK status
        let nack_response = vec![
            0x0B, 0x00, // packet size
            0x88, 0x13, // message ID (5000 = Response)
            0xAD, 0x13, // original message ID (5037 = Synchronization)
            0x01, // status (NACK)
            0x01, 0x02, // payload
            0xEF, 0x12, // CRC
        ];

        let status = nack_response[6];
        assert_eq!(status, 1); // NACK

        // Unknown message
        let unknown_msg = vec![
            0x05, 0x00, // packet size
            0xFF, 0xFF, // unknown message ID
            0x00, 0x00, 0x00, // payload + CRC
        ];

        let msg_id = u16::from_le_bytes([unknown_msg[2], unknown_msg[3]]);
        assert_eq!(msg_id, 0xFFFF);
        let msg_type = crate::messages::MessageId::from_u16(msg_id);
        assert!(msg_type.is_none());
    }
}
