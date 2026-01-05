//! GFDI Message parsing and generation
//!
//! This module handles parsing incoming GFDI messages from Garmin devices
//! and generating appropriate responses.

use crate::types::{GarminError, Result};
use std::collections::HashSet;

/// GFDI Message IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum MessageId {
    Response = 5000,
    DownloadRequest = 5002,
    UploadRequest = 5003,
    FileTransferData = 5004,
    CreateFile = 5005,
    Filter = 5007,
    SetFileFlag = 5008,
    FitDefinition = 5011,
    FitData = 5012,
    WeatherRequest = 5014,
    DeviceInformation = 5024,
    DeviceSettings = 5026,
    SystemEvent = 5030,
    SupportedFileTypesRequest = 5031,
    NotificationUpdate = 5033,
    NotificationControl = 5034,
    NotificationData = 5035,
    NotificationSubscription = 5036,
    Synchronization = 5037,
    FindMyPhoneRequest = 5039,
    FindMyPhoneCancel = 5040,
    MusicControl = 5041,
    MusicControlCapabilities = 5042,
    ProtobufRequest = 5043,
    ProtobufResponse = 5044,
    MusicControlEntityUpdate = 5049,
    Configuration = 5050,
    CurrentTimeRequest = 5052,
    AuthNegotiation = 5101,
}

impl MessageId {
    pub fn from_u16(id: u16) -> Option<Self> {
        match id {
            5000 => Some(MessageId::Response),
            5002 => Some(MessageId::DownloadRequest),
            5003 => Some(MessageId::UploadRequest),
            5004 => Some(MessageId::FileTransferData),
            5005 => Some(MessageId::CreateFile),
            5007 => Some(MessageId::Filter),
            5008 => Some(MessageId::SetFileFlag),
            5011 => Some(MessageId::FitDefinition),
            5012 => Some(MessageId::FitData),
            5014 => Some(MessageId::WeatherRequest),
            5024 => Some(MessageId::DeviceInformation),
            5026 => Some(MessageId::DeviceSettings),
            5030 => Some(MessageId::SystemEvent),
            5031 => Some(MessageId::SupportedFileTypesRequest),
            5033 => Some(MessageId::NotificationUpdate),
            5034 => Some(MessageId::NotificationControl),
            5035 => Some(MessageId::NotificationData),
            5036 => Some(MessageId::NotificationSubscription),
            5037 => Some(MessageId::Synchronization),
            5039 => Some(MessageId::FindMyPhoneRequest),
            5040 => Some(MessageId::FindMyPhoneCancel),
            5041 => Some(MessageId::MusicControl),
            5042 => Some(MessageId::MusicControlCapabilities),
            5043 => Some(MessageId::ProtobufRequest),
            5044 => Some(MessageId::ProtobufResponse),
            5049 => Some(MessageId::MusicControlEntityUpdate),
            5050 => Some(MessageId::Configuration),
            5052 => Some(MessageId::CurrentTimeRequest),
            5101 => Some(MessageId::AuthNegotiation),
            _ => None,
        }
    }
}

/// Status codes for response messages
///
/// These match the Java GFDIMessage.Status enum values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ack = 0,
    Nack = 1,
    Unsupported = 2,
    DecodeError = 3,
    CrcError = 4,
    LengthError = 5,
}

impl Status {
    pub fn from_u8(code: u8) -> Option<Self> {
        match code {
            0 => Some(Status::Ack),
            1 => Some(Status::Nack),
            2 => Some(Status::Unsupported),
            3 => Some(Status::DecodeError),
            4 => Some(Status::CrcError),
            5 => Some(Status::LengthError),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Status::Ack => "ACK",
            Status::Nack => "NACK",
            Status::Unsupported => "UNSUPPORTED",
            Status::DecodeError => "DECODE_ERROR",
            Status::CrcError => "CRC_ERROR",
            Status::LengthError => "LENGTH_ERROR",
        }
    }
}

/// Parsed GFDI Message
#[derive(Debug, Clone)]
pub enum GfdiMessage {
    DeviceInformation(DeviceInformationMessage),
    Configuration(ConfigurationMessage),
    CurrentTimeRequest,
    NotificationControl(NotificationControlMessage),
    NotificationSubscription(NotificationSubscriptionMessage),
    Synchronization(SynchronizationMessage),
    FilterStatus(FilterStatusMessage),
    WeatherRequest(WeatherRequestMessage),
    Unknown { message_id: u16, data: Vec<u8> },
}

/// Device Information Message (incoming from watch)
#[derive(Debug, Clone)]
pub struct DeviceInformationMessage {
    pub protocol_version: u16,
    pub product_number: u16,
    pub unit_number: u32,
    pub software_version: u16,
    pub max_packet_size: u16,
    pub bluetooth_friendly_name: String,
    pub device_name: String,
    pub device_model: String,
}

/// Configuration Message (incoming from watch)
#[derive(Debug, Clone)]
pub struct ConfigurationMessage {
    pub capabilities: HashSet<u16>,
}

/// Notification Control Message (incoming from watch)
#[derive(Debug, Clone)]
pub struct NotificationControlMessage {
    pub notification_id: i32,
    pub command: u8,
    pub attributes: Vec<(u8, u16)>,    // (attribute_id, max_length)
    pub action_id: Option<u8>,         // For PERFORM_NOTIFICATION_ACTION (command 128)
    pub action_string: Option<String>, // For PERFORM_NOTIFICATION_ACTION (command 128)
}

/// Notification Subscription Message (incoming from watch)
#[derive(Debug, Clone)]
pub struct NotificationSubscriptionMessage {
    pub enable: bool,
    pub unk: u8,
}

/// Synchronization Message (incoming from watch)
#[derive(Debug, Clone)]
pub struct SynchronizationMessage {
    pub synchronization_type: u8,
    pub file_type_bitmask: u64,
}

impl SynchronizationMessage {
    /// Check if synchronization should proceed based on the bitmask
    pub fn should_proceed(&self) -> bool {
        const WORKOUTS: u64 = 1 << 3;
        const ACTIVITIES: u64 = 1 << 5;
        const ACTIVITY_SUMMARY: u64 = 1 << 21;
        const SLEEP: u64 = 1 << 26;

        (self.file_type_bitmask & (WORKOUTS | ACTIVITIES | ACTIVITY_SUMMARY | SLEEP)) != 0
    }
}

/// Filter Status Message (response to Filter message)
#[derive(Debug, Clone)]
pub struct FilterStatusMessage {
    pub status: Status,
    pub filter_type: u8,
}

/// Weather Request Message (5014)
#[derive(Debug, Clone)]
pub struct WeatherRequestMessage {
    pub format: u8,
    pub latitude: i32,
    pub longitude: i32,
    pub hours_of_forecast: u8,
}

/// Message parser
pub struct MessageParser;

impl MessageParser {
    /// Parse a GFDI message from raw bytes
    pub fn parse(data: &[u8]) -> Result<GfdiMessage> {
        if data.len() < 6 {
            return Err(GarminError::InvalidMessage("Message too short".to_string()));
        }

        let mut offset = 0;

        // Skip packet size field (2 bytes)
        offset += 2;

        // Read message ID (2 bytes, little-endian)
        let mut message_id = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;

        // Check for sequence number (bit 15 set)
        // If bit 15 is set, the message ID is encoded with a sequence number
        // Format: [bit 15: 1] [bits 14-8: sequence] [bits 7-0: message_id - 5000]
        // We need to decode it: actual_id = (raw_id & 0xFF) + 5000
        if (message_id & 0x8000) != 0 {
            message_id = (message_id & 0xFF) + 5000;
        }

        match MessageId::from_u16(message_id) {
            Some(MessageId::DeviceInformation) => Self::parse_device_information(&data[offset..]),
            Some(MessageId::Configuration) => Self::parse_configuration(&data[offset..]),
            Some(MessageId::CurrentTimeRequest) => Ok(GfdiMessage::CurrentTimeRequest),
            Some(MessageId::NotificationControl) => {
                Self::parse_notification_control(&data[offset..])
            }
            Some(MessageId::NotificationSubscription) => {
                Self::parse_notification_subscription(&data[offset..])
            }
            Some(MessageId::Synchronization) => Self::parse_synchronization(&data[offset..]),
            Some(MessageId::WeatherRequest) => Self::parse_weather_request(&data[offset..]),
            Some(MessageId::Response) => {
                // Check if this is a filter status response
                if offset + 2 < data.len() {
                    let original_msg_id = u16::from_le_bytes([data[offset], data[offset + 1]]);
                    if original_msg_id == 5007 {
                        return Self::parse_filter_status(&data[offset..]);
                    }
                }
                Ok(GfdiMessage::Unknown {
                    message_id,
                    data: data[offset..].to_vec(),
                })
            }
            _ => Ok(GfdiMessage::Unknown {
                message_id,
                data: data[offset..].to_vec(),
            }),
        }
    }

    fn parse_device_information(data: &[u8]) -> Result<GfdiMessage> {
        if data.len() < 10 {
            return Err(GarminError::InvalidMessage(
                "DeviceInformation too short".to_string(),
            ));
        }

        let mut offset = 0;

        let protocol_version = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;

        let product_number = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;

        let unit_number = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        let software_version = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;

        let max_packet_size = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;

        // Read length-prefixed strings (1 byte length, then N bytes of UTF-8 string)
        let bluetooth_friendly_name = Self::read_length_prefixed_string(&data[offset..])?;
        offset += 1 + bluetooth_friendly_name.len();

        let device_name = Self::read_length_prefixed_string(&data[offset..])?;
        offset += 1 + device_name.len();

        let device_model = Self::read_length_prefixed_string(&data[offset..])?;

        Ok(GfdiMessage::DeviceInformation(DeviceInformationMessage {
            protocol_version,
            product_number,
            unit_number,
            software_version,
            max_packet_size,
            bluetooth_friendly_name,
            device_name,
            device_model,
        }))
    }

    fn parse_configuration(data: &[u8]) -> Result<GfdiMessage> {
        if data.is_empty() {
            return Err(GarminError::InvalidMessage(
                "Configuration message empty".to_string(),
            ));
        }

        let num_bytes = data[0] as usize;
        if data.len() < 1 + num_bytes {
            return Err(GarminError::InvalidMessage(
                "Configuration data truncated".to_string(),
            ));
        }

        let capability_bytes = &data[1..1 + num_bytes];
        let capabilities = Self::parse_capabilities(capability_bytes);

        Ok(GfdiMessage::Configuration(ConfigurationMessage {
            capabilities,
        }))
    }

    fn parse_notification_control(data: &[u8]) -> Result<GfdiMessage> {
        // Debug: log the raw data
        if data.len() < 7 {
            return Err(GarminError::InvalidMessage(
                "NotificationControl message too short".to_string(),
            ));
        }

        let command = data[0];
        let notification_id = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);

        // Handle PERFORM_NOTIFICATION_ACTION (command 128)
        if command == 128 {
            if data.len() < 6 {
                return Err(GarminError::InvalidMessage(
                    "PERFORM_NOTIFICATION_ACTION message too short".to_string(),
                ));
            }

            let action_id = data[5];

            // Parse null-terminated action string (if present)
            // Action string is only present for reply actions, not for dismiss
            let mut action_string = None;
            if data.len() > 6 {
                let string_data = &data[6..];
                if let Some(null_pos) = string_data.iter().position(|&b| b == 0) {
                    // Only parse if there's actual data before the null terminator
                    if null_pos > 0 {
                        if let Ok(s) = std::str::from_utf8(&string_data[..null_pos]) {
                            // Only set if it's valid, non-empty, printable text
                            if !s.is_empty()
                                && s.chars().all(|c| !c.is_control() || c == '\n' || c == '\r')
                            {
                                action_string = Some(s.to_string());
                            }
                        }
                    }
                }
            }

            return Ok(GfdiMessage::NotificationControl(
                NotificationControlMessage {
                    notification_id,
                    command,
                    attributes: Vec::new(),
                    action_id: Some(action_id),
                    action_string,
                },
            ));
        }

        // Parse requested attributes (for GET_NOTIFICATION_ATTRIBUTES, command 0)
        let mut attributes = Vec::new();
        let mut offset = 5;

        for _i in 0..data.len() {
            if offset + 3 > data.len() {
                // Watch claimed more attributes than provided in packet
                // This is normal - packet may be truncated due to MTU or watch only needs subset
                break;
            }
            let attr_id = data[offset];

            if attr_id == 0 || attr_id == 4 || attr_id == 5 {
                // HACK, we want to actually handle atributes
                attributes.push((attr_id, 128));
                offset += 1;
                continue;
            }

            if attr_id == 127 {
                attributes.push((attr_id, data[offset + 1] as u16));
                // attr_id
                offset += 1;
                // short
                offset += 1;
                // byte;
                offset += 2;
                continue;
            }

            let max_len = u16::from_le_bytes([data[offset + 1], data[offset + 2]]);
            attributes.push((attr_id, max_len));
            offset += 3;
        }

        Ok(GfdiMessage::NotificationControl(
            NotificationControlMessage {
                notification_id,
                command,
                attributes,
                action_id: None,
                action_string: None,
            },
        ))
    }

    fn parse_notification_subscription(data: &[u8]) -> Result<GfdiMessage> {
        if data.len() < 2 {
            return Err(GarminError::InvalidMessage(
                "NotificationSubscription message too short".to_string(),
            ));
        }

        let enable = data[0] == 1;
        let unk = data[1];

        Ok(GfdiMessage::NotificationSubscription(
            NotificationSubscriptionMessage { enable, unk },
        ))
    }

    fn parse_synchronization(data: &[u8]) -> Result<GfdiMessage> {
        if data.len() < 2 {
            return Err(GarminError::InvalidMessage(
                "Synchronization message too short".to_string(),
            ));
        }

        let synchronization_type = data[0];
        let size = data[1] as usize;

        if data.len() < 2 + size {
            return Err(GarminError::InvalidMessage(
                "Synchronization message truncated".to_string(),
            ));
        }

        let file_type_bitmask = if size == 8 {
            u64::from_le_bytes([
                data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
            ])
        } else if size == 4 {
            u32::from_le_bytes([data[2], data[3], data[4], data[5]]) as u64
        } else {
            return Err(GarminError::InvalidMessage(format!(
                "Unexpected synchronization bitmask size: {}",
                size
            )));
        };

        Ok(GfdiMessage::Synchronization(SynchronizationMessage {
            synchronization_type,
            file_type_bitmask,
        }))
    }

    fn parse_weather_request(data: &[u8]) -> Result<GfdiMessage> {
        if data.len() < 10 {
            return Err(GarminError::InvalidMessage(
                "Weather request message too short".to_string(),
            ));
        }

        let format = data[0];
        let latitude = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
        let longitude = i32::from_le_bytes([data[5], data[6], data[7], data[8]]);
        let hours_of_forecast = data[9];

        Ok(GfdiMessage::WeatherRequest(WeatherRequestMessage {
            format,
            latitude,
            longitude,
            hours_of_forecast,
        }))
    }

    fn parse_filter_status(data: &[u8]) -> Result<GfdiMessage> {
        if data.len() < 3 {
            return Err(GarminError::InvalidMessage(
                "Filter status message too short".to_string(),
            ));
        }

        // Skip original message ID (2 bytes)
        let status_byte = data[2];
        let status = match status_byte {
            0 => Status::Ack,
            1 => Status::Nack,
            2 => Status::Unsupported,
            _ => {
                return Err(GarminError::InvalidMessage(format!(
                    "Unknown status: {}",
                    status_byte
                )))
            }
        };

        let filter_type = if data.len() > 3 { data[3] } else { 0 };

        Ok(GfdiMessage::FilterStatus(FilterStatusMessage {
            status,
            filter_type,
        }))
    }

    fn read_length_prefixed_string(data: &[u8]) -> Result<String> {
        if data.is_empty() {
            return Err(GarminError::InvalidMessage(
                "No length byte for string".to_string(),
            ));
        }

        let length = data[0] as usize;

        if data.len() < 1 + length {
            return Err(GarminError::InvalidMessage(format!(
                "String data too short: need {} bytes, have {}",
                1 + length,
                data.len()
            )));
        }

        String::from_utf8(data[1..1 + length].to_vec())
            .map_err(|_| GarminError::InvalidMessage("Invalid UTF-8 in string".to_string()))
    }

    fn parse_capabilities(bytes: &[u8]) -> HashSet<u16> {
        let mut capabilities = HashSet::new();
        let mut current = 0u16;

        for &byte in bytes {
            for i in 0..8 {
                if (byte & (1 << i)) != 0 {
                    capabilities.insert(current);
                }
                current += 1;
            }
        }

        capabilities
    }
}

/// Message generator for responses
pub struct MessageGenerator;

impl MessageGenerator {
    /// Generate a DeviceInformation response
    pub fn device_information_response(incoming: &DeviceInformationMessage) -> Result<Vec<u8>> {
        let mut response = Vec::new();

        // Packet size placeholder (will be filled at end)
        response.extend_from_slice(&[0u8, 0u8]);

        // Message ID: RESPONSE (5000)
        response.extend_from_slice(&5000u16.to_le_bytes());

        // Original message ID: DEVICE_INFORMATION (5024)
        response.extend_from_slice(&5024u16.to_le_bytes());

        // Status: ACK
        response.push(Status::Ack as u8);

        // Our protocol version (150 = version 1.50)
        response.extend_from_slice(&150u16.to_le_bytes());

        // Our product number (-1 = 0xFFFF for phone)
        response.extend_from_slice(&0xFFFFu16.to_le_bytes());

        // Our unit number (-1)
        response.extend_from_slice(&0xFFFFFFFFu32.to_le_bytes());

        // Our software version (7791 = version 77.91, matching Gadgetbridge)
        response.extend_from_slice(&7791u16.to_le_bytes());

        // Our max packet size (-1 = 0xFFFF means no limit)
        response.extend_from_slice(&0xFFFFu16.to_le_bytes());

        // Bluetooth name (null-terminated)
        let bt_name = "Gadgetbridge-Rust";
        response.extend_from_slice(bt_name.as_bytes());
        response.push(0);

        // Device manufacturer (null-terminated)
        let manufacturer = "Gadgetbridge";
        response.extend_from_slice(manufacturer.as_bytes());
        response.push(0);

        // Device model (null-terminated)
        let model = "Linux";
        response.extend_from_slice(model.as_bytes());
        response.push(0);

        // Protocol flags (1 for v1.x, 0 for v2.x)
        let protocol_flags = if incoming.protocol_version / 100 == 1 {
            1u8
        } else {
            0u8
        };
        response.push(protocol_flags);

        // Fill in packet size (total length + 2 for checksum)
        let packet_size = (response.len() + 2) as u16;
        response[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&response);
        response.extend_from_slice(&checksum.to_le_bytes());

        Ok(response)
    }

    /// Generate a Configuration response
    pub fn configuration_response() -> Result<Vec<u8>> {
        let mut response = Vec::new();

        // Packet size placeholder
        response.extend_from_slice(&[0u8, 0u8]);

        // Message ID: CONFIGURATION (5050)
        response.extend_from_slice(&5050u16.to_le_bytes());

        // Generate our capabilities
        let capabilities = Self::generate_capabilities();

        // Number of capability bytes
        response.push(capabilities.len() as u8);

        // Capability bytes
        response.extend_from_slice(&capabilities);

        // Fill in packet size
        let packet_size = (response.len() + 2) as u16;
        response[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&response);
        response.extend_from_slice(&checksum.to_le_bytes());

        Ok(response)
    }

    /// Generate a simple ACK response
    pub fn ack_response(message_id: u16) -> Result<Vec<u8>> {
        let mut response = Vec::new();

        // Packet size
        response.extend_from_slice(&9u16.to_le_bytes()); // Fixed size for ACK

        // Message ID: RESPONSE (5000)
        response.extend_from_slice(&5000u16.to_le_bytes());

        // Original message ID
        response.extend_from_slice(&message_id.to_le_bytes());

        // Status: ACK
        response.push(Status::Ack as u8);

        // Add checksum
        let checksum = Self::compute_checksum(&response);
        response.extend_from_slice(&checksum.to_le_bytes());

        Ok(response)
    }

    /// Generate capabilities bitmap (matching Gadgetbridge's OUR_CAPABILITIES)
    fn generate_capabilities() -> Vec<u8> {
        // Create a 120-bit (15-byte) capability bitmap
        let mut capabilities = vec![0u8; 15];

        // Set ALL capabilities (0-119) except the unknown/unsupported ones
        // This matches Gadgetbridge Java's OUR_CAPABILITIES behavior
        let unsupported = [
            104, 105, 106, 107, 108, 109, 110, 111, // UNK_104-111
            114, 115, 116, 117, 118, 119, // UNK_114-119
        ];

        for cap in 0..120 {
            // Skip unsupported capabilities
            if unsupported.contains(&cap) {
                continue;
            }

            let byte_idx = cap / 8;
            let bit_idx = cap % 8;
            if byte_idx < capabilities.len() {
                capabilities[byte_idx] |= 1 << bit_idx;
            }
        }

        capabilities
    }

    /// Compute CRC-16 checksum (matching Garmin's algorithm)
    fn compute_checksum(data: &[u8]) -> u16 {
        let mut crc: u16 = 0;

        for &byte in data {
            crc ^= byte as u16;

            for _ in 0..8 {
                if (crc & 1) != 0 {
                    crc = (crc >> 1) ^ 0xA001;
                } else {
                    crc >>= 1;
                }
            }
        }

        crc
    }

    /// Generate a CurrentTimeRequest response with current time
    pub fn current_time_response() -> Result<Vec<u8>> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut response = Vec::new();

        // Packet size placeholder
        response.extend_from_slice(&[0u8, 0u8]);

        // Message ID: RESPONSE (5000)
        response.extend_from_slice(&5000u16.to_le_bytes());

        // Original message ID: CURRENT_TIME_REQUEST (5052)
        response.extend_from_slice(&5052u16.to_le_bytes());

        // Status: ACK
        response.push(Status::Ack as u8);

        // Current time as Garmin timestamp (seconds since Dec 31, 1989 00:00:00 UTC)
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        // Garmin epoch is 631065600 seconds after Unix epoch
        let garmin_time = now.wrapping_sub(631065600);

        response.extend_from_slice(&garmin_time.to_le_bytes());

        // Fill in packet size
        let packet_size = (response.len() + 2) as u16;
        response[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&response);
        response.extend_from_slice(&checksum.to_le_bytes());

        Ok(response)
    }

    /// Generate a Filter message
    pub fn filter_message(filter_type: u8) -> Result<Vec<u8>> {
        let mut message = Vec::new();

        // Packet size placeholder (will be filled at end)
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: FILTER (5007)
        message.extend_from_slice(&5007u16.to_le_bytes());

        // Filter type
        message.push(filter_type);

        // Fill in packet size
        let packet_size = (message.len() - 2) as u16;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        Ok(message)
    }

    /// Generate an ACK response for a Synchronization message
    pub fn synchronization_ack() -> Result<Vec<u8>> {
        let mut response = Vec::new();

        // Packet size placeholder
        response.extend_from_slice(&[0u8, 0u8]);

        // Message ID: RESPONSE (5000)
        response.extend_from_slice(&5000u16.to_le_bytes());

        // Original message ID: SYNCHRONIZATION (5037)
        response.extend_from_slice(&5037u16.to_le_bytes());

        // Status: ACK
        response.push(Status::Ack as u8);

        // Fill in packet size
        let packet_size = (response.len() - 2) as u16;
        response[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&response);
        response.extend_from_slice(&checksum.to_le_bytes());

        Ok(response)
    }

    /// Generate a weather response ACK
    ///
    /// This sends a simple ACK to weather requests. The actual weather data
    /// is sent separately via FIT messages (FitDefinition and FitData).
    ///
    /// # Arguments
    /// * `_request` - The incoming WeatherRequestMessage
    ///
    /// # Returns
    /// A response message with format:
    /// - Message ID: RESPONSE (5000)
    /// - Original Message ID: WEATHER_REQUEST (5014)
    /// - Status: ACK (0)
    pub fn weather_response(_request: &WeatherRequestMessage) -> Result<Vec<u8>> {
        let mut response = Vec::new();

        // Packet size placeholder
        response.extend_from_slice(&[0u8, 0u8]);

        // Message ID: RESPONSE (5000)
        response.extend_from_slice(&5000u16.to_le_bytes());

        // Original message ID: WEATHER_REQUEST (5014)
        response.extend_from_slice(&5014u16.to_le_bytes());

        // Status: ACK
        response.push(Status::Ack as u8);

        // Fill in packet size
        let packet_size = (response.len() - 2) as u16;
        response[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&response);
        response.extend_from_slice(&checksum.to_le_bytes());

        Ok(response)
    }

    /// Generate a FIT Definition message (5011)
    ///
    /// This wraps FIT definition data in a GFDI message envelope for BLE transmission.
    ///
    /// # Arguments
    /// * `fit_definition_data` - The encoded FIT definition message bytes
    ///
    /// # Returns
    /// A GFDI message containing the FIT definition
    pub fn fit_definition_message(fit_definition_data: &[u8]) -> Result<Vec<u8>> {
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: FIT_DEFINITION (5011)
        message.extend_from_slice(&5011u16.to_le_bytes());

        // FIT definition payload
        message.extend_from_slice(fit_definition_data);

        // Fill in packet size
        let packet_size = (message.len() - 2) as u16;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        Ok(message)
    }

    /// Generate a FIT Data message (5012)
    ///
    /// This wraps FIT data in a GFDI message envelope for BLE transmission.
    ///
    /// # Arguments
    /// * `fit_data` - The encoded FIT data message bytes
    ///
    /// # Returns
    /// A GFDI message containing the FIT data
    pub fn fit_data_message(fit_data: &[u8]) -> Result<Vec<u8>> {
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: FIT_DATA (5012)
        message.extend_from_slice(&5012u16.to_le_bytes());

        // FIT data payload
        message.extend_from_slice(fit_data);

        // Fill in packet size
        let packet_size = (message.len() - 2) as u16;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        Ok(message)
    }

    /// Generate a NotificationSubscription response
    ///
    /// This responds to the watch's notification subscription request (message ID 5036)
    /// with an ACK and notification status information.
    ///
    /// # Arguments
    /// * `incoming` - The incoming NotificationSubscriptionMessage
    /// * `enabled` - Whether notifications are enabled (typically true)
    ///
    /// # Returns
    /// A response message with format:
    /// - Message ID: RESPONSE (5000)
    /// - Original Message ID: NOTIFICATION_SUBSCRIPTION (5036)
    /// - Status: ACK (0)
    /// - Notification Status: ENABLED (0) or DISABLED (1)
    /// - Enable flag: 1 (enabled) or 0 (disabled)
    /// - Unknown byte: copied from incoming message
    pub fn notification_subscription_response(
        incoming: &NotificationSubscriptionMessage,
        enabled: bool,
    ) -> Result<Vec<u8>> {
        let mut response = Vec::new();

        // Packet size placeholder
        response.extend_from_slice(&[0u8, 0u8]);

        // Message ID: RESPONSE (5000)
        response.extend_from_slice(&5000u16.to_le_bytes());

        // Original message ID: NOTIFICATION_SUBSCRIPTION (5036)
        response.extend_from_slice(&5036u16.to_le_bytes());

        // Status: ACK
        response.push(Status::Ack as u8);

        // Notification Status (0 = ENABLED, 1 = DISABLED)
        response.push(if enabled { 0 } else { 1 });

        // Enable flag (matches incoming request)
        response.push(if incoming.enable { 1 } else { 0 });

        // Unknown byte (copy from incoming)
        response.push(incoming.unk);

        // Fill in packet size
        let packet_size = (response.len() + 2) as u16;
        response[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&response);
        response.extend_from_slice(&checksum.to_le_bytes());

        Ok(response)
    }

    /// Generate a SupportedFileTypesRequest message
    ///
    /// This proactively asks the watch what file types it supports.
    /// Sent after Configuration exchange during initialization.
    ///
    /// Message format: Just the message ID, no payload
    pub fn supported_file_types_request() -> Result<Vec<u8>> {
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: SUPPORTED_FILE_TYPES_REQUEST (5031)
        message.extend_from_slice(&5031u16.to_le_bytes());

        // Fill in packet size
        let packet_size = (message.len() + 2) as u16;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        Ok(message)
    }

    /// Generate a SetDeviceSettings message
    ///
    /// Sends device settings to the watch (auto upload, weather, etc.)
    /// Sent after Configuration exchange during initialization.
    ///
    /// # Arguments
    /// * `auto_upload` - Enable auto upload of activities
    /// * `weather_conditions` - Enable weather conditions
    /// * `weather_alerts` - Enable weather alerts
    pub fn set_device_settings(
        auto_upload: bool,
        weather_conditions: bool,
        weather_alerts: bool,
    ) -> Result<Vec<u8>> {
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: DEVICE_SETTINGS (5026)
        message.extend_from_slice(&5026u16.to_le_bytes());

        // Number of settings (always 3 for now)
        message.push(3);

        // Setting 1: AUTO_UPLOAD_ENABLED (index 6)
        message.push(6); // GarminDeviceSetting.AUTO_UPLOAD_ENABLED ordinal
        message.push(1); // Data length: 1 byte
        message.push(if auto_upload { 1 } else { 0 });

        // Setting 2: WEATHER_CONDITIONS_ENABLED (index 7)
        message.push(7); // GarminDeviceSetting.WEATHER_CONDITIONS_ENABLED ordinal
        message.push(1); // Data length: 1 byte
        message.push(if weather_conditions { 1 } else { 0 });

        // Setting 3: WEATHER_ALERTS_ENABLED (index 8)
        message.push(8); // GarminDeviceSetting.WEATHER_ALERTS_ENABLED ordinal
        message.push(1); // Data length: 1 byte
        message.push(if weather_alerts { 1 } else { 0 });

        // Fill in packet size
        let packet_size = (message.len() + 2) as u16;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        Ok(message)
    }

    /// Generate a SystemEvent message
    ///
    /// Sends system events to the watch (PAIR_START, SYNC_READY, etc.)
    ///
    /// # Arguments
    /// * `event_type` - The event type ordinal (see GarminSystemEventType in Java)
    /// * `value` - The event value (typically 0)
    ///
    /// Event types:
    /// - 0: SYNC_COMPLETE
    /// - 1: SYNC_FAIL
    /// - 2: FACTORY_RESET
    /// - 3: PAIR_START
    /// - 4: PAIR_COMPLETE
    /// - 5: PAIR_FAIL
    /// - 6: HOST_DID_ENTER_FOREGROUND
    /// - 7: HOST_DID_ENTER_BACKGROUND
    /// - 8: SYNC_READY
    /// - 9: NEW_DOWNLOAD_AVAILABLE
    /// - 10: DEVICE_SOFTWARE_UPDATE
    /// - 11: DEVICE_DISCONNECT
    /// - 12: TUTORIAL_COMPLETE
    /// - 13: SETUP_WIZARD_START
    /// - 14: SETUP_WIZARD_COMPLETE
    /// - 15: SETUP_WIZARD_SKIPPED
    /// - 16: TIME_UPDATED
    pub fn system_event(event_type: u8, value: u8) -> Result<Vec<u8>> {
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: SYSTEM_EVENT (5030)
        message.extend_from_slice(&5030u16.to_le_bytes());

        // Event type
        message.push(event_type);

        // Value
        message.push(value);

        // Fill in packet size
        let packet_size = (message.len() + 2) as u16;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        Ok(message)
    }

    /// Generate a ProtobufRequest for battery status updates
    ///
    /// This sends a protobuf request asking the watch to provide battery status updates.
    /// Sent during initialization (completeInitialization in Java).
    ///
    /// The protobuf structure is:
    /// ```proto
    /// Smart {
    ///   device_status_service = 8 {
    ///     remote_device_battery_status_request = 2 {
    ///       // empty message
    ///     }
    ///   }
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `request_id` - The protobuf request ID (incrementing counter)
    pub fn protobuf_battery_status_request(request_id: u16) -> Result<Vec<u8>> {
        // Build the protobuf payload manually
        // Smart.device_status_service (field 8, type length-delimited)
        // DeviceStatusService.remote_device_battery_status_request (field 2, type length-delimited)

        // Inner message: RemoteDeviceBatteryStatusRequest (empty)
        let inner_message = vec![];

        // DeviceStatusService with field 2 (remote_device_battery_status_request)
        let mut device_status_service = Vec::new();
        device_status_service.push((2 << 3) | 2); // Field 2, wire type 2 (length-delimited)
        device_status_service.push(inner_message.len() as u8); // Length of inner message (0)
        device_status_service.extend_from_slice(&inner_message);

        // Smart with field 8 (device_status_service)
        let mut smart_proto = Vec::new();
        smart_proto.push((8 << 3) | 2); // Field 8, wire type 2 (length-delimited)
        smart_proto.push(device_status_service.len() as u8); // Length
        smart_proto.extend_from_slice(&device_status_service);

        // Now build the ProtobufRequest message
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: PROTOBUF_REQUEST (5043)
        message.extend_from_slice(&5043u16.to_le_bytes());

        // Request ID
        message.extend_from_slice(&request_id.to_le_bytes());

        // Data offset (0 for non-chunked)
        message.extend_from_slice(&0u32.to_le_bytes());

        // Total protobuf length
        message.extend_from_slice(&(smart_proto.len() as u32).to_le_bytes());

        // Protobuf data length (same as total for non-chunked)
        message.extend_from_slice(&(smart_proto.len() as u32).to_le_bytes());

        // Protobuf payload
        message.extend_from_slice(&smart_proto);

        // Fill in packet size
        let packet_size = (message.len() + 2) as u16;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum
        let checksum = Self::compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        Ok(message)
    }

    /// Generate a NotificationData message with notification content
    ///
    /// This sends the actual notification content (sender, body, title, etc.)
    /// in response to the watch's NotificationControl request.
    ///
    /// # Arguments
    /// * `notification_id` - The notification ID from NotificationControl
    /// * `requested_attributes` - List of (attribute_id, max_length) from NotificationControl
    /// * `title` - Notification title
    /// * `body` - Notification body/message
    /// * `sender` - Notification sender/source
    /// * `timestamp` - Timestamp in format "yyyyMMddTHHmmss"
    /// * `app_id` - Application identifier
    pub fn notification_data(
        notification_id: u32,
        requested_attributes: &[(u8, u16)],
        title: &str,
        body: &str,
        sender: &str,
        timestamp: &str,
        app_id: &str,
    ) -> Result<Vec<u8>> {
        Self::notification_data_with_actions(
            notification_id,
            requested_attributes,
            title,
            body,
            sender,
            timestamp,
            app_id,
            true, // has_dismiss_action
        )
    }

    /// Generate NotificationData message with action support
    /// * `notification_id` - Unique notification identifier
    /// * `requested_attributes` - List of (attribute_id, max_length) tuples
    /// * `title` - Notification title
    /// * `body` - Notification body text
    /// * `sender` - Notification sender
    /// * `timestamp` - Timestamp in format "yyyyMMddTHHmmss"
    /// * `app_id` - Application identifier
    /// * `has_dismiss_action` - Whether to include a dismiss action
    pub fn notification_data_with_actions(
        notification_id: u32,
        requested_attributes: &[(u8, u16)],
        title: &str,
        body: &str,
        sender: &str,
        timestamp: &str,
        app_id: &str,
        has_dismiss_action: bool,
    ) -> Result<Vec<u8>> {
        // Build the notification data payload
        let mut payload = Vec::new();

        // Command byte: GET_NOTIFICATION_ATTRIBUTES = 0
        payload.push(0);

        // Notification ID
        payload.extend_from_slice(&notification_id.to_le_bytes());

        // Process all requested attributes except MESSAGE_SIZE first (matching Java implementation)
        let mut message_size_entry: Option<(u8, u16)> = None;

        for (attr_id, max_len) in requested_attributes {
            // Save MESSAGE_SIZE for last
            if *attr_id == 4 {
                message_size_entry = Some((*attr_id, *max_len));
                continue;
            }

            let value = match attr_id {
                0 => app_id,    // APP_IDENTIFIER
                1 => title,     // TITLE
                2 => "",        // SUBTITLE (empty for most notifications)
                3 => body,      // MESSAGE
                5 => timestamp, // DATE
                9 => sender,    // SENDER
                127 => {
                    // ACTIONS
                    if has_dismiss_action {
                        // Encode actions list with DISMISS_NOTIFICATION action (code 98)
                        // Format: [count(1), action_code(1), icon_position(1), title_len(1), title...]
                        // DISMISS_NOTIFICATION = 98, LEFT position = 0x04 (bit 2), title = "Dismiss"
                        // Icon positions are bit vectors: BOTTOM=0x01, RIGHT=0x02, LEFT=0x04
                        let dismiss_title = "Dismiss";
                        let mut actions_bytes = Vec::new();
                        actions_bytes.push(1u8); // 1 action
                        actions_bytes.push(98u8); // DISMISS_NOTIFICATION action code
                        actions_bytes.push(0x04u8); // LEFT icon position (bit 2)
                        actions_bytes.push(dismiss_title.len() as u8);
                        actions_bytes.extend_from_slice(dismiss_title.as_bytes());

                        // Write attribute with binary data
                        payload.push(*attr_id);
                        payload.extend_from_slice(&(actions_bytes.len() as u16).to_le_bytes());
                        payload.extend_from_slice(&actions_bytes);
                        continue;
                    } else {
                        // No actions - send empty action list
                        payload.push(*attr_id);
                        payload.extend_from_slice(&4u16.to_le_bytes()); // 4 bytes for empty list
                        payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
                        continue;
                    }
                }
                _ => continue, // Skip unknown attributes
            };

            // Truncate if max_len is specified and not 0xFFFF (no limit)
            let truncated = if *max_len > 0 && *max_len < 0xFFFF {
                Self::truncate_utf8(value, *max_len as usize)
            } else {
                value.to_string()
            };

            // Write attribute: [id][length][value]
            payload.push(*attr_id);
            payload.extend_from_slice(&(truncated.len() as u16).to_le_bytes());
            payload.extend_from_slice(truncated.as_bytes());
        }

        // Now process MESSAGE_SIZE last (Java does this to match protocol expectations)
        if let Some((attr_id, max_len)) = message_size_entry {
            // MESSAGE_SIZE: Length of the body as a string
            // Java reference: Integer.toString(body.length())
            let value = body.len().to_string();

            let truncated = if max_len > 0 && max_len < 0xFFFF {
                Self::truncate_utf8(&value, max_len as usize)
            } else {
                value
            };

            // Write attribute: [id][length][value]
            payload.push(attr_id);
            payload.extend_from_slice(&(truncated.len() as u16).to_le_bytes());
            payload.extend_from_slice(truncated.as_bytes());
        }

        // Calculate CRC of the payload data
        let data_crc = Self::compute_crc16(&payload);

        // Build the NotificationData message
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: NOTIFICATION_DATA (5035)
        message.extend_from_slice(&5035u16.to_le_bytes());

        // Message size (total payload size)
        message.extend_from_slice(&(payload.len() as u16).to_le_bytes());

        // Data CRC
        message.extend_from_slice(&data_crc.to_le_bytes());

        // Data offset (0 for non-chunked messages)
        message.extend_from_slice(&0u16.to_le_bytes());

        // Payload
        message.extend_from_slice(&payload);

        // Fill in packet size
        let packet_size = (message.len() + 2) as u16;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add envelope checksum
        let checksum = Self::compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        Ok(message)
    }

    /// Truncate a UTF-8 string to a maximum byte length, ensuring valid UTF-8
    fn truncate_utf8(s: &str, max_bytes: usize) -> String {
        if s.len() <= max_bytes {
            return s.to_string();
        }

        // Find the character boundary at or before max_bytes
        let mut boundary = max_bytes;
        while boundary > 0 && !s.is_char_boundary(boundary) {
            boundary -= 1;
        }

        s[..boundary].to_string()
    }

    /// Compute CRC-16 for notification data using Garmin's algorithm
    /// This matches the Java ChecksumCalculator.computeCrc implementation
    fn compute_crc16(data: &[u8]) -> u16 {
        const CONSTANTS: [u16; 16] = [
            0x0000, 0xCC01, 0xD801, 0x1400, 0xF001, 0x3C00, 0x2800, 0xE401, 0xA001, 0x6C00, 0x7800,
            0xB401, 0x5000, 0x9C01, 0x8801, 0x4400,
        ];

        let mut crc: u16 = 0;
        for &byte in data {
            let b = byte as u16;
            crc = (((crc >> 4) & 0x0FFF) ^ CONSTANTS[(crc & 0x0F) as usize])
                ^ CONSTANTS[(b & 0x0F) as usize];
            crc = (((crc >> 4) & 0x0FFF) ^ CONSTANTS[(crc & 0x0F) as usize])
                ^ CONSTANTS[((b >> 4) & 0x0F) as usize];
        }
        crc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_id_conversion() {
        assert_eq!(
            MessageId::from_u16(5024),
            Some(MessageId::DeviceInformation)
        );
        assert_eq!(MessageId::from_u16(5050), Some(MessageId::Configuration));
        assert_eq!(MessageId::from_u16(9999), None);
    }

    #[test]
    fn test_capabilities_generation() {
        let caps = MessageGenerator::generate_capabilities();
        assert_eq!(caps.len(), 15);

        // Check that GNCS (bit 6) is set
        assert_eq!(caps[0] & (1 << 6), 1 << 6);

        // Check that CALENDAR (bit 42 = byte 5, bit 2) is set
        assert_eq!(caps[5] & (1 << 2), 1 << 2);
    }

    #[test]
    fn test_checksum() {
        let data = vec![0x09, 0x00, 0x88, 0x13, 0xA0, 0x13, 0x00];
        let checksum = MessageGenerator::compute_checksum(&data);
        // The actual checksum value depends on the CRC algorithm
        assert!(checksum != 0); // Just verify it computes something
    }

    #[test]
    fn test_ack_response_format() {
        let ack = MessageGenerator::ack_response(5024).unwrap();

        // Check structure
        assert!(ack.len() >= 9); // Minimum ACK size
        assert_eq!(u16::from_le_bytes([ack[0], ack[1]]), 9); // Packet size
        assert_eq!(u16::from_le_bytes([ack[2], ack[3]]), 5000); // Response message ID
        assert_eq!(u16::from_le_bytes([ack[4], ack[5]]), 5024); // Original message ID
        assert_eq!(ack[6], Status::Ack as u8); // Status
    }

    #[test]
    fn test_synchronization_message_parsing() {
        // Create a synchronization message with 8-byte bitmask
        let mut data = vec![];
        data.extend_from_slice(&0u16.to_le_bytes()); // Packet size (placeholder)
        data.extend_from_slice(&5037u16.to_le_bytes()); // Message ID
        data.push(1); // Synchronization type
        data.push(8); // Bitmask size
        data.extend_from_slice(&0x0200_0028u64.to_le_bytes()); // Bitmask with WORKOUTS, ACTIVITIES, ACTIVITY_SUMMARY

        let result = MessageParser::parse(&data).unwrap();
        match result {
            GfdiMessage::Synchronization(msg) => {
                assert_eq!(msg.synchronization_type, 1);
                assert_eq!(msg.file_type_bitmask, 0x0200_0028);
                assert!(msg.should_proceed());
            }
            _ => panic!("Expected Synchronization message"),
        }
    }

    #[test]
    fn test_protobuf_battery_status_request() {
        let request_id = 1;
        let msg = MessageGenerator::protobuf_battery_status_request(request_id).unwrap();

        // Verify message structure
        assert!(msg.len() > 20); // Should have header + protobuf payload

        // Check packet size field
        let packet_size = u16::from_le_bytes([msg[0], msg[1]]);
        assert_eq!(packet_size as usize, msg.len());

        // Check message ID (PROTOBUF_REQUEST = 5043)
        let message_id = u16::from_le_bytes([msg[2], msg[3]]);
        assert_eq!(message_id, 5043);

        // Check request ID
        let req_id = u16::from_le_bytes([msg[4], msg[5]]);
        assert_eq!(req_id, request_id);

        // Check data offset (should be 0 for non-chunked)
        let data_offset = u32::from_le_bytes([msg[6], msg[7], msg[8], msg[9]]);
        assert_eq!(data_offset, 0);

        // Check total protobuf length
        let total_length = u32::from_le_bytes([msg[10], msg[11], msg[12], msg[13]]);
        assert!(total_length > 0);

        // Check protobuf data length (should equal total length for non-chunked)
        let data_length = u32::from_le_bytes([msg[14], msg[15], msg[16], msg[17]]);
        assert_eq!(data_length, total_length);

        // Verify protobuf payload structure
        // Smart.device_status_service (field 8) should be present
        let protobuf_start = 18;
        let protobuf_payload = &msg[protobuf_start..msg.len() - 2]; // Exclude CRC

        // First byte should be field tag for device_status_service (field 8, wire type 2)
        assert_eq!(protobuf_payload[0], (8 << 3) | 2);

        // Second byte is length of device_status_service
        let service_length = protobuf_payload[1] as usize;
        assert!(service_length > 0);

        // Inside device_status_service, should have remote_device_battery_status_request (field 2)
        assert_eq!(protobuf_payload[2], (2 << 3) | 2);

        // Length of remote_device_battery_status_request (should be 0, it's empty)
        assert_eq!(protobuf_payload[3], 0);
    }

    #[test]
    fn test_synchronization_should_proceed() {
        // Test with WORKOUTS bit set (bit 3)
        let msg = SynchronizationMessage {
            synchronization_type: 1,
            file_type_bitmask: 1 << 3,
        };
        assert!(msg.should_proceed());

        // Test with ACTIVITIES bit set (bit 5)
        let msg = SynchronizationMessage {
            synchronization_type: 1,
            file_type_bitmask: 1 << 5,
        };
        assert!(msg.should_proceed());

        // Test with ACTIVITY_SUMMARY bit set (bit 21)
        let msg = SynchronizationMessage {
            synchronization_type: 1,
            file_type_bitmask: 1 << 21,
        };
        assert!(msg.should_proceed());

        // Test with SLEEP bit set (bit 26)
        let msg = SynchronizationMessage {
            synchronization_type: 1,
            file_type_bitmask: 1 << 26,
        };
        assert!(msg.should_proceed());

        // Test with no relevant bits set
        let msg = SynchronizationMessage {
            synchronization_type: 1,
            file_type_bitmask: 1 << 10,
        };
        assert!(!msg.should_proceed());
    }

    #[test]
    fn test_filter_message_generation() {
        let filter = MessageGenerator::filter_message(3).unwrap();

        // Check structure
        assert!(filter.len() >= 7); // Minimum size with checksum
        assert_eq!(u16::from_le_bytes([filter[2], filter[3]]), 5007); // Filter message ID
        assert_eq!(filter[4], 3); // Filter type
    }

    #[test]
    fn test_synchronization_ack_generation() {
        let ack = MessageGenerator::synchronization_ack().unwrap();

        // Check structure
        assert!(ack.len() >= 9); // Minimum ACK size
        assert_eq!(u16::from_le_bytes([ack[2], ack[3]]), 5000); // Response message ID
        assert_eq!(u16::from_le_bytes([ack[4], ack[5]]), 5037); // Original message ID (Synchronization)
        assert_eq!(ack[6], Status::Ack as u8); // Status
    }

    #[test]
    fn test_filter_status_parsing() {
        // Create a filter status response
        let mut data = vec![];
        data.extend_from_slice(&0u16.to_le_bytes()); // Packet size (placeholder)
        data.extend_from_slice(&5000u16.to_le_bytes()); // Response message ID
        data.extend_from_slice(&5007u16.to_le_bytes()); // Original message ID (Filter)
        data.push(Status::Ack as u8); // Status
        data.push(3); // Filter type

        let result = MessageParser::parse(&data).unwrap();
        match result {
            GfdiMessage::FilterStatus(msg) => {
                assert_eq!(msg.status, Status::Ack);
                assert_eq!(msg.filter_type, 3);
            }
            _ => panic!("Expected FilterStatus message"),
        }
    }

    #[test]
    fn test_message_id_new_messages() {
        assert_eq!(MessageId::from_u16(5007), Some(MessageId::Filter));
        assert_eq!(MessageId::from_u16(5037), Some(MessageId::Synchronization));
    }
}
