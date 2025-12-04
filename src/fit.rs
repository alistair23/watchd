//! FIT (Flexible and Interoperable Data Transfer) Protocol Implementation
//!
//! This module implements the FIT protocol for encoding weather data messages
//! to be sent to Garmin devices via BLE.
//!
//! The FIT protocol is Garmin's binary format for structured data transfer.
//! It consists of:
//! - File Header (14 bytes)
//! - Definition Messages (define structure of data records)
//! - Data Messages (contain actual data values)
//! - CRC (2 bytes checksum)
//!
//! Reference: Garmin FIT SDK and Gadgetbridge Java implementation

use thiserror::Error;

#[derive(Error, Debug)]
pub enum FitError {
    #[error("Invalid field type: {0}")]
    InvalidFieldType(u8),

    #[error("Field value too large: {0}")]
    FieldValueTooLarge(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),
}

pub type Result<T> = std::result::Result<T, FitError>;

/// FIT file header (14 bytes)
#[derive(Debug, Clone)]
pub struct FitFileHeader {
    /// Header size (always 14)
    pub header_size: u8,
    /// Protocol version (e.g., 0x10 for version 1.0, 0x20 for version 2.0)
    pub protocol_version: u8,
    /// Profile version (e.g., 2132 for version 21.32)
    pub profile_version: u16,
    /// Data size (not including header and CRC)
    pub data_size: u32,
    /// Data type signature (".FIT" = [0x2E, 0x46, 0x49, 0x54])
    pub data_type: [u8; 4],
}

impl FitFileHeader {
    /// Create a new FIT file header
    pub fn new(data_size: u32) -> Self {
        Self {
            header_size: 14,
            protocol_version: 0x20, // Protocol version 2.0
            profile_version: 2132,  // Profile version 21.32
            data_size,
            data_type: [0x2E, 0x46, 0x49, 0x54], // ".FIT"
        }
    }

    /// Encode the header to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(14);
        buf.push(self.header_size);
        buf.push(self.protocol_version);
        buf.extend_from_slice(&self.profile_version.to_le_bytes());
        buf.extend_from_slice(&self.data_size.to_le_bytes());
        buf.extend_from_slice(&self.data_type);

        // Calculate CRC for header
        let crc = calculate_crc(&buf, 0);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }
}

/// FIT field types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FitFieldType {
    Enum = 0,
    SInt8 = 1,
    UInt8 = 2,
    SInt16 = 3,
    UInt16 = 4,
    SInt32 = 5,
    UInt32 = 6,
    String = 7,
    Float32 = 8,
    Float64 = 9,
    UInt8z = 10,
    UInt16z = 11,
    UInt32z = 12,
    Byte = 13,
    SInt64 = 14,
    UInt64 = 15,
    UInt64z = 16,
}

impl FitFieldType {
    /// Get the size in bytes of this field type
    pub fn size(&self) -> usize {
        match self {
            FitFieldType::Enum => 1,
            FitFieldType::SInt8 => 1,
            FitFieldType::UInt8 => 1,
            FitFieldType::SInt16 => 2,
            FitFieldType::UInt16 => 2,
            FitFieldType::SInt32 => 4,
            FitFieldType::UInt32 => 4,
            FitFieldType::String => 0, // Variable length
            FitFieldType::Float32 => 4,
            FitFieldType::Float64 => 8,
            FitFieldType::UInt8z => 1,
            FitFieldType::UInt16z => 2,
            FitFieldType::UInt32z => 4,
            FitFieldType::Byte => 1,
            FitFieldType::SInt64 => 8,
            FitFieldType::UInt64 => 8,
            FitFieldType::UInt64z => 8,
        }
    }

    /// Get the invalid value for this field type
    pub fn invalid_value(&self) -> Vec<u8> {
        match self {
            FitFieldType::Enum
            | FitFieldType::UInt8
            | FitFieldType::UInt8z
            | FitFieldType::Byte => {
                vec![0xFF]
            }
            FitFieldType::SInt8 => vec![0x7F],
            FitFieldType::SInt16 => vec![0xFF, 0x7F],
            FitFieldType::UInt16 | FitFieldType::UInt16z => vec![0xFF, 0xFF],
            FitFieldType::SInt32 => vec![0xFF, 0xFF, 0xFF, 0x7F],
            FitFieldType::UInt32 | FitFieldType::UInt32z => vec![0xFF, 0xFF, 0xFF, 0xFF],
            FitFieldType::Float32 => vec![0xFF, 0xFF, 0xFF, 0xFF],
            FitFieldType::Float64 => vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            FitFieldType::SInt64 => vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
            FitFieldType::UInt64 | FitFieldType::UInt64z => {
                vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
            }
            FitFieldType::String => vec![0x00], // Empty string
        }
    }
}

/// FIT field definition
#[derive(Debug, Clone)]
pub struct FitFieldDefinition {
    /// Field definition number
    pub field_number: u8,
    /// Field size in bytes
    pub size: u8,
    /// Base type
    pub base_type: FitFieldType,
}

impl FitFieldDefinition {
    pub fn new(field_number: u8, base_type: FitFieldType, size: Option<u8>) -> Self {
        let size = size.unwrap_or(base_type.size() as u8);
        Self {
            field_number,
            size,
            base_type,
        }
    }

    /// Encode the field definition to bytes
    pub fn encode(&self) -> Vec<u8> {
        vec![self.field_number, self.size, self.base_type as u8]
    }
}

/// FIT definition message
#[derive(Debug, Clone)]
pub struct FitDefinitionMessage {
    /// Local message type (0-15)
    pub local_message_type: u8,
    /// Global message number
    pub global_message_number: u16,
    /// Architecture (0 = little endian, 1 = big endian)
    pub architecture: u8,
    /// Field definitions
    pub fields: Vec<FitFieldDefinition>,
}

impl FitDefinitionMessage {
    /// Create a new definition message
    pub fn new(
        local_message_type: u8,
        global_message_number: u16,
        fields: Vec<FitFieldDefinition>,
    ) -> Self {
        Self {
            local_message_type,
            global_message_number,
            architecture: 0, // Little endian
            fields,
        }
    }

    /// Encode the definition message to bytes
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Header: 0x40 | local_message_type (definition message)
        buf.push(0x40 | (self.local_message_type & 0x0F));

        // Reserved byte
        buf.push(0x00);

        // Architecture
        buf.push(self.architecture);

        // Global message number (little endian)
        buf.extend_from_slice(&self.global_message_number.to_le_bytes());

        // Number of fields
        buf.push(self.fields.len() as u8);

        // Field definitions
        for field in &self.fields {
            buf.extend_from_slice(&field.encode());
        }

        Ok(buf)
    }
}

/// FIT data message
#[derive(Debug, Clone)]
pub struct FitDataMessage {
    /// Local message type (0-15)
    pub local_message_type: u8,
    /// Field data (field_number -> data bytes)
    pub fields: Vec<(u8, Vec<u8>)>,
}

impl FitDataMessage {
    /// Create a new data message
    pub fn new(local_message_type: u8) -> Self {
        Self {
            local_message_type,
            fields: Vec::new(),
        }
    }

    /// Add a field to the data message
    pub fn add_field(&mut self, field_number: u8, data: Vec<u8>) {
        self.fields.push((field_number, data));
    }

    /// Encode the data message to bytes
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Header: local_message_type (data message, bit 6 is 0)
        buf.push(self.local_message_type & 0x0F);

        // Field data (in order of definition)
        for (_field_number, data) in &self.fields {
            buf.extend_from_slice(data);
        }

        Ok(buf)
    }
}

/// FIT file builder
pub struct FitFileBuilder {
    definitions: Vec<FitDefinitionMessage>,
    data_messages: Vec<FitDataMessage>,
}

impl FitFileBuilder {
    /// Create a new FIT file builder
    pub fn new() -> Self {
        Self {
            definitions: Vec::new(),
            data_messages: Vec::new(),
        }
    }

    /// Add a definition message
    pub fn add_definition(&mut self, definition: FitDefinitionMessage) {
        self.definitions.push(definition);
    }

    /// Add a data message
    pub fn add_data_message(&mut self, data: FitDataMessage) {
        self.data_messages.push(data);
    }

    /// Build the complete FIT file
    pub fn build(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Encode definitions and data messages
        let mut data_buf = Vec::new();
        for def in &self.definitions {
            data_buf.extend_from_slice(&def.encode()?);
        }
        for data in &self.data_messages {
            data_buf.extend_from_slice(&data.encode()?);
        }

        // Create header with correct data size
        let header = FitFileHeader::new(data_buf.len() as u32);
        buf.extend_from_slice(&header.encode());

        // Add data
        buf.extend_from_slice(&data_buf);

        // Calculate and add CRC for data
        let crc = calculate_crc(&buf[14..], 0); // Skip header
        buf.extend_from_slice(&crc.to_le_bytes());

        Ok(buf)
    }
}

impl Default for FitFileBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Calculate CRC-16 checksum for FIT files
///
/// Uses CRC-16-CCITT (0x1021 polynomial)
pub fn calculate_crc(data: &[u8], initial: u16) -> u16 {
    const CRC_TABLE: [u16; 16] = [
        0x0000, 0xCC01, 0xD801, 0x1400, 0xF001, 0x3C00, 0x2800, 0xE401, 0xA001, 0x6C00, 0x7800,
        0xB401, 0x5000, 0x9C01, 0x8801, 0x4400,
    ];

    let mut crc = initial;

    for byte in data {
        // Process lower nibble
        let mut tmp = CRC_TABLE[(crc & 0xF) as usize];
        crc = (crc >> 4) & 0x0FFF;
        crc = crc ^ tmp ^ CRC_TABLE[(byte & 0xF) as usize];

        // Process upper nibble
        tmp = CRC_TABLE[(crc & 0xF) as usize];
        crc = (crc >> 4) & 0x0FFF;
        crc = crc ^ tmp ^ CRC_TABLE[((byte >> 4) & 0xF) as usize];
    }

    crc
}

/// Field value encoding helpers
pub mod field_value {

    /// Encode a uint8 value
    pub fn encode_uint8(value: u8) -> Vec<u8> {
        vec![value]
    }

    /// Encode a uint16 value (little endian)
    pub fn encode_uint16(value: u16) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    /// Encode a uint32 value (little endian)
    pub fn encode_uint32(value: u32) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    /// Encode a sint8 value
    pub fn encode_sint8(value: i8) -> Vec<u8> {
        vec![value as u8]
    }

    /// Encode a sint16 value (little endian)
    pub fn encode_sint16(value: i16) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    /// Encode a sint32 value (little endian)
    pub fn encode_sint32(value: i32) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    /// Encode a string value (null-terminated)
    pub fn encode_string(value: &str) -> Vec<u8> {
        let mut buf = value.as_bytes().to_vec();
        buf.push(0x00); // Null terminator
        buf
    }

    /// Encode an enum value
    pub fn encode_enum(value: u8) -> Vec<u8> {
        vec![value]
    }

    /// Encode a float32 value (little endian)
    pub fn encode_float32(value: f32) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }
}

/// Global message numbers (from FIT SDK)
pub mod global_message {
    pub const FILE_ID: u16 = 0;
    pub const WEATHER_CONDITIONS: u16 = 128;
    pub const WEATHER_ALERT: u16 = 129;
}

/// Field numbers for weather messages (from FIT SDK)
pub mod weather_field {
    // Common fields
    pub const WEATHER_REPORT: u8 = 0;
    pub const TIMESTAMP: u8 = 253;
    pub const TEMPERATURE: u8 = 1;
    pub const CONDITION: u8 = 2;
    pub const WIND_DIRECTION: u8 = 3;
    pub const WIND_SPEED: u8 = 4;
    pub const PRECIPITATION_PROBABILITY: u8 = 5;
    pub const TEMPERATURE_FEELS_LIKE: u8 = 6;
    pub const RELATIVE_HUMIDITY: u8 = 7;
    pub const LOCATION: u8 = 8;
    pub const OBSERVED_AT_TIME: u8 = 9;
    pub const OBSERVED_LOCATION_LAT: u8 = 10;
    pub const OBSERVED_LOCATION_LONG: u8 = 11;
    pub const DAY_OF_WEEK: u8 = 12;
    pub const HIGH_TEMPERATURE: u8 = 13;
    pub const LOW_TEMPERATURE: u8 = 14;
    pub const DEW_POINT: u8 = 15;
    pub const UV_INDEX: u8 = 16;
    pub const AIR_QUALITY: u8 = 17;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fit_header_encoding() {
        let header = FitFileHeader::new(100);
        let encoded = header.encode();

        assert_eq!(encoded.len(), 14);
        assert_eq!(encoded[0], 14); // Header size
        assert_eq!(encoded[1], 0x20); // Protocol version
        assert_eq!(encoded[6..10], [100, 0, 0, 0]); // Data size (little endian)
        assert_eq!(&encoded[10..14], b".FIT");
    }

    #[test]
    fn test_field_type_sizes() {
        assert_eq!(FitFieldType::UInt8.size(), 1);
        assert_eq!(FitFieldType::UInt16.size(), 2);
        assert_eq!(FitFieldType::UInt32.size(), 4);
        assert_eq!(FitFieldType::SInt32.size(), 4);
    }

    #[test]
    fn test_definition_message_encoding() {
        let fields = vec![
            FitFieldDefinition::new(0, FitFieldType::Enum, None),
            FitFieldDefinition::new(253, FitFieldType::UInt32, None),
            FitFieldDefinition::new(1, FitFieldType::SInt16, None),
        ];

        let def = FitDefinitionMessage::new(0, global_message::WEATHER_CONDITIONS, fields);
        let encoded = def.encode().unwrap();

        assert_eq!(encoded[0] & 0x40, 0x40); // Definition message bit
        assert_eq!(encoded[0] & 0x0F, 0); // Local message type
        assert_eq!(encoded[4], 3); // Number of fields
    }

    #[test]
    fn test_data_message_encoding() {
        let mut data = FitDataMessage::new(0);
        data.add_field(0, vec![1]); // weather_report = 1
        data.add_field(253, vec![0x00, 0x01, 0x02, 0x03]); // timestamp

        let encoded = data.encode().unwrap();
        assert_eq!(encoded[0] & 0x0F, 0); // Local message type
        assert_eq!(encoded.len(), 6); // 1 (header) + 1 (field 0) + 4 (field 253)
    }

    #[test]
    fn test_crc_calculation() {
        let data = b"Hello, World!";
        let crc = calculate_crc(data, 0);
        assert!(crc > 0);

        // CRC should be deterministic
        let crc2 = calculate_crc(data, 0);
        assert_eq!(crc, crc2);
    }

    #[test]
    fn test_field_value_encoding() {
        assert_eq!(field_value::encode_uint8(42), vec![42]);
        assert_eq!(field_value::encode_uint16(1000), vec![0xE8, 0x03]);
        assert_eq!(
            field_value::encode_uint32(100000),
            vec![0xA0, 0x86, 0x01, 0x00]
        );
        assert_eq!(
            field_value::encode_sint32(-100),
            vec![0x9C, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(field_value::encode_string("Test"), b"Test\0".to_vec());
    }

    #[test]
    fn test_fit_file_builder() {
        let mut builder = FitFileBuilder::new();

        // Add a simple definition
        let fields = vec![
            FitFieldDefinition::new(0, FitFieldType::Enum, None),
            FitFieldDefinition::new(253, FitFieldType::UInt32, None),
        ];
        builder.add_definition(FitDefinitionMessage::new(0, 128, fields));

        // Add a data message
        let mut data = FitDataMessage::new(0);
        data.add_field(0, vec![0]);
        data.add_field(253, vec![0x00, 0x00, 0x00, 0x00]);
        builder.add_data_message(data);

        let fit_file = builder.build().unwrap();
        assert!(fit_file.len() > 14); // At least header size
        assert_eq!(&fit_file[10..14], b".FIT"); // Check signature
    }
}
