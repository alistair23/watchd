//! Common types, enums, and error definitions for the Garmin v2 protocol

use std::fmt;
use thiserror::Error;

/// Result type alias for Garmin operations
pub type Result<T> = std::result::Result<T, GarminError>;

/// Error types for Garmin communication
#[derive(Error, Debug)]
pub enum GarminError {
    #[error("GFDI service handle not set")]
    GfdiHandleNotSet,

    #[error("Invalid handle: expected {expected}, got {got}")]
    InvalidHandle { expected: u8, got: u8 },

    #[error("Packet too short: {0} bytes")]
    PacketTooShort(usize),

    #[error("Not an MLR packet")]
    NotMlrPacket,

    #[error("Out of sequence packet: expected {expected}, got {got}")]
    OutOfSequence { expected: u8, got: u8 },

    #[error("Fragment not found at index {0}")]
    FragmentNotFound(u8),

    #[error("Message is empty")]
    EmptyMessage,

    #[error("Bluetooth error: {0}")]
    BluetoothError(String),

    #[error("Encoding error: {0}")]
    EncodingError(String),

    #[error("Decoding error: {0}")]
    DecodingError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unknown request type: {0}")]
    UnknownRequestType(u8),

    #[error("Unknown service code: {0}")]
    UnknownServiceCode(u16),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),
}

/// Request types for ML (Multi-Link) service management
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RequestType {
    RegisterMlReq = 0,
    RegisterMlResp = 1,
    CloseHandleReq = 2,
    CloseHandleResp = 3,
    UnkHandle = 4,
    CloseAllReq = 5,
    CloseAllResp = 6,
    UnkReq = 7,
    UnkResp = 8,
}

impl RequestType {
    /// Convert a byte to a RequestType
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(RequestType::RegisterMlReq),
            1 => Ok(RequestType::RegisterMlResp),
            2 => Ok(RequestType::CloseHandleReq),
            3 => Ok(RequestType::CloseHandleResp),
            4 => Ok(RequestType::UnkHandle),
            5 => Ok(RequestType::CloseAllReq),
            6 => Ok(RequestType::CloseAllResp),
            7 => Ok(RequestType::UnkReq),
            8 => Ok(RequestType::UnkResp),
            _ => Err(GarminError::UnknownRequestType(value)),
        }
    }

    /// Convert RequestType to a byte
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for RequestType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestType::RegisterMlReq => write!(f, "REGISTER_ML_REQ"),
            RequestType::RegisterMlResp => write!(f, "REGISTER_ML_RESP"),
            RequestType::CloseHandleReq => write!(f, "CLOSE_HANDLE_REQ"),
            RequestType::CloseHandleResp => write!(f, "CLOSE_HANDLE_RESP"),
            RequestType::UnkHandle => write!(f, "UNK_HANDLE"),
            RequestType::CloseAllReq => write!(f, "CLOSE_ALL_REQ"),
            RequestType::CloseAllResp => write!(f, "CLOSE_ALL_RESP"),
            RequestType::UnkReq => write!(f, "UNK_REQ"),
            RequestType::UnkResp => write!(f, "UNK_RESP"),
        }
    }
}

/// Garmin services available via the ML protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Service {
    GFDI = 1,
    Registration = 4,
    RealtimeHr = 6,
    RealtimeSteps = 7,
    RealtimeCalories = 8,
    RealtimeIntensity = 10,
    RealtimeHrv = 12,
    RealtimeStress = 13,
    RealtimeAccelerometer = 16,
    RealtimeSpo2 = 19,
    RealtimeBodyBattery = 20,
    RealtimeRespiration = 21,
    FileTransfer2 = 0x2018,
    FileTransfer4 = 0x4018,
    FileTransfer6 = 0x6018,
    FileTransferA = 0xa018,
    FileTransferC = 0xc018,
    FileTransferE = 0xe018,
}

impl Service {
    /// Convert a u16 code to a Service
    pub fn from_code(code: u16) -> Result<Self> {
        match code {
            1 => Ok(Service::GFDI),
            4 => Ok(Service::Registration),
            6 => Ok(Service::RealtimeHr),
            7 => Ok(Service::RealtimeSteps),
            8 => Ok(Service::RealtimeCalories),
            10 => Ok(Service::RealtimeIntensity),
            12 => Ok(Service::RealtimeHrv),
            13 => Ok(Service::RealtimeStress),
            16 => Ok(Service::RealtimeAccelerometer),
            19 => Ok(Service::RealtimeSpo2),
            20 => Ok(Service::RealtimeBodyBattery),
            21 => Ok(Service::RealtimeRespiration),
            0x2018 => Ok(Service::FileTransfer2),
            0x4018 => Ok(Service::FileTransfer4),
            0x6018 => Ok(Service::FileTransfer6),
            0xa018 => Ok(Service::FileTransferA),
            0xc018 => Ok(Service::FileTransferC),
            0xe018 => Ok(Service::FileTransferE),
            _ => Err(GarminError::UnknownServiceCode(code)),
        }
    }

    /// Get the service code
    pub fn code(self) -> u16 {
        self as u16
    }
}

impl fmt::Display for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Service::GFDI => write!(f, "GFDI"),
            Service::Registration => write!(f, "Registration"),
            Service::RealtimeHr => write!(f, "RealtimeHR"),
            Service::RealtimeSteps => write!(f, "RealtimeSteps"),
            Service::RealtimeCalories => write!(f, "RealtimeCalories"),
            Service::RealtimeIntensity => write!(f, "RealtimeIntensity"),
            Service::RealtimeHrv => write!(f, "RealtimeHRV"),
            Service::RealtimeStress => write!(f, "RealtimeStress"),
            Service::RealtimeAccelerometer => write!(f, "RealtimeAccelerometer"),
            Service::RealtimeSpo2 => write!(f, "RealtimeSPO2"),
            Service::RealtimeBodyBattery => write!(f, "RealtimeBodyBattery"),
            Service::RealtimeRespiration => write!(f, "RealtimeRespiration"),
            Service::FileTransfer2 => write!(f, "FileTransfer2"),
            Service::FileTransfer4 => write!(f, "FileTransfer4"),
            Service::FileTransfer6 => write!(f, "FileTransfer6"),
            Service::FileTransferA => write!(f, "FileTransferA"),
            Service::FileTransferC => write!(f, "FileTransferC"),
            Service::FileTransferE => write!(f, "FileTransferE"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_type_conversion() {
        assert_eq!(RequestType::from_u8(0).unwrap(), RequestType::RegisterMlReq);
        assert_eq!(RequestType::from_u8(5).unwrap(), RequestType::CloseAllReq);
        assert!(RequestType::from_u8(99).is_err());
    }

    #[test]
    fn test_request_type_to_u8() {
        assert_eq!(RequestType::RegisterMlReq.to_u8(), 0);
        assert_eq!(RequestType::CloseAllReq.to_u8(), 5);
    }

    #[test]
    fn test_service_conversion() {
        assert_eq!(Service::from_code(1).unwrap(), Service::GFDI);
        assert_eq!(Service::from_code(0x2018).unwrap(), Service::FileTransfer2);
        assert!(Service::from_code(9999).is_err());
    }

    #[test]
    fn test_service_code() {
        assert_eq!(Service::GFDI.code(), 1);
        assert_eq!(Service::RealtimeHr.code(), 6);
        assert_eq!(Service::FileTransfer2.code(), 0x2018);
    }
}
