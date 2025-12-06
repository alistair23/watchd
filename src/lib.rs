//! Garmin v2 BLE Communication Protocol
//!
//! This library provides a Rust implementation of the Garmin v2 Bluetooth Low Energy
//! communication protocol, including COBS encoding/decoding and the Multi-Link Reliable (MLR)
//! protocol for reliable message transmission.
//!
//! # Modules
//!
//! - `cobs`: COBS (Consistent Overhead Byte Stuffing) encoding and decoding
//! - `mlr`: Multi-Link Reliable protocol implementation
//! - `communicator`: High-level Garmin v2 communicator interface
//! - `types`: Common types and enums used throughout the library

pub mod calendar;
pub mod cobs;
pub mod communicator;
pub mod data_transfer;
pub mod fit;
pub mod http;
pub mod messages;
pub mod mlr;
pub mod protobuf_calendar;
pub mod types;
pub mod watchdog;

pub use calendar::{CalendarError, CalendarEvent, CalendarManager, CalendarProvider};
pub use cobs::CobsCoDec;
pub use communicator::{
    AsyncGfdiMessageCallback, BleSupport, CharacteristicHandle, CommunicatorV2,
    FileTransferCallback, GfdiMessageCallback, GfdiServiceCallback, RealtimeHeartRateCallback,
    RealtimeStepsCallback, ServiceCallback, ServiceWriter, Transaction,
};
pub use data_transfer::{DataTransferHandler, TransferStats};
pub use fit::{
    calculate_crc, field_value, global_message, weather_field, FitDataMessage,
    FitDefinitionMessage, FitError, FitFieldDefinition, FitFieldType, FitFileBuilder,
    FitFileHeader,
};
pub use http::{handle_http_request, HttpMethod, HttpRequest, HttpResponse};
pub use messages::{
    ConfigurationMessage, DeviceInformationMessage, FilterStatusMessage, GfdiMessage,
    MessageGenerator, MessageId, MessageParser, NotificationControlMessage,
    NotificationSubscriptionMessage, Status, SynchronizationMessage, WeatherRequestMessage,
};
pub use mlr::{MessageReceiver, MessageSender, MlrCommunicator};
pub use protobuf_calendar::{
    encode_calendar_response, handle_calendar_request, parse_calendar_request, CalendarEventProto,
    CalendarResponseStatus, CalendarServiceRequest,
};
pub use types::{GarminError, RequestType, Result, Service};
pub use watchdog::{
    HealthStatus, ReconnectReason, WatchdogConfig, WatchdogManager, WatchdogMetrics,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_imports() {
        // Smoke test to ensure all modules can be imported
        let _ = Service::GFDI;
        let _ = RequestType::RegisterMlReq;
    }
}
