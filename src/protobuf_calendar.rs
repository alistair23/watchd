//! Protobuf Calendar Service Handler
//!
//! This module handles the Calendar Service protobuf messages for Garmin watches.
//! It provides parsing of calendar requests from the watch and encoding of calendar
//! responses with event data.
//!
//! # Protocol Overview
//!
//! The Garmin calendar protocol uses protobuf messages nested within the Smart message envelope:
//!
//! ```text
//! Smart Message (field 10) → CalendarService → CalendarEventsRequest/Response
//! ```
//!
//! ## CalendarEventsRequest
//!
//! The watch sends a `CalendarEventsRequest` to query calendar events within a time range:
//!
//! - **Field 1**: `start_date` (uint64) - Start time as Unix timestamp in seconds
//! - **Field 2**: `end_date` (uint64) - End time as Unix timestamp in seconds
//! - **Field 3**: `include_organizer` (bool, default: false) - Include organizer name
//! - **Field 4**: `include_title` (bool, default: true) - Include event title
//! - **Field 5**: `include_location` (bool, default: true) - Include location
//! - **Field 6**: `include_description` (bool, default: false) - Include description
//! - **Field 7**: `include_start_date` (bool, default: true) - Include start time
//! - **Field 8**: `include_end_date` (bool, default: false) - Include end time
//! - **Field 9**: `include_all_day` (bool, default: false) - Include all-day flag
//! - **Field 10**: `max_organizer_length` (uint32, default: 0) - Max organizer string length
//! - **Field 11**: `max_title_length` (uint32, default: 0) - Max title string length
//! - **Field 12**: `max_location_length` (uint32, default: 0) - Max location string length
//! - **Field 13**: `max_description_length` (uint32, default: 0) - Max description string length
//! - **Field 14**: `max_events` (uint32, default: 100) - Maximum number of events to return
//!
//! ## CalendarEventsResponse
//!
//! Gadgetbridge responds with a `CalendarEventsResponse` containing matching events:
//!
//! - **Field 1**: `status` (enum) - Response status:
//!   - `0` = UNKNOWN_RESPONSE_STATUS
//!   - `1` = OK
//!   - `2` = INVALID_DATE_RANGE
//! - **Field 2**: `events` (repeated CalendarEvent) - Array of calendar events
//!
//! ## CalendarEvent
//!
//! Each event in the response contains:
//!
//! - **Field 1**: `organizer` (string, optional) - Organizer name
//! - **Field 2**: `title` (string) - Event title
//! - **Field 3**: `location` (string, optional) - Event location
//! - **Field 4**: `description` (string, optional) - Event description
//! - **Field 5**: `start_date` (uint64) - Start time as Unix timestamp in seconds
//! - **Field 6**: `end_date` (uint64) - End time as Unix timestamp in seconds
//! - **Field 7**: `all_day` (bool) - Whether this is an all-day event
//! - **Field 8**: `reminder_time_in_secs` (repeated uint32) - Reminder times in seconds before event
//!
//! # Example
//!
//! ```rust,no_run
//! use garmin_v2_communicator::{parse_calendar_request, handle_calendar_request, encode_calendar_response};
//! use garmin_v2_communicator::{CalendarManager, CalendarResponseStatus};
//!
//! async fn handle_calendar(data: &[u8], manager: &CalendarManager) -> Vec<u8> {
//!     // Parse the request from the watch
//!     let request = parse_calendar_request(data).expect("Failed to parse request");
//!
//!     // Fetch events from calendar backends
//!     let events = handle_calendar_request(&request, Some(manager))
//!         .await
//!         .expect("Failed to fetch events");
//!
//!     // Encode the response
//!     encode_calendar_response(&events, CalendarResponseStatus::Ok, 0x1234)
//! }
//! ```
//!
//! The implementation uses manual protobuf encoding/decoding to avoid heavy dependencies.

use crate::calendar::CalendarManager;
use log::{debug, info, warn};

/// Calendar service request from the watch
/// Based on Garmin CalendarEventsRequest protobuf message
#[derive(Debug, Clone)]
pub struct CalendarServiceRequest {
    /// Whether the request came via CoreService (field 1) or CalendarService (field 10)
    pub use_core_service_envelope: bool,

    /// Field 1: Start date/time (Unix timestamp in seconds as uint64)
    pub start_date: u64,

    /// Field 2: End date/time (Unix timestamp in seconds as uint64)
    pub end_date: u64,

    /// Field 3: Whether to include organizer field (default: false)
    pub include_organizer: bool,

    /// Field 4: Whether to include title field (default: true)
    pub include_title: bool,

    /// Field 5: Whether to include location field (default: true)
    pub include_location: bool,

    /// Field 6: Whether to include description field (default: false)
    pub include_description: bool,

    /// Field 7: Whether to include start_date field (default: true)
    pub include_start_date: bool,

    /// Field 8: Whether to include end_date field (default: false)
    pub include_end_date: bool,

    /// Field 9: Whether to include all_day field (default: false)
    pub include_all_day: bool,

    /// Field 10: Maximum organizer length (default: 0)
    pub max_organizer_length: u32,

    /// Field 11: Maximum title length (default: 0)
    pub max_title_length: u32,

    /// Field 12: Maximum location length (default: 0)
    pub max_location_length: u32,

    /// Field 13: Maximum description length (default: 0)
    pub max_description_length: u32,

    /// Field 14: Maximum number of events to return (default: 100)
    pub max_events: u32,
}

impl Default for CalendarServiceRequest {
    fn default() -> Self {
        Self {
            use_core_service_envelope: false,
            start_date: 0,
            end_date: 0,
            include_organizer: false,
            include_title: true,
            include_location: true,
            include_description: false,
            include_start_date: true,
            include_end_date: false,
            include_all_day: false,
            max_organizer_length: 0,
            max_title_length: 0,
            max_location_length: 0,
            max_description_length: 0,
            max_events: 100,
        }
    }
}

/// Calendar event in protobuf format
/// Based on Garmin CalendarEvent protobuf message
#[derive(Debug, Clone)]
pub struct CalendarEventProto {
    /// Field 1: Organizer name
    pub organizer: Option<String>,
    /// Field 2: Event title
    pub title: String,
    /// Field 3: Event location
    pub location: Option<String>,
    /// Field 4: Event description
    pub description: Option<String>,
    /// Field 5: Start date/time (Unix timestamp in seconds)
    pub start_date: u64,
    /// Field 6: End date/time (Unix timestamp in seconds)
    pub end_date: u64,
    /// Field 7: All-day event flag
    pub all_day: bool,
    /// Field 8: Reminder times in seconds before event
    pub reminder_times: Vec<u32>,
}

/// Calendar service response status
/// Based on Garmin CalendarEventsResponse.ResponseStatus enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalendarResponseStatus {
    UnknownStatus = 0,
    Ok = 1,
    InvalidDateRange = 2,
}

/// Parse a protobuf calendar service request from raw bytes
///
/// This parses the Smart message containing a CalendarService field.
/// The structure is:
/// - Smart message (field 10 = CalendarService OR field 1 = CoreService)
/// - CalendarService message (field 1 = CalendarServiceRequest)
/// - CalendarServiceRequest fields (begin, end, max_events, etc.)
pub fn parse_calendar_request(data: &[u8]) -> Result<CalendarServiceRequest, String> {
    debug!("Parsing calendar request from {} bytes", data.len());
    eprintln!("🔍 parse_calendar_request: START with {} bytes", data.len());

    let mut request = CalendarServiceRequest::default();
    let mut cursor = 0;

    // First, parse the Smart message wrapper
    // We need to find field 10 (CalendarService) or field 1 (CoreService with calendar)
    let mut calendar_service_data = None;

    eprintln!("🔍 parse_calendar_request: Starting Smart message parse loop");
    let mut loop_count = 0;
    while cursor < data.len() {
        loop_count += 1;
        if loop_count > 100 {
            eprintln!("🔍 parse_calendar_request: Loop guard - breaking after 100 iterations");
            break;
        }

        eprintln!(
            "🔍 parse_calendar_request: Loop iteration {}, cursor={}",
            loop_count, cursor
        );

        let old_cursor = cursor;
        if let Some((field_num, wire_type, field_data, next_cursor)) = parse_field(&data[cursor..])
        {
            debug!("Smart message field {}, wire type {}", field_num, wire_type);
            eprintln!(
                "🔍 parse_calendar_request: Found field {}, wire_type {}, next_cursor={}",
                field_num, wire_type, next_cursor
            );

            // Field 1 is CalendarService (according to Garmin protocol documentation)
            // Field 13 is CoreService
            if field_num == 1 {
                eprintln!(
                    "🔍 parse_calendar_request: Found calendar/core service field {}",
                    field_num
                );
                calendar_service_data = Some(field_data.to_vec());
                break;
            }

            cursor += next_cursor;

            // Safety check: ensure cursor advances
            if cursor == old_cursor {
                eprintln!("🔍 parse_calendar_request: Cursor not advancing, breaking");
                break;
            }
        } else {
            eprintln!("🔍 parse_calendar_request: parse_field returned None");
            break;
        }
    }

    eprintln!("🔍 parse_calendar_request: Finished Smart message parse loop");
    let calendar_data = calendar_service_data
        .ok_or_else(|| "No CalendarService or CoreService field found".to_string())?;

    // Field 1 is always CalendarService for calendar requests
    request.use_core_service_envelope = false;

    debug!(
        "Found calendar service data: {} bytes (via field 1 = CalendarService)",
        calendar_data.len()
    );
    eprintln!(
        "🔍 parse_calendar_request: Found calendar_data with {} bytes",
        calendar_data.len()
    );

    // Now parse the CalendarService message
    // Field 1 = CalendarServiceRequest
    cursor = 0;
    let mut request_data = None;

    eprintln!("🔍 parse_calendar_request: Starting CalendarService parse loop");
    let mut loop_count = 0;
    while cursor < calendar_data.len() {
        loop_count += 1;
        if loop_count > 100 {
            eprintln!("🔍 parse_calendar_request: CalendarService loop guard - breaking");
            break;
        }

        let old_cursor = cursor;
        if let Some((field_num, wire_type, field_data, next_cursor)) =
            parse_field(&calendar_data[cursor..])
        {
            debug!(
                "CalendarService field {}, wire type {}",
                field_num, wire_type
            );

            if field_num == 1 {
                request_data = Some(field_data.to_vec());
                break;
            }

            cursor += next_cursor;

            // Safety check: ensure cursor advances
            if cursor == old_cursor {
                eprintln!(
                    "🔍 parse_calendar_request: CalendarService cursor not advancing, breaking"
                );
                break;
            }
        } else {
            break;
        }
    }

    let request_bytes =
        request_data.ok_or_else(|| "No CalendarServiceRequest field found".to_string())?;

    debug!("Found calendar request data: {} bytes", request_bytes.len());

    // Parse the CalendarServiceRequest fields
    cursor = 0;
    let mut loop_count = 0;
    while cursor < request_bytes.len() {
        loop_count += 1;
        if loop_count > 100 {
            eprintln!("🔍 parse_calendar_request: Request fields loop guard - breaking");
            break;
        }

        let old_cursor = cursor;
        if let Some((field_num, wire_type, field_data, next_cursor)) =
            parse_field(&request_bytes[cursor..])
        {
            debug!("Request field {}, wire type {}", field_num, wire_type);

            match field_num {
                1 => {
                    // start_date (uint64)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.start_date = value;
                        debug!("  start_date = {}", request.start_date);
                    }
                }
                2 => {
                    // end_date (uint64)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.end_date = value;
                        debug!("  end_date = {}", request.end_date);
                    }
                }
                3 => {
                    // include_organizer (bool)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.include_organizer = value != 0;
                        debug!("  include_organizer = {}", request.include_organizer);
                    }
                }
                4 => {
                    // include_title (bool)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.include_title = value != 0;
                        debug!("  include_title = {}", request.include_title);
                    }
                }
                5 => {
                    // include_location (bool)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.include_location = value != 0;
                        debug!("  include_location = {}", request.include_location);
                    }
                }
                6 => {
                    // include_description (bool)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.include_description = value != 0;
                        debug!("  include_description = {}", request.include_description);
                    }
                }
                7 => {
                    // include_start_date (bool)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.include_start_date = value != 0;
                        debug!("  include_start_date = {}", request.include_start_date);
                    }
                }
                8 => {
                    // include_end_date (bool)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.include_end_date = value != 0;
                        debug!("  include_end_date = {}", request.include_end_date);
                    }
                }
                9 => {
                    // include_all_day (bool)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.include_all_day = value != 0;
                        debug!("  include_all_day = {}", request.include_all_day);
                    }
                }
                10 => {
                    // max_organizer_length (uint32)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.max_organizer_length = value as u32;
                        debug!("  max_organizer_length = {}", request.max_organizer_length);
                    }
                }
                11 => {
                    // max_title_length (uint32)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.max_title_length = value as u32;
                        debug!("  max_title_length = {}", request.max_title_length);
                    }
                }
                12 => {
                    // max_location_length (uint32)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.max_location_length = value as u32;
                        debug!("  max_location_length = {}", request.max_location_length);
                    }
                }
                13 => {
                    // max_description_length (uint32)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.max_description_length = value as u32;
                        debug!(
                            "  max_description_length = {}",
                            request.max_description_length
                        );
                    }
                }
                14 => {
                    // max_events (uint32)
                    if let Ok((value, _len)) = decode_varint(field_data) {
                        request.max_events = value as u32;
                        debug!("  max_events = {}", request.max_events);
                    }
                }
                _ => {
                    debug!("  Unknown field {}", field_num);
                }
            }

            cursor += next_cursor;

            // Safety check: ensure cursor advances
            if cursor == old_cursor {
                eprintln!(
                    "🔍 parse_calendar_request: Request fields cursor not advancing, breaking"
                );
                break;
            }
        } else {
            break;
        }
    }

    info!(
        "Parsed calendar request: start_date={}, end_date={}, max_events={}, envelope=CalendarService(field 1)",
        request.start_date,
        request.end_date,
        request.max_events
    );
    eprintln!("🔍 parse_calendar_request: COMPLETE - returning request");

    Ok(request)
}

/// Handle a calendar service request and generate a response
/// Handle a calendar request by fetching events from the calendar manager
///
/// This function processes a parsed `CalendarServiceRequest` and fetches matching
/// calendar events from available calendar providers (GNOME Calendar, KOrganizer, etc.).
/// It respects the request's field inclusion flags and length limits.
///
/// # Arguments
///
/// * `request` - The parsed calendar request from the watch
/// * `calendar_manager` - Optional reference to the calendar manager with providers
///
/// # Returns
///
/// Returns a vector of `CalendarEventProto` objects ready for encoding, or an error
/// if fetching fails. Returns an empty vector if no calendar manager is available.
///
/// # Field Filtering
///
/// The function honors the request's `include_*` flags:
/// - Only includes fields that the watch requested
/// - Truncates strings to the requested maximum lengths
/// - Filters events by the requested time range
/// - Limits the number of events to `max_events`
///
/// # Example
///
/// ```rust,no_run
/// let events = handle_calendar_request(&request, Some(&calendar_manager)).await?;
/// println!("Found {} events matching the request", events.len());
/// ```
pub async fn handle_calendar_request(
    request: &CalendarServiceRequest,
    calendar_manager: Option<&CalendarManager>,
) -> Result<Vec<CalendarEventProto>, String> {
    info!(
        "Handling calendar request: {} to {}",
        request.start_date, request.end_date
    );

    let manager = match calendar_manager {
        Some(m) => m,
        None => {
            warn!("No calendar manager available");
            return Ok(Vec::new());
        }
    };

    // Fetch events from calendar manager
    let events = manager
        .fetch_events(
            request.start_date as i64,
            request.end_date as i64,
            request.max_events as usize,
            request.include_all_day,
        )
        .await
        .map_err(|e| format!("Failed to fetch calendar events: {}", e))?;

    info!("Fetched {} calendar events", events.len());

    // Convert to protobuf format
    let mut proto_events = Vec::new();

    for event in events {
        // Truncate title to max length
        let title = if request.max_title_length > 0
            && event.title.len() > request.max_title_length as usize
        {
            event
                .title
                .chars()
                .take(request.max_title_length as usize)
                .collect()
        } else {
            event.title.clone()
        };

        // Include location if requested
        let location = if request.include_location {
            event.location.as_ref().map(|loc| {
                if request.max_location_length > 0
                    && loc.len() > request.max_location_length as usize
                {
                    loc.chars()
                        .take(request.max_location_length as usize)
                        .collect()
                } else {
                    loc.clone()
                }
            })
        } else {
            None
        };

        // Include description if requested
        let description = if request.include_description {
            event.description.as_ref().map(|desc| {
                if request.max_description_length > 0
                    && desc.len() > request.max_description_length as usize
                {
                    desc.chars()
                        .take(request.max_description_length as usize)
                        .collect()
                } else {
                    desc.clone()
                }
            })
        } else {
            None
        };

        // Include organizer if requested
        let organizer = if request.include_organizer {
            event.organizer.as_ref().map(|org| {
                if request.max_organizer_length > 0
                    && org.len() > request.max_organizer_length as usize
                {
                    org.chars()
                        .take(request.max_organizer_length as usize)
                        .collect()
                } else {
                    org.clone()
                }
            })
        } else {
            None
        };

        // Only include fields that were requested
        let final_title = if request.include_title {
            title
        } else {
            String::new()
        };
        let final_location = if request.include_location {
            location
        } else {
            None
        };
        let final_description = if request.include_description {
            description
        } else {
            None
        };
        let final_organizer = if request.include_organizer {
            organizer
        } else {
            None
        };

        proto_events.push(CalendarEventProto {
            organizer: final_organizer,
            title: final_title,
            location: final_location,
            description: final_description,
            start_date: event.start_timestamp as u64,
            end_date: event.end_timestamp as u64,
            all_day: event.all_day,
            reminder_times: event.reminders.iter().map(|&r| r as u32).collect(),
        });

        if proto_events.len() >= request.max_events as usize {
            debug!("Reached max_events limit: {}", request.max_events);
            break;
        }
    }

    info!("Returning {} calendar events", proto_events.len());
    Ok(proto_events)
}

/// Encode a calendar service response into protobuf format
///
/// This function creates a complete ProtobufResponse message containing a
/// CalendarEventsResponse with the provided events. The response is properly
/// nested within the Smart message envelope as expected by the watch.
///
/// # Arguments
///
/// * `events` - Slice of calendar events to include in the response
/// * `status` - Response status (Ok, InvalidDateRange, etc.)
/// * `request_id` - The request ID from the original ProtobufRequest (first 2 bytes)
///
/// # Returns
///
/// Returns a complete message payload ready to be sent as a ProtobufResponse (0x13B4).
/// The format is:
/// ```text
/// [request_id: 2 bytes] + [Smart message with CalendarEventsResponse]
/// ```
///
/// # Protocol Structure
///
/// The encoding creates nested protobuf messages:
/// 1. Each CalendarEvent is encoded with fields 1-8
/// 2. Events are wrapped in CalendarEventsResponse (field 2)
/// 3. Response is wrapped in CalendarService (field 2)
/// 4. CalendarService is wrapped in Smart message (field 10)
/// 5. Request ID prefix is added
///
/// # Example
///
/// ```rust,no_run
/// let response = encode_calendar_response(
///     &events,
///     CalendarResponseStatus::Ok,
///     request_id
/// );
/// // Send response as ProtobufResponse message
/// ```
pub fn encode_calendar_response(
    events: &[CalendarEventProto],
    status: CalendarResponseStatus,
    request_id: u16,
    _use_core_service_envelope: bool,
) -> Vec<u8> {
    debug!(
        "Encoding calendar response with {} events, envelope=CalendarService(field 1)",
        events.len()
    );

    // Encode CalendarServiceResponse
    let mut response_buf = Vec::new();

    // Field 1: status (enum/int32)
    encode_field_key(&mut response_buf, 1, 0); // field 1, varint
    encode_varint(&mut response_buf, status as u64);

    // Field 2: repeated calendar_event
    for event in events {
        let event_buf = encode_calendar_event(event);
        encode_field_key(&mut response_buf, 2, 2); // field 2, length-delimited
        encode_varint(&mut response_buf, event_buf.len() as u64);
        response_buf.extend_from_slice(&event_buf);
    }

    // Wrap in Smart message - CalendarService is always field 1
    let mut smart_buf = Vec::new();

    // CalendarService envelope: Smart.field1.field2 = CalendarEventsResponse
    let mut calendar_service_buf = Vec::new();
    encode_field_key(&mut calendar_service_buf, 2, 2); // field 2 = CalendarEventsResponse
    encode_varint(&mut calendar_service_buf, response_buf.len() as u64);
    calendar_service_buf.extend_from_slice(&response_buf);

    encode_field_key(&mut smart_buf, 1, 2); // field 1 = CalendarService
    encode_varint(&mut smart_buf, calendar_service_buf.len() as u64);
    smart_buf.extend_from_slice(&calendar_service_buf);

    // Create ProtobufResponse message (message ID 0x13B4 = 5044)
    // Format: [requestId:2][dataOffset:4][totalProtobufLength:4][protobufDataLength:4][protobufPayload:N]
    let mut message = Vec::new();

    // Add request ID (2 bytes, little endian)
    message.extend_from_slice(&request_id.to_le_bytes());

    // Add data offset (4 bytes, little endian) - 0 for non-chunked messages
    message.extend_from_slice(&0u32.to_le_bytes());

    // Add total protobuf length (4 bytes, little endian)
    let protobuf_length = smart_buf.len() as u32;
    message.extend_from_slice(&protobuf_length.to_le_bytes());

    // Add protobuf data length (4 bytes, little endian) - same as total for non-chunked
    message.extend_from_slice(&protobuf_length.to_le_bytes());

    // Add the Smart protobuf data
    message.extend_from_slice(&smart_buf);

    debug!(
        "Encoded calendar response: {} bytes total (header: 14, protobuf: {})",
        message.len(),
        smart_buf.len()
    );
    message
}

/// Encode a single calendar event in protobuf format
/// Based on Garmin CalendarEvent protobuf message
fn encode_calendar_event(event: &CalendarEventProto) -> Vec<u8> {
    let mut buf = Vec::new();

    // Field 1: organizer (optional string)
    if let Some(ref organizer) = event.organizer {
        encode_field_key(&mut buf, 1, 2); // field 1, length-delimited
        encode_varint(&mut buf, organizer.len() as u64);
        buf.extend_from_slice(organizer.as_bytes());
    }

    // Field 2: title (string)
    encode_field_key(&mut buf, 2, 2); // field 2, length-delimited
    encode_varint(&mut buf, event.title.len() as u64);
    buf.extend_from_slice(event.title.as_bytes());

    // Field 3: location (optional string)
    if let Some(ref location) = event.location {
        encode_field_key(&mut buf, 3, 2); // field 3, length-delimited
        encode_varint(&mut buf, location.len() as u64);
        buf.extend_from_slice(location.as_bytes());
    }

    // Field 4: description (optional string)
    if let Some(ref description) = event.description {
        encode_field_key(&mut buf, 4, 2); // field 4, length-delimited
        encode_varint(&mut buf, description.len() as u64);
        buf.extend_from_slice(description.as_bytes());
    }

    // Field 5: start_date (uint64)
    encode_field_key(&mut buf, 5, 0); // field 5, varint
    encode_varint(&mut buf, event.start_date);

    // Field 6: end_date (uint64)
    encode_field_key(&mut buf, 6, 0); // field 6, varint
    encode_varint(&mut buf, event.end_date);

    // Field 7: all_day (bool)
    encode_field_key(&mut buf, 7, 0); // field 7, varint
    encode_varint(&mut buf, if event.all_day { 1 } else { 0 });

    // Field 8: reminder_time_in_secs (repeated uint32)
    for &reminder in &event.reminder_times {
        encode_field_key(&mut buf, 8, 0); // field 8, varint
        encode_varint(&mut buf, reminder as u64);
    }

    buf
}

/// Parse a protobuf field from data
/// Returns (field_number, wire_type, field_data, next_cursor)
fn parse_field(data: &[u8]) -> Option<(u32, u8, &[u8], usize)> {
    if data.is_empty() {
        return None;
    }

    // Read field key (field number + wire type)
    let (key, key_len) = decode_varint(data).ok()?;
    let field_num = (key >> 3) as u32;
    let wire_type = (key & 0x07) as u8;

    let mut cursor = key_len;

    match wire_type {
        0 => {
            // Varint
            let (_value, value_len) = decode_varint(&data[cursor..]).ok()?;
            let field_data = &data[cursor..cursor + value_len];
            cursor += value_len;
            Some((field_num, wire_type, field_data, cursor))
        }
        2 => {
            // Length-delimited
            let (length, len_len) = decode_varint(&data[cursor..]).ok()?;
            cursor += len_len;

            // Bounds check: ensure we have enough data
            let end_pos = cursor + length as usize;
            if end_pos > data.len() {
                warn!(
                    "Length-delimited field claims {} bytes but only {} available (field {}, cursor {})",
                    length, data.len() - cursor, field_num, cursor
                );
                return None;
            }

            let field_data = &data[cursor..end_pos];
            cursor = end_pos;
            Some((field_num, wire_type, field_data, cursor))
        }
        _ => {
            // Unknown wire type - skip
            warn!("Unknown wire type: {}", wire_type);
            None
        }
    }
}

/// Decode a protobuf varint
/// Returns (value, bytes_consumed)
fn decode_varint(data: &[u8]) -> Result<(u64, usize), String> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in data.iter().take(10) {
        bytes_read += 1;
        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Ok((result, bytes_read));
        }

        shift += 7;

        if shift >= 64 {
            return Err("Varint too long".to_string());
        }
    }

    Err("Incomplete varint".to_string())
}

/// Encode a protobuf varint
fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buf.push(byte);

        if value == 0 {
            break;
        }
    }
}

/// Encode a protobuf field key (field number + wire type)
fn encode_field_key(buf: &mut Vec<u8>, field_num: u32, wire_type: u8) {
    let key = (field_num << 3) | (wire_type as u32);
    encode_varint(buf, key as u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encoding() {
        let mut buf = Vec::new();
        encode_varint(&mut buf, 150);
        assert_eq!(buf, vec![0x96, 0x01]);

        buf.clear();
        encode_varint(&mut buf, 1);
        assert_eq!(buf, vec![0x01]);

        buf.clear();
        encode_varint(&mut buf, 300);
        assert_eq!(buf, vec![0xAC, 0x02]);
    }

    #[test]
    fn test_varint_decoding() {
        let data = vec![0x96, 0x01];
        let (value, len) = decode_varint(&data).unwrap();
        assert_eq!(value, 150);
        assert_eq!(len, 2);

        let data = vec![0x01];
        let (value, len) = decode_varint(&data).unwrap();
        assert_eq!(value, 1);
        assert_eq!(len, 1);

        let data = vec![0xAC, 0x02];
        let (decoded_value, len) = decode_varint(&data).unwrap();
        assert_eq!(decoded_value, 300);
        assert_eq!(len, 2);
    }

    #[test]
    fn test_calendar_event_encoding() {
        let event = CalendarEventProto {
            organizer: None,
            title: "Test Event".to_string(),
            location: Some("Test Location".to_string()),
            description: None,
            start_date: 1234567890,
            end_date: 1234571490,
            all_day: false,
            reminder_times: vec![300, 600],
        };

        let encoded = encode_calendar_event(&event);
        assert!(!encoded.is_empty());
        // Verify it contains the title
        let title_bytes = "Test Event".as_bytes();
        assert!(encoded.windows(title_bytes.len()).any(|w| w == title_bytes));
    }
}
