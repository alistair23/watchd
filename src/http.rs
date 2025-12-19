//! HTTP Request/Response handling for Garmin GFDI v2 protocol
//!
//! This module handles parsing and responding to HTTP requests from the watch
//! that are sent over the Bluetooth connection via protobuf messages.
//!
//! The watch sends HTTP requests as ProtobufRequest messages (ID 5043) containing
//! a Smart protobuf with an http_service field (field 2). The response is sent back
//! as a ProtobufResponse message (ID 5044).

use crate::data_transfer::DataTransferHandler;
use crate::garmin_json;
use crate::garmin_weather_api;
use crate::types::{GarminError, Result};
use crate::weather_provider::UnifiedWeatherProvider;
use flate2::write::GzEncoder;
use flate2::Compression;
use log::{debug, error, info};
use reqwest;
use std::collections::HashMap;
use std::io::Write;

/// HTTP methods supported by the watch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get = 1,
    Put = 2,
    Post = 3,
    Delete = 4,
    Patch = 5,
    Head = 6,
}

impl HttpMethod {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(HttpMethod::Get),
            2 => Some(HttpMethod::Put),
            3 => Some(HttpMethod::Post),
            4 => Some(HttpMethod::Delete),
            5 => Some(HttpMethod::Patch),
            6 => Some(HttpMethod::Head),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Put => "PUT",
            HttpMethod::Post => "POST",
            HttpMethod::Delete => "DELETE",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Head => "HEAD",
        }
    }
}

/// Parsed HTTP request from the watch
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub url: String,
    pub path: String,
    pub query: HashMap<String, String>,
    pub method: HttpMethod,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub use_data_xfer: bool,
    pub request_field: u8, // Field number the request came from (1 or 5)
    pub compress_response_body: bool, // ConnectIQ field 7: COMPRESS_RESPONSE_BODY
    pub response_type: Option<u32>, // ConnectIQ field 8: RESPONSE_TYPE
    pub max_response_length: Option<u32>, // ConnectIQ field 5: MAX_RESPONSE_LENGTH
    pub version: Option<u32>, // ConnectIQ field 9: VERSION (VERSION_1=0, VERSION_2=1)
}

impl HttpRequest {
    /// Parse an HTTP request from the protobuf payload
    ///
    /// The protobuf structure is:
    /// Smart {
    ///   http_service = 2 {
    ///     rawRequest = 5 {
    ///       url: string
    ///       method: enum
    ///       headers: repeated Header
    ///       useDataXfer: bool
    ///       rawBody: bytes
    ///     }
    ///   }
    /// }
    pub fn parse(protobuf_payload: &[u8]) -> Result<Self> {
        let mut pos = 0;

        // First field should be field 2 (http_service) with wire type 2 (length-delimited)
        if protobuf_payload.len() < 2 {
            return Err(GarminError::InvalidMessage(
                "Protobuf payload too short".into(),
            ));
        }

        let first_tag = protobuf_payload[pos];
        let field_number = first_tag >> 3;
        let wire_type = first_tag & 0x07;

        if field_number != 2 || wire_type != 2 {
            return Err(GarminError::InvalidMessage(format!(
                "Expected http_service (field 2), got field {}",
                field_number
            )));
        }

        pos += 1;

        // Read length of http_service
        let (http_service_len, len_bytes) = read_varint(&protobuf_payload[pos..])?;
        pos += len_bytes;

        let http_service_end = pos + http_service_len;
        if http_service_end > protobuf_payload.len() {
            return Err(GarminError::InvalidMessage(
                "HTTP service data truncated".into(),
            ));
        }

        let http_service_data = &protobuf_payload[pos..http_service_end];

        // Now parse the HttpService message to extract field 1 (rawRequest)
        Self::parse_http_service(http_service_data)
    }

    fn parse_http_service(data: &[u8]) -> Result<Self> {
        let mut pos = 0;

        // HttpService can contain field 1 (rawRequest) or field 5 (also rawRequest in some variants)
        while pos < data.len() {
            let tag = data[pos];
            let field_number = tag >> 3;
            let wire_type = tag & 0x07;
            pos += 1;

            if (field_number == 1 || field_number == 5) && wire_type == 2 {
                // Found rawRequest field (field 1 or field 5 depending on encoding variant)
                let (len, len_bytes) = read_varint(&data[pos..])?;
                pos += len_bytes;
                let raw_request_data = &data[pos..pos + len];
                if field_number == 1 {
                    info!("      → ConnectIQHTTPRequest (should respond with field 2)");
                } else if field_number == 5 {
                    info!("      → RawResourceRequest (should respond with field 6)");
                }
                debug!("Found rawRequest in field {}, {} bytes", field_number, len);
                return Self::parse_raw_request(raw_request_data, field_number as u8);
            } else {
                // Skip other fields
                skip_field(wire_type, &data[pos..], &mut pos)?;
            }
        }

        Err(GarminError::InvalidMessage(
            "No rawRequest field found in HttpService (tried fields 1 and 5)".into(),
        ))
    }

    fn parse_raw_request(data: &[u8], request_field: u8) -> Result<Self> {
        debug!("Parsing RawRequest: {} bytes", data.len());

        let mut url = String::new();
        let mut method = HttpMethod::Get;
        let mut headers = HashMap::new();
        let mut body = Vec::new();
        let mut use_data_xfer = false;
        let _max_size: Option<u32> = None;
        let _connection_timeout: Option<u32> = None;
        let _read_timeout: Option<u32> = None;
        let compress_response_body = false;
        let mut response_type: Option<u32> = None;
        let mut max_response_length: Option<u32> = None;
        let mut version: Option<u32> = None;

        let mut pos = 0;

        while pos < data.len() {
            if pos >= data.len() {
                break;
            }

            let tag = data[pos];
            let field_number = tag >> 3;
            let wire_type = tag & 0x07;
            pos += 1;

            match field_number {
                1 => {
                    // url (string) - field 1, wire type 2
                    if wire_type != 2 {
                        return Err(GarminError::InvalidMessage(
                            "Invalid wire type for url".into(),
                        ));
                    }
                    let (len, len_bytes) = read_varint(&data[pos..])?;
                    pos += len_bytes;
                    url = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                    debug!("Parsed URL: {}", url);
                    pos += len;
                }
                2 => {
                    // method (enum) - field 2, wire type 0 (varint)
                    if wire_type != 0 {
                        error!(
                            "Unexpected wire type {} for method field, skipping",
                            wire_type
                        );
                        skip_field(wire_type, &data[pos..], &mut pos)?;
                        continue;
                    }
                    let (method_val, len_bytes) = read_varint(&data[pos..])?;
                    pos += len_bytes;

                    // Method value should be small (1-6). If it's huge, it's probably not a method field
                    if method_val > 10 {
                        error!("Method value {} too large, likely field encoding issue. Defaulting to GET", method_val);
                        method = HttpMethod::Get;
                    } else {
                        method = HttpMethod::from_u8(method_val as u8).unwrap_or_else(|| {
                            error!(
                                "Unknown HTTP method value: {}, defaulting to GET",
                                method_val
                            );
                            HttpMethod::Get
                        });
                        error!("Parsed method: {:?}", method);
                    }
                }
                3 => {
                    // Field 3: For ConnectIQ (field 1) this is HTTP_HEADER_FIELDS (Monkey-C encoded)
                    //          For RawResourceRequest (field 5) this is method or repeated headers
                    if request_field == 1 {
                        // ConnectIQ: HTTP_HEADER_FIELDS as Monkey-C encoded bytes
                        if wire_type == 2 {
                            let (len, len_bytes) = read_varint(&data[pos..])?;
                            pos += len_bytes;
                            let header_data = &data[pos..pos + len];

                            info!("Parsing ConnectIQ headers (Monkey-C format): {} bytes", len);
                            let header_hex: String = header_data
                                .iter()
                                .take(100)
                                .map(|b| format!("{:02X}", b))
                                .collect::<Vec<_>>()
                                .join(" ");
                            println!("      Header data hex (first 100 bytes): {}", header_hex);

                            // Pass the complete header data to garmin_json::decode
                            // The decoder expects the full format including magic bytes
                            match garmin_json::decode(header_data) {
                                Ok(json_value) => {
                                    let json_str = serde_json::to_string(&json_value)
                                        .unwrap_or_else(|_| "{}".to_string());
                                    debug!("Decoded headers JSON: {}", json_str);

                                    // Parse the JSON to extract header key-value pairs
                                    if let Ok(json_value) =
                                        serde_json::from_str::<serde_json::Value>(&json_str)
                                    {
                                        if let Some(obj) = json_value.as_object() {
                                            for (key, value) in obj {
                                                // Convert value to string
                                                let value_str = match value {
                                                    serde_json::Value::String(s) => s.clone(),
                                                    serde_json::Value::Number(n)
                                                        if n.as_i64() == Some(1) =>
                                                    {
                                                        // Special case: integer 1 for Content-Type means application/json
                                                        if key.to_lowercase() == "content-type" {
                                                            "application/json".to_string()
                                                        } else {
                                                            n.to_string()
                                                        }
                                                    }
                                                    _ => value.to_string(),
                                                };
                                                debug!("Header: {} = {}", key, value_str);
                                                headers.insert(key.to_lowercase(), value_str);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to decode Monkey-C headers: {}", e);
                                }
                            }
                            pos += len;
                        } else {
                            error!(
                                "Unexpected wire type {} for ConnectIQ field 3, skipping",
                                wire_type
                            );
                            skip_field(wire_type, &data[pos..], &mut pos)?;
                        }
                    } else {
                        // RawResourceRequest: method (wire type 0) or repeated headers (wire type 2)
                        if wire_type == 0 {
                            // Wire type 0 (varint) = method enum
                            let (method_val, len_bytes) = read_varint(&data[pos..])?;
                            pos += len_bytes;

                            if method_val <= 10 {
                                method = HttpMethod::from_u8(method_val as u8).unwrap_or_else(|| {
                                    error!(
                                        "Unknown HTTP method value in field 3: {}, defaulting to GET",
                                        method_val
                                    );
                                    HttpMethod::Get
                                });
                                debug!(
                                    "Parsed method from field 3: {:?} (value: {})",
                                    method, method_val
                                );
                            } else {
                                error!(
                                    "Field 3 value {} too large for method, skipping",
                                    method_val
                                );
                            }
                        } else if wire_type == 2 {
                            // Wire type 2 (length-delimited) = header
                            let (len, len_bytes) = read_varint(&data[pos..])?;
                            pos += len_bytes;
                            let header_data = &data[pos..pos + len];

                            match parse_header(header_data) {
                                Ok((key, value)) => {
                                    info!("Parsed header from field 3: {} = {}", key, value);
                                    headers.insert(key.to_lowercase(), value);
                                }
                                Err(e) => {
                                    error!("Failed to parse header from field 3: {}", e);
                                }
                            }
                            pos += len;
                        } else {
                            // Other wire types in field 3 - skip
                            error!(
                                "Field 3 with wire type {} (not method or header), skipping",
                                wire_type
                            );
                            skip_field(wire_type, &data[pos..], &mut pos)?;
                        }
                    }
                }
                4 => {
                    // rawBody (bytes) - field 4, wire type 2
                    if wire_type != 2 {
                        return Err(GarminError::InvalidMessage(
                            "Invalid wire type for rawBody".into(),
                        ));
                    }
                    let (len, len_bytes) = read_varint(&data[pos..])?;
                    pos += len_bytes;
                    let body_data = &data[pos..pos + len];
                    debug!("   📦 RAW BODY DATA FROM WATCH:");
                    debug!("      Size: {} bytes", body_data.len());
                    let hex_dump: String = body_data
                        .iter()
                        .take(200)
                        .map(|b| format!("{:02X}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    debug!("      Hex (first 200 bytes): {}", hex_dump);

                    // Check for Garmin's custom encoding with AB CD AB CD marker
                    if body_data.len() >= 4
                        && body_data[0] == 0xAB
                        && body_data[1] == 0xCD
                        && body_data[2] == 0xAB
                        && body_data[3] == 0xCD
                    {
                        debug!("Detected Garmin custom body encoding (AB CD AB CD marker)");

                        debug!("Extracted body: {} bytes", body_data.len());
                        debug!(
                            "Extracted body (first 1000 bytes): {:02X?}",
                            &body_data[..std::cmp::min(1000, body_data.len())]
                        );
                        debug!(
                            "Extracted body as string: {}",
                            String::from_utf8_lossy(
                                &body_data[..std::cmp::min(1000, body_data.len())]
                            )
                        );

                        match garmin_json::decode(&body_data) {
                            Ok(json_value) => {
                                let json_body = serde_json::to_vec(&json_value)
                                    .unwrap_or_else(|_| b"{}".to_vec());
                                debug!(
                                    "Successfully converted Garmin body to JSON: {} bytes",
                                    json_body.len()
                                );
                                debug!("      ✅ Conversion successful: {} bytes", json_body.len());
                                debug!(
                                    "      Output JSON: {}",
                                    String::from_utf8_lossy(&json_body)
                                );

                                // No transform needed with new garmin_json module
                                // The decode function handles the format directly
                                body = json_body;
                            }
                            Err(e) => {
                                error!(
                                    "Failed to convert Garmin body to JSON: {}, using raw data",
                                    e
                                );
                                // Keep the raw body as-is
                                body = body_data.to_vec();
                            }
                        }
                    } else {
                        debug!("Using standard body encoding (no AB CD marker)");
                        // Standard encoding
                        body = body_data.to_vec();
                    }

                    debug!("Parsed body: {} bytes", body.len());
                    pos += len;
                }
                5 => {
                    // Field 5: headers (repeated Header) OR MAX_RESPONSE_LENGTH (int32)
                    if wire_type == 2 {
                        // This is a header (repeated)
                        let (len, len_bytes) = read_varint(&data[pos..])?;
                        pos += len_bytes;
                        let header_data = &data[pos..pos + len];
                        let (key, value) = parse_header(header_data)?;
                        debug!("Parsed header from field 5: {} = {}", key, value);
                        headers.insert(key.to_lowercase(), value);
                        pos += len;
                    } else if wire_type == 0 {
                        // MAX_RESPONSE_LENGTH (int32) - ConnectIQ only
                        let (val, len_bytes) = read_varint(&data[pos..])?;
                        pos += len_bytes;
                        max_response_length = Some(val as u32);
                        debug!("Parsed maxResponseLength: {}", val);
                    } else {
                        debug!("Unexpected wire type {} for field 5, skipping", wire_type);
                        skip_field(wire_type, &data[pos..], &mut pos)?;
                    }
                }
                6 => {
                    // Field 6: useDataXfer (bool) OR INCLUDE_HTTP_HEADER_FIELDS_IN_RESPONSE (ConnectIQ)
                    if wire_type == 0 {
                        let (val, len_bytes) = read_varint(&data[pos..])?;
                        pos += len_bytes;
                        if request_field == 1 {
                            // ConnectIQ: INCLUDE_HTTP_HEADER_FIELDS_IN_RESPONSE
                            debug!("Parsed includeHttpHeaderFieldsInResponse: {}", val != 0);
                        } else {
                            // RawResourceRequest: useDataXfer
                            use_data_xfer = val != 0;
                            debug!("Parsed useDataXfer: {}", use_data_xfer);
                        }
                    } else {
                        debug!("Unexpected wire type {} for field 6, skipping", wire_type);
                        skip_field(wire_type, &data[pos..], &mut pos)?;
                    }
                }

                7 => {
                    if request_field == 1 {
                        // ConnectIQ: Field 7 = COMPRESS_RESPONSE_BODY (bool)
                        if wire_type == 0 {
                            let (_val, len_bytes) = read_varint(&data[pos..])?;
                            pos += len_bytes;
                            // compress_response_body = val != 0;
                            // debug!("Parsed compressResponseBody: {}", compress_response_body);
                        } else {
                            debug!("Unexpected wire type {} for field 7, skipping", wire_type);
                            skip_field(wire_type, &data[pos..], &mut pos)?;
                        }
                    } else {
                        // RawResourceRequest: Field 7 = Alternative body field (JSON body)
                        // Used by Garmin's own services for JSON payloads
                        // Only parse if wire type 2 (length-delimited), otherwise skip
                        if wire_type == 2 {
                            let (len, len_bytes) = read_varint(&data[pos..])?;
                            pos += len_bytes;
                            let body_data = &data[pos..pos + len];
                            debug!("Body field 7 found: {} bytes total", body_data.len());

                            // Field 7 typically contains standard JSON/text, not Garmin encoding
                            body = body_data.to_vec();

                            debug!("Extracted body from field 7: {} bytes", body.len());
                            debug!(
                                "Body as string: {}",
                                String::from_utf8_lossy(&body[..std::cmp::min(200, body.len())])
                            );
                            pos += len;
                        } else {
                            // Field 7 with other wire types (e.g., varint) - skip it
                            debug!("Field 7 with wire type {} (not body), skipping", wire_type);
                            skip_field(wire_type, &data[pos..], &mut pos)?;
                        }
                    }
                }
                8 => {
                    // Field 8: RESPONSE_TYPE (int32) - ConnectIQ only
                    if request_field == 1 {
                        if wire_type == 0 {
                            let (val, len_bytes) = read_varint(&data[pos..])?;
                            pos += len_bytes;
                            response_type = Some(val as u32);
                            debug!("   📦 ConnectIQ responseType: {}", val);
                        } else {
                            debug!("Unexpected wire type {} for field 8, skipping", wire_type);
                            skip_field(wire_type, &data[pos..], &mut pos)?;
                        }
                    } else {
                        skip_field(wire_type, &data[pos..], &mut pos)?;
                    }
                }
                9 => {
                    // Field 9: VERSION (int32) - ConnectIQ only
                    if request_field == 1 {
                        if wire_type == 0 {
                            let (val, len_bytes) = read_varint(&data[pos..])?;
                            pos += len_bytes;
                            version = Some(val as u32);
                            debug!("   📦 ConnectIQ version: {}", val);
                        } else {
                            debug!("Unexpected wire type {} for field 9, skipping", wire_type);
                            skip_field(wire_type, &data[pos..], &mut pos)?;
                        }
                    } else {
                        skip_field(wire_type, &data[pos..], &mut pos)?;
                    }
                }
                _ => {
                    // Skip unknown fields
                    skip_field(wire_type, &data[pos..], &mut pos)?;
                }
            }
        }

        // Parse URL into path and query
        let (path, query) = parse_url(&url);

        debug!(
            "Final parsed request: {} {} (headers: {}, body: {} bytes)",
            method.as_str(),
            path,
            headers.len(),
            body.len()
        );

        // Log detailed summary
        debug!("   ✅ Parsed HTTP request successfully:");
        debug!("      Method: {}", method.as_str());
        debug!("      URL: {}", url);
        debug!("      Path: {}", path);
        debug!("      Headers: {}", headers.len());
        for (key, value) in &headers {
            let value_preview = if value.len() > 50 {
                format!("{}... ({} bytes)", &value[..50], value.len())
            } else {
                value.clone()
            };
            debug!("         {}: {}", key, value_preview);
        }
        debug!("      Body: {} bytes", body.len());
        if body.len() > 0 {
            debug!(
                "      Body preview (first 100 chars): {}",
                String::from_utf8_lossy(&body[..std::cmp::min(100, body.len())])
            );
        }

        let request = HttpRequest {
            url,
            path,
            query,
            method,
            headers,
            body,
            use_data_xfer,
            request_field,
            compress_response_body,
            response_type,
            max_response_length,
            version,
        };

        Ok(request)
    }
}

/// HTTP response to send back to the watch
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }

    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    pub fn with_json_body(mut self, json: &str) -> Self {
        self.body = json.as_bytes().to_vec();
        self.headers
            .insert("Content-Type".to_string(), "application/json".to_string());
        self
    }

    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    pub fn ok() -> Self {
        Self::new(200)
    }

    pub fn not_found() -> Self {
        Self::new(404)
    }

    pub fn internal_error() -> Self {
        Self::new(500)
    }

    /// Encode this response as a ProtobufResponse message
    /// Encode the HTTP response as a protobuf ProtobufResponse message
    ///
    /// This matches the GadgetBridge createRawResponse implementation:
    /// - Checks accept-encoding header and compresses response if "gzip" is requested
    /// - Handles useDataXfer flag (sends via field 4 instead of field 3 for status 200)
    /// - Sets status based on HTTP status code (OK for 2xx, UNKNOWN otherwise)
    pub fn encode_protobuf_response(
        &self,
        request_id: u16,
        request: &HttpRequest,
        data_transfer: Option<&DataTransferHandler>,
    ) -> Result<Vec<u8>> {
        // Determine which field to use for the response based on the request field
        let response_field = if request.request_field == 1 {
            2 // ConnectIQHTTPRequest (field 1) → ConnectIQHTTPResponse (field 2)
        } else {
            6 // RawResourceRequest (field 5) → RawResourceResponse (field 6)
        };

        // For ConnectIQ responses (field 2), use simpler structure with just Monkey C body
        if response_field == 2 {
            return self.encode_connectiq_response(request_id, request);
        }

        // Build the RawResponse protobuf for field 6
        let mut raw_response = Vec::new();

        // Field 1: status (enum ResponseStatus) - wire type 0
        // ResponseStatus::OK = 100 (0x64) per GDIConnectIQHTTPProto
        raw_response.push((1 << 3) | 0); // Field 1, varint
        let status_enum = if self.status / 100 == 2 {
            100 // ResponseStatus::OK for 2xx status codes
        } else {
            0 // ResponseStatus::UNKNOWN for non-2xx
        };
        raw_response.extend_from_slice(&encode_varint(status_enum));

        // Field 2: httpStatus (int32) - wire type 0
        // This is the actual HTTP status code (200, 404, etc.)
        raw_response.push((2 << 3) | 0); // Field 2, varint
        raw_response.extend_from_slice(&encode_varint(self.status as usize));

        // Prepare response body (with optional gzip compression)
        let mut response_headers = self.headers.clone();
        let body_data = if !self.body.is_empty() {
            // Check if request wants gzip compression
            let accept_encoding = request
                .headers
                .get("accept-encoding")
                .or_else(|| request.headers.get("Accept-Encoding"))
                .map(|s| s.as_str())
                .unwrap_or("");

            debug!("      Original body: {:x?}", &self.body);
            debug!(
                "      Original body: {}",
                String::from_utf8_lossy(&self.body)
            );

            if accept_encoding == "gzip" {
                info!("   🗜️  Compressing response with gzip");
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(&self.body).map_err(|e| {
                    GarminError::InvalidMessage(format!("Failed to compress response: {}", e))
                })?;
                let compressed = encoder.finish().map_err(|e| {
                    GarminError::InvalidMessage(format!("Failed to finish compression: {}", e))
                })?;

                // Add Content-Encoding header
                response_headers.insert("Content-Encoding".to_string(), "gzip".to_string());
                debug!("      Original size: {} bytes", self.body.len());
                debug!("      Compressed size: {} bytes", compressed.len());
                compressed
            } else {
                // For uncompressed data, explicitly set Content-Encoding to identity
                // This tells the watch not to attempt decompression
                // response_headers.insert("Content-Encoding".to_string(), "identity".to_string());
                self.body.clone()
            }
        } else {
            Vec::new()
        };

        // Check if we should use DataTransfer (field 4) instead of body (field 3)
        // This matches GadgetBridge: useDataXfer && status == 200
        let should_use_data_transfer = self.status == 200 && request.use_data_xfer;

        let transfer_id = if should_use_data_transfer && data_transfer.is_some() {
            let handler = data_transfer.unwrap();
            let id = handler.register(body_data.clone());
            info!("   📦 Using DataTransfer for response body");
            info!("      Transfer ID: {}", id);
            info!("      Body size: {} bytes", body_data.len());
            Some(id)
        } else if should_use_data_transfer {
            error!("   ⚠️  DataTransfer needed but handler not provided - falling back to body");
            error!("      Body size: {} bytes", body_data.len());
            None
        } else {
            None
        };

        // Field 3: body (bytes) - wire type 2, OR
        // Field 4: xferData (DataTransferItem) - wire type 2
        if transfer_id.is_some() {
            // Use DataTransfer (field 4)
            let id = transfer_id.unwrap();

            // Build DataTransferItem protobuf: { id: u32, size: u32 }
            let mut xfer_data = Vec::new();
            xfer_data.push((1 << 3) | 0); // Field 1: id (varint)
            xfer_data.extend_from_slice(&encode_varint(id as usize));
            xfer_data.push((2 << 3) | 0); // Field 2: size (varint)
            xfer_data.extend_from_slice(&encode_varint(body_data.len()));

            info!("   📦 Sending DataTransfer reference (field 4)");
            info!("      Transfer ID: {}", id);
            info!("      Size: {} bytes", body_data.len());

            raw_response.push((4 << 3) | 2); // Field 4, length-delimited
            raw_response.extend_from_slice(&encode_varint(xfer_data.len()));
            raw_response.extend_from_slice(&xfer_data);
        } else if !body_data.is_empty() {
            // Use direct body (field 3)
            info!("   📦 Sending response body as raw JSON/bytes (not converting)");
            info!("      Response size: {} bytes", body_data.len());
            if body_data.len() < 1000 {
                debug!(
                    "      Response content: {}",
                    String::from_utf8_lossy(&body_data)
                );
                debug!("      Response content direct: {:?}", &body_data);
            }

            raw_response.push((3 << 3) | 2); // Field 3, length-delimited
            raw_response.extend_from_slice(&encode_varint(body_data.len()));
            raw_response.extend_from_slice(&body_data);
        }

        // Field 5: headers (repeated Header) - wire type 2
        // Skip headers with empty values as they might cause parsing issues
        let non_empty_headers: Vec<_> = response_headers
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();

        let empty_count = response_headers.len() - non_empty_headers.len();
        debug!(
            "   📋 Encoding {} response headers (skipping {} empty):",
            non_empty_headers.len(),
            empty_count
        );

        for (key, value) in &non_empty_headers {
            let header = encode_header(key, value);
            info!(
                "      Header: {} = {} ({} bytes encoded)",
                key,
                value,
                header.len()
            );
            let header_hex: String = header
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join(" ");
            debug!("        Hex: {}", header_hex);
            raw_response.push((5 << 3) | 2); // Field 5, length-delimited
            raw_response.extend_from_slice(&encode_varint(header.len()));
            raw_response.extend_from_slice(&header);
        }

        // Build HttpService with appropriate response field
        let mut http_service = Vec::new();
        http_service.push((response_field << 3) | 2); // Field 2 or 6, length-delimited
        http_service.extend_from_slice(&encode_varint(raw_response.len()));
        http_service.extend_from_slice(&raw_response);

        debug!("   📋 RawResponse structure:");
        debug!("      Field 1 (status): {} (enum value)", status_enum);
        debug!("      Field 2 (httpStatus): {}", self.status);
        debug!("      Field 3 (body): {} bytes", self.body.len());
        debug!(
            "      Field 5 (headers): {} headers (non-empty), {} skipped",
            non_empty_headers.len(),
            empty_count
        );
        debug!("      Total RawResponse size: {} bytes", raw_response.len());

        // Dump complete RawResponse hex for debugging
        let raw_hex: String = raw_response
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<_>>()
            .join(" ");
        debug!("      RawResponse hex (complete):");
        for (i, chunk) in raw_hex.as_bytes().chunks(96).enumerate() {
            debug!("        {:04}: {}", i * 48, String::from_utf8_lossy(chunk));
        }

        // Build Smart message with http_service (field 2)
        let mut smart_proto = Vec::new();
        smart_proto.push((2 << 3) | 2); // Field 2, length-delimited
        smart_proto.extend_from_slice(&encode_varint(http_service.len()));
        smart_proto.extend_from_slice(&http_service);

        // Dump HttpService hex
        let http_service_hex: String = http_service
            .iter()
            .take(512)
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<_>>()
            .join(" ");
        debug!(
            "      HttpService hex (first 512 bytes): {}",
            http_service_hex
        );

        // Dump Smart proto hex
        let smart_hex: String = smart_proto
            .iter()
            .take(512)
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<_>>()
            .join(" ");
        debug!("      Smart proto hex (first 512 bytes): {}", smart_hex);

        // Build ProtobufResponse message (ID 5044)
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: PROTOBUF_RESPONSE (5044)
        message.extend_from_slice(&5044u16.to_le_bytes());

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

        // Add checksum (CRC-16)
        let checksum = compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        debug!("   📋 Final ProtobufResponse message:");
        debug!("      Packet size: {} bytes", packet_size);
        debug!("      Request ID: {}", request_id);
        debug!("      Protobuf payload size: {} bytes", smart_proto.len());
        debug!("      Total message size: {} bytes", message.len());
        let msg_hex: String = message
            .iter()
            .take(128)
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<_>>()
            .join(" ");
        debug!("      Message hex (first 128 bytes): {}", msg_hex);

        Ok(message)
    }

    /// Encode a ConnectIQ HTTP response (field 2)
    /// This uses a simpler structure with just the Monkey C encoded body
    fn encode_connectiq_response(&self, request_id: u16, request: &HttpRequest) -> Result<Vec<u8>> {
        info!("   📦 Encoding ConnectIQ response (field 2)");

        // Convert JSON response body to Monkey C format
        let monkeyc_body = if !self.body.is_empty() {
            info!("   📦 Converting JSON response to Monkey C format");
            debug!("      Response size: {} bytes", self.body.len());
            if self.body.len() < 500 {
                debug!(
                    "      Response content: {}",
                    String::from_utf8_lossy(&self.body)
                );
            }

            // Parse JSON and encode to Garmin format
            // If body is not valid JSON, wrap it as a string
            let json_value = match serde_json::from_slice::<serde_json::Value>(&self.body) {
                Ok(v) => v,
                Err(_) => {
                    // Not valid JSON, treat as plain text and wrap it
                    debug!("      Response is not JSON, wrapping as string");
                    let text = String::from_utf8_lossy(&self.body).to_string();
                    serde_json::Value::String(text)
                }
            };

            match garmin_json::encode(&json_value) {
                Ok(body) => {
                    info!("   ✅ Converted to Monkey C body: {} bytes", body.len());
                    body
                }
                Err(e) => {
                    error!("      ❌ Failed to encode to Garmin format: {:?}", e);
                    return Err(e);
                }
            }
        } else {
            Vec::new()
        };

        // Build ConnectIQHTTPResponse with proper field structure
        let mut connectiq_response = Vec::new();

        // Field 1: status (enum ResponseStatus) - wire type 0
        // ResponseStatus::OK = 100 (0x64) per GDIConnectIQHTTPProto
        connectiq_response.push((1 << 3) | 0); // Field 1 (STATUS_FIELD_NUMBER), varint
        let status_enum = if self.status / 100 == 2 {
            100 // ResponseStatus::OK for 2xx status codes
        } else {
            101 // ResponseStatus::UNKNOWN for other status codes
        };
        connectiq_response.extend_from_slice(&encode_varint(status_enum));

        // Field 2: httpStatusCode (int32) - wire type 0
        // This is the actual HTTP status code (200, 404, etc.)
        connectiq_response.push((2 << 3) | 0); // Field 2 (HTTP_STATUS_CODE_FIELD_NUMBER), varint
        connectiq_response.extend_from_slice(&encode_varint(self.status as usize));

        // Check if compression is requested
        let (body_data, inflated_size) = if request.compress_response_body {
            info!("   🗜️  Compressing response body (gzip)...");
            let uncompressed_size = monkeyc_body.len();

            match compress_gzip(&monkeyc_body) {
                Ok(compressed) => {
                    info!(
                        "   ✅ Compressed: {} bytes → {} bytes ({:.1}% reduction)",
                        uncompressed_size,
                        compressed.len(),
                        (1.0 - (compressed.len() as f64 / uncompressed_size as f64)) * 100.0
                    );
                    (compressed, Some(uncompressed_size))
                }
                Err(e) => {
                    error!("   ⚠️  Compression failed: {}, using uncompressed", e);
                    (monkeyc_body.clone(), None)
                }
            }
        } else {
            (monkeyc_body.clone(), None)
        };

        // Field 3: httpBody (bytes) - wire type 2
        // This contains the Monkey C encoded response body (possibly compressed)
        if !body_data.is_empty() {
            connectiq_response.push((3 << 3) | 2); // Field 3 (HTTP_BODY_FIELD_NUMBER), length-delimited
            connectiq_response.extend_from_slice(&encode_varint(body_data.len()));
            connectiq_response.extend_from_slice(&body_data);
        }

        // Field 4: httpHeaderFields (bytes) - wire type 2
        // Optional: encoded headers if needed
        // TODO: Add header encoding if required

        // Field 5: inflatedSize (int32) - wire type 0
        // Set this when body is compressed
        if let Some(size) = inflated_size {
            connectiq_response.push((5 << 3) | 0); // Field 5, varint
            connectiq_response.extend_from_slice(&encode_varint(size));
            debug!("      Field 5 (inflatedSize): {}", size);
        }

        // Field 6: responseType (int32) - wire type 0
        // Echo back the response type from the request if present
        if let Some(resp_type) = request.response_type {
            connectiq_response.push((6 << 3) | 0); // Field 6, varint
            connectiq_response.extend_from_slice(&encode_varint(resp_type as usize));
            debug!("      Field 6 (responseType): {}", resp_type);
        }

        debug!("   📋 ConnectIQHTTPResponse structure:");
        debug!("      Field 1 (status): {}", status_enum);
        debug!("      Field 2 (httpStatusCode): {}", self.status);
        debug!(
            "      Field 3 (httpBody): {} bytes{}",
            body_data.len(),
            if inflated_size.is_some() {
                " (gzip compressed)"
            } else {
                " (uncompressed)"
            }
        );

        // Debug: print ConnectIQHTTPResponse hex
        let connectiq_hex: String = connectiq_response
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<_>>()
            .join(" ");
        println!("      Raw ConnectIQHTTPResponse hex: {}", connectiq_hex);
        println!(
            "      ConnectIQHTTPResponse size: {} bytes",
            connectiq_response.len()
        );

        // Build HttpService with field 2 (ConnectIQHTTPResponse)
        let mut http_service = Vec::new();
        http_service.push((2 << 3) | 2); // Field 2, length-delimited
        let http_service_len_varint = encode_varint(connectiq_response.len());
        http_service.extend_from_slice(&http_service_len_varint);
        http_service.extend_from_slice(&connectiq_response);

        // Build Smart protobuf wrapper
        let mut smart_proto = Vec::new();
        smart_proto.push((2 << 3) | 2); // Field 2, length-delimited
        let smart_proto_len_varint = encode_varint(http_service.len());
        smart_proto.extend_from_slice(&smart_proto_len_varint);
        smart_proto.extend_from_slice(&http_service);

        println!("   📋 HttpService:");
        println!("      Tag: 0x{:02X}", (2 << 3) | 2);
        println!(
            "      Length varint: {} bytes = {}",
            http_service_len_varint.len(),
            connectiq_response.len()
        );
        println!("      Payload: {} bytes", connectiq_response.len());
        println!("      Total: {} bytes", http_service.len());
        println!("   📋 Smart proto:");
        println!("      Tag: 0x{:02X}", (2 << 3) | 2);
        println!(
            "      Length varint: {} bytes = {}",
            smart_proto_len_varint.len(),
            http_service.len()
        );
        println!("      Payload: {} bytes", http_service.len());
        println!("      Total: {} bytes", smart_proto.len());

        // Build ProtobufResponse message (ID 5044)
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: PROTOBUF_RESPONSE (5044)
        message.extend_from_slice(&5044u16.to_le_bytes());

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

        // Calculate packet size: total message size INCLUDING size field and checksum
        // Packet size = size_field (2) + message_id (2) + request_id (2) + data_offset (4) + total_proto_len (4) + proto_data_len (4) + proto_payload (N)
        // This is just message.len() before adding the checksum
        let packet_size = message.len() as u16 + 2;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        debug!("   📋 ProtobufResponse structure:");
        debug!("      Size field: {} (0x{:04X})", packet_size, packet_size);
        debug!("      Message ID: 0x{:04X}", 5044);
        debug!("      Request ID: {}", request_id);
        debug!("      Data offset: 0");
        debug!("      Total protobuf length: {} bytes", smart_proto.len());
        debug!("      Protobuf data length: {} bytes", smart_proto.len());
        debug!("      Protobuf payload: {} bytes", smart_proto.len());
        debug!("      Message before checksum: {} bytes", message.len());
        debug!(
            "      Expected total with checksum: {} bytes",
            message.len() + 2
        );

        // Add checksum (CRC-16) - computed over size field + all data
        let checksum = compute_checksum(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        println!("   📋 Final ConnectIQ ProtobufResponse:");
        println!("      Total message size: {} bytes", message.len());
        println!(
            "      Breakdown: size(2) + header(16) + payload({}) + checksum(2)",
            smart_proto.len()
        );
        println!(
            "      Calculated: 2 + 16 + {} + 2 = {}",
            smart_proto.len(),
            2 + 16 + smart_proto.len() + 2
        );
        println!("      Checksum: 0x{:04X}", checksum);

        Ok(message)
    }
}

/// Handle an HTTP request with optional weather provider for Garmin weather API interception
///
/// If a weather provider is provided and the request is to Garmin's weather API,
/// it will be intercepted and responded to locally instead of being proxied.
pub async fn handle_http_request_with_weather(
    request: &HttpRequest,
    weather_provider: Option<&UnifiedWeatherProvider>,
) -> HttpResponse {
    println!(
        "🌐 HTTP Request: {} {}",
        request.method.as_str(),
        request.path
    );
    println!("   Full URL: {}", request.url);

    // Check if this URL should be blocked
    if garmin_weather_api::is_garmin_blocked_url(&request.url) {
        println!("   🚫 Blocking request to Garmin service");
        return garmin_weather_api::handle_garmin_blocked_request(request);
    }

    // Check if this is a Garmin weather API request that should be intercepted
    if let Some(provider) = weather_provider {
        if garmin_weather_api::is_garmin_weather_api(&request.url) {
            println!("   🎯 Intercepting Garmin weather API request");
            match garmin_weather_api::handle_garmin_weather_request(request, provider).await {
                Ok(response) => {
                    println!(
                        "   ✅ Weather API handled locally, status: {}",
                        response.status
                    );
                    return response;
                }
                Err(e) => {
                    eprintln!("   ❌ Weather API handler error: {}", e);
                    // Fall back to proxying if local handling fails
                    println!("   ⚠️  Falling back to proxy");
                }
            }
        }
    }

    // Proxy the request to the internet
    match proxy_http_request(request).await {
        Ok(response) => {
            println!("   ✅ Proxied successfully, status: {}", response.status);
            response
        }
        Err(e) => {
            eprintln!("   ❌ Proxy error: {}", e);
            HttpResponse::internal_error()
                .with_json_body(&format!(r#"{{"error":"Proxy failed: {}"}}"#, e))
        }
    }
}

/// Proxy an HTTP request to the actual internet destination
async fn proxy_http_request(request: &HttpRequest) -> Result<HttpResponse> {
    println!("   🔄 Proxying to: {}", request.url);
    println!("   📋 Request details:");
    println!("      Method: {}", request.method.as_str());
    println!("      Headers: {} total", request.headers.len());
    for (key, value) in &request.headers {
        let value_preview = if value.len() > 50 {
            format!("{}... ({} bytes)", &value[..50], value.len())
        } else {
            value.clone()
        };
        println!("         {}: {}", key, value_preview);
    }
    println!("      Body: {} bytes", request.body.len());
    if !request.body.is_empty() {
        let body_preview = if request.body.len() > 200 {
            format!(
                "{}... ({} bytes total)",
                String::from_utf8_lossy(&request.body[..200]),
                request.body.len()
            )
        } else {
            String::from_utf8_lossy(&request.body).to_string()
        };
        println!("      Body content: {}", body_preview);
        println!(
            "      Body hex (first 64 bytes): {:02X?}",
            &request.body[..std::cmp::min(64, request.body.len())]
        );
    }

    // Convert OAuth requests to form-encoded format
    let mut headers_copy = request.headers.clone();
    let final_body = if request.url.contains("/oauth/token") || request.url.contains("/token") {
        convert_oauth_to_form_encoded(&request.body, &mut headers_copy)?
    } else {
        request.body.clone()
    };

    // Create a blocking reqwest client
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| GarminError::InvalidMessage(format!("Failed to create HTTP client: {}", e)))?;

    // Build the request
    let mut req_builder = match request.method {
        HttpMethod::Get => client.get(&request.url),
        HttpMethod::Post => client.post(&request.url),
        HttpMethod::Put => client.put(&request.url),
        HttpMethod::Delete => client.delete(&request.url),
        HttpMethod::Patch => client.patch(&request.url),
        HttpMethod::Head => client.head(&request.url),
    };

    // Add headers (including any updated by OAuth conversion)
    for (key, value) in &headers_copy {
        // Skip some headers that shouldn't be forwarded
        let key_lower = key.to_lowercase();
        if key_lower == "host" || key_lower == "connection" || key_lower == "accept-encoding" {
            // Skip host, connection, and accept-encoding headers
            // We handle compression ourselves, not the remote server
            if key_lower == "accept-encoding" {
                println!(
                    "   ℹ️  Skipping accept-encoding header (will compress ourselves if needed)"
                );
            }
            continue;
        }

        // Validate header name and value before adding
        // Header names must be ASCII and non-empty
        if key.is_empty() || !key.chars().all(|c| c.is_ascii() && !c.is_control()) {
            println!("   ⚠️  Skipping invalid header name: {:?}", key);
            continue;
        }

        // Try to add the header, skip if it fails
        match reqwest::header::HeaderName::from_bytes(key.as_bytes()) {
            Ok(header_name) => match reqwest::header::HeaderValue::from_str(value) {
                Ok(header_value) => {
                    println!(
                        "   ✅ Adding header: {} = {}",
                        key,
                        if value.len() > 50 {
                            format!("{}...", &value[..50])
                        } else {
                            value.clone()
                        }
                    );
                    req_builder = req_builder.header(header_name, header_value);
                }
                Err(e) => {
                    println!(
                        "   ⚠️  Skipping header '{}' with invalid value: {} (error: {})",
                        key, value, e
                    );
                    println!("      Value bytes: {:02X?}", value.as_bytes());
                }
            },
            Err(e) => {
                println!("   ⚠️  Skipping invalid header name '{}': {}", key, e);
                println!("      Name bytes: {:02X?}", key.as_bytes());
            }
        }
    }

    // Add body if present
    if !final_body.is_empty() {
        println!("   📦 Adding body to request: {} bytes", final_body.len());
        req_builder = req_builder.body(final_body.clone());
    } else {
        println!("   ℹ️  No body to send (body is empty)");
    }

    // Send the request
    println!("   📤 Sending request to remote server...");

    // Build the request to inspect it before sending
    let built_request = req_builder
        .build()
        .map_err(|e| GarminError::InvalidMessage(format!("Failed to build HTTP request: {}", e)))?;

    println!("   🔍 ACTUAL REQUEST BEING SENT:");
    println!("      Method: {}", built_request.method());
    println!("      URL: {}", built_request.url());
    println!("      Headers ({} total):", built_request.headers().len());
    for (name, value) in built_request.headers() {
        let value_str = value.to_str().unwrap_or("<binary>");
        let value_preview = if value_str.len() > 60 {
            format!("{}...", &value_str[..60])
        } else {
            value_str.to_string()
        };
        println!("         {}: {}", name, value_preview);
    }
    if let Some(body) = built_request.body() {
        println!(
            "      Body: {} bytes",
            body.as_bytes().map(|b| b.len()).unwrap_or(0)
        );
        println!("      Body: {body:?}");
    } else {
        println!("      Body: None");
    }

    let response = client
        .execute(built_request)
        .await
        .map_err(|e| GarminError::InvalidMessage(format!("HTTP request failed: {}", e)))?;

    // Extract status
    let status = response.status().as_u16();
    println!("   📥 Received response: {}", status);
    if status >= 400 {
        println!("   ⚠️  HTTP error response: {}", status);
    }

    // Extract headers
    let mut headers = HashMap::new();
    for (key, value) in response.headers() {
        if let Ok(value_str) = value.to_str() {
            headers.insert(key.to_string(), value_str.to_string());
        }
    }

    // Extract body
    let body = response
        .bytes()
        .await
        .map_err(|e| GarminError::InvalidMessage(format!("Failed to read response body: {}", e)))?
        .to_vec();

    println!("   📦 Response body size: {} bytes", body.len());

    // Check if body is text or binary (e.g., gzip-compressed)
    if let Ok(body_text) = str::from_utf8(&body) {
        // Body is valid UTF-8 text
        if body.len() <= 500 {
            println!("   📦 Response body (text): {}", body_text);
        } else {
            println!(
                "   📦 Response body (text): {}... ({} bytes total)",
                &body_text[..500.min(body_text.len())],
                body.len()
            );
        }
    } else {
        // Body is binary (likely compressed)
        println!("   📦 Response body: <binary data, {} bytes>", body.len());
        if body.len() > 0 {
            let preview_len = 32.min(body.len());
            let hex_preview: String = body[..preview_len]
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join(" ");
            println!("   📦 First {} bytes (hex): {}", preview_len, hex_preview);
        }
    }

    // Create response - pass raw bytes, don't try to convert to JSON
    let mut http_response = HttpResponse::new(status);
    http_response.headers = headers;
    http_response.body = body;

    Ok(http_response)
}

// Helper functions for protobuf encoding/decoding

fn read_varint(data: &[u8]) -> Result<(usize, usize)> {
    let mut result = 0usize;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in data.iter().take(10) {
        bytes_read += 1;
        result |= ((byte & 0x7F) as usize) << shift;

        if byte & 0x80 == 0 {
            return Ok((result, bytes_read));
        }

        shift += 7;
    }

    Err(GarminError::InvalidMessage("Varint too long".into()))
}

/// Compress data using gzip
fn compress_gzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data)
        .map_err(|e| GarminError::InvalidMessage(format!("Gzip compression failed: {}", e)))?;
    encoder
        .finish()
        .map_err(|e| GarminError::InvalidMessage(format!("Gzip finish failed: {}", e)))
}

fn encode_varint(mut value: usize) -> Vec<u8> {
    let mut result = Vec::new();

    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        result.push(byte);

        if value == 0 {
            break;
        }
    }

    result
}

fn skip_field(wire_type: u8, data: &[u8], pos: &mut usize) -> Result<()> {
    match wire_type {
        0 => {
            // Varint
            let (_, bytes) = read_varint(data)?;
            *pos += bytes;
        }
        1 => {
            // 64-bit
            *pos += 8;
        }
        2 => {
            // Length-delimited
            let (len, len_bytes) = read_varint(data)?;
            *pos += len_bytes + len;
        }
        5 => {
            // 32-bit
            *pos += 4;
        }
        _ => {
            return Err(GarminError::InvalidMessage(format!(
                "Unknown wire type: {}",
                wire_type
            )));
        }
    }
    Ok(())
}

fn parse_header(data: &[u8]) -> Result<(String, String)> {
    let mut key = String::new();
    let mut value = String::new();
    let mut pos = 0;

    // Debug: print first 64 bytes of header data
    debug!("parse_header: {} bytes total", data.len());
    let preview_len = std::cmp::min(64, data.len());
    debug!(
        "  First {} bytes: {:02X?}",
        preview_len,
        &data[..preview_len]
    );

    // Check for Garmin's custom header encoding with AB CD AB CD marker
    if data.len() >= 4 && data[0] == 0xAB && data[1] == 0xCD && data[2] == 0xAB && data[3] == 0xCD {
        debug!("  Detected Garmin custom header encoding");
        pos = 4; // Skip marker

        // Skip 4 bytes of metadata
        if pos + 4 > data.len() {
            return Err(GarminError::InvalidMessage(
                "Header data truncated after marker".into(),
            ));
        }
        let metadata = &data[pos..pos + 4];
        debug!("  Metadata: {:02X?}", metadata);
        pos += 4;

        // Read key length as 2-byte big-endian
        if pos + 2 > data.len() {
            return Err(GarminError::InvalidMessage(
                "Header data truncated before key length".into(),
            ));
        }
        let key_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        debug!("  Key length: {} (0x{:04X})", key_len, key_len);
        pos += 2;

        // Read key (may include null terminator in the count)
        if pos + key_len > data.len() {
            return Err(GarminError::InvalidMessage(format!(
                "Header key data truncated: need {} bytes, have {} remaining",
                key_len,
                data.len() - pos
            )));
        }
        let key_data = &data[pos..pos + key_len];
        debug!("  Key data ({} bytes): {:02X?}", key_len, key_data);

        // Parse key, removing null terminators if present
        key = String::from_utf8_lossy(key_data)
            .trim_end_matches('\0')
            .to_string();
        debug!("  Parsed key: '{}'", key);
        pos += key_len;

        debug!(
            "  Position after key: {}, remaining bytes: {}",
            pos,
            data.len() - pos
        );
        if pos < data.len() {
            let preview_len = std::cmp::min(16, data.len() - pos);
            debug!(
                "  Next {} bytes: {:02X?}",
                preview_len,
                &data[pos..pos + preview_len]
            );
        }

        // First check for DA 7A DA 7A value marker (highest priority)
        // Pattern: [DA 7A DA 7A] immediately after key (key includes null terminator)
        if pos + 4 <= data.len()
            && data[pos] == 0xDA
            && data[pos + 1] == 0x7A
            && data[pos + 2] == 0xDA
            && data[pos + 3] == 0x7A
        {
            debug!("  Found DA 7A DA 7A value marker - using sensible default");

            // DA 7A marker indicates a complex nested structure
            // For HTTP headers, we can provide sensible defaults based on the key
            value = match key.to_lowercase().as_str() {
                "content-type" => "application/json".to_string(),
                "accept" => "application/json".to_string(),
                "authorization" => "".to_string(), // Will be skipped by validation
                _ => {
                    // Try to extract any ASCII text from the remaining data
                    let remaining = &data[pos + 4..];
                    let extracted: String = remaining
                        .iter()
                        .filter(|&&b| b >= 0x20 && b <= 0x7E) // printable ASCII
                        .map(|&b| b as char)
                        .collect();
                    if extracted.len() > 3 {
                        extracted
                    } else {
                        "".to_string()
                    }
                }
            };

            debug!("  Using value for DA 7A encoding: '{}'", value);
            return Ok((key, value));
        }

        // Check for separator bytes (00 00 or just alignment)
        if pos + 2 <= data.len() {
            debug!(
                "  Next 2 bytes after key: {:02X} {:02X}",
                data[pos],
                data[pos + 1]
            );

            if data[pos] == 0x00 && data[pos + 1] == 0x00 {
                debug!("  Skipping 00 00 separator");
                pos += 2;
            } else if data[pos] == 0x00 {
                debug!("  Skipping single 00 byte");
                pos += 1;
            }
        }

        // Read value length - try 2-byte first, fall back to 1-byte
        if pos + 2 > data.len() {
            return Err(GarminError::InvalidMessage(
                "Header data truncated before value length".into(),
            ));
        }

        // Try reading as 2-byte big-endian first
        let value_len_2byte = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        let value_len_1byte = data[pos] as usize;

        debug!(
            "  Potential value length: 2-byte={}, 1-byte={}",
            value_len_2byte, value_len_1byte
        );
        debug!("  Remaining data: {} bytes", data.len() - pos);

        // Heuristic: if 2-byte length fits in remaining data, use it; otherwise try 1-byte
        let (value_len, len_bytes) = if pos + 2 + value_len_2byte <= data.len() {
            debug!("  Using 2-byte value length: {}", value_len_2byte);
            (value_len_2byte, 2)
        } else if pos + 1 + value_len_1byte <= data.len() {
            debug!("  Using 1-byte value length: {}", value_len_1byte);
            (value_len_1byte, 1)
        } else {
            return Err(GarminError::InvalidMessage(format!(
                "Cannot determine valid value length at pos {}",
                pos
            )));
        };
        pos += len_bytes;

        // Read value
        if pos + value_len > data.len() {
            return Err(GarminError::InvalidMessage(format!(
                "Header value data truncated: need {} bytes, have {} remaining",
                value_len,
                data.len() - pos
            )));
        }
        let value_data = &data[pos..pos + value_len];
        debug!(
            "  Value data ({} bytes): first 32 bytes = {:02X?}",
            value_len,
            &value_data[..std::cmp::min(32, value_len)]
        );

        value = String::from_utf8_lossy(value_data)
            .trim_end_matches('\0')
            .to_string();
        debug!(
            "  Parsed value: '{}' (truncated to 64 chars)",
            &value[..std::cmp::min(64, value.len())]
        );

        return Ok((key, value));
    }

    // Standard protobuf encoding (fallback)
    debug!("  Using standard protobuf encoding");
    while pos < data.len() {
        let tag = data[pos];
        let field_number = tag >> 3;
        let wire_type = tag & 0x07;
        pos += 1;

        if wire_type != 2 {
            return Err(GarminError::InvalidMessage(
                "Invalid wire type in header".into(),
            ));
        }

        let (len, len_bytes) = read_varint(&data[pos..])?;
        pos += len_bytes;

        let str_data = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
        pos += len;

        match field_number {
            1 => key = str_data,
            2 => value = str_data,
            _ => {}
        }
    }

    Ok((key, value))
}

fn encode_header(key: &str, value: &str) -> Vec<u8> {
    let mut result = Vec::new();

    // Field 1: key (string)
    result.push((1 << 3) | 2);
    result.extend_from_slice(&encode_varint(key.len()));
    result.extend_from_slice(key.as_bytes());

    // Field 2: value (string)
    result.push((2 << 3) | 2);
    result.extend_from_slice(&encode_varint(value.len()));
    result.extend_from_slice(value.as_bytes());

    result
}

fn parse_url(url: &str) -> (String, HashMap<String, String>) {
    let mut query = HashMap::new();

    // Split URL into base and query string
    let parts: Vec<&str> = url.splitn(2, '?').collect();
    let base_url = parts[0];

    // Extract path from URL (handle full URLs like https://domain.com/path)
    let path = if base_url.contains("://") {
        // Full URL with scheme - extract path after domain
        if let Some(path_start) = base_url
            .find("://")
            .and_then(|i| base_url[i + 3..].find('/'))
        {
            base_url[base_url.find("://").unwrap() + 3 + path_start..].to_string()
        } else {
            // No path after domain, use root
            "/".to_string()
        }
    } else {
        // Already just a path
        base_url.to_string()
    };

    // Parse query parameters
    if parts.len() > 1 {
        for param in parts[1].split('&') {
            let kv: Vec<&str> = param.splitn(2, '=').collect();
            if kv.len() == 2 {
                query.insert(kv[0].to_string(), kv[1].to_string());
            }
        }
    }

    (path, query)
}

/// Convert OAuth JSON request to form-encoded format
/// OAuth token endpoints expect application/x-www-form-urlencoded, not JSON
fn convert_oauth_to_form_encoded(
    json_data: &[u8],
    headers: &mut std::collections::HashMap<String, String>,
) -> Result<Vec<u8>> {
    use std::str;

    // Trim any null terminators from the data (common with C/protobuf strings)
    let json_data_trimmed = if json_data.last() == Some(&0) {
        &json_data[..json_data.len() - 1]
    } else {
        json_data
    };

    let json_str = str::from_utf8(json_data_trimmed)
        .map_err(|e| GarminError::InvalidMessage(format!("Invalid UTF-8 in JSON: {}", e)))?;

    let json_value: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| GarminError::InvalidMessage(format!("Failed to parse JSON: {}", e)))?;

    // Check if this looks like an OAuth request (has grant_type field)
    if let Some(obj) = json_value.as_object() {
        if obj.contains_key("grant_type") {
            debug!("Detected OAuth request, converting to form-encoded format");
            println!("      🔐 OAuth request detected, converting to form-encoded");

            // Helper function for percent encoding
            fn percent_encode(s: &str) -> String {
                let mut result = String::new();
                for byte in s.bytes() {
                    match byte {
                        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                            result.push(byte as char);
                        }
                        _ => {
                            result.push_str(&format!("%{:02X}", byte));
                        }
                    }
                }
                result
            }

            // Build form-encoded string
            let mut form_parts = Vec::new();
            for (key, value) in obj.iter() {
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => continue,
                };

                // URL encode the key and value
                let encoded_key = percent_encode(&key);
                let encoded_value = percent_encode(&value_str);
                form_parts.push(format!("{}={}", encoded_key, encoded_value));
            }

            let form_body = form_parts.join("&");
            debug!("Form-encoded body: {}", form_body);
            println!("      Form body: {}", form_body);

            // Update Content-Type header
            headers.insert(
                "Content-Type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            );

            return Ok(form_body.into_bytes());
        }
    }

    // Not an OAuth request, return JSON as-is
    Ok(json_data.to_vec())
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encoding() {
        assert_eq!(encode_varint(0), vec![0]);
        assert_eq!(encode_varint(1), vec![1]);
        assert_eq!(encode_varint(127), vec![127]);
        assert_eq!(encode_varint(128), vec![0x80, 0x01]);
        assert_eq!(encode_varint(300), vec![0xAC, 0x02]);
    }

    #[test]
    fn test_http_error_response_encoding() {
        // Integration test: simulate encoding a 401 error response
        let response = HttpResponse::new(401).with_body(b"401: Unauthorized".to_vec());

        // Create a minimal request for testing
        let request = HttpRequest {
            url: "http://example.com".to_string(),
            path: "/test".to_string(),
            query: HashMap::new(),
            method: HttpMethod::Get,
            headers: HashMap::new(),
            body: Vec::new(),
            use_data_xfer: false,
            request_field: 2,
            compress_response_body: false,
            response_type: None,
            max_response_length: None,
            version: None,
        };

        // This should not panic even though body is plain text, not JSON
        let result = response.encode_connectiq_response(1, &request);

        // Should succeed by wrapping plain text as a JSON string
        assert!(result.is_ok(), "Should handle plain text error responses");

        let encoded = result.unwrap();
        assert!(!encoded.is_empty(), "Encoded response should not be empty");
    }

    #[test]
    fn test_garmin_body_decoding() {
        // Test decoding with new garmin_json module
        // This test data format may not match the actual Garmin format anymore
        // Consider using real payloads from garmin_json tests instead

        // Skip this test as the old format is no longer used
        // Real tests are in garmin_json module
    }

    #[test]
    fn test_body_without_outer_magic_bytes() {
        // Test data that has string section and data section but no outer magic bytes
        // This is the format seen in real payloads like:
        // 00 05 64 61 74 61 00 00 05 74 79 70 65 00 ... DA 7A DA 7A ...
        use crate::garmin_json;

        // Create a simple Garmin JSON structure
        let json = serde_json::json!({
            "type": "test",
            "data": "value"
        });

        // Encode it properly first
        let encoded = garmin_json::encode(&json).expect("Failed to encode");

        // Strip the outer magic bytes and length to simulate the problematic format
        // Format: AB CD AB CD [len] [string section] DA 7A DA 7A [len] [data]
        // We want: [string section] DA 7A DA 7A [len] [data]

        // Find where string section starts (after first 8 bytes)
        if encoded.len() > 8 && &encoded[0..4] == &[0xAB, 0xCD, 0xAB, 0xCD] {
            // Extract: [string section] + [data section magic and data]
            let without_outer_magic = &encoded[8..];

            // Now simulate the wrapping logic
            if let Some(data_section_pos) = without_outer_magic
                .windows(4)
                .position(|w| w[0] == 0xDA && w[1] == 0x7A && w[2] == 0xDA && w[3] == 0x7A)
            {
                let string_section = &without_outer_magic[..data_section_pos];
                let data_section = &without_outer_magic[data_section_pos..];

                let mut wrapped = Vec::new();
                wrapped.extend_from_slice(&[0xAB, 0xCD, 0xAB, 0xCD]);
                wrapped.extend_from_slice(&(string_section.len() as u32).to_be_bytes());
                wrapped.extend_from_slice(string_section);
                wrapped.extend_from_slice(data_section);

                // Should be able to decode the wrapped version
                let decoded = garmin_json::decode(&wrapped).expect("Failed to decode wrapped");
                assert_eq!(decoded, json);
            }
        }
    }

    #[test]
    fn test_varint_decoding() {
        assert_eq!(read_varint(&[0]).unwrap(), (0, 1));
        assert_eq!(read_varint(&[1]).unwrap(), (1, 1));
        assert_eq!(read_varint(&[127]).unwrap(), (127, 1));
        assert_eq!(read_varint(&[0x80, 0x01]).unwrap(), (128, 2));
        assert_eq!(read_varint(&[0xAC, 0x02]).unwrap(), (300, 2));
    }

    #[test]
    fn test_parse_url() {
        // Test full URL with scheme and domain
        let (path, query) = parse_url("https://example.com/api/test?key=value&foo=bar");
        assert_eq!(path, "/api/test");
        assert_eq!(query.get("key"), Some(&"value".to_string()));
        assert_eq!(query.get("foo"), Some(&"bar".to_string()));

        // Test full URL without query
        let (path, query) = parse_url("https://geolocation.garmin.com/geolocation/whereami");
        assert_eq!(path, "/geolocation/whereami");
        assert_eq!(query.len(), 0);

        // Test path-only URL
        let (path, query) = parse_url("/weather/forecast?lat=37.7&lon=-122.4");
        assert_eq!(path, "/weather/forecast");
        assert_eq!(query.get("lat"), Some(&"37.7".to_string()));
        assert_eq!(query.get("lon"), Some(&"-122.4".to_string()));

        // Test URL with domain but no path
        let (path, query) = parse_url("https://example.com");
        assert_eq!(path, "/");
        assert_eq!(query.len(), 0);
    }

    #[test]
    fn test_http_method_conversion() {
        assert_eq!(HttpMethod::from_u8(1), Some(HttpMethod::Get));
        assert_eq!(HttpMethod::from_u8(3), Some(HttpMethod::Post));
        assert_eq!(HttpMethod::Get.as_str(), "GET");
        assert_eq!(HttpMethod::Post.as_str(), "POST");
    }

    #[test]
    fn test_http_response_builder() {
        let response = HttpResponse::ok()
            .with_json_body(r#"{"test":"value"}"#)
            .with_header("X-Custom".to_string(), "header".to_string());

        assert_eq!(response.status, 200);
        assert_eq!(
            response.headers.get("Content-Type"),
            Some(&"application/json".to_string())
        );
        assert_eq!(
            response.headers.get("X-Custom"),
            Some(&"header".to_string())
        );
        assert_eq!(response.body, r#"{"test":"value"}"#.as_bytes());
    }
}
