//! Garmin JSON (Monkey-C binary format) encoding and decoding
//!
//! This implementation provides clean encoding/decoding between JSON and Garmin's
//! Monkey-C binary format used by ConnectIQ apps.

use crate::types::{GarminError, Result};
use log::{debug, error, info};
use serde_json::{Map, Number, Value};
use std::collections::{HashMap, HashSet, VecDeque};

const STRING_SECTION_MAGIC: &[u8] = &[0xAB, 0xCD, 0xAB, 0xCD];
const DATA_SECTION_MAGIC: &[u8] = &[0xDA, 0x7A, 0xDA, 0x7A];

const TYPE_NULL: u8 = 0x00;
const TYPE_SINT32: u8 = 0x01;
const TYPE_FLOAT: u8 = 0x02;
const TYPE_STRING: u8 = 0x03;
const TYPE_ARRAY: u8 = 0x05;
const TYPE_BOOL: u8 = 0x09;
const TYPE_MAP: u8 = 0x0B;
const TYPE_SINT64: u8 = 0x0E;
const TYPE_DOUBLE: u8 = 0x0F;

/// Encode a JSON value to Garmin Monkey-C binary format
pub fn encode(value: &Value) -> Result<Vec<u8>> {
    // Collect all strings breadth-first for consistent ordering
    let strings = collect_strings(value);

    // Build string section
    let mut string_section = Vec::new();
    let mut string_offsets = HashMap::new();
    let mut current_offset = 0usize;

    for s in &strings {
        string_offsets.insert(s.clone(), current_offset);
        let bytes = s.as_bytes();
        let length = (bytes.len() + 1) as u16; // +1 for null terminator

        string_section.push((length >> 8) as u8);
        string_section.push((length & 0xFF) as u8);
        string_section.extend_from_slice(bytes);
        string_section.push(0x00);

        current_offset += 2 + bytes.len() + 1;
    }

    // Build data section using breadth-first encoding for maps
    let mut data_section = Vec::new();
    encode_value(value, &mut data_section, &string_offsets)?;

    // Assemble output
    let mut output = Vec::new();

    if !string_section.is_empty() {
        output.extend_from_slice(STRING_SECTION_MAGIC);
        output.extend_from_slice(&(string_section.len() as u32).to_be_bytes());
        output.extend_from_slice(&string_section);
    }

    output.extend_from_slice(DATA_SECTION_MAGIC);
    output.extend_from_slice(&(data_section.len() as u32).to_be_bytes());
    output.extend_from_slice(&data_section);

    Ok(output)
}

/// Encode a value using breadth-first queue-based approach matching Java implementation
fn encode_value(
    value: &Value,
    output: &mut Vec<u8>,
    string_offsets: &HashMap<String, usize>,
) -> Result<()> {
    enum QueueItem<'a> {
        Value(&'a Value),
        String(&'a str),
    }

    let mut queue = VecDeque::new();
    queue.push_back(QueueItem::Value(value));

    while let Some(item) = queue.pop_front() {
        match item {
            QueueItem::String(s) => {
                output.push(TYPE_STRING);
                let offset = *string_offsets.get(s).ok_or_else(|| {
                    GarminError::InvalidMessage(format!("String not found: {}", s))
                })?;
                output.extend_from_slice(&(offset as u32).to_be_bytes());
            }
            QueueItem::Value(val) => match val {
                Value::Null => {
                    output.push(TYPE_NULL);
                }
                Value::Bool(b) => {
                    output.push(TYPE_BOOL);
                    output.push(if *b { 0x01 } else { 0x00 });
                }
                Value::Number(n) => {
                    encode_number(n, output)?;
                }
                Value::String(s) => {
                    output.push(TYPE_STRING);
                    let offset = *string_offsets.get(s).ok_or_else(|| {
                        GarminError::InvalidMessage(format!("String not found: {}", s))
                    })?;
                    output.extend_from_slice(&(offset as u32).to_be_bytes());
                }
                Value::Array(arr) => {
                    output.push(TYPE_ARRAY);
                    output.extend_from_slice(&(arr.len() as u32).to_be_bytes());
                    // Add all elements to queue for breadth-first processing
                    for element in arr {
                        queue.push_back(QueueItem::Value(element));
                    }
                }
                Value::Object(obj) => {
                    output.push(TYPE_MAP);
                    output.extend_from_slice(&(obj.len() as u32).to_be_bytes());
                    // Add key-value pairs sequentially to queue for breadth-first processing
                    for (key, value) in obj {
                        queue.push_back(QueueItem::String(key.as_str()));
                        queue.push_back(QueueItem::Value(value));
                    }
                }
            },
        }
    }
    Ok(())
}

/// Decode Garmin Monkey-C binary format to JSON
pub fn decode(bytes: &[u8]) -> Result<Value> {
    let mut pos = 0;

    if bytes.len() < 8 {
        return Err(GarminError::InvalidMessage("Payload too short".into()));
    }

    let mut magic = [0u8; 4];
    magic.copy_from_slice(&bytes[pos..pos + 4]);
    pos += 4;

    let mut strings: HashMap<usize, String> = HashMap::new();

    // Parse string section if present
    if magic == STRING_SECTION_MAGIC {
        let string_len =
            u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        pos += 4;

        let start = pos;
        let end = pos + string_len;

        while pos < end {
            let str_start = pos - start;
            let len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
            pos += 2;

            if len == 0 {
                return Err(GarminError::InvalidMessage("Invalid string length".into()));
            }

            let str_bytes = &bytes[pos..pos + len - 1];
            pos += len;

            let s = String::from_utf8(str_bytes.to_vec())
                .map_err(|e| GarminError::InvalidMessage(format!("Invalid UTF-8: {}", e)))?;

            info!("String at offset {}: {:?}", str_start, s);
            strings.insert(str_start, s);
        }

        magic.copy_from_slice(&bytes[pos..pos + 4]);
        pos += 4;
    }

    if magic != DATA_SECTION_MAGIC {
        return Err(GarminError::InvalidMessage(format!(
            "Invalid magic: {:02X} {:02X} {:02X} {:02X}",
            magic[0], magic[1], magic[2], magic[3]
        )));
    }

    let data_len =
        u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]) as usize;
    pos += 4;

    if pos + data_len != bytes.len() {
        return Err(GarminError::InvalidMessage(format!(
            "Length mismatch: expected {}, got {}",
            data_len,
            bytes.len() - pos
        )));
    }

    // Decode the data section
    let mut offset = 1;
    let mut top_level = Map::new();
    let data = &bytes[pos..];

    let size = u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]) as usize;
    offset += 4;

    for i in 0..size {
        debug!("  Reading key-value pair #{}", i);
        debug!("    Position before key: {}", offset);
        let key_val = decode_value(data, &mut offset, &strings)?;
        debug!("    Key decoded: {:?}", key_val);
        let key = match key_val {
            Value::String(s) => s,
            _ => return Err(GarminError::InvalidMessage("Map key must be string".into())),
        };
        debug!("    Position before value: {}", offset);
        let val = decode_value(data, &mut offset, &strings)?;
        debug!("    Position after value: {}", offset);

        info!(
            "    Value decoded (truncated): {}",
            serde_json::to_string(&val)
                .unwrap_or_default()
                .chars()
                .take(100)
                .collect::<String>()
        );

        top_level.insert(key, val);
    }

    consolidate(&mut top_level, data, &mut offset, &strings)?;

    for level in top_level.values_mut() {
        match level {
            Value::Object(ref mut obj) => {
                consolidate(obj, data, &mut offset, &strings)?;
            }
            _ => {}
        }
    }

    Ok(Value::Object(top_level))
}

fn consolidate(
    top_level: &mut Map<String, Value>,
    data: &[u8],
    offset: &mut usize,
    strings: &HashMap<usize, String>,
) -> Result<()> {
    for obj in top_level.values_mut() {
        println!("obj: {obj}");
        match obj {
            Value::Object(obj) => {
                if !obj.contains_key("fake0") {
                    continue;
                }

                let size = obj.len();
                obj.clear();

                println!("Found Objet: {size}");

                for i in 0..size {
                    debug!("  Reading key-value pair #{}", i);
                    debug!("    Position before key: {}", offset);
                    let key_val = decode_value(data, offset, strings)?;
                    debug!("    Key decoded: {:?}", key_val);
                    let key = match key_val {
                        Value::String(s) => s,
                        _ => {
                            return Err(GarminError::InvalidMessage(
                                "Map key must be string".into(),
                            ))
                        }
                    };
                    debug!("    Position before value: {}", offset);
                    let val = decode_value(data, offset, strings)?;
                    debug!("    Position after value: {}", offset);

                    info!(
                        "    Value decoded (truncated): {}",
                        serde_json::to_string(&val)
                            .unwrap_or_default()
                            .chars()
                            .take(100)
                            .collect::<String>()
                    );

                    obj.insert(key, val);
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Decode a value recursively - matches the encoder structure
fn decode_value(data: &[u8], pos: &mut usize, strings: &HashMap<usize, String>) -> Result<Value> {
    if *pos >= data.len() {
        return Err(GarminError::InvalidMessage("Unexpected end of data".into()));
    }

    let type_byte = data[*pos];
    *pos += 1;

    println!("type_byte: {type_byte:?} / {pos}");

    match type_byte {
        TYPE_NULL => Ok(Value::Null),
        TYPE_BOOL => {
            if *pos >= data.len() {
                return Err(GarminError::InvalidMessage(
                    "Unexpected end reading bool".into(),
                ));
            }
            let b = data[*pos] != 0x00;
            *pos += 1;
            Ok(Value::Bool(b))
        }
        TYPE_SINT32 => {
            if *pos + 4 > data.len() {
                return Err(GarminError::InvalidMessage(
                    "Unexpected end reading sint32".into(),
                ));
            }
            let i =
                i32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            Ok(Value::Number(Number::from(i)))
        }
        TYPE_SINT64 => {
            if *pos + 8 > data.len() {
                return Err(GarminError::InvalidMessage(
                    "Unexpected end reading sint64".into(),
                ));
            }
            let i = i64::from_be_bytes([
                data[*pos],
                data[*pos + 1],
                data[*pos + 2],
                data[*pos + 3],
                data[*pos + 4],
                data[*pos + 5],
                data[*pos + 6],
                data[*pos + 7],
            ]);
            *pos += 8;
            Ok(Value::Number(Number::from(i)))
        }
        TYPE_FLOAT => {
            if *pos + 4 > data.len() {
                return Err(GarminError::InvalidMessage(
                    "Unexpected end reading float".into(),
                ));
            }
            let f =
                f32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            let num = Number::from_f64(f as f64)
                .ok_or_else(|| GarminError::InvalidMessage(format!("Invalid float: {}", f)))?;
            Ok(Value::Number(num))
        }
        TYPE_DOUBLE => {
            if *pos + 8 > data.len() {
                return Err(GarminError::InvalidMessage(
                    "Unexpected end reading double".into(),
                ));
            }
            let f = f64::from_be_bytes([
                data[*pos],
                data[*pos + 1],
                data[*pos + 2],
                data[*pos + 3],
                data[*pos + 4],
                data[*pos + 5],
                data[*pos + 6],
                data[*pos + 7],
            ]);
            *pos += 8;
            let num = Number::from_f64(f)
                .ok_or_else(|| GarminError::InvalidMessage(format!("Invalid double: {}", f)))?;
            Ok(Value::Number(num))
        }
        TYPE_STRING => {
            if *pos + 4 > data.len() {
                return Err(GarminError::InvalidMessage(
                    "Unexpected end reading string offset".into(),
                ));
            }
            let offset =
                u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]])
                    as usize;
            *pos += 4;
            let s = strings.get(&offset).ok_or_else(|| {
                error!(
                    "ERROR: String not found at offset {}. Available offsets: {:?}",
                    offset,
                    strings.keys().collect::<Vec<_>>()
                );
                GarminError::InvalidMessage(format!("String not found at offset: {}", offset))
            })?;
            debug!("String {} from offset {}: {:?}", pos, offset, s);
            Ok(Value::String(s.clone()))
        }
        TYPE_ARRAY => {
            if *pos + 4 > data.len() {
                return Err(GarminError::InvalidMessage(
                    "Unexpected end reading array length".into(),
                ));
            }
            let length =
                u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]])
                    as usize;
            *pos += 4;

            let mut arr = Vec::with_capacity(length);
            for _ in 0..length {
                arr.push(decode_value(data, pos, strings)?);
            }
            Ok(Value::Array(arr))
        }
        TYPE_MAP => {
            if *pos + 4 > data.len() {
                return Err(GarminError::InvalidMessage(
                    "Unexpected end reading map size".into(),
                ));
            }
            let size =
                u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]])
                    as usize;
            *pos += 4;

            debug!("Decoding MAP with size {}", size);

            let mut placeholder_obj = Map::with_capacity(size);
            for i in 0..size {
                placeholder_obj.insert(format!("fake{i}").to_string(), Value::Null);
            }
            debug!("  Placeholder MAP decoding complete: {size} entries");
            Ok(Value::Object(placeholder_obj))
        }
        _ => Err(GarminError::InvalidMessage(format!(
            "Unknown type: 0x{:02X}",
            type_byte
        ))),
    }
}

/// Collect all strings from a JSON value (breadth-first for consistency)
fn collect_strings(value: &Value) -> Vec<String> {
    let mut strings = Vec::new();
    let mut seen = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(value);

    while let Some(val) = queue.pop_front() {
        match val {
            Value::Object(obj) => {
                for (key, value) in obj {
                    if seen.insert(key.clone()) {
                        strings.push(key.clone());
                    }
                    if let Value::String(s) = value {
                        if seen.insert(s.clone()) {
                            strings.push(s.clone());
                        }
                    } else if value.is_object() || value.is_array() {
                        queue.push_back(value);
                    }
                }
            }
            Value::Array(arr) => {
                for element in arr {
                    if let Value::String(s) = element {
                        if seen.insert(s.clone()) {
                            strings.push(s.clone());
                        }
                    } else if element.is_object() || element.is_array() {
                        queue.push_back(element);
                    }
                }
            }
            Value::String(s) => {
                if seen.insert(s.clone()) {
                    strings.push(s.clone());
                }
            }
            _ => {}
        }
    }

    strings
}

/// Encode a number based on its type and value
fn encode_number(n: &Number, output: &mut Vec<u8>) -> Result<()> {
    if let Some(i) = n.as_i64() {
        if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
            output.push(TYPE_SINT32);
            output.extend_from_slice(&(i as i32).to_be_bytes());
        } else {
            output.push(TYPE_SINT64);
            output.extend_from_slice(&i.to_be_bytes());
        }
    } else if let Some(f) = n.as_f64() {
        // Check if it fits in a float
        if f.abs() <= f32::MAX as f64 && f == f as f32 as f64 {
            output.push(TYPE_FLOAT);
            output.extend_from_slice(&(f as f32).to_be_bytes());
        } else {
            output.push(TYPE_DOUBLE);
            output.extend_from_slice(&f.to_be_bytes());
        }
    } else {
        return Err(GarminError::InvalidMessage(format!(
            "Unsupported number: {}",
            n
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_encode_decode_simple_object() {
        let obj = json!({"key": "value"});
        let encoded = encode(&obj).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(obj, decoded);
    }

    #[test]
    fn test_real_decoder_one() {
        let hex = [
            0xAB, 0xCD, 0xAB, 0xCD, 0x00, 0x00, 0x00, 0x71, 0x00, 0x05, 0x64, 0x61, 0x74, 0x61,
            0x00, 0x00, 0x05, 0x74, 0x79, 0x70, 0x65, 0x00, 0x00, 0x10, 0x72, 0x65, 0x6E, 0x64,
            0x65, 0x72, 0x5F, 0x74, 0x65, 0x6D, 0x70, 0x6C, 0x61, 0x74, 0x65, 0x00, 0x00, 0x0F,
            0x67, 0x6C, 0x61, 0x6E, 0x63, 0x65, 0x54, 0x65, 0x6D, 0x70, 0x6C, 0x61, 0x74, 0x65,
            0x00, 0x00, 0x09, 0x74, 0x65, 0x6D, 0x70, 0x6C, 0x61, 0x74, 0x65, 0x00, 0x00, 0x33,
            0x7B, 0x7B, 0x20, 0x73, 0x74, 0x61, 0x74, 0x65, 0x73, 0x28, 0x27, 0x73, 0x65, 0x6E,
            0x73, 0x6F, 0x72, 0x2E, 0x73, 0x6F, 0x6C, 0x61, 0x72, 0x6E, 0x65, 0x74, 0x5F, 0x70,
            0x6F, 0x77, 0x65, 0x72, 0x5F, 0x67, 0x72, 0x69, 0x64, 0x5F, 0x69, 0x6D, 0x70, 0x6F,
            0x72, 0x74, 0x27, 0x29, 0x20, 0x7D, 0x7D, 0x57, 0x00, 0xDA, 0x7A, 0xDA, 0x7A, 0x00,
            0x00, 0x00, 0x2D, 0x0B, 0x00, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x0B,
            0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x07, 0x03, 0x00, 0x00, 0x00, 0x0E,
            0x03, 0x00, 0x00, 0x00, 0x20, 0x0B, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00,
            0x31, 0x03, 0x00, 0x00, 0x00, 0x3C,
        ];

        let template = json!({
            "data": {
                "glanceTemplate": {
                    "template": "{{ states('sensor.solarnet_power_grid_import') }}W"
                }
            },
            "type": "render_template"
        });

        let result = decode(&hex);
        if let Err(e) = &result {
            error!("Decode error: {:?}", e);
        }
        let decoded = result.unwrap();
        error!(
            "Decoded: {}",
            serde_json::to_string_pretty(&decoded).unwrap()
        );
        assert_eq!(template, decoded);
    }

    #[test]
    fn test_real_decoder_two() {
        let hex = [
            0xAB, 0xCD, 0xAB, 0xCD, 0x00, 0x00, 0x00, 0x71, 0x00, 0x05, 0x64, 0x61, 0x74, 0x61,
            0x00, 0x00, 0x05, 0x74, 0x79, 0x70, 0x65, 0x00, 0x00, 0x10, 0x72, 0x65, 0x6E, 0x64,
            0x65, 0x72, 0x5F, 0x74, 0x65, 0x6D, 0x70, 0x6C, 0x61, 0x74, 0x65, 0x00, 0x00, 0x0F,
            0x67, 0x6C, 0x61, 0x6E, 0x63, 0x65, 0x54, 0x65, 0x6D, 0x70, 0x6C, 0x61, 0x74, 0x65,
            0x00, 0x00, 0x09, 0x74, 0x65, 0x6D, 0x70, 0x6C, 0x61, 0x74, 0x65, 0x00, 0x00, 0x33,
            0x7B, 0x7B, 0x20, 0x73, 0x74, 0x61, 0x74, 0x65, 0x73, 0x28, 0x27, 0x73, 0x65, 0x6E,
            0x73, 0x6F, 0x72, 0x2E, 0x73, 0x6F, 0x6C, 0x61, 0x72, 0x6E, 0x65, 0x74, 0x5F, 0x70,
            0x6F, 0x77, 0x65, 0x72, 0x5F, 0x67, 0x72, 0x69, 0x64, 0x5F, 0x69, 0x6D, 0x70, 0x6F,
            0x72, 0x74, 0x27, 0x29, 0x20, 0x7D, 0x7D, 0x57, 0x00, 0xDA, 0x7A, 0xDA, 0x7A, 0x00,
            0x00, 0x00, 0x2D, 0x0B, 0x00, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x0B,
            0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x07, 0x03, 0x00, 0x00, 0x00, 0x0E,
            0x03, 0x00, 0x00, 0x00, 0x20, 0x0B, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00,
            0x31, 0x03, 0x00, 0x00, 0x00, 0x3C,
        ];

        let template = json!({
            "data": {
                "glanceTemplate": {
                    "template": "{{ states('sensor.solarnet_power_grid_import') }}W"
                }
            },
            "type": "render_template"
        });

        let result = decode(&hex);
        if let Err(e) = &result {
            error!("Decode error: {:?}", e);
        }
        let decoded = result.unwrap();
        error!(
            "Decoded: {}",
            serde_json::to_string_pretty(&decoded).unwrap()
        );
        assert_eq!(template, decoded);
    }

    #[test]
    fn test_real_decoder_three() {
        let hex = [
            0xAB, 0xCD, 0xAB, 0xCD, 0x00, 0x00, 0x01, 0x96, 0x00, 0x05, 0x64, 0x61, 0x74, 0x61,
            0x00, 0x00, 0x05, 0x74, 0x79, 0x70, 0x65, 0x00, 0x00, 0x10, 0x72, 0x65, 0x6E, 0x64,
            0x65, 0x72, 0x5F, 0x74, 0x65, 0x6D, 0x70, 0x6C, 0x61, 0x74, 0x65, 0x00, 0x00, 0x03,
            0x36, 0x74, 0x00, 0x00, 0x03, 0x32, 0x74, 0x00, 0x00, 0x03, 0x30, 0x74, 0x00, 0x00,
            0x03, 0x35, 0x74, 0x00, 0x00, 0x03, 0x33, 0x74, 0x00, 0x00, 0x02, 0x34, 0x00, 0x00,
            0x03, 0x31, 0x74, 0x00, 0x00, 0x09, 0x74, 0x65, 0x6D, 0x70, 0x6C, 0x61, 0x74, 0x65,
            0x00, 0x00, 0x2D, 0x7B, 0x7B, 0x73, 0x74, 0x61, 0x74, 0x65, 0x73, 0x28, 0x27, 0x73,
            0x77, 0x69, 0x74, 0x63, 0x68, 0x2E, 0x75, 0x6E, 0x64, 0x65, 0x72, 0x5F, 0x68, 0x6F,
            0x75, 0x73, 0x65, 0x5F, 0x72, 0x6F, 0x6C, 0x6C, 0x65, 0x72, 0x5F, 0x64, 0x6F, 0x6F,
            0x72, 0x27, 0x29, 0x7D, 0x7D, 0x00, 0x00, 0x30, 0x7B, 0x7B, 0x73, 0x74, 0x61, 0x74,
            0x65, 0x73, 0x28, 0x27, 0x73, 0x77, 0x69, 0x74, 0x63, 0x68, 0x2E, 0x6C, 0x69, 0x76,
            0x69, 0x6E, 0x67, 0x5F, 0x72, 0x6F, 0x6F, 0x6D, 0x5F, 0x6C, 0x69, 0x67, 0x68, 0x74,
            0x5F, 0x73, 0x77, 0x69, 0x74, 0x63, 0x68, 0x5F, 0x32, 0x27, 0x29, 0x7D, 0x7D, 0x00,
            0x00, 0x28, 0x7B, 0x7B, 0x73, 0x74, 0x61, 0x74, 0x65, 0x73, 0x28, 0x27, 0x73, 0x77,
            0x69, 0x74, 0x63, 0x68, 0x2E, 0x78, 0x62, 0x6F, 0x78, 0x5F, 0x70, 0x6F, 0x77, 0x65,
            0x72, 0x5F, 0x63, 0x6F, 0x6E, 0x74, 0x72, 0x6F, 0x6C, 0x27, 0x29, 0x7D, 0x7D, 0x00,
            0x00, 0x21, 0x7B, 0x7B, 0x73, 0x74, 0x61, 0x74, 0x65, 0x73, 0x28, 0x27, 0x73, 0x77,
            0x69, 0x74, 0x63, 0x68, 0x2E, 0x67, 0x61, 0x72, 0x61, 0x67, 0x65, 0x5F, 0x64, 0x6F,
            0x6F, 0x72, 0x27, 0x29, 0x7D, 0x7D, 0x00, 0x00, 0x31, 0x7B, 0x7B, 0x73, 0x74, 0x61,
            0x74, 0x65, 0x73, 0x28, 0x27, 0x6C, 0x69, 0x67, 0x68, 0x74, 0x2E, 0x6B, 0x69, 0x74,
            0x63, 0x68, 0x65, 0x6E, 0x5F, 0x75, 0x6E, 0x64, 0x65, 0x72, 0x5F, 0x63, 0x61, 0x62,
            0x6E, 0x69, 0x65, 0x74, 0x5F, 0x6C, 0x69, 0x67, 0x68, 0x74, 0x73, 0x27, 0x29, 0x7D,
            0x7D, 0x00, 0x00, 0x3E, 0x7B, 0x7B, 0x20, 0x73, 0x74, 0x61, 0x74, 0x65, 0x73, 0x28,
            0x27, 0x62, 0x69, 0x6E, 0x61, 0x72, 0x79, 0x5F, 0x73, 0x65, 0x6E, 0x73, 0x6F, 0x72,
            0x2E, 0x67, 0x61, 0x74, 0x65, 0x5F, 0x73, 0x65, 0x6E, 0x73, 0x6F, 0x72, 0x5F, 0x77,
            0x69, 0x6E, 0x64, 0x6F, 0x77, 0x5F, 0x64, 0x6F, 0x6F, 0x72, 0x5F, 0x69, 0x73, 0x5F,
            0x6F, 0x70, 0x65, 0x6E, 0x27, 0x29, 0x20, 0x7D, 0x7D, 0x00, 0x00, 0x26, 0x7B, 0x7B,
            0x73, 0x74, 0x61, 0x74, 0x65, 0x73, 0x28, 0x27, 0x6C, 0x69, 0x67, 0x68, 0x74, 0x2E,
            0x6C, 0x69, 0x76, 0x69, 0x6E, 0x67, 0x5F, 0x72, 0x6F, 0x6F, 0x6D, 0x5F, 0x6C, 0x69,
            0x67, 0x68, 0x74, 0x27, 0x29, 0x7D, 0x7D, 0x00, 0xDA, 0x7A, 0xDA, 0x7A, 0x00, 0x00,
            0x00, 0xA5, 0x0B, 0x00, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x0B, 0x00,
            0x00, 0x00, 0x07, 0x03, 0x00, 0x00, 0x00, 0x07, 0x03, 0x00, 0x00, 0x00, 0x0E, 0x03,
            0x00, 0x00, 0x00, 0x20, 0x0B, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x25,
            0x0B, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x2A, 0x0B, 0x00, 0x00, 0x00,
            0x01, 0x03, 0x00, 0x00, 0x00, 0x2F, 0x0B, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00,
            0x00, 0x34, 0x0B, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x39, 0x0B, 0x00,
            0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x3D, 0x0B, 0x00, 0x00, 0x00, 0x01, 0x03,
            0x00, 0x00, 0x00, 0x42, 0x03, 0x00, 0x00, 0x00, 0x4D, 0x03, 0x00, 0x00, 0x00, 0x42,
            0x03, 0x00, 0x00, 0x00, 0x7C, 0x03, 0x00, 0x00, 0x00, 0x42, 0x03, 0x00, 0x00, 0x00,
            0xAE, 0x03, 0x00, 0x00, 0x00, 0x42, 0x03, 0x00, 0x00, 0x00, 0xD8, 0x03, 0x00, 0x00,
            0x00, 0x42, 0x03, 0x00, 0x00, 0x00, 0xFB, 0x03, 0x00, 0x00, 0x00, 0x42, 0x03, 0x00,
            0x00, 0x01, 0x2E, 0x03, 0x00, 0x00, 0x00, 0x42, 0x03, 0x00, 0x00, 0x01, 0x6E,
        ];

        let template = json!({
          "data": {
            "6t": {
              "template": "{{states('switch.under_house_roller_door')}}"
            },
            "2t": {
              "template": "{{states('switch.living_room_light_switch_2')}}"
            },
            "0t": {
              "template": "{{states('switch.xbox_power_control')}}"
            },
            "5t": {
              "template": "{{states('switch.garage_door')}}"
            },
            "3t": {
              "template": "{{states('light.kitchen_under_cabniet_lights')}}"
            },
            "4": {
              "template": "{{ states('binary_sensor.gate_sensor_window_door_is_open') }}"
            },
            "1t": {
              "template": "{{states('light.living_room_light')}}"
            }
          },
          "type": "render_template"
        });

        let result = decode(&hex);
        if let Err(e) = &result {
            error!("Decode error: {:?}", e);
        }
        let decoded = result.unwrap();
        error!(
            "Decoded: {}",
            serde_json::to_string_pretty(&decoded).unwrap()
        );
        assert_eq!(template, decoded);
    }

    #[test]
    fn test_real_render_template() {
        // Real structure from ConnectIQ HTTP requests
        let template =
            json!({"0t":"off","1t":"off","2t":"on","3t":"off","4":"off","5t":"off","6t":"on"});

        let encoded = encode(&template).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(template, decoded);

        // // Verify structure
        // let obj = decoded.as_object().unwrap();
        // assert!(obj.contains_key("data"));
        // assert!(obj.contains_key("type"));
        // assert_eq!(
        //     obj.get("type").unwrap().as_str().unwrap(),
        //     "render_template"
        // );
    }
}
