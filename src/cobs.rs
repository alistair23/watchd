//! COBS (Consistent Overhead Byte Stuffing) codec implementation
//!
//! This module implements the COBS encoding and decoding used by Garmin devices.
//! The Garmin variant includes a leading and trailing zero byte (the leading byte
//! is not part of standard COBS implementations).

use crate::types::Result;
use std::time::{Duration, Instant};

/// Default buffer timeout in milliseconds
const BUFFER_TIMEOUT_MS: u64 = 1500;

/// COBS encoder/decoder with buffering support for streaming data
pub struct CobsCoDec {
    buffer: Vec<u8>,
    last_update: Option<Instant>,
    decoded_message: Option<Vec<u8>>,
    buffer_timeout: Duration,
}

impl Default for CobsCoDec {
    fn default() -> Self {
        Self::new()
    }
}

impl CobsCoDec {
    /// Create a new COBS codec with default timeout
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(10_000),
            last_update: None,
            decoded_message: None,
            buffer_timeout: Duration::from_millis(BUFFER_TIMEOUT_MS),
        }
    }

    /// Create a new COBS codec with custom timeout
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            buffer: Vec::with_capacity(10_000),
            last_update: None,
            decoded_message: None,
            buffer_timeout: timeout,
        }
    }

    /// Accumulate received bytes in the internal buffer and attempt to decode
    ///
    /// The buffer is automatically cleared after a timeout period of inactivity.
    pub fn receive_bytes(&mut self, bytes: &[u8]) {
        let now = Instant::now();

        // Clear buffer if timeout expired
        if let Some(last_update) = self.last_update {
            if now.duration_since(last_update) > self.buffer_timeout {
                self.reset();
            }
        }

        self.last_update = Some(now);
        self.buffer.extend_from_slice(bytes);
        let _ = self.decode();
    }

    /// Reset the internal buffer and decoded message
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.decoded_message = None;
    }

    /// Retrieve a decoded message if available
    ///
    /// This consumes the decoded message, so subsequent calls will return None
    /// until a new message is decoded.
    pub fn retrieve_message(&mut self) -> Option<Vec<u8>> {
        self.decoded_message.take()
    }

    /// Check if a decoded message is available
    pub fn has_message(&self) -> bool {
        self.decoded_message.is_some()
    }

    /// Decode COBS data from the internal buffer
    ///
    /// This is the Garmin variant which relies on a leading and trailing 0 byte.
    /// The complete message is removed from the internal buffer if successfully decoded.
    fn decode(&mut self) -> Result<()> {
        // If we already have a decoded message waiting, don't decode more
        if self.decoded_message.is_some() {
            return Ok(());
        }

        // Minimum payload length including padding
        if self.buffer.len() < 4 {
            return Ok(());
        }

        // Check for trailing 0x00
        if self.buffer.last() != Some(&0) {
            return Ok(());
        }

        // Don't include the trailing 0 in processing
        let buffer_len = self.buffer.len() - 1;

        // Check for leading 0x00
        if self.buffer[0] != 0 {
            return Ok(());
        }

        let mut decoded = Vec::with_capacity(buffer_len);
        let mut pos = 1; // Skip leading 0x00

        while pos < buffer_len {
            let code = self.buffer[pos];
            if code == 0 {
                break;
            }

            pos += 1;
            let code_value = code as usize;
            let payload_size = code_value - 1;

            // Copy payload bytes
            for _ in 0..payload_size {
                if pos >= buffer_len {
                    return Ok(());
                }
                decoded.push(self.buffer[pos]);
                pos += 1;
            }

            // Append zero byte after payload unless code == 0xFF (continuation)
            // or we hit the terminating zero with non-empty payload
            if code_value != 0xFF {
                if pos < buffer_len && self.buffer[pos] == 0 {
                    // Next byte is terminator - only skip appending zero if we had payload
                    if payload_size == 0 {
                        decoded.push(0);
                    }
                } else {
                    // Either more segments coming or we're past buffer_len - always append
                    decoded.push(0);
                }
            }
        }

        // Successfully decoded - save message and remove from buffer
        self.decoded_message = Some(decoded);
        self.buffer.drain(..=buffer_len);

        Ok(())
    }

    /// Encode data using COBS
    ///
    /// This is the Garmin variant with a leading and trailing 0 byte.
    pub fn encode(data: &[u8]) -> Vec<u8> {
        // Maximum expansion: each byte could potentially require 2 bytes, plus leading/trailing 0
        let mut encoded = Vec::with_capacity(data.len() * 2 + 2);

        // Garmin initial padding
        encoded.push(0);

        if data.is_empty() {
            // For empty data, just add the code byte (0x01) and trailing zero
            encoded.push(0x01);
            encoded.push(0);
            return encoded;
        }

        let mut pos = 0;
        let mut last_was_zero = false;
        let mut last_payload_size = 0;

        while pos < data.len() {
            let start_pos = pos;

            // Find next zero or end of data
            let mut zero_pos = pos;
            while zero_pos < data.len() && data[zero_pos] != 0 {
                zero_pos += 1;
            }

            let mut payload_size = zero_pos - start_pos;
            let mut current_start = start_pos;

            // Handle payloads larger than 0xFE
            while payload_size >= 0xFE {
                encoded.push(0xFF); // Maximum payload size indicator
                encoded.extend_from_slice(&data[current_start..current_start + 0xFE]);
                payload_size -= 0xFE;
                current_start += 0xFE;
            }

            // Encode remaining payload
            encoded.push((payload_size + 1) as u8);
            if payload_size > 0 {
                encoded.extend_from_slice(&data[current_start..current_start + payload_size]);
            }

            last_payload_size = payload_size;

            // Move past the zero byte if we found one
            if zero_pos < data.len() {
                pos = zero_pos + 1;
                last_was_zero = true;
            } else {
                pos = zero_pos;
                last_was_zero = false;
            }
        }

        // If the last operation moved past a zero and we're at the end,
        // emit a code=1 to explicitly represent that trailing zero
        // BUT only if the last segment had non-zero payload (otherwise the zero
        // is already represented by the code=1 we just emitted)
        if last_was_zero && pos >= data.len() && last_payload_size > 0 {
            encoded.push(1);
        }

        // Append trailing zero byte
        encoded.push(0);

        encoded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_empty() {
        let data = [];
        let encoded = CobsCoDec::encode(&data);
        assert_eq!(encoded, vec![0, 1, 0]);
    }

    #[test]
    fn test_encode_simple() {
        let data = [0x01, 0x02, 0x03];
        let encoded = CobsCoDec::encode(&data);
        // Leading 0, then 0x04 (3 bytes + 1), then the data, then trailing 0
        assert_eq!(encoded, vec![0, 4, 1, 2, 3, 0]);
    }

    #[test]
    fn test_encode_with_zero() {
        let data = [0x01, 0x00, 0x02];
        let encoded = CobsCoDec::encode(&data);
        // Leading 0, 0x02 (1 byte + 1), 0x01, 0x02 (1 byte + 1), 0x02, trailing 0
        assert_eq!(encoded, vec![0, 2, 1, 2, 2, 0]);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = vec![0x01, 0x02, 0x03, 0x00, 0x05, 0x06];
        let encoded = CobsCoDec::encode(&original);

        let mut codec = CobsCoDec::new();
        codec.receive_bytes(&encoded);

        let decoded = codec
            .retrieve_message()
            .expect("Should decode successfully");
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_incomplete() {
        let mut codec = CobsCoDec::new();

        // Send incomplete data (no trailing zero)
        codec.receive_bytes(&[0, 4, 1, 2, 3]);

        assert!(!codec.has_message());
        assert!(codec.retrieve_message().is_none());
    }

    #[test]
    fn test_decode_multiple_messages() {
        let mut codec = CobsCoDec::new();

        let msg1 = vec![0x01, 0x02];
        let msg2 = vec![0x03, 0x04];

        let encoded1 = CobsCoDec::encode(&msg1);
        let encoded2 = CobsCoDec::encode(&msg2);

        // Send first message
        codec.receive_bytes(&encoded1);
        assert_eq!(codec.retrieve_message(), Some(msg1));

        // Send second message
        codec.receive_bytes(&encoded2);
        assert_eq!(codec.retrieve_message(), Some(msg2));
    }

    #[test]
    fn test_decode_partial_then_complete() {
        let mut codec = CobsCoDec::new();

        let data = vec![0x01, 0x02, 0x03];
        let encoded = CobsCoDec::encode(&data);

        // Split encoded data
        let mid = encoded.len() / 2;
        let (part1, part2) = encoded.split_at(mid);

        // Send first part
        codec.receive_bytes(part1);
        assert!(!codec.has_message());

        // Send second part
        codec.receive_bytes(part2);
        assert!(codec.has_message());
        assert_eq!(codec.retrieve_message(), Some(data));
    }

    #[test]
    fn test_buffer_timeout() {
        let mut codec = CobsCoDec::with_timeout(Duration::from_millis(1));

        codec.receive_bytes(&[0, 4, 1, 2]);

        // Sleep to trigger timeout
        std::thread::sleep(Duration::from_millis(10));

        // This should reset the buffer
        codec.receive_bytes(&[3]);

        // Buffer should have been cleared and restarted with just [3]
        assert!(!codec.has_message());
    }

    #[test]
    fn test_encode_long_sequence() {
        // Test encoding of data larger than 0xFE
        let data: Vec<u8> = (0..300).map(|i| (i % 256) as u8).collect();
        let encoded = CobsCoDec::encode(&data);

        let mut codec = CobsCoDec::new();
        codec.receive_bytes(&encoded);

        let decoded = codec
            .retrieve_message()
            .expect("Should decode long sequence");
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_encode_all_zeros() {
        let data = vec![0, 0, 0];
        let encoded = CobsCoDec::encode(&data);
        // All zeros encode as: leading 0, then 0x01 for each zero, trailing 0
        // [0, 1, 1, 1, 0]
        assert_eq!(encoded, vec![0, 1, 1, 1, 0]);

        let mut codec = CobsCoDec::new();
        codec.receive_bytes(&encoded);

        let decoded = codec.retrieve_message().expect("Should decode zeros");
        // With the fixed COBS decode, trailing zeros are now properly preserved
        assert_eq!(decoded.len(), 3); // Decoded as [0, 0, 0]
        assert_eq!(decoded, vec![0, 0, 0]);
    }
}
