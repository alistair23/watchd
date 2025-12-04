//! Multi-Link Reliable (MLR) Protocol Implementation
//!
//! This module implements the MLR protocol used by Garmin devices for reliable
//! message transmission over BLE. The protocol provides automatic retransmission,
//! acknowledgment, and flow control.
//!
//! Reference: <https://gadgetbridge.org/internals/specifics/garmin-protocol/#multi-link-reliable-protocol>

use crate::types::{GarminError, Result};
use log::{debug, error, info, warn};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

// MLR Protocol Constants
const MLR_FLAG_MASK: u8 = 0x80;
const HANDLE_MASK: u8 = 0x70;
const HANDLE_SHIFT: u8 = 4;
const REQ_NUM_MASK: u8 = 0x0F;
const SEQ_NUM_MASK: u8 = 0x3F;

const MAX_SEQ_NUM: u8 = 0x3F;
const INITIAL_MAX_UNACKED_SEND: usize = 0x20;
const MAX_RETRANSMISSION_TIMEOUT_MS: u64 = 20000;
const INITIAL_RETRANSMISSION_TIMEOUT_MS: u64 = 1000;
const ACK_TIMEOUT_MS: u64 = 250;
const ACK_TRIGGER_THRESHOLD: usize = 5;

/// A fragment of a message to be sent
#[derive(Debug, Clone)]
struct Fragment {
    task_name: String,
    num: usize,
    data: Vec<u8>,
}

/// Message sender trait for sending packets to the device
pub trait MessageSender: Send + Sync {
    fn send_packet(&self, task_name: &str, packet: &[u8]) -> Result<()>;
}

/// Message receiver trait for handling received data
pub trait MessageReceiver: Send + Sync {
    fn on_data_received(&self, data: &[u8]) -> Result<()>;
}

/// MLR Communicator state
struct MlrState {
    handle: u8,
    max_packet_size: usize,
    last_send_ack: u8,
    next_send_seq: u8,
    next_rcv_seq: u8,
    last_rcv_ack: u8,
    max_num_unacked_send: usize,
    retransmission_timeout: Duration,
    fragment_queue: VecDeque<Fragment>,
    sent_fragments: [Option<Fragment>; (MAX_SEQ_NUM + 1) as usize],
    last_ack_time: Option<Instant>,
    last_retransmit_time: Option<Instant>,
    paused: bool,
}

impl MlrState {
    fn new(handle: u8, max_packet_size: usize) -> Self {
        Self {
            handle,
            max_packet_size,
            last_send_ack: 0,
            next_send_seq: 0,
            next_rcv_seq: 0,
            last_rcv_ack: 0,
            max_num_unacked_send: INITIAL_MAX_UNACKED_SEND,
            retransmission_timeout: Duration::from_millis(INITIAL_RETRANSMISSION_TIMEOUT_MS),
            fragment_queue: VecDeque::new(),
            sent_fragments: [const { None }; (MAX_SEQ_NUM + 1) as usize],
            last_ack_time: None,
            last_retransmit_time: None,
            paused: false,
        }
    }
}

/// MLR Communicator for reliable message transmission
pub struct MlrCommunicator {
    state: Arc<Mutex<MlrState>>,
    sender: Arc<dyn MessageSender>,
    receiver: Arc<dyn MessageReceiver>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl MlrCommunicator {
    /// Create a new MLR communicator
    pub fn new(
        handle: u8,
        max_packet_size: usize,
        sender: Arc<dyn MessageSender>,
        receiver: Arc<dyn MessageReceiver>,
    ) -> Self {
        debug!("Creating MLR communicator for handle {}", handle);

        Self {
            state: Arc::new(Mutex::new(MlrState::new(handle, max_packet_size))),
            sender,
            receiver,
            shutdown_tx: None,
        }
    }

    /// Set the maximum packet size
    pub fn set_max_packet_size(&self, max_packet_size: usize) {
        let mut state = self.state.lock().unwrap();
        state.max_packet_size = max_packet_size;
        debug!("MLR max packet size set to {}", max_packet_size);
    }

    /// Start the MLR communicator with background timers
    pub fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.shutdown_tx = Some(tx);

        let state = Arc::clone(&self.state);
        let sender = Arc::clone(&self.sender);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;

                if rx.try_recv().is_ok() {
                    debug!("MLR communicator shutting down");
                    break;
                }

                if let Err(e) = Self::check_ack_timeout(&state, &sender) {
                    error!("ACK timeout check failed: {}", e);
                }

                if let Err(e) = Self::check_retransmit_timeout(&state, &sender) {
                    error!("Retransmit timeout check failed: {}", e);
                    // If error contains "Not connected", clear MLR state to stop retries
                    let err_msg = format!("{}", e);
                    if err_msg.contains("Not connected") || err_msg.contains("not connected") {
                        warn!(
                            "Detected disconnection - clearing MLR state to stop retransmissions"
                        );
                        Self::clear_state(&state);
                    }
                }
            }
        });

        Ok(())
    }

    /// Queue a message for sending
    pub fn send_message(&self, task_name: &str, message: &[u8]) -> Result<()> {
        if message.is_empty() {
            return Err(GarminError::EmptyMessage);
        }

        let mut state = self.state.lock().unwrap();

        // Don't queue new messages if paused
        if state.paused {
            debug!("MLR is paused - rejecting send_message for {}", task_name);
            return Err(GarminError::BluetoothError(
                "MLR is paused during reconnection".to_string(),
            ));
        }

        debug!(
            "Queuing MLR message for '{}' ({} bytes)",
            task_name,
            message.len()
        );

        let max_data_size = state.max_packet_size.saturating_sub(2);
        let mut remaining = message.len();
        let mut position = 0;
        let mut fragment_num = 0;

        while remaining > 0 {
            let chunk_size = remaining.min(max_data_size);
            let fragment = Fragment {
                task_name: task_name.to_string(),
                num: fragment_num,
                data: message[position..position + chunk_size].to_vec(),
            };
            state.fragment_queue.push_back(fragment);
            position += chunk_size;
            remaining -= chunk_size;
            fragment_num += 1;
        }

        drop(state);
        self.run_protocol()?;

        Ok(())
    }

    /// Handle a received packet
    pub fn on_packet_received(&self, packet: &[u8]) -> Result<()> {
        if packet.len() < 2 {
            return Err(GarminError::PacketTooShort(packet.len()));
        }

        let byte0 = packet[0];
        let byte1 = packet[1];

        // Check MLR flag
        if (byte0 & MLR_FLAG_MASK) == 0 {
            return Err(GarminError::NotMlrPacket);
        }

        let packet_handle = (byte0 & HANDLE_MASK) >> HANDLE_SHIFT;
        let req_num = ((byte0 & REQ_NUM_MASK) << 2) | ((byte1 >> 6) & 0x03);
        let seq_num = byte1 & SEQ_NUM_MASK;

        let mut state = self.state.lock().unwrap();

        if packet_handle != (state.handle & 0x07) {
            return Err(GarminError::InvalidHandle {
                expected: state.handle & 0x07,
                got: packet_handle,
            });
        }

        debug!(
            "MLR packet received: reqNum={}, seqNum={}, dataLen={}",
            req_num,
            seq_num,
            packet.len() - 2
        );

        // Process ACK if request number changed
        if req_num != state.last_rcv_ack {
            Self::process_ack(&mut state, req_num)?;
        }

        // Process data if any, and in sequence
        if packet.len() > 2 {
            if seq_num == state.next_rcv_seq {
                // In-sequence packet
                let data = packet[2..].to_vec();
                state.next_rcv_seq = (state.next_rcv_seq + 1) % (MAX_SEQ_NUM + 1);
                info!("Scheduling ACK");
                Self::schedule_ack(&mut state);
                drop(state);

                if let Err(e) = self.receiver.on_data_received(&data) {
                    error!("Receiver failed to handle MLR data: {}", e);
                }
            } else {
                warn!(
                    "Out-of-sequence packet - expected {}, got {}",
                    state.next_rcv_seq, seq_num
                );
                // Correct sequence will be retransmitted by sender
                // Regardless, re-send the expected ack since the sender shouldn't be sending these
                Self::send_ack_packet(&state, &self.sender)?;
                drop(state);
            }
        } else {
            drop(state);
        }

        info!("Running Protocol");
        // Run protocol multiple times to drain queue if window opened up
        for _ in 0..10 {
            match self.run_protocol() {
                Ok(_) => {
                    // Check if there are more fragments to send
                    let state = self.state.lock().unwrap();
                    if state.fragment_queue.is_empty() {
                        break;
                    }
                    let num_sent_unacked = Self::seq_diff(state.next_send_seq, state.last_rcv_ack);
                    if num_sent_unacked >= state.max_num_unacked_send {
                        debug!("Send window full, will resume after next ACK");
                        break;
                    }
                    drop(state);
                }
                Err(e) => {
                    error!("Protocol run failed: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Process an acknowledgment
    fn process_ack(state: &mut MlrState, req_num: u8) -> Result<()> {
        let num_acked = Self::seq_diff(req_num, state.last_rcv_ack);

        debug!(
            "Processing ACK: reqNum={}, numAcked={}, expiring fragments [{}, {}]",
            req_num,
            num_acked,
            state.last_rcv_ack,
            req_num.wrapping_sub(1)
        );

        // Clear retransmit timer
        state.last_retransmit_time = None;

        // Remove acked messages from the array
        let mut i = state.last_rcv_ack;
        while i != req_num {
            if state.sent_fragments[i as usize].is_none() {
                // This can happen with duplicate ACKs or out-of-order packets
                debug!("Fragment at index {} already cleared (duplicate ACK?)", i);
            }
            state.sent_fragments[i as usize] = None;
            i = (i + 1) % (MAX_SEQ_NUM + 1);
        }

        state.last_rcv_ack = req_num;

        // Restart retransmission timer if there are still unacked packets
        // (timer was already cleared at the start of this function)
        if state.last_rcv_ack != state.next_send_seq {
            state.last_retransmit_time = Some(Instant::now());
        }

        debug!(
            "After ACK processing: last_rcv_ack={}, next_send_seq={}, unacked={}",
            state.last_rcv_ack,
            state.next_send_seq,
            Self::seq_diff(state.next_send_seq, state.last_rcv_ack)
        );

        Ok(())
    }

    /// Schedule an ACK to be sent
    fn schedule_ack(state: &mut MlrState) {
        let num_rcvd_unacked = Self::seq_diff(state.next_rcv_seq, state.last_send_ack);

        if num_rcvd_unacked >= ACK_TRIGGER_THRESHOLD {
            state.last_send_ack = state.next_rcv_seq;
            state.last_ack_time = None;
        } else {
            state.last_ack_time = Some(Instant::now());
        }
    }

    /// Send an ACK-only packet
    fn send_ack_packet(state: &MlrState, sender: &Arc<dyn MessageSender>) -> Result<()> {
        let packet = Self::create_packet(state, state.next_rcv_seq, 0, &[]);
        sender.send_packet(&format!("ack reqNum={}", state.next_rcv_seq), &packet)?;
        debug!("Sent ACK packet: reqNum={}", state.next_rcv_seq);
        Ok(())
    }

    /// Run the protocol - send pending fragments if possible
    fn run_protocol(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        // Don't send anything if paused
        if state.paused {
            return Ok(());
        }

        let num_sent_unacked = Self::seq_diff(state.next_send_seq, state.last_rcv_ack);

        if num_sent_unacked >= state.max_num_unacked_send {
            debug!(
                "Cannot send more packets: {} unacked, max {} (queue_len={})",
                num_sent_unacked,
                state.max_num_unacked_send,
                state.fragment_queue.len()
            );
            return Ok(());
        }

        if state.fragment_queue.is_empty() {
            // No more fragments to send
            return Ok(());
        }

        if let Some(fragment) = state.fragment_queue.pop_front() {
            let packet = Self::create_packet(
                &state,
                state.next_rcv_seq,
                state.next_send_seq,
                &fragment.data,
            );
            let task_name = format!("{} ({})", fragment.task_name, fragment.num);

            let seq = state.next_send_seq;
            state.sent_fragments[seq as usize] = Some(fragment);
            state.next_send_seq = (state.next_send_seq + 1) % (MAX_SEQ_NUM + 1);

            // Start retransmission timer if this is the first unacked packet
            if num_sent_unacked == 0 {
                state.last_retransmit_time = Some(Instant::now());
            }

            drop(state);
            self.sender.send_packet(&task_name, &packet)?;

            debug!(
                "Sent MLR packet: seqNum={}, dataLen={}",
                seq,
                packet.len() - 2
            );
        }

        Ok(())
    }

    /// Create an MLR packet
    fn create_packet(state: &MlrState, req_num: u8, seq_num: u8, data: &[u8]) -> Vec<u8> {
        let mut packet = Vec::with_capacity(2 + data.len());

        // First byte: MLR flag (1) + handle (3 bits) + reqNum high bits (4 bits)
        let byte0 = MLR_FLAG_MASK
            | ((state.handle & 0x07) << HANDLE_SHIFT)
            | ((req_num >> 2) & REQ_NUM_MASK);
        packet.push(byte0);

        // Second byte: reqNum low bits (2 bits) + seqNum (6 bits)
        let byte1 = ((req_num & 0x03) << 6) | (seq_num & SEQ_NUM_MASK);
        packet.push(byte1);

        // Data
        packet.extend_from_slice(data);

        packet
    }

    /// Check if ACK timeout has expired and send ACK if needed
    fn check_ack_timeout(
        state: &Arc<Mutex<MlrState>>,
        sender: &Arc<dyn MessageSender>,
    ) -> Result<()> {
        let mut state_guard = state.lock().unwrap();

        // Don't send ACKs if paused
        if state_guard.paused {
            return Ok(());
        }

        if let Some(last_ack_time) = state_guard.last_ack_time {
            if last_ack_time.elapsed() >= Duration::from_millis(ACK_TIMEOUT_MS) {
                let next_rcv_seq = state_guard.next_rcv_seq;
                let packet = Self::create_packet(&state_guard, next_rcv_seq, 0, &[]);
                state_guard.last_send_ack = next_rcv_seq;
                state_guard.last_ack_time = None;

                drop(state_guard);
                sender.send_packet(&format!("ack reqNum={}", next_rcv_seq), &packet)?;
                debug!("Sent ACK packet after timeout: reqNum={}", next_rcv_seq);
            }
        }

        Ok(())
    }

    /// Pause MLR operations (prevents sending and retransmissions)
    pub fn pause(&self) {
        let mut state_guard = self.state.lock().unwrap();
        debug!("Pausing MLR communicator");
        state_guard.paused = true;
    }

    /// Resume MLR operations
    pub fn resume(&self) {
        let mut state_guard = self.state.lock().unwrap();
        debug!("Resuming MLR communicator");
        state_guard.paused = false;
    }

    /// Clear MLR state and pause (used when connection is lost)
    pub fn clear_and_pause(&self) {
        let mut state_guard = self.state.lock().unwrap();

        debug!("Clearing and pausing MLR state due to disconnection");

        // Pause operations
        state_guard.paused = true;

        // Clear sent fragments
        for i in 0..state_guard.sent_fragments.len() {
            state_guard.sent_fragments[i] = None;
        }

        // Clear fragment queue
        state_guard.fragment_queue.clear();

        // Reset sequence numbers but don't reset next_send_seq (keep it for after reconnect)
        state_guard.last_rcv_ack = state_guard.next_send_seq;

        // Clear timers
        state_guard.last_ack_time = None;
        state_guard.last_retransmit_time = None;

        // Reset timeout parameters to defaults
        state_guard.retransmission_timeout =
            Duration::from_millis(INITIAL_RETRANSMISSION_TIMEOUT_MS);
        state_guard.max_num_unacked_send = INITIAL_MAX_UNACKED_SEND;

        debug!("MLR state cleared and paused");
    }

    /// Clear MLR state (used when connection is lost)
    fn clear_state(state: &Arc<Mutex<MlrState>>) {
        let mut state_guard = state.lock().unwrap();

        debug!("Clearing MLR state due to disconnection");

        // Clear sent fragments
        for i in 0..state_guard.sent_fragments.len() {
            state_guard.sent_fragments[i] = None;
        }

        // Clear fragment queue
        state_guard.fragment_queue.clear();

        // Reset sequence numbers but don't reset next_send_seq (keep it for after reconnect)
        state_guard.last_rcv_ack = state_guard.next_send_seq;

        // Clear timers
        state_guard.last_ack_time = None;
        state_guard.last_retransmit_time = None;

        // Reset timeout parameters to defaults
        state_guard.retransmission_timeout =
            Duration::from_millis(INITIAL_RETRANSMISSION_TIMEOUT_MS);
        state_guard.max_num_unacked_send = INITIAL_MAX_UNACKED_SEND;

        debug!("MLR state cleared");
    }

    /// Check if retransmission timeout has expired and retransmit if needed
    fn check_retransmit_timeout(
        state: &Arc<Mutex<MlrState>>,
        sender: &Arc<dyn MessageSender>,
    ) -> Result<()> {
        let mut state_guard = state.lock().unwrap();

        // Don't retransmit if paused
        if state_guard.paused {
            return Ok(());
        }

        if let Some(last_retransmit) = state_guard.last_retransmit_time {
            if last_retransmit.elapsed() >= state_guard.retransmission_timeout {
                debug!("Retransmission timeout expired");

                // Backoff retransmission timeout and reduce the maximum unacked
                let new_timeout = (state_guard.retransmission_timeout.as_millis() as u64 * 2)
                    .min(MAX_RETRANSMISSION_TIMEOUT_MS);
                state_guard.retransmission_timeout = Duration::from_millis(new_timeout);
                state_guard.max_num_unacked_send =
                    state_guard.max_num_unacked_send.div_ceil(2).max(1);

                debug!(
                    "Retransmission: timeout={}ms, maxUnacked={}, will re-send fragments [{}, {}]",
                    new_timeout,
                    state_guard.max_num_unacked_send,
                    state_guard.last_rcv_ack,
                    state_guard.next_send_seq.wrapping_sub(1)
                );

                // Re-send all unacked fragments
                let mut i = state_guard.last_rcv_ack;
                let mut sent_count = 0;
                while i != state_guard.next_send_seq {
                    if let Some(fragment) = &state_guard.sent_fragments[i as usize] {
                        let packet = Self::create_packet(
                            &state_guard,
                            state_guard.next_rcv_seq,
                            i,
                            &fragment.data,
                        );
                        let task_name =
                            format!("retransmission {} ({})", fragment.task_name, fragment.num);

                        drop(state_guard);

                        // Try to send, but if we get "Not connected" error, abort immediately
                        match sender.send_packet(&task_name, &packet) {
                            Ok(_) => {
                                state_guard = state.lock().unwrap();
                            }
                            Err(e) => {
                                let err_msg = format!("{}", e);
                                if err_msg.contains("Not connected")
                                    || err_msg.contains("not connected")
                                {
                                    // Connection lost - clear state and abort retransmission
                                    warn!("Connection lost during retransmission - clearing MLR state");
                                    state_guard = state.lock().unwrap();
                                    // Clear sent fragments to stop retransmission loop
                                    for i in 0..state_guard.sent_fragments.len() {
                                        state_guard.sent_fragments[i] = None;
                                    }
                                    state_guard.fragment_queue.clear();
                                    state_guard.last_retransmit_time = None;
                                    return Err(e);
                                }
                                return Err(e);
                            }
                        }

                        debug!("Re-sent fragment {}", i);
                        sent_count += 1;
                    } else {
                        error!("Attempting to re-send null fragment at index {}", i);
                    }

                    i = (i + 1) % (MAX_SEQ_NUM + 1);
                }

                // Only restart the timer if we actually sent some fragments
                if sent_count > 0 {
                    state_guard.last_retransmit_time = Some(Instant::now());
                } else {
                    // All fragments were null, clear the timer to stop retrying
                    state_guard.last_retransmit_time = None;
                    warn!(
                        "Retransmission timeout with no valid fragments to send - clearing timer"
                    );
                }
            }
        }

        Ok(())
    }

    /// Calculate the difference between two sequence numbers (wrapping)
    fn seq_diff(a: u8, b: u8) -> usize {
        ((a as isize - b as isize + MAX_SEQ_NUM as isize + 1) % (MAX_SEQ_NUM as isize + 1)) as usize
    }

    /// Close the MLR communicator and stop background tasks
    pub fn close(&mut self) {
        debug!("Closing MLR communicator");

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.try_send(());
        }

        let mut state = self.state.lock().unwrap();
        state.fragment_queue.clear();
    }

    /// Handle connection state changes
    pub fn on_connection_state_change(&self, connected: bool) {
        if !connected {
            let mut state = self.state.lock().unwrap();
            state.last_ack_time = None;
            state.last_retransmit_time = None;
        }
    }
}

impl Drop for MlrCommunicator {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    struct TestSender {
        packets: Arc<StdMutex<Vec<Vec<u8>>>>,
    }

    impl TestSender {
        fn new() -> Self {
            Self {
                packets: Arc::new(StdMutex::new(Vec::new())),
            }
        }

        fn get_packets(&self) -> Vec<Vec<u8>> {
            self.packets.lock().unwrap().clone()
        }

        #[allow(dead_code)]
        fn clear(&self) {
            self.packets.lock().unwrap().clear();
        }
    }

    impl MessageSender for TestSender {
        fn send_packet(&self, _task_name: &str, packet: &[u8]) -> Result<()> {
            self.packets.lock().unwrap().push(packet.to_vec());
            Ok(())
        }
    }

    struct TestReceiver {
        messages: Arc<StdMutex<Vec<Vec<u8>>>>,
    }

    impl TestReceiver {
        fn new() -> Self {
            Self {
                messages: Arc::new(StdMutex::new(Vec::new())),
            }
        }

        fn get_messages(&self) -> Vec<Vec<u8>> {
            self.messages.lock().unwrap().clone()
        }
    }

    impl MessageReceiver for TestReceiver {
        fn on_data_received(&self, data: &[u8]) -> Result<()> {
            self.messages.lock().unwrap().push(data.to_vec());
            Ok(())
        }
    }

    #[test]
    fn test_mlr_create() {
        let sender = Arc::new(TestSender::new());
        let receiver = Arc::new(TestReceiver::new());
        let _mlr = MlrCommunicator::new(1, 100, sender, receiver);
    }

    #[test]
    fn test_mlr_send_small_message() {
        let sender = Arc::new(TestSender::new());
        let receiver = Arc::new(TestReceiver::new());
        let mlr = MlrCommunicator::new(1, 100, sender.clone(), receiver);

        let message = vec![0x01, 0x02, 0x03, 0x04];
        mlr.send_message("test", &message).unwrap();

        let packets = sender.get_packets();
        assert_eq!(packets.len(), 1);

        // Check packet structure
        let packet = &packets[0];
        assert!(packet[0] & MLR_FLAG_MASK != 0); // MLR flag set
        assert_eq!(packet.len() - 2, message.len()); // Data length
    }

    #[test]
    fn test_mlr_send_fragmented_message() {
        let sender = Arc::new(TestSender::new());
        let receiver = Arc::new(TestReceiver::new());
        let mlr = MlrCommunicator::new(1, 10, sender.clone(), receiver);

        let message = vec![0x01; 30]; // 30 bytes with max_packet_size 10 should fragment
        mlr.send_message("test", &message).unwrap();

        // The message should be queued internally as fragments
        // We can't easily test the actual sending without async runtime
        // but we can verify the send_message doesn't error with large messages
        // This tests that fragmentation logic doesn't panic
    }

    #[test]
    fn test_mlr_receive_packet() {
        let sender = Arc::new(TestSender::new());
        let receiver = Arc::new(TestReceiver::new());
        let mlr = MlrCommunicator::new(1, 100, sender, receiver.clone());

        // Create a valid MLR packet with handle 1, reqNum 0, seqNum 0
        let mut packet = vec![0x90, 0x00]; // MLR flag + handle 1
        packet.extend_from_slice(&[0x01, 0x02, 0x03]);

        mlr.on_packet_received(&packet).unwrap();

        let messages = receiver.get_messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0], vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_seq_diff() {
        assert_eq!(MlrCommunicator::seq_diff(5, 3), 2);
        assert_eq!(MlrCommunicator::seq_diff(1, 63), 2); // Wrap around
        assert_eq!(MlrCommunicator::seq_diff(0, 0), 0);
    }
}
